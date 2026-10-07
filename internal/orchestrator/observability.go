package orchestrator

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// MaxHistoryEvents bounds journal storage per retained run. Older entries roll off.
const MaxHistoryEvents = 4096

var observationsBucket = []byte("run_events")
var observationCount = []byte("_count")
var observationDropped = []byte("_dropped_through")

// HistoryEvent records committed execution metadata. It deliberately excludes
// inputs, outputs, response bodies, credentials, and arbitrary action errors.
type HistoryEvent struct {
	ID            string    `json:"id"`
	Time          time.Time `json:"time"`
	RunID         string    `json:"run_id"`
	WorkflowID    string    `json:"workflow_id"`
	Version       int       `json:"version"`
	Kind          string    `json:"kind"`
	StepPath      string    `json:"step_path,omitempty"`
	OperationKey  string    `json:"operation_key,omitempty"`
	State         string    `json:"state,omitempty"`
	PreviousState string    `json:"previous_state,omitempty"`
	Attempt       int       `json:"attempt,omitempty"`
	Phase         string    `json:"phase,omitempty"`
	ResumeCount   int       `json:"resume_count,omitempty"`
	Reason        string    `json:"reason,omitempty"`
}

type HistoryPage struct {
	Events         []HistoryEvent `json:"events"`
	NextAfter      string         `json:"next_after"`
	HasMore        bool           `json:"has_more"`
	DroppedThrough string         `json:"dropped_through,omitempty"`
	Truncated      bool           `json:"truncated"`
	State          string         `json:"state"`
}

// MetricsSnapshot describes current retained state, not lifetime counters.
type MetricsSnapshot struct {
	Time         time.Time      `json:"time"`
	Ready        bool           `json:"ready"`
	Runs         map[string]int `json:"runs"`
	RetainedRuns int            `json:"retained_runs"`
	ActiveRuns   int            `json:"active_runs"`
	QueuedRuns   int            `json:"queued_runs"`
	RunLimit     int            `json:"run_limit"`
	QueueLimit   int            `json:"queue_limit"`
	WorkerLimit  int            `json:"worker_limit"`
	HistoryLimit int            `json:"history_limit_per_run"`
}

// Existing format-2 databases acquire an empty journal. No past transitions are
// invented, and old completed result records remain byte-for-byte unchanged.
func initializeObservability(tx *bolt.Tx) error {
	_, err := tx.CreateBucketIfNotExists(observationsBucket)
	return err
}

func verifyObservability(tx *bolt.Tx) error {
	root := tx.Bucket(observationsBucket)
	if root == nil {
		return errors.New("run journal bucket is missing")
	}
	last, err := identity.Last(tx)
	if err != nil {
		return err
	}
	invalid := errors.New("run journal index is inconsistent")
	return root.ForEach(func(runID, value []byte) error {
		if value != nil || tx.Bucket(runsBucket).Get(runID) == nil {
			return invalid
		}
		bucket := root.Bucket(runID)
		count := bucket.Get(observationCount)
		if len(count) != 8 || binary.BigEndian.Uint64(count) > MaxHistoryEvents {
			return invalid
		}
		dropped := string(bucket.Get(observationDropped))
		if dropped != "" && (!identity.Valid(dropped) || dropped > last) {
			return invalid
		}
		seen := uint64(0)
		if err := bucket.ForEach(func(key, data []byte) error {
			if string(key) == string(observationCount) || string(key) == string(observationDropped) {
				return nil
			}
			var event HistoryEvent
			if !identity.Valid(string(key)) || string(key) > last || string(key) <= dropped || workflow.Decode(data, &event) != nil || event.ID != string(key) || event.RunID != string(runID) {
				return invalid
			}
			seen++
			return nil
		}); err != nil {
			return err
		}
		if seen != binary.BigEndian.Uint64(count) {
			return invalid
		}
		return nil
	})
}

func pruneObservations(tx *bolt.Tx, id string) error {
	bucket := tx.Bucket(observationsBucket)
	if bucket == nil || bucket.Bucket([]byte(id)) == nil {
		return nil
	}
	return bucket.DeleteBucket([]byte(id))
}

// recordTransitions must run before replacing the run row in the same transaction.
func recordTransitions(tx *bolt.Tx, next Run) error {
	var previous Run
	if tx.Bucket(runsBucket).Get([]byte(next.ID)) != nil {
		record, err := readRun(tx, next.ID)
		if err != nil {
			return err
		}
		previous = record.Run
	}
	appendEvent := func(event HistoryEvent) error {
		event.RunID, event.WorkflowID, event.Version = next.ID, next.WorkflowID, next.Version
		event.Time = time.Now().UTC()
		if event.StepPath != "" {
			event.OperationKey = next.ID + "/" + event.StepPath
		}
		return appendObservation(tx, event)
	}
	if previous.State != next.State {
		if err := appendEvent(HistoryEvent{Kind: "run.state", State: next.State, PreviousState: previous.State}); err != nil {
			return err
		}
	}
	if next.CancellationRequested && !previous.CancellationRequested {
		if err := appendEvent(HistoryEvent{Kind: "run.cancel_requested", State: next.State}); err != nil {
			return err
		}
	}
	if next.ResumeCount > previous.ResumeCount {
		if err := appendEvent(HistoryEvent{Kind: "run.resume", State: next.State, ResumeCount: next.ResumeCount, Reason: next.ResumeReason}); err != nil {
			return err
		}
	}
	oldSteps := make(map[string]workflow.StepResult)
	var collect func([]workflow.StepResult, string)
	collect = func(steps []workflow.StepResult, parent string) {
		for _, step := range steps {
			path := parent + step.ID
			oldSteps[path] = step
			collect(step.Children, path+"/")
		}
	}
	collect(previous.Steps, "")
	var visit func([]workflow.StepResult, string) error
	visit = func(steps []workflow.StepResult, parent string) error {
		for _, step := range steps {
			path := parent + step.ID
			before := oldSteps[path]
			if before.State != step.State {
				if err := appendEvent(HistoryEvent{Kind: "step.state", StepPath: path, State: step.State, PreviousState: before.State, Attempt: step.Attempts}); err != nil {
					return err
				}
			}
			if before.Attempts != step.Attempts && step.Attempts > 0 {
				if err := appendEvent(HistoryEvent{Kind: "step.attempt", StepPath: path, State: step.State, Attempt: step.Attempts}); err != nil {
					return err
				}
			}
			if before.Phase != step.Phase && step.Phase != "" {
				if err := appendEvent(HistoryEvent{Kind: "step.phase", StepPath: path, State: step.State, Attempt: step.Attempts, Phase: step.Phase}); err != nil {
					return err
				}
			}
			if err := visit(step.Children, path+"/"); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(next.Steps, "")
}

func appendObservation(tx *bolt.Tx, event HistoryEvent) error {
	root := tx.Bucket(observationsBucket)
	if root == nil {
		return errors.New("run journal bucket is missing")
	}
	bucket, err := root.CreateBucketIfNotExists([]byte(event.RunID))
	if err != nil {
		return err
	}
	id, err := identity.Next(tx, event.Time)
	if err != nil {
		return err
	}
	event.ID = id
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if err := bucket.Put([]byte(id), data); err != nil {
		return err
	}
	count := uint64(0)
	if value := bucket.Get(observationCount); len(value) == 8 {
		count = binary.BigEndian.Uint64(value)
	}
	count++
	if count > MaxHistoryEvents {
		key, _ := bucket.Cursor().First()
		if !identity.Valid(string(key)) {
			return errors.New("run journal index is inconsistent")
		}
		dropped := append([]byte(nil), key...)
		if err := bucket.Delete(key); err != nil {
			return err
		}
		if err := bucket.Put(observationDropped, dropped); err != nil {
			return err
		}
		count--
	}
	return bucket.Put(observationCount, binary.BigEndian.AppendUint64(nil, count))
}

func (s *Service) History(id, after string, limit int) (HistoryPage, error) {
	if limit < 1 || limit > 100 || after != "" && !identity.Valid(after) {
		return HistoryPage{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.history(id, after, limit)
}

func (s *Service) history(id, after string, limit int) (HistoryPage, error) {
	page := HistoryPage{Events: []HistoryEvent{}, NextAfter: after}
	err := s.db.View(func(tx *bolt.Tx) error {
		record, err := readRun(tx, id)
		if err != nil {
			return err
		}
		page.State = record.Run.State
		bucket := tx.Bucket(observationsBucket).Bucket([]byte(id))
		if bucket == nil {
			return nil
		}
		page.DroppedThrough = string(bucket.Get(observationDropped))
		page.Truncated = page.DroppedThrough != "" && after < page.DroppedThrough
		cursor := bucket.Cursor()
		key, data := cursor.First()
		if after != "" {
			key, data = cursor.Seek([]byte(after))
			if string(key) == after {
				key, data = cursor.Next()
			}
		}
		for ; key != nil && identity.Valid(string(key)); key, data = cursor.Next() {
			if len(page.Events) == limit {
				page.HasMore = true
				break
			}
			var event HistoryEvent
			if err := workflow.Decode(data, &event); err != nil {
				return err
			}
			page.Events = append(page.Events, event)
			page.NextAfter = event.ID
		}
		return nil
	})
	return page, err
}

// WaitHistory waits for a committed change. Canceling this wait never cancels a run.
func (s *Service) WaitHistory(ctx context.Context, id, after string, limit int) (HistoryPage, error) {
	if limit < 1 || limit > 100 || after != "" && !identity.Valid(after) {
		return HistoryPage{}, ErrInvalid
	}
	for {
		s.mu.Lock()
		page, err := s.history(id, after, limit)
		changed, stopping := s.changed, s.closing || s.storageFailed
		s.mu.Unlock()
		if err != nil || len(page.Events) > 0 || terminal(page.State) {
			return page, err
		}
		if stopping {
			return page, ErrUnavailable
		}
		select {
		case <-ctx.Done():
			return page, ctx.Err()
		case <-changed:
		}
	}
}

func (s *Service) Metrics() (MetricsSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := MetricsSnapshot{Time: time.Now().UTC(), Ready: !s.closing && !s.storageFailed,
		Runs: map[string]int{}, ActiveRuns: len(s.active), RunLimit: s.options.MaxRuns,
		QueueLimit: s.options.QueueSize, WorkerLimit: s.options.Workers, HistoryLimit: MaxHistoryEvents}
	err := s.db.View(func(tx *bolt.Tx) error {
		snapshot.QueuedRuns = countKeys(tx.Bucket(pendingBucket))
		return tx.Bucket(summariesBucket).ForEach(func(_, value []byte) error {
			var run Run
			if err := workflow.Decode(value, &run); err != nil {
				return err
			}
			snapshot.Runs[run.State]++
			snapshot.RetainedRuns++
			return nil
		})
	})
	return snapshot, err
}
