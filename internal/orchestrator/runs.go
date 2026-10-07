package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

type Request struct {
	WorkflowID    string               `json:"workflow_id,omitempty"`
	Version       int                  `json:"version,omitempty"`
	Specification *workflow.Definition `json:"specification,omitempty"`
	Input         map[string]any       `json:"input,omitempty"`
	Async         bool                 `json:"async,omitempty"`
}

type Run struct {
	workflow.Result
	CreatedAt             time.Time `json:"created_at"`
	CancellationRequested bool      `json:"cancellation_requested,omitempty"`
	SequenceID            string    `json:"sequence_id"`
	ResumeCount           int       `json:"resume_count,omitempty"`
	ResumeReason          string    `json:"resume_reason,omitempty"`
}

type storedRun struct {
	Run        Run                  `json:"run"`
	Input      map[string]any       `json:"input"`
	Inline     *workflow.Definition `json:"inline,omitempty"`
	PendingKey string               `json:"pending_key,omitempty"`
}

type receipt struct {
	ID        string    `json:"id,omitempty"`
	Hash      string    `json:"hash"`
	IDs       []string  `json:"run_ids"`
	CreatedAt time.Time `json:"created_at"`
}

func hash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func timeKey(t time.Time) string { return t.UTC().Format("20060102T150405.000000000") }
func terminal(state string) bool { return state != "queued" && state != "running" }

// Bucket.Stats can describe pages from before mutations in the current write
// transaction. Cursor counts include removals made by retention in this admission.
func countKeys(bucket *bolt.Bucket) int {
	count := 0
	cursor := bucket.Cursor()
	for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
		count++
	}
	return count
}

func put(tx *bolt.Tx, bucket []byte, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.Bucket(bucket).Put([]byte(key), data)
}

func readRun(tx *bolt.Tx, id string) (storedRun, error) {
	var record storedRun
	data := tx.Bucket(runsBucket).Get([]byte(id))
	if data == nil {
		return record, ErrNotFound
	}
	err := workflow.Decode(data, &record)
	if err == nil && record.Run.SequenceID == "" {
		var summary Run
		err = workflow.Decode(tx.Bucket(summariesBucket).Get([]byte(id)), &summary)
		record.Run.SequenceID = summary.SequenceID
	}
	return record, err
}

func saveRun(tx *bolt.Tx, record storedRun) error {
	if !terminal(record.Run.State) {
		if err := artifact.Pin(tx, "run/"+record.Run.ID, record.Run.ID, record.Run); err != nil {
			return err
		}
	}
	if record.Run.SequenceID == "" {
		id, err := identity.Next(tx, record.Run.CreatedAt)
		if err != nil {
			return err
		}
		record.Run.SequenceID = id
	}
	if err := recordTransitions(tx, record.Run); err != nil {
		return err
	}
	if err := put(tx, runsBucket, record.Run.ID, record); err != nil {
		return err
	}
	if err := saveRunIndex(tx, record.Run); err != nil {
		return err
	}
	if terminal(record.Run.State) {
		if err := artifact.Finalize(tx, record.Run.ID, record.Run); err != nil {
			return err
		}
		return tx.Bucket(expiryBucket).Put([]byte(timeKey(record.Run.FinishedAt)+"/run/"+record.Run.ID), []byte(record.Run.ID))
	}
	return nil
}

func saveRunIndex(tx *bolt.Tx, run Run) error {
	if err := tx.Bucket(orderBucket).Put([]byte(run.SequenceID), []byte(run.ID)); err != nil {
		return err
	}
	run.Steps, run.Output = nil, nil
	return put(tx, summariesBucket, run.ID, run)
}

func (s *Service) recoverRuns() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var recovered []storedRun
		err := tx.Bucket(runsBucket).ForEach(func(key, _ []byte) error {
			record, err := readRun(tx, string(key))
			if err != nil {
				return err
			}
			if record.Run.State == "running" || record.Run.State == "interrupted" {
				wasRunning := record.Run.State == "running"
				plan, planErr := s.planFor(record)
				if planErr == nil && plan.ResumesAutomatically() && !record.Run.CancellationRequested && record.Run.ResumeCount < MaxResumes &&
					(record.Run.StartedAt.IsZero() || time.Now().Before(plan.Deadline(record.Run.StartedAt))) &&
					countKeys(tx.Bucket(pendingBucket)) < s.options.MaxRuns+s.options.QueueSize {
					prepared, resumeErr := plan.PrepareResume(record.Run.Result, nil)
					if resumeErr == nil {
						if err := queueResumed(tx, &record, prepared, "automatic checkpoint recovery"); err != nil {
							return err
						}
						recovered = append(recovered, record)
						return nil
					}
					record.Run.Error = resumeErr.Error()
				}
				if !wasRunning {
					return nil
				}
				record.Run.State, record.Run.Error = "interrupted", "server stopped during execution; review external effects before resubmitting"
				record.Run.FinishedAt = time.Now().UTC()
				markInterrupted(record.Run.Steps)
				recovered = append(recovered, record)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, record := range recovered {
			if err := saveRun(tx, record); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Service) prepare(request Request) (storedRun, *workflow.Plan, error) {
	var plan *workflow.Plan
	var err error
	if request.Specification != nil {
		if request.WorkflowID != "" || request.Version != 0 {
			return storedRun{}, nil, ErrInvalid
		}
		def, _, err := copyDefinition(*request.Specification)
		if err != nil {
			return storedRun{}, nil, err
		}
		request.Specification = &def
		plan, err = workflow.Compile(def, s.registry, s.options.Timeout, s.resolve)
		if err != nil {
			return storedRun{}, nil, err
		}
	} else {
		if request.WorkflowID == "" || request.Version < 0 {
			return storedRun{}, nil, ErrInvalid
		}
		plan, err = s.resolve(workflow.Reference{WorkflowID: request.WorkflowID, Version: request.Version})
		if err != nil {
			return storedRun{}, nil, err
		}
	}
	if err := plan.ValidateInput(request.Input); err != nil {
		return storedRun{}, nil, err
	}
	now := time.Now().UTC()
	record := storedRun{Run: Run{Result: workflow.Result{WorkflowID: plan.ID, Version: plan.Version, State: "queued", Steps: []workflow.StepResult{}}, CreatedAt: now},
		Input: request.Input, Inline: request.Specification}
	return record, plan, nil
}

// Submit commits admission before a worker can execute an action.
func (s *Service) Submit(request Request, idempotencyKey string) (Run, bool, error) {
	if len(idempotencyKey) > 256 || strings.ContainsAny(idempotencyKey, "\r\n") {
		return Run{}, false, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	request.Async = false // Waiting is a transport choice, not part of the operation.
	key := ""
	if idempotencyKey != "" {
		key = "run/" + hash(idempotencyKey)
	}
	runs, duplicate, err := s.admit(key, hash(request), func() ([]storedRun, error) {
		record, _, err := s.prepare(request)
		if err != nil {
			return nil, err
		}
		return []storedRun{record}, nil
	})
	if err != nil {
		return Run{}, false, err
	}
	return runs[0], duplicate, nil
}

// admit holds mu and commits all fanout runs and the receipt in one transaction.
func (s *Service) admit(key, fingerprint string, prepare func() ([]storedRun, error)) ([]Run, bool, error) {
	if s.closing || s.storageFailed {
		return nil, false, ErrUnavailable
	}
	var runs []Run
	duplicate := false
	var validationErr error
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := s.prune(tx); err != nil {
			return err
		}
		if key != "" {
			if data := tx.Bucket(receiptsBucket).Get([]byte(key)); data != nil {
				var existing receipt
				if err := workflow.Decode(data, &existing); err != nil {
					return err
				}
				if existing.Hash != fingerprint {
					validationErr = ErrConflict
					return validationErr
				}
				for _, id := range existing.IDs {
					record, err := readRun(tx, id)
					if errors.Is(err, ErrNotFound) {
						// A fanout receipt can outlive an earlier-finishing run's history.
						runs = append(runs, Run{Result: workflow.Result{ID: id, State: "expired"}})
						continue
					}
					if err != nil {
						validationErr = ErrConflict
						return validationErr
					}
					runs = append(runs, record.Run)
				}
				duplicate = true
				return nil
			}
		}
		records, err := prepare()
		if err != nil {
			validationErr = err
			return err
		}
		if countKeys(tx.Bucket(pendingBucket))+len(s.active)+len(records) > s.options.MaxRuns+s.options.QueueSize {
			validationErr = workflow.ErrBusy
			return validationErr
		}
		if countKeys(tx.Bucket(runsBucket))+len(records) > s.options.MaxRecords || (key != "" && countKeys(tx.Bucket(receiptsBucket)) >= s.options.MaxRecords) {
			validationErr = ErrHistoryFull
			return validationErr
		}
		ids := make([]string, 0, len(records))
		runs = make([]Run, 0, len(records))
		for _, record := range records {
			id, err := identity.Next(tx, record.Run.CreatedAt)
			if err != nil {
				return err
			}
			record.Run.ID, record.Run.SequenceID, record.PendingKey = id, id, id
			if err := artifact.Pin(tx, "run/"+id, "", []any{record.Input, record.Inline}); err != nil {
				if errors.Is(err, artifact.ErrNotFound) || errors.Is(err, artifact.ErrInvalid) {
					validationErr = err
				}
				return err
			}
			if err := saveRun(tx, record); err != nil {
				return err
			}
			if err := tx.Bucket(pendingBucket).Put([]byte(record.PendingKey), []byte(record.Run.ID)); err != nil {
				return err
			}
			runs = append(runs, record.Run)
			ids = append(ids, record.Run.ID)
		}
		if key != "" {
			now := time.Now().UTC()
			id, err := identity.Next(tx, now)
			if err != nil {
				return err
			}
			if err := put(tx, receiptsBucket, key, receipt{ID: id, Hash: fingerprint, IDs: ids, CreatedAt: now}); err != nil {
				return err
			}
			if err := tx.Bucket(expiryBucket).Put([]byte(timeKey(now)+"/receipt/"+key), []byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && validationErr == nil {
		s.failStorage()
		return nil, false, ErrUnavailable
	}
	if err != nil {
		return nil, false, err
	}
	s.notify()
	s.signal()
	return runs, duplicate, nil
}

func (s *Service) prune(tx *bolt.Tx) error {
	cutoff := time.Now().Add(-s.options.Retention)
	cursor := tx.Bucket(expiryBucket).Cursor()
	var expired [][]byte
	// Receipts remain until their runs have also aged past the retention window.
	for key, value := cursor.First(); key != nil && string(key[:min(len(key), len(timeKey(cutoff)))]) < timeKey(cutoff); key, value = cursor.Next() {
		parts := strings.SplitN(string(key), "/", 3)
		if len(parts) != 3 {
			return errors.New("invalid expiry record")
		}
		if parts[1] == "run" {
			record, err := readRun(tx, string(value))
			if err != nil {
				return err
			}
			if err := tx.Bucket(orderBucket).Delete([]byte(record.Run.SequenceID)); err != nil {
				return err
			}
			if err := tx.Bucket(summariesBucket).Delete(value); err != nil {
				return err
			}
			if err := artifact.Unpin(tx, "run/"+string(value)); err != nil {
				return err
			}
			if err := pruneObservations(tx, string(value)); err != nil {
				return err
			}
			if err := tx.Bucket(runsBucket).Delete(value); err != nil {
				return err
			}
		} else {
			var saved receipt
			if data := tx.Bucket(receiptsBucket).Get(value); data != nil {
				if err := workflow.Decode(data, &saved); err != nil {
					return err
				}
				keep := false
				for _, id := range saved.IDs {
					record, err := readRun(tx, id)
					if err == nil && (!terminal(record.Run.State) || record.Run.FinishedAt.After(cutoff)) {
						keep = true
						break
					}
					if err != nil && !errors.Is(err, ErrNotFound) {
						return err
					}
				}
				if keep {
					continue
				}
			}
			if err := tx.Bucket(receiptsBucket).Delete(value); err != nil {
				return err
			}
		}
		expired = append(expired, append([]byte(nil), key...))
	}
	for _, key := range expired {
		if err := tx.Bucket(expiryBucket).Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Get(id string) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.get(id)
}

func (s *Service) get(id string) (Run, error) {
	var record storedRun
	err := s.db.View(func(tx *bolt.Tx) error { var err error; record, err = readRun(tx, id); return err })
	return record.Run, err
}

func (s *Service) Wait(ctx context.Context, id string) (Run, error) {
	for {
		s.mu.Lock()
		run, err := s.get(id)
		changed, stopping := s.changed, s.closing || s.storageFailed
		s.mu.Unlock()
		if err != nil || terminal(run.State) {
			return run, err
		}
		if stopping {
			return run, ErrUnavailable
		}
		select {
		case <-ctx.Done():
			return run, ctx.Err()
		case <-changed:
		}
	}
}

// Runs returns bounded metadata, not every retained result body.
func (s *Service) Runs(limit int) ([]Run, error) {
	return s.RunsBefore(limit, "")
}

func (s *Service) RunsBefore(limit int, before string) ([]Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limit = min(100, max(1, limit))
	runs := []Run{}
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(orderBucket).Cursor()
		key, id := cursor.Last()
		if before != "" {
			key, id = cursor.Seek([]byte(before))
			if key == nil {
				key, id = cursor.Last()
			} else {
				key, id = cursor.Prev()
			}
		}
		for ; key != nil && len(runs) < limit; key, id = cursor.Prev() {
			var run Run
			if err := workflow.Decode(tx.Bucket(summariesBucket).Get(id), &run); err != nil {
				return err
			}
			runs = append(runs, run)
		}
		return nil
	})
	return runs, err
}

func (s *Service) Cancel(id string) (Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.storageFailed {
		return Run{}, ErrUnavailable
	}
	var record storedRun
	err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		record, err = readRun(tx, id)
		if err != nil {
			return err
		}
		if terminal(record.Run.State) {
			return nil
		}
		record.Run.CancellationRequested = true
		if record.Run.State == "queued" {
			record.Run.State, record.Run.FinishedAt = "canceled", time.Now().UTC()
			if err := tx.Bucket(pendingBucket).Delete([]byte(record.PendingKey)); err != nil {
				return err
			}
		}
		return saveRun(tx, record)
	})
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			s.failStorage()
			return Run{}, ErrUnavailable
		}
		return Run{}, err
	}
	if cancel := s.active[id]; cancel != nil {
		cancel()
	}
	s.notify()
	return record.Run, nil
}

func (s *Service) claim() (storedRun, *workflow.Plan, context.Context, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.storageFailed {
		return storedRun{}, nil, nil, false
	}
	var record storedRun
	found := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		key, id := tx.Bucket(pendingBucket).Cursor().First()
		if key == nil {
			return nil
		}
		var err error
		record, err = readRun(tx, string(id))
		if err != nil {
			return err
		}
		record.Run.State = "running"
		if record.Run.StartedAt.IsZero() {
			record.Run.StartedAt = time.Now().UTC()
		}
		if err := saveRun(tx, record); err != nil {
			return err
		}
		found = true
		return tx.Bucket(pendingBucket).Delete(key)
	})
	if err != nil {
		s.failStorage()
		return record, nil, nil, false
	}
	if !found {
		return record, nil, nil, false
	}
	var plan *workflow.Plan
	if record.Inline != nil {
		plan, err = workflow.Compile(*record.Inline, s.registry, s.options.Timeout, s.resolve)
	} else {
		plan, err = s.resolve(workflow.Reference{WorkflowID: record.Run.WorkflowID, Version: record.Run.Version})
	}
	if err != nil {
		record.Run.State, record.Run.Error, record.Run.FinishedAt = "failed", "stored specification is no longer executable", time.Now().UTC()
		if err := s.db.Update(func(tx *bolt.Tx) error { return saveRun(tx, record) }); err != nil {
			s.failStorage()
		}
		s.notify()
		s.signal()
		return record, nil, nil, false
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.active[record.Run.ID] = cancel
	s.notify()
	return record, plan, ctx, true
}

func (s *Service) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.wake:
		}
		for {
			record, plan, ctx, ok := s.claim()
			if !ok {
				break
			}
			result, err := s.engine.RunTrackedFrom(ctx, plan, record.Input, record.Run.ID, record.Run.Result, func(result workflow.Result) error {
				s.mu.Lock()
				defer s.mu.Unlock()
				err := s.db.Update(func(tx *bolt.Tx) error {
					current, err := readRun(tx, record.Run.ID)
					if err != nil {
						return err
					}
					current.Run.Result = result
					if terminal(result.State) {
						s.adjustFinal(&current.Run)
					}
					return saveRun(tx, current)
				})
				if errors.Is(err, artifact.ErrNotFound) || errors.Is(err, artifact.ErrInvalid) {
					// Untrusted action JSON can contain a malformed file reference.
					// Reject that run without treating it as a disk failure.
					invalid := err
					err = s.db.Update(func(tx *bolt.Tx) error {
						current, err := readRun(tx, record.Run.ID)
						if err != nil {
							return err
						}
						current.Run.State, current.Run.Error, current.Run.FinishedAt = "failed", "action returned an invalid file reference", time.Now().UTC()
						for i := range current.Run.Steps {
							if current.Run.Steps[i].State == "running" {
								current.Run.Steps[i].State = "failed"
							}
							if current.Run.Steps[i].State == "pending" {
								current.Run.Steps[i].State = "skipped"
							}
						}
						return saveRun(tx, current)
					})
					if err == nil {
						s.notify()
						return invalid
					}
				}
				if err != nil {
					s.failStorage()
				}
				s.notify()
				return err
			})
			s.mu.Lock()
			if result.ID == "" {
				result = record.Run.Result
				result.State, result.Error, result.FinishedAt = "failed", "execution could not start", time.Now().UTC()
			}
			if err != nil {
				result.State, result.Error = "interrupted", "execution or storage failed; review external effects"
			}
			finalState := result.State
			writeErr := s.db.Update(func(tx *bolt.Tx) error {
				current, err := readRun(tx, record.Run.ID)
				if err != nil {
					return err
				}
				// A completed observer transaction is authoritative. Do not rewrite
				// it when cancellation or shutdown arrives after its commit.
				if terminal(current.Run.State) {
					finalState = current.Run.State
					return nil
				}
				current.Run.Result = result
				s.adjustFinal(&current.Run)
				finalState = current.Run.State
				return saveRun(tx, current)
			})
			if writeErr != nil {
				s.failStorage()
			}
			if cancel := s.active[record.Run.ID]; cancel != nil {
				cancel()
			}
			delete(s.active, record.Run.ID)
			s.notify()
			s.mu.Unlock()
			if s.options.Logger != nil {
				if writeErr != nil {
					s.options.Logger.Error("run state write failed", "run_id", record.Run.ID)
				} else {
					s.options.Logger.Info("run finished", "run_id", record.Run.ID, "workflow_id", record.Run.WorkflowID, "version", record.Run.Version, "state", finalState)
				}
			}
		}
	}
}

func (s *Service) adjustFinal(run *Run) {
	if run.FinishedAt.IsZero() {
		run.FinishedAt = time.Now().UTC()
	}
	if run.CancellationRequested && run.State == "succeeded" {
		run.State, run.Output = "canceled", nil
	}
	if s.ctx.Err() != nil && !run.CancellationRequested {
		run.State, run.Error = "interrupted", "server stopped during execution; review external effects before resubmitting"
	}
}
