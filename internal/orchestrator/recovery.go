package orchestrator

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

const MaxResumes = 32

type ResumeRequest struct {
	Reason      string                         `json:"reason,omitempty"`
	Resolutions map[string]workflow.Resolution `json:"resolutions,omitempty"`
}

func (s *Service) planFor(record storedRun) (*workflow.Plan, error) {
	if record.Inline != nil {
		return workflow.Compile(*record.Inline, s.registry, s.options.Timeout, s.resolve)
	}
	return s.resolve(workflow.Reference{WorkflowID: record.Run.WorkflowID, Version: record.Run.Version})
}

func markInterrupted(steps []workflow.StepResult) {
	for i := range steps {
		if steps[i].State == "running" {
			steps[i].State = "interrupted"
		}
		if steps[i].State == "pending" {
			steps[i].State, steps[i].SkipReason = "skipped", "blocked"
		}
		markInterrupted(steps[i].Children)
	}
}

func queueResumed(tx *bolt.Tx, record *storedRun, prepared workflow.Result, reason string) error {
	if !record.Run.FinishedAt.IsZero() {
		if err := tx.Bucket(expiryBucket).Delete([]byte(timeKey(record.Run.FinishedAt) + "/run/" + record.Run.ID)); err != nil {
			return err
		}
	}
	record.Run.Result = prepared
	record.Run.ResumeCount++
	record.Run.ResumeReason = reason
	record.Run.CancellationRequested = false
	record.PendingKey = record.Run.SequenceID
	return tx.Bucket(pendingBucket).Put([]byte(record.PendingKey), []byte(record.Run.ID))
}

// Resume preserves identity and completed effects. Any uncertain unsafe leaf
// needs an explicit operator decision, committed with its new queue entry.
func (s *Service) Resume(id string, request ResumeRequest) (Run, error) {
	if len(request.Reason) > 1000 || strings.ContainsAny(request.Reason, "\r\n\x00") || len(request.Resolutions) > workflow.MaxExpandedSteps ||
		(len(request.Resolutions) > 0 && strings.TrimSpace(request.Reason) == "") {
		return Run{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.storageFailed {
		return Run{}, ErrUnavailable
	}
	if s.active[id] != nil {
		return Run{}, fmt.Errorf("%w: run worker has not stopped", ErrConflict)
	}
	var record storedRun
	var validationErr error
	err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		record, err = readRun(tx, id)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				validationErr = err
			}
			return err
		}
		if !terminal(record.Run.State) || record.Run.State == "succeeded" || record.Run.ResumeCount >= MaxResumes {
			validationErr = fmt.Errorf("%w: only stopped unfinished runs can resume, at most %d times", ErrConflict, MaxResumes)
			return validationErr
		}
		if countKeys(tx.Bucket(pendingBucket))+len(s.active) >= s.options.MaxRuns+s.options.QueueSize {
			validationErr = workflow.ErrBusy
			return validationErr
		}
		plan, err := s.planFor(record)
		if err != nil {
			validationErr = err
			return err
		}
		if !record.Run.StartedAt.IsZero() && !time.Now().Before(plan.Deadline(record.Run.StartedAt)) {
			validationErr = fmt.Errorf("%w: original run deadline has expired", ErrConflict)
			return validationErr
		}
		prepared, err := plan.PrepareResume(record.Run.Result, request.Resolutions)
		if err != nil {
			validationErr = fmt.Errorf("%w: %s", ErrConflict, err)
			return validationErr
		}
		reason := request.Reason
		if reason == "" {
			reason = "operator requested safe checkpoint resume"
		}
		if err := queueResumed(tx, &record, prepared, reason); err != nil {
			return err
		}
		if err := saveRun(tx, record); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		if validationErr == nil {
			s.failStorage()
			return Run{}, ErrUnavailable
		}
		return Run{}, validationErr
	}
	s.notify()
	s.signal()
	return record.Run, nil
}
