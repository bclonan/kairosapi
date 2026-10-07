package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

func TestRunHistoryIsOrderedRedactedAndSurvivesRestart(t *testing.T) {
	options := testOptions(t)
	registry := workflow.Registry{"value": action.Value}
	s := openTest(t, options, registry, valueDef("observed", "secret-result-value"))
	run, _, err := s.Submit(Request{WorkflowID: "observed", Input: map[string]any{"password": "secret-input-value"}}, "secret-idempotency-value")
	if err != nil {
		t.Fatal(err)
	}
	await(t, s, run.ID)
	var events []HistoryEvent
	after := ""
	for {
		page, err := s.History(run.ID, after, 2)
		if err != nil || page.Truncated {
			t.Fatalf("page=%+v err=%v", page, err)
		}
		events = append(events, page.Events...)
		after = page.NextAfter
		if !page.HasMore {
			break
		}
	}
	if len(events) < 6 || events[0].State != "queued" || events[len(events)-1].State != "succeeded" {
		t.Fatalf("missing transitions: %+v", events)
	}
	var sawOperation bool
	previous := run.ID
	for _, event := range events {
		if !identity.Valid(event.ID) || event.ID <= previous || event.RunID != run.ID {
			t.Fatalf("invalid event ordering: %+v after %s", event, previous)
		}
		previous = event.ID
		if event.StepPath == "value" && event.OperationKey == run.ID+"/value" {
			sawOperation = true
		}
	}
	encoded, _ := json.Marshal(events)
	if !sawOperation || strings.Contains(string(encoded), "secret-") || strings.Contains(string(encoded), "password") {
		t.Fatalf("correlation or redaction failed: %s", encoded)
	}
	metrics, err := s.Metrics()
	if err != nil || metrics.Runs["succeeded"] != 1 || metrics.RetainedRuns != 1 || metrics.QueuedRuns != 0 {
		t.Fatalf("metrics=%+v err=%v", metrics, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := openTest(t, options, registry)
	page, err := restarted.History(run.ID, "", 100)
	if err != nil || len(page.Events) != len(events) || page.NextAfter != after {
		t.Fatalf("journal did not survive restart: %+v %v", page, err)
	}
}

func TestRunHistoryRollsBackAndPrunesWithState(t *testing.T) {
	s := openTest(t, testOptions(t), workflow.Registry{"value": action.Value}, valueDef("atomic_history", "ok"))
	run, _, err := s.Submit(Request{WorkflowID: "atomic_history"}, "")
	if err != nil {
		t.Fatal(err)
	}
	await(t, s, run.ID)
	before, _ := s.History(run.ID, "", 100)
	s.mu.Lock()
	rollback := errors.New("rollback")
	err = s.db.Update(func(tx *bolt.Tx) error {
		record, err := readRun(tx, run.ID)
		if err != nil {
			return err
		}
		record.Run.State = "failed"
		if err := saveRun(tx, record); err != nil {
			return err
		}
		return rollback
	})
	s.mu.Unlock()
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	after, _ := s.History(run.ID, "", 100)
	if after.State != "succeeded" || after.NextAfter != before.NextAfter || len(after.Events) != len(before.Events) {
		t.Fatal("journal or state escaped rolled-back transaction")
	}
	s.mu.Lock()
	err = s.db.Update(func(tx *bolt.Tx) error {
		record, err := readRun(tx, run.ID)
		if err != nil {
			return err
		}
		oldExpiry := timeKey(record.Run.FinishedAt) + "/run/" + run.ID
		if err := tx.Bucket(expiryBucket).Delete([]byte(oldExpiry)); err != nil {
			return err
		}
		record.Run.FinishedAt = time.Now().Add(-2 * time.Hour)
		if err := saveRun(tx, record); err != nil {
			return err
		}
		if err := s.prune(tx); err != nil {
			return err
		}
		if tx.Bucket(observationsBucket).Bucket([]byte(run.ID)) != nil {
			return errors.New("journal survived run pruning")
		}
		return nil
	})
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.History(run.ID, "", 100); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestRunHistoryCapacityGapAndValidation(t *testing.T) {
	s := openTest(t, testOptions(t), workflow.Registry{"value": action.Value}, valueDef("bounded_history", "ok"))
	run, _, err := s.Submit(Request{WorkflowID: "bounded_history"}, "")
	if err != nil {
		t.Fatal(err)
	}
	await(t, s, run.ID)
	first, _ := s.History(run.ID, "", 1)
	s.mu.Lock()
	err = s.db.Update(func(tx *bolt.Tx) error {
		for range MaxHistoryEvents + 2 {
			if err := appendObservation(tx, HistoryEvent{RunID: run.ID, WorkflowID: run.WorkflowID, Version: run.Version, Time: time.Now(), Kind: "test.transition"}); err != nil {
				return err
			}
		}
		return verifyObservability(tx)
	})
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	page, err := s.History(run.ID, first.NextAfter, 100)
	if err != nil || !page.Truncated || page.DroppedThrough < first.NextAfter || len(page.Events) != 100 || !page.HasMore {
		t.Fatalf("gap not reported: %+v %v", page, err)
	}
	count := len(page.Events)
	for page.HasMore {
		page, err = s.History(run.ID, page.NextAfter, 100)
		if err != nil || page.Truncated {
			t.Fatalf("page=%+v err=%v", page, err)
		}
		count += len(page.Events)
	}
	if count != MaxHistoryEvents {
		t.Fatalf("retained %d events", count)
	}
	if _, err := s.History(run.ID, "bad", 10); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid cursor accepted")
	}
	if _, err := s.History(run.ID, "", 101); !errors.Is(err, ErrInvalid) {
		t.Fatal("oversized page accepted")
	}
}

func TestWaitHistoryCancellationDoesNotCancelRun(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	registry := workflow.Registry{"block": func(json.RawMessage) (workflow.Handler, error) {
		return func(ctx context.Context, _ map[string]any) (any, error) {
			close(started)
			select {
			case <-release:
				return map[string]any{"ok": true}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, nil
	}}
	s := openTest(t, testOptions(t), registry, workflow.Definition{ID: "waiting", Version: 1, Steps: []workflow.Step{{ID: "block", Action: "block"}}})
	run, _, err := s.Submit(Request{WorkflowID: "waiting"}, "")
	if err != nil {
		t.Fatal(err)
	}
	<-started
	page, err := s.History(run.ID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.WaitHistory(ctx, run.ID, page.NextAfter, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait cancellation: %v", err)
	}
	active, _ := s.Get(run.ID)
	if active.State != "running" || active.CancellationRequested {
		t.Fatalf("wait canceled execution: %+v", active)
	}
	close(release)
	if completed := await(t, s, run.ID); completed.State != "succeeded" {
		t.Fatalf("run did not complete: %+v", completed)
	}
}

func TestNestedAttemptIsCommittedBeforeEffectAndPhaseIsJournaled(t *testing.T) {
	var s *Service
	var calls atomic.Int32
	registry := workflow.Registry{"retry": func(json.RawMessage) (workflow.Handler, error) {
		return func(ctx context.Context, _ map[string]any) (any, error) {
			attempt := int(calls.Add(1))
			page, err := s.History(workflow.RunID(ctx), "", 100)
			if err != nil {
				return nil, err
			}
			found := false
			for _, event := range page.Events {
				if event.Kind == "step.attempt" && event.StepPath == "child/effect" && event.Attempt == attempt && event.OperationKey == workflow.OperationKey(ctx) {
					found = true
				}
			}
			if !found {
				return nil, errors.New("effect ran before nested attempt committed")
			}
			if err := workflow.ReportProgress(ctx, "provider_pending"); err != nil {
				return nil, err
			}
			if attempt == 1 {
				return nil, &workflow.ActionError{Message: "private-provider-error", Transient: true}
			}
			return map[string]any{"ok": true}, nil
		}, nil
	}}
	child := workflow.Definition{ID: "child_observed", Version: 1, Steps: []workflow.Step{{ID: "effect", Action: "retry", Retry: workflow.Retry{Idempotent: true, MaxAttempts: 2}}}, Output: map[string]any{"result": map[string]any{"$ref": "/steps/effect"}}}
	parent := workflow.Definition{ID: "parent_observed", Version: 1, Steps: []workflow.Step{{ID: "child", Action: "workflow", Config: json.RawMessage(`{"workflow_id":"child_observed","version":1}`)}}}
	s = openTest(t, testOptions(t), registry, child, parent)
	run, _, err := s.Submit(Request{WorkflowID: parent.ID}, "")
	if err != nil {
		t.Fatal(err)
	}
	completed := await(t, s, run.ID)
	if completed.State != "succeeded" || calls.Load() != 2 {
		t.Fatalf("nested attempt failed: %+v calls=%d", completed, calls.Load())
	}
	page, err := s.History(run.ID, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	phase := false
	for _, event := range page.Events {
		if event.Kind == "step.phase" && event.StepPath == "child/effect" && event.Phase == "provider_pending" {
			phase = true
		}
	}
	encoded, _ := json.Marshal(page)
	if !phase || strings.Contains(string(encoded), "private-provider-error") {
		t.Fatalf("phase or redaction failed: %s", encoded)
	}
}
