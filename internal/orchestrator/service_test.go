package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

func testOptions(t *testing.T) Options {
	return Options{Path: filepath.Join(t.TempDir(), "kairos.db"), Workers: 1, MaxRuns: 1, QueueSize: 2, MaxRecords: 20, Retention: time.Minute, Timeout: 5 * time.Second}
}

func openTest(t *testing.T, options Options, registry workflow.Registry, seeds ...workflow.Definition) *Service {
	t.Helper()
	s, err := Open(options, registry, seeds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s.Ready() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	return s
}

func valueDef(id, message string) workflow.Definition {
	return workflow.Definition{ID: id, Version: 1, Steps: []workflow.Step{{ID: "value", Action: "value", Input: map[string]any{"message": message}}}, Output: map[string]any{"message": map[string]any{"$ref": "/steps/value/message"}}}
}

func await(t *testing.T, s *Service, id string) Run {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	run, err := s.Wait(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestPublishReuseVersionPinningAndPersistence(t *testing.T) {
	options := testOptions(t)
	registry := workflow.Registry{"value": action.Value}
	s := openTest(t, options, registry)
	child := valueDef("child", "one")
	if created, err := s.Publish(child); err != nil || !created {
		t.Fatalf("%v %v", created, err)
	}
	if created, err := s.Publish(child); err != nil || created {
		t.Fatalf("duplicate %v %v", created, err)
	}
	changed := valueDef("child", "different")
	if _, err := s.Publish(changed); !errors.Is(err, ErrConflict) {
		t.Fatalf("%v", err)
	}
	parent := workflow.Definition{ID: "parent", Version: 1, Steps: []workflow.Step{{ID: "reuse", Action: "workflow", Config: json.RawMessage(`{"workflow_id":"child","version":1}`)}}, Output: map[string]any{"message": map[string]any{"$ref": "/steps/reuse/message"}}}
	if _, err := s.Publish(parent); err != nil {
		t.Fatal(err)
	}
	changed.Version = 2
	if _, err := s.Publish(changed); err != nil {
		t.Fatal(err)
	}
	request := Request{WorkflowID: "parent"}
	run, duplicate, err := s.Submit(request, "request-1")
	if err != nil || duplicate {
		t.Fatalf("%v %v", duplicate, err)
	}
	completed := await(t, s, run.ID)
	if completed.State != "succeeded" || completed.Output.(map[string]any)["message"] != "one" {
		t.Fatalf("%+v", completed)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := openTest(t, options, registry)
	replayed, duplicate, err := restarted.Submit(request, "request-1")
	if err != nil || !duplicate || replayed.ID != run.ID || replayed.State != "succeeded" {
		t.Fatalf("%+v %v %v", replayed, duplicate, err)
	}
	if len(restarted.List()) != 3 {
		t.Fatal("catalog did not survive restart")
	}
	if _, _, err := restarted.Submit(Request{WorkflowID: "child"}, "request-1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("%v", err)
	}
}

func TestEventFanoutDedupAndLatestVersion(t *testing.T) {
	s := openTest(t, testOptions(t), workflow.Registry{"value": action.Value})
	for _, id := range []string{"first", "second"} {
		def := valueDef(id, "v1")
		def.Triggers = []workflow.Trigger{{Type: "orders.*", Source: "/shop", Match: map[string]any{"/data/paid": true}}}
		def.Steps[0].Input = map[string]any{"message": map[string]any{"$ref": "/data/id", "default": "wrong-root"}}
		// Inputs live under /input. Invalid references fail at publication.
		if _, err := s.Publish(def); err == nil {
			t.Fatal("invalid input reference accepted")
		}
		def.Steps[0].Input = map[string]any{"message": map[string]any{"$ref": "/input/data/id"}}
		if _, err := s.Publish(def); err != nil {
			t.Fatal(err)
		}
		if id == "first" {
			def.Version = 2
			if _, err := s.Publish(def); err != nil {
				t.Fatal(err)
			}
		}
	}
	event := map[string]any{"specversion": "1.0", "id": "event-1", "source": "/shop", "type": "orders.paid", "data": map[string]any{"id": "order-7", "paid": true}}
	delivery, err := s.Emit(event)
	if err != nil || len(delivery.Runs) != 2 || delivery.Duplicate {
		t.Fatalf("%+v %v", delivery, err)
	}
	for _, run := range delivery.Runs {
		completed := await(t, s, run.ID)
		if completed.State != "succeeded" || completed.Output.(map[string]any)["message"] != "order-7" {
			t.Fatalf("%+v", completed)
		}
		if run.WorkflowID == "first" && run.Version != 2 {
			t.Fatal("did not route to latest version")
		}
	}
	again, err := s.Emit(event)
	if err != nil || !again.Duplicate || again.Runs[0].ID != delivery.Runs[0].ID {
		t.Fatalf("%+v %v", again, err)
	}
	event["data"] = map[string]any{"id": "changed", "paid": true}
	if _, err := s.Emit(event); !errors.Is(err, ErrConflict) {
		t.Fatalf("%v", err)
	}
	event["id"] = "event-2"
	event["type"] = "unmatched"
	empty, err := s.Emit(event)
	if err != nil || len(empty.Runs) != 0 {
		t.Fatalf("%+v %v", empty, err)
	}
}

func TestAtomicEventAdmissionAndSchemaValidation(t *testing.T) {
	options := testOptions(t)
	options.QueueSize = 0
	s := openTest(t, options, workflow.Registry{"value": action.Value})
	for _, id := range []string{"a", "b"} {
		def := valueDef(id, "ok")
		def.Triggers = []workflow.Trigger{{Type: "created"}}
		if _, err := s.Publish(def); err != nil {
			t.Fatal(err)
		}
	}
	event := map[string]any{"specversion": "1.0", "id": "one", "source": "/source", "type": "created"}
	if _, err := s.Emit(event); !errors.Is(err, workflow.ErrBusy) {
		t.Fatalf("%v", err)
	}
	runs, _ := s.Runs(100)
	if len(runs) != 0 {
		t.Fatal("partial event fanout was accepted")
	}
	def := valueDef("schema", "ok")
	def.InputSchema = json.RawMessage(`{"type":"object","required":["name"]}`)
	if _, err := s.Publish(def); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit(Request{WorkflowID: "schema"}, ""); !errors.Is(err, workflow.ErrInput) {
		t.Fatalf("%v", err)
	}
	if runs, _ := s.Runs(100); len(runs) != 0 {
		t.Fatal("invalid input entered queue")
	}
}

func TestCancellationCapacityAndRestartQueue(t *testing.T) {
	started := make(chan string, 4)
	var calls atomic.Int32
	registry := workflow.Registry{"wait": func(json.RawMessage) (workflow.Handler, error) {
		return func(ctx context.Context, input map[string]any) (any, error) {
			calls.Add(1)
			started <- input["label"].(string)
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil
	}, "value": action.Value}
	def := workflow.Definition{ID: "wait", Version: 1, Steps: []workflow.Step{{ID: "wait", Action: "wait", Input: map[string]any{"label": map[string]any{"$ref": "/input/label"}}}}}
	options := testOptions(t)
	s := openTest(t, options, registry, def, valueDef("quick", "finished"))
	running, _, err := s.Submit(Request{WorkflowID: "wait", Input: map[string]any{"label": "first"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("did not start")
	}
	queued, _, err := s.Submit(Request{WorkflowID: "wait", Input: map[string]any{"label": "never"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	third, _, err := s.Submit(Request{WorkflowID: "quick"}, "persistent-queue")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit(Request{WorkflowID: "quick"}, ""); !errors.Is(err, workflow.ErrBusy) {
		t.Fatalf("%v", err)
	}
	if run, err := s.Cancel(queued.ID); err != nil || run.State != "canceled" {
		t.Fatalf("%+v %v", run, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := openTest(t, options, registry)
	old, err := restarted.Get(running.ID)
	if err != nil || old.State != "interrupted" {
		t.Fatalf("%+v %v", old, err)
	}
	if resumed := await(t, restarted, third.ID); resumed.State != "succeeded" {
		t.Fatalf("%+v", resumed)
	}
	if calls.Load() != 1 {
		t.Fatalf("replayed uncertain or canceled work, calls=%d", calls.Load())
	}
	active, _, err := restarted.Submit(Request{WorkflowID: "wait", Input: map[string]any{"label": "cancel"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("did not start")
	}
	if _, err := restarted.Cancel(active.ID); err != nil {
		t.Fatal(err)
	}
	if canceled := await(t, restarted, active.ID); canceled.State != "canceled" || !canceled.CancellationRequested {
		t.Fatalf("%+v", canceled)
	}
}

func TestCrashRecordRecoveryDoesNotReplay(t *testing.T) {
	options := testOptions(t)
	registry := workflow.Registry{"value": action.Value}
	s := openTest(t, options, registry, valueDef("saved", "ok"))
	run, _, err := s.Submit(Request{WorkflowID: "saved"}, "")
	if err != nil {
		t.Fatal(err)
	}
	await(t, s, run.ID)
	// Inject the persisted state left by a process dying after action admission.
	// Wait for the worker to release this completed run before altering its row.
	for {
		s.mu.Lock()
		active := s.active[run.ID] != nil
		changed := s.changed
		s.mu.Unlock()
		if !active {
			break
		}
		select {
		case <-changed:
		case <-time.After(3 * time.Second):
			t.Fatal("worker did not release completed run")
		}
	}
	s.mu.Lock()
	err = s.db.Update(func(tx *bolt.Tx) error {
		record, err := readRun(tx, run.ID)
		if err != nil {
			return err
		}
		record.Run.State = "running"
		record.Run.Steps[0].State = "running"
		if err := tx.Bucket(expiryBucket).Delete([]byte(timeKey(record.Run.FinishedAt) + "/run/" + run.ID)); err != nil {
			return err
		}
		record.Run.FinishedAt = time.Time{}
		record.Run.Output = nil
		record.Run.Steps[0].Output = nil
		record.Run.Steps[0].FinishedAt = time.Time{}
		return saveRun(tx, record)
	})
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := openTest(t, options, registry)
	recovered, err := restarted.Get(run.ID)
	if err != nil || recovered.State != "interrupted" || recovered.Steps[0].State != "interrupted" {
		t.Fatalf("%+v %v", recovered, err)
	}
}

func TestConcurrentIdempotencyAndRetention(t *testing.T) {
	options := testOptions(t)
	options.QueueSize = 1
	options.MaxRecords = 2
	s := openTest(t, options, workflow.Registry{"value": action.Value}, valueDef("work", "ok"))
	var wg sync.WaitGroup
	ids := make(chan string, 10)
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, _, err := s.Submit(Request{WorkflowID: "work"}, "same")
			if err != nil {
				t.Error(err)
				return
			}
			ids <- run.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if first != id {
			t.Fatal("duplicate execution admitted")
		}
	}
	await(t, s, first)
	second, _, err := s.Submit(Request{WorkflowID: "work"}, "")
	if err != nil {
		t.Fatal(err)
	}
	await(t, s, second.ID)
	if _, _, err := s.Submit(Request{WorkflowID: "work"}, ""); !errors.Is(err, ErrHistoryFull) {
		t.Fatalf("%v", err)
	}
	s.mu.Lock()
	err = s.db.Update(func(tx *bolt.Tx) error {
		record, err := readRun(tx, first)
		if err != nil {
			return err
		}
		record.Run.FinishedAt = time.Now().Add(-2 * time.Minute)
		return saveRun(tx, record)
	})
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit(Request{WorkflowID: "work"}, ""); err != nil {
		t.Fatal("retention did not release capacity", err)
	}
}

func TestFanoutReceiptOutlivesEarlyResultWithoutReexecution(t *testing.T) {
	s := openTest(t, testOptions(t), workflow.Registry{"value": action.Value})
	for _, id := range []string{"a", "b"} {
		def := valueDef(id, "ok")
		def.Triggers = []workflow.Trigger{{Type: "number", Match: map[string]any{"/data/count": 1}}}
		if _, err := s.Publish(def); err != nil {
			t.Fatal(err)
		}
	}
	event := map[string]any{"specversion": "1.0", "id": "retained", "source": "/test", "type": "number", "data": map[string]any{"count": 1}}
	delivery, err := s.Emit(event)
	if err != nil || len(delivery.Runs) != 2 {
		t.Fatalf("%+v %v", delivery, err)
	}
	for _, run := range delivery.Runs {
		await(t, s, run.ID)
	}
	firstID := delivery.Runs[0].ID
	s.mu.Lock()
	err = s.db.Update(func(tx *bolt.Tx) error {
		record, err := readRun(tx, firstID)
		if err != nil {
			return err
		}
		record.Run.FinishedAt = time.Now().Add(-2 * time.Minute)
		return saveRun(tx, record)
	})
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.Emit(event)
	if err != nil || !replayed.Duplicate || replayed.Runs[0].ID != firstID || replayed.Runs[0].State != "expired" {
		t.Fatalf("%+v %v", replayed, err)
	}
}

func TestStorageFailureStopsFollowingEffectsAndReadiness(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	registry := workflow.Registry{"test": func(json.RawMessage) (workflow.Handler, error) {
		return func(context.Context, map[string]any) (any, error) {
			if calls.Add(1) == 1 {
				close(started)
				<-release
			}
			return "ok", nil
		}, nil
	}}
	def := workflow.Definition{ID: "failure", Version: 1, Steps: []workflow.Step{{ID: "first", Action: "test"}, {ID: "next", Action: "test", DependsOn: []string{"first"}}}}
	options := testOptions(t)
	s := openTest(t, options, registry, def)
	run, _, err := s.Submit(Request{WorkflowID: "failure"}, "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("action did not start")
	}
	s.mu.Lock()
	err = s.db.Close()
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	exited := make(chan struct{})
	go func() { s.wg.Wait(); close(exited) }()
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
		t.Fatal("storage failure did not stop workers")
	}
	if s.Ready() || calls.Load() != 1 {
		t.Fatalf("ready=%v calls=%d", s.Ready(), calls.Load())
	}
	restarted := openTest(t, options, registry)
	recovered, err := restarted.Get(run.ID)
	if err != nil || recovered.State != "interrupted" {
		t.Fatalf("%+v %v", recovered, err)
	}
}
