package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

type crashReceipt struct{ RunID, QueuedID, FileID, DiscardID string }

func TestKilledProcessRecoveryAndFullBackupRestore(t *testing.T) {
	if path := os.Getenv("KAIROS_TEST_CRASH_DB"); path != "" {
		runCrashChild(t, path)
		return
	}
	options := testOptions(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestKilledProcessRecoveryAndFullBackupRestore$", "-test.count=1")
	command.Env = append(os.Environ(), "KAIROS_TEST_CRASH_DB="+options.Path)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if command.ProcessState == nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	line, err := bufio.NewReader(stdout).ReadBytes('\n')
	if err != nil {
		t.Fatalf("child: %v %s", err, stderr.String())
	}
	var receipt crashReceipt
	if err := json.Unmarshal(line, &receipt); err != nil {
		t.Fatalf("%v %s", err, line)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("child exited normally instead of being killed")
	}
	var effects atomic.Int32
	restoredRegistry := workflow.Registry{"value": action.Value, "blob": func(json.RawMessage) (workflow.Handler, error) {
		return func(context.Context, map[string]any) (any, error) { effects.Add(1); return nil, nil }, nil
	}, "wait": func(json.RawMessage) (workflow.Handler, error) {
		return func(context.Context, map[string]any) (any, error) { effects.Add(1); return nil, nil }, nil
	}}
	s := openTest(t, options, restoredRegistry)
	run, err := s.Get(receipt.RunID)
	if err != nil || run.State != "interrupted" || run.Steps[0].State != "succeeded" || run.Steps[1].State != "interrupted" {
		t.Fatalf("%+v %v", run, err)
	}
	if queued := await(t, s, receipt.QueuedID); queued.State != "succeeded" {
		t.Fatal(queued)
	}
	if effects.Load() != 0 {
		t.Fatal("uncertain action replayed")
	}
	meta, reader, err := s.Files().Read(context.Background(), receipt.FileID, "")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(reader)
	reader.Close()
	if string(data) != "completed step bytes" {
		t.Fatal("completed file lost")
	}
	if _, err := s.Files().Get(receipt.DiscardID); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatal("unreferenced file survived crash recovery")
	}
	if err := s.Files().Delete(meta.ID); !errors.Is(err, artifact.ErrReferenced) {
		t.Fatal("recovered result not pinned")
	}
	newRun, _, err := s.Submit(Request{WorkflowID: "queued"}, "after-crash")
	if err != nil || newRun.ID <= receipt.FileID || !identity.Valid(newRun.ID) {
		t.Fatalf("sequence %s %v", newRun.ID, err)
	}
	await(t, s, newRun.ID)
	backup, _, _, err := s.Files().Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := io.ReadAll(backup)
	backup.Close()
	if err != nil {
		t.Fatal(err)
	}
	restoredOptions := options
	restoredOptions.Path = filepath.Join(t.TempDir(), "restored.db")
	if err := os.WriteFile(restoredOptions.Path, snapshot, 0600); err != nil {
		t.Fatal(err)
	}
	restored := openTest(t, restoredOptions, restoredRegistry)
	replayed, duplicate, err := restored.Submit(Request{WorkflowID: "queued"}, "after-crash")
	if err != nil || !duplicate || replayed.ID != newRun.ID || replayed.State != "succeeded" {
		t.Fatal("backup lost admission receipt or result")
	}
	if _, err := restored.Files().Get(receipt.FileID); err != nil {
		t.Fatal(err)
	}
	if effects.Load() != 0 {
		t.Fatal("backup restore replayed effects")
	}
}

func runCrashChild(t *testing.T, path string) {
	var s *Service
	var kept, discarded artifact.Metadata
	ready := make(chan struct{})
	registry := workflow.Registry{"value": action.Value, "blob": func(json.RawMessage) (workflow.Handler, error) {
		return func(ctx context.Context, _ map[string]any) (any, error) {
			var err error
			kept, _, err = s.Files().Put(ctx, bytes.NewBufferString("completed step bytes"), artifact.Metadata{}, "", "", workflow.RunID(ctx))
			if err != nil {
				return nil, err
			}
			discarded, _, err = s.Files().Put(ctx, bytes.NewBufferString("unreferenced bytes"), artifact.Metadata{}, "", "", workflow.RunID(ctx))
			return kept.Reference(), err
		}, nil
	}, "wait": func(json.RawMessage) (workflow.Handler, error) {
		return func(ctx context.Context, _ map[string]any) (any, error) {
			close(ready)
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil
	}}
	def := workflow.Definition{ID: "crash", Version: 1, Steps: []workflow.Step{{ID: "produce", Action: "blob"}, {ID: "wait", Action: "wait", DependsOn: []string{"produce"}}}}
	var err error
	s, err = Open(Options{Path: path, Workers: 1, MaxRuns: 1, QueueSize: 2, MaxRecords: 20, Retention: time.Minute, Timeout: time.Minute}, registry, []workflow.Definition{def, valueDef("queued", "survived")})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := s.Submit(Request{WorkflowID: "crash"}, "crash-run")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("action did not start")
	}
	queued, _, err := s.Submit(Request{WorkflowID: "queued"}, "queued-before-crash")
	if err != nil {
		t.Fatal(err)
	}
	line, _ := json.Marshal(crashReceipt{run.ID, queued.ID, kept.ID, discarded.ID})
	fmt.Println(string(line))
	<-context.Background().Done()
}

func TestFormatOneMigrationPreservesRunIDsAndCursorOrder(t *testing.T) {
	options := testOptions(t)
	db, err := bolt.Open(options.Path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ids := []string{"legacy-earlier-32-hex-id", "legacy-later-32-hex-id"}
	originals := map[string][]byte{}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{[]byte("meta"), definitionsBucket, runsBucket, pendingBucket, receiptsBucket, expiryBucket} {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		if err := tx.Bucket([]byte("meta")).Put([]byte("format"), []byte("1")); err != nil {
			return err
		}
		if err := put(tx, definitionsBucket, "old/1", valueDef("old", "value")); err != nil {
			return err
		}
		for i, id := range ids {
			created := now.Add(time.Duration(i) * time.Millisecond)
			record := storedRun{Run: Run{Result: workflow.Result{ID: id, WorkflowID: "old", Version: 1, State: "succeeded", FinishedAt: created}, CreatedAt: created}}
			if err := put(tx, runsBucket, id, record); err != nil {
				return err
			}
			originals[id] = append([]byte(nil), tx.Bucket(runsBucket).Get([]byte(id))...)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	s := openTest(t, options, workflow.Registry{"value": action.Value})
	if err := s.db.View(func(tx *bolt.Tx) error {
		for id, data := range originals {
			if !bytes.Equal(tx.Bucket(runsBucket).Get([]byte(id)), data) {
				t.Fatal("migration rewrote completed result body")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	runs, err := s.Runs(1)
	if err != nil || len(runs) != 1 || runs[0].ID != ids[1] || !identity.Valid(runs[0].SequenceID) {
		t.Fatalf("%+v %v", runs, err)
	}
	next, err := s.RunsBefore(1, runs[0].SequenceID)
	if err != nil || len(next) != 1 || next[0].ID != ids[0] {
		t.Fatalf("%+v %v", next, err)
	}
	for _, id := range ids {
		if run, err := s.Get(id); err != nil || run.State != "succeeded" {
			t.Fatal("legacy lookup changed")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTest(t, options, workflow.Registry{"value": action.Value})
	after, _ := reopened.Runs(1)
	if after[0].SequenceID != runs[0].SequenceID {
		t.Fatal("migration repeated")
	}
}

func TestStartupRejectsCorruptedFileOrMissingRunIndex(t *testing.T) {
	for _, problem := range []string{"file_bytes", "run_index", "file_index", "ulid_sequence"} {
		t.Run(problem, func(t *testing.T) {
			options := testOptions(t)
			s := openTest(t, options, workflow.Registry{"value": action.Value}, valueDef("ok", "yes"))
			meta, _, err := s.Files().Put(context.Background(), bytes.NewBufferString("original"), artifact.Metadata{}, "", "", "")
			if err != nil {
				t.Fatal(err)
			}
			run, _, err := s.Submit(Request{WorkflowID: "ok"}, "")
			if err != nil {
				t.Fatal(err)
			}
			await(t, s, run.ID)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := bolt.Open(options.Path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = db.Update(func(tx *bolt.Tx) error {
				switch problem {
				case "file_bytes":
					bucket := tx.Bucket([]byte("file_chunks")).Bucket([]byte(meta.ID))
					key, _ := bucket.Cursor().First()
					return bucket.Put(key, []byte("tampered"))
				case "run_index":
					return tx.Bucket(orderBucket).Delete([]byte(run.SequenceID))
				case "file_index":
					return tx.DeleteBucket([]byte("file_receipts"))
				default:
					return tx.Bucket([]byte("meta")).Delete([]byte("last_ulid"))
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			broken, err := Open(options, workflow.Registry{"value": action.Value}, nil)
			if err == nil {
				broken.Close()
				t.Fatal("corrupt database started workers")
			}
		})
	}
}

func TestInvalidActionFileReferenceDoesNotStopService(t *testing.T) {
	registry := workflow.Registry{"bad": func(json.RawMessage) (workflow.Handler, error) {
		return func(context.Context, map[string]any) (any, error) {
			return map[string]any{"$artifact": identity.New()}, nil
		}, nil
	}, "value": action.Value}
	def := workflow.Definition{ID: "bad", Version: 1, Steps: []workflow.Step{{ID: "bad", Action: "bad"}}}
	s := openTest(t, testOptions(t), registry, def, valueDef("healthy", "works"))
	run, _, err := s.Submit(Request{WorkflowID: "bad"}, "")
	if err != nil {
		t.Fatal(err)
	}
	finished := await(t, s, run.ID)
	if finished.State != "failed" || !s.Ready() {
		t.Fatalf("%+v ready=%v", finished, s.Ready())
	}
	next, _, err := s.Submit(Request{WorkflowID: "healthy"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if await(t, s, next.ID).State != "succeeded" {
		t.Fatal("service failed after bad reference")
	}
}

func TestFileFanoutRollbackAndRetentionUnpin(t *testing.T) {
	options := testOptions(t)
	s := openTest(t, options, workflow.Registry{"value": action.Value})
	meta, _, err := s.Files().Put(context.Background(), bytes.NewBufferString("input"), artifact.Metadata{}, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	def := valueDef("use", "ok")
	def.Triggers = []workflow.Trigger{{Type: "file.created"}}
	if _, err := s.Publish(def); err != nil {
		t.Fatal(err)
	}
	event := map[string]any{"specversion": "1.0", "source": "/files", "type": "file.created", "id": "one", "data": meta.Reference()}
	delivery, err := s.Emit(event)
	if err != nil {
		t.Fatal(err)
	}
	if !identity.Valid(delivery.ReceiptID) {
		t.Fatal("event receipt lacks ULID")
	}
	run := await(t, s, delivery.Runs[0].ID)
	if err := s.Files().Delete(meta.ID); err != artifact.ErrReferenced {
		t.Fatal(err)
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		record, err := readRun(tx, run.ID)
		if err != nil {
			return err
		}
		if err := tx.Bucket(expiryBucket).Delete([]byte(timeKey(record.Run.FinishedAt) + "/run/" + run.ID)); err != nil {
			return err
		}
		record.Run.FinishedAt = time.Now().Add(-2 * time.Minute)
		return saveRun(tx, record)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Submit(Request{WorkflowID: "use"}, "prune"); err != nil {
		t.Fatal(err)
	}
	if err := s.Files().Delete(meta.ID); err != nil {
		t.Fatal("retention did not unpin input", err)
	}
	bad := map[string]any{"specversion": "1.0", "source": "/files", "type": "file.created", "id": "bad", "data": map[string]any{"$artifact": identity.New()}}
	before, _ := s.Runs(100)
	if _, err := s.Emit(bad); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatal(err)
	}
	after, _ := s.Runs(100)
	if len(after) != len(before) || !s.Ready() {
		t.Fatal("invalid file event partially admitted")
	}
}
