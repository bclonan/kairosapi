package client_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/client"
	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/api"
	"github.com/bclonan/kairosapi/internal/orchestrator"
	"github.com/bclonan/kairosapi/internal/workflow"
)

const runnerToken = "test-runner-token-with-at-least-32-bytes"
const adminToken = "test-admin-token-with-at-least-32-bytes"

func clients(t *testing.T) (*client.Client, *client.Client) {
	t.Helper()
	service, err := orchestrator.Open(orchestrator.Options{Path: filepath.Join(t.TempDir(), "client.db"), Workers: 2, MaxRuns: 2, QueueSize: 2, MaxRecords: 20, Retention: time.Hour, Timeout: 5 * time.Second}, workflow.Registry{"value": action.Value, "delay": action.Delay}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	handler, err := api.New(service, runnerToken, adminToken, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	runner, err := client.New(server.URL, runnerToken, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	admin, err := client.New(server.URL, adminToken, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	runner.PollInterval, admin.PollInterval = time.Millisecond, time.Millisecond
	return runner, admin
}

func TestClientPublishesAdmitsDeduplicatesWaitsAndReadsHistory(t *testing.T) {
	runner, admin := clients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	spec := json.RawMessage(`{"id":"greeting","version":1,"steps":[{"id":"greet","action":"value","input":{"message":{"$concat":["Hello ",{"$ref":"/input/name"}]}}}],"output":{"message":{"$ref":"/steps/greet/message"}}}`)
	publication, err := admin.Publish(ctx, spec)
	if err != nil || !publication.Created {
		t.Fatalf("publish=%+v err=%v", publication, err)
	}
	request := client.Request{WorkflowID: "greeting", Version: 1, Input: map[string]any{"name": "Ada"}}
	run, duplicate, err := runner.Submit(ctx, request, "greeting-request-1")
	if err != nil || duplicate || run.ID == "" {
		t.Fatalf("submit=%+v duplicate=%v err=%v", run, duplicate, err)
	}
	completed, err := runner.Wait(ctx, run.ID)
	if err != nil || completed.State != "succeeded" || !bytes.Contains(completed.Output, []byte("Hello Ada")) {
		t.Fatalf("wait=%+v err=%v", completed, err)
	}
	replayed, duplicate, err := runner.Submit(ctx, request, "greeting-request-1")
	if err != nil || !duplicate || replayed.ID != run.ID {
		t.Fatalf("duplicate=%+v %v %v", replayed, duplicate, err)
	}
	page, err := runner.History(ctx, run.ID, "", 100)
	if err != nil || len(page.Events) < 6 || page.State != "succeeded" || page.Truncated {
		t.Fatalf("history=%+v err=%v", page, err)
	}
	metrics, err := runner.Metrics(ctx)
	if err != nil || metrics["retained_runs"] != json.Number("1") {
		t.Fatalf("metrics=%+v err=%v", metrics, err)
	}
	if _, err := runner.Publish(ctx, spec); err == nil {
		t.Fatal("runner published a definition")
	} else {
		var apiError *client.Error
		if !errors.As(err, &apiError) || apiError.StatusCode != 403 {
			t.Fatalf("expected typed 403, got %v", err)
		}
	}
}

func TestClientWaitCancellationIsLocalAndCancelIsExplicit(t *testing.T) {
	runner, admin := clients(t)
	ctx := context.Background()
	if _, err := admin.Publish(ctx, json.RawMessage(`{"id":"slow","version":1,"steps":[{"id":"wait","action":"delay","config":{"milliseconds":1000}}]}`)); err != nil {
		t.Fatal(err)
	}
	run, _, err := runner.Submit(ctx, client.Request{WorkflowID: "slow"}, "slow-one")
	if err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := runner.Wait(short, run.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait: %v", err)
	}
	active, err := runner.Get(ctx, run.ID)
	if err != nil || active.Terminal() || active.CancellationRequested {
		t.Fatalf("local wait stopped remote run: %+v %v", active, err)
	}
	if _, err := runner.Cancel(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	finished, err := runner.Wait(ctx, run.ID)
	if err != nil || finished.State != "canceled" {
		t.Fatalf("cancel result=%+v err=%v", finished, err)
	}
	replayed, duplicate, err := runner.Submit(ctx, client.Request{WorkflowID: "slow"}, "slow-one")
	if err != nil || !duplicate || replayed.State != "canceled" {
		t.Fatalf("terminal duplicate lost result: %+v %v %v", replayed, duplicate, err)
	}
}

func TestClientBinaryUploadDownloadAndCorruptionRejection(t *testing.T) {
	runner, _ := clients(t)
	ctx := context.Background()
	body := append([]byte{0, 255, 128, 13, 10}, bytes.Repeat([]byte("opaque"), 20000)...)
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	upload, err := runner.Upload(ctx, "opaque.bin", "application/octet-stream", bytes.NewReader(body), digest, "file-one")
	if err != nil || upload.Duplicate || upload.File.Size != int64(len(body)) {
		t.Fatalf("upload=%+v err=%v", upload, err)
	}
	again, err := runner.Upload(ctx, "opaque.bin", "application/octet-stream", bytes.NewReader(body), digest, "file-one")
	if err != nil || !again.Duplicate || again.File.ID != upload.File.ID {
		t.Fatalf("repeat=%+v err=%v", again, err)
	}
	var output bytes.Buffer
	metadata, err := runner.Download(ctx, upload.File.ID, &output)
	if err != nil || metadata.SHA256 != digest || !bytes.Equal(body, output.Bytes()) {
		t.Fatalf("download=%+v err=%v bytes=%d", metadata, err, output.Len())
	}
	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/content") {
			w.Header().Set("X-Content-SHA256", digest)
			_, _ = w.Write(bytes.Repeat([]byte{0}, len(body)))
			return
		}
		_ = json.NewEncoder(w).Encode(upload.File)
	}))
	defer badServer.Close()
	badClient, _ := client.New(badServer.URL, runnerToken, badServer.Client())
	output.Reset()
	if _, err := badClient.Download(ctx, upload.File.ID, &output); err == nil || output.Len() != 0 {
		t.Fatal("corrupt download reached destination")
	}
}

func TestClientRejectsRedirectAndDoesNotLeakBearer(t *testing.T) {
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	c, err := client.New(redirect.URL, runnerToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(context.Background(), "run"); err == nil || calls.Load() != 0 {
		t.Fatalf("followed redirect: calls=%d err=%v", calls.Load(), err)
	}
	for _, endpoint := range []string{"file:///secret", "https://user:password@example.com", "https://example.com?token=secret"} {
		if _, err := client.New(endpoint, runnerToken, nil); err == nil {
			t.Fatalf("accepted %s", endpoint)
		}
	}
}
