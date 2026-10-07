package action

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/workflow"
)

func TestHTTPAsyncWaitsForProviderBeforeDependentStep(t *testing.T) {
	var starts, polls, downstream atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/start":
			starts.Add(1)
			if r.Method != http.MethodPost || r.Header.Get("Idempotency-Key") == "" {
				t.Error("initial request lost method or idempotency key")
			}
			w.Header().Set("Location", "/operations/42")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"status":"pending"}`)
		case "/operations/42":
			count := polls.Add(1)
			if r.Method != http.MethodGet || r.Header.Get("Idempotency-Key") != "" || r.Header.Get("Content-Type") != "" {
				t.Error("poll must be GET without body or initial idempotency headers")
			}
			if r.Header.Get("Authorization") != "Bearer local-test-secret" {
				t.Error("same-origin poll lost its configured secret")
			}
			if downstream.Load() != 0 {
				t.Error("dependent step ran before operation completed")
			}
			if count < 2 {
				_, _ = io.WriteString(w, `{"status":"running"}`)
			} else {
				_, _ = io.WriteString(w, `{"status":"completed","value":"provider-result"}`)
			}
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	h := localHTTP(t, server.URL)
	raw, _ := json.Marshal(HTTPConfig{URL: server.URL + "/start", Method: "POST", IdempotencyHeader: "Idempotency-Key", SecretHeaders: map[string]string{"Authorization": "KAIROS_SECRET_PARTNER"}, Async: &HTTPAsyncConfig{IntervalMS: 100}})
	registry := workflow.Registry{"http": h.Prepare, "after": func(json.RawMessage) (workflow.Handler, error) {
		return func(_ context.Context, input map[string]any) (any, error) {
			downstream.Add(1)
			if polls.Load() != 2 || input["value"] != "provider-result" {
				t.Errorf("dependent input or execution order is wrong: %#v", input)
			}
			return input, nil
		}, nil
	}}
	plan, err := workflow.Compile(workflow.Definition{ID: "async_chain", Version: 1, Steps: []workflow.Step{
		{ID: "start", Action: "http", Config: raw},
		{ID: "after", Action: "after", DependsOn: []string{"start"}, Input: map[string]any{"value": map[string]any{"$ref": "/steps/start/body/value"}}},
	}}, registry, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := workflow.New(2, 1)
	result, err := engine.Run(context.Background(), plan, nil)
	if err != nil || result.State != "succeeded" || starts.Load() != 1 || polls.Load() != 2 || downstream.Load() != 1 {
		t.Fatalf("result=%+v error=%v starts=%d polls=%d downstream=%d", result, err, starts.Load(), polls.Load(), downstream.Load())
	}
}

func TestHTTPAsyncJSONPointerAndScalarStatuses(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"operation":{"url":"/work/one"}}`)
			return
		}
		if polls.Add(1) == 1 {
			_, _ = io.WriteString(w, `{"done":false}`)
			return
		}
		_, _ = io.WriteString(w, `{"done":true,"value":7}`)
	}))
	defer server.Close()
	handler := prepare(t, localHTTP(t, server.URL), HTTPConfig{URL: server.URL + "/start", Method: "POST", Async: &HTTPAsyncConfig{
		PollURLPointer: "/operation/url", StatusPointer: "/done", PendingValues: []any{false}, SuccessValues: []any{true}, FailureValues: []any{"error"}, IntervalMS: 100,
	}})
	result, err := handler(context.Background(), nil)
	if err != nil || result.(map[string]any)["body"].(map[string]any)["value"] != json.Number("7") || polls.Load() != 2 {
		t.Fatalf("result=%#v error=%v polls=%d", result, err, polls.Load())
	}
}

func TestHTTPAsyncTransientPollingDoesNotRestartOperation(t *testing.T) {
	var starts, polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			starts.Add(1)
			w.Header().Set("Location", "/work")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if polls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, `{"status":"succeeded"}`)
	}))
	defer server.Close()
	handler := prepare(t, localHTTP(t, server.URL), HTTPConfig{URL: server.URL + "/start", Method: "POST", Async: &HTTPAsyncConfig{IntervalMS: 100}})
	if _, err := handler(context.Background(), nil); err != nil || starts.Load() != 1 || polls.Load() != 2 {
		t.Fatalf("err=%v starts=%d polls=%d", err, starts.Load(), polls.Load())
	}
}

func TestHTTPAsyncFailureAndBounds(t *testing.T) {
	for _, scenario := range []string{"provider_failure", "unknown_status", "missing_status", "invalid_json", "oversize", "max_polls", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			var polls, redirected atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/start" {
					w.Header().Set("Location", "/work")
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if r.URL.Path == "/redirect" {
					redirected.Add(1)
					return
				}
				polls.Add(1)
				switch scenario {
				case "provider_failure":
					_, _ = io.WriteString(w, `{"status":"failed","error":"provider-secret"}`)
				case "unknown_status":
					_, _ = io.WriteString(w, `{"status":"provider-secret"}`)
				case "missing_status":
					_, _ = io.WriteString(w, `{"message":"provider-secret"}`)
				case "invalid_json":
					_, _ = io.WriteString(w, "provider-secret")
				case "oversize":
					_, _ = io.WriteString(w, strings.Repeat("x", workflow.MaxDataBytes+1))
				case "max_polls":
					_, _ = io.WriteString(w, `{"status":"pending"}`)
				case "redirect":
					w.Header().Set("Location", "/redirect")
					w.WriteHeader(http.StatusFound)
				}
			}))
			defer server.Close()
			handler := prepare(t, localHTTP(t, server.URL), HTTPConfig{URL: server.URL + "/start", Method: "POST", Async: &HTTPAsyncConfig{IntervalMS: 100, MaxPolls: 2}})
			_, err := handler(context.Background(), nil)
			wantPolls := int32(1)
			if scenario == "max_polls" {
				wantPolls = 2
			}
			var retryable *workflow.ActionError
			if err == nil || strings.Contains(err.Error(), "provider-secret") || errors.As(err, &retryable) || polls.Load() != wantPolls || redirected.Load() != 0 {
				t.Fatalf("error=%v polls=%d redirected=%d", err, polls.Load(), redirected.Load())
			}
		})
	}
}

func TestHTTPAsyncRejectsCrossOriginOperationAndCredentials(t *testing.T) {
	var crossOriginCalls atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { crossOriginCalls.Add(1) }))
	defer other.Close()
	for _, target := range []string{other.URL + "/work", "https://user:password@host.example/work", "/work#fragment", "/work/%7Bparam%7D", "/work/%5Cbad"} {
		t.Run(target, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", target)
				w.WriteHeader(http.StatusAccepted)
			}))
			defer server.Close()
			handler := prepare(t, localHTTP(t, server.URL, other.URL), HTTPConfig{URL: server.URL, Method: "POST", SecretHeaders: map[string]string{"Authorization": "KAIROS_SECRET_PARTNER"}, Async: &HTTPAsyncConfig{IntervalMS: 100}})
			if _, err := handler(context.Background(), nil); err == nil {
				t.Fatal("unsafe operation URL accepted")
			}
		})
	}
	if crossOriginCalls.Load() != 0 {
		t.Fatal("cross-origin server received a request")
	}
}

func TestHTTPAsyncCancellationDuringPollWait(t *testing.T) {
	started := make(chan struct{})
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			w.Header().Set("Location", "/work")
			w.WriteHeader(http.StatusAccepted)
			close(started)
			return
		}
		polls.Add(1)
	}))
	defer server.Close()
	handler := prepare(t, localHTTP(t, server.URL), HTTPConfig{URL: server.URL + "/start", Method: "POST", Async: &HTTPAsyncConfig{IntervalMS: 30000}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := handler(ctx, nil); done <- err }()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || polls.Load() != 0 {
			t.Fatalf("error=%v polls=%d", err, polls.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("async poll wait ignored cancellation")
	}
}

func TestHTTPAsyncConfigurationValidation(t *testing.T) {
	h := localHTTP(t, "https://provider.example")
	for _, cfg := range []*HTTPAsyncConfig{
		{PollURLPointer: "bad"}, {StatusPointer: "/bad~2"}, {IntervalMS: 99}, {IntervalMS: 30001}, {MaxPolls: -1}, {MaxPolls: 1001},
		{PendingValues: []any{"duplicate"}, SuccessValues: []any{"duplicate"}},
		{PendingValues: []any{map[string]any{"status": "pending"}}},
		{SuccessValues: []any{""}}, {FailureValues: []any{strings.Repeat("x", 129)}},
	} {
		raw, _ := json.Marshal(HTTPConfig{URL: "https://provider.example/start", Method: "POST", Async: cfg})
		if _, err := h.Prepare(raw); err == nil {
			t.Fatalf("accepted invalid config %#v", cfg)
		}
	}
	raw, _ := json.Marshal(HTTPConfig{URL: "https://provider.example/start", Method: "POST", Async: &HTTPAsyncConfig{}, ResponseMode: "file"})
	if _, err := h.Prepare(raw); err == nil {
		t.Fatal("accepted file response with JSON async operation status")
	}
}

func TestHTTPAsyncOptInAndSynchronousCompletion(t *testing.T) {
	for _, scenario := range []string{"disabled", "synchronous"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if scenario == "disabled" {
					w.WriteHeader(http.StatusAccepted)
				}
				_, _ = io.WriteString(w, `{"ok":true}`)
			}))
			defer server.Close()
			config := HTTPConfig{URL: server.URL, Method: "POST"}
			if scenario == "synchronous" {
				config.Async = &HTTPAsyncConfig{}
			}
			result, err := prepare(t, localHTTP(t, server.URL), config)(context.Background(), nil)
			if err != nil || calls.Load() != 1 || result.(map[string]any)["body"].(map[string]any)["ok"] != true {
				t.Fatalf("result=%#v error=%v calls=%d", result, err, calls.Load())
			}
		})
	}
}

func TestHTTPAsyncCheckpointResumesWithoutRepeatingPOST(t *testing.T) {
	var starts, polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			starts.Add(1)
			w.Header().Set("Location", "/work")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		polls.Add(1)
		_, _ = io.WriteString(w, `{"status":"completed","value":"resumed"}`)
	}))
	defer server.Close()
	h := localHTTP(t, server.URL)
	raw, _ := json.Marshal(HTTPConfig{URL: server.URL + "/start", Method: "POST", Async: &HTTPAsyncConfig{IntervalMS: 100}})
	plan, err := workflow.Compile(workflow.Definition{ID: "resume_async", Version: 1, Steps: []workflow.Step{{ID: "operation", Action: "http", Config: raw}}}, workflow.Registry{"http": h.Prepare}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := workflow.New(1, 1)
	var persisted workflow.Result
	id := workflow.NewID()
	_, err = engine.RunTracked(context.Background(), plan, nil, id, func(result workflow.Result) error {
		if result.Steps[0].Phase == "accepted" {
			return errors.New("injected persistence failure after handle commit")
		}
		if result.State == "running" && result.Steps[0].Checkpoint["kind"] == "http_async" {
			data, _ := json.Marshal(result)
			_ = workflow.Decode(data, &persisted)
		}
		return nil
	})
	if !errors.Is(err, workflow.ErrPersistence) || starts.Load() != 1 || polls.Load() != 0 || persisted.ID == "" {
		t.Fatalf("error=%v starts=%d polls=%d persisted=%+v", err, starts.Load(), polls.Load(), persisted)
	}
	resume, err := plan.PrepareResume(persisted, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.RunTrackedFrom(context.Background(), plan, nil, id, resume, nil)
	if err != nil || result.State != "succeeded" || starts.Load() != 1 || polls.Load() != 1 || result.Steps[0].Checkpoint["polls"] != json.Number("1") {
		t.Fatalf("result=%+v error=%v starts=%d polls=%d", result, err, starts.Load(), polls.Load())
	}
}

func TestHTTPAsyncInitialRetryKeepsIdempotencyKey(t *testing.T) {
	var starts atomic.Int32
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/start" {
			_, _ = io.WriteString(w, `{"status":"completed"}`)
			return
		}
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		if starts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Location", "/work")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	h := localHTTP(t, server.URL)
	raw, _ := json.Marshal(HTTPConfig{URL: server.URL + "/start", Method: "POST", IdempotencyHeader: "Idempotency-Key", Async: &HTTPAsyncConfig{IntervalMS: 100}})
	plan, err := workflow.Compile(workflow.Definition{ID: "retry_async", Version: 1, Steps: []workflow.Step{
		{ID: "operation", Action: "http", Config: raw, Retry: workflow.Retry{MaxAttempts: 2, Idempotent: true}},
	}}, workflow.Registry{"http": h.Prepare}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := workflow.New(1, 1)
	result, err := engine.Run(context.Background(), plan, nil)
	if err != nil || result.State != "succeeded" || len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("result=%+v error=%v keys=%v", result, err, keys)
	}
}

func TestHTTPAsyncPollBudgetSurvivesResume(t *testing.T) {
	var starts, polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			starts.Add(1)
			w.Header().Set("Location", "/work")
			w.WriteHeader(http.StatusAccepted)
			return
		}
		polls.Add(1)
		_, _ = io.WriteString(w, `{"status":"completed"}`)
	}))
	defer server.Close()
	h := localHTTP(t, server.URL)
	raw, _ := json.Marshal(HTTPConfig{URL: server.URL + "/start", Method: "POST", Async: &HTTPAsyncConfig{IntervalMS: 100, MaxPolls: 1}})
	plan, err := workflow.Compile(workflow.Definition{ID: "poll_budget", Version: 1, Steps: []workflow.Step{{ID: "operation", Action: "http", Config: raw}}}, workflow.Registry{"http": h.Prepare}, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	engine, _ := workflow.New(1, 1)
	var persisted workflow.Result
	id := workflow.NewID()
	_, err = engine.RunTracked(context.Background(), plan, nil, id, func(result workflow.Result) error {
		if result.Steps[0].Phase == "polling" {
			return errors.New("injected interruption after poll count commit")
		}
		if result.State == "running" && result.Steps[0].Checkpoint["polls"] == json.Number("1") {
			data, _ := json.Marshal(result)
			_ = workflow.Decode(data, &persisted)
		}
		return nil
	})
	if !errors.Is(err, workflow.ErrPersistence) || starts.Load() != 1 || polls.Load() != 0 || persisted.ID == "" {
		t.Fatalf("error=%v starts=%d polls=%d persisted=%+v", err, starts.Load(), polls.Load(), persisted)
	}
	resume, err := plan.PrepareResume(persisted, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engine.RunTrackedFrom(context.Background(), plan, nil, id, resume, nil)
	if err != nil || result.State != "failed" || result.Steps[0].Error != "async operation exceeded max_polls" || starts.Load() != 1 || polls.Load() != 0 {
		t.Fatalf("result=%+v error=%v starts=%d polls=%d", result, err, starts.Load(), polls.Load())
	}
}
