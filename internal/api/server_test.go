package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/orchestrator"
	"github.com/bclonan/kairosapi/internal/workflow"
)

const testToken = "local-test-token-with-at-least-32-characters"
const adminToken = "separate-admin-token-with-at-least-32-characters"

func testAPI(t *testing.T, registry workflow.Registry, queue int, logs io.Writer, definitions ...workflow.Definition) (http.Handler, *orchestrator.Service) {
	t.Helper()
	service, err := orchestrator.Open(orchestrator.Options{
		Path: filepath.Join(t.TempDir(), "test.db"), Workers: 1, MaxRuns: 1, QueueSize: queue,
		MaxRecords: 20, Retention: time.Hour, Timeout: 5 * time.Second,
	}, registry, definitions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	handler, err := New(service, testToken, adminToken, slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return handler, service
}

func request(handler http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func send(t *testing.T, h http.Handler, method, path string, value any, token string, status int) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	response := request(h, method, path, string(data), token)
	if response.Code != status {
		t.Fatalf("%s %s got %d want %d: %s", method, path, response.Code, status, response.Body)
	}
	return response
}

func TestAPIPublishReuseEventHTTPChainAndDedup(t *testing.T) {
	var customerCalls, notificationCalls atomic.Int32
	keys := make(chan string, 4)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/customers":
			keys <- r.Header.Get("Idempotency-Key")
			if r.Header.Get("X-Tenant") != "tenant-1" {
				t.Error("input header missing")
			}
			var data map[string]any
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil || data["name"] != "Ada" {
				t.Error("wrong customer input")
			}
			if customerCalls.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			_, _ = io.WriteString(w, `{"id":"customer_123"}`)
		case "/notifications/customer_123":
			notificationCalls.Add(1)
			if customerCalls.Load() != 2 {
				t.Error("dependent action ran before customer succeeded")
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer upstream.Close()
	httpAction, err := action.NewHTTP([]string{upstream.URL}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer httpAction.Close()
	var logs bytes.Buffer
	handler, service := testAPI(t, workflow.Registry{"http": httpAction.Prepare, "value": action.Value}, 4, &logs)
	childConfig, _ := json.Marshal(action.HTTPConfig{URLFromInput: true, Method: "POST", InputHeaders: []string{"X-Tenant"}, IdempotencyHeader: "Idempotency-Key"})
	child := workflow.Definition{ID: "create_customer", Version: 1,
		InputSchema: json.RawMessage(`{"type":"object","required":["url","name"],"properties":{"url":{"type":"string"},"name":{"type":"string"}}}`),
		Steps: []workflow.Step{{ID: "request", Action: "http", Config: childConfig,
			Retry: workflow.Retry{MaxAttempts: 3, Idempotent: true, BackoffMS: 1},
			Input: map[string]any{"url": map[string]any{"$ref": "/input/url"}, "headers": map[string]any{"X-Tenant": "tenant-1"}, "body": map[string]any{"name": map[string]any{"$ref": "/input/name"}}}}},
		Output: map[string]any{"id": map[string]any{"$ref": "/steps/request/body/id"}},
	}
	send(t, handler, "POST", "/v1/workflows", child, testToken, 403)
	send(t, handler, "POST", "/v1/workflows", child, adminToken, 201)
	notification, _ := json.Marshal(action.HTTPConfig{URL: upstream.URL + "/notifications/{id}", Method: "POST", ExpectJSON: map[string]any{"/ok": true}})
	parent := workflow.Definition{ID: "signup", Version: 1, Triggers: []workflow.Trigger{{Type: "customer.created", Source: "/signup"}},
		Steps: []workflow.Step{
			{ID: "customer", Action: "workflow", Config: json.RawMessage(`{"workflow_id":"create_customer","version":1}`), Input: map[string]any{"url": upstream.URL + "/customers", "name": map[string]any{"$ref": "/input/data/name"}}},
			{ID: "notify", Action: "http", Config: notification, DependsOn: []string{"customer"}, Input: map[string]any{"path": map[string]any{"id": map[string]any{"$ref": "/steps/customer/id"}}}},
		}, Output: map[string]any{"customer_id": map[string]any{"$ref": "/steps/customer/id"}},
	}
	send(t, handler, "POST", "/v1/workflows", parent, adminToken, 201)
	send(t, handler, "POST", "/v1/workflows", parent, adminToken, 200)
	event := map[string]any{"specversion": "1.0", "id": "evt-1", "source": "/signup", "type": "customer.created", "data": map[string]any{"name": "Ada", "secret": "do-not-log-input"}}
	response := send(t, handler, "POST", "/v1/events", event, testToken, 202)
	var delivery orchestrator.Delivery
	if err := json.Unmarshal(response.Body.Bytes(), &delivery); err != nil || len(delivery.Runs) != 1 {
		t.Fatalf("%s %v", response.Body, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	run, err := service.Wait(ctx, delivery.Runs[0].ID)
	if err != nil || run.State != "succeeded" || run.Output.(map[string]any)["customer_id"] != "customer_123" {
		t.Fatalf("%+v %v", run, err)
	}
	if customerCalls.Load() != 2 || notificationCalls.Load() != 1 {
		t.Fatalf("customer=%d notifications=%d", customerCalls.Load(), notificationCalls.Load())
	}
	key1, key2 := <-keys, <-keys
	if len(key1) != 64 || key1 != key2 {
		t.Fatal("retries changed the upstream idempotency key")
	}
	duplicate := send(t, handler, "POST", "/v1/events", event, testToken, 200)
	var repeated orchestrator.Delivery
	_ = json.Unmarshal(duplicate.Body.Bytes(), &repeated)
	if !repeated.Duplicate || repeated.Runs[0].ID != run.ID || customerCalls.Load() != 2 {
		t.Fatalf("%s", duplicate.Body)
	}
	send(t, handler, "GET", "/v1/runs/"+run.ID, nil, testToken, 200)
	send(t, handler, "GET", "/v1/workflows/signup/versions/1", nil, adminToken, 200)
	if strings.Contains(logs.String(), "do-not-log-input") || strings.Contains(logs.String(), testToken) {
		t.Fatal("credentials or input leaked to logs")
	}
}

func TestAPIValidationAuthenticationAndInlineSpecification(t *testing.T) {
	def := workflow.Definition{ID: "welcome", Version: 1, Steps: []workflow.Step{{ID: "value", Action: "value"}}}
	handler, _ := testAPI(t, workflow.Registry{"value": action.Value}, 2, io.Discard, def)
	cases := []struct {
		method, path, body, token string
		status                    int
	}{
		{"GET", "/healthz", "", "", 200}, {"GET", "/readyz", "", "", 200},
		{"GET", "/v1/workflows", "", "", 401}, {"GET", "/v1/workflows", "", "wrong", 401}, {"GET", "/v1/workflows", "", testToken, 200},
		{"GET", "/v1/workflows/welcome", "", testToken, 403},
		{"POST", "/v1/runs", `{"workflow_id":"welcome"}`, "", 401},
		{"POST", "/v1/runs", `{"workflow_id":"missing"}`, testToken, 404},
		{"POST", "/v1/runs", `{"workflow_id":"welcome","typo":true}`, testToken, 400},
		{"POST", "/v1/runs", `{"workflow_id":"welcome","workflow_id":"welcome"}`, testToken, 400},
		{"POST", "/v1/runs", `{"workflow_id":"welcome"} {}`, testToken, 400},
		{"POST", "/v1/runs", `{"workflow_id":"welcome","input":[]}`, testToken, 400},
		{"POST", "/v1/runs", strings.Repeat("x", MaxRequestBytes+1), testToken, 413},
		{"POST", "/v1/runs", `null`, testToken, 400},
		{"POST", "/test/get-intent", `{}`, testToken, 404},
		{"POST", "/v1/runs", `{"workflow_id":"welcome","input":{}}`, testToken, 200},
		{"POST", "/v1/events", `{"id":"one"}`, testToken, 422},
		{"GET", "/v1/runs?limit=101", "", testToken, 400},
		{"GET", "/v1/runs/missing", "", testToken, 404},
	}
	for _, tc := range cases {
		response := request(handler, tc.method, tc.path, tc.body, tc.token)
		if response.Code != tc.status {
			t.Errorf("%s %s got %d want %d: %s", tc.method, tc.path, response.Code, tc.status, response.Body)
		}
	}
	r := httptest.NewRequest("POST", "/v1/runs", strings.NewReader("{}"))
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatalf("got %d", w.Code)
	}
	inline := orchestrator.Request{Specification: &def}
	send(t, handler, "POST", "/v1/runs", inline, testToken, 403)
	send(t, handler, "POST", "/v1/runs", inline, adminToken, 200)
	changed := def
	changed.Description = "changed"
	send(t, handler, "POST", "/v1/workflows", changed, adminToken, 409)
}

func TestAPIAsyncCapacityCancellationAndIdempotency(t *testing.T) {
	started := make(chan struct{}, 1)
	def := workflow.Definition{ID: "wait", Version: 1, Steps: []workflow.Step{{ID: "wait", Action: "wait"}}}
	handler, service := testAPI(t, workflow.Registry{"wait": func(json.RawMessage) (workflow.Handler, error) {
		return func(ctx context.Context, _ map[string]any) (any, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}, nil
	}}, 0, io.Discard, def)
	create := func(body string, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/runs", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	first := create(`{"workflow_id":"wait","async":true}`, "same")
	var run orchestrator.Run
	if err := json.Unmarshal(first.Body.Bytes(), &run); err != nil || first.Code != 202 {
		t.Fatalf("%s %v", first.Body, err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("run not started")
	}
	again := create(`{"workflow_id":"wait","async":true}`, "same")
	if again.Code != 202 || again.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("%s", again.Body)
	}
	full := create(`{"workflow_id":"wait","async":true}`, "different")
	if full.Code != 429 || full.Header().Get("Retry-After") != "1" {
		t.Fatalf("%d %s", full.Code, full.Body)
	}
	send(t, handler, "POST", "/v1/runs/"+run.ID+"/cancel", nil, testToken, 200)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	finished, err := service.Wait(ctx, run.ID)
	if err != nil || finished.State != "canceled" {
		t.Fatalf("%+v %v", finished, err)
	}
}
