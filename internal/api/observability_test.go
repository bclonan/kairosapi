package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/orchestrator"
	"github.com/bclonan/kairosapi/internal/workflow"
)

func TestHistoryReplayStreamAndMetricsAuthorization(t *testing.T) {
	definition := workflow.Definition{ID: "observed", Version: 1, Steps: []workflow.Step{{ID: "result", Action: "value", Input: map[string]any{"credential": "private-value"}}}}
	handler, _ := testAPI(t, workflow.Registry{"value": action.Value}, 2, io.Discard, definition)
	accepted := send(t, handler, "POST", "/v1/runs", map[string]any{"workflow_id": "observed"}, testToken, 200)
	var run orchestrator.Run
	if err := json.Unmarshal(accepted.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	path := "/v1/runs/" + run.ID
	for _, endpoint := range []string{path + "/history", path + "/stream", "/v1/metrics"} {
		if response := request(handler, "GET", endpoint, "", ""); response.Code != 401 {
			t.Fatalf("unprotected endpoint %s: %d", endpoint, response.Code)
		}
	}
	response := request(handler, "GET", path+"/history?limit=2", "", testToken)
	var page orchestrator.HistoryPage
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &page) != nil || len(page.Events) != 2 || !page.HasMore {
		t.Fatalf("history: %d %s", response.Code, response.Body)
	}
	r := httptest.NewRequest(http.MethodGet, path+"/stream", nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Last-Event-ID", page.NextAfter)
	stream := httptest.NewRecorder()
	handler.ServeHTTP(stream, r)
	if stream.Code != 200 || stream.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(stream.Body.String(), "event: complete\n") || !strings.Contains(stream.Body.String(), "event: transition\n") {
		t.Fatalf("stream: %d %s", stream.Code, stream.Body)
	}
	if strings.Contains(stream.Body.String(), "id: "+page.NextAfter+"\n") || strings.Contains(stream.Body.String(), "private-value") {
		t.Fatal("stream replayed cursor or exposed result payload")
	}
	metrics := request(handler, "GET", "/v1/metrics", "", testToken)
	var snapshot orchestrator.MetricsSnapshot
	if metrics.Code != 200 || json.Unmarshal(metrics.Body.Bytes(), &snapshot) != nil || snapshot.Runs["succeeded"] != 1 || snapshot.RetainedRuns != 1 {
		t.Fatalf("metrics: %d %s", metrics.Code, metrics.Body)
	}
	for _, query := range []string{"after=invalid", "limit=101", "limit=0"} {
		if response := request(handler, "GET", path+"/history?"+query, "", testToken); response.Code != 400 {
			t.Fatalf("query %s returned %d", query, response.Code)
		}
	}
	if response := request(handler, "GET", "/v1/runs/missing/history", "", testToken); response.Code != 404 {
		t.Fatalf("missing run returned %d", response.Code)
	}
}
