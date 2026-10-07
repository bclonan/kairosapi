package action

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReusableHTTPRequestArguments(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PROPFIND" || r.URL.Path != "/items/item 7" || r.Header.Get("X-Tenant") != "one" {
			t.Errorf("unexpected request %s %s %s", r.Method, r.URL.Path, r.Header.Get("X-Tenant"))
		}
		if values := r.URL.Query()["tag"]; len(values) != 2 || values[0] != "a" || values[1] != "b" {
			t.Errorf("query=%v", values)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "field=x&field=y" {
			t.Errorf("body=%s", body)
		}
		w.Header().Set("X-Page", "next")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	h, err := NewHTTP([]string{upstream.URL}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	raw, _ := json.Marshal(HTTPConfig{URLFromInput: true, MethodFromInput: true, Encoding: "form", InputHeaders: []string{"X-Tenant"}, ResponseHeaders: []string{"X-Page"}})
	handler, err := h.Prepare(raw)
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"url": upstream.URL + "/items/{id}", "method": "PROPFIND", "path": map[string]any{"id": "item 7"}, "query": map[string]any{"tag": []any{"a", "b"}}, "body": map[string]any{"field": []any{"x", "y"}}, "headers": map[string]any{"X-Tenant": "one"}}
	output, err := handler(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if output.(map[string]any)["headers"].(map[string]any)["X-Page"] != "next" {
		t.Fatalf("%#v", output)
	}
	input["headers"] = map[string]any{"Authorization": "secret"}
	if _, err := handler(context.Background(), input); err == nil {
		t.Fatal("undeclared header accepted")
	}
	input["headers"] = map[string]any{}
	input["url"] = "https://unlisted.example/"
	if _, err := handler(context.Background(), input); err == nil {
		t.Fatal("dynamic URL bypassed origin policy")
	}
}

func TestPublicWildcardStillBlocksPrivateDestinations(t *testing.T) {
	if _, err := NewHTTP([]string{"*"}, true, nil); err == nil {
		t.Fatal("wildcard private access accepted")
	}
	h, err := NewHTTP([]string{"*"}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	handler, err := h.Prepare(json.RawMessage(`{"url":"https://127.0.0.1/private","method":"GET"}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := handler(ctx, nil); err == nil {
		t.Fatal("private target accepted")
	}
	for _, raw := range []string{
		`{"url":"http://example.com/","method":"GET"}`,
		`{"url_from_input":true,"method":"GET","secret_headers":{"Authorization":"KAIROS_SECRET_AUTH"}}`,
		`{"url":"https://example.com/","method":"CONNECT"}`,
		`{"url":"https://example.com/","method":"GET","input_headers":["Host"]}`,
		`{"url":"https://example.com/","method":"GET","idempotency_header":"X-Key","input_headers":["x-key"]}`,
	} {
		if _, err := h.Prepare(json.RawMessage(raw)); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestDelayHonorsCancellation(t *testing.T) {
	h, err := Delay(json.RawMessage(`{"milliseconds":60000}`))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h(ctx, nil); err != context.Canceled {
		t.Fatalf("%v", err)
	}
	if _, err := Delay(json.RawMessage(`{"milliseconds":60001}`)); err == nil {
		t.Fatal("unbounded delay")
	}
}
