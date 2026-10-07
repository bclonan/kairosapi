package action

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/workflow"
)

func prepare(t *testing.T, h *HTTP, config HTTPConfig) workflow.Handler {
	t.Helper()
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := h.Prepare(raw)
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func localHTTP(t *testing.T, origins ...string) *HTTP {
	t.Helper()
	h, err := NewHTTP(origins, true, func(name string) (string, bool) { return "Bearer local-test-secret", name == "KAIROS_SECRET_PARTNER" })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}

func TestHTTPRequestBindingsAndSecretHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/customers/customer 1" || r.URL.Query().Get("limit") != "2" || r.URL.Query().Get("fixed") != "yes" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer local-test-secret" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("incorrect headers")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["name"] != "Ada" {
			t.Error("incorrect body")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"ok":true,"id":9007199254740993}`)
	}))
	defer server.Close()
	h := localHTTP(t, server.URL)
	handler := prepare(t, h, HTTPConfig{URL: server.URL + "/customers/{id}?fixed=yes", Method: "POST", SecretHeaders: map[string]string{"Authorization": "KAIROS_SECRET_PARTNER"}, ExpectJSON: map[string]any{"/ok": true}})
	result, err := handler(context.Background(), map[string]any{"path": map[string]any{"id": "customer 1"}, "query": map[string]any{"limit": json.Number("2")}, "body": map[string]any{"name": "Ada"}})
	if err != nil {
		t.Fatal(err)
	}
	output := result.(map[string]any)
	if output["status"] != 201 || output["body"].(map[string]any)["id"] != json.Number("9007199254740993") {
		t.Fatalf("%#v", result)
	}
}

func TestFormAndRawBodies(t *testing.T) {
	for _, encoding := range []string{"form", "raw"} {
		t.Run(encoding, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if encoding == "form" && string(body) != "customer=cus_1&enabled=true" {
					t.Errorf("unexpected form %s", body)
				}
				if encoding == "raw" && string(body) != "<message>Hello</message>" {
					t.Errorf("unexpected raw body %s", body)
				}
				w.WriteHeader(204)
			}))
			defer server.Close()
			handler := prepare(t, localHTTP(t, server.URL), HTTPConfig{URL: server.URL, Method: "POST", Encoding: encoding})
			var body any = map[string]any{"customer": "cus_1", "enabled": true}
			if encoding == "raw" {
				body = "<message>Hello</message>"
			}
			if _, err := handler(context.Background(), map[string]any{"body": body}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUpstreamFailuresAreBoundedAndNotRetried(t *testing.T) {
	for _, scenario := range []string{"status", "redirect", "size", "condition"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				switch scenario {
				case "status":
					w.WriteHeader(429)
					_, _ = io.WriteString(w, "upstream-secret")
				case "redirect":
					w.Header().Set("Location", "/redirect-target")
					w.WriteHeader(302)
				case "size":
					_, _ = io.WriteString(w, strings.Repeat("x", workflow.MaxDataBytes+1))
				case "condition":
					_, _ = io.WriteString(w, `{"ok":false,"error":"upstream-secret"}`)
				}
			}))
			defer server.Close()
			cfg := HTTPConfig{URL: server.URL, Method: "POST"}
			if scenario == "condition" {
				cfg.ExpectJSON = map[string]any{"/ok": true}
			}
			handler := prepare(t, localHTTP(t, server.URL), cfg)
			_, err := handler(context.Background(), nil)
			if err == nil || strings.Contains(err.Error(), "secret") || calls.Load() != 1 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestHTTPContextCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { close(started); <-release }))
	defer server.Close()
	defer close(release)
	handler := prepare(t, localHTTP(t, server.URL), HTTPConfig{URL: server.URL, Method: "GET"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, err := handler(ctx, nil); finished <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP request ignored cancellation")
	}
}

func TestOriginAndSecretValidation(t *testing.T) {
	h := localHTTP(t, "https://allowed.example")
	cases := []HTTPConfig{
		{URL: "https://allowed.example", Method: "GET", ExpectJSON: map[string]any{"/bad~2": true}},
		{URL: "https://other.example", Method: "GET"},
		{URL: "https://allowed.example:8443", Method: "GET"},
		{URL: "https://user:secret@allowed.example", Method: "GET"},
		{URL: "https://allowed.example", Method: "CONNECT"},
		{URL: "https://allowed.example", Method: "GET", Headers: map[string]string{"Authorization": "secret"}},
		{URL: "https://allowed.example", Method: "GET", Headers: map[string]string{"X-Header": "bad\r\nInjected: yes"}},
		{URL: "https://allowed.example", Method: "GET", SecretHeaders: map[string]string{"Authorization": "KAIROS_API_TOKEN"}},
		{URL: "https://allowed.example", Method: "GET", SecretHeaders: map[string]string{"Authorization": "KAIROS_SECRET_MISSING"}},
	}
	for _, cfg := range cases {
		raw, _ := json.Marshal(cfg)
		if _, err := h.Prepare(raw); err == nil {
			t.Fatal("accepted invalid HTTP configuration")
		}
	}
	if _, err := NewHTTP([]string{"http://example.com"}, false, nil); err == nil {
		t.Fatal("allowed plaintext HTTP")
	}
	if _, err := NewHTTP([]string{"https://example.com/path"}, false, nil); err == nil {
		t.Fatal("allowed path in origin")
	}
}

func TestNetworkPolicy(t *testing.T) {
	for _, address := range []string{"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.100.100.200", "0.0.0.0", "0.0.0.1", "224.0.0.1", "::1", "::ffff:127.0.0.1", "::7f00:1", "fc00::1", "fe80::1", "2001:db8::1", "64:ff9b::7f00:1"} {
		if publicIP(netip.MustParseAddr(address)) {
			t.Errorf("allowed restricted address %s", address)
		}
	}
	if !publicIP(netip.MustParseAddr("1.1.1.1")) || !publicIP(netip.MustParseAddr("2606:4700:4700::1111")) {
		t.Fatal("rejected public address")
	}
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	h, err := NewHTTP([]string{server.URL}, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	handler := prepare(t, h, HTTPConfig{URL: server.URL, Method: "GET"})
	if _, err := handler(context.Background(), nil); err == nil || calls.Load() != 0 {
		t.Fatal("reached loopback destination")
	}
	// Exercise the dial policy directly so TLS trust failure cannot make this pass.
	if _, err := h.dialContext(context.Background(), "tcp", strings.TrimPrefix(server.URL, "https://")); err == nil || !strings.Contains(err.Error(), "restricted") {
		t.Fatalf("policy did not reject destination: %v", err)
	}
}

func TestPathParametersCannotChangeRouteStructure(t *testing.T) {
	for _, value := range []string{"..", ".", "/admin", "a/b", "a\\b", ""} {
		if _, err := renderPath([]string{"", "users", "{id}"}, map[string]any{"id": value}); err == nil {
			t.Errorf("accepted %q", value)
		}
	}
	path, err := renderPath([]string{"", "users", "{id}"}, map[string]any{"id": "a?admin=true"})
	if err != nil || path != "/users/a%3Fadmin=true" {
		t.Fatalf("%s %v", path, err)
	}
}
