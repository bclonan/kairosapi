package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/orchestrator"
	"github.com/bclonan/kairosapi/internal/workflow"
)

func fileRequest(h http.Handler, method, path, token string, data []byte, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestOpaqueFileTypesUploadDownloadAndReferences(t *testing.T) {
	h, _ := testAPI(t, workflow.Registry{"value": action.Value}, 1, io.Discard)
	formats := []struct {
		name, media string
		data        []byte
	}{
		{"example.pdf", "application/pdf", []byte("%PDF-1.7\nopaque bytes\x00\xff")},
		{"image.png", "image/png", []byte{137, 80, 78, 71, 13, 10, 26, 10, 0, 255}},
		{"audio.wav", "audio/wav", []byte("RIFF\x00\xffWAVE")},
		{"archive.zip", "application/zip", []byte{'P', 'K', 3, 4, 0, 255, 128}},
		{"data.csv", "text/csv; charset=utf-8", []byte("name,value\nAda,1\n")},
		{"opaque.bin", "application/octet-stream", []byte{0, 1, 127, 128, 255}},
		{"empty.bin", "application/octet-stream", []byte{}},
	}
	var previous string
	for _, format := range formats {
		t.Run(format.name, func(t *testing.T) {
			sum := sha256.Sum256(format.data)
			digest := hex.EncodeToString(sum[:])
			headers := map[string]string{"Content-Type": format.media, "X-File-Name": format.name, "X-Content-SHA256": digest, "Idempotency-Key": format.name}
			response := fileRequest(h, "POST", "/v1/files", testToken, format.data, headers)
			if response.Code != 201 {
				t.Fatalf("%d %s", response.Code, response.Body)
			}
			var saved struct {
				File      artifact.Metadata
				Reference map[string]any
			}
			if err := json.Unmarshal(response.Body.Bytes(), &saved); err != nil {
				t.Fatal(err)
			}
			if !identity.Valid(saved.File.ID) || saved.File.ID <= previous {
				t.Fatal("files not sequenced")
			}
			previous = saved.File.ID
			replay := fileRequest(h, "POST", "/v1/files", testToken, format.data, headers)
			if replay.Code != 200 || replay.Header().Get("Idempotency-Replayed") != "true" {
				t.Fatal("duplicate file")
			}
			download := fileRequest(h, "GET", "/v1/files/"+saved.File.ID+"/content", testToken, nil, nil)
			if download.Code != 200 || !bytes.Equal(download.Body.Bytes(), format.data) || download.Header().Get("X-Content-SHA256") != digest || !strings.HasPrefix(download.Header().Get("Content-Disposition"), "attachment;") {
				t.Fatal("file bytes or metadata changed")
			}
			if len(format.data) > 2 {
				part := fileRequest(h, "GET", "/v1/files/"+saved.File.ID+"/content", testToken, nil, map[string]string{"Range": "bytes=1-2"})
				if part.Code != 206 || !bytes.Equal(part.Body.Bytes(), format.data[1:3]) {
					t.Fatal("range failed")
				}
			}
			if fileRequest(h, "DELETE", "/v1/files/"+saved.File.ID, testToken, nil, nil).Code != 403 {
				t.Fatal("runner deleted file")
			}
			if fileRequest(h, "DELETE", "/v1/files/"+saved.File.ID, adminToken, nil, nil).Code != 204 {
				t.Fatal("admin deletion failed")
			}
		})
	}
	if fileRequest(h, "POST", "/v1/files", "", []byte("no auth"), nil).Code != 401 {
		t.Fatal("anonymous upload accepted")
	}
	if fileRequest(h, "POST", "/v1/files", testToken, []byte("bad hash"), map[string]string{"X-Content-SHA256": strings.Repeat("0", 64)}).Code != 400 {
		t.Fatal("bad digest accepted")
	}
	if fileRequest(h, "POST", "/v1/files", testToken, nil, map[string]string{"X-File-Name": "../outside"}).Code != 400 {
		t.Fatal("path traversal accepted")
	}
}

func TestHTTPBinaryChainStagingAndMultipart(t *testing.T) {
	data := make([]byte, 180000)
	for i := range data {
		data[i] = byte(i*19 + i/65536)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/source":
			w.Header().Set("Content-Type", "application/zip")
			_, _ = w.Write(data)
		case "/raw":
			got, err := io.ReadAll(r.Body)
			if err != nil || !bytes.Equal(got, data) || r.Header.Get("Content-Type") != "application/zip" {
				t.Error("raw bytes changed")
				w.WriteHeader(400)
				return
			}
			writeJSON(w, 200, map[string]any{"received": len(got)})
		case "/multipart":
			reader, err := r.MultipartReader()
			if err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			found := false
			var tags []string
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				got, _ := io.ReadAll(part)
				if part.FormName() == "tag" {
					tags = append(tags, string(got))
				} else {
					found = part.FormName() == "document" && part.FileName() == "report.zip" && bytes.Equal(got, data)
				}
			}
			if !found || strings.Join(tags, ",") != "a,b" {
				t.Error("multipart changed")
				w.WriteHeader(400)
				return
			}
			writeJSON(w, 200, map[string]any{"ok": true})
		default:
			w.WriteHeader(404)
		}
	}))
	defer upstream.Close()
	httpAction, err := action.NewHTTP([]string{upstream.URL}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer httpAction.Close()
	seen := make(chan string, 1)
	gate := make(chan struct{})
	registry := workflow.Registry{"http": httpAction.Prepare, "gate": func(json.RawMessage) (workflow.Handler, error) {
		return func(ctx context.Context, input map[string]any) (any, error) {
			id, err := artifact.ReferenceID(input["file"])
			if err != nil {
				return nil, err
			}
			seen <- id
			select {
			case <-gate:
				return input, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, nil
	}}
	s, err := orchestrator.Open(orchestrator.Options{Path: filepath.Join(t.TempDir(), "test.db"), Workers: 2, MaxRuns: 1, QueueSize: 2, MaxRecords: 10, Retention: time.Hour, Timeout: 10 * time.Second, ConfigureFiles: httpAction.SetFiles}, registry, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h, err := New(s, testToken, adminToken, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	raw := func(value any) json.RawMessage { data, _ := json.Marshal(value); return data }
	ref := map[string]any{"$ref": "/steps/fetch/body"}
	def := workflow.Definition{ID: "binary-chain", Version: 1, Steps: []workflow.Step{
		{ID: "fetch", Action: "http", Config: raw(map[string]any{"url": upstream.URL + "/source", "method": "GET", "response_mode": "file", "response_name": "report.zip"})},
		{ID: "hold", Action: "gate", DependsOn: []string{"fetch"}, Input: map[string]any{"file": ref}},
		{ID: "send", Action: "http", DependsOn: []string{"fetch", "hold"}, Input: map[string]any{"body": ref}, Config: raw(map[string]any{"url": upstream.URL + "/raw", "method": "POST", "encoding": "file"})},
		{ID: "form", Action: "http", DependsOn: []string{"fetch", "send"}, Input: map[string]any{"body": map[string]any{"fields": map[string]any{"tag": []any{"a", "b"}}, "files": []any{map[string]any{"field": "document", "file": ref}}}}, Config: raw(map[string]any{"url": upstream.URL + "/multipart", "method": "POST", "encoding": "multipart"})},
	}, Output: map[string]any{"file": ref, "ok": map[string]any{"$ref": "/steps/form/body/ok"}}}
	send(t, h, "POST", "/v1/workflows", def, adminToken, 201)
	accepted := send(t, h, "POST", "/v1/runs", map[string]any{"workflow_id": def.ID, "async": true}, testToken, 202)
	var run orchestrator.Run
	if err := json.Unmarshal(accepted.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}
	var fileID string
	select {
	case fileID = <-seen:
	case <-time.After(3 * time.Second):
		t.Fatal("file action did not finish")
	}
	if fileRequest(h, "GET", "/v1/files/"+fileID+"/content", testToken, nil, nil).Code != 404 {
		t.Fatal("active run published output")
	}
	close(gate)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed, err := s.Wait(ctx, run.ID)
	if err != nil || completed.State != "succeeded" {
		t.Fatalf("%+v %v", completed, err)
	}
	down := fileRequest(h, "GET", "/v1/files/"+fileID+"/content", testToken, nil, nil)
	if down.Code != 200 || !bytes.Equal(down.Body.Bytes(), data) {
		t.Fatal("final publication missing")
	}
	if fileRequest(h, "DELETE", "/v1/files/"+fileID, adminToken, nil, nil).Code != 409 {
		t.Fatal("referenced file deleted")
	}
}

func TestFileAdmissionBackupAndPagination(t *testing.T) {
	h, s := testAPI(t, workflow.Registry{"value": action.Value}, 2, io.Discard)
	var ids []string
	for _, name := range []string{"one", "two", "three"} {
		response := fileRequest(h, "POST", "/v1/files", testToken, []byte(name), map[string]string{"X-File-Name": name})
		var saved struct{ File artifact.Metadata }
		if err := json.Unmarshal(response.Body.Bytes(), &saved); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, saved.File.ID)
	}
	page := fileRequest(h, "GET", "/v1/files?limit=1&before="+ids[2], testToken, nil, nil)
	var listing struct{ Files []artifact.Metadata }
	json.Unmarshal(page.Body.Bytes(), &listing)
	if page.Code != 200 || len(listing.Files) != 1 || listing.Files[0].ID != ids[1] {
		t.Fatal("incorrect file cursor")
	}
	def := workflow.Definition{ID: "file-user", Version: 1, Steps: []workflow.Step{{ID: "echo", Action: "value", Input: map[string]any{"file": map[string]any{"$ref": "/input/file"}}}}}
	send(t, h, "POST", "/v1/workflows", def, adminToken, 201)
	run := send(t, h, "POST", "/v1/runs", map[string]any{"workflow_id": def.ID, "input": map[string]any{"file": map[string]any{"$artifact": ids[0]}}}, testToken, 200)
	if !strings.Contains(run.Body.String(), ids[0]) {
		t.Fatal("reference lost")
	}
	send(t, h, "POST", "/v1/runs", map[string]any{"workflow_id": def.ID, "input": map[string]any{"file": map[string]any{"$artifact": identity.New()}}}, testToken, 404)
	if !s.Ready() {
		t.Fatal("bad file input stopped service")
	}
	if fileRequest(h, "DELETE", "/v1/files/"+ids[0], adminToken, nil, nil).Code != 409 {
		t.Fatal("input pin missing")
	}
	if fileRequest(h, "GET", "/v1/admin/backup", testToken, nil, nil).Code != 403 {
		t.Fatal("runner downloaded database")
	}
	backup := fileRequest(h, "GET", "/v1/admin/backup", adminToken, nil, nil)
	sum := sha256.Sum256(backup.Body.Bytes())
	if backup.Code != 200 || backup.Header().Get("X-Content-SHA256") != hex.EncodeToString(sum[:]) {
		t.Fatal("invalid backup response")
	}
	if fileRequest(h, "GET", "/v1/runs?before=invalid", testToken, nil, nil).Code != 400 {
		t.Fatal("invalid run cursor accepted")
	}
}
