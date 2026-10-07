package api

import (
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/orchestrator"
)

func transferDeadline(w http.ResponseWriter) {
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(time.Minute))
	_ = controller.SetWriteDeadline(time.Now().Add(2 * time.Minute))
}

func (s *Server) uploadFile(w http.ResponseWriter, r *http.Request) {
	if !s.service.Ready() {
		failure(w, orchestrator.ErrUnavailable)
		return
	}
	transferDeadline(w)
	defer r.Body.Close()
	if r.ContentLength > s.service.Files().Limits().MaxBytes {
		failure(w, artifact.ErrTooLarge)
		return
	}
	meta, duplicate, err := s.service.Files().Put(r.Context(), r.Body, artifact.Metadata{Name: r.Header.Get("X-File-Name"), MediaType: r.Header.Get("Content-Type"), ContentEncoding: r.Header.Get("Content-Encoding")}, r.Header.Get("X-Content-SHA256"), r.Header.Get("Idempotency-Key"), "")
	if err != nil {
		failure(w, err)
		return
	}
	status := 201
	if duplicate {
		status = 200
		w.Header().Set("Idempotency-Replayed", "true")
	}
	w.Header().Set("Location", "/v1/files/"+meta.ID)
	writeJSON(w, status, map[string]any{"file": meta, "reference": meta.Reference(), "duplicate": duplicate})
}

func (s *Server) listFiles(w http.ResponseWriter, r *http.Request) {
	limit := 25
	if value := r.URL.Query().Get("limit"); value != "" {
		var err error
		limit, err = strconv.Atoi(value)
		if err != nil || limit < 1 || limit > 100 {
			writeJSON(w, 400, map[string]string{"error": "limit must be between 1 and 100"})
			return
		}
	}
	before := r.URL.Query().Get("before")
	if before != "" && !identity.Valid(before) {
		writeJSON(w, 400, map[string]string{"error": "before must be a canonical ULID"})
		return
	}
	files, err := s.service.Files().List(limit, before)
	if err != nil {
		failure(w, err)
		return
	}
	next := ""
	if len(files) == limit {
		next = files[len(files)-1].ID
	}
	writeJSON(w, 200, map[string]any{"files": files, "next_before": next})
}

func (s *Server) fileMetadata(w http.ResponseWriter, r *http.Request) {
	meta, err := s.service.Files().Get(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, meta)
}

func (s *Server) downloadFile(w http.ResponseWriter, r *http.Request) {
	transferDeadline(w)
	meta, reader, err := s.service.Files().Read(r.Context(), r.PathValue("id"), "")
	if err != nil {
		failure(w, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", meta.MediaType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": meta.Name}))
	w.Header().Set("ETag", `"`+meta.SHA256+`"`)
	w.Header().Set("X-Content-SHA256", meta.SHA256)
	if meta.ContentEncoding != "" {
		w.Header().Set("X-File-Content-Encoding", meta.ContentEncoding)
	}
	http.ServeContent(w, r, meta.Name, meta.CreatedAt, reader)
}

func (s *Server) deleteFile(w http.ResponseWriter, r *http.Request) {
	if !s.service.Ready() {
		failure(w, orchestrator.ErrUnavailable)
		return
	}
	if err := s.service.Files().Delete(r.PathValue("id")); err != nil {
		failure(w, err)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	if !s.service.Ready() {
		failure(w, orchestrator.ErrUnavailable)
		return
	}
	transferDeadline(w)
	reader, size, digest, err := s.service.Files().Snapshot(r.Context())
	if err != nil {
		failure(w, err)
		return
	}
	defer reader.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="kairos.db"`)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.Header().Set("X-Content-SHA256", digest)
	http.ServeContent(w, r, "kairos.db", time.Time{}, reader)
}
