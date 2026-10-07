package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/identity"
	"github.com/bclonan/kairosapi/internal/orchestrator"
	"github.com/bclonan/kairosapi/internal/workflow"
)

const MaxRequestBytes = 1 << 20

type Server struct {
	service      *orchestrator.Service
	token        [32]byte
	admin        [32]byte
	adminEnabled bool
	logger       *slog.Logger
}

type adminContext struct{}

func New(service *orchestrator.Service, token, adminToken string, logger *slog.Logger) (http.Handler, error) {
	if service == nil || len(token) < 32 || strings.TrimSpace(token) != token || logger == nil {
		return nil, errors.New("API needs an orchestrator, a token of at least 32 characters, and a logger")
	}
	if adminToken != "" && (len(adminToken) < 32 || strings.TrimSpace(adminToken) != adminToken || adminToken == token) {
		return nil, errors.New("admin token must be distinct and contain at least 32 characters")
	}
	s := &Server{service: service, token: sha256.Sum256([]byte(token)), admin: sha256.Sum256([]byte(adminToken)), adminEnabled: adminToken != "", logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !service.Ready() {
			writeJSON(w, 503, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /v1/workflows", s.authorize(http.HandlerFunc(s.list), false))
	mux.Handle("GET /v1/workflows/{id}", s.authorize(http.HandlerFunc(s.definition), true))
	mux.Handle("GET /v1/workflows/{id}/versions/{version}", s.authorize(http.HandlerFunc(s.definition), true))
	mux.Handle("POST /v1/workflows", s.authorize(http.HandlerFunc(s.publish), true))
	mux.Handle("POST /v1/workflows/validate", s.authorize(http.HandlerFunc(s.validate), true))
	mux.Handle("GET /v1/capabilities", s.authorize(http.HandlerFunc(s.capabilities), false))
	mux.Handle("GET /v1/schema", s.authorize(http.HandlerFunc(s.schema), false))
	mux.Handle("POST /v1/runs", s.authorize(http.HandlerFunc(s.run), false))
	mux.Handle("GET /v1/runs", s.authorize(http.HandlerFunc(s.runs), false))
	mux.Handle("GET /v1/runs/{id}", s.authorize(http.HandlerFunc(s.getRun), false))
	mux.Handle("POST /v1/runs/{id}/cancel", s.authorize(http.HandlerFunc(s.cancelRun), false))
	mux.Handle("POST /v1/runs/{id}/resume", s.authorize(http.HandlerFunc(s.resume), true))
	mux.Handle("POST /v1/events", s.authorize(http.HandlerFunc(s.event), false))
	mux.Handle("POST /v1/files", s.authorize(http.HandlerFunc(s.uploadFile), false))
	mux.Handle("GET /v1/files", s.authorize(http.HandlerFunc(s.listFiles), false))
	mux.Handle("GET /v1/files/{id}", s.authorize(http.HandlerFunc(s.fileMetadata), false))
	mux.Handle("GET /v1/files/{id}/content", s.authorize(http.HandlerFunc(s.downloadFile), false))
	mux.Handle("DELETE /v1/files/{id}", s.authorize(http.HandlerFunc(s.deleteFile), true))
	mux.Handle("GET /v1/admin/backup", s.authorize(http.HandlerFunc(s.backup), true))
	s.registerObservationRoutes(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	}), nil
}

func (s *Server) authorize(next http.Handler, adminOnly bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		candidate := sha256.Sum256([]byte(strings.TrimPrefix(header, "Bearer ")))
		isAdmin := subtle.ConstantTimeCompare(s.admin[:], candidate[:]) == 1 && s.adminEnabled
		isRunner := subtle.ConstantTimeCompare(s.token[:], candidate[:]) == 1
		if !strings.HasPrefix(header, "Bearer ") || (!isAdmin && !isRunner) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		if adminOnly && !isAdmin {
			writeJSON(w, 403, map[string]string{"error": "a configured admin token is required"})
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), adminContext{}, isAdmin)))
	})
}

func (s *Server) list(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]any{"workflows": s.service.List()})
}

func (s *Server) definition(w http.ResponseWriter, r *http.Request) {
	version := 0
	if text := r.PathValue("version"); text != "" {
		var err error
		version, err = strconv.Atoi(text)
		if err != nil || version < 1 {
			writeJSON(w, 400, map[string]string{"error": "version must be a positive integer"})
			return
		}
	}
	def, err := s.service.Definition(r.PathValue("id"), version)
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, def)
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var def workflow.Definition
	if !readJSON(w, r, &def, false) {
		return
	}
	created, err := s.service.Publish(def)
	if err != nil {
		failure(w, err)
		return
	}
	status := 200
	if created {
		status = 201
	}
	w.Header().Set("Location", "/v1/workflows/"+def.ID+"/versions/"+strconv.Itoa(def.Version))
	writeJSON(w, status, map[string]any{"id": def.ID, "version": def.Version, "created": created})
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	var request orchestrator.Request
	if !readJSON(w, r, &request, false) {
		return
	}
	if request.Specification != nil && r.Context().Value(adminContext{}) != true {
		writeJSON(w, 403, map[string]string{"error": "inline specifications require an admin token"})
		return
	}
	run, duplicate, err := s.service.Submit(request, r.Header.Get("Idempotency-Key"))
	if err != nil {
		failure(w, err)
		return
	}
	w.Header().Set("Location", "/v1/runs/"+run.ID)
	if duplicate {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	s.logger.Info("run accepted", "run_id", run.ID, "workflow_id", run.WorkflowID, "version", run.Version, "duplicate", duplicate)
	if !request.Async && !isTerminal(run.State) {
		// The queue owns the run. HTTP disconnects and this wait timeout do not cancel it.
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		run, err = s.service.Wait(ctx, run.ID)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			failure(w, err)
			return
		}
	}
	status := 202
	switch run.State {
	case "succeeded":
		status = 200
	case "failed":
		status = 424
	case "timed_out":
		status = 504
	case "canceled":
		status = 408
	case "interrupted":
		status = 409
	}
	writeJSON(w, status, run)
}

func isTerminal(state string) bool { return state != "running" && state != "queued" }

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.service.Get(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, run)
}

func (s *Server) runs(w http.ResponseWriter, r *http.Request) {
	limit := 25
	if text := r.URL.Query().Get("limit"); text != "" {
		var err error
		limit, err = strconv.Atoi(text)
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
	runs, err := s.service.RunsBefore(limit, before)
	if err != nil {
		failure(w, err)
		return
	}
	next := ""
	if len(runs) == limit {
		next = runs[len(runs)-1].SequenceID
	}
	writeJSON(w, 200, map[string]any{"runs": runs, "next_before": next})
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.service.Cancel(r.PathValue("id"))
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, run)
}

func (s *Server) event(w http.ResponseWriter, r *http.Request) {
	var event map[string]any
	if !readJSON(w, r, &event, true) {
		return
	}
	delivery, err := s.service.Emit(event)
	if err != nil {
		failure(w, err)
		return
	}
	status := 202
	if delivery.Duplicate {
		status = 200
	}
	writeJSON(w, status, delivery)
}

func readJSON(w http.ResponseWriter, r *http.Request, target any, allowEvent bool) bool {
	media := strings.ToLower(strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]))
	if media != "application/json" && !(allowEvent && media == "application/cloudevents+json") {
		writeJSON(w, 415, map[string]string{"error": "Content-Type must be application/json or the event JSON media type"})
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxRequestBytes)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, 413, map[string]string{"error": "request exceeds size limit"})
		} else {
			writeJSON(w, 400, map[string]string{"error": "could not read request"})
		}
		return false
	}
	if err := workflow.Decode(body, target); err != nil {
		writeJSON(w, 400, map[string]string{"error": "expected one JSON object with known fields"})
		return false
	}
	return true
}

func failure(w http.ResponseWriter, err error) {
	status := 422
	switch {
	case errors.Is(err, orchestrator.ErrNotFound), errors.Is(err, artifact.ErrNotFound):
		status = 404
	case errors.Is(err, orchestrator.ErrConflict), errors.Is(err, artifact.ErrConflict), errors.Is(err, artifact.ErrReferenced):
		status = 409
	case errors.Is(err, workflow.ErrBusy), errors.Is(err, artifact.ErrBusy):
		status = 429
		w.Header().Set("Retry-After", "1")
	case errors.Is(err, orchestrator.ErrHistoryFull), errors.Is(err, artifact.ErrQuota):
		status = 507
	case errors.Is(err, orchestrator.ErrUnavailable), errors.Is(err, artifact.ErrUnavailable), errors.Is(err, artifact.ErrIntegrity):
		status = 503
	case errors.Is(err, orchestrator.ErrInvalid), errors.Is(err, artifact.ErrInvalid), errors.Is(err, artifact.ErrRead):
		status = 400
	case errors.Is(err, artifact.ErrTooLarge):
		status = 413
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
