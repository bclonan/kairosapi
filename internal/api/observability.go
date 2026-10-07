package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/bclonan/kairosapi/internal/identity"
)

func (s *Server) registerObservationRoutes(mux *http.ServeMux) {
	mux.Handle("GET /v1/runs/{id}/history", s.authorize(http.HandlerFunc(s.runHistory), false))
	mux.Handle("GET /v1/runs/{id}/stream", s.authorize(http.HandlerFunc(s.runStream), false))
	mux.Handle("GET /v1/metrics", s.authorize(http.HandlerFunc(s.metrics), false))
}

func historyArguments(w http.ResponseWriter, r *http.Request) (string, int, bool) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			writeJSON(w, 400, map[string]string{"error": "limit must be between 1 and 100"})
			return "", 0, false
		}
		limit = value
	}
	after := r.URL.Query().Get("after")
	if after == "" {
		after = r.Header.Get("Last-Event-ID")
	}
	if after != "" && !identity.Valid(after) {
		writeJSON(w, 400, map[string]string{"error": "after must be a canonical ULID"})
		return "", 0, false
	}
	return after, limit, true
}

func (s *Server) runHistory(w http.ResponseWriter, r *http.Request) {
	after, limit, ok := historyArguments(w, r)
	if !ok {
		return
	}
	page, err := s.service.History(r.PathValue("id"), after, limit)
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, page)
}

// Streams close after 25 seconds so the server's normal write deadline remains
// in force. Clients reconnect with Last-Event-ID or after to replay committed data.
func (s *Server) runStream(w http.ResponseWriter, r *http.Request) {
	after, limit, ok := historyArguments(w, r)
	if !ok {
		return
	}
	page, err := s.service.History(r.PathValue("id"), after, limit)
	if err != nil {
		failure(w, err)
		return
	}
	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	send := func(kind, id string, value any) bool {
		body, err := json.Marshal(value)
		if err != nil {
			return false
		}
		if id != "" {
			if _, err := fmt.Fprintf(w, "id: %s\n", id); err != nil {
				return false
			}
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, body); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	for {
		if page.Truncated {
			if !send("gap", "", map[string]string{"dropped_through": page.DroppedThrough}) {
				return
			}
		}
		for _, event := range page.Events {
			if !send("transition", event.ID, event) {
				return
			}
			after = event.ID
		}
		if !page.HasMore && isTerminal(page.State) {
			send("complete", "", map[string]string{"state": page.State, "after": after})
			return
		}
		if !page.HasMore {
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil || controller.Flush() != nil {
				return
			}
		}
		wait, stop := context.WithTimeout(ctx, 10*time.Second)
		page, err = s.service.WaitHistory(wait, r.PathValue("id"), after, limit)
		stop()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			send("unavailable", "", map[string]string{"error": "journal unavailable; reconnect and inspect the run"})
			return
		}
	}
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	metrics, err := s.service.Metrics()
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, metrics)
}
