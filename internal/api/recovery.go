package api

import (
	"net/http"

	"github.com/bclonan/kairosapi/internal/orchestrator"
)

func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	var request orchestrator.ResumeRequest
	if !readJSON(w, r, &request, false) {
		return
	}
	run, err := s.service.Resume(r.PathValue("id"), request)
	if err != nil {
		failure(w, err)
		return
	}
	w.Header().Set("Location", "/v1/runs/"+run.ID)
	writeJSON(w, http.StatusAccepted, run)
}
