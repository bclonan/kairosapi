package api

import (
	"github.com/bclonan/kairosapi/internal/workflow"
	"net/http"
)

func (s *Server) validate(w http.ResponseWriter, r *http.Request) {
	var def workflow.Definition
	if !readJSON(w, r, &def, false) {
		return
	}
	plan, err := s.service.Validate(def)
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"valid": true, "plan": plan})
}
func (s *Server) capabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, s.service.Capabilities())
}
func (s *Server) schema(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, workflow.SpecificationSchema())
}
