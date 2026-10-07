package orchestrator

import (
	"sort"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

// Validate compiles without publishing or invoking an action.
func (s *Service) Validate(def workflow.Definition) (workflow.Description, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.storageFailed {
		return workflow.Description{}, ErrUnavailable
	}
	copied, _, err := copyDefinition(def)
	if err != nil {
		return workflow.Description{}, err
	}
	plan, err := workflow.Compile(copied, s.registry, s.options.Timeout, s.resolve)
	if err != nil {
		return workflow.Description{}, err
	}
	err = s.db.View(func(tx *bolt.Tx) error { return artifact.ValidateReferences(tx, "", copied) })
	if err != nil {
		return workflow.Description{}, err
	}
	return plan.Describe(), nil
}

func (s *Service) Capabilities() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	actions := []string{"workflow"}
	for name := range s.registry {
		if name != "workflow" {
			actions = append(actions, name)
		}
	}
	sort.Strings(actions)
	return map[string]any{
		"spec_versions": []string{"1.0"}, "schema_url": "/v1/schema", "actions": actions,
		"execution_modes": []string{"dag", "sequential"}, "recovery_modes": []string{"manual", "resume"},
		"limits":     map[string]any{"direct_steps": workflow.MaxSteps, "expanded_steps": workflow.MaxExpandedSteps, "depth": workflow.MaxDepth, "input_bytes": workflow.MaxDataBytes, "result_bytes": workflow.MaxResultBytes, "checkpoint_bytes": workflow.MaxCheckpointBytes, "run_timeout_seconds": int(s.options.Timeout.Seconds()), "resumes_per_run": MaxResumes, "history_events_per_run": MaxHistoryEvents, "workers": s.options.Workers, "active_runs": s.options.MaxRuns, "queued_runs": s.options.QueueSize},
		"guarantees": map[string]string{"database": "atomic local transactions", "remote_effects": "provider idempotency or reconciliation required for uncertain outcomes", "recovery": "completed steps retained; unsafe uncertain steps require an admin decision"},
		"endpoints":  map[string]string{"validate": "/v1/workflows/validate", "publish": "/v1/workflows", "submit": "/v1/runs", "events": "/v1/events", "files": "/v1/files", "metrics": "/v1/metrics"},
	}
}
