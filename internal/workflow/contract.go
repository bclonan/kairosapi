package workflow

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bclonan/kairosapi/internal/identity"
)

//go:embed specification.schema.json
var specificationSchema []byte

func SpecificationSchema() json.RawMessage {
	return append(json.RawMessage(nil), specificationSchema...)
}

type PlanStep struct {
	ID             string     `json:"id"`
	Action         string     `json:"action"`
	DependsOn      []string   `json:"depends_on"`
	TimeoutSeconds int        `json:"timeout_seconds"`
	Child          *Reference `json:"child,omitempty"`
}
type Description struct {
	WorkflowID     string     `json:"workflow_id"`
	Version        int        `json:"version"`
	Mode           string     `json:"mode"`
	Recovery       string     `json:"recovery"`
	ExpandedSteps  int        `json:"expanded_steps"`
	Depth          int        `json:"depth"`
	TimeoutSeconds int        `json:"timeout_seconds"`
	Steps          []PlanStep `json:"steps"`
}

func (p *Plan) Describe() Description {
	mode, recovery := p.execution.Mode, p.execution.Recovery
	if mode == "" {
		mode = "dag"
	}
	if recovery == "" {
		recovery = "manual"
	}
	out := Description{WorkflowID: p.ID, Version: p.Version, Mode: mode, Recovery: recovery, ExpandedSteps: p.expanded, Depth: p.depth, TimeoutSeconds: int(p.timeout.Seconds()), Steps: []PlanStep{}}
	for _, step := range p.steps {
		summary := PlanStep{ID: step.step.ID, Action: step.step.Action, DependsOn: append([]string{}, step.step.DependsOn...), TimeoutSeconds: int(step.timeout.Seconds())}
		if step.child != nil {
			summary.Child = &Reference{WorkflowID: step.child.ID, Version: step.child.Version}
		}
		out.Steps = append(out.Steps, summary)
	}
	return out
}

func resolveResources(def *Definition) error {
	if len(def.Resources) > 32 {
		return errors.New("a definition supports at most 32 named resources")
	}
	for name, ref := range def.Resources {
		id, ok := ref["$artifact"].(string)
		if !identifier.MatchString(name) || !ok || !identity.Valid(id) {
			return errors.New("resources need valid names and artifact ULIDs")
		}
		if _, reserved := ref["$resource"]; reserved {
			return errors.New("resources cannot reference another resource alias")
		}
	}
	var expand func(any, int) (any, error)
	expand = func(value any, depth int) (any, error) {
		if depth > 32 {
			return nil, errors.New("resource configuration exceeds depth limit")
		}
		switch node := value.(type) {
		case map[string]any:
			if name, present := node["$resource"]; present {
				key, ok := name.(string)
				if !ok || len(node) != 1 || def.Resources[key] == nil {
					return nil, errors.New("unknown or invalid resource alias")
				}
				return def.Resources[key], nil
			}
			for key, child := range node {
				resolved, err := expand(child, depth+1)
				if err != nil {
					return nil, err
				}
				node[key] = resolved
			}
		case []any:
			for i, child := range node {
				resolved, err := expand(child, depth+1)
				if err != nil {
					return nil, err
				}
				node[i] = resolved
			}
		}
		return value, nil
	}
	for i := range def.Steps {
		if len(def.Steps[i].Config) == 0 {
			continue
		}
		var config any
		if err := Decode(def.Steps[i].Config, &config); err != nil {
			return err
		}
		resolved, err := expand(config, 0)
		if err != nil {
			return fmt.Errorf("step %q: %w", def.Steps[i].ID, err)
		}
		def.Steps[i].Config, err = json.Marshal(resolved)
		if err != nil {
			return err
		}
	}
	return nil
}
