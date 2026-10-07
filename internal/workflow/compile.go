package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

var identifier = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,63}$`)

// Decode rejects unknown fields and trailing values and preserves JSON numbers.
func Decode(data []byte, target any) error {
	if err := checkJSON(data); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}

func Compile(def Definition, registry Registry, maxTimeout time.Duration, resolvers ...Resolver) (*Plan, error) {
	if def.SpecVersion != "" && def.SpecVersion != "1.0" {
		return nil, errors.New("unsupported spec_version; expected 1.0")
	}
	if !identifier.MatchString(def.ID) || def.Version < 1 {
		return nil, errors.New("workflow needs a valid id and positive version")
	}
	if len(def.Steps) < 1 || len(def.Steps) > MaxSteps {
		return nil, fmt.Errorf("workflow must contain 1 to %d steps", MaxSteps)
	}
	if maxTimeout < time.Second || maxTimeout > 5*time.Minute {
		return nil, errors.New("maximum run timeout must be between 1s and 5m")
	}
	timeout, err := duration(def.TimeoutSeconds, maxTimeout)
	if err != nil {
		return nil, err
	}
	// Copy JSON before publishing the plan. Callers cannot change its inputs later.
	data, err := json.Marshal(def)
	if err != nil || len(data) > 1<<20 {
		return nil, errors.New("invalid or oversized workflow")
	}
	var copied Definition
	if err := Decode(data, &copied); err != nil {
		return nil, err
	}
	def = copied
	if err := resolveResources(&def); err != nil {
		return nil, err
	}
	if def.Execution.Mode != "" && def.Execution.Mode != "dag" && def.Execution.Mode != "sequential" {
		return nil, errors.New("execution mode must be dag or sequential")
	}
	if def.Execution.Recovery != "" && def.Execution.Recovery != "manual" && def.Execution.Recovery != "resume" {
		return nil, errors.New("execution recovery must be manual or resume")
	}
	if def.Execution.Mode == "sequential" {
		for i := range def.Steps {
			for j := 0; j < i; j++ {
				found := false
				for _, dep := range def.Steps[i].DependsOn {
					if dep == def.Steps[j].ID {
						found = true
					}
				}
				if !found {
					def.Steps[i].DependsOn = append(def.Steps[i].DependsOn, def.Steps[j].ID)
				}
			}
		}
	}
	if err := validateTriggers(def.Triggers); err != nil {
		return nil, err
	}
	byID := make(map[string]Step, len(def.Steps))
	for _, step := range def.Steps {
		if !identifier.MatchString(step.ID) {
			return nil, errors.New("invalid step id")
		}
		if _, exists := byID[step.ID]; exists {
			return nil, fmt.Errorf("duplicate step %q", step.ID)
		}
		byID[step.ID] = step
	}
	for _, step := range def.Steps {
		deps := map[string]bool{}
		for _, dep := range step.DependsOn {
			if _, exists := byID[dep]; !exists || dep == step.ID || deps[dep] {
				return nil, fmt.Errorf("step %q has an invalid dependency", step.ID)
			}
			deps[dep] = true
		}
		if err := validateReferences(step.Input, deps, 0); err != nil {
			return nil, fmt.Errorf("step %q: %w", step.ID, err)
		}
		if err := validateCondition(step.When, deps, 0); err != nil {
			return nil, fmt.Errorf("step %q: %w", step.ID, err)
		}
		if step.Retry.MaxAttempts < 0 || step.Retry.MaxAttempts > 5 || step.Retry.BackoffMS < 0 || step.Retry.BackoffMS > 30000 {
			return nil, errors.New("retry supports up to 5 attempts and a backoff of 0 to 30000 ms")
		}
		if step.Retry.MaxAttempts > 1 && !step.Retry.Idempotent {
			return nil, errors.New("retrying a step requires an explicit idempotent declaration")
		}
		if step.Action == "workflow" && step.Retry.MaxAttempts > 1 {
			return nil, errors.New("configure retries on child actions, not the whole subflow")
		}
	}
	allSteps := map[string]bool{}
	for id := range byID {
		allSteps[id] = true
	}
	if err := validateReferences(def.Output, allSteps, 0); err != nil {
		return nil, fmt.Errorf("output: %w", err)
	}
	visited, visiting := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visiting[id] {
			return errors.New("workflow contains a dependency cycle")
		}
		if visited[id] {
			return nil
		}
		visiting[id] = true
		for _, dep := range byID[id].DependsOn {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[id], visited[id] = false, true
		return nil
	}
	for _, step := range def.Steps {
		if err := visit(step.ID); err != nil {
			return nil, err
		}
	}
	plan := &Plan{ID: def.ID, Version: def.Version, timeout: timeout, output: def.Output, depth: 1, execution: def.Execution}
	plan.schema, err = compileSchema(def.InputSchema)
	if err != nil {
		return nil, err
	}
	for _, step := range def.Steps {
		stepTimeout, err := duration(step.TimeoutSeconds, timeout)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", step.ID, err)
		}
		compiled := compiledStep{step: step, timeout: stepTimeout}
		if step.Action == "workflow" {
			var reference Reference
			if err := Decode(step.Config, &reference); err != nil || reference.Version < 1 || !identifier.MatchString(reference.WorkflowID) {
				return nil, errors.New("workflow action requires workflow_id and a pinned positive version")
			}
			if len(resolvers) != 1 || resolvers[0] == nil {
				return nil, errors.New("subflow resolver is unavailable")
			}
			compiled.child, err = resolvers[0](reference)
			if err != nil {
				return nil, fmt.Errorf("step %q: %w", step.ID, err)
			}
			if compiled.child == nil || len(compiled.child.output) == 0 {
				return nil, errors.New("a reusable subflow must declare output")
			}
			plan.expanded += compiled.child.expanded
			plan.depth = max(plan.depth, compiled.child.depth+1)
		} else {
			factory := registry[step.Action]
			if factory == nil {
				return nil, fmt.Errorf("step %q: unknown action %q", step.ID, step.Action)
			}
			compiled.handler, err = factory(step.Config)
			if err != nil {
				return nil, fmt.Errorf("step %q: %w", step.ID, err)
			}
			if compiled.handler == nil {
				return nil, fmt.Errorf("step %q: action has no handler", step.ID)
			}
		}
		plan.expanded++
		if plan.expanded > MaxExpandedSteps || plan.depth > MaxDepth {
			return nil, errors.New("subflow expansion exceeds the step or nesting limit")
		}
		plan.steps = append(plan.steps, compiled)
	}
	return plan, nil
}

func duration(seconds int, maximum time.Duration) (time.Duration, error) {
	if seconds == 0 {
		return maximum, nil
	}
	if seconds < 1 || seconds > int(maximum/time.Second) {
		return 0, errors.New("timeout exceeds configured limit")
	}
	return time.Duration(seconds) * time.Second, nil
}

func validateReferences(value any, deps map[string]bool, depth int) error {
	if depth > 32 {
		return errors.New("input exceeds maximum nesting depth")
	}
	switch value := value.(type) {
	case map[string]any:
		if ref, exists := value["$ref"]; exists {
			path, ok := ref.(string)
			_, hasDefault := value["default"]
			if !ok || (len(value) != 1 && !(len(value) == 2 && hasDefault)) {
				return errors.New("a reference supports a string $ref and an optional literal default")
			}
			parts, err := pointerParts(path)
			if err != nil || len(parts) == 0 {
				return errors.New("invalid reference path")
			}
			if parts[0] == "input" {
				return nil
			}
			if parts[0] == "run" && len(parts) == 2 && (parts[1] == "id" || parts[1] == "workflow_id" || parts[1] == "version") {
				return nil
			}
			if len(parts) < 2 || parts[0] != "steps" || !deps[parts[1]] {
				return errors.New("step references require an explicit depends_on entry")
			}
			return nil
		}
		if literal, exists := value["$literal"]; exists {
			if len(value) != 1 {
				return errors.New("$literal must be the only key")
			}
			budget := MaxDataBytes
			_, err := resolve(literal, nil, &budget, depth+1)
			return err
		}
		if joined, exists := value["$concat"]; exists {
			items, ok := joined.([]any)
			if !ok || len(value) != 1 || len(items) > 64 {
				return errors.New("$concat requires an array of up to 64 values")
			}
			return validateReferences(items, deps, depth+1)
		}
		for _, child := range value {
			if err := validateReferences(child, deps, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range value {
			if err := validateReferences(child, deps, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func pointerParts(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("JSON pointer must start with a slash")
	}
	parts := strings.Split(pointer[1:], "/")
	for i, part := range parts {
		for j := 0; j < len(part); j++ {
			if part[j] == '~' {
				j++
				if j >= len(part) || (part[j] != '0' && part[j] != '1') {
					return nil, errors.New("invalid JSON pointer escape")
				}
			}
		}
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
	}
	return parts, nil
}
