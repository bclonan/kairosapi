package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const MaxCheckpointBytes = 16 << 10

type actionProgressKey struct{}
type actionProgress struct {
	mu     sync.Mutex
	result *StepResult
	save   func(StepResult) error
}

// LoadCheckpoint returns a copy of the last durable action checkpoint.
func LoadCheckpoint(ctx context.Context) (map[string]any, bool) {
	progress, _ := ctx.Value(actionProgressKey{}).(*actionProgress)
	if progress == nil {
		return nil, false
	}
	progress.mu.Lock()
	defer progress.mu.Unlock()
	if len(progress.result.Checkpoint) == 0 {
		return nil, false
	}
	value, _ := copyCheckpoint(progress.result.Checkpoint)
	return value, true
}

// SaveCheckpoint persists before returning in tracked execution.
// Standalone handlers without an engine can call it as a no-op.
func SaveCheckpoint(ctx context.Context, value map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	copied, err := copyCheckpoint(value)
	if err != nil {
		return err
	}
	progress, _ := ctx.Value(actionProgressKey{}).(*actionProgress)
	if progress == nil {
		return nil
	}
	progress.mu.Lock()
	defer progress.mu.Unlock()
	progress.result.Checkpoint = copied
	return progress.save(*progress.result)
}

// ReportProgress records a short phase name without request or response payloads.
func ReportProgress(ctx context.Context, phase string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(phase) > 64 || strings.ContainsAny(phase, "\r\n\x00") {
		return errors.New("invalid action phase")
	}
	progress, _ := ctx.Value(actionProgressKey{}).(*actionProgress)
	if progress == nil {
		return nil
	}
	progress.mu.Lock()
	defer progress.mu.Unlock()
	progress.result.Phase = phase
	return progress.save(*progress.result)
}

func copyCheckpoint(value map[string]any) (map[string]any, error) {
	data, err := json.Marshal(value)
	if err != nil || len(data) > MaxCheckpointBytes {
		return nil, errors.New("action checkpoint exceeds 16 KiB")
	}
	var copied map[string]any
	if err := Decode(data, &copied); err != nil {
		return nil, err
	}
	budget := MaxCheckpointBytes
	if _, err := resolve(copied, nil, &budget, 0); err != nil {
		return nil, err
	}
	return copied, nil
}

func (p *Plan) ResumesAutomatically() bool         { return p.execution.Recovery == "resume" }
func (p *Plan) Deadline(start time.Time) time.Time { return start.Add(p.timeout) }

type Resolution struct {
	Outcome string `json:"outcome"`
	Output  any    `json:"output,omitempty"`
}

// PrepareResume preserves recorded success and rejects an uncertain effect
// unless its caller supplies a reconciliation decision.
func (p *Plan) PrepareResume(previous Result, resolutions map[string]Resolution) (Result, error) {
	data, err := json.Marshal(previous)
	if err != nil || len(data) > MaxResultBytes {
		return Result{}, errors.New("invalid saved result")
	}
	var result Result
	if err := Decode(data, &result); err != nil {
		return Result{}, err
	}
	if result.WorkflowID != p.ID || result.Version != p.Version {
		return Result{}, errors.New("checkpoint workflow mismatch")
	}
	used := map[string]bool{}
	steps, err := p.resumeSteps(result.Steps, "", result.ID, resolutions, used)
	if err != nil {
		return Result{}, err
	}
	for path := range resolutions {
		if !used[path] {
			return Result{}, fmt.Errorf("resolution does not identify unfinished leaf %q", path)
		}
	}
	result.Steps, result.State, result.Output, result.Error = steps, "queued", nil, ""
	result.FinishedAt = time.Time{}
	return result, nil
}

func (p *Plan) resumeSteps(saved []StepResult, prefix, runID string, resolutions map[string]Resolution, used map[string]bool) ([]StepResult, error) {
	if len(saved) != 0 && len(saved) != len(p.steps) {
		return nil, errors.New("checkpoint step count mismatch")
	}
	out := make([]StepResult, len(p.steps))
	for i, compiled := range p.steps {
		path := prefix + compiled.step.ID
		step := StepResult{ID: compiled.step.ID, State: "pending", OperationID: runID + "/" + path}
		if len(saved) > 0 {
			step = saved[i]
			if step.ID != compiled.step.ID {
				return nil, errors.New("checkpoint step identity mismatch")
			}
			step.OperationID = runID + "/" + path
		}
		if step.State == "succeeded" || (step.State == "skipped" && step.SkipReason == "condition") {
			out[i] = step
			continue
		}
		if compiled.child != nil {
			children, err := compiled.child.resumeSteps(step.Children, path+"/", runID, resolutions, used)
			if err != nil {
				return nil, err
			}
			step.Children = children
		} else {
			resolution, supplied := resolutions[path]
			if supplied {
				used[path] = true
				switch resolution.Outcome {
				case "succeeded":
					data, err := json.Marshal(resolution.Output)
					if err != nil || len(data) > MaxDataBytes {
						return nil, errors.New("resolution output exceeds data limit")
					}
					if err := Decode(data, &step.Output); err != nil {
						return nil, err
					}
					budget := MaxDataBytes
					if _, err := resolve(step.Output, nil, &budget, 0); err != nil {
						return nil, err
					}
					step.State, step.Error, step.Phase, step.FinishedAt = "succeeded", "", "reconciled", time.Now().UTC()
					step.Checkpoint = nil
					out[i] = step
					continue
				case "retry":
					step.Checkpoint = nil
					step.Phase = "operator_retry"
				default:
					return nil, errors.New("resolution outcome must be succeeded or retry")
				}
			}
			polling := compiled.step.Action == "http" && step.Checkpoint["kind"] == "http_async"
			if url, _ := step.Checkpoint["poll_url"].(string); url == "" {
				polling = false
			}
			pure := compiled.step.Action == "value" || compiled.step.Action == "delay" || compiled.step.Action == "dictionary" || compiled.step.Action == "protobuf"
			if !supplied && step.Attempts > 0 && !pure && !compiled.step.Retry.Idempotent && !polling {
				return nil, fmt.Errorf("step %q needs provider reconciliation before resume", path)
			}
		}
		step.State, step.Error, step.Output, step.SkipReason = "pending", "", nil, ""
		step.FinishedAt = time.Time{}
		out[i] = step
	}
	return out, nil
}
