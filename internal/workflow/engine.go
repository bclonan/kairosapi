package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bclonan/kairosapi/internal/identity"
)

var ErrBusy = errors.New("run capacity exhausted")
var ErrPersistence = errors.New("could not record run progress")

type Engine struct {
	workers chan struct{}
	runs    chan struct{}
}

func New(workers, maxRuns int) (*Engine, error) {
	if workers < 1 || workers > 128 || maxRuns < 1 || maxRuns > 128 {
		return nil, errors.New("workers and max runs must be between 1 and 128")
	}
	return &Engine{workers: make(chan struct{}, workers), runs: make(chan struct{}, maxRuns)}, nil
}

func NewID() string                  { return identity.New() }
func (e *Engine) ActiveWorkers() int { return len(e.workers) }

type operationContext struct{}

// OperationKey stays unchanged across retries and recovery.
func OperationKey(ctx context.Context) string {
	key, _ := ctx.Value(operationContext{}).(string)
	return key
}
func RunID(ctx context.Context) string {
	key := OperationKey(ctx)
	for i := range key {
		if key[i] == '/' {
			return key[:i]
		}
	}
	return key
}

func (e *Engine) Run(parent context.Context, plan *Plan, input map[string]any) (Result, error) {
	return e.RunTracked(parent, plan, input, NewID(), nil)
}
func (e *Engine) RunTracked(parent context.Context, plan *Plan, input map[string]any, id string, observe Observer) (Result, error) {
	return e.RunTrackedFrom(parent, plan, input, id, Result{}, observe)
}

// RunTrackedFrom accepts a prepared recovery snapshot. Completed steps stay complete.
func (e *Engine) RunTrackedFrom(parent context.Context, plan *Plan, input map[string]any, id string, previous Result, observe Observer) (Result, error) {
	if plan == nil || len(plan.steps) == 0 {
		return Result{}, errors.New("a compiled workflow is required")
	}
	input, err := plan.copyInput(input)
	if err != nil {
		return Result{}, err
	}
	if previous.ID != "" && (previous.ID != id || previous.WorkflowID != plan.ID || previous.Version != plan.Version) {
		return Result{}, errors.New("checkpoint workflow mismatch")
	}
	select {
	case e.runs <- struct{}{}:
		defer func() { <-e.runs }()
	default:
		return Result{}, ErrBusy
	}
	return e.execute(parent, plan, input, id, id, previous, observe)
}

func (e *Engine) execute(parent context.Context, plan *Plan, input map[string]any, id, path string, previous Result, observe Observer) (Result, error) {
	start := previous.StartedAt
	if start.IsZero() {
		start = time.Now().UTC()
	}
	ctx, cancel := context.WithDeadline(parent, start.Add(plan.timeout))
	defer cancel()
	result := Result{ID: id, WorkflowID: plan.ID, Version: plan.Version, State: "running", StartedAt: start, Steps: make([]StepResult, len(plan.steps))}
	outputs := map[string]any{}
	states := map[string]string{}
	root := map[string]any{"input": input, "steps": outputs, "run": map[string]any{"id": id, "workflow_id": plan.ID, "version": json.Number(strconv.Itoa(plan.Version))}}
	if len(previous.Steps) != 0 && len(previous.Steps) != len(plan.steps) {
		return Result{}, errors.New("checkpoint step count mismatch")
	}
	for i, step := range plan.steps {
		saved := StepResult{ID: step.step.ID, State: "pending", OperationID: path + "/" + step.step.ID}
		if len(previous.Steps) > 0 {
			saved = previous.Steps[i]
			if saved.ID != step.step.ID || (saved.State != "pending" && saved.State != "succeeded" && saved.State != "skipped") {
				return Result{}, errors.New("checkpoint must be prepared before execution")
			}
			saved.OperationID = path + "/" + step.step.ID
		}
		result.Steps[i] = saved
		states[saved.ID] = saved.State
		if saved.State == "succeeded" {
			outputs[saved.ID] = saved.Output
		}
	}
	var recordErr error
	record := func() bool {
		if recordErr != nil {
			return false
		}
		if observe != nil && observe(result) != nil {
			recordErr = ErrPersistence
			cancel()
			return false
		}
		return true
	}
	type update struct {
		index int
		step  StepResult
		final bool
		ack   chan error
	}
	updates := make(chan update, len(plan.steps))
	inflight, failed := 0, false
	record()
	for {
		if !failed && ctx.Err() == nil {
			progress := true
			for progress && !failed && ctx.Err() == nil {
				progress = false
				for i, step := range plan.steps {
					if result.Steps[i].State != "pending" {
						continue
					}
					ready := true
					for _, dep := range step.step.DependsOn {
						if states[dep] != "succeeded" && states[dep] != "skipped" {
							ready = false
						}
					}
					if !ready {
						continue
					}
					progress = true
					allowed, err := evaluate(step.step.When, root)
					if err == nil && !allowed {
						result.Steps[i].State, result.Steps[i].SkipReason = "skipped", "condition"
						result.Steps[i].FinishedAt = time.Now().UTC()
						states[step.step.ID] = "skipped"
						record()
						continue
					}
					budget := MaxDataBytes
					var args any
					if err == nil {
						args, err = resolve(step.step.Input, root, &budget, 0)
					}
					arguments, object := args.(map[string]any)
					if err == nil && !object {
						err = errors.New("step input must resolve to an object")
					}
					if err != nil {
						result.Steps[i].State, result.Steps[i].Error, result.Steps[i].FinishedAt = "failed", err.Error(), time.Now().UTC()
						failed = true
						cancel()
						record()
						break
					}
					result.Steps[i].State = "running"
					if result.Steps[i].StartedAt.IsZero() {
						result.Steps[i].StartedAt = time.Now().UTC()
					}
					if !record() {
						break
					}
					inflight++
					go func(index int, compiled compiledStep, args map[string]any, saved StepResult) {
						publish := func(progress StepResult) error {
							// Detach nested slices and maps before crossing goroutines.
							data, err := json.Marshal(progress)
							if err != nil || len(data) > MaxResultBytes {
								return errors.New("step progress exceeds result limit")
							}
							var snapshot StepResult
							if err := Decode(data, &snapshot); err != nil {
								return err
							}
							ack := make(chan error, 1)
							updates <- update{index: index, step: snapshot, ack: ack}
							return <-ack
						}
						completed := e.invoke(ctx, compiled, args, id, path+"/"+compiled.step.ID, saved, publish)
						updates <- update{index: index, step: completed, final: true}
					}(i, step, arguments, result.Steps[i])
				}
			}
		}
		if inflight == 0 {
			break
		}
		changed := <-updates
		result.Steps[changed.index] = changed.step
		var updateErr error
		if encoded, err := json.Marshal(result); err != nil || len(encoded) > MaxResultBytes {
			result.Steps[changed.index].Output = nil
			result.Steps[changed.index].Children = nil
			result.Steps[changed.index].Checkpoint = nil
			result.Steps[changed.index].State = "failed"
			result.Steps[changed.index].Error = "run results exceed 4 MiB limit"
			changed.step = result.Steps[changed.index]
			updateErr = errors.New("run results exceed 4 MiB limit")
			failed = true
			cancel()
		}
		if changed.final {
			inflight--
			states[changed.step.ID] = changed.step.State
			if changed.step.State == "succeeded" {
				outputs[changed.step.ID] = changed.step.Output
			} else if !failed {
				if ctx.Err() == nil {
					failed = true
				}
				cancel()
			}
		}
		if !record() {
			updateErr = ErrPersistence
		}
		if changed.ack != nil {
			changed.ack <- updateErr
		}
	}
	result.State = "succeeded"
	if failed {
		result.State = "failed"
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		result.State = "timed_out"
	} else if ctx.Err() != nil {
		result.State = "canceled"
	}
	for i := range result.Steps {
		if result.Steps[i].State == "pending" {
			result.Steps[i].State, result.Steps[i].SkipReason = "skipped", "blocked"
		}
	}
	if result.State == "succeeded" && len(plan.output) > 0 {
		budget := MaxDataBytes
		output, err := resolve(plan.output, root, &budget, 0)
		if err != nil {
			result.State, result.Error = "failed", "could not resolve declared output"
		} else {
			result.Output = output
			if data, err := json.Marshal(result); err != nil || len(data) > MaxResultBytes {
				result.Output, result.State, result.Error = nil, "failed", "run results exceed 4 MiB limit"
			}
		}
	}
	result.FinishedAt = time.Now().UTC()
	if recordErr != nil {
		result.State, result.Error = "interrupted", ErrPersistence.Error()
	}
	if !record() {
		result.State, result.Error = "interrupted", ErrPersistence.Error()
	}
	return result, recordErr
}

func (e *Engine) invoke(parent context.Context, step compiledStep, input map[string]any, runID, path string, previous StepResult, publish func(StepResult) error) (result StepResult) {
	result = previous
	result.State, result.Error, result.Output, result.OperationID = "running", "", nil, path
	if result.StartedAt.IsZero() {
		result.StartedAt = time.Now().UTC()
	}
	defer func() {
		result.FinishedAt = time.Now().UTC()
		if recover() != nil {
			result.Output, result.State, result.Error = nil, "failed", "action panicked"
		}
	}()
	ctx, cancel := context.WithDeadline(context.WithValue(parent, operationContext{}, path), result.StartedAt.Add(step.timeout))
	defer cancel()
	if step.child != nil {
		childInput, err := step.child.copyInput(input)
		if err != nil {
			result.State, result.Error = "failed", ErrInput.Error()
			return
		}
		result.Attempts++
		result.Phase = "subflow"
		if err := publish(result); err != nil {
			result.State, result.Error = "interrupted", err.Error()
			return
		}
		seed := Result{ID: runID, WorkflowID: step.child.ID, Version: step.child.Version, StartedAt: result.StartedAt, Steps: previous.Children}
		child, err := e.execute(ctx, step.child, childInput, runID, path, seed, func(child Result) error {
			result.Children = child.Steps
			result.Output = child.Output
			return publish(result)
		})
		result.Children, result.Output, result.State = child.Steps, child.Output, child.State
		if err != nil || child.State != "succeeded" {
			result.Error = "subflow did not succeed"
		}
		return
	}
	control := &actionProgress{result: &result, save: publish}
	ctx = context.WithValue(ctx, actionProgressKey{}, control)
	previousAttempts := previous.Attempts
	attempts := max(1, step.step.Retry.MaxAttempts)
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			result.State, result.Error = contextState(ctx.Err()), "step canceled before execution"
			return
		}
		select {
		case e.workers <- struct{}{}:
		case <-ctx.Done():
			result.State, result.Error = contextState(ctx.Err()), "step canceled before execution"
			return
		}
		result.Attempts, result.Phase = previousAttempts+attempt, "executing"
		output, err := func() (any, error) {
			lease := &workerLease{slots: e.workers, held: true}
			defer lease.release()
			// Commit the attempt before calling a handler that can cause an effect.
			if err := publish(result); err != nil {
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			budget := MaxDataBytes
			copied, err := resolve(input, nil, &budget, 0)
			if err != nil {
				return nil, err
			}
			return step.handler(context.WithValue(ctx, workerLeaseKey{}, lease), copied.(map[string]any))
		}()
		if ctx.Err() != nil {
			result.State, result.Error = contextState(ctx.Err()), "step deadline or cancellation reached"
			return
		}
		if err == nil {
			data, err := json.Marshal(output)
			if err != nil || len(data) > MaxDataBytes {
				result.State, result.Error = "failed", fmt.Sprintf("action output must be JSON no larger than %d bytes", MaxDataBytes)
				return
			}
			if err := Decode(data, &result.Output); err != nil {
				result.State, result.Error = "failed", "action returned invalid JSON"
				return
			}
			budget := MaxDataBytes
			if _, err := resolve(result.Output, nil, &budget, 0); err != nil {
				result.Output, result.State, result.Error = nil, "failed", "action output exceeds data limits"
				return
			}
			result.State, result.Phase, result.Error = "succeeded", "complete", ""
			return
		}
		var retryable *ActionError
		if attempt == attempts || !errors.As(err, &retryable) || !retryable.Transient {
			result.State, result.Error = contextState(err), err.Error()
			return
		}
		result.Phase, result.Error = "retry_wait", err.Error()
		if err := publish(result); err != nil {
			result.State, result.Error = "interrupted", err.Error()
			return
		}
		delay := min(30*time.Second, time.Duration(max(100, step.step.Retry.BackoffMS))*time.Millisecond*(1<<uint(attempt-1)))
		delay = max(delay, retryable.RetryAfter)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.State, result.Error = contextState(ctx.Err()), "step canceled during retry backoff"
			return
		case <-timer.C:
		}
	}
	return
}

func contextState(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timed_out"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "failed"
}
