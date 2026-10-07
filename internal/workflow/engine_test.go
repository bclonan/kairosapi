package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func compileTest(t *testing.T, steps []Step, handler Handler) *Plan {
	t.Helper()
	plan, err := Compile(Definition{ID: "test", Version: 1, Steps: steps}, Registry{
		"test": func(json.RawMessage) (Handler, error) { return handler, nil },
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func engineTest(t *testing.T, workers, runs int) *Engine {
	t.Helper()
	engine, err := New(workers, runs)
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for test worker")
		var zero T
		return zero
	}
}

func TestParallelDependenciesAndRepeatedActions(t *testing.T) {
	started, release := make(chan string, 2), make(chan struct{})
	plan := compileTest(t, []Step{
		{ID: "left", Action: "test", Input: map[string]any{"name": "left"}},
		{ID: "right", Action: "test", Input: map[string]any{"name": "right"}},
		{ID: "join", Action: "test", DependsOn: []string{"left", "right"}, Input: map[string]any{
			"left":   map[string]any{"$ref": "/steps/left/name"},
			"right":  map[string]any{"$ref": "/steps/right/name"},
			"number": map[string]any{"$ref": "/input/number"},
		}},
	}, func(ctx context.Context, input map[string]any) (any, error) {
		if name, ok := input["name"].(string); ok {
			started <- name
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return input, nil
	})
	engine := engineTest(t, 2, 1)
	finished := make(chan Result, 1)
	go func() {
		result, _ := engine.Run(context.Background(), plan, map[string]any{"number": json.Number("9007199254740993")})
		finished <- result
	}()
	first, second := receive(t, started), receive(t, started)
	if first == second {
		t.Fatal("repeated action instances shared their input")
	}
	close(release)
	result := receive(t, finished)
	if result.State != "succeeded" {
		t.Fatalf("%+v", result)
	}
	joined := result.Steps[2].Output.(map[string]any)
	if joined["left"] != "left" || joined["right"] != "right" || joined["number"] != json.Number("9007199254740993") {
		t.Fatalf("incorrect dependency results: %#v", joined)
	}
}

func TestGlobalConcurrencyLimitAcrossRuns(t *testing.T) {
	var active, peak atomic.Int32
	started, release := make(chan struct{}, 4), make(chan struct{})
	plan := compileTest(t, []Step{{ID: "work", Action: "test"}}, func(ctx context.Context, _ map[string]any) (any, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old && !peak.CompareAndSwap(old, current); old = peak.Load() {
		}
		started <- struct{}{}
		select {
		case <-release:
			return "ok", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	engine := engineTest(t, 2, 4)
	finished := make(chan Result, 4)
	for range 4 {
		go func() { result, _ := engine.Run(context.Background(), plan, nil); finished <- result }()
	}
	receive(t, started)
	receive(t, started)
	close(release)
	for range 4 {
		if result := receive(t, finished); result.State != "succeeded" {
			t.Fatalf("%+v", result)
		}
	}
	if peak.Load() != 2 || active.Load() != 0 {
		t.Fatalf("peak=%d active=%d", peak.Load(), active.Load())
	}
}

func TestCapacityCancellationAndSlotReuse(t *testing.T) {
	started := make(chan struct{}, 1)
	plan := compileTest(t, []Step{{ID: "work", Action: "test"}}, func(ctx context.Context, _ map[string]any) (any, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	engine := engineTest(t, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan Result, 1)
	go func() { result, _ := engine.Run(ctx, plan, nil); finished <- result }()
	receive(t, started)
	if _, err := engine.Run(context.Background(), plan, nil); !errors.Is(err, ErrBusy) {
		t.Fatalf("got %v", err)
	}
	cancel()
	if result := receive(t, finished); result.State != "canceled" || result.Steps[0].State != "canceled" {
		t.Fatalf("%+v", result)
	}
	result, err := engine.Run(ctx, plan, nil)
	if err != nil || result.State != "canceled" || result.Steps[0].State != "skipped" {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestFailureAndPanicStopDependentEffects(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panics], func(t *testing.T) {
			var calls atomic.Int32
			plan := compileTest(t, []Step{{ID: "bad", Action: "test"}, {ID: "dependent", Action: "test", DependsOn: []string{"bad"}}},
				func(context.Context, map[string]any) (any, error) {
					calls.Add(1)
					if panics {
						panic("do not leak this secret")
					}
					return nil, errors.New("failed")
				})
			result, err := engineTest(t, 2, 1).Run(context.Background(), plan, nil)
			if err != nil || result.State != "failed" || result.Steps[1].State != "skipped" || calls.Load() != 1 {
				t.Fatalf("%+v %v", result, err)
			}
			if strings.Contains(result.Steps[0].Error, "secret") {
				t.Fatal("panic content leaked")
			}
		})
	}
}

func TestFailureCancelsRunningSiblingAndWaitsForExit(t *testing.T) {
	started, exited := make(chan struct{}), make(chan struct{})
	plan := compileTest(t, []Step{
		{ID: "slow", Action: "test", Input: map[string]any{"slow": true}},
		{ID: "bad", Action: "test"},
	}, func(ctx context.Context, input map[string]any) (any, error) {
		if input["slow"] == true {
			close(started)
			<-ctx.Done()
			close(exited)
			return nil, ctx.Err()
		}
		select {
		case <-started:
			return nil, errors.New("failed")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	result, err := engineTest(t, 2, 1).Run(context.Background(), plan, nil)
	if err != nil || result.State != "failed" {
		t.Fatalf("%+v %v", result, err)
	}
	select {
	case <-exited:
	default:
		t.Fatal("engine returned before sibling exited")
	}
}

func TestTimeouts(t *testing.T) {
	for _, runDeadline := range []bool{false, true} {
		plan := compileTest(t, []Step{{ID: "wait", Action: "test"}}, func(ctx context.Context, _ map[string]any) (any, error) { <-ctx.Done(); return nil, ctx.Err() })
		if runDeadline {
			plan.timeout = 20 * time.Millisecond
		} else {
			plan.steps[0].timeout = 20 * time.Millisecond
		}
		result, err := engineTest(t, 1, 1).Run(context.Background(), plan, nil)
		want := "failed"
		if runDeadline {
			want = "timed_out"
		}
		if err != nil || result.State != want || result.Steps[0].State != "timed_out" {
			t.Fatalf("%+v %v", result, err)
		}
	}
}

func TestCompileRejectsInvalidGraphsBeforeActions(t *testing.T) {
	cases := map[string][]Step{
		"empty":                {},
		"duplicate":            {{ID: "a", Action: "test"}, {ID: "a", Action: "test"}},
		"unknown action":       {{ID: "a", Action: "missing"}},
		"unknown dependency":   {{ID: "a", Action: "test", DependsOn: []string{"b"}}},
		"cycle":                {{ID: "a", Action: "test", DependsOn: []string{"b"}}, {ID: "b", Action: "test", DependsOn: []string{"a"}}},
		"undeclared reference": {{ID: "a", Action: "test", Input: map[string]any{"x": map[string]any{"$ref": "/steps/b"}}}},
		"bad reference":        {{ID: "a", Action: "test", Input: map[string]any{"x": map[string]any{"$ref": "/input/bad~2"}}}},
		"timeout":              {{ID: "a", Action: "test", TimeoutSeconds: 2}},
	}
	for name, steps := range cases {
		t.Run(name, func(t *testing.T) {
			registry := Registry{"test": func(json.RawMessage) (Handler, error) {
				return func(context.Context, map[string]any) (any, error) {
					t.Fatal("action executed during validation")
					return nil, nil
				}, nil
			}}
			if _, err := Compile(Definition{ID: "test", Version: 1, Steps: steps}, registry, time.Second); err == nil {
				t.Fatal("accepted invalid graph")
			}
		})
	}
}

func TestInputsAreIsolatedAndReferencesAreData(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	plan := compileTest(t, []Step{
		{ID: "left", Action: "test", Input: map[string]any{"id": "left", "data": map[string]any{"$ref": "/input"}}},
		{ID: "right", Action: "test", Input: map[string]any{"id": "right", "data": map[string]any{"$ref": "/input"}}},
	}, func(_ context.Context, args map[string]any) (any, error) {
		data := args["data"].(map[string]any)
		mu.Lock()
		seen[args["id"].(string)] = data["name"].(string)
		mu.Unlock()
		data["name"] = args["id"]
		return data, nil
	})
	input := map[string]any{"name": "original", "$ref": "/steps/untrusted"}
	result, err := engineTest(t, 2, 1).Run(context.Background(), plan, input)
	if err != nil || result.State != "succeeded" || input["name"] != "original" || seen["left"] != "original" || seen["right"] != "original" {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestMissingReferencesAndOutputLimits(t *testing.T) {
	plan := compileTest(t, []Step{{ID: "a", Action: "test", Input: map[string]any{"x": map[string]any{"$ref": "/input/missing"}}}}, func(context.Context, map[string]any) (any, error) {
		t.Fatal("called with missing input")
		return nil, nil
	})
	result, _ := engineTest(t, 1, 1).Run(context.Background(), plan, nil)
	if result.State != "failed" {
		t.Fatalf("%+v", result)
	}
	plan = compileTest(t, []Step{{ID: "a", Action: "test"}}, func(context.Context, map[string]any) (any, error) { return strings.Repeat("x", MaxDataBytes), nil })
	result, _ = engineTest(t, 1, 1).Run(context.Background(), plan, nil)
	if result.State != "failed" || result.Steps[0].Output != nil {
		t.Fatalf("%+v", result)
	}
}
