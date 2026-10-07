package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func valueRegistry() Registry {
	return Registry{"value": func(json.RawMessage) (Handler, error) {
		return func(_ context.Context, input map[string]any) (any, error) { return input, nil }, nil
	}}
}

func TestReusableWorkflowWithOneWorker(t *testing.T) {
	child, err := Compile(Definition{ID: "child", Version: 1, InputSchema: json.RawMessage(`{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}}`),
		Steps:  []Step{{ID: "value", Action: "value", Input: map[string]any{"message": map[string]any{"$concat": []any{"Hello ", map[string]any{"$ref": "/input/name"}}}}}},
		Output: map[string]any{"message": map[string]any{"$ref": "/steps/value/message"}}}, valueRegistry(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := Compile(Definition{ID: "parent", Version: 1, Steps: []Step{
		{ID: "child", Action: "workflow", Config: json.RawMessage(`{"workflow_id":"child","version":1}`), Input: map[string]any{"name": map[string]any{"$ref": "/input/name"}}},
		{ID: "after", Action: "value", DependsOn: []string{"child"}, Input: map[string]any{"message": map[string]any{"$ref": "/steps/child/message"}, "version": map[string]any{"$concat": []any{"v", map[string]any{"$ref": "/run/version"}}}}},
	}, Output: map[string]any{"result": map[string]any{"$ref": "/steps/after"}}}, valueRegistry(), time.Second, func(ref Reference) (*Plan, error) { return child, nil })
	if err != nil {
		t.Fatal(err)
	}
	result, err := engineTest(t, 1, 1).Run(context.Background(), parent, map[string]any{"name": "Ada"})
	if err != nil || result.State != "succeeded" || result.Steps[1].Output.(map[string]any)["message"] != "Hello Ada" || len(result.Steps[0].Children) != 1 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestConditionsSkipAndJoinWithDefault(t *testing.T) {
	def := Definition{ID: "branch", Version: 1, Steps: []Step{
		{ID: "join", Action: "value", DependsOn: []string{"paid", "free"}, Input: map[string]any{
			"paid": map[string]any{"$ref": "/steps/paid/name", "default": "none"}, "free": map[string]any{"$ref": "/steps/free/name", "default": "none"},
			"literal": map[string]any{"$literal": map[string]any{"$ref": "/this/is/data"}},
		}},
		{ID: "paid", Action: "value", When: &Condition{Path: "/input/amount", Op: "gt", Value: json.Number("1e1")}, Input: map[string]any{"name": "paid"}},
		{ID: "free", Action: "value", When: &Condition{Not: &Condition{Path: "/input/amount", Op: "gt", Value: json.Number("10")}}, Input: map[string]any{"name": "free"}},
	}}
	plan, err := Compile(def, valueRegistry(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := engineTest(t, 1, 1).Run(context.Background(), plan, map[string]any{"amount": json.Number("10.00")})
	if err != nil || result.State != "succeeded" || result.Steps[1].State != "skipped" {
		t.Fatalf("%+v %v", result, err)
	}
	joined := result.Steps[0].Output.(map[string]any)
	if joined["paid"] != "none" || joined["free"] != "free" || joined["literal"].(map[string]any)["$ref"] != "/this/is/data" {
		t.Fatalf("%#v", joined)
	}
}

func TestRetryContextAndInputIsolation(t *testing.T) {
	var calls atomic.Int32
	keys := []string{}
	plan := compileTest(t, []Step{{ID: "retry", Action: "test", Retry: Retry{MaxAttempts: 3, Idempotent: true, BackoffMS: 1}, Input: map[string]any{"name": "original"}}}, func(ctx context.Context, input map[string]any) (any, error) {
		keys = append(keys, OperationKey(ctx))
		if input["name"] != "original" {
			t.Error("retry reused mutated input")
		}
		input["name"] = "changed"
		if calls.Add(1) < 3 {
			return nil, &ActionError{Message: "temporary", Transient: true}
		}
		return map[string]any{"ok": true}, nil
	})
	result, err := engineTest(t, 1, 1).Run(context.Background(), plan, nil)
	if err != nil || result.State != "succeeded" || result.Steps[0].Attempts != 3 || len(keys) != 3 || keys[0] == "" || keys[0] != keys[2] {
		t.Fatalf("%+v %v %v", result, err, keys)
	}
	plan.steps[0].handler = func(context.Context, map[string]any) (any, error) { return nil, errors.New("permanent") }
	result, _ = engineTest(t, 1, 1).Run(context.Background(), plan, nil)
	if result.Steps[0].Attempts != 1 || result.State != "failed" {
		t.Fatalf("%+v", result)
	}
}

func TestSchemaRejectsBeforeAnyActionAndBlocksExternalReferences(t *testing.T) {
	var calls atomic.Int32
	registry := Registry{"value": func(json.RawMessage) (Handler, error) {
		return func(context.Context, map[string]any) (any, error) { calls.Add(1); return nil, nil }, nil
	}}
	def := Definition{ID: "schema", Version: 1, Steps: []Step{{ID: "work", Action: "value"}}, InputSchema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["count"],"properties":{"count":{"$ref":"#/$defs/count"}},"$defs":{"count":{"type":"integer","minimum":1}}}`)}
	plan, err := Compile(def, registry, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engineTest(t, 1, 1).Run(context.Background(), plan, map[string]any{"count": 0}); !errors.Is(err, ErrInput) || calls.Load() != 0 {
		t.Fatalf("%v calls=%d", err, calls.Load())
	}
	if err := plan.ValidateInput(map[string]any{"count": 2}); err != nil {
		t.Fatal(err)
	}
	for _, schema := range []string{`{"$ref":"file:///etc/passwd"}`, `{"$ref":"https://example.com/schema.json"}`, `{"type":"unknown"}`} {
		def.InputSchema = json.RawMessage(schema)
		if _, err := Compile(def, registry, time.Second); err == nil {
			t.Fatalf("accepted %s", schema)
		}
	}
}

func TestObserverFailureStopsEffects(t *testing.T) {
	var calls atomic.Int32
	plan := compileTest(t, []Step{{ID: "a", Action: "test"}}, func(context.Context, map[string]any) (any, error) { calls.Add(1); return nil, nil })
	result, err := engineTest(t, 1, 1).RunTracked(context.Background(), plan, nil, "test-id", func(Result) error { return errors.New("disk full") })
	if !errors.Is(err, ErrPersistence) || result.State != "interrupted" || calls.Load() != 0 {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestRootInputTemplateCannotPanicScheduler(t *testing.T) {
	for _, input := range []map[string]any{
		{"$concat": []any{"a", "b"}},
		{"$ref": "/input/scalar"},
		{"$literal": nil},
	} {
		plan := compileTest(t, []Step{{ID: "work", Action: "test", Input: input}}, func(context.Context, map[string]any) (any, error) {
			t.Error("invalid input reached an action")
			return nil, nil
		})
		result, err := engineTest(t, 1, 1).Run(context.Background(), plan, map[string]any{"scalar": "value"})
		if err != nil || result.State != "failed" {
			t.Fatalf("%+v %v", result, err)
		}
	}
}

func TestSpecificationLimits(t *testing.T) {
	for _, data := range []string{`{"id":1,"id":2}`, `{"n":1e100000}`, `{"n":` + strings.Repeat("1", 1025) + `}`, strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65)} {
		if err := Decode([]byte(data), new(any)); err == nil {
			t.Fatal("accepted invalid or unbounded JSON")
		}
	}
	def := Definition{ID: "test", Version: 1, Steps: []Step{{ID: "a", Action: "value", Retry: Retry{MaxAttempts: 2}}}}
	if _, err := Compile(def, valueRegistry(), time.Second); err == nil {
		t.Fatal("retry without idempotency accepted")
	}
	def.Steps[0].Retry = Retry{}
	var prior *Plan
	for depth := 1; depth <= MaxDepth+1; depth++ {
		if prior != nil {
			def.Steps = []Step{{ID: "nested", Action: "workflow", Config: json.RawMessage(`{"workflow_id":"test","version":1}`)}}
		}
		def.Output = map[string]any{"ok": true}
		plan, err := Compile(def, valueRegistry(), time.Second, func(Reference) (*Plan, error) { return prior, nil })
		if depth <= MaxDepth && err != nil {
			t.Fatal(err)
		}
		if depth > MaxDepth && err == nil {
			t.Fatal("unbounded nesting accepted")
		}
		prior = plan
	}
}
