// Package workflow validates and executes finite workflows within one process.
// The execution engine uses cooperative cancellation and bounded action workers.
package workflow

import (
	"context"
	"encoding/json"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const MaxSteps = 32
const MaxDataBytes = 256 << 10
const MaxResultBytes = 4 << 20
const MaxExpandedSteps = 128
const MaxDepth = 8

type Definition struct {
	SpecVersion    string                    `json:"spec_version,omitempty"`
	ID             string                    `json:"id"`
	Version        int                       `json:"version"`
	Description    string                    `json:"description,omitempty"`
	InputSchema    json.RawMessage           `json:"input_schema,omitempty"`
	Triggers       []Trigger                 `json:"triggers,omitempty"`
	Output         map[string]any            `json:"output,omitempty"`
	TimeoutSeconds int                       `json:"timeout_seconds,omitempty"`
	Execution      Execution                 `json:"execution,omitzero"`
	Resources      map[string]map[string]any `json:"resources,omitempty"`
	Steps          []Step                    `json:"steps"`
}

// Execution selects dependency scheduling and restart behavior.
type Execution struct {
	Mode     string `json:"mode,omitempty"`
	Recovery string `json:"recovery,omitempty"`
}

type Step struct {
	ID             string          `json:"id"`
	Action         string          `json:"action"`
	DependsOn      []string        `json:"depends_on,omitempty"`
	Input          map[string]any  `json:"input,omitempty"`
	Config         json.RawMessage `json:"config,omitempty"`
	TimeoutSeconds int             `json:"timeout_seconds,omitempty"`
	When           *Condition      `json:"when,omitempty"`
	Retry          Retry           `json:"retry,omitzero"`
}

type Reference struct {
	WorkflowID string `json:"workflow_id"`
	Version    int    `json:"version"`
}

type Resolver func(Reference) (*Plan, error)

type Retry struct {
	MaxAttempts int  `json:"max_attempts,omitempty"`
	BackoffMS   int  `json:"backoff_ms,omitempty"`
	Idempotent  bool `json:"idempotent,omitempty"`
}

type Trigger struct {
	Type    string         `json:"type"`
	Source  string         `json:"source,omitempty"`
	Subject string         `json:"subject,omitempty"`
	Match   map[string]any `json:"match,omitempty"`
}

type Condition struct {
	Path  string      `json:"path,omitempty"`
	Op    string      `json:"op,omitempty"`
	Value any         `json:"value,omitempty"`
	All   []Condition `json:"all,omitempty"`
	Any   []Condition `json:"any,omitempty"`
	Not   *Condition  `json:"not,omitempty"`
}

// ActionError marks a transient error without exposing an upstream response.
type ActionError struct {
	Message    string
	Transient  bool
	RetryAfter time.Duration
}

func (e *ActionError) Error() string { return e.Message }

type Observer func(Result) error

// Handlers must honor ctx and return JSON data. They must not mutate shared state.
// A goroutine cannot forcibly stop a handler that ignores cancellation.
type Handler func(ctx context.Context, input map[string]any) (any, error)
type Factory func(config json.RawMessage) (Handler, error)
type Registry map[string]Factory

type compiledStep struct {
	step    Step
	handler Handler
	timeout time.Duration
	child   *Plan
}

// Plan is immutable after compilation and may be shared by concurrent runs.
type Plan struct {
	ID        string
	Version   int
	timeout   time.Duration
	steps     []compiledStep
	output    map[string]any
	schema    *jsonschema.Schema
	expanded  int
	depth     int
	execution Execution
}

type StepResult struct {
	ID          string         `json:"id"`
	State       string         `json:"state"`
	Output      any            `json:"output,omitempty"`
	Error       string         `json:"error,omitempty"`
	StartedAt   time.Time      `json:"started_at,omitzero"`
	FinishedAt  time.Time      `json:"finished_at,omitzero"`
	Attempts    int            `json:"attempts,omitempty"`
	Children    []StepResult   `json:"children,omitempty"`
	OperationID string         `json:"operation_id,omitempty"`
	Phase       string         `json:"phase,omitempty"`
	Checkpoint  map[string]any `json:"checkpoint,omitempty"`
	SkipReason  string         `json:"skip_reason,omitempty"`
}

type Result struct {
	ID         string       `json:"id"`
	WorkflowID string       `json:"workflow_id"`
	Version    int          `json:"version"`
	State      string       `json:"state"`
	StartedAt  time.Time    `json:"started_at,omitzero"`
	FinishedAt time.Time    `json:"finished_at,omitzero"`
	Steps      []StepResult `json:"steps"`
	Output     any          `json:"output,omitempty"`
	Error      string       `json:"error,omitempty"`
}
