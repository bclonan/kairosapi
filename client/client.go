// Package client provides HTTP utilities for Kairos applications and agents.
// It never retries a mutating request automatically and never cancels a remote
// run when the caller stops waiting. Supply stable keys to retry admission safely.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxResponseBytes = 8 << 20

type Client struct {
	base         string
	token        string
	http         *http.Client
	PollInterval time.Duration
}

// New accepts an optional configured HTTP client. Redirects are disabled even
// when a client is supplied so bearer credentials cannot follow a redirect.
func New(baseURL, token string, transport *http.Client) (*Client, error) {
	base, err := url.Parse(baseURL)
	if err != nil || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, errors.New("base URL must be HTTP or HTTPS without credentials, query, or fragment")
	}
	if len(token) < 32 || strings.TrimSpace(token) != token || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("bearer token must contain at least 32 characters without surrounding whitespace")
	}
	configured := http.Client{Timeout: 3 * time.Minute}
	if transport != nil {
		configured = *transport
	}
	configured.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: strings.TrimRight(base.String(), "/"), token: token, http: &configured, PollInterval: 200 * time.Millisecond}, nil
}

type Error struct {
	StatusCode int
	Message    string
	RetryAfter string
}

func (e *Error) Error() string { return fmt.Sprintf("Kairos HTTP %d: %s", e.StatusCode, e.Message) }

type Step struct {
	ID          string          `json:"id"`
	State       string          `json:"state"`
	OperationID string          `json:"operation_id,omitempty"`
	Phase       string          `json:"phase,omitempty"`
	Attempts    int             `json:"attempts,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
	Children    []Step          `json:"children,omitempty"`
}

type Run struct {
	ID                    string          `json:"id"`
	WorkflowID            string          `json:"workflow_id"`
	Version               int             `json:"version"`
	State                 string          `json:"state"`
	SequenceID            string          `json:"sequence_id"`
	CreatedAt             time.Time       `json:"created_at"`
	StartedAt             time.Time       `json:"started_at"`
	FinishedAt            time.Time       `json:"finished_at"`
	CancellationRequested bool            `json:"cancellation_requested,omitempty"`
	ResumeCount           int             `json:"resume_count,omitempty"`
	Steps                 []Step          `json:"steps"`
	Output                json.RawMessage `json:"output,omitempty"`
	Error                 string          `json:"error,omitempty"`
}

func (r Run) Terminal() bool { return r.State != "queued" && r.State != "running" && r.State != "" }

type Request struct {
	WorkflowID    string         `json:"workflow_id,omitempty"`
	Version       int            `json:"version,omitempty"`
	Specification any            `json:"specification,omitempty"`
	Input         map[string]any `json:"input,omitempty"`
	Async         bool           `json:"async"`
}

type Publication struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
	Created bool   `json:"created"`
}

func (c *Client) Publish(ctx context.Context, specification any) (Publication, error) {
	var result Publication
	_, err := c.json(ctx, http.MethodPost, "/v1/workflows", specification, "", &result)
	return result, err
}

func (c *Client) Validate(ctx context.Context, specification any) (map[string]any, error) {
	var result map[string]any
	_, err := c.json(ctx, http.MethodPost, "/v1/workflows/validate", specification, "", &result)
	return result, err
}

func (c *Client) Capabilities(ctx context.Context) (map[string]any, error) {
	var result map[string]any
	_, err := c.json(ctx, http.MethodGet, "/v1/capabilities", nil, "", &result)
	return result, err
}

// Submit always asks for asynchronous admission. A duplicate key with the same
// request returns the original run; duplicate reports the replay response header.
func (c *Client) Submit(ctx context.Context, request Request, key string) (run Run, duplicate bool, err error) {
	request.Async = true
	header, err := c.json(ctx, http.MethodPost, "/v1/runs", request, key, &run)
	return run, header.Get("Idempotency-Replayed") == "true", err
}

func (c *Client) Get(ctx context.Context, id string) (Run, error) {
	var run Run
	_, err := c.json(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(id), nil, "", &run)
	return run, err
}

// Wait returns terminal success or failure as a Run. Check State for the outcome.
// A canceled context stops local polling only. Cancel is a separate explicit call.
func (c *Client) Wait(ctx context.Context, id string) (Run, error) {
	interval := c.PollInterval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	for {
		run, err := c.Get(ctx, id)
		if err != nil || run.Terminal() {
			return run, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return run, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *Client) Cancel(ctx context.Context, id string) (Run, error) {
	var run Run
	_, err := c.json(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(id)+"/cancel", map[string]any{}, "", &run)
	return run, err
}

// Resume requires an administrator client. Resolution fields follow the server
// resume contract. An empty request asks the server to resume only safe work.
func (c *Client) Resume(ctx context.Context, id string, request any) (Run, error) {
	if request == nil {
		request = map[string]any{}
	}
	var run Run
	_, err := c.json(ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(id)+"/resume", request, "", &run)
	return run, err
}

type HistoryEvent struct {
	ID            string    `json:"id"`
	Time          time.Time `json:"time"`
	RunID         string    `json:"run_id"`
	WorkflowID    string    `json:"workflow_id"`
	Version       int       `json:"version"`
	Kind          string    `json:"kind"`
	StepPath      string    `json:"step_path,omitempty"`
	OperationKey  string    `json:"operation_key,omitempty"`
	State         string    `json:"state,omitempty"`
	PreviousState string    `json:"previous_state,omitempty"`
	Attempt       int       `json:"attempt,omitempty"`
	Phase         string    `json:"phase,omitempty"`
	ResumeCount   int       `json:"resume_count,omitempty"`
	Reason        string    `json:"reason,omitempty"`
}

type HistoryPage struct {
	Events         []HistoryEvent `json:"events"`
	NextAfter      string         `json:"next_after"`
	HasMore        bool           `json:"has_more"`
	DroppedThrough string         `json:"dropped_through,omitempty"`
	Truncated      bool           `json:"truncated"`
	State          string         `json:"state"`
}

func (c *Client) History(ctx context.Context, id, after string, limit int) (HistoryPage, error) {
	if limit < 1 || limit > 100 {
		return HistoryPage{}, errors.New("history limit must be between 1 and 100")
	}
	query := url.Values{"limit": {fmt.Sprint(limit)}}
	if after != "" {
		query.Set("after", after)
	}
	var result HistoryPage
	_, err := c.json(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(id)+"/history?"+query.Encode(), nil, "", &result)
	return result, err
}

func (c *Client) Metrics(ctx context.Context) (map[string]any, error) {
	var result map[string]any
	_, err := c.json(ctx, http.MethodGet, "/v1/metrics", nil, "", &result)
	return result, err
}

func (c *Client) json(ctx context.Context, method, path string, value any, key string, result any) (http.Header, error) {
	var body io.Reader
	if value != nil {
		data, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}
	request, err := c.request(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		if target, ok := result.(*Run); ok && (response.StatusCode == 408 || response.StatusCode == 409 || response.StatusCode == 424 || response.StatusCode == 504) {
			data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
			if err != nil || len(data) > maxResponseBytes {
				return response.Header, errors.New("could not read bounded run response")
			}
			var stopped Run
			if json.Unmarshal(data, &stopped) == nil && stopped.ID != "" && stopped.Terminal() {
				*target = stopped
				return response.Header, nil
			}
			response.Body = io.NopCloser(bytes.NewReader(data))
		}
		return response.Header, responseError(response)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return response.Header, err
	}
	if len(data) > maxResponseBytes {
		return response.Header, errors.New("Kairos JSON response exceeds 8 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(result); err != nil {
		return response.Header, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return response.Header, errors.New("Kairos response contains more than one JSON value")
	}
	return response.Header, nil
}

func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	return request, nil
}

func responseError(response *http.Response) error {
	var payload struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 16<<10)).Decode(&payload)
	if payload.Error == "" {
		payload.Error = http.StatusText(response.StatusCode)
	}
	return &Error{StatusCode: response.StatusCode, Message: payload.Error, RetryAfter: response.Header.Get("Retry-After")}
}
