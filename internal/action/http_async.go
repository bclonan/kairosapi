package action

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

	"github.com/bclonan/kairosapi/internal/workflow"
)

// HTTPAsyncConfig describes the provider's operation resource. Polls use GET
// and stay on the initial request's origin. An absent config preserves normal
// HTTP behavior, including returning a 202 response immediately.
type HTTPAsyncConfig struct {
	PollURLPointer string `json:"poll_url_pointer,omitempty"`
	StatusPointer  string `json:"status_pointer,omitempty"`
	PendingValues  []any  `json:"pending_values,omitempty"`
	SuccessValues  []any  `json:"success_values,omitempty"`
	FailureValues  []any  `json:"failure_values,omitempty"`
	IntervalMS     int    `json:"interval_ms,omitempty"`
	MaxPolls       int    `json:"max_polls,omitempty"`
}

func (c *HTTPAsyncConfig) normalize() error {
	if c.PollURLPointer != "" {
		if err := workflow.ValidatePointer(c.PollURLPointer); err != nil {
			return errors.New("async poll_url_pointer must be a JSON pointer")
		}
	}
	if c.StatusPointer == "" {
		c.StatusPointer = "/status"
	}
	if err := workflow.ValidatePointer(c.StatusPointer); err != nil {
		return errors.New("async status_pointer must be a JSON pointer")
	}
	if len(c.PendingValues) == 0 {
		c.PendingValues = []any{"pending", "running"}
	}
	if len(c.SuccessValues) == 0 {
		c.SuccessValues = []any{"succeeded", "completed"}
	}
	if len(c.FailureValues) == 0 {
		c.FailureValues = []any{"failed", "cancelled", "canceled"}
	}
	if c.IntervalMS == 0 {
		c.IntervalMS = 1000
	}
	if c.MaxPolls == 0 {
		c.MaxPolls = 300
	}
	if c.IntervalMS < 100 || c.IntervalMS > 30000 || c.MaxPolls < 1 || c.MaxPolls > 1000 {
		return errors.New("async interval_ms must be 100 to 30000 and max_polls must be 1 to 1000")
	}
	seen := []any{}
	for _, values := range [][]any{c.PendingValues, c.SuccessValues, c.FailureValues} {
		if len(values) > 16 {
			return errors.New("async status lists support up to 16 scalar values each")
		}
		for _, value := range values {
			switch v := value.(type) {
			case string:
				if len(v) == 0 || len(v) > 128 {
					return errors.New("async string status values must contain 1 to 128 bytes")
				}
			case bool, json.Number:
			default:
				return errors.New("async status values must be strings, booleans, or numbers")
			}
			for _, previous := range seen {
				if workflow.Equal(previous, value) {
					return errors.New("async status values must be distinct")
				}
			}
			seen = append(seen, value)
		}
	}
	return nil
}

type asyncCheckpoint struct {
	Kind       string    `json:"kind"`
	PollURL    string    `json:"poll_url"`
	Polls      int       `json:"polls"`
	NextPollAt time.Time `json:"next_poll_at"`
}

func (c asyncCheckpoint) save(ctx context.Context) error {
	// Keep this small and exclude request credentials and provider response
	// bodies. The operation URL is trusted run data and may itself be sensitive.
	return workflow.SaveCheckpoint(ctx, map[string]any{
		"kind": c.Kind, "poll_url": c.PollURL, "polls": c.Polls,
		"next_poll_at": c.NextPollAt.UTC().Format(time.RFC3339Nano),
	})
}

func (h *HTTP) doRequest(ctx context.Context, req *http.Request, config HTTPConfig) (*http.Response, error) {
	if config.Async == nil {
		return h.client.Do(req)
	}
	var checkpoint asyncCheckpoint
	if saved, ok := workflow.LoadCheckpoint(ctx); ok {
		data, err := json.Marshal(saved)
		if err != nil || workflow.Decode(data, &checkpoint) != nil || checkpoint.Kind != "http_async" ||
			checkpoint.PollURL == "" || checkpoint.Polls < 0 || checkpoint.Polls > config.Async.MaxPolls ||
			checkpoint.NextPollAt.IsZero() {
			return nil, errors.New("invalid async HTTP checkpoint")
		}
		if req.Body != nil {
			_ = req.Body.Close()
		}
		// The current egress policy is checked again after every restart.
		if _, err := h.asyncURL(req.URL, checkpoint.PollURL); err != nil {
			return nil, err
		}
		return h.pollOperation(ctx, req, config, checkpoint)
	}
	resp, err := h.client.Do(req)
	if req.Body != nil {
		_ = req.Body.Close()
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &workflow.ActionError{Message: "upstream request failed", Transient: true}
	}
	if resp.StatusCode != http.StatusAccepted {
		return resp, nil
	}
	data, err := readAsyncBody(ctx, resp)
	if err != nil {
		return nil, err
	}
	pollURL := resp.Header.Get("Location")
	if config.Async.PollURLPointer != "" {
		var body any
		if err := workflow.Decode(data, &body); err != nil {
			return nil, errors.New("async acceptance body must contain JSON")
		}
		value, err := workflow.Lookup(body, config.Async.PollURLPointer)
		var ok bool
		pollURL, ok = value.(string)
		if err != nil || !ok {
			return nil, errors.New("async response has no string operation URL")
		}
	}
	target, err := h.asyncURL(req.URL, pollURL)
	if err != nil {
		return nil, err
	}
	checkpoint = asyncCheckpoint{
		Kind: "http_async", PollURL: target.String(),
		NextPollAt: time.Now().Add(max(time.Duration(config.Async.IntervalMS)*time.Millisecond, retryAfter(resp.Header.Get("Retry-After")))),
	}
	if err := checkpoint.save(ctx); err != nil {
		return nil, err
	}
	if err := workflow.ReportProgress(ctx, "accepted"); err != nil {
		return nil, err
	}
	return h.pollOperation(ctx, req, config, checkpoint)
}

func (h *HTTP) asyncURL(base *url.URL, text string) (*url.URL, error) {
	if text == "" || len(text) > 8192 || strings.ContainsAny(text, "{}\\") {
		return nil, errors.New("async operation URL is missing or invalid")
	}
	relative, err := url.Parse(text)
	if err != nil || relative.User != nil || relative.Fragment != "" {
		return nil, errors.New("async operation URL is invalid")
	}
	target := base.ResolveReference(relative)
	if originKey(base) != originKey(target) {
		return nil, errors.New("async operation URL must stay on the initial request origin")
	}
	target, segments, err := h.parseURL(target.String())
	if err != nil {
		return nil, err
	}
	for _, segment := range segments {
		if strings.ContainsAny(segment, "{}\\") {
			return nil, errors.New("async operation URL cannot contain path parameters")
		}
	}
	return target, nil
}

func (h *HTTP) pollOperation(ctx context.Context, original *http.Request, config HTTPConfig, checkpoint asyncCheckpoint) (*http.Response, error) {
	for checkpoint.Polls < config.Async.MaxPolls {
		timer := time.NewTimer(max(0, time.Until(checkpoint.NextPollAt)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		target, err := h.asyncURL(original.URL, checkpoint.PollURL)
		if err != nil {
			return nil, err
		}
		checkpoint.Polls++
		// Count attempted GETs before dispatch, so restarts cannot reset the
		// provider poll limit. A crash between this write and GET spends a slot.
		checkpoint.NextPollAt = time.Now().Add(time.Duration(config.Async.IntervalMS) * time.Millisecond)
		if err := checkpoint.save(ctx); err != nil {
			return nil, err
		}
		if err := workflow.ReportProgress(ctx, "polling"); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return nil, errors.New("could not construct async poll request")
		}
		req.Header = original.Header.Clone()
		for _, name := range []string{"Content-Type", "Content-Encoding", "Content-Length", config.IdempotencyHeader} {
			req.Header.Del(name)
		}
		resp, err := h.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if err := workflow.ReportProgress(ctx, "poll_retry"); err != nil {
				return nil, err
			}
			continue
		}
		data, readErr := readAsyncBody(ctx, resp)
		if readErr != nil {
			var transient *workflow.ActionError
			if !errors.As(readErr, &transient) || !transient.Transient {
				return nil, readErr
			}
			if err := workflow.ReportProgress(ctx, "poll_retry"); err != nil {
				return nil, err
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			if !transientHTTPStatus(resp.StatusCode) {
				return nil, fmt.Errorf("async poll returned HTTP %d", resp.StatusCode)
			}
			if wait := retryAfter(resp.Header.Get("Retry-After")); wait > time.Duration(config.Async.IntervalMS)*time.Millisecond {
				checkpoint.NextPollAt = time.Now().Add(wait)
				if err := checkpoint.save(ctx); err != nil {
					return nil, err
				}
			}
			if err := workflow.ReportProgress(ctx, "poll_retry"); err != nil {
				return nil, err
			}
			continue
		}
		var body any
		if err := workflow.Decode(data, &body); err != nil {
			return nil, errors.New("async poll response must contain JSON")
		}
		status, err := workflow.Lookup(body, config.Async.StatusPointer)
		if err != nil {
			return nil, errors.New("async poll response is missing the configured status")
		}
		if asyncMatches(status, config.Async.SuccessValues) {
			if err := workflow.ReportProgress(ctx, "succeeded"); err != nil {
				return nil, err
			}
			resp.Body = io.NopCloser(bytes.NewReader(data))
			return resp, nil
		}
		if asyncMatches(status, config.Async.FailureValues) {
			if err := workflow.ReportProgress(ctx, "failed"); err != nil {
				return nil, err
			}
			return nil, errors.New("upstream asynchronous operation failed")
		}
		if !asyncMatches(status, config.Async.PendingValues) {
			return nil, errors.New("async poll returned an unrecognized status")
		}
		if wait := retryAfter(resp.Header.Get("Retry-After")); wait > time.Duration(config.Async.IntervalMS)*time.Millisecond {
			checkpoint.NextPollAt = time.Now().Add(wait)
			if err := checkpoint.save(ctx); err != nil {
				return nil, err
			}
		}
		if err := workflow.ReportProgress(ctx, "pending"); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("async operation exceeded max_polls")
}

func asyncMatches(actual any, values []any) bool {
	for _, expected := range values {
		if workflow.Equal(actual, expected) {
			return true
		}
	}
	return false
}

func transientHTTPStatus(code int) bool {
	return code == 408 || code == 429 || code == 500 || code == 502 || code == 503 || code == 504
}

func readAsyncBody(ctx context.Context, resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, workflow.MaxDataBytes+1))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &workflow.ActionError{Message: "could not read async response", Transient: true}
	}
	if len(data) > workflow.MaxDataBytes {
		return nil, errors.New("async response exceeds limit")
	}
	return data, nil
}
