package action

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/workflow"
)

type HTTPConfig struct {
	URL               string            `json:"url"`
	Method            string            `json:"method"`
	URLFromInput      bool              `json:"url_from_input,omitempty"`
	MethodFromInput   bool              `json:"method_from_input,omitempty"`
	InputHeaders      []string          `json:"input_headers,omitempty"`
	ResponseHeaders   []string          `json:"response_headers,omitempty"`
	IdempotencyHeader string            `json:"idempotency_header,omitempty"`
	Encoding          string            `json:"encoding,omitempty"`
	Headers           map[string]string `json:"headers,omitempty"`
	SecretHeaders     map[string]string `json:"secret_headers,omitempty"`
	ExpectJSON        map[string]any    `json:"expect_json,omitempty"`
	ResponseMode      string            `json:"response_mode,omitempty"`
	ResponseName      string            `json:"response_name,omitempty"`
	Async             *HTTPAsyncConfig  `json:"async,omitempty"`
}

type HTTP struct {
	client        *http.Client
	origins       map[string]bool
	allowPrivate  bool
	publicOrigins bool
	lookupSecret  func(string) (string, bool)
	files         atomic.Pointer[artifact.Store]
}

var pathParameter = regexp.MustCompile(`^\{([a-zA-Z][a-zA-Z0-9_]*)\}$`)
var secretName = regexp.MustCompile(`^KAIROS_SECRET_[A-Z0-9_]+$`)
var methodName = regexp.MustCompile(`^[A-Z][A-Z0-9_-]{0,31}$`)

// NewHTTP enforces exact configured origins or an explicit public HTTPS wildcard.
// Proxy environment variables are deliberately not used by this transport.
func NewHTTP(origins []string, allowPrivate bool, lookupSecret func(string) (string, bool)) (*HTTP, error) {
	h := &HTTP{origins: map[string]bool{}, allowPrivate: allowPrivate, lookupSecret: lookupSecret}
	for _, origin := range origins {
		if origin == "*" {
			if allowPrivate {
				return nil, errors.New("wildcard origins cannot be combined with private-network access")
			}
			h.publicOrigins = true
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return nil, errors.New("allowed origins must be scheme://host[:port]")
		}
		if u.Scheme != "https" && !(allowPrivate && u.Scheme == "http") {
			return nil, errors.New("HTTP origins require the explicit private-network development option")
		}
		h.origins[originKey(u)] = true
	}
	transport := &http.Transport{
		DialContext: h.dialContext, ForceAttemptHTTP2: true,
		MaxIdleConns: 128, MaxIdleConnsPerHost: 16, MaxConnsPerHost: 32,
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second, MaxResponseHeaderBytes: 32 << 10,
	}
	h.client = &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return h, nil
}

func (h *HTTP) Close() { h.client.CloseIdleConnections() }

func (h *HTTP) SetFiles(files *artifact.Store) error {
	if files == nil || !h.files.CompareAndSwap(nil, files) {
		return errors.New("file store already configured or nil")
	}
	return nil
}

func originKey(u *url.URL) string { return strings.ToLower(u.Scheme + "://" + u.Host) }

func (h *HTTP) Prepare(raw json.RawMessage) (workflow.Handler, error) {
	var config HTTPConfig
	if err := workflow.Decode(raw, &config); err != nil {
		return nil, errors.New("invalid HTTP action configuration")
	}
	var u *url.URL
	var segments []string
	var err error
	if config.URLFromInput {
		if config.URL != "" || len(config.SecretHeaders) != 0 {
			return nil, errors.New("url_from_input requires an empty url and cannot forward server secret headers")
		}
	} else {
		u, segments, err = h.parseURL(config.URL)
		if err != nil {
			return nil, err
		}
	}
	if config.MethodFromInput {
		if config.Method != "" {
			return nil, errors.New("method_from_input requires an empty method")
		}
	} else if !validMethod(config.Method) {
		return nil, errors.New("HTTP action needs an uppercase method other than CONNECT")
	}
	if config.Encoding == "" {
		config.Encoding = "json"
	}
	if config.Encoding != "json" && config.Encoding != "form" && config.Encoding != "raw" && config.Encoding != "file" && config.Encoding != "multipart" {
		return nil, errors.New("HTTP encoding must be json, form, raw, file, or multipart")
	}
	if config.ResponseMode != "" && config.ResponseMode != "auto" && config.ResponseMode != "file" {
		return nil, errors.New("response_mode must be auto or file")
	}
	if config.ResponseMode == "file" && len(config.ExpectJSON) > 0 {
		return nil, errors.New("file responses cannot use expect_json")
	}
	if config.Async != nil {
		if config.ResponseMode == "file" {
			return nil, errors.New("async HTTP requires JSON operation responses")
		}
		if err := config.Async.normalize(); err != nil {
			return nil, err
		}
	}
	if config.ResponseName != "" && config.ResponseMode != "file" {
		return nil, errors.New("response_name requires file response mode")
	}
	if err := artifact.ValidateMetadata(artifact.Metadata{Name: config.ResponseName}); err != nil {
		return nil, err
	}
	if (config.ResponseMode == "file" || config.Encoding == "file" || config.Encoding == "multipart") && h.files.Load() == nil {
		return nil, errors.New("HTTP files require a configured file store")
	}
	headers := make(http.Header)
	for name, value := range config.Headers {
		lower := strings.ToLower(name)
		if lower == "authorization" || lower == "proxy-authorization" || lower == "cookie" || lower == "x-api-key" {
			return nil, errors.New("credential headers must use secret_headers")
		}
		if err := addHeader(headers, name, value); err != nil {
			return nil, err
		}
	}
	for name, envName := range config.SecretHeaders {
		if !secretName.MatchString(envName) || h.lookupSecret == nil {
			return nil, errors.New("secret headers require a KAIROS_SECRET_ environment reference")
		}
		value, ok := h.lookupSecret(envName)
		if !ok || value == "" {
			return nil, errors.New("HTTP action secret is missing")
		}
		if err := addHeader(headers, name, value); err != nil {
			return nil, err
		}
	}
	for pointer := range config.ExpectJSON {
		// Syntax validation does not require the eventual response to exist.
		if err := workflow.ValidatePointer(pointer); err != nil {
			return nil, errors.New("expect_json keys must be JSON pointers")
		}
	}
	inputHeaders := map[string]bool{}
	reserved := headers.Clone()
	if config.IdempotencyHeader != "" {
		if err := addHeader(reserved, config.IdempotencyHeader, "reserved"); err != nil {
			return nil, err
		}
	}
	if len(config.InputHeaders) > 32 || len(config.ResponseHeaders) > 32 {
		return nil, errors.New("too many configurable headers")
	}
	for _, name := range config.InputHeaders {
		if err := addHeader(reserved, name, "reserved"); err != nil {
			return nil, err
		}
		inputHeaders[http.CanonicalHeaderKey(name)] = true
	}
	for _, name := range config.ResponseHeaders {
		if err := addHeader(make(http.Header), name, "validate"); err != nil {
			return nil, err
		}
		if strings.EqualFold(name, "Set-Cookie") {
			return nil, errors.New("response cookie capture is disabled")
		}
	}
	return func(ctx context.Context, input map[string]any) (any, error) {
		for key := range input {
			if key != "body" && key != "query" && key != "path" && key != "headers" && !(key == "url" && config.URLFromInput) && !(key == "method" && config.MethodFromInput) {
				return nil, errors.New("unsupported HTTP input field")
			}
		}
		requestBase, requestSegments := u, segments
		if config.URLFromInput {
			text, ok := input["url"].(string)
			if !ok {
				return nil, errors.New("HTTP input requires a string url")
			}
			var err error
			requestBase, requestSegments, err = h.parseURL(text)
			if err != nil {
				return nil, err
			}
		}
		method := config.Method
		if config.MethodFromInput {
			method, _ = input["method"].(string)
			if !validMethod(method) {
				return nil, errors.New("invalid HTTP input method")
			}
		}
		requestURL := *requestBase
		path, err := renderPath(requestSegments, input["path"])
		if err != nil {
			return nil, err
		}
		requestURL.RawPath = path
		requestURL.Path, _ = url.PathUnescape(path)
		query := requestURL.Query()
		if err := addValues(query, input["query"]); err != nil {
			return nil, err
		}
		requestURL.RawQuery = query.Encode()
		body, length, contentType, contentEncoding, err := h.requestBody(ctx, input["body"], config.Encoding)
		if err != nil {
			return nil, err
		}
		defer body.Close()
		req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), body)
		if err != nil {
			return nil, errors.New("could not construct HTTP request")
		}
		req.Header = headers.Clone()
		req.ContentLength = length
		if supplied, exists := input["headers"]; exists {
			values, ok := supplied.(map[string]any)
			if !ok {
				return nil, errors.New("HTTP headers must be an object")
			}
			for name, value := range values {
				text, ok := value.(string)
				if !ok || !inputHeaders[http.CanonicalHeaderKey(name)] {
					return nil, errors.New("HTTP input header is not permitted")
				}
				if err := addHeader(req.Header, name, text); err != nil {
					return nil, err
				}
			}
		}
		if config.IdempotencyHeader != "" {
			key := workflow.OperationKey(ctx)
			if key == "" {
				return nil, errors.New("idempotency header requires workflow execution context")
			}
			sum := sha256.Sum256([]byte(key))
			req.Header.Set(config.IdempotencyHeader, hex.EncodeToString(sum[:]))
		}
		if contentType != "" && (config.Encoding == "multipart" || req.Header.Get("Content-Type") == "") {
			req.Header.Set("Content-Type", contentType)
		}
		if contentEncoding != "" && req.Header.Get("Content-Encoding") == "" {
			req.Header.Set("Content-Encoding", contentEncoding)
		}
		if config.ResponseMode == "file" && req.Header.Get("Accept-Encoding") == "" {
			req.Header.Set("Accept-Encoding", "identity")
		}
		resp, err := h.doRequest(ctx, req, config)
		_ = body.Close()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if config.Async != nil {
				return nil, err
			}
			return nil, &workflow.ActionError{Message: "upstream request failed", Transient: true}
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, &workflow.ActionError{Message: fmt.Sprintf("upstream returned HTTP %d", resp.StatusCode),
				Transient:  transientHTTPStatus(resp.StatusCode),
				RetryAfter: retryAfter(resp.Header.Get("Retry-After"))}
		}
		var output any
		if config.ResponseMode == "file" {
			owner := workflow.RunID(ctx)
			if owner == "" {
				return nil, errors.New("file response requires durable workflow context")
			}
			meta, _, err := h.files.Load().Put(ctx, resp.Body, artifact.Metadata{Name: config.ResponseName, MediaType: resp.Header.Get("Content-Type"), ContentEncoding: resp.Header.Get("Content-Encoding")}, "", "", owner)
			if err != nil {
				if errors.Is(err, artifact.ErrRead) {
					return nil, &workflow.ActionError{Message: err.Error(), Transient: true}
				}
				return nil, err
			}
			output = meta.Reference()
		} else {
			data, err := io.ReadAll(io.LimitReader(resp.Body, workflow.MaxDataBytes+1))
			if err != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, &workflow.ActionError{Message: "could not read upstream response", Transient: true}
			}
			if len(data) > workflow.MaxDataBytes {
				return nil, errors.New("upstream response exceeds limit")
			}
			output = string(data)
			if json.Valid(data) {
				if err := workflow.Decode(data, &output); err != nil {
					return nil, errors.New("could not decode upstream response")
				}
			}
			for pointer, expected := range config.ExpectJSON {
				actual, err := workflow.Lookup(output, pointer)
				if err != nil || !workflow.Equal(actual, expected) {
					return nil, errors.New("upstream response did not meet expect_json condition")
				}
			}
		}
		result := map[string]any{"status": resp.StatusCode, "body": output}
		if len(config.ResponseHeaders) > 0 {
			selected := map[string]any{}
			for _, name := range config.ResponseHeaders {
				selected[http.CanonicalHeaderKey(name)] = resp.Header.Get(name)
			}
			result["headers"] = selected
		}
		return result, nil
	}, nil
}

func addHeader(headers http.Header, name, value string) error {
	if name == "" || strings.ContainsAny(value, "\r\n") {
		return errors.New("invalid HTTP header")
	}
	for _, r := range name {
		if !strings.ContainsRune("!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", r) {
			return errors.New("invalid HTTP header name")
		}
	}
	switch strings.ToLower(name) {
	case "host", "connection", "content-length", "transfer-encoding", "upgrade", "proxy-authorization", "proxy-connection", "trailer", "te":
		return errors.New("transport-controlled headers are not configurable")
	}
	if _, exists := headers[http.CanonicalHeaderKey(name)]; exists {
		return errors.New("duplicate HTTP header")
	}
	headers.Set(name, value)
	return nil
}

func renderPath(segments []string, value any) (string, error) {
	params, ok := value.(map[string]any)
	if value != nil && !ok {
		return "", errors.New("HTTP path must be an object")
	}
	used := map[string]bool{}
	out := make([]string, len(segments))
	for i, segment := range segments {
		if match := pathParameter.FindStringSubmatch(segment); match != nil {
			var exists bool
			value, exists = params[match[1]]
			if !exists {
				return "", errors.New("missing HTTP path parameter")
			}
			var err error
			segment, err = scalar(value)
			if err != nil || segment == "" || segment == "." || segment == ".." || strings.ContainsAny(segment, "/\\") {
				return "", errors.New("invalid HTTP path parameter")
			}
			used[match[1]] = true
		}
		out[i] = url.PathEscape(segment)
	}
	if len(used) != len(params) {
		return "", errors.New("unexpected HTTP path parameter")
	}
	return strings.Join(out, "/"), nil
}

func scalar(value any) (string, error) {
	switch value := value.(type) {
	case string:
		return value, nil
	case json.Number:
		return value.String(), nil
	case bool:
		return strconv.FormatBool(value), nil
	default:
		return "", errors.New("query, form, and path values must be strings, numbers, or booleans")
	}
}

func addValues(target url.Values, value any) error {
	if value == nil {
		return nil
	}
	values, ok := value.(map[string]any)
	if !ok {
		return errors.New("query and form values must be objects")
	}
	for key, value := range values {
		items, ok := value.([]any)
		if !ok {
			items = []any{value}
		}
		target.Del(key)
		for _, item := range items {
			text, err := scalar(item)
			if err != nil {
				return err
			}
			target.Add(key, text)
		}
	}
	return nil
}

func encodeBody(value any, encoding string) ([]byte, string, error) {
	if value == nil {
		return nil, "", nil
	}
	switch encoding {
	case "form":
		form := url.Values{}
		if err := addValues(form, value); err != nil {
			return nil, "", err
		}
		return []byte(form.Encode()), "application/x-www-form-urlencoded", nil
	case "raw":
		text, ok := value.(string)
		if !ok {
			return nil, "", errors.New("raw body must be a string")
		}
		return []byte(text), "text/plain", nil
	default:
		data, err := json.Marshal(value)
		return data, "application/json", err
	}
}

func (h *HTTP) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return nil, errors.New("destination could not be resolved")
	}
	for _, ip := range ips {
		if !h.allowPrivate && !publicIP(ip) {
			return nil, errors.New("destination resolves to a restricted network")
		}
	}
	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("destination could not be reached")
}

var restrictedNetworks = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"), netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"), netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("64:ff9b::/96"), netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("2001::/32"),
}

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, prefix := range restrictedNetworks {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
