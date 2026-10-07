package action

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

func validMethod(method string) bool { return methodName.MatchString(method) && method != "CONNECT" }

func (h *HTTP) parseURL(text string) (*url.URL, []string, error) {
	if len(text) > 8192 {
		return nil, nil, errors.New("HTTP URL exceeds limit")
	}
	u, err := url.Parse(text)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (!h.origins[originKey(u)] && !h.publicOrigins) {
		return nil, nil, errors.New("HTTP URL must use an allowed origin without credentials or fragments")
	}
	if u.Scheme != "https" && !(h.allowPrivate && u.Scheme == "http") {
		return nil, nil, errors.New("HTTP action requires HTTPS")
	}
	segments := strings.Split(u.EscapedPath(), "/")
	for i, encoded := range segments {
		segment, err := url.PathUnescape(encoded)
		if err != nil {
			return nil, nil, errors.New("invalid HTTP path encoding")
		}
		segments[i] = segment
		if strings.ContainsAny(segment, "{}") && !pathParameter.MatchString(segment) {
			return nil, nil, errors.New("path parameters must occupy a whole path segment")
		}
	}
	return u, segments, nil
}

func retryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(min(seconds, 300)) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(0, min(5*time.Minute, time.Until(at)))
	}
	return 0
}
