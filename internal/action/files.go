package action

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/workflow"
)

func (h *HTTP) requestBody(ctx context.Context, value any, encoding string) (io.ReadCloser, int64, string, string, error) {
	if encoding == "file" {
		id, err := artifact.ReferenceID(value)
		if err != nil {
			return nil, 0, "", "", err
		}
		meta, reader, err := h.files.Load().Read(ctx, id, workflow.RunID(ctx))
		return reader, meta.Size, meta.MediaType, meta.ContentEncoding, err
	}
	if encoding == "multipart" {
		body, ok := value.(map[string]any)
		if !ok {
			return nil, 0, "", "", errors.New("multipart body must contain fields and files")
		}
		for key := range body {
			if key != "fields" && key != "files" {
				return nil, 0, "", "", errors.New("unknown multipart body field")
			}
		}
		fields := url.Values{}
		if err := addValues(fields, body["fields"]); err != nil {
			return nil, 0, "", "", err
		}
		parts := []artifact.Part{}
		if raw, exists := body["files"]; exists {
			items, ok := raw.([]any)
			if !ok || len(items) > 16 {
				return nil, 0, "", "", errors.New("multipart files must be an array of at most 16 parts")
			}
			for _, item := range items {
				part, ok := item.(map[string]any)
				if !ok || len(part) != 2 {
					return nil, 0, "", "", artifact.ErrInvalid
				}
				field, ok := part["field"].(string)
				if !ok {
					return nil, 0, "", "", artifact.ErrInvalid
				}
				id, err := artifact.ReferenceID(part["file"])
				if err != nil {
					return nil, 0, "", "", err
				}
				parts = append(parts, artifact.Part{Field: field, ID: id})
			}
		}
		reader, length, media, err := h.files.Load().Multipart(ctx, fields, parts, workflow.RunID(ctx))
		return reader, length, media, "", err
	}
	data, media, err := encodeBody(value, encoding)
	if err != nil {
		return nil, 0, "", "", err
	}
	if len(data) > workflow.MaxDataBytes {
		return nil, 0, "", "", errors.New("HTTP request body exceeds limit")
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), media, "", nil
}
