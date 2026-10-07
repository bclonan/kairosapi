package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type File struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	MediaType       string    `json:"media_type"`
	ContentEncoding string    `json:"content_encoding,omitempty"`
	Size            int64     `json:"size"`
	SHA256          string    `json:"sha256"`
	CreatedAt       time.Time `json:"created_at"`
}

func (f File) Reference() map[string]any {
	return map[string]any{"$artifact": f.ID, "name": f.Name, "media_type": f.MediaType, "size": f.Size, "sha256": f.SHA256}
}

type UploadResult struct {
	File      File           `json:"file"`
	Reference map[string]any `json:"reference"`
	Duplicate bool           `json:"duplicate"`
}

// Upload sends opaque bytes. Compute the expected SHA-256 before calling and use
// the same key, metadata, and bytes when retrying an uncertain upload response.
func (c *Client) Upload(ctx context.Context, name, mediaType string, reader io.Reader, digest, key string) (UploadResult, error) {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || reader == nil {
		return UploadResult{}, errors.New("upload requires a reader and a SHA-256 hex digest")
	}
	request, err := c.request(ctx, http.MethodPost, "/v1/files", reader)
	if err != nil {
		return UploadResult{}, err
	}
	request.Header.Set("Content-Type", mediaType)
	request.Header.Set("X-File-Name", name)
	request.Header.Set("X-Content-SHA256", strings.ToLower(digest))
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return UploadResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return UploadResult{}, responseError(response)
	}
	var result UploadResult
	if err := json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result); err != nil {
		return UploadResult{}, err
	}
	if result.File.ID == "" || result.File.SHA256 != strings.ToLower(digest) {
		return UploadResult{}, errors.New("upload response did not confirm the expected digest")
	}
	return result, nil
}

func (c *Client) File(ctx context.Context, id string) (File, error) {
	var result File
	_, err := c.json(ctx, http.MethodGet, "/v1/files/"+url.PathEscape(id), nil, "", &result)
	return result, err
}

// Download checks stored metadata, size, and SHA-256 before it writes to target.
// It spools at most 256 MiB to a private temporary file, then removes that file.
// A target write failure can leave a partial destination. Use a temporary target
// and rename it after success when the destination itself must publish atomically.
func (c *Client) Download(ctx context.Context, id string, target io.Writer) (File, error) {
	if target == nil {
		return File{}, errors.New("download requires a destination writer")
	}
	metadata, err := c.File(ctx, id)
	if err != nil {
		return File{}, err
	}
	digest, err := hex.DecodeString(metadata.SHA256)
	if err != nil || len(digest) != sha256.Size || metadata.ID != id || metadata.Size < 0 || metadata.Size > 256<<20 {
		return File{}, errors.New("invalid file metadata")
	}
	request, err := c.request(ctx, http.MethodGet, "/v1/files/"+url.PathEscape(id)+"/content", nil)
	if err != nil {
		return File{}, err
	}
	request.Header.Set("Accept-Encoding", "identity")
	response, err := c.http.Do(request)
	if err != nil {
		return File{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return File{}, responseError(response)
	}
	if response.Header.Get("X-Content-SHA256") != metadata.SHA256 {
		return File{}, errors.New("download digest header differs from file metadata")
	}
	spool, err := os.CreateTemp("", "kairos-client-download-*")
	if err != nil {
		return File{}, err
	}
	defer func() { _ = spool.Close(); _ = os.Remove(spool.Name()) }()
	hasher := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, hasher), io.LimitReader(response.Body, metadata.Size+1))
	if err != nil {
		return File{}, err
	}
	if size != metadata.Size || hex.EncodeToString(hasher.Sum(nil)) != metadata.SHA256 {
		return File{}, errors.New("download size or SHA-256 verification failed")
	}
	if err := ctx.Err(); err != nil {
		return File{}, err
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return File{}, err
	}
	if _, err := io.Copy(target, spool); err != nil {
		return File{}, err
	}
	return metadata, nil
}
