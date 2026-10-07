package artifact

import (
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"os"
	"sort"
	"strings"

	bolt "go.etcd.io/bbolt"
)

type Part struct{ Field, ID string }

// Multipart assembles and verifies bytes before sending any external request.
// Only fast disk copies hold database snapshots. Network sends use the spool.
func (s *Store) Multipart(ctx context.Context, fields map[string][]string, parts []Part, owner string) (io.ReadSeekCloser, int64, string, error) {
	release, err := s.acquire(ctx)
	if err != nil {
		return nil, 0, "", err
	}
	file, err := os.CreateTemp(s.directory, "kairos-multipart-*")
	if err != nil {
		release()
		return nil, 0, "", s.storageError(err)
	}
	reader := &tempFile{File: file, release: release}
	writer := multipart.NewWriter(contextWriter{ctx, file})
	fail := func(err error) (io.ReadSeekCloser, int64, string, error) {
		_ = reader.Close()
		return nil, 0, "", s.storageError(err)
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	var total int64
	validField := func(name string) bool {
		return name != "" && len(name) <= 128 && !strings.ContainsAny(name, "\r\n\x00")
	}
	for _, name := range names {
		if !validField(name) {
			return fail(ErrInvalid)
		}
		for _, value := range fields[name] {
			total += int64(len(value))
			if total > 256<<10 {
				return fail(ErrTooLarge)
			}
			if err := writer.WriteField(name, value); err != nil {
				return fail(err)
			}
		}
	}
	if len(parts) > 16 {
		return fail(ErrInvalid)
	}
	for _, part := range parts {
		if !validField(part.Field) {
			return fail(ErrInvalid)
		}
		err := s.db.View(func(tx *bolt.Tx) error {
			saved, err := get(tx, part.ID)
			if err != nil {
				return err
			}
			if saved.Owner != "" && saved.Owner != owner {
				return ErrNotFound
			}
			total += saved.Metadata.Size
			if total > s.limits.MaxBytes {
				return ErrTooLarge
			}
			headers := textproto.MIMEHeader{}
			headers.Set("Content-Disposition", mime.FormatMediaType("form-data", map[string]string{"name": part.Field, "filename": saved.Metadata.Name}))
			headers.Set("Content-Type", saved.Metadata.MediaType)
			if saved.Metadata.ContentEncoding != "" {
				headers.Set("Content-Encoding", saved.Metadata.ContentEncoding)
			}
			destination, err := writer.CreatePart(headers)
			if err != nil {
				return err
			}
			return copyVerified(ctx, tx, saved.Metadata, destination)
		})
		if err != nil {
			return fail(err)
		}
	}
	if err := writer.Close(); err != nil {
		return fail(err)
	}
	length, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fail(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fail(err)
	}
	return reader, length, writer.FormDataContentType(), nil
}
