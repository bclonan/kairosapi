// Package artifact stores opaque, immutable bytes inside the execution database.
// File publication can join a run's final state transaction.
package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/bclonan/kairosapi/internal/identity"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrNotFound    = errors.New("file not found or not yet published")
	ErrTooLarge    = errors.New("file exceeds configured size limit")
	ErrQuota       = errors.New("file storage quota exhausted")
	ErrBusy        = errors.New("file transfer capacity exhausted")
	ErrConflict    = errors.New("file idempotency key conflicts with existing content")
	ErrReferenced  = errors.New("file is referenced by a retained run or published workflow")
	ErrIntegrity   = errors.New("stored file integrity check failed")
	ErrUnavailable = errors.New("file storage is unavailable")
	ErrInvalid     = errors.New("invalid file metadata or digest")
	ErrRead        = errors.New("file body could not be read completely")
)

var files = []byte("files")
var chunks = []byte("file_chunks")
var references = []byte("file_references")
var receipts = []byte("file_receipts")
var staged = []byte("file_staged")
var owners = []byte("file_owners")

const chunkSize = 64 << 10

type Limits struct {
	MaxBytes   int64
	TotalBytes int64
	MaxFiles   int
	Transfers  int
}

func (l Limits) Defaults() Limits {
	if l.MaxBytes == 0 {
		l.MaxBytes = 32 << 20
	}
	if l.TotalBytes == 0 {
		l.TotalBytes = 256 << 20
	}
	if l.MaxFiles == 0 {
		l.MaxFiles = 1000
	}
	if l.Transfers == 0 {
		l.Transfers = 2
	}
	return l
}

type Metadata struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	MediaType       string    `json:"media_type"`
	ContentEncoding string    `json:"content_encoding,omitempty"`
	Size            int64     `json:"size"`
	SHA256          string    `json:"sha256"`
	CreatedAt       time.Time `json:"created_at"`
}

func (m Metadata) Reference() map[string]any {
	return map[string]any{"$artifact": m.ID, "name": m.Name, "media_type": m.MediaType, "size": m.Size, "sha256": m.SHA256}
}

type record struct {
	Metadata Metadata `json:"metadata"`
	Owner    string   `json:"owner,omitempty"`
	Receipt  string   `json:"receipt,omitempty"`
}

type Store struct {
	db        *bolt.DB
	directory string
	limits    Limits
	slots     chan struct{}
	fault     func(error)
}

func Init(tx *bolt.Tx) error {
	for _, name := range [][]byte{files, chunks, references, receipts, staged, owners} {
		format := string(tx.Bucket([]byte("meta")).Get([]byte("format")))
		if (format == "2" || format == "3") && tx.Bucket(name) == nil {
			return ErrIntegrity
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return nil
}

func New(db *bolt.DB, directory string, limits Limits, fault func(error)) (*Store, error) {
	limits = limits.Defaults()
	if limits.MaxBytes < 1 || limits.MaxBytes > 256<<20 || limits.TotalBytes < limits.MaxBytes || limits.TotalBytes > 1<<40 || limits.MaxFiles < 1 || limits.MaxFiles > 100000 || limits.Transfers < 1 || limits.Transfers > 16 {
		return nil, errors.New("invalid file limits")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	// Only this database owner uses this directory. Never traverse a client path.
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(), "kairos-") {
			continue
		}
		if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil {
			return nil, err
		}
	}
	return &Store{db: db, directory: directory, limits: limits, slots: make(chan struct{}, limits.Transfers), fault: fault}, nil
}

func (s *Store) Limits() Limits { return s.limits }

func (s *Store) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case s.slots <- struct{}{}:
		return func() { <-s.slots }, nil
	default:
		return nil, ErrBusy
	}
}

func (s *Store) storageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrTooLarge) || errors.Is(err, ErrQuota) || errors.Is(err, ErrBusy) || errors.Is(err, ErrConflict) || errors.Is(err, ErrReferenced) || errors.Is(err, ErrInvalid) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if s.fault != nil {
		s.fault(err)
	}
	if errors.Is(err, ErrIntegrity) {
		return err
	}
	return ErrUnavailable
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

type checkedWriter struct {
	writer io.Writer
	err    error
}

func (w *checkedWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}

type tempFile struct {
	*os.File
	release func()
	once    sync.Once
	err     error
}

func (f *tempFile) Close() error {
	f.once.Do(func() {
		f.err = f.File.Close()
		if err := os.Remove(f.File.Name()); f.err == nil {
			f.err = err
		}
		if f.release != nil {
			f.release()
		}
	})
	return f.err
}

func normalize(meta Metadata) (Metadata, error) {
	if meta.Name == "" {
		meta.Name = "file.bin"
	}
	if !utf8.ValidString(meta.Name) || len(meta.Name) > 255 || meta.Name == "." || meta.Name == ".." || strings.ContainsAny(meta.Name, "/\\") {
		return meta, ErrInvalid
	}
	for _, r := range meta.Name {
		if r < 32 || r == 127 {
			return meta, ErrInvalid
		}
	}
	if meta.MediaType == "" {
		meta.MediaType = "application/octet-stream"
	}
	if len(meta.MediaType) > 255 || len(meta.ContentEncoding) > 64 || strings.ContainsAny(meta.ContentEncoding, "\r\n\x00") {
		return meta, ErrInvalid
	}
	kind, params, err := mime.ParseMediaType(meta.MediaType)
	if err != nil {
		return meta, ErrInvalid
	}
	meta.MediaType = mime.FormatMediaType(kind, params)
	return meta, nil
}

func ValidateMetadata(meta Metadata) error { _, err := normalize(meta); return err }

func digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

// Put spools a bounded body before obtaining the write transaction. Metadata,
// all chunks, the quota counter, receipt, and ULID sequence commit together.
func (s *Store) Put(ctx context.Context, body io.Reader, meta Metadata, expectedSHA, key, owner string) (Metadata, bool, error) {
	meta, err := normalize(meta)
	if err != nil {
		return Metadata{}, false, err
	}
	if (expectedSHA != "" && !digest(expectedSHA)) || len(key) > 256 || strings.ContainsAny(key, "\r\n") {
		return Metadata{}, false, ErrInvalid
	}
	release, err := s.acquire(ctx)
	if err != nil {
		return Metadata{}, false, err
	}
	defer release()
	file, err := os.CreateTemp(s.directory, "kairos-upload-*")
	if err != nil {
		return Metadata{}, false, s.storageError(err)
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	hasher := sha256.New()
	destination := &checkedWriter{writer: file}
	n, err := io.Copy(io.MultiWriter(destination, hasher), io.LimitReader(contextReader{ctx, body}, s.limits.MaxBytes+1))
	if err != nil {
		if destination.err != nil {
			return Metadata{}, false, s.storageError(destination.err)
		}
		if ctx.Err() != nil {
			return Metadata{}, false, ctx.Err()
		}
		return Metadata{}, false, ErrRead
	}
	if n > s.limits.MaxBytes {
		return Metadata{}, false, ErrTooLarge
	}
	meta.Size, meta.SHA256 = n, hex.EncodeToString(hasher.Sum(nil))
	if expectedSHA != "" && meta.SHA256 != expectedSHA {
		return Metadata{}, false, ErrInvalid
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return Metadata{}, false, s.storageError(err)
	}
	duplicate := false
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		receiptKey := ""
		if key != "" {
			sum := sha256.Sum256([]byte(key))
			receiptKey = hex.EncodeToString(sum[:])
			if id := tx.Bucket(receipts).Get([]byte(receiptKey)); id != nil {
				existing, err := get(tx, string(id))
				if err != nil {
					return ErrIntegrity
				}
				if existing.Owner != "" || existing.Metadata.SHA256 != meta.SHA256 || existing.Metadata.Name != meta.Name || existing.Metadata.MediaType != meta.MediaType || existing.Metadata.ContentEncoding != meta.ContentEncoding {
					return ErrConflict
				}
				meta, duplicate = existing.Metadata, true
				return nil
			}
		}
		if owner != "" {
			data := tx.Bucket([]byte("runs")).Get([]byte(owner))
			var state struct {
				Run struct {
					State string `json:"state"`
				} `json:"run"`
			}
			if data == nil || json.Unmarshal(data, &state) != nil || state.Run.State != "running" {
				return ErrInvalid
			}
		}
		if used(tx)+meta.Size > s.limits.TotalBytes || tx.Bucket(files).Stats().KeyN >= s.limits.MaxFiles {
			return ErrQuota
		}
		meta.CreatedAt = time.Now().UTC()
		id, err := identity.Next(tx, meta.CreatedAt)
		if err != nil {
			return err
		}
		meta.ID = id
		bucket, err := tx.Bucket(chunks).CreateBucket([]byte(id))
		if err != nil {
			return err
		}
		buf := make([]byte, chunkSize)
		storedHash := sha256.New()
		var storedSize int64
		for index := uint64(0); ; index++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			n, err := io.ReadFull(file, buf)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				return err
			}
			if n > 0 {
				storedSize += int64(n)
				_, _ = storedHash.Write(buf[:n])
				var k [8]byte
				binary.BigEndian.PutUint64(k[:], index)
				if err := bucket.Put(k[:], append([]byte(nil), buf[:n]...)); err != nil {
					return err
				}
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
		}
		if storedSize != meta.Size || hex.EncodeToString(storedHash.Sum(nil)) != meta.SHA256 {
			return ErrIntegrity
		}
		if err := write(tx, record{meta, owner, receiptKey}); err != nil {
			return err
		}
		if err := setUsed(tx, used(tx)+meta.Size); err != nil {
			return err
		}
		if receiptKey != "" {
			if err := tx.Bucket(receipts).Put([]byte(receiptKey), []byte(id)); err != nil {
				return err
			}
		}
		if owner != "" {
			if err := tx.Bucket(staged).Put([]byte(owner+"/"+id), []byte(id)); err != nil {
				return err
			}
		}
		return ctx.Err()
	})
	if err != nil {
		return Metadata{}, false, s.storageError(err)
	}
	return meta, duplicate, nil
}

func get(tx *bolt.Tx, id string) (record, error) {
	var saved record
	data := tx.Bucket(files).Get([]byte(id))
	if data == nil {
		return saved, ErrNotFound
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		return saved, ErrIntegrity
	}
	return saved, nil
}
func write(tx *bolt.Tx, saved record) error {
	data, err := json.Marshal(saved)
	if err != nil {
		return err
	}
	return tx.Bucket(files).Put([]byte(saved.Metadata.ID), data)
}
func used(tx *bolt.Tx) int64 {
	data := tx.Bucket([]byte("meta")).Get([]byte("file_bytes"))
	if len(data) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data))
}
func setUsed(tx *bolt.Tx, n int64) error {
	if n < 0 {
		return ErrIntegrity
	}
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], uint64(n))
	return tx.Bucket([]byte("meta")).Put([]byte("file_bytes"), data[:])
}

func (s *Store) Get(id string) (Metadata, error) {
	var saved record
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		saved, err = get(tx, id)
		if err != nil {
			return err
		}
		if saved.Owner != "" {
			return ErrNotFound
		}
		return nil
	})
	return saved.Metadata, s.storageError(err)
}

// Read copies a stable database snapshot into a temporary file and checks its
// digest before returning bytes. A slow network client never holds a DB read tx.
func (s *Store) Read(ctx context.Context, id, owner string) (Metadata, io.ReadSeekCloser, error) {
	release, err := s.acquire(ctx)
	if err != nil {
		return Metadata{}, nil, err
	}
	file, err := os.CreateTemp(s.directory, "kairos-read-*")
	if err != nil {
		release()
		return Metadata{}, nil, s.storageError(err)
	}
	reader := &tempFile{File: file, release: release}
	var saved record
	err = s.db.View(func(tx *bolt.Tx) error {
		var err error
		saved, err = get(tx, id)
		if err != nil {
			return err
		}
		if saved.Owner != "" && saved.Owner != owner {
			return ErrNotFound
		}
		return copyVerified(ctx, tx, saved.Metadata, file)
	})
	if err != nil {
		_ = reader.Close()
		return Metadata{}, nil, s.storageError(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = reader.Close()
		return Metadata{}, nil, s.storageError(err)
	}
	return saved.Metadata, reader, nil
}

func copyVerified(ctx context.Context, tx *bolt.Tx, meta Metadata, destination io.Writer) error {
	bucket := tx.Bucket(chunks).Bucket([]byte(meta.ID))
	if bucket == nil {
		return ErrIntegrity
	}
	hasher := sha256.New()
	target := io.MultiWriter(destination, hasher)
	var size int64
	var index uint64
	err := bucket.ForEach(func(k, data []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(k) != 8 || binary.BigEndian.Uint64(k) != index || len(data) == 0 || len(data) > chunkSize {
			return ErrIntegrity
		}
		index++
		size += int64(len(data))
		if size > meta.Size {
			return ErrIntegrity
		}
		_, err := target.Write(data)
		return err
	})
	if err != nil {
		return err
	}
	if size != meta.Size || hex.EncodeToString(hasher.Sum(nil)) != meta.SHA256 {
		return ErrIntegrity
	}
	return nil
}

func (s *Store) List(limit int, before string) ([]Metadata, error) {
	limit = min(100, max(1, limit))
	out := []Metadata{}
	err := s.db.View(func(tx *bolt.Tx) error {
		cursor := tx.Bucket(files).Cursor()
		k, v := cursor.Last()
		if before != "" {
			k, v = cursor.Seek([]byte(before))
			if k == nil {
				k, v = cursor.Last()
			} else {
				k, v = cursor.Prev()
			}
		}
		for ; k != nil && len(out) < limit; k, v = cursor.Prev() {
			var saved record
			if err := json.Unmarshal(v, &saved); err != nil {
				return ErrIntegrity
			}
			if saved.Owner == "" {
				out = append(out, saved.Metadata)
			}
		}
		return nil
	})
	return out, s.storageError(err)
}

func (s *Store) Delete(id string) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		saved, err := get(tx, id)
		if err != nil {
			return err
		}
		if saved.Owner != "" {
			return ErrNotFound
		}
		prefix := id + "\x00"
		key, _ := tx.Bucket(references).Cursor().Seek([]byte(prefix))
		if strings.HasPrefix(string(key), prefix) {
			return ErrReferenced
		}
		return remove(tx, saved)
	})
	return s.storageError(err)
}

func remove(tx *bolt.Tx, saved record) error {
	prefix := saved.Metadata.ID + "\x00"
	cursor := tx.Bucket(references).Cursor()
	var referenceKeys [][]byte
	for key, _ := cursor.Seek([]byte(prefix)); strings.HasPrefix(string(key), prefix); key, _ = cursor.Next() {
		owner := string(key[len(prefix):])
		if err := tx.Bucket(owners).Delete([]byte(owner + "\x00" + saved.Metadata.ID)); err != nil {
			return err
		}
		referenceKeys = append(referenceKeys, append([]byte(nil), key...))
	}
	for _, key := range referenceKeys {
		if err := tx.Bucket(references).Delete(key); err != nil {
			return err
		}
	}
	if err := tx.Bucket(chunks).DeleteBucket([]byte(saved.Metadata.ID)); err != nil {
		return err
	}
	if err := tx.Bucket(files).Delete([]byte(saved.Metadata.ID)); err != nil {
		return err
	}
	if saved.Receipt != "" {
		if err := tx.Bucket(receipts).Delete([]byte(saved.Receipt)); err != nil {
			return err
		}
	}
	if saved.Owner != "" {
		if err := tx.Bucket(staged).Delete([]byte(saved.Owner + "/" + saved.Metadata.ID)); err != nil {
			return err
		}
	}
	return setUsed(tx, used(tx)-saved.Metadata.Size)
}

func (s *Store) Verify(ctx context.Context, tx *bolt.Tx) error {
	last, err := identity.Last(tx)
	if err != nil {
		return err
	}
	var total int64
	err = tx.Bucket(files).ForEach(func(k, data []byte) error {
		var saved record
		if json.Unmarshal(data, &saved) != nil || string(k) != saved.Metadata.ID || !identity.Valid(saved.Metadata.ID) || saved.Metadata.ID > last || saved.Metadata.Size < 0 {
			return ErrIntegrity
		}
		if err := copyVerified(ctx, tx, saved.Metadata, io.Discard); err != nil {
			return err
		}
		if saved.Owner != "" {
			if tx.Bucket(staged).Get([]byte(saved.Owner+"/"+saved.Metadata.ID)) == nil {
				return ErrIntegrity
			}
			var run struct {
				Run struct {
					State       string `json:"state"`
					ResumeCount int    `json:"resume_count"`
				} `json:"run"`
			}
			if json.Unmarshal(tx.Bucket([]byte("runs")).Get([]byte(saved.Owner)), &run) != nil || (run.Run.State != "running" && !(run.Run.State == "queued" && run.Run.ResumeCount > 0)) {
				return ErrIntegrity
			}
		}
		if saved.Receipt != "" && string(tx.Bucket(receipts).Get([]byte(saved.Receipt))) != saved.Metadata.ID {
			return ErrIntegrity
		}
		total += saved.Metadata.Size
		return nil
	})
	if err != nil {
		return err
	}
	if total != used(tx) {
		return ErrIntegrity
	}
	if tx.Bucket(chunks).Stats().BucketN != tx.Bucket(files).Stats().KeyN+1 {
		return ErrIntegrity
	}
	if err := tx.Bucket(references).ForEach(func(key, _ []byte) error {
		pair := strings.SplitN(string(key), "\x00", 2)
		if len(pair) != 2 || string(tx.Bucket(owners).Get([]byte(pair[1]+"\x00"+pair[0]))) != pair[0] {
			return ErrIntegrity
		}
		if _, err := get(tx, pair[0]); err != nil {
			return ErrIntegrity
		}
		return nil
	}); err != nil {
		return err
	}
	if tx.Bucket(references).Stats().KeyN != tx.Bucket(owners).Stats().KeyN {
		return ErrIntegrity
	}
	if err := tx.Bucket(staged).ForEach(func(key, id []byte) error {
		saved, err := get(tx, string(id))
		if err != nil || saved.Owner == "" || string(key) != saved.Owner+"/"+saved.Metadata.ID {
			return ErrIntegrity
		}
		return nil
	}); err != nil {
		return err
	}
	if err := tx.Bucket(receipts).ForEach(func(key, id []byte) error {
		saved, err := get(tx, string(id))
		if err != nil || saved.Owner != "" || saved.Receipt != string(key) {
			return ErrIntegrity
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// Snapshot completes a consistent backup before its download begins.
func (s *Store) Snapshot(ctx context.Context) (io.ReadSeekCloser, int64, string, error) {
	release, err := s.acquire(ctx)
	if err != nil {
		return nil, 0, "", err
	}
	file, err := os.CreateTemp(s.directory, "kairos-backup-*")
	if err != nil {
		release()
		return nil, 0, "", s.storageError(err)
	}
	reader := &tempFile{File: file, release: release}
	hasher := sha256.New()
	var size int64
	err = s.db.View(func(tx *bolt.Tx) error {
		var err error
		size, err = tx.WriteTo(contextWriter{ctx, io.MultiWriter(file, hasher)})
		return err
	})
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		_, err = file.Seek(0, io.SeekStart)
	}
	if err != nil {
		_ = reader.Close()
		return nil, 0, "", s.storageError(err)
	}
	return reader, size, fmt.Sprintf("%x", hasher.Sum(nil)), nil
}
