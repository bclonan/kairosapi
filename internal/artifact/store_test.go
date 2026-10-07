package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bclonan/kairosapi/internal/identity"
	bolt "go.etcd.io/bbolt"
)

func testStore(t *testing.T, limits Limits, fault func(error)) *Store {
	t.Helper()
	root := t.TempDir()
	db, err := bolt.Open(filepath.Join(root, "files.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{"meta", "runs"} {
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		return Init(tx)
	}); err != nil {
		t.Fatal(err)
	}
	s, err := New(db, filepath.Join(root, "spool"), limits, fault)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func upload(t *testing.T, s *Store, data []byte, key, owner string) Metadata {
	t.Helper()
	meta, _, err := s.Put(context.Background(), bytes.NewReader(data), Metadata{Name: "payload.bin"}, "", key, owner)
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func TestBinaryChunksDigestAndIdempotency(t *testing.T) {
	s := testStore(t, Limits{}, nil)
	data := make([]byte, chunkSize*3+119)
	for i := range data {
		data[i] = byte(i*37 + i/chunkSize)
	}
	meta := upload(t, s, data, "same", "")
	if !identity.Valid(meta.ID) {
		t.Fatal(meta)
	}
	sum := sha256.Sum256(data)
	if meta.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("digest differs")
	}
	_, reader, err := s.Read(context.Background(), meta.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("binary data changed")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() { _ = reader.Close() })
	}
	wg.Wait()
	replayed, duplicate, err := s.Put(context.Background(), bytes.NewReader(data), Metadata{Name: "payload.bin"}, meta.SHA256, "same", "")
	if err != nil || !duplicate || replayed.ID != meta.ID {
		t.Fatalf("replay %v %v", duplicate, err)
	}
	if _, _, err := s.Put(context.Background(), bytes.NewReader([]byte("different")), Metadata{Name: "payload.bin"}, "", "same", ""); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error { return s.Verify(context.Background(), tx) }); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(s.directory)
	if len(entries) != 0 {
		t.Fatal("spool leaked")
	}
}

type brokenReader struct{ sent bool }

func (r *brokenReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.ErrUnexpectedEOF
	}
	r.sent = true
	return copy(p, []byte("incomplete")), nil
}

func TestFailedUploadsLeaveNoRecordsQuotaOrIDs(t *testing.T) {
	s := testStore(t, Limits{MaxBytes: 16, TotalBytes: 16, MaxFiles: 1}, nil)
	for _, reader := range []io.Reader{&brokenReader{}, strings.NewReader(strings.Repeat("x", 17))} {
		if _, _, err := s.Put(context.Background(), reader, Metadata{}, "", "", ""); err == nil {
			t.Fatal("accepted incomplete or oversized input")
		}
	}
	if _, _, err := s.Put(context.Background(), strings.NewReader("bad hash"), Metadata{}, strings.Repeat("0", 64), "", ""); err != ErrInvalid {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.Put(ctx, strings.NewReader("canceled"), Metadata{}, "", "", ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if used(tx) != 0 || tx.Bucket(files).Stats().KeyN != 0 || tx.Bucket([]byte("meta")).Get([]byte("last_ulid")) != nil {
			t.Fatal("failed upload changed durable state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	meta := upload(t, s, []byte("1234567890123456"), "", "")
	if _, _, err := s.Put(context.Background(), strings.NewReader("x"), Metadata{}, "", "", ""); err != ErrQuota {
		t.Fatal(err)
	}
	if err := s.Delete(meta.ID); err != nil {
		t.Fatal(err)
	}
	upload(t, s, []byte("replacement"), "", "")
}

func TestStagingAndFinalStateShareRollbackBoundary(t *testing.T) {
	s := testStore(t, Limits{}, nil)
	owner := identity.New()
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("runs")).Put([]byte(owner), []byte(`{"run":{"state":"running"}}`))
	}); err != nil {
		t.Fatal(err)
	}
	kept := upload(t, s, []byte("keep"), "", owner)
	discarded := upload(t, s, []byte("discard"), "", owner)
	if _, err := s.Get(kept.ID); err != ErrNotFound {
		t.Fatal("staged file is public")
	}
	_, reader, err := s.Read(context.Background(), kept.ID, owner)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	reject := errors.New("injected transaction failure")
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := Finalize(tx, owner, kept.Reference()); err != nil {
			return err
		}
		if err := tx.Bucket([]byte("runs")).Put([]byte(owner), []byte(`{"run":{"state":"succeeded"}}`)); err != nil {
			return err
		}
		return reject
	}); err != reject {
		t.Fatal(err)
	}
	if _, err := s.Get(kept.ID); err != ErrNotFound {
		t.Fatal("rollback published bytes")
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		if used(tx) != 11 || !bytes.Contains(tx.Bucket([]byte("runs")).Get([]byte(owner)), []byte("running")) {
			t.Fatal("rollback changed quota or run")
		}
		return s.Verify(context.Background(), tx)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := Finalize(tx, owner, kept.Reference()); err != nil {
			return err
		}
		return tx.Bucket([]byte("runs")).Put([]byte(owner), []byte(`{"run":{"state":"succeeded"}}`))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(kept.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(discarded.ID); err != ErrNotFound {
		t.Fatal("unreferenced staged bytes retained")
	}
	if err := s.Delete(kept.ID); err != ErrReferenced {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error { return Unpin(tx, "run/"+owner) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(kept.ID); err != nil {
		t.Fatal(err)
	}
}

func TestChecksumCorruptionFailsBeforeBytesEscape(t *testing.T) {
	var faults int
	s := testStore(t, Limits{}, func(error) { faults++ })
	meta := upload(t, s, []byte("original"), "", "")
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(chunks).Bucket([]byte(meta.ID))
		k, _ := bucket.Cursor().First()
		return bucket.Put(k, []byte("tampered"))
	}); err != nil {
		t.Fatal(err)
	}
	_, reader, err := s.Read(context.Background(), meta.ID, "")
	if !errors.Is(err, ErrIntegrity) || reader != nil || faults != 1 {
		t.Fatalf("%v %v %d", reader, err, faults)
	}
	if err := s.db.View(func(tx *bolt.Tx) error { return s.Verify(context.Background(), tx) }); err != ErrIntegrity {
		t.Fatal(err)
	}
}

func TestMultipartPreservesFileBytes(t *testing.T) {
	s := testStore(t, Limits{Transfers: 1}, nil)
	data := []byte{0, 255, 1, 2, 128}
	meta := upload(t, s, data, "", "")
	reader, size, media, err := s.Multipart(context.Background(), map[string][]string{"tag": {"a", "b"}}, []Part{{Field: "document", ID: meta.ID}}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	_, params, err := mime.ParseMediaType(media)
	if err != nil {
		t.Fatal(err)
	}
	parsed := multipart.NewReader(reader, params["boundary"])
	var tags []string
	found := false
	for {
		part, err := parsed.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(part)
		if err != nil {
			t.Fatal(err)
		}
		if part.FormName() == "tag" {
			tags = append(tags, string(got))
		} else {
			found = part.FileName() == meta.Name && bytes.Equal(got, data)
		}
	}
	if !found || strings.Join(tags, ",") != "a,b" || size <= int64(len(data)) {
		t.Fatal("multipart content changed")
	}
	if _, _, err := s.Read(context.Background(), meta.ID, ""); err != ErrBusy {
		t.Fatal("transfer bound not enforced")
	}
}

func TestSnapshotRestoresExactFileAndCleansAbandonedSpool(t *testing.T) {
	s := testStore(t, Limits{}, nil)
	meta := upload(t, s, []byte("recover me"), "stable", "")
	reader, size, digest, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	sum := sha256.Sum256(data)
	if err != nil || int64(len(data)) != size || hex.EncodeToString(sum[:]) != digest {
		t.Fatal("invalid backup")
	}
	path := filepath.Join(t.TempDir(), "restored.db")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dir := path + ".uploads"
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "kairos-upload-abandoned"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	restored, err := New(db, dir, Limits{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *bolt.Tx) error { return restored.Verify(context.Background(), tx) }); err != nil {
		t.Fatal(err)
	}
	_, content, err := restored.Read(context.Background(), meta.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(content)
	content.Close()
	if string(got) != "recover me" {
		t.Fatal("backup content differs")
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Fatal("abandoned spool retained")
	}
}
