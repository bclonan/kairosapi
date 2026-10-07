package resource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bclonan/kairosapi/internal/artifact"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func testLoader(t *testing.T) (*Loader, *artifact.Store, *bolt.DB) {
	t.Helper()
	root := t.TempDir()
	db, err := bolt.Open(filepath.Join(root, "resources.db"), 0600, nil)
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
		return artifact.Init(tx)
	}); err != nil {
		t.Fatal(err)
	}
	files, err := artifact.New(db, filepath.Join(root, "spool"), artifact.Limits{MaxBytes: 2 << 20, TotalBytes: 32 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	loader, err := New(files)
	if err != nil {
		t.Fatal(err)
	}
	return loader, files, db
}

func save(t *testing.T, files *artifact.Store, data []byte) artifact.Metadata {
	t.Helper()
	meta, _, err := files.Put(context.Background(), bytes.NewReader(data), artifact.Metadata{Name: "resource.bin"}, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func TestDictionaryIsolationConcurrentLoadsAndCancellation(t *testing.T) {
	loader, files, _ := testLoader(t)
	meta := save(t, files, []byte(`{"countries":{"US":{"currency":"USD","count":9007199254740993}},"a/b~c":42}`))
	const count = 24
	var wg sync.WaitGroup
	for range count {
		wg.Go(func() {
			value, err := loader.Lookup(context.Background(), meta.Reference(), "/countries/US")
			if err != nil {
				t.Error(err)
				return
			}
			selected := value.(map[string]any)
			if selected["currency"] != "USD" || selected["count"] != json.Number("9007199254740993") {
				t.Errorf("unexpected dictionary %v", selected)
			}
			selected["currency"] = "changed"
		})
	}
	wg.Wait()
	value, err := loader.Lookup(context.Background(), meta.Reference(), "/a~1b~0c")
	if err != nil || value != json.Number("42") {
		t.Fatalf("value=%v err=%v", value, err)
	}
	if loader.lru.Len() != 1 {
		t.Fatal("parallel loads should share one cached dictionary")
	}
	loader.gate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := loader.Lookup(ctx, meta.Reference(), ""); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-loader.gate
	if err := files.Delete(meta.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := loader.Lookup(context.Background(), meta.Reference(), ""); !errors.Is(err, artifact.ErrNotFound) {
		t.Fatalf("cache exposed a deleted resource: %v", err)
	}
}

func TestDictionaryValidationAndCacheLimits(t *testing.T) {
	loader, files, _ := testLoader(t)
	for _, source := range []string{`[]`, `null`, `{"same":1,"same":2}`, `{"nested":{"same":1,"same":2}}`} {
		meta := save(t, files, []byte(source))
		if _, err := loader.Lookup(context.Background(), meta.Reference(), ""); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted invalid dictionary %q: %v", source, err)
		}
	}
	large := save(t, files, []byte(strings.Repeat("x", MaxBytes+1)))
	if _, err := loader.Lookup(context.Background(), large.Reference(), ""); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	for i := 0; i < MaxEntries+5; i++ {
		meta := save(t, files, []byte(fmt.Sprintf(`{"index":%d}`, i)))
		if _, err := loader.Lookup(context.Background(), meta.Reference(), "/index"); err != nil {
			t.Fatal(err)
		}
	}
	if loader.lru.Len() != MaxEntries || len(loader.cache) != MaxEntries {
		t.Fatal("entry limit did not evict old resource data")
	}
	for range 10 {
		meta := save(t, files, []byte(`{"text":"`+strings.Repeat("a", MaxBytes-16)+`"}`))
		if _, err := loader.Lookup(context.Background(), meta.Reference(), "/text"); err != nil {
			t.Fatal(err)
		}
	}
	if loader.bytes > MaxCacheBytes || loader.lru.Len() >= 10 {
		t.Fatal("byte limit did not evict old resource data")
	}
}

func descriptorData(t *testing.T) []byte {
	t.Helper()
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name: proto.String("person.proto"), Package: proto.String("demo"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Person"), Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("name"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()}}}},
	}}}
	data, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDescriptorsResolveLocallyAndRejectInvalidGraphs(t *testing.T) {
	loader, files, db := testLoader(t)
	meta := save(t, files, descriptorData(t))
	message, types, err := loader.Message(context.Background(), meta.Reference(), "demo.Person")
	if err != nil || string(message.FullName()) != "demo.Person" || types == nil {
		t.Fatalf("%v %v", message, err)
	}
	if _, _, err := loader.Message(context.Background(), meta.Reference(), "demo.Missing"); err == nil {
		t.Fatal("unknown message was accepted")
	}
	if _, _, err := loader.Message(context.Background(), meta.Reference(), "../Person"); err == nil {
		t.Fatal("invalid name was accepted")
	}
	badSet := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{Name: proto.String("missing.proto"), Syntax: proto.String("proto3"), Dependency: []string{"not-installed.proto"}}}}
	data, _ := proto.Marshal(badSet)
	bad := save(t, files, data)
	if _, _, err := loader.Message(context.Background(), bad.Reference(), "demo.Person"); err == nil {
		t.Fatal("missing import was accepted")
	}
	// A new loader must verify the persisted bytes before parsing them.
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("file_chunks")).Bucket([]byte(meta.ID))
		key, value := bucket.Cursor().First()
		changed := append([]byte(nil), value...)
		changed[0] ^= 1
		return bucket.Put(key, changed)
	}); err != nil {
		t.Fatal(err)
	}
	fresh, _ := New(files)
	if _, _, err := fresh.Message(context.Background(), meta.Reference(), "demo.Person"); !errors.Is(err, artifact.ErrIntegrity) {
		t.Fatalf("corruption was not caught: %v", err)
	}
}

func TestDescriptorComplexityAndCompressedResourceLimits(t *testing.T) {
	loader, files, _ := testLoader(t)
	compressed, _, err := files.Put(context.Background(), strings.NewReader(`{"ok":true}`), artifact.Metadata{Name: "dictionary.json", ContentEncoding: "gzip"}, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loader.Lookup(context.Background(), compressed.Reference(), ""); err == nil {
		t.Fatal("compressed dictionary was accepted")
	}
	set := &descriptorpb.FileDescriptorSet{}
	for i := 0; i < MaxDescriptorFiles+1; i++ {
		set.File = append(set.File, &descriptorpb.FileDescriptorProto{Name: proto.String(fmt.Sprintf("part%d.proto", i)), Syntax: proto.String("proto3")})
	}
	data, _ := proto.Marshal(set)
	if _, err := parseDescriptors(data); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	set.File = set.File[:1]
	set.File[0].MessageType = []*descriptorpb.DescriptorProto{{Name: proto.String("Huge")}}
	for i := 1; i <= MaxDescriptorNodes; i++ {
		set.File[0].MessageType[0].Field = append(set.File[0].MessageType[0].Field, &descriptorpb.FieldDescriptorProto{Name: proto.String(fmt.Sprintf("field%d", i)), Number: proto.Int32(int32(i)), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()})
	}
	data, _ = proto.Marshal(set)
	if _, err := parseDescriptors(data); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}
