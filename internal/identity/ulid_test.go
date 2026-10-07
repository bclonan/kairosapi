package identity

import (
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	bolt "go.etcd.io/bbolt"
)

func TestPersistentSequenceRestartRollbackAndClockReversal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids.db")
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucket([]byte("meta")); return err }); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	var first, second, aborted string
	if err := db.Update(func(tx *bolt.Tx) error { var err error; first, err = Next(tx, now); return err }); err != nil {
		t.Fatal(err)
	}
	reject := errors.New("abort")
	if err := db.Update(func(tx *bolt.Tx) error {
		var err error
		aborted, err = Next(tx, now.Add(-time.Hour))
		if err != nil {
			return err
		}
		return reject
	}); err != reject {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error { var err error; second, err = Next(tx, now.Add(-24*time.Hour)); return err }); err != nil {
		t.Fatal(err)
	}
	if !Valid(first) || !Valid(second) || second <= first || second != aborted {
		t.Fatalf("sequence %s %s %s", first, second, aborted)
	}
}

func TestConcurrentULIDsUniqueAndCanonical(t *testing.T) {
	ids := make([]string, 1000)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func(i int) { defer wg.Done(); ids[i] = New() }(i)
	}
	wg.Wait()
	sort.Strings(ids)
	for i, id := range ids {
		if !Valid(id) || (i > 0 && id <= ids[i-1]) {
			t.Fatal("invalid or repeated ULID")
		}
	}
	for _, id := range []string{"", "8ZZZZZZZZZZZZZZZZZZZZZZZZZ", "01arz3ndektsv4rrffq69g5fav", "01234567890123456789012345!"} {
		if Valid(id) {
			t.Fatalf("accepted %q", id)
		}
	}
}

func TestEntropyOverflowAdvancesLogicalTimestamp(t *testing.T) {
	var last ulid.ULID
	if err := last.SetTime(1234); err != nil {
		t.Fatal(err)
	}
	for i := 6; i < len(last); i++ {
		last[i] = 255
	}
	next, err := advance(last, time.UnixMilli(1233))
	if err != nil || next.Time() != 1235 || next.Compare(last) <= 0 {
		t.Fatalf("%s %v", next, err)
	}
}
