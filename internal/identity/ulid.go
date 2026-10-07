// Package identity allocates monotonic ULIDs. Persistent allocation participates
// in the caller's transaction, so an uncommitted identifier is never published.
package identity

import (
	"crypto/rand"
	"errors"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	bolt "go.etcd.io/bbolt"
)

var local struct {
	sync.Mutex
	last ulid.ULID
}

func advance(last ulid.ULID, now time.Time) (ulid.ULID, error) {
	ms := uint64(max(0, now.UnixMilli()))
	if ms > ulid.MaxTime() {
		return ulid.ULID{}, errors.New("ULID timestamp exhausted")
	}
	if last == (ulid.ULID{}) || ms > last.Time() {
		return ulid.New(ms, rand.Reader)
	}
	next := last
	for i := len(next) - 1; i >= 6; i-- {
		next[i]++
		if next[i] != 0 {
			return next, nil
		}
	}
	if last.Time() == ulid.MaxTime() {
		return ulid.ULID{}, errors.New("ULID sequence exhausted")
	}
	return ulid.New(last.Time()+1, rand.Reader)
}

func New() string {
	local.Lock()
	defer local.Unlock()
	next, err := advance(local.last, time.Now())
	if err != nil {
		panic(err)
	}
	local.last = next
	return next.String()
}

func Next(tx *bolt.Tx, now time.Time) (string, error) {
	meta := tx.Bucket([]byte("meta"))
	if meta == nil {
		return "", errors.New("identity metadata is unavailable")
	}
	var last ulid.ULID
	if data := meta.Get([]byte("last_ulid")); data != nil {
		if len(data) != len(last) {
			return "", errors.New("invalid persisted ULID sequence")
		}
		copy(last[:], data)
	}
	next, err := advance(last, now)
	if err != nil {
		return "", err
	}
	if err := meta.Put([]byte("last_ulid"), next[:]); err != nil {
		return "", err
	}
	return next.String(), nil
}

func Valid(id string) bool {
	parsed, err := ulid.ParseStrict(id)
	return err == nil && parsed.String() == id
}

func Last(tx *bolt.Tx) (string, error) {
	var last ulid.ULID
	data := tx.Bucket([]byte("meta")).Get([]byte("last_ulid"))
	if data == nil {
		return "", nil
	}
	if len(data) != len(last) {
		return "", errors.New("invalid persisted ULID sequence")
	}
	copy(last[:], data)
	return last.String(), nil
}
