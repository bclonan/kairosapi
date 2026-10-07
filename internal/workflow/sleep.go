package workflow

import (
	"context"
	"sync"
	"time"
)

type workerLeaseKey struct{}
type workerLease struct {
	mu    sync.Mutex
	slots chan struct{}
	held  bool
}

func (l *workerLease) release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held {
		<-l.slots
		l.held = false
	}
}

// Sleep yields the action worker slot while waiting, then reacquires it before
// the handler continues. Call it serially within a handler, not detached work.
func Sleep(ctx context.Context, delay time.Duration) error {
	lease, _ := ctx.Value(workerLeaseKey{}).(*workerLease)
	if lease != nil {
		lease.mu.Lock()
		defer lease.mu.Unlock()
		if lease.held {
			<-lease.slots
			lease.held = false
		}
	}
	timer := time.NewTimer(max(0, delay))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	if lease != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case lease.slots <- struct{}{}:
			lease.held = true
		}
	}
	return ctx.Err()
}
