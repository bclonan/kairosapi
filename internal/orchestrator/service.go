// Package orchestrator stores versioned specifications and schedules bounded runs.
// One service owns one database file. It does not replay uncertain external effects.
package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/workflow"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrNotFound    = errors.New("not found")
	ErrConflict    = errors.New("immutable version or idempotency key conflicts with existing content")
	ErrUnavailable = errors.New("orchestrator is stopping or storage is unavailable")
	ErrHistoryFull = errors.New("retained history capacity exhausted")
	ErrInvalid     = errors.New("invalid run request")
)

var definitionsBucket = []byte("definitions")
var runsBucket = []byte("runs")
var pendingBucket = []byte("pending")
var receiptsBucket = []byte("receipts")
var expiryBucket = []byte("expiry")
var orderBucket = []byte("run_order")
var summariesBucket = []byte("run_summaries")
var publicationsBucket = []byte("publications")

type Options struct {
	Logger         *slog.Logger
	Path           string
	Workers        int
	MaxRuns        int
	QueueSize      int
	MaxRecords     int
	Retention      time.Duration
	Timeout        time.Duration
	Files          artifact.Limits
	ConfigureFiles func(*artifact.Store) error
}

type Service struct {
	mu            sync.Mutex
	db            *bolt.DB
	options       Options
	registry      workflow.Registry
	plans         map[string]*workflow.Plan
	definitions   map[string]workflow.Definition
	latest        map[string]int
	engine        *workflow.Engine
	ctx           context.Context
	cancel        context.CancelFunc
	wake          chan struct{}
	changed       chan struct{}
	active        map[string]context.CancelFunc
	wg            sync.WaitGroup
	closing       bool
	storageFailed bool
	files         *artifact.Store
	publications  map[string]string
}

func Open(options Options, registry workflow.Registry, seeds []workflow.Definition) (*Service, error) {
	if options.Path == "" || options.QueueSize < 0 || options.QueueSize > 4096 || options.MaxRecords < options.MaxRuns+options.QueueSize || options.MaxRecords > 100000 || options.Retention < time.Minute || options.Retention > 365*24*time.Hour {
		return nil, errors.New("invalid database path, queue, history capacity, or retention")
	}
	engine, err := workflow.New(options.Workers, options.MaxRuns)
	if err != nil {
		return nil, err
	}
	if options.Timeout < time.Second || options.Timeout > 5*time.Minute {
		return nil, errors.New("invalid maximum run timeout")
	}
	if err := os.MkdirAll(filepath.Dir(options.Path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(options.Path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{db: db, options: options, registry: registry, engine: engine, ctx: ctx, cancel: cancel,
		plans: map[string]*workflow.Plan{}, definitions: map[string]workflow.Definition{}, latest: map[string]int{},
		publications: map[string]string{},
		wake:         make(chan struct{}, options.MaxRuns), changed: make(chan struct{}), active: map[string]context.CancelFunc{}}
	err = db.View(func(tx *bolt.Tx) error {
		var first error
		for problem := range tx.Check() {
			if first == nil {
				first = problem
			}
		}
		return first
	})
	if err == nil {
		err = s.initialize()
	}
	if err == nil {
		s.files, err = artifact.New(db, options.Path+".uploads", options.Files, func(error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.failStorage()
		})
	}
	if err == nil {
		err = db.View(func(tx *bolt.Tx) error { return s.files.Verify(ctx, tx) })
	}
	if err == nil && options.ConfigureFiles != nil {
		err = options.ConfigureFiles(s.files)
	}
	if err == nil {
		err = s.verifyIndexes()
	}
	if err == nil {
		err = s.loadCatalog(seeds)
	}
	if err == nil {
		err = s.recoverRuns()
	}
	if err != nil {
		cancel()
		_ = db.Close()
		return nil, err
	}
	for range options.MaxRuns {
		s.wg.Add(1)
		go s.worker()
	}
	s.signal()
	return s, nil
}

func (s *Service) Files() *artifact.Store { return s.files }

func (s *Service) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrUnavailable
	}
	s.closing = true
	s.cancel()
	s.notify()
	s.mu.Unlock()
	s.wg.Wait()
	return s.db.Close()
}

func (s *Service) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closing && !s.storageFailed
}

// notify is called with mu held. Waiters observe changes without polling.
func (s *Service) notify() { close(s.changed); s.changed = make(chan struct{}) }

func (s *Service) signal() {
	for range s.options.MaxRuns {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

func (s *Service) failStorage() { s.storageFailed = true; s.cancel(); s.notify() }
