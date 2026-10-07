package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bclonan/kairosapi/internal/action"
	"github.com/bclonan/kairosapi/internal/api"
	"github.com/bclonan/kairosapi/internal/artifact"
	"github.com/bclonan/kairosapi/internal/config"
	"github.com/bclonan/kairosapi/internal/orchestrator"
	"github.com/bclonan/kairosapi/internal/workflow"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := serve(logger); err != nil {
		logger.Error("server stopped", "error", err.Error())
		os.Exit(1)
	}
}

func serve(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	httpAction, err := action.NewHTTP(cfg.Origins, cfg.AllowPrivate, os.LookupEnv)
	if err != nil {
		return err
	}
	defer httpAction.Close()
	file, err := os.Open(cfg.WorkflowFile)
	if err != nil {
		return fmt.Errorf("open workflow file: %w", err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(data) > 1<<20 {
		return errors.New("workflow file could not be read or exceeds 1 MiB")
	}
	var definitions []workflow.Definition
	if err := workflow.Decode(data, &definitions); err != nil {
		return fmt.Errorf("decode workflow file: %w", err)
	}
	resources := action.NewResources()
	registry := workflow.Registry{"value": action.Value, "http": httpAction.Prepare, "delay": action.Delay, "dictionary": resources.Dictionary, "protobuf": resources.Protobuf}
	service, err := orchestrator.Open(orchestrator.Options{
		Logger: logger,
		Path:   cfg.DBPath, Workers: cfg.Workers, MaxRuns: cfg.MaxRuns, QueueSize: cfg.QueueSize,
		MaxRecords: cfg.MaxRecords, Retention: cfg.Retention, Timeout: cfg.RunTimeout,
		Files: cfg.Files, ConfigureFiles: func(files *artifact.Store) error {
			if err := httpAction.SetFiles(files); err != nil {
				return err
			}
			return resources.SetFiles(files)
		},
	}, registry, definitions)
	if err != nil {
		return err
	}
	defer service.Close()
	handler, err := api.New(service, cfg.Token, cfg.AdminToken, logger)
	if err != nil {
		return err
	}
	root, cancelRuns := context.WithCancel(context.Background())
	defer cancelRuns()
	server := &http.Server{
		Addr: cfg.Address, Handler: handler,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 45 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 << 10,
		BaseContext:    func(net.Listener) context.Context { return root },
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- server.ListenAndServe() }()
	logger.Info("server starting", "address", cfg.Address, "workflows", len(service.List()), "workers", cfg.Workers, "max_runs", cfg.MaxRuns, "queue_size", cfg.QueueSize)
	select {
	case err := <-errorsCh:
		return err
	case <-ctx.Done():
		logger.Info("shutdown requested")
		// Stop waiting HTTP clients. Service.Close records interrupted active runs;
		// queued runs remain in the database for the next process.
		cancelRuns()
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return err
		}
		if err := <-errorsCh; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
