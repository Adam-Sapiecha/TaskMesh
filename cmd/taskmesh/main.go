package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/api"
	"github.com/Adam-Sapiecha/TaskMesh/internal/handlers"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
	"github.com/Adam-Sapiecha/TaskMesh/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("process failed", "reason", err.Error())
		os.Exit(1)
	}
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: taskmesh api|worker|controller|migrate|probe")
	}
	mode := os.Args[1]
	if mode == "probe" {
		client := http.Client{Timeout: 3 * time.Second}
		resp, err := client.Get("http://127.0.0.1:" + env("PORT", "8080") + "/readyz")
		if err != nil {
			return errors.New("health endpoint unavailable")
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return errors.New("service not ready")
		}
		return nil
	}
	if mode != "api" && mode != "worker" && mode != "controller" && mode != "migrate" {
		return errors.New("unknown process mode")
	}
	if os.Getenv("DATABASE_URL") == "" {
		return errors.New("DATABASE_URL is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var s *storage.Store
	var err error
	for attempt := 0; attempt < 12; attempt++ {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		s, err = storage.Open(call, os.Getenv("DATABASE_URL"))
		cancel()
		if err == nil {
			break
		}
		slog.Warn("database unavailable; waiting")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
	if err != nil {
		return errors.New("database startup retry budget exhausted")
	}
	defer s.Close()
	if mode == "migrate" {
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err = s.Migrate(call); err != nil {
			slog.Error("migration failed; inspect schema and migration checksums")
			return errors.New("migration failed")
		}
		if os.Getenv("TASKMESH_GRANT_RUNTIME_ROLE") == "1" {
			if err = s.GrantRuntime(call, "taskmesh_app"); err != nil {
				return errors.New("runtime role grants failed")
			}
		}
		slog.Info("migrations applied")
		return nil
	}
	mux := http.NewServeMux()
	api.Health(mux, s)
	var handler http.Handler = mux
	if mode == "api" {
		token := os.Getenv("TASKMESH_API_KEY")
		if len(token) < 32 {
			return errors.New("TASKMESH_API_KEY must contain at least 32 bytes")
		}
		handler = api.Server{Store: s, Registry: handlers.Builtins(), Token: token}.Handler()
	}
	server := http.Server{Addr: ":" + env("PORT", "8080"), Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384}
	httpErrors := make(chan error, 1)
	go func() { httpErrors <- server.ListenAndServe() }()
	finished := make(chan error, 1)
	switch mode {
	case "worker":
		cfg := worker.DefaultConfig()
		cfg.Name = env("WORKER_NAME", "worker")
		cfg.Queues = strings.Split(env("WORKER_QUEUES", "default,reports,analytics"), ",")
		cfg.Concurrency, err = strconv.Atoi(env("WORKER_CONCURRENCY", "2"))
		if err != nil {
			return errors.New("invalid WORKER_CONCURRENCY")
		}
		if err = cfg.Validate(); err != nil {
			return err
		}
		go func() { finished <- worker.Run(ctx, s, handlers.Builtins(), cfg) }()
	case "controller":
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					finished <- nil
					return
				case <-ticker.C:
					call, cancel := context.WithTimeout(ctx, 4*time.Second)
					n, err := s.Recover(call)
					cancel()
					if err != nil {
						slog.Warn("recovery scan unavailable")
					} else if n > 0 {
						slog.Info("expired leases recovered", "count", n)
					}
				}
			}
		}()
	}
	var result error
	processFinished := false
	select {
	case <-ctx.Done():
	case err = <-httpErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			result = fmt.Errorf("HTTP listener failed")
		}
	case err = <-finished:
		result = err
		processFinished = true
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	if mode != "api" && !processFinished {
		select {
		case err = <-finished:
			if result == nil {
				result = err
			}
		case <-time.After(35 * time.Second):
		}
	}
	return result
}
