package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/handlers"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type Config struct {
	Name                                 string
	Queues                               []string
	Concurrency                          int
	Poll, Lease, Renew, Heartbeat, Grace time.Duration
}

func DefaultConfig() Config {
	return Config{Name: "worker", Queues: []string{"default", "reports", "analytics"}, Concurrency: 2, Poll: 500 * time.Millisecond, Lease: 30 * time.Second, Renew: 10 * time.Second, Heartbeat: 5 * time.Second, Grace: 30 * time.Second}
}
func (c Config) Validate() error {
	if c.Concurrency < 1 || c.Concurrency > 16 || len(c.Queues) < 1 || len(c.Name) < 1 || len(c.Name) > 128 {
		return errors.New("invalid worker identity, queues or concurrency (1..16)")
	}
	if c.Poll <= 0 || c.Lease < time.Second || c.Renew <= 0 || c.Renew >= c.Lease || c.Heartbeat <= 0 || c.Grace <= 0 {
		return errors.New("invalid worker timing")
	}
	seen := map[string]bool{}
	for _, q := range c.Queues {
		if seen[q] {
			return errors.New("duplicate queue")
		}
		seen[q] = true
	}
	return nil
}
func NewID() pgtype.UUID {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return pgtype.UUID{Bytes: b, Valid: true}
}
func Run(ctx context.Context, s *storage.Store, registry handlers.Registry, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	id := NewID()
	if err := s.Register(ctx, id, cfg.Name, cfg.Queues, registry.Types(), cfg.Concurrency); err != nil {
		return err
	}
	slog.Info("worker registered", "name", cfg.Name, "id", id, "concurrency", cfg.Concurrency)
	// Runtime context survives the stop signal while in-flight work drains.
	runtimeCtx, stopRuntime := context.WithCancel(context.Background())
	defer stopRuntime()
	var heart sync.WaitGroup
	heart.Add(1)
	go func() {
		defer heart.Done()
		ticker := time.NewTicker(cfg.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-runtimeCtx.Done():
				return
			case <-ticker.C:
				call, cancel := context.WithTimeout(runtimeCtx, 3*time.Second)
				err := s.Heartbeat(call, id)
				cancel()
				if err != nil {
					slog.Warn("worker heartbeat unavailable")
				}
			}
		}
	}()
	var active sync.WaitGroup
	for slot := 0; slot < cfg.Concurrency; slot++ {
		active.Add(1)
		go func(offset int) {
			defer active.Done()
			for {
				if ctx.Err() != nil || runtimeCtx.Err() != nil {
					return
				}
				claimed := false
				for n := 0; n < len(cfg.Queues); n++ {
					queue := cfg.Queues[(n+offset)%len(cfg.Queues)]
					call, cancel := context.WithTimeout(ctx, 3*time.Second)
					job, err := s.Claim(call, id, queue, cfg.Lease)
					cancel()
					if err != nil {
						if !errors.Is(err, pgx.ErrNoRows) && ctx.Err() == nil {
							slog.Warn("claim unavailable", "queue", queue)
						}
						continue
					}
					claimed = true
					execute(runtimeCtx, s, registry, cfg, id, job)
					offset++
					break
				}
				if !claimed {
					timer := time.NewTimer(cfg.Poll)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}
		}(slot)
	}
	<-ctx.Done()
	done := make(chan struct{})
	go func() { active.Wait(); close(done) }()
	timer := time.NewTimer(cfg.Grace)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		stopRuntime()
		<-done
	}
	stopRuntime()
	heart.Wait()
	call, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.Stop(call, id)
}
func execute(parent context.Context, s *storage.Store, r handlers.Registry, cfg Config, id pgtype.UUID, job storage.Claim) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(job.TimeoutSeconds)*time.Second)
	defer cancel()
	renewDone := make(chan struct{})
	var renewWG sync.WaitGroup
	renewWG.Add(1)
	go func() {
		defer renewWG.Done()
		ticker := time.NewTicker(cfg.Renew)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-renewDone:
				return
			case <-ticker.C:
				call, stop := context.WithTimeout(ctx, 3*time.Second)
				err := s.Renew(call, id, job, cfg.Lease)
				stop()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	var result json.RawMessage
	var taskErr error
	permanent := false
	if err := r.Validate(job.Type, job.Payload); err != nil {
		taskErr = err
		permanent = true
	} else {
		result, taskErr = safeExecute(ctx, r[job.Type], job.Payload)
	}
	close(renewDone)
	renewWG.Wait()
	// A timed-out handler may return a value: never acknowledge it as success.
	if ctx.Err() != nil {
		taskErr = ctx.Err()
		result = nil
	}
	call, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := s.Finish(call, id, job, result, taskErr, permanent); err != nil {
		slog.Warn("result not acknowledged; lease recovery will reconcile", "generation", job.Generation)
	}
}
func safeExecute(ctx context.Context, h handlers.Handler, payload json.RawMessage) (result json.RawMessage, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("handler panic")
			result = nil
		}
	}()
	return h.Execute(ctx, payload)
}
