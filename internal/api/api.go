package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/handlers"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
	"github.com/jackc/pgx/v5"
)

type Server struct {
	Store    *storage.Store
	Registry handlers.Registry
	Token    string
}

func Health(mux *http.ServeMux, s *storage.Store) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		var version string
		err := s.Pool.QueryRow(ctx, "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&version)
		if err != nil {
			failure(w, 503, "database or migrations unavailable")
			return
		}
		write(w, 200, map[string]string{"status": "ready", "schema": version})
	})
}
func (s Server) Handler() http.Handler {
	mux := http.NewServeMux()
	Health(mux, s.Store)
	protected := http.NewServeMux()
	protected.HandleFunc("POST /api/v1/jobs", s.submit)
	protected.HandleFunc("GET /api/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		b, err := s.Store.Queries.ListJobs(r.Context(), 100)
		respond(w, b, err)
	})
	protected.HandleFunc("GET /api/v1/jobs/{id}", s.get)
	protected.HandleFunc("GET /api/v1/jobs/{id}/attempts", s.get)
	protected.HandleFunc("GET /api/v1/jobs/{id}/events", s.get)
	protected.HandleFunc("POST /api/v1/jobs/{id}/cancel", s.cancel)
	protected.HandleFunc("GET /api/v1/queues", func(w http.ResponseWriter, r *http.Request) {
		b, err := s.Store.Queries.ListQueues(r.Context())
		respond(w, b, err)
	})
	protected.HandleFunc("GET /api/v1/workers", func(w http.ResponseWriter, r *http.Request) {
		b, err := s.Store.Queries.ListWorkers(r.Context())
		respond(w, b, err)
	})
	expected := sha256.Sum256([]byte("Bearer " + s.Token))
	slots := make(chan struct{}, 32)
	mux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
		if len(s.Token) < 32 || subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
			failure(w, 401, "valid bearer token required")
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			w.Header().Set("Retry-After", "1")
			failure(w, 429, "too many active requests")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		protected.ServeHTTP(w, r.WithContext(ctx))
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		mux.ServeHTTP(w, r)
	})
}
func (s Server) submit(w http.ResponseWriter, r *http.Request) {
	typ, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || typ != "application/json" {
		failure(w, 415, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		failure(w, 413, "body limit is 64 KiB")
		return
	}
	var input storage.Submit
	if err = handlers.Decode(raw, &input); err != nil {
		failure(w, 400, "invalid job JSON: "+err.Error())
		return
	}
	input.Defaults()
	if err = input.Validate(); err == nil {
		err = s.Registry.Validate(input.Type, input.Payload)
	}
	if err != nil {
		failure(w, 400, err.Error())
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 {
		failure(w, 400, "Idempotency-Key must be <=128 bytes")
		return
	}
	b, reused, err := s.Store.Submit(r.Context(), input, key)
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrConflict):
			failure(w, 409, err.Error())
		case errors.Is(err, storage.ErrQueue):
			failure(w, 400, err.Error())
		case errors.Is(err, storage.ErrCapacity):
			w.Header().Set("Retry-After", "5")
			failure(w, 429, err.Error())
		default:
			failure(w, 503, "database unavailable")
		}
		return
	}
	var result struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(b, &result)
	w.Header().Set("Location", "/api/v1/jobs/"+result.ID)
	status := 201
	if reused {
		status = 200
	}
	write(w, status, b)
}
func (s Server) get(w http.ResponseWriter, r *http.Request) {
	id, err := storage.UUID(r.PathValue("id"))
	if err != nil {
		failure(w, 400, "invalid UUID")
		return
	}
	b, err := s.Store.Queries.GetJob(r.Context(), id)
	if err != nil {
		respond(w, b, err)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/attempts") {
		b, err = s.Store.Queries.ListAttempts(r.Context(), id)
	}
	if strings.HasSuffix(r.URL.Path, "/events") {
		b, err = s.Store.Queries.ListEvents(r.Context(), id)
	}
	respond(w, b, err)
}
func (s Server) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := storage.UUID(r.PathValue("id"))
	if err != nil {
		failure(w, 400, "invalid UUID")
		return
	}
	if err = s.Store.Cancel(r.Context(), id); err != nil {
		failure(w, 503, "database unavailable")
		return
	}
	s.get(w, r)
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func failure(w http.ResponseWriter, status int, message string) {
	write(w, status, map[string]string{"error": message})
}
func respond(w http.ResponseWriter, b string, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		failure(w, 404, "job not found")
		return
	}
	if err != nil {
		slog.Warn("database request failed")
		failure(w, 503, "database unavailable")
		return
	}
	write(w, 200, json.RawMessage(b))
}
