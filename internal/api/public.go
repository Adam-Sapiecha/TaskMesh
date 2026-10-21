package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/handlers"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
	"github.com/jackc/pgx/v5"
)

type PublicServer struct {
	Store                    *storage.Store
	Registry                 handlers.Registry
	Origin, BasePath, WebDir string
	SecureCookie             bool
}
type scopeKey struct{}

func publicHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func (p PublicServer) cookiePath() string {
	if p.BasePath == "" {
		return "/"
	}
	return strings.TrimRight(p.BasePath, "/") + "/"
}
func (p PublicServer) scope(r *http.Request) string {
	cookie, err := r.Cookie("taskmesh_session")
	if err != nil || len(cookie.Value) != 64 {
		return ""
	}
	return publicHash(cookie.Value)
}
func (p PublicServer) Handler() http.Handler {
	mux := http.NewServeMux()
	Health(mux, p.Store)
	mux.HandleFunc("POST /public/v1/session", p.session)
	auth := http.NewServeMux()
	auth.HandleFunc("GET /public/v1/jobs", func(w http.ResponseWriter, r *http.Request) {
		scope := r.Context().Value(scopeKey{}).(string)
		raw, err := p.Store.PublicJobs(r.Context(), scope)
		respond(w, raw, err)
	})
	auth.HandleFunc("POST /public/v1/jobs", p.submit)
	auth.HandleFunc("GET /public/v1/jobs/{id}", p.get)
	auth.HandleFunc("GET /public/v1/jobs/{id}/attempts", p.get)
	auth.HandleFunc("GET /public/v1/jobs/{id}/events", p.get)
	auth.HandleFunc("POST /public/v1/jobs/{id}/cancel", p.cancel)
	auth.HandleFunc("GET /public/v1/workers", func(w http.ResponseWriter, r *http.Request) {
		raw, err := p.Store.PublicWorkers(r.Context())
		respond(w, raw, err)
	})
	mux.Handle("/public/v1/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := p.scope(r)
		if scope == "" {
			failure(w, 401, "start a visitor session first")
			return
		}
		if _, err := p.Store.PublicSession(r.Context(), scope); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				failure(w, 401, "visitor session expired")
			} else {
				failure(w, 503, "database unavailable")
			}
			return
		}
		auth.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), scopeKey{}, scope)))
	}))
	files := http.FileServer(http.Dir(p.WebDir))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path != "/" {
			clean := filepath.Clean("/" + r.URL.Path)
			info, err := os.Stat(filepath.Join(p.WebDir, clean))
			if err != nil || info.IsDir() {
				http.NotFound(w, r)
				return
			}
		}
		if r.URL.Path != "/" {
			w.Header().Set("Cache-Control", "public, max-age=300")
		}
		files.ServeHTTP(w, r)
	})
	slots := make(chan struct{}, 12)
	var rateMu sync.Mutex
	tokens := 80.0
	last := time.Now()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.BasePath != "" && r.URL.Path == strings.TrimRight(p.BasePath, "/") {
			http.Redirect(w, r, p.cookiePath(), http.StatusPermanentRedirect)
			return
		}
		if p.BasePath != "" && strings.HasPrefix(r.URL.Path, p.cookiePath()) {
			cloned := r.Clone(r.Context())
			cloned.URL.Path = strings.TrimPrefix(r.URL.Path, strings.TrimRight(p.BasePath, "/"))
			r = cloned
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		if r.Method != "GET" && r.Method != "HEAD" && r.Header.Get("Origin") != p.Origin {
			failure(w, 403, "request origin not allowed")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/public/") {
			rateMu.Lock()
			now := time.Now()
			tokens += now.Sub(last).Seconds() * 40
			if tokens > 80 {
				tokens = 80
			}
			last = now
			allowed := tokens >= 1
			if allowed {
				tokens--
			}
			rateMu.Unlock()
			if !allowed {
				w.Header().Set("Retry-After", "2")
				failure(w, 429, "request limit reached; try again shortly")
				return
			}
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			failure(w, 429, "service is busy; try again shortly")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func (p PublicServer) session(w http.ResponseWriter, r *http.Request) {
	if scope := p.scope(r); scope != "" {
		raw, err := p.Store.PublicSession(r.Context(), scope)
		if err == nil {
			write(w, 200, json.RawMessage(raw))
			return
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			failure(w, 503, "database unavailable")
			return
		}
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		failure(w, 503, "session creation unavailable")
		return
	}
	token := hex.EncodeToString(secret[:])
	raw, err := p.Store.CreatePublicSession(r.Context(), publicHash(token))
	if errors.Is(err, storage.ErrCapacity) {
		failure(w, 429, "visitor session limit reached; try again next hour")
		return
	}
	if err != nil {
		failure(w, 503, "database unavailable")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: "taskmesh_session", Value: token, Path: p.cookiePath(), MaxAge: 86400, HttpOnly: true, Secure: p.SecureCookie, SameSite: http.SameSiteStrictMode})
	write(w, 201, json.RawMessage(raw))
}
func (p PublicServer) submit(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		failure(w, 415, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		failure(w, 413, "input limit is 8 KiB")
		return
	}
	var input storage.Submit
	if err = handlers.Decode(raw, &input); err != nil {
		failure(w, 400, "invalid job JSON")
		return
	}
	input.Defaults()
	if input.Queue != "default" && input.Queue != "reports" && input.Queue != "analytics" {
		failure(w, 400, "unknown queue")
		return
	}
	if input.RunAt != nil {
		failure(w, 400, "delayed scheduling is unavailable in visitor sessions")
		return
	}
	if input.MaxAttempts > 3 || input.TimeoutSeconds > 120 {
		failure(w, 400, "visitor task limits exceeded")
		return
	}
	input.TimeoutSeconds = 15
	if err = input.Validate(); err == nil {
		err = p.Registry.Validate(input.Type, input.Payload)
	}
	if err != nil {
		failure(w, 400, err.Error())
		return
	}
	if input.Type == "benchmark.compute" {
		var payload struct {
			Seed   string `json:"seed"`
			Rounds int    `json:"rounds"`
		}
		_ = json.Unmarshal(input.Payload, &payload)
		if payload.Rounds > 10000 {
			failure(w, 400, "visitor computation limit is 10000 rounds")
			return
		}
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 {
		failure(w, 400, "idempotency key too long")
		return
	}
	result, reused, err := p.Store.SubmitPublic(r.Context(), input, key, r.Context().Value(scopeKey{}).(string))
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrCapacity):
			failure(w, 429, "limit reached: 30 jobs per session, 200 per hour for this public instance")
		case errors.Is(err, storage.ErrConflict):
			failure(w, 409, "idempotency key already used with other input")
		case errors.Is(err, storage.ErrSession):
			failure(w, 401, "session expired")
		default:
			failure(w, 503, "submission unavailable")
		}
		return
	}
	code := 201
	if reused {
		code = 200
	}
	write(w, code, storage.PublicResult(result))
}
func (p PublicServer) get(w http.ResponseWriter, r *http.Request) {
	id, err := storage.UUID(r.PathValue("id"))
	if err != nil {
		failure(w, 400, "invalid job ID")
		return
	}
	raw, err := p.Store.PublicJob(r.Context(), r.Context().Value(scopeKey{}).(string), id)
	if err != nil {
		respond(w, raw, err)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/attempts") {
		raw, err = p.Store.Queries.ListAttempts(r.Context(), id)
	}
	if strings.HasSuffix(r.URL.Path, "/events") {
		raw, err = p.Store.Queries.ListEvents(r.Context(), id)
	}
	respond(w, raw, err)
}
func (p PublicServer) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := storage.UUID(r.PathValue("id"))
	if err != nil {
		failure(w, 400, "invalid job ID")
		return
	}
	raw, err := p.Store.PublicJob(r.Context(), r.Context().Value(scopeKey{}).(string), id)
	if err != nil {
		respond(w, raw, err)
		return
	}
	if err = p.Store.Cancel(r.Context(), id); err != nil {
		failure(w, 503, "cancellation unavailable")
		return
	}
	p.get(w, r)
}
