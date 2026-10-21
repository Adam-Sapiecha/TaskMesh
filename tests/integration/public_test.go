package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Adam-Sapiecha/TaskMesh/internal/api"
	"github.com/Adam-Sapiecha/TaskMesh/internal/handlers"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
)

func TestPublicVisitorIsolationAndCSRF(t *testing.T) {
	s, _ := database(t)
	web := t.TempDir()
	if err := os.WriteFile(filepath.Join(web, "index.html"), []byte("<!doctype html><title>TaskMesh</title>"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.PublicServer{Store: s, Registry: handlers.Builtins(), Origin: "https://portfolio.test", WebDir: web}.Handler())
	defer server.Close()
	client := func() *http.Client { jar, _ := cookiejar.New(nil); return &http.Client{Jar: jar} }
	a, b := client(), client()
	request := func(c *http.Client, method, path, body, origin string) (int, []byte, http.Header) {
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", origin)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, raw, resp.Header
	}
	for _, c := range []*http.Client{a, b} {
		code, raw, headers := request(c, "POST", "/public/v1/session", "{}", "https://portfolio.test")
		if code != 201 {
			t.Fatalf("session %d %s", code, raw)
		}
		cookie := headers.Get("Set-Cookie")
		if !strings.Contains(cookie, "HttpOnly") || !strings.Contains(cookie, "SameSite=Strict") {
			t.Fatal("cookie flags missing")
		}
	}
	code, raw, _ := request(a, "POST", "/public/v1/jobs", `{"type":"text.analyze","payload":{"text":"visitor A private text"}}`, "https://portfolio.test")
	if code != 201 {
		t.Fatalf("submit %d %s", code, raw)
	}
	var job struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(raw, &job)
	if strings.Contains(string(raw), "public_session_hash") {
		t.Fatal("scope hash leaked")
	}
	for _, suffix := range []string{"", "/events", "/attempts", "/cancel"} {
		method := "GET"
		if suffix == "/cancel" {
			method = "POST"
		}
		code, raw, _ = request(b, method, "/public/v1/jobs/"+job.ID+suffix, "{}", "https://portfolio.test")
		if code != 404 {
			t.Fatalf("cross-session access %s: %d %s", suffix, code, raw)
		}
	}
	code, raw, _ = request(b, "GET", "/public/v1/jobs", "", "https://portfolio.test")
	if code != 200 || strings.TrimSpace(string(raw)) != "[]" {
		t.Fatal("list leaked another visitor's job", string(raw))
	}
	code, _, _ = request(a, "GET", "/api/v1/jobs", "", "https://portfolio.test")
	if code != 404 {
		t.Fatal("administrative API exposed", code)
	}
	for _, origin := range []string{"", "https://attacker.example"} {
		code, _, _ = request(a, "POST", "/public/v1/jobs", `{"type":"text.analyze","payload":{"text":"csrf"}}`, origin)
		if code != 403 {
			t.Fatal("CSRF request accepted", code)
		}
	}
	code, raw, _ = request(a, "POST", "/public/v1/jobs", `{"type":"benchmark.compute","payload":{"seed":"bounded","rounds":100000}}`, "https://portfolio.test")
	if code != 400 {
		t.Fatal("compute bound missing", code, string(raw))
	}
	code, raw, _ = request(a, "GET", "/public/v1/jobs/"+job.ID, "", "https://portfolio.test")
	if code != 200 || !strings.Contains(string(raw), "visitor A private text") {
		t.Fatal("owner cannot read own data", code)
	}
	if _, err := s.Pool.Exec(context.Background(), "UPDATE public_sessions SET expires_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	code, _, _ = request(a, "GET", "/public/v1/jobs", "", "https://portfolio.test")
	if code != 401 {
		t.Fatal("expired session accepted", code)
	}
}

func TestPublicQuotaAtomicAndIdempotencyScoped(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	for _, hash := range []string{"a", "b"} {
		if _, err := s.CreatePublicSession(ctx, hash); err != nil {
			t.Fatal(err)
		}
	}
	r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"bounded data"}`)}
	r.Defaults()
	first, _, err := s.SubmitPublic(ctx, r, "same-key", "a")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := s.SubmitPublic(ctx, r, "same-key", "b")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) {
		t.Fatal("idempotency key crossed session boundary")
	}
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := s.SubmitPublic(ctx, r, "", "a")
			if err != nil && !errors.Is(err, storage.ErrCapacity) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var jobs, used int
	if err = s.Pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM jobs WHERE public_session_hash='a'),submitted FROM public_sessions WHERE token_hash='a'").Scan(&jobs, &used); err != nil {
		t.Fatal(err)
	}
	if jobs != 30 || used != 30 {
		t.Fatalf("quota violated jobs=%d submitted=%d", jobs, used)
	}
	if _, reused, err := s.SubmitPublic(ctx, r, "same-key", "a"); err != nil || !reused {
		t.Fatal("idempotent retry at quota failed", reused, err)
	}
	if _, _, err := s.SubmitPublic(ctx, r, "", ""); !errors.Is(err, storage.ErrSession) {
		t.Fatal("empty session scope accepted", err)
	}
	var orphan int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE public_session_hash IS NULL").Scan(&orphan)
	if orphan != 0 {
		t.Fatal("orphaned public jobs", orphan)
	}
}

func TestPublicRetentionPreservesPrivateJobs(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	private := submit(t, s, "")
	if _, err := s.CreatePublicSession(ctx, "expired"); err != nil {
		t.Fatal(err)
	}
	r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"expired visitor data"}`)}
	r.Defaults()
	raw, _, err := s.SubmitPublic(ctx, r, "old", "expired")
	if err != nil {
		t.Fatal(err)
	}
	var job struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(raw, &job)
	id, err := storage.UUID(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Cancel(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE public_sessions SET expires_at=clock_timestamp()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	if err = s.CleanupPublic(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Queries.GetJob(ctx, private); err != nil {
		t.Fatal("private job deleted", err)
	}
	var count int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE public_session_hash IS NOT NULL").Scan(&count)
	if count != 0 {
		t.Fatal("expired visitor data retained", count)
	}
}

func TestPublicGlobalBudgetAndRollback(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	if _, err := s.CreatePublicSession(ctx, "budget"); err != nil {
		t.Fatal(err)
	}
	r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"budgeted"}`)}
	r.Defaults()
	r.Queue = "missing"
	if _, _, err := s.SubmitPublic(ctx, r, "", "budget"); !errors.Is(err, storage.ErrQueue) {
		t.Fatal(err)
	}
	var consumed int
	_ = s.Pool.QueryRow(ctx, "SELECT jobs FROM public_budget WHERE id=1").Scan(&consumed)
	if consumed != 0 {
		t.Fatal("failed submission consumed quota")
	}
	_, err := s.Pool.Exec(ctx, "UPDATE public_budget SET jobs=200,sessions=60 WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
	r.Queue = "default"
	if _, _, err = s.SubmitPublic(ctx, r, "", "budget"); !errors.Is(err, storage.ErrCapacity) {
		t.Fatal("global job limit not enforced", err)
	}
	if _, err = s.CreatePublicSession(ctx, "over-budget"); !errors.Is(err, storage.ErrCapacity) {
		t.Fatal("global session limit not enforced", err)
	}
}

func Example_publicIsolation() {
	fmt.Println("Visitor session -> scoped jobs -> dedicated public PostgreSQL stack")
	// Output: Visitor session -> scoped jobs -> dedicated public PostgreSQL stack
}
