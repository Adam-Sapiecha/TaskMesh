package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/api"
	"github.com/Adam-Sapiecha/TaskMesh/internal/handlers"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
	"github.com/Adam-Sapiecha/TaskMesh/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const token = "integration-test-token-at-least-32-characters"

func database(t *testing.T) (*storage.Store, string) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") == "true" {
			t.Fatal("TEST_DATABASE_URL is required in CI")
		}
		t.Skip("set TEST_DATABASE_URL to a real disposable PostgreSQL database")
	}
	ctx := context.Background()
	admin, err := storage.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	id := worker.NewID()
	schema := "test_" + hex.EncodeToString(id.Bytes[:])
	if _, err = admin.Pool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := parsed.Query()
	q.Set("search_path", schema)
	parsed.RawQuery = q.Encode()
	isolated := parsed.String()
	s, err := storage.Open(ctx, isolated)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		admin.Pool.Reset() // Discard connections invalidated by the optional DB restart test.
		_, err := admin.Pool.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		if err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Migrate(ctx); err != nil {
		t.Fatalf("idempotent migration: %v", err)
	}
	return s, isolated
}
func submit(t *testing.T, s *storage.Store, key string) pgtype.UUID {
	t.Helper()
	r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"real task input"}`)}
	r.Defaults()
	b, _, err := s.Submit(context.Background(), r, key)
	if err != nil {
		t.Fatal(err)
	}
	var j struct {
		ID string `json:"id"`
	}
	if err = json.Unmarshal(b, &j); err != nil {
		t.Fatal(err)
	}
	id, err := storage.UUID(j.ID)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func register(t *testing.T, s *storage.Store) pgtype.UUID {
	t.Helper()
	id := worker.NewID()
	if err := s.Register(context.Background(), id, "test", []string{"default"}, handlers.Builtins().Types(), 16); err != nil {
		t.Fatal(err)
	}
	return id
}
func forceExpired(t *testing.T, s *storage.Store, id pgtype.UUID) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
}
func forceReady(t *testing.T, s *storage.Store, id pgtype.UUID) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), "UPDATE jobs SET run_at=clock_timestamp()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentClaimsNeverDuplicate(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		submit(t, s, "")
	}
	owners := make([]pgtype.UUID, 20)
	for i := range owners {
		owners[i] = register(t, s)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[pgtype.UUID]bool{}
	for _, owner := range owners {
		wg.Add(1)
		go func(owner pgtype.UUID) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				c, err := s.Claim(ctx, owner, "default", 30*time.Second)
				if errors.Is(err, pgx.ErrNoRows) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				if seen[c.ID] {
					t.Error("duplicate active claim")
				}
				seen[c.ID] = true
				mu.Unlock()
				if err = s.Finish(ctx, owner, c, json.RawMessage(`{"ok":true}`), nil, false); err != nil {
					t.Error(err)
					return
				}
			}
		}(owner)
	}
	wg.Wait()
	if len(seen) != 100 {
		t.Fatalf("claimed %d/100", len(seen))
	}
	var bad int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status<>'SUCCEEDED' OR attempt_count<>1").Scan(&bad); err != nil || bad != 0 {
		t.Fatalf("invalid jobs=%d err=%v", bad, err)
	}
}
func TestQueueAndWorkerConcurrencyCaps(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, "UPDATE queues SET concurrency_limit=2 WHERE name='default'"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		submit(t, s, "")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		owner := register(t, s)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Claim(ctx, owner, "default", time.Minute)
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var active int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='RUNNING'").Scan(&active)
	if active != 2 {
		t.Fatalf("global cap: got %d", active)
	}
	if _, err := s.Pool.Exec(ctx, "UPDATE queues SET concurrency_limit=8 WHERE name='default'"); err != nil {
		t.Fatal(err)
	}
	owner := register(t, s)
	if _, err := s.Pool.Exec(ctx, "UPDATE workers SET concurrency=1 WHERE id=$1", owner); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, owner, "default", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, owner, "default", time.Minute); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("worker cap: %v", err)
	}
}
func TestRecoveryFencesStaleOwnerAndRecordsAttempts(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	id := submit(t, s, "")
	a, b := register(t, s), register(t, s)
	first, err := s.Claim(ctx, a, "default", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	forceExpired(t, s, id)
	if err = s.Finish(ctx, a, first, json.RawMessage(`{}`), nil, false); !errors.Is(err, storage.ErrStaleLease) {
		t.Fatal("expired ACK accepted", err)
	}
	if err = s.Renew(ctx, a, first, time.Minute); !errors.Is(err, storage.ErrStaleLease) {
		t.Fatal("expired renewal accepted", err)
	}
	n, err := s.Recover(ctx)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	forceReady(t, s, id)
	second, err := s.Claim(ctx, b, "default", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatal("generation did not increase")
	}
	if err = s.Finish(ctx, a, first, json.RawMessage(`{"stale":true}`), nil, false); !errors.Is(err, storage.ErrStaleLease) {
		t.Fatal("stale ACK accepted", err)
	}
	if err = s.Finish(ctx, b, second, json.RawMessage(`{"fresh":true}`), nil, false); err != nil {
		t.Fatal(err)
	}
	attempts, err := s.Queries.ListAttempts(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	_ = json.Unmarshal([]byte(attempts), &rows)
	if len(rows) != 2 || rows[0]["error"] != "lease expired" || rows[1]["outcome"] != "SUCCEEDED" {
		t.Fatal(attempts)
	}
	if _, err = s.Claim(ctx, b, "default", time.Minute); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("terminal job reclaimed", err)
	}
}
func TestRetriesDeadLetterAndCancellation(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	id := submit(t, s, "")
	owner := register(t, s)
	for i := 0; i < 3; i++ {
		forceReady(t, s, id)
		c, err := s.Claim(ctx, owner, "default", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Finish(ctx, owner, c, nil, errors.New("temporary failure"), false); err != nil {
			t.Fatal(err)
		}
	}
	b, err := s.Queries.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b, `"status": "DEAD_LETTER"`) {
		t.Fatal(b)
	}
	cancelled := submit(t, s, "")
	c, err := s.Claim(ctx, owner, "default", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Cancel(ctx, cancelled); err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, owner, c, json.RawMessage(`{}`), nil, false); !errors.Is(err, storage.ErrStaleLease) {
		t.Fatal("cancelled ACK accepted", err)
	}
}
func TestIdempotentConcurrentSubmissions(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"same"}`)}
			r.Defaults()
			if _, _, err := s.Submit(ctx, r, "shared-key"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var count int
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs").Scan(&count)
	if count != 1 {
		t.Fatalf("created %d jobs", count)
	}
	r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"different"}`)}
	r.Defaults()
	if _, _, err := s.Submit(ctx, r, "shared-key"); !errors.Is(err, storage.ErrConflict) {
		t.Fatal("missing idempotency conflict", err)
	}
}
func TestClaimRollbackIncludesAttemptAndEvent(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	id := submit(t, s, "")
	owner := register(t, s)
	_, err := s.Pool.Exec(ctx, `CREATE FUNCTION fail_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected failure'; END $$; CREATE TRIGGER fail_attempt BEFORE INSERT ON job_attempts FOR EACH ROW EXECUTE FUNCTION fail_attempt()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, owner, "default", time.Minute); err == nil {
		t.Fatal("claim unexpectedly committed")
	}
	var status string
	var attempts, events int
	if err = s.Pool.QueryRow(ctx, "SELECT status,attempt_count,(SELECT count(*) FROM job_events WHERE job_id=$1) FROM jobs WHERE id=$1", id).Scan(&status, &attempts, &events); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" || attempts != 0 || events != 1 {
		t.Fatalf("rollback failed: %s %d %d", status, attempts, events)
	}
}
func TestDelayedPriorityAndHandlerFiltering(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	owner := register(t, s)
	future := time.Now().Add(time.Hour)
	priority := 9
	r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"future"}`), RunAt: &future, Priority: &priority}
	r.Defaults()
	if _, _, err := s.Submit(ctx, r, ""); err != nil {
		t.Fatal(err)
	}
	ready := submit(t, s, "")
	c, err := s.Claim(ctx, owner, "default", time.Minute)
	if err != nil || c.ID != ready {
		t.Fatal("delayed job claimed", err)
	}
	if err = s.Finish(ctx, owner, c, json.RawMessage(`{}`), nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, owner, "default", time.Minute); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("future job claimed", err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE jobs SET run_at=clock_timestamp(),type='unknown' WHERE status='PENDING'"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Claim(ctx, owner, "default", time.Minute); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("unsupported type claimed", err)
	}
}
func TestAPIAuthValidationPersistenceAndDBOutage(t *testing.T) {
	s, dsn := database(t)
	handler := api.Server{Store: s, Registry: handlers.Builtins(), Token: token}.Handler()
	request := func(h http.Handler, method, path, body, auth string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if auth != "" {
			r.Header.Set("Authorization", "Bearer "+auth)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	if w := request(handler, "GET", "/api/v1/jobs", "", ""); w.Code != 401 {
		t.Fatal(w.Code)
	}
	for _, body := range []string{`{"type":"shell.exec","payload":{}}`, `{"type":"text.analyze","payload":{"text":"x"},"priority":99}`, `{"type":"text.analyze","payload":{"text":"x"},"typo":1}`, `{"type":"text.analyze","payload":{"text":"x"}} {}`} {
		if w := request(handler, "POST", "/api/v1/jobs", body, token); w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := request(handler, "POST", "/api/v1/jobs", `{"type":"text.analyze","payload":{"text":"persist me"}}`, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	location := w.Header().Get("Location")
	restarted, err := storage.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	fresh := api.Server{Store: restarted, Registry: handlers.Builtins(), Token: token}.Handler()
	if w = request(fresh, "GET", location, "", token); w.Code != 200 {
		t.Fatal(w.Code)
	}
	restarted.Close()
	if w = request(fresh, "POST", "/api/v1/jobs", `{"type":"text.analyze","payload":{"text":"db down"}}`, token); w.Code != 503 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = request(fresh, "GET", "/readyz", "", ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
}

func Example_submission() {
	fmt.Println(`{"type":"report.generate","queue":"reports","payload":{"values":[10,20,30]}}`)
	// Output: {"type":"report.generate","queue":"reports","payload":{"values":[10,20,30]}}
}
