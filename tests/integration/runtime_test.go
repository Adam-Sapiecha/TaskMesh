package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/api"
	"github.com/Adam-Sapiecha/TaskMesh/internal/handlers"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
	"github.com/Adam-Sapiecha/TaskMesh/internal/worker"
	"github.com/jackc/pgx/v5"
)

func TestRestrictedRuntimeRole(t *testing.T) {
	s, dsn := database(t)
	ctx := context.Background()
	id := worker.NewID()
	role := "runtime_" + hex.EncodeToString(id.Bytes[:])
	identifier := pgx.Identifier{role}.Sanitize()
	if _, err := s.Pool.Exec(ctx, "CREATE ROLE "+identifier+" LOGIN PASSWORD 'test_local_only' NOSUPERUSER NOCREATEDB NOCREATEROLE"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(ctx, "DROP OWNED BY "+identifier+"; DROP ROLE "+identifier); err != nil {
			t.Error(err)
		}
	})
	if err := s.GrantRuntime(ctx, role); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "test_local_only")
	runtime, err := storage.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	for _, sql := range []string{"DELETE FROM jobs", "CREATE TABLE forbidden(id int)", "UPDATE schema_migrations SET checksum='bad'"} {
		if _, err = runtime.Pool.Exec(ctx, sql); err == nil {
			t.Fatal("runtime role unexpectedly authorized:", sql)
		}
	}
	jobID := submit(t, runtime, "")
	owner := register(t, runtime)
	c, err := runtime.Claim(ctx, owner, "default", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.Renew(ctx, owner, c, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = runtime.Finish(ctx, owner, c, json.RawMessage(`{"ok":true}`), nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err = runtime.Queries.GetJob(ctx, jobID); err != nil {
		t.Fatal(err)
	}
}

// Exercises real HTTP, three independent worker runtimes and PostgreSQL without
// requiring the OS to allow launching child executables from a test runner.
func TestThousandJobsWorkerRuntimesAndAPIReopen(t *testing.T) {
	s, dsn := database(t)
	ctx, cancel := context.WithCancel(context.Background())
	var group sync.WaitGroup
	defer func() { cancel(); group.Wait() }()
	for i := 0; i < 3; i++ {
		cfg := worker.DefaultConfig()
		cfg.Name = "load-worker"
		cfg.Poll = 10 * time.Millisecond
		group.Add(1)
		go func() {
			defer group.Done()
			if err := worker.Run(ctx, s, handlers.Builtins(), cfg); err != nil {
				t.Error(err)
			}
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM workers").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker registration timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
	server := httptest.NewServer(api.Server{Store: s, Registry: handlers.Builtins(), Token: token}.Handler())
	defer server.Close()
	client := http.Client{Timeout: 10 * time.Second}
	var producers sync.WaitGroup
	began := time.Now()
	for i := 0; i < 10; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < 100; j++ {
				req, _ := http.NewRequest("POST", server.URL+"/api/v1/jobs", strings.NewReader(`{"type":"report.generate","queue":"reports","payload":{"values":[10,20,30]}}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := client.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 201 {
					t.Errorf("HTTP %d", resp.StatusCode)
					return
				}
			}
		}()
	}
	producers.Wait()
	if t.Failed() {
		return
	}
	deadline = time.Now().Add(60 * time.Second)
	for {
		var n int
		if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE status='SUCCEEDED'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed %d/1000", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(began)
	var owners, bad int
	_ = s.Pool.QueryRow(ctx, "SELECT count(DISTINCT worker_id) FROM job_attempts").Scan(&owners)
	_ = s.Pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE result->>'sum'<>'60' OR attempt_count<>1").Scan(&bad)
	if owners != 3 || bad != 0 {
		t.Fatalf("owners=%d invalid=%d", owners, bad)
	}
	server.Close()
	fresh, err := storage.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	reopened := httptest.NewServer(api.Server{Store: fresh, Registry: handlers.Builtins(), Token: token}.Handler())
	defer reopened.Close()
	req, _ := http.NewRequest("GET", reopened.URL+"/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var jobs []map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || len(jobs) != 100 {
		t.Fatal("persisted results not returned")
	}
	t.Logf("1000 jobs / 3 worker runtimes: %s, %.1f jobs/s including HTTP submission; persisted results read by reopened API", elapsed.Round(time.Millisecond), 1000/elapsed.Seconds())
}
