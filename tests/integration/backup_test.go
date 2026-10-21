package integration

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
	"github.com/Adam-Sapiecha/TaskMesh/internal/worker"
	"github.com/jackc/pgx/v5"
)

func TestPersistedResultSurvivesPostgreSQLRestart(t *testing.T) {
	bin, data := os.Getenv("TASKMESH_PG_BIN"), os.Getenv("TASKMESH_TEST_PG_DATA")
	if bin == "" || data == "" {
		t.Skip("optional: configure native disposable PostgreSQL test instance for restart verification")
	}
	s, _ := database(t)
	ctx := context.Background()
	id := submit(t, s, "")
	owner := register(t, s)
	c, err := s.Claim(ctx, owner, "default", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, owner, c, json.RawMessage(`{"durable":true}`), nil, false); err != nil {
		t.Fatal(err)
	}
	restart := exec.Command(filepath.Join(bin, "pg_ctl"), "-D", data, "-l", filepath.Join(filepath.Dir(data), "postgres.log"), "-m", "fast", "-w", "restart")
	// PostgreSQL can inherit pipe handles on Windows. A regular file avoids
	// waiting for a running server to close a command-output pipe.
	commandLog, err := os.Create(filepath.Join(filepath.Dir(data), "pgctl-restart-test.log"))
	if err != nil {
		t.Fatal(err)
	}
	restart.Stdout = commandLog
	restart.Stderr = commandLog
	err = restart.Run()
	_ = commandLog.Close()
	if err != nil {
		t.Fatalf("restart failed: %v (see native instance's pgctl-restart-test.log)", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var raw string
	for time.Now().Before(deadline) {
		raw, err = s.Queries.GetJob(ctx, id)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	var j struct {
		Status string `json:"status"`
		Result struct {
			Durable bool `json:"durable"`
		} `json:"result"`
	}
	if err = json.Unmarshal([]byte(raw), &j); err != nil {
		t.Fatal(err)
	}
	if j.Status != "SUCCEEDED" || !j.Result.Durable {
		t.Fatal("result lost across PostgreSQL restart", raw)
	}
}

func TestDumpRestoresPersistedResult(t *testing.T) {
	bin := os.Getenv("TASKMESH_PG_BIN")
	if bin == "" {
		t.Skip("set TASKMESH_PG_BIN to matching PostgreSQL bin directory for restore verification")
	}
	s, _ := database(t)
	ctx := context.Background()
	id := submit(t, s, "")
	owner := register(t, s)
	c, err := s.Claim(ctx, owner, "default", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, owner, c, json.RawMessage(`{"verified":"backup-result"}`), nil, false); err != nil {
		t.Fatal(err)
	}
	var schemaName string
	if err = s.Pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schemaName); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "taskmesh.dump")
	dump := exec.Command(filepath.Join(bin, "pg_dump"), "--format=custom", "--schema="+schemaName, "--file="+archive, "--dbname="+os.Getenv("TEST_DATABASE_URL"))
	if out, err := dump.CombinedOutput(); err != nil {
		t.Fatalf("dump failed: %v %s", err, out)
	}
	random := worker.NewID()
	target := "restore_" + hex.EncodeToString(random.Bytes[:])
	if _, err = s.Pool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{target}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := s.Pool.Exec(ctx, "DROP DATABASE "+pgx.Identifier{target}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	targetURL, err := url.Parse(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	targetURL.Path = "/" + target
	restore := exec.Command(filepath.Join(bin, "pg_restore"), "--exit-on-error", "--no-owner", "--dbname="+targetURL.String(), archive)
	if out, err := restore.CombinedOutput(); err != nil {
		t.Fatalf("restore failed: %v %s", err, out)
	}
	q := targetURL.Query()
	q.Set("search_path", schemaName)
	targetURL.RawQuery = q.Encode()
	restored, err := storage.Open(ctx, targetURL.String())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err = restored.Migrate(ctx); err != nil {
		t.Fatal("restored migration state:", err)
	}
	result, err := restored.Queries.GetJob(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	var j struct {
		Status string `json:"status"`
		Result struct {
			Verified string `json:"verified"`
		} `json:"result"`
	}
	if err = json.Unmarshal([]byte(result), &j); err != nil {
		t.Fatal(err)
	}
	if j.Status != "SUCCEEDED" || j.Result.Verified != "backup-result" {
		t.Fatal("restored result differs", result)
	}
	var attempts, events int
	if err = restored.Pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM job_attempts),(SELECT count(*) FROM job_events)").Scan(&attempts, &events); err != nil || attempts != 1 || events != 3 {
		t.Fatalf("attempts=%d events=%d err=%v", attempts, events, err)
	}
}
