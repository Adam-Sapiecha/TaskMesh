package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Adam-Sapiecha/TaskMesh/internal/storage"
)

func TestRenewalDeadlinePermanentFailureAndTerminalGuard(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	r := storage.Submit{Type: "text.analyze", Payload: json.RawMessage(`{"text":"deadline"}`), TimeoutSeconds: 1, MaxAttempts: 1}
	r.Defaults()
	if _, _, err := s.Submit(ctx, r, ""); err != nil {
		t.Fatal(err)
	}
	owner := register(t, s)
	c, err := s.Claim(ctx, owner, "default", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Renew(ctx, owner, c, time.Minute); err != nil {
		t.Fatal(err)
	}
	var within bool
	if err = s.Pool.QueryRow(ctx, `SELECT j.lease_expires_at<=a.started_at+interval '1 second' FROM jobs j JOIN job_attempts a ON a.job_id=j.id AND a.generation=j.lease_generation WHERE j.id=$1`, c.ID).Scan(&within); err != nil || !within {
		t.Fatal("renewal exceeded attempt deadline", err)
	}
	forceExpired(t, s, c.ID)
	if n, err := s.Recover(ctx); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if _, err = s.Pool.Exec(ctx, "UPDATE jobs SET status='PENDING',completed_at=NULL WHERE id=$1", c.ID); err == nil {
		t.Fatal("terminal transition was allowed")
	}
	submit(t, s, "")
	next, err := s.Claim(ctx, owner, "default", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, owner, next, nil, errors.New("permanent"), true); err != nil {
		t.Fatal(err)
	}
	var status string
	var count int
	if err = s.Pool.QueryRow(ctx, "SELECT status,attempt_count FROM jobs WHERE id=$1", next.ID).Scan(&status, &count); err != nil || status != "DEAD_LETTER" || count != 1 {
		t.Fatal(status, count, err)
	}
}

func TestDuePriorityOrdering(t *testing.T) {
	s, _ := database(t)
	ctx := context.Background()
	low, high := submit(t, s, ""), submit(t, s, "")
	if _, err := s.Pool.Exec(ctx, "UPDATE jobs SET priority=CASE WHEN id=$1 THEN 9 ELSE 0 END", high); err != nil {
		t.Fatal(err)
	}
	owner := register(t, s)
	c, err := s.Claim(ctx, owner, "default", time.Minute)
	if err != nil || c.ID != high {
		t.Fatal("high priority not selected", err)
	}
	if err = s.Finish(ctx, owner, c, json.RawMessage(`{}`), nil, false); err != nil {
		t.Fatal(err)
	}
	c, err = s.Claim(ctx, owner, "default", time.Minute)
	if err != nil || c.ID != low {
		t.Fatal("remaining low priority not selected", err)
	}
}
