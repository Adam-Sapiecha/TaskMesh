package storage

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrSession = errors.New("visitor session expired or missing")

// All public admission uses budget -> session -> queue lock order. It shares
// the job transaction, so failed submissions never consume a quota or orphan a job.
func lockPublicBudget(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, "SELECT id FROM public_budget WHERE id=1 FOR UPDATE"); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, "UPDATE public_budget SET window_start=date_trunc('hour',clock_timestamp()),sessions=0,jobs=0 WHERE id=1 AND window_start<date_trunc('hour',clock_timestamp())")
	return err
}
func (s *Store) lockPublicAdmission(ctx context.Context, tx pgx.Tx, scope string) error {
	if err := lockPublicBudget(ctx, tx); err != nil {
		return err
	}
	var used int
	err := tx.QueryRow(ctx, "SELECT submitted FROM public_sessions WHERE token_hash=$1 AND expires_at>clock_timestamp() FOR UPDATE", scope).Scan(&used)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrSession
	}
	if err != nil {
		return err
	}
	// Quota is checked after idempotency reuse in submit; a retry at quota must
	// still be able to retrieve its already accepted job.
	return nil
}
func (s *Store) publicQuota(ctx context.Context, tx pgx.Tx, scope string) error {
	var allowed bool
	err := tx.QueryRow(ctx, `SELECT s.submitted<30 AND b.jobs<200 AND (SELECT count(*) FROM jobs WHERE public_session_hash IS NOT NULL)<5000
 FROM public_sessions s CROSS JOIN public_budget b WHERE s.token_hash=$1 AND b.id=1`, scope).Scan(&allowed)
	if err != nil {
		return err
	}
	if !allowed {
		return ErrCapacity
	}
	return nil
}
func (s *Store) CreatePublicSession(ctx context.Context, hash string) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if err = lockPublicBudget(ctx, tx); err != nil {
		return "", err
	}
	var allowed bool
	if err = tx.QueryRow(ctx, "SELECT sessions<60 AND (SELECT count(*) FROM public_sessions)<5000 FROM public_budget WHERE id=1").Scan(&allowed); err != nil {
		return "", err
	}
	if !allowed {
		return "", ErrCapacity
	}
	var session string
	err = tx.QueryRow(ctx, "INSERT INTO public_sessions(token_hash) VALUES($1) RETURNING (to_jsonb(public_sessions)-'token_hash')::text", hash).Scan(&session)
	if err != nil {
		return "", err
	}
	if _, err = tx.Exec(ctx, "UPDATE public_budget SET sessions=sessions+1 WHERE id=1"); err != nil {
		return "", err
	}
	return session, tx.Commit(ctx)
}
func (s *Store) PublicSession(ctx context.Context, scope string) (string, error) {
	var raw string
	err := s.Pool.QueryRow(ctx, "SELECT (to_jsonb(s)-'token_hash')::text FROM public_sessions s WHERE token_hash=$1 AND expires_at>clock_timestamp()", scope).Scan(&raw)
	return raw, err
}
func (s *Store) PublicJobs(ctx context.Context, scope string) (string, error) {
	var raw string
	err := s.Pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(j)-'public_session_hash'-'lease_owner'),'[]'::jsonb)::text FROM (SELECT * FROM jobs WHERE public_session_hash=$1 ORDER BY created_at DESC,id LIMIT 30) j`, scope).Scan(&raw)
	return raw, err
}
func (s *Store) PublicJob(ctx context.Context, scope string, id pgtype.UUID) (string, error) {
	var raw string
	err := s.Pool.QueryRow(ctx, "SELECT (to_jsonb(j)-'public_session_hash'-'lease_owner')::text FROM jobs j WHERE id=$1 AND public_session_hash=$2", id, scope).Scan(&raw)
	return raw, err
}
func PublicResult(raw json.RawMessage) json.RawMessage {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return json.RawMessage(`{}`)
	}
	delete(object, "public_session_hash")
	delete(object, "lease_owner")
	result, _ := json.Marshal(object)
	return result
}
func (s *Store) PublicWorkers(ctx context.Context) (string, error) {
	var raw string
	err := s.Pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(w)),'[]'::jsonb)::text FROM (SELECT name,concurrency,heartbeat_at,stopped_at,heartbeat_at>clock_timestamp()-interval '20 seconds' AND stopped_at IS NULL AS online,(SELECT count(*) FROM jobs j WHERE j.lease_owner=workers.id AND j.status='RUNNING') AS active FROM workers ORDER BY heartbeat_at DESC LIMIT 3) w`).Scan(&raw)
	return raw, err
}

func (s *Store) GrantPublicRuntime(ctx context.Context, role string) error {
	quoted := pgx.Identifier{role}.Sanitize()
	_, err := s.Pool.Exec(ctx, "GRANT SELECT, INSERT, UPDATE, DELETE ON public_sessions,public_budget TO "+quoted+"; GRANT DELETE ON jobs,job_attempts,job_events,idempotency_keys TO "+quoted)
	return err
}

// Terminal rows belonging to expired visitor sessions only; private NULL-scoped
// jobs cannot match. Live attempts remain intact until finished/recovered.
func (s *Store) CleanupPublic(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT j.id FROM jobs j JOIN public_sessions s ON s.token_hash=j.public_session_hash WHERE s.expires_at<=clock_timestamp() AND j.status IN ('SUCCEEDED','DEAD_LETTER','CANCELLED') FOR UPDATE OF j SKIP LOCKED LIMIT 100`)
	if err != nil {
		return err
	}
	var ids []pgtype.UUID
	for rows.Next() {
		var id pgtype.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	for _, table := range []string{"idempotency_keys", "job_events", "job_attempts"} {
		if _, err = tx.Exec(ctx, "DELETE FROM "+table+" WHERE job_id=ANY($1::uuid[])", ids); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, "DELETE FROM jobs WHERE id=ANY($1::uuid[])", ids); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "DELETE FROM public_sessions s WHERE expires_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.public_session_hash=s.token_hash)"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
