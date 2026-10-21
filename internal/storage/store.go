package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	schema "github.com/Adam-Sapiecha/TaskMesh/db"
	"github.com/Adam-Sapiecha/TaskMesh/internal/storage/queries"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConflict = errors.New("idempotency key already used with different request")
var ErrStaleLease = errors.New("stale lease or terminal job")
var ErrQueue = errors.New("queue missing or disabled")
var ErrCapacity = errors.New("queue backlog limit of 10000 reached")

type Store struct {
	Pool    *pgxpool.Pool
	Queries *queries.Queries
}

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("invalid DATABASE_URL")
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("database pool initialization failed")
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, errors.New("database unavailable")
	}
	return &Store{p, queries.New(p)}, nil
}
func (s *Store) Close() { s.Pool.Close() }

// GrantRuntime is run by the migration owner, never by the API or workers.
// The role cannot create schemas, migrate, delete records or administer PostgreSQL.
func (s *Store) GrantRuntime(ctx context.Context, role string) error {
	var database, schemaName string
	if err := s.Pool.QueryRow(ctx, "SELECT current_database(),current_schema()").Scan(&database, &schemaName); err != nil {
		return err
	}
	roleSQL := pgx.Identifier{role}.Sanitize()
	_, err := s.Pool.Exec(ctx, fmt.Sprintf(`
GRANT CONNECT ON DATABASE %s TO %s;
GRANT USAGE ON SCHEMA %s TO %s;
GRANT SELECT, INSERT, UPDATE ON jobs,queues,workers,job_attempts,idempotency_keys TO %s;
GRANT SELECT, INSERT ON job_events TO %s;
GRANT SELECT ON schema_migrations TO %s;
GRANT USAGE, SELECT ON SEQUENCE job_events_id_seq TO %s;`,
		pgx.Identifier{database}.Sanitize(), roleSQL, pgx.Identifier{schemaName}.Sanitize(), roleSQL, roleSQL, roleSQL, roleSQL, roleSQL))
	return err
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(748193001)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations(version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	files, err := schema.Migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	for _, file := range files {
		data, err := schema.Migrations.ReadFile("migrations/" + file.Name())
		if err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		checksum := hex.EncodeToString(hash[:])
		var old string
		err = tx.QueryRow(ctx, "SELECT checksum FROM schema_migrations WHERE version=$1", file.Name()).Scan(&old)
		if err == nil {
			if old != checksum {
				return fmt.Errorf("migration checksum changed: %s", file.Name())
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if _, err = tx.Exec(ctx, string(data)); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)", file.Name(), checksum); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

type Submit struct {
	Type           string          `json:"type"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       *int            `json:"priority,omitempty"`
	RunAt          *time.Time      `json:"run_at,omitempty"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
}

func (r *Submit) Defaults() {
	if r.Queue == "" {
		r.Queue = "default"
	}
	if r.Priority == nil {
		p := 5
		r.Priority = &p
	}
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	}
	if r.TimeoutSeconds == 0 {
		r.TimeoutSeconds = 120
	}
	if r.RunAt != nil {
		t := r.RunAt.UTC()
		r.RunAt = &t
	}
}
func (r Submit) Validate() error {
	if r.Priority == nil || *r.Priority < 0 || *r.Priority > 9 {
		return errors.New("priority must be 0..9")
	}
	if r.MaxAttempts < 1 || r.MaxAttempts > 10 {
		return errors.New("max_attempts must be 1..10")
	}
	if r.TimeoutSeconds < 1 || r.TimeoutSeconds > 300 {
		return errors.New("timeout_seconds must be 1..300")
	}
	if len(r.Queue) > 63 {
		return ErrQueue
	}
	return nil
}
func UUID(text string) (pgtype.UUID, error) { var id pgtype.UUID; err := id.Scan(text); return id, err }
func (s *Store) Submit(ctx context.Context, r Submit, key string) (json.RawMessage, bool, error) {
	// Marshal through interface{} normalizes nested object key order and number representation.
	b, err := json.Marshal(r)
	if err != nil {
		return nil, false, err
	}
	var normalized any
	if err = json.Unmarshal(b, &normalized); err != nil {
		return nil, false, err
	}
	b, err = json.Marshal(normalized)
	if err != nil {
		return nil, false, err
	}
	h := sha256.Sum256(b)
	hash := hex.EncodeToString(h[:])
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)
	if key != "" {
		// Serialize only submissions sharing the key; collision only causes harmless contention.
		if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", key); err != nil {
			return nil, false, err
		}
		var oldHash, existing string
		err = tx.QueryRow(ctx, `SELECT k.request_hash,to_jsonb(j)::text FROM idempotency_keys k JOIN jobs j ON j.id=k.job_id WHERE k.key=$1`, key).Scan(&oldHash, &existing)
		if err == nil {
			if oldHash != hash {
				return nil, false, ErrConflict
			}
			return json.RawMessage(existing), true, tx.Commit(ctx)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, false, err
		}
	}
	var enabled bool
	if err = tx.QueryRow(ctx, "SELECT enabled FROM queues WHERE name=$1 FOR UPDATE", r.Queue).Scan(&enabled); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, ErrQueue
		}
		return nil, false, err
	}
	if !enabled {
		return nil, false, ErrQueue
	}
	var backlog int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE queue=$1 AND status IN ('PENDING','RETRY_SCHEDULED','RUNNING')", r.Queue).Scan(&backlog); err != nil {
		return nil, false, err
	}
	if backlog >= 10000 {
		return nil, false, ErrCapacity
	}
	var result string
	var id pgtype.UUID
	err = tx.QueryRow(ctx, `INSERT INTO jobs(type,queue,payload,priority,run_at,max_attempts,timeout_seconds) VALUES($1,$2,$3,$4,COALESCE($5,clock_timestamp()),$6,$7) RETURNING id,to_jsonb(jobs)::text`, r.Type, r.Queue, r.Payload, *r.Priority, r.RunAt, r.MaxAttempts, r.TimeoutSeconds).Scan(&id, &result)
	if err != nil {
		return nil, false, err
	}
	if key != "" {
		if _, err = tx.Exec(ctx, "INSERT INTO idempotency_keys(key,request_hash,job_id) VALUES($1,$2,$3)", key, hash, id); err != nil {
			return nil, false, err
		}
	}
	return json.RawMessage(result), false, tx.Commit(ctx)
}

type Claim struct {
	ID             pgtype.UUID
	Type           string
	Payload        json.RawMessage
	Generation     int64
	TimeoutSeconds int
}

func (s *Store) Register(ctx context.Context, id pgtype.UUID, name string, queueNames, types []string, concurrency int) error {
	var count int
	if err := s.Pool.QueryRow(ctx, "SELECT count(*) FROM queues WHERE name=ANY($1)", queueNames).Scan(&count); err != nil {
		return err
	}
	if count != len(queueNames) {
		return ErrQueue
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO workers(id,name,queues,types,concurrency) VALUES($1,$2,$3,$4,$5)`, id, name, queueNames, types, concurrency)
	return err
}
func (s *Store) Heartbeat(ctx context.Context, id pgtype.UUID) error {
	_, err := s.Pool.Exec(ctx, "UPDATE workers SET heartbeat_at=clock_timestamp() WHERE id=$1 AND stopped_at IS NULL", id)
	return err
}
func (s *Store) Stop(ctx context.Context, id pgtype.UUID) error {
	_, err := s.Pool.Exec(ctx, "UPDATE workers SET stopped_at=clock_timestamp() WHERE id=$1", id)
	return err
}
func (s *Store) Claim(ctx context.Context, owner pgtype.UUID, queue string, lease time.Duration) (Claim, error) {
	var c Claim
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return c, err
	}
	defer tx.Rollback(ctx)
	// Queue row serializes claim/count operations, enforcing a global queue cap.
	var limit int
	err = tx.QueryRow(ctx, "SELECT concurrency_limit FROM queues WHERE name=$1 AND enabled FOR UPDATE SKIP LOCKED", queue).Scan(&limit)
	if err != nil {
		return c, err
	}
	var n int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE queue=$1 AND status='RUNNING'", queue).Scan(&n); err != nil {
		return c, err
	}
	if n >= limit {
		return c, pgx.ErrNoRows
	}
	var types []string
	var capacity int
	err = tx.QueryRow(ctx, "SELECT types,concurrency FROM workers WHERE id=$1 AND $2=ANY(queues) AND stopped_at IS NULL AND heartbeat_at>clock_timestamp()-interval '20 seconds' FOR UPDATE", owner, queue).Scan(&types, &capacity)
	if err != nil {
		return c, err
	}
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE lease_owner=$1 AND status='RUNNING'", owner).Scan(&n); err != nil {
		return c, err
	}
	if n >= capacity {
		return c, pgx.ErrNoRows
	}
	err = tx.QueryRow(ctx, `WITH candidate AS (
 SELECT id FROM jobs WHERE queue=$1 AND type=ANY($2) AND status IN ('PENDING','RETRY_SCHEDULED') AND run_at<=clock_timestamp() AND attempt_count<max_attempts
 ORDER BY priority DESC,run_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE jobs j SET status='RUNNING',attempt_count=j.attempt_count+1,lease_generation=j.lease_generation+1,lease_owner=$3,
 lease_expires_at=clock_timestamp()+make_interval(secs=>LEAST($4::double precision,j.timeout_seconds::double precision))
 FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.type,j.payload,j.lease_generation,j.timeout_seconds`, queue, types, owner, lease.Seconds()).Scan(&c.ID, &c.Type, &c.Payload, &c.Generation, &c.TimeoutSeconds)
	if err != nil {
		return c, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO job_attempts(job_id,generation,worker_id) VALUES($1,$2,$3)", c.ID, c.Generation, owner); err != nil {
		return c, err
	}
	return c, tx.Commit(ctx)
}
func (s *Store) Renew(ctx context.Context, owner pgtype.UUID, c Claim, lease time.Duration) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE jobs j SET lease_expires_at=LEAST(clock_timestamp()+make_interval(secs=>$4),a.started_at+make_interval(secs=>j.timeout_seconds)) FROM job_attempts a
 WHERE j.id=$1 AND j.lease_owner=$2 AND j.lease_generation=$3 AND j.status='RUNNING' AND j.lease_expires_at>clock_timestamp()
 AND a.job_id=j.id AND a.generation=j.lease_generation AND a.started_at+make_interval(secs=>j.timeout_seconds)>clock_timestamp()`, c.ID, owner, c.Generation, lease.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStaleLease
	}
	return nil
}
func (s *Store) Finish(ctx context.Context, owner pgtype.UUID, c Claim, result json.RawMessage, taskError error, permanent bool) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	status := "SUCCEEDED"
	msg := ""
	if taskError != nil {
		status = "RETRY_SCHEDULED"
		if permanent {
			status = "DEAD_LETTER"
		}
		msg = taskError.Error()
		if len(msg) > 1000 {
			msg = msg[:1000]
		}
	}
	var outcome string
	err = tx.QueryRow(ctx, `UPDATE jobs SET
 status=CASE WHEN $4='RETRY_SCHEDULED' AND attempt_count>=max_attempts THEN 'DEAD_LETTER' ELSE $4 END,
 result=$5,last_error=NULLIF($6,''),
 run_at=CASE WHEN $4='RETRY_SCHEDULED' THEN clock_timestamp()+make_interval(secs=>LEAST(60,power(2,attempt_count))*(0.75+random()*0.5)) ELSE run_at END,
 completed_at=CASE WHEN $4 IN ('SUCCEEDED','DEAD_LETTER') OR attempt_count>=max_attempts THEN clock_timestamp() ELSE NULL END,
 lease_owner=NULL,lease_expires_at=NULL
 WHERE id=$1 AND lease_owner=$2 AND lease_generation=$3 AND status='RUNNING' AND lease_expires_at>clock_timestamp() RETURNING status`, c.ID, owner, c.Generation, status, result, msg).Scan(&outcome)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrStaleLease
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE job_attempts SET finished_at=clock_timestamp(),outcome=$3,error=NULLIF($4,'') WHERE job_id=$1 AND generation=$2", c.ID, c.Generation, outcome, msg); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Recover(ctx context.Context) (int64, error) {
	// Batched update locks only expired jobs; event trigger and attempt closure share this statement.
	tag, err := s.Pool.Exec(ctx, `WITH expired AS (
 SELECT id FROM jobs WHERE status='RUNNING' AND lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at FOR UPDATE SKIP LOCKED LIMIT 100),
 recovered AS (UPDATE jobs j SET status=CASE WHEN attempt_count>=max_attempts THEN 'DEAD_LETTER' ELSE 'RETRY_SCHEDULED' END,
 run_at=clock_timestamp()+make_interval(secs=>LEAST(60,power(2,attempt_count))*(0.75+random()*0.5)),last_error='lease expired',
 completed_at=CASE WHEN attempt_count>=max_attempts THEN clock_timestamp() ELSE NULL END,lease_owner=NULL,lease_expires_at=NULL
 FROM expired WHERE j.id=expired.id RETURNING j.id,j.lease_generation,j.status)
 UPDATE job_attempts a SET finished_at=clock_timestamp(),outcome=r.status,error='lease expired' FROM recovered r WHERE a.job_id=r.id AND a.generation=r.lease_generation`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
func (s *Store) Cancel(ctx context.Context, id pgtype.UUID) error {
	tag, err := s.Pool.Exec(ctx, `WITH cancelled AS (UPDATE jobs SET status='CANCELLED',completed_at=clock_timestamp(),lease_owner=NULL,lease_expires_at=NULL WHERE id=$1 AND status IN ('PENDING','RETRY_SCHEDULED','RUNNING') RETURNING id,lease_generation)
 UPDATE job_attempts a SET finished_at=clock_timestamp(),outcome='CANCELLED' FROM cancelled c WHERE a.job_id=c.id AND a.generation=c.lease_generation`, id)
	_ = tag
	return err
}
