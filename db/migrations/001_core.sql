CREATE TABLE queues (
    name text PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9_-]{0,62}$'),
    concurrency_limit integer NOT NULL DEFAULT 8 CHECK (concurrency_limit BETWEEN 1 AND 64),
    enabled boolean NOT NULL DEFAULT true
);
INSERT INTO queues(name) VALUES ('default'), ('reports'), ('analytics');

CREATE TABLE workers (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    queues text[] NOT NULL,
    types text[] NOT NULL,
    concurrency integer NOT NULL CHECK (concurrency BETWEEN 1 AND 16),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    stopped_at timestamptz
);

CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    queue text NOT NULL REFERENCES queues(name),
    type text NOT NULL,
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status text NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','RUNNING','RETRY_SCHEDULED','SUCCEEDED','DEAD_LETTER','CANCELLED')),
    priority integer NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),
    run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempt_count integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    timeout_seconds integer NOT NULL DEFAULT 120 CHECK (timeout_seconds BETWEEN 1 AND 300),
    lease_owner uuid REFERENCES workers(id),
    lease_generation bigint NOT NULL DEFAULT 0,
    lease_expires_at timestamptz,
    result jsonb,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CHECK (attempt_count BETWEEN 0 AND max_attempts),
    CHECK ((status = 'RUNNING') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK ((status IN ('SUCCEEDED','DEAD_LETTER','CANCELLED')) = (completed_at IS NOT NULL))
);
CREATE INDEX jobs_ready ON jobs(queue, priority DESC, run_at, id) WHERE status IN ('PENDING','RETRY_SCHEDULED');
CREATE INDEX jobs_expired ON jobs(lease_expires_at) WHERE status = 'RUNNING';
CREATE INDEX jobs_created ON jobs(created_at DESC, id);

CREATE TABLE job_attempts (
    job_id uuid NOT NULL REFERENCES jobs(id),
    generation bigint NOT NULL,
    worker_id uuid NOT NULL REFERENCES workers(id),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    outcome text,
    error text,
    PRIMARY KEY(job_id, generation)
);

CREATE TABLE job_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES jobs(id),
    status text NOT NULL,
    lease_generation bigint NOT NULL,
    worker_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX events_job ON job_events(job_id, id);

CREATE TABLE idempotency_keys (
    key text PRIMARY KEY CHECK (length(key) BETWEEN 1 AND 128),
    request_hash text NOT NULL,
    job_id uuid NOT NULL REFERENCES jobs(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Only the repository's conditional updates may advance this state machine.
-- The trigger additionally protects against accidental invalid SQL transitions.
CREATE FUNCTION record_job_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.status <> NEW.status THEN
        IF NOT (
            (OLD.status IN ('PENDING','RETRY_SCHEDULED') AND NEW.status IN ('RUNNING','CANCELLED')) OR
            (OLD.status = 'RUNNING' AND NEW.status IN ('SUCCEEDED','RETRY_SCHEDULED','DEAD_LETTER','CANCELLED'))
        ) THEN
            RAISE EXCEPTION 'invalid job transition: % -> %', OLD.status, NEW.status;
        END IF;
    END IF;
    IF TG_OP = 'INSERT' OR OLD.status IS DISTINCT FROM NEW.status THEN
        INSERT INTO job_events(job_id,status,lease_generation,worker_id)
        VALUES(NEW.id,NEW.status,NEW.lease_generation,COALESCE(NEW.lease_owner,OLD.lease_owner));
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER job_transition AFTER INSERT OR UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION record_job_transition();
