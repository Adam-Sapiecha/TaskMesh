// Coordination statements retained verbatim from the original implementation.
pub const CLAIM: &str = r#"WITH candidate AS (
 SELECT id FROM jobs WHERE queue=$1 AND type=ANY($2) AND status IN ('PENDING','RETRY_SCHEDULED') AND run_at<=clock_timestamp() AND attempt_count<max_attempts
 ORDER BY priority DESC,run_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE jobs j SET status='RUNNING',attempt_count=j.attempt_count+1,lease_generation=j.lease_generation+1,lease_owner=$3,
 lease_expires_at=clock_timestamp()+make_interval(secs=>LEAST($4::double precision,j.timeout_seconds::double precision))
 FROM candidate WHERE j.id=candidate.id RETURNING j.id,j.type,j.payload,j.lease_generation,j.timeout_seconds"#;
pub const RENEW: &str = r#"UPDATE jobs j SET lease_expires_at=LEAST(clock_timestamp()+make_interval(secs=>$4),a.started_at+make_interval(secs=>j.timeout_seconds)) FROM job_attempts a
 WHERE j.id=$1 AND j.lease_owner=$2 AND j.lease_generation=$3 AND j.status='RUNNING' AND j.lease_expires_at>clock_timestamp()
 AND a.job_id=j.id AND a.generation=j.lease_generation AND a.started_at+make_interval(secs=>j.timeout_seconds)>clock_timestamp()"#;
pub const FINISH: &str = r#"UPDATE jobs SET
 status=CASE WHEN $4='RETRY_SCHEDULED' AND attempt_count>=max_attempts THEN 'DEAD_LETTER' ELSE $4 END,
 result=$5,last_error=NULLIF($6,''),
 run_at=CASE WHEN $4='RETRY_SCHEDULED' THEN clock_timestamp()+make_interval(secs=>LEAST(60,power(2,attempt_count))*(0.75+random()*0.5)) ELSE run_at END,
 completed_at=CASE WHEN $4 IN ('SUCCEEDED','DEAD_LETTER') OR attempt_count>=max_attempts THEN clock_timestamp() ELSE NULL END,
 lease_owner=NULL,lease_expires_at=NULL
 WHERE id=$1 AND lease_owner=$2 AND lease_generation=$3 AND status='RUNNING' AND lease_expires_at>clock_timestamp() RETURNING status"#;
pub const RECOVER: &str = r#"WITH expired AS (
 SELECT id FROM jobs WHERE status='RUNNING' AND lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at FOR UPDATE SKIP LOCKED LIMIT 100),
 recovered AS (UPDATE jobs j SET status=CASE WHEN attempt_count>=max_attempts THEN 'DEAD_LETTER' ELSE 'RETRY_SCHEDULED' END,
 run_at=clock_timestamp()+make_interval(secs=>LEAST(60,power(2,attempt_count))*(0.75+random()*0.5)),last_error='lease expired',
 completed_at=CASE WHEN attempt_count>=max_attempts THEN clock_timestamp() ELSE NULL END,lease_owner=NULL,lease_expires_at=NULL
 FROM expired WHERE j.id=expired.id RETURNING j.id,j.lease_generation,j.status)
 UPDATE job_attempts a SET finished_at=clock_timestamp(),outcome=r.status,error='lease expired' FROM recovered r WHERE a.job_id=r.id AND a.generation=r.lease_generation"#;
pub const CANCEL: &str = r#"WITH cancelled AS (UPDATE jobs SET status='CANCELLED',completed_at=clock_timestamp(),lease_owner=NULL,lease_expires_at=NULL WHERE id=$1 AND status IN ('PENDING','RETRY_SCHEDULED','RUNNING') RETURNING id,lease_generation)
 UPDATE job_attempts a SET finished_at=clock_timestamp(),outcome='CANCELLED' FROM cancelled c WHERE a.job_id=c.id AND a.generation=c.lease_generation"#;
pub const GET_JOB: &str = r#"SELECT to_jsonb(j)::text FROM jobs j WHERE id = $1;"#;
pub const LIST_JOBS: &str = r#"SELECT COALESCE(jsonb_agg(to_jsonb(j)), '[]'::jsonb)::text FROM
 (SELECT * FROM jobs ORDER BY created_at DESC, id DESC LIMIT $1) j;"#;
pub const LIST_ATTEMPTS: &str = r#"SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY a.generation), '[]'::jsonb)::text
FROM job_attempts a WHERE job_id = $1;"#;
pub const LIST_EVENTS: &str = r#"SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.id), '[]'::jsonb)::text
FROM job_events e WHERE job_id = $1;"#;
pub const LIST_WORKERS: &str = r#"SELECT COALESCE(jsonb_agg(to_jsonb(w)), '[]'::jsonb)::text FROM
 (SELECT * FROM workers ORDER BY heartbeat_at DESC LIMIT 100) w;"#;
pub const LIST_QUEUES: &str =
    r#"SELECT COALESCE(jsonb_agg(to_jsonb(q)), '[]'::jsonb)::text FROM queues q;"#;
