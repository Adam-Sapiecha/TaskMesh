-- name: GetJob :one
SELECT to_jsonb(j)::text FROM jobs j WHERE id = $1;

-- name: ListJobs :one
SELECT COALESCE(jsonb_agg(to_jsonb(j)), '[]'::jsonb)::text FROM
 (SELECT * FROM jobs ORDER BY created_at DESC, id DESC LIMIT $1) j;

-- name: ListAttempts :one
SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY a.generation), '[]'::jsonb)::text
FROM job_attempts a WHERE job_id = $1;

-- name: ListEvents :one
SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.id), '[]'::jsonb)::text
FROM job_events e WHERE job_id = $1;

-- name: ListWorkers :one
SELECT COALESCE(jsonb_agg(to_jsonb(w)), '[]'::jsonb)::text FROM
 (SELECT * FROM workers ORDER BY heartbeat_at DESC LIMIT 100) w;

-- name: ListQueues :one
SELECT COALESCE(jsonb_agg(to_jsonb(q)), '[]'::jsonb)::text FROM queues q;
