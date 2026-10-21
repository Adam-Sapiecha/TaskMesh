use crate::sql;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    PgPool, Postgres, Row, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, time::Duration};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("idempotency key already used with different request")]
    Conflict,
    #[error("stale lease or terminal job")]
    Stale,
    #[error("queue missing or disabled")]
    Queue,
    #[error("admission limit reached")]
    Capacity,
    #[error("visitor session expired or missing")]
    Session,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("migration checksum changed: {0}")]
    Migration(String),
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct Store {
    pub pool: PgPool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Submit {
    #[serde(rename = "type")]
    pub typ: String,
    #[serde(default)]
    pub queue: String,
    pub payload: Value,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub max_attempts: i32,
    #[serde(default)]
    pub timeout_seconds: i32,
}
impl Submit {
    pub fn defaults(&mut self) {
        if self.queue.is_empty() {
            self.queue = "default".into();
        }
        self.priority.get_or_insert(5);
        if self.max_attempts == 0 {
            self.max_attempts = 3;
        }
        if self.timeout_seconds == 0 {
            self.timeout_seconds = 120;
        }
    }
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !self.priority.is_some_and(|v| (0..=9).contains(&v)) {
            return Err("priority must be 0..9".into());
        }
        if !(1..=10).contains(&self.max_attempts) {
            return Err("max_attempts must be 1..10".into());
        }
        if !(1..=300).contains(&self.timeout_seconds) {
            return Err("timeout_seconds must be 1..300".into());
        }
        if self.queue.len() > 63 {
            return Err("queue missing or disabled".into());
        }
        crate::handlers::validate(&self.typ, &self.payload)
    }
}
#[derive(Clone, Debug)]
pub struct Claim {
    pub id: Uuid,
    pub typ: String,
    pub payload: Value,
    pub generation: i64,
    pub timeout_seconds: i32,
}

pub fn hash(bytes: impl AsRef<[u8]>) -> String {
    hex::encode(Sha256::digest(bytes.as_ref()))
}

// Match encoding/json's normalized map representation, preserving pre-migration
// idempotency hashes (including integral floats and HTML/Unicode escaping).
fn canonical(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<_> = m.keys().collect();
            keys.sort();
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|k| format!(
                        "{}:{}",
                        canonical(&Value::String(k.clone())),
                        canonical(&m[k])
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(canonical).collect::<Vec<_>>().join(",")
        ),
        Value::String(s) => serde_json::to_string(s)
            .expect("string serialization")
            .replace('&', "\\u0026")
            .replace('<', "\\u003c")
            .replace('>', "\\u003e")
            .replace('\u{2028}', "\\u2028")
            .replace('\u{2029}', "\\u2029"),
        Value::Number(n) => {
            let f = n.as_f64().expect("finite JSON number");
            if f == 0.0 {
                return if f.is_sign_negative() {
                    "-0".into()
                } else {
                    "0".into()
                };
            }
            let mut buffer = ryu::Buffer::new();
            let shortest = buffer.format_finite(f);
            if f.abs() >= 1e-6 && f.abs() < 1e21 {
                if shortest.contains('e') {
                    format!("{f}")
                } else {
                    shortest.strip_suffix(".0").unwrap_or(shortest).into()
                }
            } else {
                let exp = format!("{f:e}");
                let (mantissa, exponent) = exp.split_once('e').expect("scientific format");
                let e: i32 = exponent.parse().expect("exponent");
                format!("{mantissa}e{e:+}")
            }
        }
        _ => v.to_string(),
    }
}
pub fn request_hash(r: &Submit) -> Result<String> {
    let mut value = serde_json::to_value(r)?;
    if let Some(time) = r.run_at {
        // Go RFC3339Nano trims fractional trailing zeroes rather than padding
        // to chrono's groups of 3/6/9 digits.
        let stamp = time.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let (date, fraction) = stamp.split_once('.').expect("nanosecond format");
        let fraction = fraction.trim_end_matches('Z').trim_end_matches('0');
        value["run_at"] = Value::String(if fraction.is_empty() {
            format!("{date}Z")
        } else {
            format!("{date}.{fraction}Z")
        });
    }
    Ok(hash(canonical(&value)))
}

impl Store {
    pub async fn open(url: &str) -> anyhow::Result<Self> {
        let opts = PgConnectOptions::from_str(url)
            .map_err(|_| anyhow::anyhow!("invalid DATABASE_URL"))?
            .application_name("taskmesh-rust")
            .options([
                ("statement_timeout", "5000"),
                ("idle_in_transaction_session_timeout", "10000"),
            ]);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(opts)
            .await
            .map_err(|_| anyhow::anyhow!("database unavailable"))?;
        Ok(Self { pool })
    }
    pub async fn migrate(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(748193001)")
            .execute(&mut *tx)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())").execute(&mut *tx).await?;
        for (name, bytes) in [
            (
                "001_core.sql",
                include_bytes!("../db/migrations/001_core.sql").as_slice(),
            ),
            (
                "002_public_sessions.sql",
                include_bytes!("../db/migrations/002_public_sessions.sql").as_slice(),
            ),
        ] {
            let sum = hash(bytes);
            let old: Option<String> =
                sqlx::query_scalar("SELECT checksum FROM schema_migrations WHERE version=$1")
                    .bind(name)
                    .fetch_optional(&mut *tx)
                    .await?;
            if let Some(old) = old {
                if old != sum {
                    return Err(Error::Migration(name.into()));
                }
                continue;
            }
            sqlx::raw_sql(std::str::from_utf8(bytes).expect("SQL UTF-8"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO schema_migrations(version,checksum) VALUES($1,$2)")
                .bind(name)
                .bind(sum)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn grant_runtime(&self, public: bool) -> Result<()> {
        sqlx::raw_sql("GRANT USAGE ON SCHEMA public TO taskmesh_app; GRANT SELECT,INSERT,UPDATE ON jobs,queues,workers,job_attempts,idempotency_keys TO taskmesh_app; GRANT SELECT,INSERT ON job_events TO taskmesh_app; GRANT SELECT ON schema_migrations TO taskmesh_app; GRANT USAGE,SELECT ON SEQUENCE job_events_id_seq TO taskmesh_app;").execute(&self.pool).await?;
        if public {
            sqlx::raw_sql("GRANT SELECT,INSERT,UPDATE,DELETE ON public_sessions,public_budget TO taskmesh_app; GRANT DELETE ON jobs,job_attempts,job_events,idempotency_keys TO taskmesh_app;").execute(&self.pool).await?;
        }
        Ok(())
    }
    pub async fn ready(&self) -> Result<String> {
        Ok(sqlx::query_scalar(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
        )
        .fetch_one(&self.pool)
        .await?)
    }
    async fn budget(tx: &mut Transaction<'_, Postgres>) -> Result<()> {
        sqlx::query("SELECT id FROM public_budget WHERE id=1 FOR UPDATE")
            .execute(&mut **tx)
            .await?;
        sqlx::query("UPDATE public_budget SET window_start=date_trunc('hour',clock_timestamp()),sessions=0,jobs=0 WHERE id=1 AND window_start<date_trunc('hour',clock_timestamp())").execute(&mut **tx).await?;
        Ok(())
    }
    pub async fn submit(&self, r: &Submit, key: &str, scope: &str) -> Result<(Value, bool)> {
        let key = if !scope.is_empty() && !key.is_empty() {
            format!("public:{}", hash(format!("{scope}:{key}")))
        } else {
            key.into()
        };
        let sum = request_hash(r)?;
        let mut tx = self.pool.begin().await?;
        if !scope.is_empty() {
            Self::budget(&mut tx).await?;
            let exists:Option<i32>=sqlx::query_scalar("SELECT submitted FROM public_sessions WHERE token_hash=$1 AND expires_at>clock_timestamp() FOR UPDATE").bind(scope).fetch_optional(&mut *tx).await?;
            if exists.is_none() {
                return Err(Error::Session);
            }
        }
        if !key.is_empty() {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
                .bind(&key)
                .execute(&mut *tx)
                .await?;
            let old=sqlx::query("SELECT k.request_hash,to_jsonb(j) AS job FROM idempotency_keys k JOIN jobs j ON j.id=k.job_id WHERE k.key=$1").bind(&key).fetch_optional(&mut *tx).await?;
            if let Some(old) = old {
                if old.get::<String, _>("request_hash") != sum {
                    return Err(Error::Conflict);
                }
                let job = old.get("job");
                tx.commit().await?;
                return Ok((job, true));
            }
        }
        if !scope.is_empty() {
            let allowed:bool=sqlx::query_scalar("SELECT s.submitted<30 AND b.jobs<200 AND (SELECT count(*) FROM jobs WHERE public_session_hash IS NOT NULL)<5000 FROM public_sessions s CROSS JOIN public_budget b WHERE s.token_hash=$1 AND b.id=1").bind(scope).fetch_one(&mut *tx).await?;
            if !allowed {
                return Err(Error::Capacity);
            }
        }
        let enabled: Option<bool> =
            sqlx::query_scalar("SELECT enabled FROM queues WHERE name=$1 FOR UPDATE")
                .bind(&r.queue)
                .fetch_optional(&mut *tx)
                .await?;
        if enabled != Some(true) {
            return Err(Error::Queue);
        }
        let backlog:i64=sqlx::query_scalar("SELECT count(*) FROM jobs WHERE queue=$1 AND status IN ('PENDING','RETRY_SCHEDULED','RUNNING')").bind(&r.queue).fetch_one(&mut *tx).await?;
        if backlog >= 10000 {
            return Err(Error::Capacity);
        }
        let row=sqlx::query("INSERT INTO jobs(type,queue,payload,priority,run_at,max_attempts,timeout_seconds,public_session_hash) VALUES($1,$2,$3,$4,COALESCE($5,clock_timestamp()),$6,$7,NULLIF($8,'')) RETURNING id,to_jsonb(jobs) AS job")
            .bind(&r.typ).bind(&r.queue).bind(&r.payload).bind(r.priority.unwrap_or(5)).bind(r.run_at).bind(r.max_attempts).bind(r.timeout_seconds).bind(scope).fetch_one(&mut *tx).await?;
        let id: Uuid = row.get("id");
        let job = row.get("job");
        if !key.is_empty() {
            sqlx::query("INSERT INTO idempotency_keys(key,request_hash,job_id) VALUES($1,$2,$3)")
                .bind(key)
                .bind(sum)
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        if !scope.is_empty() {
            sqlx::query("UPDATE public_sessions SET submitted=submitted+1 WHERE token_hash=$1")
                .bind(scope)
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE public_budget SET jobs=jobs+1 WHERE id=1")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok((job, false))
    }
    pub async fn register(
        &self,
        id: Uuid,
        name: &str,
        queues: &[String],
        concurrency: i32,
    ) -> Result<()> {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM queues WHERE name=ANY($1)")
            .bind(queues)
            .fetch_one(&self.pool)
            .await?;
        if count != queues.len() as i64 {
            return Err(Error::Queue);
        }
        sqlx::query("INSERT INTO workers(id,name,queues,types,concurrency) VALUES($1,$2,$3,$4,$5)")
            .bind(id)
            .bind(name)
            .bind(queues)
            .bind(crate::handlers::TYPES.to_vec())
            .bind(concurrency)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn heartbeat(&self, id: Uuid) -> Result<()> {
        sqlx::query(
            "UPDATE workers SET heartbeat_at=clock_timestamp() WHERE id=$1 AND stopped_at IS NULL",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    pub async fn stop(&self, id: Uuid) -> Result<()> {
        sqlx::query("UPDATE workers SET stopped_at=clock_timestamp() WHERE id=$1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn claim(&self, owner: Uuid, queue: &str, lease: f64) -> Result<Option<Claim>> {
        let mut tx = self.pool.begin().await?;
        let limit: Option<i32> = sqlx::query_scalar(
            "SELECT concurrency_limit FROM queues WHERE name=$1 AND enabled FOR UPDATE SKIP LOCKED",
        )
        .bind(queue)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(limit) = limit else { return Ok(None) };
        let n: i64 =
            sqlx::query_scalar("SELECT count(*) FROM jobs WHERE queue=$1 AND status='RUNNING'")
                .bind(queue)
                .fetch_one(&mut *tx)
                .await?;
        if n >= limit as i64 {
            return Ok(None);
        }
        let worker=sqlx::query("SELECT types,concurrency FROM workers WHERE id=$1 AND $2=ANY(queues) AND stopped_at IS NULL AND heartbeat_at>clock_timestamp()-interval '20 seconds' FOR UPDATE").bind(owner).bind(queue).fetch_optional(&mut *tx).await?;
        let Some(worker) = worker else {
            return Ok(None);
        };
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs WHERE lease_owner=$1 AND status='RUNNING'",
        )
        .bind(owner)
        .fetch_one(&mut *tx)
        .await?;
        if n >= worker.get::<i32, _>("concurrency") as i64 {
            return Ok(None);
        }
        let types: Vec<String> = worker.get("types");
        let row = sqlx::query(sql::CLAIM)
            .bind(queue)
            .bind(types)
            .bind(owner)
            .bind(lease)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else { return Ok(None) };
        let c = Claim {
            id: row.get("id"),
            typ: row.get("type"),
            payload: row.get("payload"),
            generation: row.get("lease_generation"),
            timeout_seconds: row.get("timeout_seconds"),
        };
        sqlx::query("INSERT INTO job_attempts(job_id,generation,worker_id) VALUES($1,$2,$3)")
            .bind(c.id)
            .bind(c.generation)
            .bind(owner)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(Some(c))
    }
    pub async fn renew(&self, owner: Uuid, c: &Claim, lease: f64) -> Result<()> {
        let tag = sqlx::query(sql::RENEW)
            .bind(c.id)
            .bind(owner)
            .bind(c.generation)
            .bind(lease)
            .execute(&self.pool)
            .await?;
        if tag.rows_affected() != 1 {
            return Err(Error::Stale);
        }
        Ok(())
    }
    pub async fn finish(
        &self,
        owner: Uuid,
        c: &Claim,
        result: Option<Value>,
        error: Option<&str>,
        permanent: bool,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let status = if error.is_none() {
            "SUCCEEDED"
        } else if permanent {
            "DEAD_LETTER"
        } else {
            "RETRY_SCHEDULED"
        };
        let msg: String = error.unwrap_or("").chars().take(1000).collect();
        let outcome: Option<String> = sqlx::query_scalar(sql::FINISH)
            .bind(c.id)
            .bind(owner)
            .bind(c.generation)
            .bind(status)
            .bind(result)
            .bind(&msg)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(outcome) = outcome else {
            return Err(Error::Stale);
        };
        sqlx::query("UPDATE job_attempts SET finished_at=clock_timestamp(),outcome=$3,error=NULLIF($4,'') WHERE job_id=$1 AND generation=$2").bind(c.id).bind(c.generation).bind(outcome).bind(msg).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn recover(&self) -> Result<u64> {
        Ok(sqlx::query(sql::RECOVER)
            .execute(&self.pool)
            .await?
            .rows_affected())
    }
    pub async fn cancel(&self, id: Uuid) -> Result<()> {
        sqlx::query(sql::CANCEL)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn get(&self, id: Uuid, scope: Option<&str>) -> Result<Value> {
        let raw: String = if let Some(scope) = scope {
            sqlx::query_scalar("SELECT (to_jsonb(j)-'public_session_hash'-'lease_owner')::text FROM jobs j WHERE id=$1 AND public_session_hash=$2").bind(id).bind(scope).fetch_one(&self.pool).await?
        } else {
            sqlx::query_scalar(sql::GET_JOB)
                .bind(id)
                .fetch_one(&self.pool)
                .await?
        };
        Ok(serde_json::from_str(&raw)?)
    }
    pub async fn details(&self, id: Uuid, kind: &str) -> Result<Value> {
        let raw: String = sqlx::query_scalar(if kind == "events" {
            sql::LIST_EVENTS
        } else {
            sql::LIST_ATTEMPTS
        })
        .bind(id)
        .fetch_one(&self.pool)
        .await?;
        Ok(serde_json::from_str(&raw)?)
    }
    pub async fn list(&self, kind: &str, scope: Option<&str>) -> Result<Value> {
        let raw:String=match (kind,scope) {
            ("jobs",Some(scope))=>sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(j)-'public_session_hash'-'lease_owner'),'[]'::jsonb)::text FROM (SELECT * FROM jobs WHERE public_session_hash=$1 ORDER BY created_at DESC,id LIMIT 30) j").bind(scope).fetch_one(&self.pool).await?,
            ("jobs",None)=>sqlx::query_scalar(sql::LIST_JOBS).bind(100_i32).fetch_one(&self.pool).await?,
            ("workers",Some(_))=>sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(w)),'[]'::jsonb)::text FROM (SELECT name,concurrency,heartbeat_at,stopped_at,heartbeat_at>clock_timestamp()-interval '20 seconds' AND stopped_at IS NULL AS online,(SELECT count(*) FROM jobs j WHERE j.lease_owner=workers.id AND j.status='RUNNING') AS active FROM workers ORDER BY heartbeat_at DESC LIMIT 3) w").fetch_one(&self.pool).await?,
            ("workers",None)=>sqlx::query_scalar(sql::LIST_WORKERS).fetch_one(&self.pool).await?,
            _=>sqlx::query_scalar(sql::LIST_QUEUES).fetch_one(&self.pool).await?,
        };
        Ok(serde_json::from_str(&raw)?)
    }
    pub async fn session(&self, scope: &str) -> Result<Value> {
        let raw:String=sqlx::query_scalar("SELECT (to_jsonb(s)-'token_hash')::text FROM public_sessions s WHERE token_hash=$1 AND expires_at>clock_timestamp()").bind(scope).fetch_one(&self.pool).await?;
        Ok(serde_json::from_str(&raw)?)
    }
    pub async fn create_session(&self, scope: &str) -> Result<Value> {
        let mut tx = self.pool.begin().await?;
        Self::budget(&mut tx).await?;
        let allowed:bool=sqlx::query_scalar("SELECT sessions<60 AND (SELECT count(*) FROM public_sessions)<5000 FROM public_budget WHERE id=1").fetch_one(&mut *tx).await?;
        if !allowed {
            return Err(Error::Capacity);
        }
        let raw:String=sqlx::query_scalar("INSERT INTO public_sessions(token_hash) VALUES($1) RETURNING (to_jsonb(public_sessions)-'token_hash')::text").bind(scope).fetch_one(&mut *tx).await?;
        sqlx::query("UPDATE public_budget SET sessions=sessions+1 WHERE id=1")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(serde_json::from_str(&raw)?)
    }
    pub async fn cleanup_public(&self) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let ids:Vec<Uuid>=sqlx::query_scalar("SELECT j.id FROM jobs j JOIN public_sessions s ON s.token_hash=j.public_session_hash WHERE s.expires_at<=clock_timestamp() AND j.status IN ('SUCCEEDED','DEAD_LETTER','CANCELLED') FOR UPDATE OF j SKIP LOCKED LIMIT 100").fetch_all(&mut *tx).await?;
        for query in [
            "DELETE FROM idempotency_keys WHERE job_id=ANY($1::uuid[])",
            "DELETE FROM job_events WHERE job_id=ANY($1::uuid[])",
            "DELETE FROM job_attempts WHERE job_id=ANY($1::uuid[])",
            "DELETE FROM jobs WHERE id=ANY($1::uuid[])",
        ] {
            sqlx::query(query).bind(&ids).execute(&mut *tx).await?;
        }
        sqlx::query("DELETE FROM public_sessions s WHERE expires_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.public_session_hash=s.token_hash)").execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hashes_match_the_previous_go_binary() {
        for (raw, expected) in [
            (
                r#"{"type":"report.generate","queue":"reports","payload":{"values":[2,4,8,16]},"timeout_seconds":15}"#,
                "a1005248d045aea15b69eebb3c30acc9bbd9ff010da3346c2e71da73620aeb4d",
            ),
            (
                r#"{"type":"report.generate","payload":{"values":[1.0,0.000001,1e-7,1000000000000]},"run_at":"2026-10-21T12:00:00.100000000Z"}"#,
                "f9a7fedc27acfe0725f74535c48adf44ec6ac4df4418d407c6f7e5421f7a42a9",
            ),
            (
                r#"{"type":"text.analyze","payload":{"text":"Zażółć <>&\u2028"}}"#,
                "264f48e3ebe81b551bc8c3aaae46325783d723c0f1831cc59c8dbdf2d86a79c1",
            ),
        ] {
            let mut r: Submit = serde_json::from_str(raw).unwrap();
            r.defaults();
            assert_eq!(request_hash(&r).unwrap(), expected);
        }
    }
    #[test]
    fn canonical_compatibility() {
        assert_eq!(
            canonical(&serde_json::json!({"z":1.0,"a":"<>&\u{2028}","n":[0.000001,1e-7,1e21]})),
            r#"{"a":"\u003c\u003e\u0026\u2028","n":[0.000001,1e-7,1e+21],"z":1}"#
        );
    }
}
