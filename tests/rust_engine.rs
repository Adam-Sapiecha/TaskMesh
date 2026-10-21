use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use taskmesh::{
    api,
    storage::{Error, Store, Submit},
    worker,
};
use tokio::{task::JoinSet, time::sleep};
use tower::ServiceExt;
use uuid::Uuid;

struct Database {
    store: Store,
    admin: sqlx::PgPool,
    schema: String,
    url: String,
}
impl Database {
    async fn new() -> Self {
        let base = std::env::var("TEST_DATABASE_URL")
            .expect("set TEST_DATABASE_URL to a disposable PostgreSQL database");
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&base)
            .await
            .unwrap();
        let schema = format!("test_{}", Uuid::new_v4().simple());
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let url = format!(
            "{base}{}options=-csearch_path%3D{schema}",
            if base.contains('?') { '&' } else { '?' }
        );
        let store = Store::open(&url).await.unwrap();
        store.migrate().await.unwrap();
        store.migrate().await.unwrap();
        Self {
            store,
            admin,
            schema,
            url,
        }
    }
    async fn close(self) {
        self.store.pool.close().await;
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}
fn job(values: Value) -> Submit {
    let mut r: Submit = serde_json::from_value(
        json!({"type":"report.generate","queue":"reports","payload":{"values":values}}),
    )
    .unwrap();
    r.defaults();
    r
}
async fn register(s: &Store, concurrency: i32) -> Uuid {
    let id = Uuid::new_v4();
    s.register(id, "test-worker", &["reports".into()], concurrency)
        .await
        .unwrap();
    id
}
fn id(v: &Value) -> Uuid {
    Uuid::parse_str(v["id"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn competing_claims_obey_worker_and_queue_capacity() {
    let d = Database::new().await;
    let s = &d.store;
    sqlx::query("UPDATE queues SET concurrency_limit=2 WHERE name='reports'")
        .execute(&s.pool)
        .await
        .unwrap();
    for _ in 0..20 {
        s.submit(&job(json!([1, 2])), "", "").await.unwrap();
    }
    let owners = [
        register(s, 1).await,
        register(s, 1).await,
        register(s, 1).await,
    ];
    let mut tasks = JoinSet::new();
    for i in 0..24 {
        let s = s.clone();
        let owner = owners[i % 3];
        tasks.spawn(async move { s.claim(owner, "reports", 30.0).await.unwrap() });
    }
    let mut ids = std::collections::HashSet::new();
    while let Some(c) = tasks.join_next().await {
        if let Some(c) = c.unwrap() {
            assert!(ids.insert(c.id));
        }
    }
    assert!(!ids.is_empty());
    assert!(ids.len() <= 2);
    let n:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT lease_owner,count(*) AS n FROM jobs WHERE status='RUNNING' GROUP BY lease_owner HAVING count(*)>1) x").fetch_one(&s.pool).await.unwrap();
    assert_eq!(n, 0);
    d.close().await;
}

#[tokio::test]
async fn lease_fencing_recovery_retry_dead_letter_and_cancel() {
    let d = Database::new().await;
    let s = &d.store;
    let owner = register(s, 2).await;
    let mut r = job(json!([2, 4, 8, 16]));
    r.max_attempts = 2;
    let (j, _) = s.submit(&r, "lease-test", "").await.unwrap();
    let old = s.claim(owner, "reports", 30.0).await.unwrap().unwrap();
    s.renew(owner, &old, 30.0).await.unwrap();
    sqlx::query(
        "UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(old.id)
    .execute(&s.pool)
    .await
    .unwrap();
    assert!(matches!(
        s.renew(owner, &old, 30.0).await,
        Err(Error::Stale)
    ));
    assert!(matches!(
        s.finish(owner, &old, Some(json!({})), None, false).await,
        Err(Error::Stale)
    ));
    assert_eq!(s.recover().await.unwrap(), 1);
    sqlx::query("UPDATE jobs SET run_at=clock_timestamp() WHERE id=$1")
        .bind(old.id)
        .execute(&s.pool)
        .await
        .unwrap();
    let new = s.claim(owner, "reports", 30.0).await.unwrap().unwrap();
    assert!(new.generation > old.generation);
    assert!(matches!(
        s.finish(owner, &old, Some(json!({})), None, false).await,
        Err(Error::Stale)
    ));
    s.finish(owner, &new, None, Some("temporary failure"), false)
        .await
        .unwrap();
    assert_eq!(s.get(id(&j), None).await.unwrap()["status"], "DEAD_LETTER");
    assert_eq!(
        s.details(id(&j), "attempts")
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let (cancelled, _) = s.submit(&r, "cancel", "").await.unwrap();
    let c = s.claim(owner, "reports", 30.0).await.unwrap().unwrap();
    s.cancel(c.id).await.unwrap();
    assert!(matches!(
        s.finish(owner, &c, Some(json!({})), None, false).await,
        Err(Error::Stale)
    ));
    assert_eq!(
        s.get(id(&cancelled), None).await.unwrap()["status"],
        "CANCELLED"
    );
    s.submit(&r, "permanent", "").await.unwrap();
    let c = s.claim(owner, "reports", 30.0).await.unwrap().unwrap();
    s.finish(owner, &c, None, Some("invalid payload"), true)
        .await
        .unwrap();
    assert_eq!(s.get(c.id, None).await.unwrap()["status"], "DEAD_LETTER");
    d.close().await;
}

#[tokio::test]
async fn priorities_scheduling_idempotency_and_success() {
    let d = Database::new().await;
    let s = &d.store;
    let owner = register(s, 1).await;
    let mut r = job(json!([1.0, 2.0]));
    r.priority = Some(1);
    let (a, reused) = s.submit(&r, "same", "").await.unwrap();
    assert!(!reused);
    let (b, reused) = s.submit(&r, "same", "").await.unwrap();
    assert!(reused);
    assert_eq!(a["id"], b["id"]);
    let mut other = r.clone();
    other.payload = json!({"values":[4]});
    assert!(matches!(
        s.submit(&other, "same", "").await,
        Err(Error::Conflict)
    ));
    r.priority = Some(9);
    let (priority, _) = s.submit(&r, "high", "").await.unwrap();
    r.run_at = Some(chrono::Utc::now() + chrono::Duration::hours(1));
    s.submit(&r, "future", "").await.unwrap();
    let c = s.claim(owner, "reports", 30.0).await.unwrap().unwrap();
    assert_eq!(c.id, id(&priority));
    let output = taskmesh::handlers::execute(&c.typ, &c.payload)
        .await
        .unwrap();
    s.finish(owner, &c, Some(output), None, false)
        .await
        .unwrap();
    assert_eq!(s.get(c.id, None).await.unwrap()["result"]["sum"], 3.0);
    let c = s.claim(owner, "reports", 30.0).await.unwrap().unwrap();
    s.finish(owner, &c, Some(json!({})), None, false)
        .await
        .unwrap();
    assert!(s.claim(owner, "reports", 30.0).await.unwrap().is_none());
    sqlx::query("UPDATE schema_migrations SET checksum='tampered' WHERE version='001_core.sql'")
        .execute(&s.pool)
        .await
        .unwrap();
    assert!(matches!(s.migrate().await, Err(Error::Migration(_))));
    d.close().await;
}

#[tokio::test]
async fn public_quota_is_atomic_and_reuse_at_quota_works() {
    let d = Database::new().await;
    let s = &d.store;
    s.create_session("visitor-a").await.unwrap();
    s.create_session("visitor-b").await.unwrap();
    let (first, _) = s
        .submit(&job(json!([1, 2])), "same", "visitor-a")
        .await
        .unwrap();
    let (other, _) = s
        .submit(&job(json!([1, 2])), "same", "visitor-b")
        .await
        .unwrap();
    assert_ne!(first["id"], other["id"]);
    let mut tasks = JoinSet::new();
    for _ in 0..40 {
        let s = s.clone();
        tasks.spawn(async move { s.submit(&job(json!([1, 2])), "", "visitor-a").await });
    }
    let mut accepted = 0;
    while let Some(r) = tasks.join_next().await {
        match r.unwrap() {
            Ok(_) => accepted += 1,
            Err(Error::Capacity) => {}
            Err(e) => panic!("{e}"),
        }
    }
    assert_eq!(accepted, 29);
    assert!(
        s.submit(&job(json!([1, 2])), "same", "visitor-a")
            .await
            .unwrap()
            .1
    );
    assert!(s.get(id(&first), Some("visitor-b")).await.is_err());
    sqlx::query("UPDATE public_budget SET jobs=199")
        .execute(&s.pool)
        .await
        .unwrap();
    s.submit(&job(json!([1, 2])), "last", "visitor-b")
        .await
        .unwrap();
    assert!(matches!(
        s.submit(&job(json!([1, 2])), "overflow", "visitor-b").await,
        Err(Error::Capacity)
    ));
    let used: i32 =
        sqlx::query_scalar("SELECT submitted FROM public_sessions WHERE token_hash='visitor-b'")
            .fetch_one(&s.pool)
            .await
            .unwrap();
    assert_eq!(used, 2);
    d.close().await;
}

#[tokio::test]
async fn retention_deletes_only_expired_terminal_public_data() {
    let d = Database::new().await;
    let s = &d.store;
    s.create_session("expired").await.unwrap();
    let (private, _) = s.submit(&job(json!([1])), "private", "").await.unwrap();
    let (public, _) = s
        .submit(&job(json!([1])), "public", "expired")
        .await
        .unwrap();
    s.cancel(id(&public)).await.unwrap();
    let (live, _) = s.submit(&job(json!([2])), "live", "expired").await.unwrap();
    sqlx::query("UPDATE public_sessions SET expires_at=clock_timestamp()-interval '1 second'")
        .execute(&s.pool)
        .await
        .unwrap();
    s.cleanup_public().await.unwrap();
    assert!(s.get(id(&private), None).await.is_ok());
    assert!(s.get(id(&public), None).await.is_err());
    assert!(s.get(id(&live), None).await.is_ok());
    s.cancel(id(&live)).await.unwrap();
    s.cleanup_public().await.unwrap();
    assert!(s.session("expired").await.is_err());
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM public_sessions")
        .fetch_one(&s.pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
    d.close().await;
}

async fn call(
    router: &axum::Router,
    method: &str,
    path: &str,
    cookie: &str,
    origin: &str,
    body: Value,
) -> (u16, Value, String) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json")
        .header("origin", origin);
    if !cookie.is_empty() {
        builder = builder.header("cookie", cookie);
    }
    let r = router
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = r.status().as_u16();
    let cookie = r
        .headers()
        .get("set-cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let bytes = to_bytes(r.into_body(), 65536).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json, cookie)
}
#[tokio::test]
async fn public_http_contract_cookie_origin_and_cross_session_denial() {
    let d = Database::new().await;
    let router = api::router(
        d.store.clone(),
        api::Config {
            mode: "public".into(),
            origin: "https://example.test".into(),
            base_path: "/taskmesh".into(),
            token: String::new(),
            web_dir: "web/dist".into(),
        },
    );
    let (status, _, a) = call(
        &router,
        "POST",
        "/taskmesh/public/v1/session",
        "",
        "https://example.test",
        json!({}),
    )
    .await;
    assert_eq!(status, 201);
    assert!(a.contains("HttpOnly"));
    assert!(a.contains("Secure"));
    assert!(a.contains("SameSite=Strict"));
    assert!(a.contains("Path=/taskmesh/"));
    let (_, _, b) = call(
        &router,
        "POST",
        "/public/v1/session",
        "",
        "https://example.test",
        json!({}),
    )
    .await;
    let input = json!({"type":"report.generate","payload":{"values":[2,4,8,16]}});
    let (status, j, _) = call(
        &router,
        "POST",
        "/taskmesh/public/v1/jobs",
        &a,
        "https://example.test",
        input.clone(),
    )
    .await;
    assert_eq!(status, 201);
    assert!(j.get("public_session_hash").is_none());
    assert!(j.get("lease_owner").is_none());
    for suffix in ["", "/attempts", "/events", "/cancel"] {
        let path = format!("/public/v1/jobs/{}{suffix}", j["id"].as_str().unwrap());
        let method = if suffix == "/cancel" { "POST" } else { "GET" };
        assert_eq!(
            call(
                &router,
                method,
                &path,
                &b,
                "https://example.test",
                json!({})
            )
            .await
            .0,
            404
        );
    }
    assert_eq!(
        call(
            &router,
            "POST",
            "/public/v1/jobs",
            &a,
            "https://evil.test",
            input.clone()
        )
        .await
        .0,
        403
    );
    assert_eq!(
        call(&router, "GET", "/api/v1/jobs", &a, "", json!({}))
            .await
            .0,
        404
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/public/v1/jobs",
            &a,
            "https://example.test",
            json!({"type":"text.analyze","payload":{"text":"a".repeat(9000)}})
        )
        .await
        .0,
        413
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/public/v1/jobs",
            &a,
            "https://example.test",
            json!({"type":"benchmark.compute","payload":{"seed":"a","rounds":10001}})
        )
        .await
        .0,
        400
    );
    assert_eq!(
        call(
            &router,
            "POST",
            "/public/v1/session",
            &a,
            "https://example.test",
            json!({})
        )
        .await
        .0,
        200
    );
    let private = api::router(
        d.store.clone(),
        api::Config {
            mode: "api".into(),
            token: "test-token-at-least-32-characters-long".into(),
            origin: String::new(),
            base_path: String::new(),
            web_dir: String::new(),
        },
    );
    assert_eq!(
        call(&private, "GET", "/api/v1/jobs", "", "", json!({}))
            .await
            .0,
        401
    );
    d.close().await;
}

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn process(mode: &str, port: u16, url: &str, name: &str) -> Process {
    Process(
        Command::new(env!("CARGO_BIN_EXE_taskmesh"))
            .arg(mode)
            .env("DATABASE_URL", url)
            .env("PORT", port.to_string())
            .env("WORKER_NAME", name)
            .env(
                "TASKMESH_API_KEY",
                "acceptance-token-at-least-32-characters",
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
async fn http(port: u16, path: &str, body: Option<&str>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let body = body.unwrap_or("");
    let method = if body.is_empty() { "GET" } else { "POST" };
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nAuthorization: Bearer acceptance-token-at-least-32-characters\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut out = String::new();
    stream.read_to_string(&mut out).await.unwrap();
    out
}
async fn ready(port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            let r = http(port, "/readyz", None).await;
            if r.starts_with("HTTP/1.1 200") {
                return;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    panic!("process not ready");
}
#[tokio::test]
async fn thousand_jobs_three_rust_worker_processes_and_api_restart() {
    let d = Database::new().await;
    let mut processes = Vec::new();
    for i in 0..3 {
        let port = free_port();
        processes.push(process("worker", port, &d.url, &format!("acceptance-{i}")));
        ready(port).await;
    }
    let port = free_port();
    let server = process("api", port, &d.url, "");
    ready(port).await;
    let mut producers = JoinSet::new();
    for _ in 0..10 {
        producers.spawn(async move{for _ in 0..100{let r=http(port,"/api/v1/jobs",Some(r#"{"type":"report.generate","queue":"reports","payload":{"values":[2,4,8,16]}}"#)).await;assert!(r.starts_with("HTTP/1.1 201"),"{r}");}});
    }
    while let Some(r) = producers.join_next().await {
        r.unwrap();
    }
    drop(server);
    let server = process("api", port, &d.url, "");
    ready(port).await;
    let mut done = 0;
    for _ in 0..1800 {
        done = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM jobs WHERE status='SUCCEEDED'")
            .fetch_one(&d.store.pool)
            .await
            .unwrap();
        if done == 1000 {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(done, 1000);
    let attempts: i64 =
        sqlx::query_scalar("SELECT count(*) FROM job_attempts WHERE outcome='SUCCEEDED'")
            .fetch_one(&d.store.pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1000);
    assert!(http(port, "/api/v1/jobs", None).await.contains("SUCCEEDED"));
    drop(server);
    drop(processes);
    d.close().await;
}

#[tokio::test]
async fn rust_worker_drains_and_records_stop() {
    let d = Database::new().await;
    d.store.submit(&job(json!([2, 4])), "", "").await.unwrap();
    let stop = tokio_util::sync::CancellationToken::new();
    let task = tokio::spawn(worker::run(
        d.store.clone(),
        worker::Config {
            name: "drain".into(),
            queues: vec!["reports".into()],
            concurrency: 1,
        },
        stop.clone(),
    ));
    let mut done = false;
    for _ in 0..100 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE status='SUCCEEDED'")
            .fetch_one(&d.store.pool)
            .await
            .unwrap();
        if n == 1 {
            done = true;
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(done);
    stop.cancel();
    task.await.unwrap().unwrap();
    let stopped: bool =
        sqlx::query_scalar("SELECT stopped_at IS NOT NULL FROM workers WHERE name='drain'")
            .fetch_one(&d.store.pool)
            .await
            .unwrap();
    assert!(stopped);
    d.close().await;
}
