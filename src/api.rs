use crate::storage::{self, Store, Submit};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use tower::ServiceExt;
use tower_http::services::ServeDir;
use uuid::Uuid;

#[derive(Clone)]
pub struct Config {
    pub mode: String,
    pub token: String,
    pub origin: String,
    pub base_path: String,
    pub web_dir: String,
}
#[derive(Clone)]
struct App {
    store: Store,
    config: Config,
    slots: Arc<Semaphore>,
    rate: Arc<Mutex<(f64, Instant)>>,
}
pub fn router(store: Store, config: Config) -> Router {
    let public = config.mode == "public";
    Router::new().fallback(handle).with_state(App {
        store,
        config,
        slots: Arc::new(Semaphore::new(if public { 12 } else { 32 })),
        rate: Arc::new(Mutex::new((80.0, Instant::now()))),
    })
}
fn response(code: u16, value: Value) -> Response {
    (StatusCode::from_u16(code).expect("status"), Json(value)).into_response()
}
fn failure(code: u16, message: &str) -> Response {
    response(code, json!({"error":message}))
}
fn db_error(e: storage::Error) -> Response {
    match e {
        storage::Error::Database(sqlx::Error::RowNotFound) => failure(404, "job not found"),
        storage::Error::Conflict => {
            failure(409, "idempotency key already used with different request")
        }
        storage::Error::Queue => failure(400, "queue missing or disabled"),
        storage::Error::Capacity => failure(
            429,
            "limit reached: 30 jobs per session, 200 per hour for this public instance",
        ),
        storage::Error::Session => failure(401, "visitor session expired"),
        _ => {
            tracing::warn!("database request unavailable");
            failure(503, "database unavailable")
        }
    }
}
fn result(r: storage::Result<Value>) -> Response {
    match r {
        Ok(v) => response(200, v),
        Err(e) => db_error(e),
    }
}
fn header<'a>(headers: &'a HeaderMap, key: &str) -> &'a str {
    headers.get(key).and_then(|v| v.to_str().ok()).unwrap_or("")
}
fn scope(headers: &HeaderMap) -> String {
    for cookie in header(headers, "cookie").split(';') {
        if let Some(token) = cookie.trim().strip_prefix("taskmesh_session=") {
            if token.len() == 64 {
                return storage::hash(token);
            }
        }
    }
    String::new()
}
async fn handle(State(app): State<App>, mut req: Request) -> Response {
    let public = app.config.mode == "public";
    if public && !app.config.base_path.is_empty() {
        let path = req.uri().path();
        if path == app.config.base_path {
            let mut r = response(308, json!({"redirect":format!("{}/",app.config.base_path)}));
            if let Ok(v) = HeaderValue::from_str(&format!("{}/", app.config.base_path)) {
                r.headers_mut().insert("location", v);
            }
            return r;
        }
        if let Some(rest) = path.strip_prefix(&format!("{}/", app.config.base_path)) {
            let stripped = format!(
                "/{}{}",
                rest,
                req.uri()
                    .query()
                    .map(|q| format!("?{q}"))
                    .unwrap_or_default()
            );
            match stripped.parse::<Uri>() {
                Ok(uri) => *req.uri_mut() = uri,
                Err(_) => return failure(400, "invalid path"),
            }
        }
    }
    let mut out = async {
        if public
            && !matches!(*req.method(), Method::GET | Method::HEAD)
            && header(req.headers(), "origin") != app.config.origin
        {
            return failure(403, "request origin not allowed");
        }
        if public && req.uri().path().starts_with("/public/") {
            let allowed = {
                let mut rate = app.rate.lock().expect("rate mutex");
                rate.0 = (rate.0 + rate.1.elapsed().as_secs_f64() * 40.0).min(80.0);
                rate.1 = Instant::now();
                if rate.0 >= 1.0 {
                    rate.0 -= 1.0;
                    true
                } else {
                    false
                }
            };
            if !allowed {
                return failure(429, "request limit reached; try again shortly");
            }
        }
        let Ok(_permit) = app.slots.try_acquire() else {
            return failure(429, "service is busy; try again shortly");
        };
        match tokio::time::timeout(Duration::from_secs(5), dispatch(&app, req)).await {
            Ok(r) => r,
            Err(_) => failure(503, "request timed out"),
        }
    }
    .await;
    let headers = out.headers_mut();
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("cache-control", HeaderValue::from_static("no-store"));
    if public {
        headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
        headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
        headers.insert("content-security-policy",HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"));
    }
    if out.status() == StatusCode::TOO_MANY_REQUESTS {
        out.headers_mut()
            .insert("retry-after", HeaderValue::from_static("2"));
    }
    out
}
async fn dispatch(app: &App, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    if method == Method::GET && path == "/healthz" {
        return response(200, json!({"status":"alive","runtime":"rust"}));
    }
    if method == Method::GET && path == "/readyz" {
        return match app.store.ready().await {
            Ok(version) => response(
                200,
                json!({"status":"ready","schema":version,"runtime":"rust"}),
            ),
            Err(_) => failure(503, "database or migrations unavailable"),
        };
    }
    if app.config.mode != "api" && app.config.mode != "public" {
        return failure(404, "not found");
    }
    let public = app.config.mode == "public";
    if public && path == "/public/v1/session" && method == Method::POST {
        return session(app, req.headers()).await;
    }
    let prefix = if public { "/public/v1/" } else { "/api/v1/" };
    if !path.starts_with(prefix) {
        if public
            && !path.starts_with("/api/")
            && !path.starts_with("/public/")
            && matches!(method, Method::GET | Method::HEAD)
        {
            return match ServeDir::new(&app.config.web_dir).oneshot(req).await {
                Ok(r) => r.map(Body::new),
                Err(_) => failure(404, "not found"),
            };
        }
        return failure(404, "not found");
    }
    let scope = if public {
        let scope = scope(req.headers());
        if scope.is_empty() {
            return failure(401, "start a visitor session first");
        }
        match app.store.session(&scope).await {
            Ok(_) => scope,
            Err(storage::Error::Database(sqlx::Error::RowNotFound)) => {
                return failure(401, "visitor session expired");
            }
            Err(e) => return db_error(e),
        }
    } else {
        let expected = storage::hash(format!("Bearer {}", app.config.token));
        let actual = storage::hash(header(req.headers(), "authorization"));
        if app.config.token.len() < 32 || !bool::from(expected.as_bytes().ct_eq(actual.as_bytes()))
        {
            return failure(401, "valid bearer token required");
        }
        String::new()
    };
    let owner_scope = if public { Some(scope.as_str()) } else { None };
    let route = path.trim_start_matches(prefix);
    if route == "jobs" && method == Method::POST {
        return submit(app, req, &scope).await;
    }
    if method == Method::GET
        && (route == "jobs" || route == "workers" || (!public && route == "queues"))
    {
        return result(app.store.list(route, owner_scope).await);
    }
    let parts: Vec<_> = route.split('/').collect();
    if parts.len() >= 2 && parts[0] == "jobs" {
        let id = match Uuid::parse_str(parts[1]) {
            Ok(id) => id,
            Err(_) => return failure(400, "invalid job ID"),
        };
        let valid = (method == Method::GET
            && (parts.len() == 2
                || (parts.len() == 3 && ["events", "attempts"].contains(&parts[2]))))
            || (method == Method::POST && parts.len() == 3 && parts[2] == "cancel");
        if !valid {
            return failure(404, "not found");
        }
        let job = match app.store.get(id, owner_scope).await {
            Ok(job) => job,
            Err(e) => return db_error(e),
        };
        if parts.len() == 2 {
            return response(200, job);
        }
        if parts[2] == "cancel" {
            if let Err(e) = app.store.cancel(id).await {
                return db_error(e);
            }
            return result(app.store.get(id, owner_scope).await);
        }
        return result(app.store.details(id, parts[2]).await);
    }
    failure(404, "not found")
}
async fn session(app: &App, headers: &HeaderMap) -> Response {
    let scope = scope(headers);
    if !scope.is_empty() {
        match app.store.session(&scope).await {
            Ok(v) => return response(200, v),
            Err(storage::Error::Database(sqlx::Error::RowNotFound)) => {}
            Err(e) => return db_error(e),
        }
    }
    let token = hex::encode(rand::random::<[u8; 32]>());
    let value = match app.store.create_session(&storage::hash(&token)).await {
        Ok(v) => v,
        Err(e) => return db_error(e),
    };
    let mut r = response(201, value);
    let path = if app.config.base_path.is_empty() {
        "/".into()
    } else {
        format!("{}/", app.config.base_path)
    };
    let secure = if app.config.origin.starts_with("https://") {
        "; Secure"
    } else {
        ""
    };
    let cookie = format!(
        "taskmesh_session={token}; Path={path}; Max-Age=86400; HttpOnly; SameSite=Strict{secure}"
    );
    r.headers_mut().insert(
        "set-cookie",
        HeaderValue::from_str(&cookie).expect("validated cookie config"),
    );
    r
}
async fn submit(app: &App, req: Request, scope: &str) -> Response {
    if header(req.headers(), "content-type")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        != "application/json"
    {
        return failure(415, "Content-Type must be application/json");
    }
    let key = header(req.headers(), "idempotency-key").to_owned();
    if key.len() > 128 {
        return failure(400, "Idempotency-Key must be <=128 bytes");
    }
    let public = app.config.mode == "public";
    let limit = if public { 8192 } else { 65536 };
    let bytes = match to_bytes(req.into_body(), limit).await {
        Ok(b) => b,
        Err(_) => return failure(413, "input body limit exceeded"),
    };
    let mut input: Submit = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return failure(400, "invalid job JSON"),
    };
    input.defaults();
    if public {
        if !["default", "reports", "analytics"].contains(&input.queue.as_str()) {
            return failure(400, "unknown queue");
        }
        if input.run_at.is_some() {
            return failure(400, "delayed scheduling is unavailable in visitor sessions");
        }
        if input.max_attempts > 3 || input.timeout_seconds > 120 {
            return failure(400, "visitor task limits exceeded");
        }
        input.timeout_seconds = 15;
        if input.typ == "benchmark.compute"
            && input.payload["rounds"].as_u64().is_some_and(|n| n > 10000)
        {
            return failure(400, "visitor computation limit is 10000 rounds");
        }
    }
    if let Err(e) = input.validate() {
        return failure(400, &e);
    }
    match app.store.submit(&input, &key, scope).await {
        Ok((mut job, reused)) => {
            if public {
                if let Some(m) = job.as_object_mut() {
                    m.remove("public_session_hash");
                    m.remove("lease_owner");
                }
            }
            let location = format!("/api/v1/jobs/{}", job["id"].as_str().unwrap_or_default());
            let mut r = response(if reused { 200 } else { 201 }, job);
            if !public {
                if let Ok(v) = HeaderValue::from_str(&location) {
                    r.headers_mut().insert("location", v);
                }
            }
            r
        }
        Err(e) => db_error(e),
    }
}
