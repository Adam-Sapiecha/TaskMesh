use std::{env, time::Duration};
use taskmesh::{api, storage::Store, worker};
use tokio_util::sync::CancellationToken;

fn setting(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.into())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(setting("RUST_LOG", "info"))
        .init();
    let mode = env::args().nth(1).unwrap_or_default();
    if mode == "probe" {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        return tokio::time::timeout(Duration::from_secs(3), async {
            let mut socket =
                tokio::net::TcpStream::connect(format!("127.0.0.1:{}", setting("PORT", "8080")))
                    .await?;
            socket
                .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await?;
            let mut bytes = [0; 64];
            let n = socket.read(&mut bytes).await?;
            anyhow::ensure!(bytes[..n].starts_with(b"HTTP/1.1 200"), "service not ready");
            Ok(())
        })
        .await?;
    }
    anyhow::ensure!(
        ["api", "public", "worker", "controller", "migrate"].contains(&mode.as_str()),
        "usage: taskmesh api|public|worker|controller|migrate|probe"
    );
    let url = env::var("DATABASE_URL").map_err(|_| anyhow::anyhow!("DATABASE_URL is required"))?;
    let mut store = None;
    for _ in 0..12 {
        match Store::open(&url).await {
            Ok(s) => {
                store = Some(s);
                break;
            }
            Err(_) => {
                tracing::warn!("database unavailable; waiting");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    let store = store.ok_or_else(|| anyhow::anyhow!("database startup retry budget exhausted"))?;
    if mode == "migrate" {
        store.migrate().await?;
        if setting("TASKMESH_GRANT_RUNTIME_ROLE", "") == "1" {
            store
                .grant_runtime(setting("TASKMESH_GRANT_PUBLIC_ROLE", "") == "1")
                .await?;
        }
        tracing::info!("migrations applied");
        return Ok(());
    }
    let cfg = api::Config {
        mode: mode.clone(),
        token: setting("TASKMESH_API_KEY", ""),
        origin: setting("PUBLIC_ORIGIN", ""),
        base_path: setting("PUBLIC_BASE_PATH", "").trim_end_matches('/').into(),
        web_dir: setting("WEB_DIR", "web/dist"),
    };
    if mode == "api" {
        anyhow::ensure!(
            cfg.token.len() >= 32,
            "TASKMESH_API_KEY must contain at least 32 bytes"
        );
    }
    if mode == "public" {
        anyhow::ensure!(
            cfg.origin.starts_with("https://") || cfg.origin.starts_with("http://"),
            "PUBLIC_ORIGIN is required"
        );
        anyhow::ensure!(
            !cfg.origin.contains(['\r', '\n', ';']) && !cfg.base_path.contains(['\r', '\n', ';']),
            "invalid public configuration"
        );
        anyhow::ensure!(
            cfg.base_path.is_empty() || cfg.base_path.starts_with('/'),
            "base path must start with /"
        );
    }
    let listener =
        tokio::net::TcpListener::bind(format!("0.0.0.0:{}", setting("PORT", "8080"))).await?;
    let stop = CancellationToken::new();
    let mut tasks = tokio::task::JoinSet::new();
    let router = api::router(store.clone(), cfg);
    {
        let stop = stop.clone();
        tasks.spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(stop.cancelled_owned())
                .await
                .map_err(anyhow::Error::from)
        });
    }
    if mode == "worker" {
        let cfg = worker::Config {
            name: setting("WORKER_NAME", "worker"),
            queues: setting("WORKER_QUEUES", "default,reports,analytics")
                .split(',')
                .map(String::from)
                .collect(),
            concurrency: setting("WORKER_CONCURRENCY", "2").parse()?,
        };
        cfg.validate()?;
        let store = store.clone();
        let stop = stop.clone();
        tasks.spawn(async move { worker::run(store, cfg, stop).await });
    }
    if mode == "controller" || mode == "public" {
        let store = store.clone();
        let stop = stop.clone();
        let public = mode == "public";
        tasks.spawn(async move{
            let mut tick=tokio::time::interval(Duration::from_secs(if public{60}else{5}));
            loop{tokio::select!{_ = stop.cancelled()=>break,_ = tick.tick()=>{
                let res=if public{store.cleanup_public().await.map(|_|())}else{store.recover().await.map(|_|())};
                if res.is_err(){tracing::warn!("maintenance scan unavailable");}
            }}}Ok(())
        });
    }
    tracing::info!(%mode,version=env!("CARGO_PKG_VERSION"),runtime="rust","process ready");
    let result = tokio::select! {_ = shutdown()=>Ok(()),r=tasks.join_next()=>match r{Some(Ok(r))=>r,Some(Err(e))=>Err(e.into()),None=>Ok(())}};
    stop.cancel();
    let drain = async {
        while let Some(r) = tasks.join_next().await {
            r??;
        }
        Ok::<_, anyhow::Error>(())
    };
    match tokio::time::timeout(Duration::from_secs(36), drain).await {
        Ok(r) => r?,
        Err(_) => {
            tasks.abort_all();
            anyhow::bail!("shutdown drain timed out");
        }
    }
    store.pool.close().await;
    result
}
async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {_ = term.recv()=>{},_ = tokio::signal::ctrl_c()=>{}}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
