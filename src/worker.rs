use crate::{
    handlers,
    storage::{Claim, Store},
};
use futures_util::FutureExt;
use std::time::Duration;
use tokio::{
    task::JoinSet,
    time::{interval, sleep},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone)]
pub struct Config {
    pub name: String,
    pub queues: Vec<String>,
    pub concurrency: i32,
}
impl Config {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.name.is_empty()
                && self.name.len() <= 128
                && (1..=16).contains(&self.concurrency)
                && !self.queues.is_empty(),
            "invalid worker identity, queues or concurrency"
        );
        let mut q = self.queues.clone();
        q.sort();
        q.dedup();
        anyhow::ensure!(q.len() == self.queues.len(), "duplicate queue");
        Ok(())
    }
}
pub async fn run(store: Store, cfg: Config, stop: CancellationToken) -> anyhow::Result<()> {
    cfg.validate()?;
    let owner = Uuid::new_v4();
    store
        .register(owner, &cfg.name, &cfg.queues, cfg.concurrency)
        .await?;
    tracing::info!(worker=%cfg.name,id=%owner,"worker registered");
    let runtime = CancellationToken::new();
    let heart = {
        let store = store.clone();
        let runtime = runtime.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(5));
            loop {
                tokio::select! {_ = runtime.cancelled()=>break,_ = tick.tick()=>{if store.heartbeat(owner).await.is_err(){tracing::warn!("worker heartbeat unavailable");}}}
            }
        })
    };
    let mut slots = JoinSet::new();
    for slot in 0..cfg.concurrency {
        let store = store.clone();
        let cfg = cfg.clone();
        let stop = stop.clone();
        let runtime = runtime.clone();
        slots.spawn(async move {
            let mut offset=slot as usize;
            loop {
                if stop.is_cancelled() || runtime.is_cancelled(){break;}
                let mut claimed=false;
                for n in 0..cfg.queues.len() {
                    if stop.is_cancelled() || runtime.is_cancelled(){break;}
                    let queue=&cfg.queues[(n+offset)%cfg.queues.len()];
                    match store.claim(owner,queue,30.0).await {
                        Ok(Some(job))=>{claimed=true;execute(&store,owner,job,&runtime).await;offset+=1;break;}
                        Ok(None)=>{},
                        Err(_)=>tracing::warn!(%queue,"claim unavailable"),
                    }
                }
                if !claimed {tokio::select!{_ = stop.cancelled()=>break,_ = runtime.cancelled()=>break,_ = sleep(Duration::from_millis(500))=>{}}}
            }
        });
    }
    stop.cancelled().await;
    let drain = async {
        while let Some(r) = slots.join_next().await {
            if let Err(e) = r {
                tracing::error!(error=%e,"worker slot failed");
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(30), drain)
        .await
        .is_err()
    {
        runtime.cancel();
        while slots.join_next().await.is_some() {}
    }
    runtime.cancel();
    heart.await?;
    store.stop(owner).await?;
    Ok(())
}
async fn execute(store: &Store, owner: Uuid, job: Claim, stop: &CancellationToken) {
    let permanent = handlers::validate(&job.typ, &job.payload).is_err();
    let expires = tokio::time::Instant::now() + Duration::from_secs(job.timeout_seconds as u64);
    let outcome = {
        let work = catch_handler(handlers::execute(&job.typ, &job.payload));
        tokio::pin!(work);
        let deadline = sleep(Duration::from_secs(job.timeout_seconds as u64));
        tokio::pin!(deadline);
        let mut renew = interval(Duration::from_secs(10));
        renew.tick().await;
        loop {
            tokio::select! {
                _ = stop.cancelled()=>break Err("worker shutdown".into()),
                _ = &mut deadline=>break Err("task timed out".into()),
                result = &mut work=>break result,
                _ = renew.tick()=>{if store.renew(owner,&job,30.0).await.is_err(){break Err("lease lost".into());}}
            }
        }
    };
    let outcome = if tokio::time::Instant::now() >= expires {
        Err("task timed out".into())
    } else {
        outcome
    };
    let (result, error) = match outcome {
        Ok(v) => (Some(v), None),
        Err(e) => (None, Some(e)),
    };
    if store
        .finish(owner, &job, result, error.as_deref(), permanent)
        .await
        .is_err()
    {
        tracing::warn!(
            generation = job.generation,
            "result not acknowledged; lease recovery will reconcile"
        );
    }
}

async fn catch_handler<F>(future: F) -> Result<serde_json::Value, String>
where
    F: std::future::Future<Output = Result<serde_json::Value, String>>,
{
    std::panic::AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err("handler panic".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn panic_does_not_destroy_the_worker_slot() {
        let result = catch_handler(async { panic!("controlled handler panic") }).await;
        assert_eq!(result.unwrap_err(), "handler panic");
        assert_eq!(
            catch_handler(async { Ok(serde_json::json!({"ok": true})) })
                .await
                .unwrap()["ok"],
            true
        );
    }
}
