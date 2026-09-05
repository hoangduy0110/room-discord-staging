use crate::{config::Config, state::SharedRegistry};
use std::{fs, sync::Arc, time::Duration};
use tokio::sync::watch;

pub async fn monitor(
    registry: SharedRegistry,
    config: Arc<Config>,
    shutdown_tx: watch::Sender<bool>,
) {
    let mut high_samples = 0u32;
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        let rss = rss_mb().unwrap_or(0.0);
        let should_restart = {
            let mut guard = registry.write().await;
            guard.rss_mb = rss;
            guard.peak_rss_mb = guard.peak_rss_mb.max(rss);
            if rss >= config.memory_restart_mb {
                high_samples += 1;
            } else {
                high_samples = 0;
            }
            if high_samples >= config.memory_restart_samples {
                guard.restart_requested = true;
                guard.restart_reason = Some(format!("memory_guard:{rss:.1}MB"));
                true
            } else {
                false
            }
        };
        if rss >= config.memory_warn_mb {
            tracing::warn!(rss_mb = rss, "memory pressure");
        }
        if should_restart {
            let _ = shutdown_tx.send(true);
            break;
        }
    }
}

fn rss_mb() -> Option<f64> {
    let statm = fs::read_to_string("/proc/self/statm").ok()?;
    let pages: f64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096.0 / 1024.0 / 1024.0)
}
