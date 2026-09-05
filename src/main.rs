mod config;
mod gateway;
mod health;
mod memory;
mod state;
mod supervisor;
mod voice;

use anyhow::Result;
use config::Config;
use state::{Registry, SharedRegistry};
use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,room_discord_rs=info".into()),
        )
        .compact()
        .init();

    let config = Arc::new(Config::from_env()?);
    let labels = config
        .accounts
        .iter()
        .map(|a| a.label.clone())
        .collect::<Vec<_>>();
    let registry: SharedRegistry = Arc::new(RwLock::new(Registry::new(labels)));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    info!(
        accounts=config.accounts.len(),
        guild=%config.guild_id,
        channel=%config.channel_id,
        port=config.port,
        mode="minimal_voice",
        "service starting"
    );

    let health_task = tokio::spawn(health::serve(registry.clone(), config.clone()));
    let memory_task = tokio::spawn(memory::monitor(
        registry.clone(),
        config.clone(),
        shutdown_tx.clone(),
    ));

    let mut supervisors = Vec::new();
    for account in config.accounts.clone() {
        supervisors.push(tokio::spawn(supervisor::run_account(
            account,
            config.clone(),
            registry.clone(),
            shutdown_rx.clone(),
        )));
    }

    tokio::select! {
        _ = wait_for_shutdown(shutdown_rx.clone()) => {},
        _ = tokio::signal::ctrl_c() => {},
        result = health_task => {
            match result {
                Ok(Ok(())) => tracing::warn!("health server stopped"),
                Ok(Err(e)) => tracing::error!(?e, "health server failed"),
                Err(e) => tracing::error!(?e, "health task failed"),
            }
        }
    }
    let _ = shutdown_tx.send(true);

    for task in supervisors {
        let _ = task.await;
    }
    memory_task.abort();

    let restart = registry.read().await.restart_requested;
    if restart {
        std::process::exit(75);
    }
    Ok(())
}

async fn wait_for_shutdown(mut rx: watch::Receiver<bool>) {
    while rx.changed().await.is_ok() {
        if *rx.borrow() {
            return;
        }
    }
}
