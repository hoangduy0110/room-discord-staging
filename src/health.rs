use crate::{config::Config, state::SharedRegistry};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};

#[derive(Clone)]
struct AppState {
    registry: SharedRegistry,
    config: Arc<Config>,
}

pub async fn serve(registry: SharedRegistry, config: Arc<Config>) -> anyhow::Result<()> {
    let state = AppState {
        registry,
        config: config.clone(),
    };
    let app = Router::new()
        .route("/", get(root))
        .route("/live", get(live))
        .route("/health", get(health))
        .route("/status", get(status))
        .with_state(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn root() -> &'static str {
    "room-discord-rs"
}

async fn live(State(state): State<AppState>) -> impl IntoResponse {
    let guard = state.registry.read().await;
    let startup = guard.started.elapsed() < state.config.startup_grace;
    let supervisors_alive = guard.nodes.values().filter(|n| n.supervisor_alive).count();
    let ok = startup || (supervisors_alive == guard.nodes.len() && !guard.restart_requested);
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(json!({
            "alive": ok,
            "startup_grace": startup,
            "supervisors_alive": supervisors_alive,
            "supervisors_expected": guard.nodes.len(),
            "rss_mb": guard.rss_mb
        })),
    )
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let guard = state.registry.read().await;
    let startup = guard.started.elapsed() < state.config.startup_grace;
    let expired = guard
        .nodes
        .values()
        .any(|n| n.unhealthy_for() >= state.config.health_fail_after);
    let supervisors_failed = guard.nodes.values().any(|n| !n.supervisor_alive);
    let snapshot = guard.snapshot();
    let ok = startup || (!guard.restart_requested && !supervisors_failed && !expired);
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let label = if startup {
        "starting"
    } else if snapshot.healthy {
        "healthy"
    } else if ok {
        "recovering"
    } else {
        "unhealthy"
    };
    (
        code,
        Json(json!({
            "status": label,
            "startup_grace": startup,
            "failure_threshold_seconds": state.config.health_fail_after.as_secs(),
            "snapshot": snapshot
        })),
    )
}

async fn status(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({"snapshot": state.registry.read().await.snapshot()}))
}
