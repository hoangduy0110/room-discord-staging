use serde::Serialize;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

pub type SharedRegistry = Arc<RwLock<Registry>>;

#[derive(Clone, Debug, Serialize)]
pub struct NodeState {
    pub label: String,
    pub supervisor_alive: bool,
    pub generation: u64,
    pub gateway_ready: bool,
    pub gateway_heartbeat_ok: bool,
    pub user_id: Option<String>,
    pub in_target_channel: bool,
    pub current_channel_id: Option<String>,
    pub voice_connected: bool,
    pub voice_heartbeat_ok: bool,
    pub dave_ready: bool,
    pub gateway_reconnects: u64,
    pub voice_resumes: u64,
    pub voice_full_reconnects: u64,

    // Connection observability. These fields are diagnostic only.
    pub main_disconnect_count: u64,
    pub main_resume_count: u64,
    pub main_resume_fail_count: u64,
    pub voice_disconnect_count: u64,
    pub voice_resume_count: u64,
    pub voice_resume_fail_count: u64,
    pub last_main_disconnect_reason: Option<String>,
    pub last_main_close_code: Option<u16>,
    pub last_voice_disconnect_reason: Option<String>,
    pub last_main_resume_latency_ms: Option<u64>,
    pub last_voice_resume_latency_ms: Option<u64>,

    pub join_attempts: u64,
    pub join_successes: u64,
    pub last_gateway_ack_at: Option<u64>,
    pub last_voice_state_at: Option<u64>,
    pub last_join_attempt_at: Option<u64>,
    pub last_voice_ack_at: Option<u64>,
    pub last_voice_connected_at: Option<u64>,

    // Subsystem-specific errors
    pub main_error: Option<String>,
    pub voice_error: Option<String>,
    pub dave_error: Option<String>,
    pub supervisor_error: Option<String>,

    // Voice ownership – authoritative, protected by registry lock
    pub voice_worker_id: u64,

    #[serde(skip)]
    unhealthy_since: Option<Instant>,
}

impl NodeState {
    pub fn new(label: String) -> Self {
        Self {
            label,
            supervisor_alive: false,
            generation: 0,
            gateway_ready: false,
            gateway_heartbeat_ok: false,
            user_id: None,
            in_target_channel: false,
            current_channel_id: None,
            voice_connected: false,
            voice_heartbeat_ok: false,
            dave_ready: false,
            gateway_reconnects: 0,
            voice_resumes: 0,
            voice_full_reconnects: 0,
            main_disconnect_count: 0,
            main_resume_count: 0,
            main_resume_fail_count: 0,
            voice_disconnect_count: 0,
            voice_resume_count: 0,
            voice_resume_fail_count: 0,
            last_main_disconnect_reason: None,
            last_main_close_code: None,
            last_voice_disconnect_reason: None,
            last_main_resume_latency_ms: None,
            last_voice_resume_latency_ms: None,
            join_attempts: 0,
            join_successes: 0,
            last_gateway_ack_at: None,
            last_voice_state_at: None,
            last_join_attempt_at: None,
            last_voice_ack_at: None,
            last_voice_connected_at: None,
            main_error: None,
            voice_error: None,
            dave_error: None,
            supervisor_error: None,
            voice_worker_id: 0,
            unhealthy_since: Some(Instant::now()),
        }
    }

    pub fn healthy(&self) -> bool {
        self.supervisor_alive
            && self.gateway_ready
            && self.gateway_heartbeat_ok
            && self.in_target_channel
            && self.voice_connected
            && self.voice_heartbeat_ok
            && self.dave_ready
            && self.main_error.is_none()
            && self.voice_error.is_none()
            && self.dave_error.is_none()
            && self.supervisor_error.is_none()
    }

    pub fn status(&self) -> &'static str {
        if !self.supervisor_alive {
            "supervisor_stopped"
        } else if !self.gateway_ready {
            "gateway_connecting"
        } else if !self.gateway_heartbeat_ok {
            "gateway_recovering"
        } else if !self.in_target_channel {
            "joining_voice"
        } else if !self.voice_connected {
            "voice_connecting"
        } else if !self.voice_heartbeat_ok {
            "voice_recovering"
        } else if self.dave_error.is_some() {
            "dave_issue"
        } else if !self.dave_ready {
            "dave_pending"
        } else if self.main_error.is_some() {
            "main_issue"
        } else if self.supervisor_error.is_some() {
            "supervisor_issue"
        } else if self.voice_error.is_some() {
            "voice_issue"
        } else {
            "healthy"
        }
    }

    pub fn refresh_health_clock(&mut self) {
        if self.healthy() {
            self.unhealthy_since = None;
        } else if self.unhealthy_since.is_none() {
            self.unhealthy_since = Some(Instant::now());
        }
    }

    pub fn unhealthy_for(&self) -> Duration {
        self.unhealthy_since
            .map(|v| v.elapsed())
            .unwrap_or_default()
    }
}

#[derive(Debug, Serialize)]
pub struct NodeView {
    #[serde(flatten)]
    pub state: NodeState,
    pub status: &'static str,
    pub healthy: bool,
    pub unhealthy_seconds: f64,
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub mode: &'static str,
    pub healthy: bool,
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub rss_mb: f64,
    pub peak_rss_mb: f64,
    pub restart_requested: bool,
    pub restart_reason: Option<String>,
    pub nodes: Vec<NodeView>,
}

pub struct Registry {
    pub started: Instant,
    pub nodes: BTreeMap<String, NodeState>,
    pub rss_mb: f64,
    pub peak_rss_mb: f64,
    pub restart_requested: bool,
    pub restart_reason: Option<String>,
}

impl Registry {
    pub fn new(labels: impl IntoIterator<Item = String>) -> Self {
        Self {
            started: Instant::now(),
            nodes: labels
                .into_iter()
                .map(|l| (l.clone(), NodeState::new(l)))
                .collect(),
            rss_mb: 0.0,
            peak_rss_mb: 0.0,
            restart_requested: false,
            restart_reason: None,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let nodes: Vec<NodeView> = self
            .nodes
            .values()
            .cloned()
            .map(|state| NodeView {
                status: state.status(),
                healthy: state.healthy(),
                unhealthy_seconds: state.unhealthy_for().as_secs_f64(),
                state,
            })
            .collect();
        let healthy_nodes = nodes.iter().filter(|n| n.healthy).count();
        Snapshot {
            mode: "minimal_voice",
            healthy: healthy_nodes == nodes.len() && !nodes.is_empty() && !self.restart_requested,
            total_nodes: nodes.len(),
            healthy_nodes,
            rss_mb: self.rss_mb,
            peak_rss_mb: self.peak_rss_mb,
            restart_requested: self.restart_requested,
            restart_reason: self.restart_reason.clone(),
            nodes,
        }
    }
}

pub async fn with_node(registry: &SharedRegistry, label: &str, f: impl FnOnce(&mut NodeState)) {
    let mut guard = registry.write().await;
    if let Some(node) = guard.nodes.get_mut(label) {
        f(node);
        node.refresh_health_clock();
    }
}

pub fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_healthy_equivalence_for_all_health_inputs() {
        for mask in 0u16..2048 {
            let mut node = NodeState::new("test".into());
            node.supervisor_alive = mask & (1 << 0) != 0;
            node.gateway_ready = mask & (1 << 1) != 0;
            node.gateway_heartbeat_ok = mask & (1 << 2) != 0;
            node.in_target_channel = mask & (1 << 3) != 0;
            node.voice_connected = mask & (1 << 4) != 0;
            node.voice_heartbeat_ok = mask & (1 << 5) != 0;
            node.dave_ready = mask & (1 << 6) != 0;
            node.main_error = (mask & (1 << 7) != 0).then_some(String::from("main"));
            node.voice_error = (mask & (1 << 8) != 0).then_some(String::from("voice"));
            node.dave_error = (mask & (1 << 9) != 0).then_some(String::from("dave"));
            node.supervisor_error = (mask & (1 << 10) != 0).then_some(String::from("supervisor"));

            assert_eq!(
                node.status() == "healthy",
                node.healthy(),
                "status/healthy mismatch for mask {mask:#013b}: status={}",
                node.status()
            );
        }
    }

    #[test]
    fn dave_error_takes_precedence_over_ready_flag() {
        let mut node = NodeState::new("test".into());
        node.supervisor_alive = true;
        node.gateway_ready = true;
        node.gateway_heartbeat_ok = true;
        node.in_target_channel = true;
        node.voice_connected = true;
        node.voice_heartbeat_ok = true;
        node.dave_ready = true;
        node.dave_error = Some("dave_failed".into());

        assert!(!node.healthy());
        assert_eq!(node.status(), "dave_issue");
    }
}
