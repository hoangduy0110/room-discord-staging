use crate::{
    state::{now_epoch, SharedRegistry},
    voice::{
        close_codes,
        dave::{DaveOut, DaveState},
        opcodes::*,
        udp,
    },
};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use rand_distr::{Distribution, Normal};
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

const MODE: &str = "aead_aes256_gcm_rtpsize";
const MAX_MISSED_HEARTBEAT_ACKS: u32 = 2;

#[derive(Debug, Clone)]
pub struct VoiceParams {
    pub endpoint: String,
    pub guild_id: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_id: String,
    pub token: String,
    pub heartbeat_jitter_percent: u32,
    pub jitter_stddev_percent: u32,
}

#[derive(Debug)]
pub enum VoiceOutcome {
    Shutdown,
    TryResume,
    NeedFullReconnect,
    Fatal(String),
}

#[derive(Debug, Clone)]
struct ResumeCtx {
    last_seq: i64,
}

pub struct VoiceOwner {
    generation: u64,
    worker_id: u64,
}

impl VoiceOwner {
    pub fn new(generation: u64, worker_id: u64) -> Self {
        Self { generation, worker_id }
    }
}

pub struct VoiceConnection {
    params: VoiceParams,
    resume_ctx: Option<ResumeCtx>,
    dave: DaveState,
    owner: VoiceOwner,
    label: String,
    registry: SharedRegistry,
    shutdown: watch::Receiver<bool>,
}

impl VoiceConnection {
    pub fn new(
        params: VoiceParams,
        owner: VoiceOwner,
        label: String,
        registry: SharedRegistry,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        let user_id = params.user_id.parse::<u64>().unwrap_or(0);
        let channel_id = params.channel_id.parse::<u64>().unwrap_or(0);
        Self {
            params,
            resume_ctx: None,
            dave: DaveState::new(user_id, channel_id),
            owner,
            label,
            registry,
            shutdown,
        }
    }

    pub async fn update_node<F>(&self, f: F) -> bool
    where
        F: FnOnce(&mut crate::state::NodeState),
    {
        let mut guard = self.registry.write().await;
        if let Some(node) = guard.nodes.get_mut(&self.label) {
            if node.generation != self.owner.generation
                || node.voice_worker_id != self.owner.worker_id
            {
                return false;
            }
            f(node);
            node.refresh_health_clock();
            true
        } else {
            false
        }
    }

    async fn sync_dave_state(&self) {
        let dave_ready = self.dave.is_ready();
        self.update_node(|n| {
            n.dave_ready = dave_ready;
            n.dave_error = if dave_ready { None } else { Some("dave_not_ready".to_string()) };
        }).await;
    }

    pub async fn run_once(&mut self) -> anyhow::Result<VoiceOutcome> {
        let url = format!("wss://{}/?v=9", self.params.endpoint);
        info!(node=%self.label, endpoint=%self.params.endpoint, resume=self.resume_ctx.is_some(), "voice gateway connecting");
        let (ws, _) = connect_async(&url).await?;
        let (mut sink, mut stream) = ws.split();

        let hello = read_next_json(&mut stream).await?;
        let heartbeat_ms = hello
            .pointer("/d/heartbeat_interval")
            .and_then(Value::as_f64)
            .ok_or_else(|| anyhow::anyhow!("voice HELLO missing heartbeat_interval"))?
            as u64;

        if let Some(ctx) = &self.resume_ctx {
            let resume = json!({"op": OP_RESUME, "d": {
                "server_id": self.params.guild_id,
                "session_id": self.params.session_id,
                "token": self.params.token,
                "seq_ack": ctx.last_seq
            }});
            sink.send(Message::Text(resume.to_string())).await?;
            info!(node=%self.label, seq_ack=ctx.last_seq, "voice RESUME sent");
        } else {
            let identify = json!({"op": OP_IDENTIFY, "d": {
                "server_id": self.params.guild_id,
                "channel_id": self.params.channel_id,
                "user_id": self.params.user_id,
                "session_id": self.params.session_id,
                "token": self.params.token,
                "video": false,
                "streams": [],
                "max_dave_protocol_version": 1
            }});
            sink.send(Message::Text(identify.to_string())).await?;
        }

        let mut last_seq = self.resume_ctx.as_ref().map(|v| v.last_seq).unwrap_or(-1);
        let mut heartbeat_sleep = Box::pin(tokio::time::sleep(
            heartbeat_delay(heartbeat_ms, self.params.heartbeat_jitter_percent, self.params.jitter_stddev_percent),
        ));
        let mut missed_acks = 0u32;

        loop {
            tokio::select! {
                changed = self.shutdown.changed() => {
                    if changed.is_err() || *self.shutdown.borrow() {
                        let _ = sink.send(Message::Close(None)).await;
                        return Ok(VoiceOutcome::Shutdown);
                    }
                }
                _ = &mut heartbeat_sleep => {
                    if missed_acks >= MAX_MISSED_HEARTBEAT_ACKS {
                        self.update_node(|n| {
                            n.voice_connected = false;
                            n.voice_heartbeat_ok = false;
                            n.voice_error = Some("voice_heartbeat_ack_timeout".into());
                        }).await;
                        self.resume_ctx = Some(ResumeCtx { last_seq });
                        warn!(node=%self.label, missed_acks, "voice heartbeat ACK timeout; attempting resume");
                        return Ok(VoiceOutcome::TryResume);
                    }
                    let nonce: i64 = rand::thread_rng().gen_range(0..(1i64 << 52));
                    let hb = json!({"op": OP_HEARTBEAT, "d": {"t": nonce, "seq_ack": last_seq}});
                    if let Err(e) = sink.send(Message::Text(hb.to_string())).await {
                        self.update_node(|n| {
                            n.voice_connected = false;
                            n.voice_heartbeat_ok = false;
                            n.voice_error = Some("voice_heartbeat_send_failed".into());
                        }).await;
                        self.resume_ctx = Some(ResumeCtx { last_seq });
                        warn!(node=%self.label, error=%e, "voice heartbeat send failed; attempting resume");
                        return Ok(VoiceOutcome::TryResume);
                    }
                    missed_acks += 1;
                    heartbeat_sleep.as_mut().reset(
                        Instant::now() + heartbeat_delay(heartbeat_ms, self.params.heartbeat_jitter_percent, self.params.jitter_stddev_percent),
                    );
                }
                msg = stream.next() => {
                    let msg = match msg {
                        Some(Ok(v)) => v,
                        Some(Err(e)) => {
                            self.update_node(|n| {
                                n.voice_connected = false;
                                n.voice_heartbeat_ok = false;
                                n.voice_error = Some("voice_stream_error".into());
                            }).await;
                            self.resume_ctx = Some(ResumeCtx { last_seq });
                            warn!(node=%self.label, error=%e, "voice stream error; attempting resume");
                            return Ok(VoiceOutcome::TryResume);
                        }
                        None => {
                            self.update_node(|n| {
                                n.voice_connected = false;
                                n.voice_heartbeat_ok = false;
                                n.voice_error = Some("voice_stream_ended".into());
                            }).await;
                            self.resume_ctx = Some(ResumeCtx { last_seq });
                            warn!(node=%self.label, "voice stream ended; attempting resume");
                            return Ok(VoiceOutcome::TryResume);
                        }
                    };
                    match msg {
                        Message::Text(text) => {
                            let v: Value = serde_json::from_str(&text)?;
                            if let Some(seq) = v.get("seq").and_then(Value::as_i64) {
                                last_seq = seq;
                            }
                            let op = v.get("op").and_then(Value::as_u64).unwrap_or(255) as u8;
                            match op {
                                OP_READY => {
                                    let ssrc = v.pointer("/d/ssrc").and_then(Value::as_u64).unwrap_or(0) as u32;
                                    let ip = v.pointer("/d/ip").and_then(Value::as_str).unwrap_or("");
                                    let port = v.pointer("/d/port").and_then(Value::as_u64).unwrap_or(0) as u16;
                                    let discovered = udp::discover(ssrc, ip, port).await?;
                                    let select = json!({"op": OP_SELECT_PROTOCOL, "d": {
                                        "protocol": "udp",
                                        "data": {
                                            "address": discovered.external_ip,
                                            "port": discovered.external_port,
                                            "mode": MODE
                                        }
                                    }});
                                    sink.send(Message::Text(select.to_string())).await?;
                                }
                                OP_SESSION_DESCRIPTION => {
                                    let dpv = v.pointer("/d/dave_protocol_version").and_then(Value::as_u64).unwrap_or(0) as u16;
                                    let outs = self.dave.on_session_description(dpv);
                                    send_dave_out(&mut sink, outs).await?;
                                    let dave_ready = self.dave.is_ready();
                                    self.update_node(|n| {
                                        n.voice_connected = true;
                                        n.voice_heartbeat_ok = true;
                                        n.dave_ready = dave_ready;
                                        n.last_voice_connected_at = Some(now_epoch());
                                        n.dave_error = if dave_ready { None } else { Some("dave_not_ready".into()) };
                                        n.voice_error = None;
                                    }).await;
                                    info!(node=%self.label, dave_protocol_version=dpv, dave_ready, "minimal voice connected");
                                }
                                OP_DAVE_PREPARE_TRANSITION
                                | OP_DAVE_EXECUTE_TRANSITION
                                | OP_DAVE_PREPARE_EPOCH => {
                                    let outs = self.dave.on_text_op(op, v.get("d").unwrap_or(&Value::Null));
                                    send_dave_out(&mut sink, outs).await?;
                                    self.sync_dave_state().await;
                                }
                                OP_HEARTBEAT_ACK => {
                                    missed_acks = 0;
                                    self.update_node(|n| {
                                        n.voice_heartbeat_ok = true;
                                        n.last_voice_ack_at = Some(now_epoch());
                                    }).await;
                                }
                                OP_RESUMED => {
                                    missed_acks = 0;
                                    let dave_ready = self.dave.is_ready();
                                    self.update_node(|n| {
                                        n.voice_connected = true;
                                        n.voice_heartbeat_ok = true;
                                        n.voice_resumes += 1;
                                        n.dave_ready = dave_ready;
                                        n.dave_error = if dave_ready { None } else { Some("dave_not_ready".into()) };
                                        n.voice_error = None;
                                    }).await;
                                    info!(node=%self.label, "minimal voice resumed");
                                }
                                OP_HELLO | OP_CLIENT_DISCONNECT => {}
                                _ => debug!(node=%self.label, op, "voice opcode"),
                            }
                        }
                        Message::Binary(bin) => {
                            if bin.len() >= 3 {
                                last_seq = u16::from_be_bytes([bin[0], bin[1]]) as i64;
                                let outs = self.dave.on_binary(bin[2], &bin[3..]);
                                send_dave_out(&mut sink, outs).await?;
                                self.sync_dave_state().await;
                            }
                        }
                        Message::Close(frame) => {
                            let (code, reason) = frame.map(|f| (u16::from(f.code), f.reason.to_string())).unwrap_or_default();
                            let action = close_codes::classify(code);
                            self.update_node(|n| {
                                n.voice_connected = false;
                                n.voice_heartbeat_ok = false;
                                n.voice_error = Some(format!("voice_close:{code}:{reason}"));
                            }).await;
                            warn!(node=%self.label, code, %reason, ?action, "voice gateway closed");
                            let outcome = close_action_to_outcome(action);
                            match outcome {
                                VoiceOutcome::TryResume => {
                                    self.resume_ctx = Some(ResumeCtx { last_seq });
                                    return Ok(VoiceOutcome::TryResume);
                                }
                                VoiceOutcome::NeedFullReconnect => {
                                    self.resume_ctx = None;
                                    let user_id = self.params.user_id.parse::<u64>().unwrap_or(0);
                                    let channel_id = self.params.channel_id.parse::<u64>().unwrap_or(0);
                                    self.dave = DaveState::new(user_id, channel_id);
                                    return Ok(VoiceOutcome::NeedFullReconnect);
                                }
                                VoiceOutcome::Fatal(reason) => {
                                    self.resume_ctx = None;
                                    let user_id = self.params.user_id.parse::<u64>().unwrap_or(0);
                                    let channel_id = self.params.channel_id.parse::<u64>().unwrap_or(0);
                                    self.dave = DaveState::new(user_id, channel_id);
                                    warn!(node=%self.label, %reason, "voice close action fatal; propagating to supervisor");
                                    return Ok(VoiceOutcome::Fatal(reason));
                                }
                                VoiceOutcome::Shutdown => unreachable!(),
                            }
                        }
                        Message::Ping(p) => { sink.send(Message::Pong(p)).await?; }
                        _ => {}
                    }
                }
            }
        }
    }
}

async fn send_dave_out<S>(sink: &mut S, outs: Vec<DaveOut>) -> anyhow::Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::error::Error + Send + Sync + 'static,
{
    for out in outs {
        let msg = match out {
            DaveOut::Binary { op, data } => {
                let mut buf = Vec::with_capacity(data.len() + 1);
                buf.push(op);
                buf.extend_from_slice(&data);
                Message::Binary(buf)
            }
            DaveOut::Json(v) => Message::Text(v.to_string()),
        };
        sink.send(msg).await?;
    }
    Ok(())
}

async fn read_next_json<S>(stream: &mut S) -> anyhow::Result<Value>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => return Ok(serde_json::from_str(&t)?),
            Some(Ok(Message::Binary(b))) => return Ok(serde_json::from_slice(&b)?),
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(frame))) => anyhow::bail!("voice closed before HELLO: {frame:?}"),
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("voice stream ended before HELLO"),
        }
    }
}

fn heartbeat_delay(base_ms: u64, percent: u32, stddev_percent: u32) -> Duration {
    let mean = base_ms as f64;
    let stddev = base_ms as f64 * stddev_percent as f64 / 100.0;
    if stddev <= 0.0 {
        return Duration::from_millis(base_ms.max(1));
    }
    let normal = Normal::new(mean, stddev)
        .unwrap_or_else(|_| Normal::new(mean, 1.0).expect("valid fallback"));
    let mut rng = rand::thread_rng();
    let val = normal.sample(&mut rng);
    let val = val.clamp(mean - mean * percent as f64 / 100.0, mean + mean * percent as f64 / 100.0);
    Duration::from_millis(val.max(1.0) as u64)
}

// Pure helper: map CloseAction to VoiceOutcome (used by production)
pub(crate) fn close_action_to_outcome(action: close_codes::CloseAction) -> VoiceOutcome {
    match action {
        close_codes::CloseAction::Resume => VoiceOutcome::TryResume,
        close_codes::CloseAction::FullReconnect => VoiceOutcome::NeedFullReconnect,
        close_codes::CloseAction::Fatal(reason) => VoiceOutcome::Fatal(reason.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Registry;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn test_params() -> VoiceParams {
        VoiceParams {
            endpoint: "example.invalid".into(),
            guild_id: "1".into(),
            channel_id: "2".into(),
            user_id: "3".into(),
            session_id: "4".into(),
            token: "5".into(),
            heartbeat_jitter_percent: 10,
            jitter_stddev_percent: 3,
        }
    }

    fn registry_with_owner(generation: u64, worker_id: u64) -> (SharedRegistry, String) {
        let label = "test".to_string();
        let mut registry = Registry::new([label.clone()]);
        let node = registry.nodes.get_mut(&label).expect("test node exists");
        node.generation = generation;
        node.voice_worker_id = worker_id;
        (Arc::new(RwLock::new(registry)), label)
    }

    fn connection(
        registry: SharedRegistry,
        label: String,
        generation: u64,
        worker_id: u64,
    ) -> VoiceConnection {
        VoiceConnection::new(
            test_params(),
            VoiceOwner::new(generation, worker_id),
            label,
            registry,
            tokio::sync::watch::channel(false).1,
        )
    }

    #[tokio::test]
    async fn current_owner_can_write() {
        let (registry, label) = registry_with_owner(5, 8);
        let voice = connection(registry.clone(), label.clone(), 5, 8);
        assert!(voice.update_node(|n| n.voice_connected = true).await);
        assert!(registry.read().await.nodes[&label].voice_connected);
    }

    #[tokio::test]
    async fn same_generation_stale_worker_is_rejected() {
        let (registry, label) = registry_with_owner(5, 9);
        let voice = connection(registry.clone(), label.clone(), 5, 8);
        assert!(!voice.update_node(|n| n.voice_connected = true).await);
        assert!(!registry.read().await.nodes[&label].voice_connected);
    }

    #[tokio::test]
    async fn cross_generation_aba_is_rejected() {
        let (registry, label) = registry_with_owner(6, 1);
        let old_voice = connection(registry.clone(), label.clone(), 5, 1);
        assert!(!old_voice.update_node(|n| n.voice_connected = true).await);
        let node = registry.read().await;
        assert_eq!(node.nodes[&label].generation, 6);
        assert_eq!(node.nodes[&label].voice_worker_id, 1);
        assert!(!node.nodes[&label].voice_connected);
    }

    #[tokio::test]
    async fn stale_worker_cannot_overwrite_connection_state() {
        let (registry, label) = registry_with_owner(7, 3);
        {
            let mut guard = registry.write().await;
            let node = guard.nodes.get_mut(&label).expect("test node exists");
            node.voice_connected = true;
            node.voice_heartbeat_ok = true;
        }
        let stale = connection(registry.clone(), label.clone(), 7, 2);
        assert!(!stale.update_node(|n| {
            n.voice_connected = false;
            n.voice_heartbeat_ok = false;
        }).await);

        let guard = registry.read().await;
        let node = &guard.nodes[&label];
        assert!(node.voice_connected);
        assert!(node.voice_heartbeat_ok);
    }

    #[tokio::test]
    async fn stale_worker_cannot_clear_new_worker_error() {
        let (registry, label) = registry_with_owner(7, 3);
        registry.write().await.nodes.get_mut(&label).expect("test node exists").voice_error =
            Some("new_worker_error".into());
        let stale = connection(registry.clone(), label.clone(), 7, 2);
        assert!(!stale.update_node(|n| n.voice_error = None).await);
        assert_eq!(
            registry.read().await.nodes[&label].voice_error.as_deref(),
            Some("new_worker_error")
        );
    }

    #[tokio::test]
    async fn stale_worker_cannot_modify_dave_state() {
        let (registry, label) = registry_with_owner(8, 4);
        {
            let mut guard = registry.write().await;
            let node = guard.nodes.get_mut(&label).expect("test node exists");
            node.dave_ready = true;
            node.dave_error = None;
        }
        let stale = connection(registry.clone(), label.clone(), 8, 3);
        assert!(!stale.update_node(|n| {
            n.dave_ready = false;
            n.dave_error = Some("stale_dave_error".into());
        }).await);

        let guard = registry.read().await;
        let node = &guard.nodes[&label];
        assert!(node.dave_ready);
        assert!(node.dave_error.is_none());
    }

    #[tokio::test]
    async fn stale_worker_cannot_modify_heartbeat_state() {
        let (registry, label) = registry_with_owner(9, 5);
        {
            let mut guard = registry.write().await;
            let node = guard.nodes.get_mut(&label).expect("test node exists");
            node.voice_heartbeat_ok = true;
            node.last_voice_ack_at = Some(123);
        }
        let stale = connection(registry.clone(), label.clone(), 9, 4);
        assert!(!stale.update_node(|n| {
            n.voice_heartbeat_ok = false;
            n.last_voice_ack_at = Some(999);
        }).await);

        let guard = registry.read().await;
        let node = &guard.nodes[&label];
        assert!(node.voice_heartbeat_ok);
        assert_eq!(node.last_voice_ack_at, Some(123));
    }

    #[test]
    fn close_action_fatal_maps_to_voice_outcome_fatal() {
        let action = close_codes::CloseAction::Fatal("test reason");
        let outcome = close_action_to_outcome(action);
        assert!(matches!(outcome, VoiceOutcome::Fatal(ref r) if r == "test reason"));
    }
}