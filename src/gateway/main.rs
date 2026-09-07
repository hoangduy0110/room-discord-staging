use crate::{
    config::ClientProfile,
    state::{now_epoch, with_node, SharedRegistry},
};
use futures_util::{Sink, SinkExt, StreamExt};
use rand_distr::{Distribution, Normal};
use serde_json::{json, Value};
use std::pin::Pin;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tokio::time::Sleep;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
};
use tracing::{debug, info, warn};

use super::identify;

const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";
const GATEWAY_MAX_MESSAGE_BYTES: usize = 64 << 20;
const GATEWAY_MAX_FRAME_BYTES: usize = 64 << 20;
const GATEWAY_RECONNECT_MAX: Duration = Duration::from_secs(15);
const GATEWAY_STABLE_AFTER: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub enum MainEvent {
    Ready {
        user_id: String,
    },
    VoiceState {
        user_id: String,
        guild_id: String,
        channel_id: Option<String>,
        session_id: String,
    },
    VoiceServer {
        guild_id: String,
        endpoint: Option<String>,
        token: String,
    },
}

#[derive(Debug, Clone)]
pub enum MainRequest {
    Join {
        guild_id: String,
        channel_id: String,
        mute: bool,
        deaf: bool,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error("heartbeat timeout")]
    HeartbeatTimeout,
    #[error("authentication failed")]
    AuthFailed,
    #[error("network error: {0}")]
    Network(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

#[derive(Debug, Clone, Copy)]
enum GatewayPayloadOutcome {
    Continue,
    Reconnect,
    InvalidSession,
}

#[derive(Debug, Clone, Copy)]
enum SocketOutcome {
    Shutdown,
    Reconnect,
    InvalidSession,
}

pub async fn run(
    label: String,
    token: String,
    profile: ClientProfile,
    registry: SharedRegistry,
    events_tx: mpsc::Sender<MainEvent>,
    mut request_rx: mpsc::Receiver<MainRequest>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut seq: Option<u64> = None;
    let mut session_id: Option<String> = None;
    let mut resume_gateway_url: Option<String> = None;
    let mut self_user_id: Option<String> = None;
    let mut reconnect_backoff = Duration::from_secs(1);

    loop {
        if *shutdown.borrow() {
            break;
        }

        let url = gateway_url(resume_gateway_url.as_deref());
        let can_resume = session_id.is_some() && seq.is_some();
        info!(node=%label, resume=can_resume, "main gateway connecting");

        let ws_config = WebSocketConfig {
            max_message_size: Some(GATEWAY_MAX_MESSAGE_BYTES),
            max_frame_size: Some(GATEWAY_MAX_FRAME_BYTES),
            ..Default::default()
        };

        let connected_at = Instant::now();
        let connect_result = connect_async_with_config(&url, Some(ws_config), false).await;
        let (ws, _) = match connect_result {
            Ok(v) => v,
            Err(e) => {
                warn!(node=%label, ?reconnect_backoff, error=%e, "main gateway connect failed; retrying");
                with_node(&registry, &label, |n| {
                    n.gateway_ready = false;
                    n.gateway_heartbeat_ok = false;
                    n.main_error = Some(format!("main_gateway_connect:{e}"));
                })
                .await;
                if wait_or_shutdown(&mut shutdown, reconnect_backoff).await {
                    break;
                }
                reconnect_backoff = next_reconnect_backoff(reconnect_backoff);
                continue;
            }
        };

        let mut resume_attempted = false;
        let mut resume_succeeded = false;
        let outcome = run_socket(
            &label,
            &token,
            &profile,
            &registry,
            &events_tx,
            &mut request_rx,
            &mut shutdown,
            ws,
            &mut seq,
            &mut session_id,
            &mut resume_gateway_url,
            &mut self_user_id,
            &mut resume_attempted,
            &mut resume_succeeded,
        )
        .await;

        let connection_uptime = connected_at.elapsed();
        let resume_failed = resume_attempted
            && !resume_succeeded
            && !matches!(&outcome, Ok(SocketOutcome::Shutdown));
        if resume_failed {
            with_node(&registry, &label, |n| n.main_resume_fail_count += 1).await;
        }
        if connection_uptime >= GATEWAY_STABLE_AFTER {
            reconnect_backoff = Duration::from_secs(1);
        } else {
            reconnect_backoff = next_reconnect_backoff(reconnect_backoff);
        }

        match outcome {
            Ok(SocketOutcome::Shutdown) => break,
            Ok(SocketOutcome::InvalidSession) => {
                warn!(node=%label, uptime=?connection_uptime, "main gateway session invalid; starting a fresh session");
                seq = None;
                session_id = None;
                resume_gateway_url = None;
                with_node(&registry, &label, |n| {
                    n.gateway_reconnects += 1;
                    n.main_disconnect_count += 1;
                    n.gateway_ready = false;
                    n.gateway_heartbeat_ok = false;
                    n.main_error = Some("main_gateway_invalid_session".into());
                })
                .await;
            }
            Ok(SocketOutcome::Reconnect) => {
                warn!(node=%label, uptime=?connection_uptime, resume=session_id.is_some() && seq.is_some(), "main gateway disconnected; preserving voice transport");
                with_node(&registry, &label, |n| {
                    n.gateway_reconnects += 1;
                    n.main_disconnect_count += 1;
                    n.gateway_ready = false;
                    n.gateway_heartbeat_ok = false;
                    n.main_error = Some("main_gateway_disconnected".into());
                })
                .await;
            }
            Err(e) => {
                if matches!(&e, GatewayError::AuthFailed) {
                    return Err(e.into());
                }
                warn!(node=%label, uptime=?connection_uptime, error=%e, "main gateway connection error; preserving voice transport");
                with_node(&registry, &label, |n| {
                    n.gateway_reconnects += 1;
                    n.main_disconnect_count += 1;
                    n.last_main_disconnect_reason = Some(e.to_string());
                    n.last_main_close_code = None;
                    n.gateway_ready = false;
                    n.gateway_heartbeat_ok = false;
                    n.main_error = Some(format!("main_gateway:{e}"));
                })
                .await;
            }
        }

        if wait_or_shutdown(&mut shutdown, reconnect_backoff).await {
            break;
        }
    }

    warn!(node=%label, "main gateway stopped");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_socket<S>(
    label: &str,
    token: &str,
    profile: &ClientProfile,
    registry: &SharedRegistry,
    events_tx: &mpsc::Sender<MainEvent>,
    request_rx: &mut mpsc::Receiver<MainRequest>,
    shutdown: &mut watch::Receiver<bool>,
    ws: tokio_tungstenite::WebSocketStream<S>,
    seq: &mut Option<u64>,
    session_id: &mut Option<String>,
    resume_gateway_url: &mut Option<String>,
    self_user_id: &mut Option<String>,
    resume_attempted: &mut bool,
    resume_succeeded: &mut bool,
) -> Result<SocketOutcome, GatewayError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut sink, mut stream) = ws.split();

    let hello = next_json(&mut stream)
        .await
        .map_err(|e| GatewayError::Protocol(e.to_string()))?;
    let heartbeat_ms = hello
        .pointer("/d/heartbeat_interval")
        .and_then(Value::as_u64)
        .ok_or_else(|| GatewayError::Protocol("missing heartbeat interval".to_string()))?;

    let can_resume = session_id.is_some() && seq.is_some();
    let mut resume_started_at = None;
    if can_resume {
        let resume = json!({
            "op": 6,
            "d": {
                "token": token,
                "session_id": session_id.as_deref().unwrap_or_default(),
                "seq": *seq
            }
        });
        sink.send(Message::Text(resume.to_string()))
            .await
            .map_err(|e| GatewayError::Network(e.to_string()))?;
        *resume_attempted = true;
        resume_started_at = Some(Instant::now());
        info!(node=%label, seq=?seq, "main gateway RESUME sent");
    } else {
        sink.send(Message::Text(
            identify::identify(token, profile).to_string(),
        ))
        .await
        .map_err(|e| GatewayError::Network(e.to_string()))?;
    }

    let mut awaiting_ack = false;
    let mut heartbeat_sleep = Box::pin(tokio::time::sleep(heartbeat_delay(
        heartbeat_ms,
        profile.heartbeat_jitter_percent,
        profile.jitter_stddev_percent,
    )));

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = sink.send(Message::Close(None)).await;
                    return Ok(SocketOutcome::Shutdown);
                }
            }
            _ = &mut heartbeat_sleep => {
                if awaiting_ack {
                    with_node(registry, label, |n| {
                        n.gateway_ready = false;
                        n.gateway_heartbeat_ok = false;
                        n.main_error = Some("main_gateway_heartbeat_ack_timeout".into());
                    }).await;
                    return Err(GatewayError::HeartbeatTimeout);
                }
                sink.send(Message::Text(identify::heartbeat(*seq).to_string()))
                    .await.map_err(|e| GatewayError::Network(e.to_string()))?;
                awaiting_ack = true;
                with_node(registry, label, |n| n.gateway_heartbeat_ok = false).await;
                heartbeat_sleep.as_mut().reset(
                    (Instant::now() + heartbeat_delay(heartbeat_ms, profile.heartbeat_jitter_percent, profile.jitter_stddev_percent)).into(),
                );
            }
            req = request_rx.recv() => {
                let Some(req) = req else { return Ok(SocketOutcome::Shutdown); };
                let frame = match req {
                    MainRequest::Join { guild_id, channel_id, mute, deaf } => {
                        identify::voice_state(&guild_id, Some(&channel_id), mute, deaf)
                    }
                };
                sink.send(Message::Text(frame.to_string())).await
                    .map_err(|e| GatewayError::Network(e.to_string()))?;
            }
            item = stream.next() => {
                let msg = match item {
                    Some(Ok(v)) => v,
                    Some(Err(e)) => {
                        warn!(node=%label, error=%e, "main gateway stream error");
                        return Err(GatewayError::Network(e.to_string()));
                    }
                    None => {
                        with_node(registry, label, |n| {
                            n.last_main_disconnect_reason = Some("stream_ended".into());
                            n.last_main_close_code = None;
                        })
                        .await;
                        return Ok(SocketOutcome::Reconnect);
                    }
                };

                match msg {
                    Message::Text(text) => {
                        let v: Value = serde_json::from_str(&text)
                            .map_err(|e| GatewayError::Protocol(e.to_string()))?;
                        match process_gateway_payload(
                            label,
                            &v,
                            registry,
                            events_tx,
                            self_user_id,
                            seq,
                            session_id,
                            resume_gateway_url,
                            &mut awaiting_ack,
                            &mut heartbeat_sleep,
                            heartbeat_ms,
                            profile,
                            &mut sink,
                            &mut resume_started_at,
                            resume_succeeded,
                        ).await {
                            Ok(GatewayPayloadOutcome::Continue) => {}
                            Ok(GatewayPayloadOutcome::Reconnect) => return Ok(SocketOutcome::Reconnect),
                            Ok(GatewayPayloadOutcome::InvalidSession) => return Ok(SocketOutcome::InvalidSession),
                            Err(e) => return Err(e),
                        }
                    }
                    Message::Binary(bin) => {
                        let v: Value = serde_json::from_slice(&bin)
                            .map_err(|e| GatewayError::Protocol(e.to_string()))?;
                        match process_gateway_payload(
                            label,
                            &v,
                            registry,
                            events_tx,
                            self_user_id,
                            seq,
                            session_id,
                            resume_gateway_url,
                            &mut awaiting_ack,
                            &mut heartbeat_sleep,
                            heartbeat_ms,
                            profile,
                            &mut sink,
                            &mut resume_started_at,
                            resume_succeeded,
                        ).await {
                            Ok(GatewayPayloadOutcome::Continue) => {}
                            Ok(GatewayPayloadOutcome::Reconnect) => return Ok(SocketOutcome::Reconnect),
                            Ok(GatewayPayloadOutcome::InvalidSession) => return Ok(SocketOutcome::InvalidSession),
                            Err(e) => return Err(e),
                        }
                    }
                    Message::Close(frame) => {
                        let (code, reason) = frame.map(|f| (u16::from(f.code), f.reason.to_string())).unwrap_or_default();
                        with_node(registry, label, |n| {
                            n.last_main_disconnect_reason = Some(format!("close:{code}:{reason}"));
                            n.last_main_close_code = Some(code);
                        })
                        .await;
                        if code == 4004 {
                            return Err(GatewayError::AuthFailed);
                        }
                        if matches!(code, 4007 | 4009) {
                            warn!(node=%label, code, %reason, "main gateway session cannot be resumed");
                            return Ok(SocketOutcome::InvalidSession);
                        }
                        info!(node=%label, code, %reason, "main gateway closed; preserving voice transport");
                        return Ok(SocketOutcome::Reconnect);
                    }
                    Message::Ping(p) => {
                        sink.send(Message::Pong(p)).await.map_err(|e| GatewayError::Network(e.to_string()))?;
                    }
                    _ => {}
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_gateway_payload(
    label: &str,
    v: &Value,
    registry: &SharedRegistry,
    events_tx: &mpsc::Sender<MainEvent>,
    self_user_id: &mut Option<String>,
    seq: &mut Option<u64>,
    session_id: &mut Option<String>,
    resume_gateway_url: &mut Option<String>,
    awaiting_ack: &mut bool,
    heartbeat_sleep: &mut Pin<Box<Sleep>>,
    heartbeat_ms: u64,
    profile: &ClientProfile,
    sink: &mut (impl Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    resume_started_at: &mut Option<Instant>,
    resume_succeeded: &mut bool,
) -> Result<GatewayPayloadOutcome, GatewayError> {
    if let Some(s) = v.get("s").and_then(Value::as_u64) {
        *seq = Some(s);
    }
    let op = v.get("op").and_then(Value::as_u64).unwrap_or(999);
    match op {
        0 => {
            let event_name = v.get("t").and_then(Value::as_str).unwrap_or("");
            if event_name == "READY" {
                if let Some(value) = v.pointer("/d/session_id").and_then(Value::as_str) {
                    *session_id = Some(value.to_string());
                }
                if let Some(value) = v.pointer("/d/resume_gateway_url").and_then(Value::as_str) {
                    *resume_gateway_url = Some(value.to_string());
                }
            } else if event_name == "RESUMED" {
                let latency_ms = resume_started_at
                    .take()
                    .map(|started| started.elapsed().as_millis().min(u64::MAX as u128) as u64);
                *resume_succeeded = true;
                with_node(registry, label, |n| {
                    n.gateway_ready = true;
                    n.gateway_heartbeat_ok = true;
                    n.main_resume_count += 1;
                    n.last_main_resume_latency_ms = latency_ms;
                    n.main_error = None; // clear main error on resume
                })
                .await;
                info!(node=%label, ?latency_ms, "main gateway resumed");
            }
            handle_dispatch(label, registry, events_tx, v, self_user_id).await;
        }
        1 => {
            sink.send(Message::Text(identify::heartbeat(*seq).to_string()))
                .await
                .map_err(|e| GatewayError::Network(e.to_string()))?;
            *awaiting_ack = true;
            with_node(registry, label, |n| n.gateway_heartbeat_ok = false).await;
            heartbeat_sleep.as_mut().reset(
                (Instant::now()
                    + heartbeat_delay(
                        heartbeat_ms,
                        profile.heartbeat_jitter_percent,
                        profile.jitter_stddev_percent,
                    ))
                .into(),
            );
        }
        7 => {
            with_node(registry, label, |n| {
                n.last_main_disconnect_reason = Some("server_reconnect_opcode_7".into());
                n.last_main_close_code = None;
            })
            .await;
            return Ok(GatewayPayloadOutcome::Reconnect);
        }
        9 => {
            let resumable = v.get("d").and_then(Value::as_bool).unwrap_or(false);
            with_node(registry, label, |n| {
                n.last_main_disconnect_reason = Some(if resumable {
                    "invalid_session_resumable".into()
                } else {
                    "invalid_session_nonresumable".into()
                });
                n.last_main_close_code = None;
            })
            .await;
            return Ok(if resumable {
                GatewayPayloadOutcome::Reconnect
            } else {
                GatewayPayloadOutcome::InvalidSession
            });
        }
        11 => {
            *awaiting_ack = false;
            with_node(registry, label, |n| {
                n.gateway_heartbeat_ok = true;
                n.last_gateway_ack_at = Some(now_epoch());
            })
            .await;
        }
        _ => debug!(node=%label, op, "gateway opcode"),
    }
    Ok(GatewayPayloadOutcome::Continue)
}

async fn handle_dispatch(
    label: &str,
    registry: &SharedRegistry,
    tx: &mpsc::Sender<MainEvent>,
    v: &Value,
    self_user_id: &mut Option<String>,
) {
    match v.get("t").and_then(Value::as_str).unwrap_or("") {
        "READY" => {
            let user_id = v
                .pointer("/d/user/id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if user_id.is_empty() {
                return;
            }
            *self_user_id = Some(user_id.clone());
            with_node(registry, label, |n| {
                n.gateway_ready = true;
                n.gateway_heartbeat_ok = true;
                n.user_id = Some(user_id.clone());
                n.main_error = None;
            })
            .await;
            let _ = tx.send(MainEvent::Ready { user_id }).await;
        }
        "VOICE_STATE_UPDATE" => {
            let event_user_id = v
                .pointer("/d/user_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            if self_user_id.as_deref() != Some(event_user_id) {
                return;
            }
            let guild_id = v
                .pointer("/d/guild_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let channel_id = v
                .pointer("/d/channel_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let voice_session_id = v
                .pointer("/d/session_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            with_node(registry, label, |n| {
                n.current_channel_id = channel_id.clone();
                n.last_voice_state_at = Some(now_epoch());
            })
            .await;
            let _ = tx
                .send(MainEvent::VoiceState {
                    user_id: event_user_id.to_string(),
                    guild_id,
                    channel_id,
                    session_id: voice_session_id,
                })
                .await;
        }
        "VOICE_SERVER_UPDATE" => {
            let guild_id = v
                .pointer("/d/guild_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let endpoint = v
                .pointer("/d/endpoint")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let token = v
                .pointer("/d/token")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let _ = tx
                .send(MainEvent::VoiceServer {
                    guild_id,
                    endpoint,
                    token,
                })
                .await;
        }
        _ => {}
    }
}

async fn next_json<S>(stream: &mut S) -> anyhow::Result<Value>
where
    S: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => return Ok(serde_json::from_str(&t)?),
            Some(Ok(Message::Binary(b))) => return Ok(serde_json::from_slice(&b)?),
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Ok(Message::Close(frame))) => {
                anyhow::bail!("gateway closed before HELLO: {frame:?}")
            }
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(e.into()),
            None => anyhow::bail!("gateway stream ended before HELLO"),
        }
    }
}

fn gateway_url(resume_gateway_url: Option<&str>) -> String {
    match resume_gateway_url {
        Some(base) if !base.trim().is_empty() => {
            format!("{}/?v=10&encoding=json", base.trim_end_matches('/'))
        }
        _ => GATEWAY_URL.to_string(),
    }
}

fn next_reconnect_backoff(current: Duration) -> Duration {
    Duration::from_secs_f64(
        (current.as_secs_f64() * 1.7)
            .max(1.0)
            .min(GATEWAY_RECONNECT_MAX.as_secs_f64()),
    )
}

pub(crate) fn heartbeat_delay(base_ms: u64, percent: u32, stddev_percent: u32) -> Duration {
    let mean = base_ms as f64;
    let stddev = base_ms as f64 * stddev_percent as f64 / 100.0;
    if stddev <= 0.0 {
        return Duration::from_millis(base_ms.max(1));
    }
    let normal = Normal::new(mean, stddev)
        .unwrap_or_else(|_| Normal::new(mean, 1.0).expect("valid fallback"));
    let mut rng = rand::thread_rng();
    let val = normal.sample(&mut rng);
    let val = val.clamp(
        mean - mean * percent as f64 / 100.0,
        mean + mean * percent as f64 / 100.0,
    );
    Duration::from_millis(val.max(1.0) as u64)
}

async fn wait_or_shutdown(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        changed = shutdown.changed() => changed.is_ok() && *shutdown.borrow(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heartbeat_delay_no_double_base() {
        let base = 40000u64;
        let percent = 10;
        let stddev = 3;
        let delay = heartbeat_delay(base, percent, stddev);
        let ms = delay.as_millis() as u64;
        assert!(ms >= base - base * percent as u64 / 100);
        assert!(ms <= base + base * percent as u64 / 100);
        assert!(ms < 2 * base);
    }

    #[test]
    fn test_heartbeat_delay_zero_stddev_fallback() {
        let base = 40000u64;
        let delay = heartbeat_delay(base, 10, 0);
        assert_eq!(delay.as_millis() as u64, base);
    }
}
