use crate::{
    config::{AccountConfig, Config},
    gateway::main::{self as gateway, GatewayError, MainEvent, MainRequest},
    state::{now_epoch, with_node, SharedRegistry},
    voice::ws::{VoiceConnection, VoiceOutcome, VoiceOwner, VoiceParams},
};
use futures_util::FutureExt;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tracing::{info, warn};

const STABLE_GENERATION_AFTER: Duration = Duration::from_secs(120);
const INITIAL_SUPERVISOR_BACKOFF: Duration = Duration::from_secs(2);
const INITIAL_JOIN_BACKOFF: Duration = Duration::from_secs(2);
const MAX_JOIN_BACKOFF: Duration = Duration::from_secs(30);
const JOIN_CONFIRM_TIMEOUT: Duration = Duration::from_secs(12);
const VOICE_MATERIAL_TIMEOUT: Duration = Duration::from_secs(12);
const FRESH_READY_REASSERT_DELAY: Duration = Duration::from_secs(2);
const OUT_OF_ROOM_REJOIN_DELAY: Duration = Duration::from_secs(1);
const INITIAL_VOICE_BACKOFF: Duration = Duration::from_millis(500);
const MAX_VOICE_BACKOFF: Duration = Duration::from_secs(8);
const STABLE_VOICE_AFTER: Duration = Duration::from_secs(60);
const MAX_QUICK_VOICE_FAILURES: u32 = 5;

#[derive(Debug)]
enum VoiceWorkerExit {
    Shutdown,
    NeedFreshSession { reason: String },
    Fatal(String),
}

enum GenerationOutcome {
    Ok,
    Recoverable(String),
    Fatal(String),
}

pub async fn run_account(
    account: AccountConfig,
    config: Arc<Config>,
    registry: SharedRegistry,
    mut shutdown: watch::Receiver<bool>,
) {
    let label = account.label.clone();
    with_node(&registry, &label, |n| n.supervisor_alive = true).await;
    let mut backoff = INITIAL_SUPERVISOR_BACKOFF;

    loop {
        if *shutdown.borrow() {
            break;
        }

        with_node(&registry, &label, |n| {
            n.generation += 1;
            n.gateway_ready = false;
            n.gateway_heartbeat_ok = false;
            n.in_target_channel = false;
            n.current_channel_id = None;
            n.voice_connected = false;
            n.voice_heartbeat_ok = false;
            n.dave_ready = false;
            n.main_error = None;
            n.voice_error = None;
            n.dave_error = None;
            n.supervisor_error = None;
            n.voice_worker_id = 0;
        })
        .await;

        let generation_started = Instant::now();
        let outcome =
            run_generation(&account, config.clone(), registry.clone(), shutdown.clone()).await;
        let generation_uptime = generation_started.elapsed();

        if *shutdown.borrow() {
            break;
        }

        match outcome {
            GenerationOutcome::Ok => break,
            GenerationOutcome::Fatal(reason) => {
                with_node(&registry, &label, |n| {
                    n.gateway_ready = false;
                    n.gateway_heartbeat_ok = false;
                    n.voice_connected = false;
                    n.voice_heartbeat_ok = false;
                    n.dave_ready = false;
                    n.supervisor_alive = false;
                    n.supervisor_error = Some(format!("fatal: {reason}"));
                })
                .await;
                warn!(node=%label, %reason, "account supervisor stopped due to fatal");
                break;
            }
            GenerationOutcome::Recoverable(error) => {
                if generation_uptime >= STABLE_GENERATION_AFTER {
                    backoff = INITIAL_SUPERVISOR_BACKOFF;
                }

                with_node(&registry, &label, |n| {
                    n.gateway_ready = false;
                    n.gateway_heartbeat_ok = false;
                    n.in_target_channel = false;
                    n.current_channel_id = None;
                    n.voice_connected = false;
                    n.voice_heartbeat_ok = false;
                    n.dave_ready = false;
                    if !error.is_empty() {
                        n.supervisor_error = Some(error.clone());
                    }
                })
                .await;

                let sleep = jitter(backoff, Duration::from_secs(3));
                warn!(node=%label, ?sleep, uptime=?generation_uptime, error=%error, "generation ended; restarting");
                if wait_or_shutdown(&mut shutdown, sleep).await {
                    break;
                }
                backoff = Duration::from_secs_f64(
                    (backoff.as_secs_f64() * 1.7).min(config.supervisor_backoff_max.as_secs_f64()),
                );
            }
        }
    }

    with_node(&registry, &label, |n| n.supervisor_alive = false).await;
}

fn classify_gateway_error(e: &anyhow::Error) -> GenerationOutcome {
    if let Some(GatewayError::AuthFailed) = e.downcast_ref::<GatewayError>() {
        GenerationOutcome::Fatal("main gateway auth failed".to_string())
    } else {
        GenerationOutcome::Recoverable(format!("main gateway error: {e}"))
    }
}

fn classify_voice_exit(exit: VoiceWorkerExit) -> GenerationOutcome {
    match exit {
        VoiceWorkerExit::Fatal(reason) => GenerationOutcome::Fatal(reason),
        VoiceWorkerExit::NeedFreshSession { reason } => GenerationOutcome::Recoverable(reason),
        VoiceWorkerExit::Shutdown => GenerationOutcome::Ok,
    }
}

async fn run_generation(
    account: &AccountConfig,
    config: Arc<Config>,
    registry: SharedRegistry,
    mut shutdown: watch::Receiver<bool>,
) -> GenerationOutcome {
    let label = account.label.clone();
    let current_generation = match registry
        .read()
        .await
        .nodes
        .get(&label)
        .map(|n| n.generation)
    {
        Some(g) => g,
        None => return GenerationOutcome::Fatal("missing node state".into()),
    };
    let (events_tx, mut events_rx) = mpsc::channel::<MainEvent>(32);
    let (request_tx, request_rx) = mpsc::channel::<MainRequest>(8);
    let mut gateway_task = tokio::spawn(gateway::run(
        label.clone(),
        account.token.clone(),
        config.client_profile.clone(),
        registry.clone(),
        events_tx,
        request_rx,
        shutdown.clone(),
    ));

    let mut user_id = match wait_ready(&mut events_rx, &mut gateway_task).await {
        Ok(id) => id,
        Err(e) => {
            gateway_task.abort();
            // AuthFailed trước READY cũng thành Fatal
            return classify_gateway_error(&e);
        }
    };
    let mut in_target = false;
    let mut join_backoff = INITIAL_JOIN_BACKOFF;
    let mut join_due = Some(Instant::now());
    let mut confirmation_deadline: Option<Instant> = None;
    let mut material_deadline: Option<Instant> = None;

    let mut voice_session_id: Option<String> = None;
    let mut voice_endpoint: Option<String> = None;
    let mut voice_token: Option<String> = None;
    let mut voice_task: Option<JoinHandle<()>> = None;
    let mut voice_worker_id = 0u64;
    let mut active_voice_session_id: Option<String> = None;
    let mut active_voice_endpoint: Option<String> = None;
    let mut seeking_fresh_voice_material = true;
    let (voice_exit_tx, mut voice_exit_rx) = mpsc::channel::<(u64, VoiceWorkerExit)>(8);

    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    invalidate_and_abort_voice_task(&registry, &label, &mut voice_worker_id, &mut voice_task).await;
                    gateway_task.abort();
                    return GenerationOutcome::Ok;
                }
            }
            gw = &mut gateway_task => {
                invalidate_and_abort_voice_task(&registry, &label, &mut voice_worker_id, &mut voice_task).await;
                match gw {
                    Ok(Ok(())) => return GenerationOutcome::Recoverable("main gateway exited normally".into()),
                    Ok(Err(e)) => return classify_gateway_error(&e),
                    Err(e) => return GenerationOutcome::Recoverable(format!("gateway task panic: {e}")),
                }
            }
            exit = voice_exit_rx.recv() => {
                if let Some((worker_id, exit)) = exit {
                    if worker_id != voice_worker_id {
                        continue;
                    }
                    voice_task = None;
                    active_voice_session_id = None;
                    active_voice_endpoint = None;
                    with_node(&registry, &label, |n| {
                        n.voice_connected = false;
                        n.voice_heartbeat_ok = false;
                        n.dave_ready = false;
                        n.voice_worker_id = 0;
                    }).await;

                    match exit {
                        VoiceWorkerExit::Shutdown => {
                            if *shutdown.borrow() {
                                gateway_task.abort();
                                return GenerationOutcome::Ok;
                            }
                            seeking_fresh_voice_material = true;
                            voice_session_id = None;
                            voice_endpoint = None;
                            voice_token = None;
                            join_due = Some(Instant::now() + OUT_OF_ROOM_REJOIN_DELAY);
                        }
                        VoiceWorkerExit::NeedFreshSession { reason } => {
                            with_node(&registry, &label, |n| {
                                n.voice_full_reconnects += 1;
                                n.voice_error = Some(reason.clone());
                            }).await;
                            seeking_fresh_voice_material = true;
                            voice_session_id = None;
                            voice_endpoint = None;
                            voice_token = None;
                            confirmation_deadline = None;
                            material_deadline = None;
                            join_due = Some(Instant::now() + OUT_OF_ROOM_REJOIN_DELAY);
                            warn!(node=%label, %reason, "minimal voice needs fresh session; reasserting join without leave");
                        }
                        VoiceWorkerExit::Fatal(reason) => {
                            with_node(&registry, &label, |n| {
                                n.voice_error = Some(reason.clone());
                            }).await;
                            gateway_task.abort();
                            // Dùng helper để production path khớp test
                            return classify_voice_exit(VoiceWorkerExit::Fatal(reason));
                        }
                    }
                }
            }
            ev = events_rx.recv() => {
                match ev {
                    Some(MainEvent::Ready { user_id: ready_user_id }) => {
                        user_id = ready_user_id;
                        join_backoff = INITIAL_JOIN_BACKOFF;
                        confirmation_deadline = None;
                        join_due = Some(Instant::now() + FRESH_READY_REASSERT_DELAY);

                        if voice_task.is_none() {
                            in_target = false;
                            seeking_fresh_voice_material = true;
                            voice_session_id = None;
                            voice_endpoint = None;
                            voice_token = None;
                            material_deadline = None;
                            with_node(&registry, &label, |n| {
                                n.in_target_channel = false;
                                n.current_channel_id = None;
                                n.voice_error = Some("room_state_unknown_after_fresh_ready".into());
                            }).await;
                        } else {
                            info!(node=%label, "fresh gateway READY; preserving active voice transport and reasserting room state");
                        }
                    }
                    Some(MainEvent::VoiceState {
                        user_id: event_user_id,
                        guild_id,
                        channel_id,
                        session_id,
                    }) if event_user_id == user_id && guild_id == config.guild_id => {
                        let target = channel_id.as_deref() == Some(config.channel_id.as_str());
                        let was_in_target = in_target;
                        in_target = target;

                        with_node(&registry, &label, |n| {
                            n.current_channel_id = channel_id.clone();
                            n.in_target_channel = target;
                            n.last_voice_state_at = Some(now_epoch());
                            if target {
                                if !was_in_target { n.join_successes += 1; }
                                if n.voice_connected { n.voice_error = None; }
                            } else {
                                n.voice_error = Some("outside_target_channel".into());
                            }
                        }).await;

                        if target {
                            join_backoff = INITIAL_JOIN_BACKOFF;
                            join_due = None;
                            confirmation_deadline = None;

                            if !session_id.is_empty() {
                                let session_changed = active_voice_session_id
                                    .as_deref()
                                    .is_some_and(|active| active != session_id.as_str());
                                if session_changed {
                                    warn!(node=%label, "voice session id changed; replacing stale voice transport");
                                    invalidate_and_abort_voice_task(&registry, &label, &mut voice_worker_id, &mut voice_task).await;
                                    active_voice_session_id = None;
                                    active_voice_endpoint = None;
                                    voice_endpoint = None;
                                    voice_token = None;
                                    seeking_fresh_voice_material = true;
                                    material_deadline = Some(Instant::now() + VOICE_MATERIAL_TIMEOUT);
                                }
                                voice_session_id = Some(session_id);
                            }

                            if voice_task.is_none() && (voice_endpoint.is_none() || voice_token.is_none()) {
                                material_deadline = Some(Instant::now() + VOICE_MATERIAL_TIMEOUT);
                            }

                            if !was_in_target {
                                info!(node=%label, "target voice state confirmed");
                            }
                        } else {
                            invalidate_and_abort_voice_task(&registry, &label, &mut voice_worker_id, &mut voice_task).await;
                            active_voice_session_id = None;
                            active_voice_endpoint = None;
                            voice_session_id = None;
                            voice_endpoint = None;
                            voice_token = None;
                            seeking_fresh_voice_material = true;
                            confirmation_deadline = None;
                            material_deadline = None;
                            join_due = Some(Instant::now() + OUT_OF_ROOM_REJOIN_DELAY);
                            with_node(&registry, &label, |n| {
                                n.voice_connected = false;
                                n.voice_heartbeat_ok = false;
                                n.dave_ready = false;
                                n.voice_error = Some("outside_target_channel".into());
                            }).await;
                            warn!(node=%label, channel=?channel_id, "account left target voice state; scheduling rejoin");
                        }
                    }
                    Some(MainEvent::VoiceServer { guild_id, endpoint, token })
                        if guild_id == config.guild_id =>
                    {
                        if let Some(endpoint) = endpoint.filter(|v| !v.trim().is_empty()) {
                            let endpoint_changed = active_voice_endpoint
                                .as_deref()
                                .is_some_and(|active| active != endpoint.as_str());
                            voice_endpoint = Some(endpoint.clone());
                            if !token.is_empty() { voice_token = Some(token); }

                            if endpoint_changed && voice_task.is_some() {
                                warn!(node=%label, endpoint=%endpoint, "voice server changed; replacing voice transport without leaving room");
                                invalidate_and_abort_voice_task(&registry, &label, &mut voice_worker_id, &mut voice_task).await;
                                active_voice_session_id = None;
                                active_voice_endpoint = None;
                                seeking_fresh_voice_material = false;
                            }
                        } else if voice_task.is_none() {
                            voice_endpoint = None;
                            material_deadline = Some(Instant::now() + VOICE_MATERIAL_TIMEOUT);
                        }
                    }
                    Some(_) => {}
                    None => {
                        invalidate_and_abort_voice_task(&registry, &label, &mut voice_worker_id, &mut voice_task).await;
                        gateway_task.abort();
                        return GenerationOutcome::Recoverable("gateway event channel closed".into());
                    }
                }
            }
            _ = ticker.tick() => {
                let now = Instant::now();

                let voice_healthy = registry
                    .read()
                    .await
                    .nodes
                    .get(&label)
                    .map(|n| {
                        n.voice_connected
                            && n.voice_heartbeat_ok
                            && n.dave_ready
                            && n.dave_error.is_none()
                            && n.voice_error.is_none()
                    })
                    .unwrap_or(false);
                if voice_healthy {
                    join_backoff = INITIAL_JOIN_BACKOFF;
                    material_deadline = None;
                    seeking_fresh_voice_material = false;
                }

                if confirmation_deadline.is_some_and(|deadline| now >= deadline) {
                    confirmation_deadline = None;
                    if !in_target {
                        let delay = jitter(join_backoff, Duration::from_secs(2));
                        join_due = Some(now + delay);
                        join_backoff = next_join_backoff(join_backoff);
                        with_node(&registry, &label, |n| {
                            n.voice_error = Some("voice_state_confirmation_timeout".into());
                        }).await;
                        warn!(node=%label, ?delay, "voice state not confirmed; retrying without leave");
                    }
                }

                if material_deadline.is_some_and(|deadline| now >= deadline) {
                    material_deadline = None;
                    if voice_task.is_none() {
                        let delay = jitter(join_backoff, Duration::from_secs(2));
                        join_due = Some(now + delay);
                        join_backoff = next_join_backoff(join_backoff);
                        seeking_fresh_voice_material = true;
                        with_node(&registry, &label, |n| {
                            n.voice_error = Some("voice_material_timeout".into());
                        }).await;
                        warn!(node=%label, ?delay, "voice session material incomplete; reasserting join without leave");
                    }
                }

                if join_due.is_some_and(|deadline| now >= deadline) {
                    let gateway_ready = registry
                        .read()
                        .await
                        .nodes
                        .get(&label)
                        .map(|n| n.gateway_ready)
                        .unwrap_or(false);

                    if gateway_ready {
                        if voice_task.is_none() && seeking_fresh_voice_material {
                            voice_session_id = None;
                            voice_endpoint = None;
                            voice_token = None;
                        }

                        if let Err(e) = send_join(&request_tx, &config).await {
                            invalidate_and_abort_voice_task(&registry, &label, &mut voice_worker_id, &mut voice_task).await;
                            gateway_task.abort();
                            return GenerationOutcome::Recoverable(format!("send_join: {e}"));
                        }

                        with_node(&registry, &label, |n| {
                            n.join_attempts += 1;
                            n.last_join_attempt_at = Some(now_epoch());
                            if !n.in_target_channel {
                                n.voice_error = Some("awaiting_voice_state_confirmation".into());
                            }
                        }).await;
                        if !in_target {
                            confirmation_deadline = Some(now + JOIN_CONFIRM_TIMEOUT);
                        }
                        if voice_task.is_none() {
                            material_deadline = Some(now + VOICE_MATERIAL_TIMEOUT);
                        }
                        join_due = None;
                        info!(node=%label, known_in_target=in_target, active_voice=voice_task.is_some(), "voice-state join/reassert sent");
                    } else {
                        join_due = Some(now + Duration::from_secs(1));
                    }
                }
            }
        }

        if voice_task.is_none() && in_target {
            if let (Some(session_id), Some(endpoint), Some(token)) = (
                voice_session_id.clone(),
                voice_endpoint.clone(),
                voice_token.clone(),
            ) {
                if !session_id.is_empty() && !endpoint.is_empty() && !token.is_empty() {
                    voice_worker_id = voice_worker_id.wrapping_add(1);
                    let worker_id = voice_worker_id;
                    set_voice_owner(&registry, &label, worker_id).await;
                    active_voice_session_id = Some(session_id.clone());
                    active_voice_endpoint = Some(endpoint.clone());
                    seeking_fresh_voice_material = false;
                    material_deadline = None;

                    with_node(&registry, &label, |n| {
                        n.voice_connected = false;
                        n.voice_heartbeat_ok = false;
                        n.dave_ready = false;
                        n.voice_error = Some("voice_transport_connecting".into());
                    })
                    .await;

                    let voice_params = VoiceParams {
                        endpoint,
                        guild_id: config.guild_id.clone(),
                        channel_id: config.channel_id.clone(),
                        user_id: user_id.clone(),
                        session_id,
                        token,
                        heartbeat_jitter_percent: config.client_profile.heartbeat_jitter_percent,
                        jitter_stddev_percent: config.client_profile.jitter_stddev_percent,
                    };
                    let owner = VoiceOwner::new(current_generation, worker_id);
                    let voice = VoiceConnection::new(
                        voice_params,
                        owner,
                        label.clone(),
                        registry.clone(),
                        shutdown.clone(),
                    );
                    voice_task = Some(spawn_voice_worker(
                        worker_id,
                        voice,
                        label.clone(),
                        voice_exit_tx.clone(),
                    ));
                    info!(node=%label, worker_id, "minimal voice worker started");
                }
            }
        }
    }
}

fn spawn_voice_worker(
    worker_id: u64,
    voice: VoiceConnection,
    label: String,
    exit_tx: mpsc::Sender<(u64, VoiceWorkerExit)>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let exit = std::panic::AssertUnwindSafe(run_voice_worker(voice, &label))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| VoiceWorkerExit::NeedFreshSession {
                reason: "voice_worker_panic".into(),
            });
        let _ = exit_tx.send((worker_id, exit)).await;
    })
}

async fn run_voice_worker(mut voice: VoiceConnection, label: &str) -> VoiceWorkerExit {
    let mut backoff = INITIAL_VOICE_BACKOFF;
    let mut quick_failures = 0u32;

    loop {
        let started = Instant::now();
        let result = voice.run_once().await;
        let uptime = started.elapsed();

        if uptime >= STABLE_VOICE_AFTER {
            backoff = INITIAL_VOICE_BACKOFF;
            quick_failures = 0;
        }

        match result {
            Ok(VoiceOutcome::Shutdown) => return VoiceWorkerExit::Shutdown,
            Ok(VoiceOutcome::NeedFullReconnect) => {
                return VoiceWorkerExit::NeedFreshSession {
                    reason: "voice_session_requires_fresh_material".into(),
                };
            }
            Ok(VoiceOutcome::TryResume) => {
                quick_failures = quick_failures.saturating_add(1);
                if quick_failures >= MAX_QUICK_VOICE_FAILURES {
                    return VoiceWorkerExit::NeedFreshSession {
                        reason: "voice_resume_budget_exhausted".into(),
                    };
                }
                let delay = jitter(backoff, Duration::from_secs(1));
                warn!(node=%label, ?delay, quick_failures, "minimal voice retrying/resuming");
                tokio::time::sleep(delay).await;
                backoff = next_voice_backoff(backoff);
            }
            Ok(VoiceOutcome::Fatal(reason)) => {
                return VoiceWorkerExit::Fatal(reason);
            }
            Err(e) => {
                voice
                    .update_node(|n| {
                        n.voice_connected = false;
                        n.voice_heartbeat_ok = false;
                        n.dave_ready = false;
                        n.voice_error = Some(format!("voice_transport:{e}"));
                    })
                    .await;

                quick_failures = quick_failures.saturating_add(1);
                let endpoint_unavailable =
                    is_voice_endpoint_unavailable(&e.to_string()) && quick_failures >= 2;
                if endpoint_unavailable || quick_failures >= MAX_QUICK_VOICE_FAILURES {
                    return VoiceWorkerExit::NeedFreshSession {
                        reason: format!("voice_transport_refresh:{e}"),
                    };
                }

                let delay = jitter(backoff, Duration::from_secs(1));
                warn!(node=%label, ?delay, quick_failures, error=%e, "minimal voice transport error; retrying same session");
                tokio::time::sleep(delay).await;
                backoff = next_voice_backoff(backoff);
            }
        }
    }
}

async fn wait_ready(
    events: &mut mpsc::Receiver<MainEvent>,
    gateway_task: &mut tokio::task::JoinHandle<anyhow::Result<()>>,
) -> anyhow::Result<String> {
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => anyhow::bail!("timeout waiting for gateway READY"),
            gw = &mut *gateway_task => {
                match gw {
                    Ok(Ok(())) => anyhow::bail!("gateway exited before READY"),
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(e.into()),
                }
            }
            ev = events.recv() => match ev {
                Some(MainEvent::Ready { user_id }) => return Ok(user_id),
                Some(_) => continue,
                None => anyhow::bail!("gateway event channel closed"),
            }
        }
    }
}

async fn send_join(tx: &mpsc::Sender<MainRequest>, config: &Config) -> anyhow::Result<()> {
    tx.send(MainRequest::Join {
        guild_id: config.guild_id.clone(),
        channel_id: config.channel_id.clone(),
        mute: config.self_mute,
        deaf: config.self_deaf,
    })
    .await
    .map_err(|_| anyhow::anyhow!("main gateway request channel closed"))
}

async fn set_voice_owner(registry: &SharedRegistry, label: &str, worker_id: u64) {
    with_node(registry, label, |n| n.voice_worker_id = worker_id).await;
}

async fn invalidate_and_abort_voice_task(
    registry: &SharedRegistry,
    label: &str,
    voice_worker_id: &mut u64,
    voice_task: &mut Option<JoinHandle<()>>,
) {
    *voice_worker_id = voice_worker_id.wrapping_add(1);
    set_voice_owner(registry, label, *voice_worker_id).await;
    abort_voice_task(voice_task);
}

fn abort_voice_task(task: &mut Option<JoinHandle<()>>) {
    if let Some(task) = task.take() {
        task.abort();
    }
}

fn is_voice_endpoint_unavailable(error: &str) -> bool {
    error.contains("HTTP error: 522")
}

fn next_join_backoff(current: Duration) -> Duration {
    Duration::from_secs_f64(
        (current.as_secs_f64() * 1.7)
            .max(INITIAL_JOIN_BACKOFF.as_secs_f64())
            .min(MAX_JOIN_BACKOFF.as_secs_f64()),
    )
}

fn next_voice_backoff(current: Duration) -> Duration {
    Duration::from_secs_f64(
        (current.as_secs_f64() * 1.7)
            .max(INITIAL_VOICE_BACKOFF.as_secs_f64())
            .min(MAX_VOICE_BACKOFF.as_secs_f64()),
    )
}

fn jitter(base: Duration, max_extra: Duration) -> Duration {
    let extra = rand::thread_rng()
        .gen_range(0.0..=(base.as_secs_f64() * 0.15).min(max_extra.as_secs_f64()));
    Duration::from_secs_f64(base.as_secs_f64() + extra)
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
    use crate::state::Registry;
    use tokio::sync::RwLock;

    fn test_registry(generation: u64, worker_id: u64) -> (SharedRegistry, String) {
        let label = "test".to_string();
        let mut registry = Registry::new([label.clone()]);
        let node = registry.nodes.get_mut(&label).expect("test node exists");
        node.generation = generation;
        node.voice_worker_id = worker_id;
        (std::sync::Arc::new(RwLock::new(registry)), label)
    }

    #[tokio::test]
    async fn invalidation_advances_owner_before_task_handle_is_dropped() {
        let (registry, label) = test_registry(4, 7);
        let mut worker_id = 7;
        let mut task = Some(tokio::spawn(async {
            std::future::pending::<()>().await;
        }));

        invalidate_and_abort_voice_task(&registry, &label, &mut worker_id, &mut task).await;

        assert_eq!(worker_id, 8);
        assert_eq!(registry.read().await.nodes[&label].voice_worker_id, 8);
        assert!(task.is_none());
    }

    #[tokio::test]
    async fn invalidation_advances_owner_even_without_a_tracked_task() {
        let (registry, label) = test_registry(4, 7);
        let mut worker_id = 7;
        let mut task = None;

        invalidate_and_abort_voice_task(&registry, &label, &mut worker_id, &mut task).await;

        assert_eq!(worker_id, 8);
        assert_eq!(registry.read().await.nodes[&label].voice_worker_id, 8);
        assert!(task.is_none());
    }

    #[test]
    fn voice_http_522_is_treated_as_endpoint_unavailable() {
        assert!(is_voice_endpoint_unavailable(
            "HTTP error: 522 <unknown status code>"
        ));
        assert!(!is_voice_endpoint_unavailable(
            "IO error: peer closed connection without sending TLS close_notify"
        ));
    }

    #[test]
    fn classify_auth_failed_as_fatal() {
        let err = anyhow::anyhow!(GatewayError::AuthFailed);
        let outcome = classify_gateway_error(&err);
        assert!(matches!(outcome, GenerationOutcome::Fatal(_)));
    }

    #[test]
    fn classify_network_error_as_recoverable() {
        let err = anyhow::anyhow!(GatewayError::Network("test".to_string()));
        let outcome = classify_gateway_error(&err);
        assert!(matches!(outcome, GenerationOutcome::Recoverable(_)));
    }

    #[test]
    fn classify_voice_fatal_as_generation_fatal() {
        let exit = VoiceWorkerExit::Fatal("test".to_string());
        let outcome = classify_voice_exit(exit);
        assert!(matches!(outcome, GenerationOutcome::Fatal(_)));
    }

    #[test]
    fn classify_voice_fresh_session_as_recoverable() {
        let exit = VoiceWorkerExit::NeedFreshSession {
            reason: "test".to_string(),
        };
        let outcome = classify_voice_exit(exit);
        assert!(matches!(outcome, GenerationOutcome::Recoverable(_)));
    }
}
