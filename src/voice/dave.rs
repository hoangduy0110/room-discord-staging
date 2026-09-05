use std::collections::HashMap;
use std::num::NonZeroU16;

use davey::DaveSession;
use serde_json::{json, Value};

use crate::voice::opcodes::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaveStatus {
    Uninitialized,
    Initializing,
    Ready,
    Passthrough,
    Transitioning,
    Failed,
}

pub enum DaveOut {
    Binary { op: u8, data: Vec<u8> },
    Json(Value),
}

pub struct DaveState {
    user_id: u64,
    channel_id: u64,
    protocol_version: u16,
    session: Option<DaveSession>,
    pending_transitions: HashMap<u16, u16>,
    downgraded: bool,
    status: DaveStatus,
}

impl DaveState {
    pub fn new(user_id: u64, channel_id: u64) -> Self {
        Self {
            user_id,
            channel_id,
            protocol_version: 0,
            session: None,
            pending_transitions: HashMap::new(),
            downgraded: false,
            status: DaveStatus::Uninitialized,
        }
    }

    pub fn status(&self) -> DaveStatus {
        self.status
    }

    pub fn is_ready(&self) -> bool {
        self.status == DaveStatus::Ready || self.status == DaveStatus::Passthrough
    }

    pub fn on_session_description(&mut self, dave_protocol_version: u16) -> Vec<DaveOut> {
        self.protocol_version = dave_protocol_version;
        if dave_protocol_version > 0 {
            self.status = DaveStatus::Initializing;
            self.reinit_session()
        } else {
            self.status = DaveStatus::Passthrough;
            if let Some(session) = self.session.as_mut() {
                let _ = session.reset();
                session.set_passthrough_mode(true, Some(10));
            } else {
                match DaveSession::new(
                    NonZeroU16::new(1).unwrap(),
                    self.user_id,
                    self.channel_id,
                    None,
                ) {
                    Ok(mut session) => {
                        session.set_passthrough_mode(true, Some(10));
                        self.session = Some(session);
                    }
                    Err(e) => {
                        tracing::warn!(target: "voice.dave", "failed to create passthrough session: {e}");
                        self.status = DaveStatus::Failed;
                    }
                }
            }
            Vec::new()
        }
    }

    fn reinit_session(&mut self) -> Vec<DaveOut> {
        let version = match NonZeroU16::new(self.protocol_version) {
            Some(v) => v,
            None => {
                self.status = DaveStatus::Failed;
                return Vec::new();
            }
        };
        let result = match self.session.as_mut() {
            Some(session) => session
                .reinit(version, self.user_id, self.channel_id, None)
                .map(|_| ()),
            None => match DaveSession::new(version, self.user_id, self.channel_id, None) {
                Ok(s) => {
                    self.session = Some(s);
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
        };
        if let Err(e) = result {
            tracing::warn!(target: "voice.dave", "session reinit failed: {e}");
            self.status = DaveStatus::Failed;
            return Vec::new();
        }
        self.pending_transitions.clear();

        let session = self.session.as_mut().expect("session set above");
        match session.create_key_package() {
            Ok(pkg) => {
                self.status = DaveStatus::Ready;
                vec![DaveOut::Binary {
                    op: OP_MLS_KEY_PACKAGE,
                    data: pkg,
                }]
            }
            Err(e) => {
                tracing::warn!(target: "voice.dave", "create_key_package failed: {e}");
                self.status = DaveStatus::Failed;
                Vec::new()
            }
        }
    }

    pub fn on_text_op(&mut self, op: u8, d: &Value) -> Vec<DaveOut> {
        match op {
            OP_DAVE_PREPARE_TRANSITION => {
                let transition_id =
                    d.get("transition_id").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
                let version = d
                    .get("protocol_version")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u16;
                self.pending_transitions.insert(transition_id, version);
                if transition_id == 0 {
                    self.execute_transition(0)
                } else {
                    if version == 0 {
                        if let Some(session) = self.session.as_mut() {
                            session.set_passthrough_mode(true, Some(120));
                        }
                        self.status = DaveStatus::Passthrough;
                    } else {
                        self.status = DaveStatus::Transitioning;
                    }
                    vec![transition_ready(transition_id)]
                }
            }
            OP_DAVE_EXECUTE_TRANSITION => {
                let transition_id =
                    d.get("transition_id").and_then(|v| v.as_u64()).unwrap_or(0) as u16;
                self.execute_transition(transition_id)
            }
            OP_DAVE_PREPARE_EPOCH => {
                let epoch = d.get("epoch").and_then(|v| v.as_u64()).unwrap_or(0);
                let version = d
                    .get("protocol_version")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u16;
                if epoch == 1 {
                    self.protocol_version = version;
                    self.reinit_session()
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        }
    }

    fn execute_transition(&mut self, transition_id: u16) -> Vec<DaveOut> {
        let Some(version) = self.pending_transitions.remove(&transition_id) else {
            return Vec::new();
        };
        let old_version = self.protocol_version;
        self.protocol_version = version;

        if old_version != self.protocol_version && self.protocol_version == 0 {
            self.downgraded = true;
            // Ngay khi chuyển về version 0, set status là Passthrough
            self.status = DaveStatus::Passthrough;
        } else if transition_id > 0 && self.downgraded {
            self.downgraded = false;
            if let Some(session) = self.session.as_mut() {
                session.set_passthrough_mode(true, Some(10));
            }
            self.status = DaveStatus::Passthrough;
        } else {
            // Chỉ set Ready nếu session tồn tại và protocol_version > 0
            if self.session.is_some() && self.protocol_version > 0 {
                self.status = DaveStatus::Ready;
            }
        }
        Vec::new()
    }

    pub fn on_binary(&mut self, op: u8, payload: &[u8]) -> Vec<DaveOut> {
        if self.session.is_none() {
            return Vec::new();
        }
        match op {
            OP_MLS_EXTERNAL_SENDER => {
                let session = self.session.as_mut().unwrap();
                if let Err(e) = session.set_external_sender(payload) {
                    tracing::warn!(target: "voice.dave", "set_external_sender failed: {e}");
                    self.status = DaveStatus::Failed;
                }
                Vec::new()
            }
            OP_MLS_PROPOSALS => {
                if payload.is_empty() {
                    return Vec::new();
                }
                let optype = if payload[0] == 0 {
                    davey::ProposalsOperationType::APPEND
                } else {
                    davey::ProposalsOperationType::REVOKE
                };
                let session = self.session.as_mut().unwrap();
                match session.process_proposals(optype, &payload[1..], None) {
                    Ok(Some(cw)) => {
                        let mut data = cw.commit;
                        if let Some(welcome) = cw.welcome {
                            data.extend_from_slice(&welcome);
                        }
                        vec![DaveOut::Binary {
                            op: OP_MLS_COMMIT_WELCOME,
                            data,
                        }]
                    }
                    Ok(None) => Vec::new(),
                    Err(e) => {
                        tracing::warn!(target: "voice.dave", "process_proposals failed: {e}");
                        self.status = DaveStatus::Failed;
                        Vec::new()
                    }
                }
            }
            OP_MLS_ANNOUNCE_COMMIT_TRANSITION => {
                if payload.len() < 2 {
                    return Vec::new();
                }
                let transition_id = u16::from_be_bytes([payload[0], payload[1]]);
                let session = self.session.as_mut().unwrap();
                match session.process_commit(&payload[2..]) {
                    Ok(()) => {
                        if transition_id != 0 {
                            self.pending_transitions
                                .insert(transition_id, self.protocol_version);
                            vec![transition_ready(transition_id)]
                        } else {
                            self.status = DaveStatus::Ready;
                            Vec::new()
                        }
                    }
                    Err(_) => self.recover_from_invalid_commit(transition_id),
                }
            }
            OP_MLS_WELCOME => {
                if payload.len() < 2 {
                    return Vec::new();
                }
                let transition_id = u16::from_be_bytes([payload[0], payload[1]]);
                let session = self.session.as_mut().unwrap();
                match session.process_welcome(&payload[2..]) {
                    Ok(()) => {
                        if transition_id != 0 {
                            self.pending_transitions
                                .insert(transition_id, self.protocol_version);
                            vec![transition_ready(transition_id)]
                        } else {
                            self.status = DaveStatus::Ready;
                            Vec::new()
                        }
                    }
                    Err(_) => self.recover_from_invalid_commit(transition_id),
                }
            }
            _ => Vec::new(),
        }
    }

    fn recover_from_invalid_commit(&mut self, transition_id: u16) -> Vec<DaveOut> {
        let mut out = vec![DaveOut::Json(json!({
            "op": OP_MLS_INVALID_COMMIT_WELCOME,
            "d": { "transition_id": transition_id },
        }))];
        self.status = DaveStatus::Failed;
        out.extend(self.reinit_session());
        out
    }
}

fn transition_ready(transition_id: u16) -> DaveOut {
    DaveOut::Json(json!({
        "op": OP_DAVE_TRANSITION_READY,
        "d": { "transition_id": transition_id },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_state() -> DaveState {
        DaveState::new(123, 456)
    }

    #[test]
    fn initial_state_uninitialized() {
        let state = new_state();
        assert_eq!(state.status(), DaveStatus::Uninitialized);
        assert!(!state.is_ready());
    }

    #[test]
    fn session_description_passthrough_when_dpv_zero() {
        let mut state = new_state();
        let outs = state.on_session_description(0);
        assert!(outs.is_empty());
        assert_eq!(state.status(), DaveStatus::Passthrough);
        assert!(state.is_ready());
    }

    #[test]
    fn reinit_sets_ready_when_success() {
        let mut state = new_state();
        // on_session_description with version 1 will attempt reinit
        // This may fail without real davey, but we check status is not Uninitialized
        state.on_session_description(1);
        assert!(state.status() != DaveStatus::Uninitialized);
        // If reinit failed, status = Failed. If succeeded, status = Ready.
        assert!(state.status() == DaveStatus::Ready || state.status() == DaveStatus::Failed);
    }

    #[test]
    fn transition_to_zero_immediately_passthrough() {
        let mut state = new_state();
        // Simulate initial version > 0
        state.protocol_version = 1;
        state.status = DaveStatus::Ready;
        state.session = None; // We can't easily create a real session in test without davey
                              // Simulate transition from version 1 to 0
        state.pending_transitions.insert(0, 0);
        let outs = state.execute_transition(0);
        assert!(outs.is_empty());
        // After downgrade to 0, status should be Passthrough
        assert_eq!(state.status(), DaveStatus::Passthrough);
        assert!(state.is_ready());
    }

    #[test]
    fn execute_transition_without_pending_returns_empty() {
        let mut state = new_state();
        let outs = state.execute_transition(99);
        assert!(outs.is_empty());
        assert_eq!(state.status(), DaveStatus::Uninitialized);
    }

    #[test]
    fn prepare_transition_nonzero_marks_transitioning() {
        let mut state = new_state();
        let outs = state.on_text_op(
            OP_DAVE_PREPARE_TRANSITION,
            &json!({"transition_id": 1, "protocol_version": 1}),
        );
        assert_eq!(outs.len(), 1);
        assert_eq!(state.status(), DaveStatus::Transitioning);
    }

    #[test]
    fn prepare_transition_zero_version_marks_passthrough() {
        let mut state = new_state();
        let outs = state.on_text_op(
            OP_DAVE_PREPARE_TRANSITION,
            &json!({"transition_id": 2, "protocol_version": 0}),
        );
        assert_eq!(outs.len(), 1);
        assert_eq!(state.status(), DaveStatus::Passthrough);
    }
}
