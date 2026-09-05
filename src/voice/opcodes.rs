#![allow(dead_code)]
//! Discord voice gateway (v8) opcodes. Same set voice-camper uses.

pub const OP_IDENTIFY: u8 = 0;
pub const OP_SELECT_PROTOCOL: u8 = 1;
pub const OP_READY: u8 = 2;
pub const OP_HEARTBEAT: u8 = 3;
pub const OP_SESSION_DESCRIPTION: u8 = 4;
pub const OP_SPEAKING: u8 = 5;
pub const OP_HEARTBEAT_ACK: u8 = 6;
pub const OP_RESUME: u8 = 7;
pub const OP_HELLO: u8 = 8;
pub const OP_RESUMED: u8 = 9;
pub const OP_CLIENT_DISCONNECT: u8 = 13;

pub const OP_DAVE_PREPARE_TRANSITION: u8 = 21;
pub const OP_DAVE_EXECUTE_TRANSITION: u8 = 22;
pub const OP_DAVE_TRANSITION_READY: u8 = 23;
pub const OP_DAVE_PREPARE_EPOCH: u8 = 24;
pub const OP_MLS_EXTERNAL_SENDER: u8 = 25;
pub const OP_MLS_KEY_PACKAGE: u8 = 26;
pub const OP_MLS_PROPOSALS: u8 = 27;
pub const OP_MLS_COMMIT_WELCOME: u8 = 28;
pub const OP_MLS_ANNOUNCE_COMMIT_TRANSITION: u8 = 29;
pub const OP_MLS_WELCOME: u8 = 30;
pub const OP_MLS_INVALID_COMMIT_WELCOME: u8 = 31;
