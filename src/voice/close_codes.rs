#[derive(Debug)]
pub enum CloseAction {
    Resume,
    FullReconnect,
    Fatal(&'static str),
}

pub fn classify(code: u16) -> CloseAction {
    match code {
        1000 | 1001 => CloseAction::Resume,
        4001 => CloseAction::Fatal("unknown opcode"),
        4002 => CloseAction::Fatal("failed to decode payload"),
        4003 => CloseAction::FullReconnect,
        4004 => CloseAction::Fatal("voice authentication failed"),
        4005 | 4006 | 4009 | 4011 => CloseAction::FullReconnect,
        4012 => CloseAction::Fatal("unknown protocol"),
        4014 => CloseAction::FullReconnect,
        4015 => CloseAction::Resume,
        4016 | 4022 => CloseAction::FullReconnect,
        _ => CloseAction::FullReconnect,
    }
}
