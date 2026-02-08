use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftEvent {
    pub timestamp_ms: u128,
    pub node_id: u64,
    pub kind: String,
    pub message: String,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_len: u64,
}

static EVENT_TX: OnceLock<UnboundedSender<RaftEvent>> = OnceLock::new();

pub fn install_event_sender(tx: UnboundedSender<RaftEvent>) {
    let _ = EVENT_TX.set(tx);
}

pub fn emit_event(event: RaftEvent) {
    if let Some(tx) = EVENT_TX.get() {
        let _ = tx.send(event);
    }
}

pub fn new_event(
    node_id: u64,
    kind: &str,
    message: String,
    term: u64,
    commit_index: u64,
    last_applied: u64,
    log_len: u64,
) -> RaftEvent {
    RaftEvent {
        timestamp_ms: now_ms(),
        node_id,
        kind: kind.to_string(),
        message,
        term,
        commit_index,
        last_applied,
        log_len,
    }
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
