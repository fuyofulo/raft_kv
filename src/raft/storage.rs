use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::raft::state::{CachedClientReply, LogIndex, PersistentState, RaftNode};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DurableNodeState {
    pub persistent: PersistentState,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub state_machine: HashMap<String, String>,
    pub dedup_table: HashMap<u64, CachedClientReply>,
}

pub fn load_durable_state(path: &Path) -> io::Result<Option<DurableNodeState>> {
    if !path.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(path)?;
    let state: DurableNodeState = serde_json::from_str(&raw).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to parse durable state: {}", e),
        )
    })?;
    Ok(Some(state))
}

pub fn save_node_state(path: &Path, node: &RaftNode) -> io::Result<()> {
    let state = DurableNodeState {
        persistent: node.persistent.clone(),
        commit_index: node.volatile.commit_index,
        last_applied: node.volatile.last_applied,
        state_machine: node.state_machine.clone(),
        dedup_table: node.dedup_table.clone(),
    };
    save_durable_state(path, &state)
}

pub fn save_durable_state(path: &Path, state: &DurableNodeState) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let tmp = path.with_extension("tmp");
    let raw = serde_json::to_string_pretty(state).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to serialize durable state: {}", e),
        )
    })?;
    fs::write(&tmp, raw)?;
    fs::rename(&tmp, path)?;
    Ok(())
}
