use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::raft::state::{
    AppendEntries,
    AppendEntriesResponse,
    NodeId,
    RaftNode,
    RequestVote,
    RequestVoteResponse
};
use crate::raft::transport::{
    RaftTransport,
    TransportError
};

#[derive(Clone, Default)]
pub struct InMemoryTransport {
    pub nodes: Arc<Mutex<HashMap<NodeId, Arc<Mutex<RaftNode>>>>>, 
}

impl InMemoryTransport {
    pub fn register_nodes(&self, node: RaftNode) {
        let mut guard = self.nodes.lock().expect("transport lock poisoned");
        guard.insert(node.id, Arc::new(Mutex::new(node)));
    }
}

impl RaftTransport for InMemoryTransport {
    fn send_request_vote(&self, target: NodeId, req: RequestVote) -> Result<RequestVoteResponse, TransportError> {
        let node_arc = {
            let guard = self.nodes.lock().expect("transport lock poisoned");
            guard.get(&target).cloned()
        }
        .ok_or_else(|| TransportError {
            message: format!("target node {} not found", target),
        })?;
        
        let mut node = node_arc.lock().expect("node lock poisoned");
        Ok(node.on_request_vote(req))
    }
    
    fn send_append_entries(&self, target: NodeId, req: AppendEntries) -> Result<AppendEntriesResponse, TransportError> {
        let node_arc = {
            let guard = self.nodes.lock().expect("transport lock poisoned");
            guard.get(&target).cloned()
        }
        .ok_or_else(|| TransportError {
            message: format!("target node {} not found", target),
        })?;
        
        let mut node = node_arc.lock().expect("node lock poisoned");
        Ok(node.on_append_entries(req))
    }
}