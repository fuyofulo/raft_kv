use std::fmt;
use crate::raft::state::{
    AppendEntries,
    AppendEntriesResponse,
    NodeId,
    RequestVote,
    RequestVoteResponse
};

#[derive(Debug, Clone)]
pub struct TransportError {
    pub message: String
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for TransportError {}

pub trait RaftTransport {
    fn send_request_vote(&self, target: NodeId, req: RequestVote) -> Result<RequestVoteResponse, TransportError>;
    
    fn send_append_entries(&self, target: NodeId, req: AppendEntries) -> Result<AppendEntriesResponse, TransportError>;
}