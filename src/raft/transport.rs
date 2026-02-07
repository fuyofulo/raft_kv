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

pub trait RaftTransport {
    fn send_request_vote(&self, target: NodeId, req: RequestVote) -> Result<RequestVoteResponse, TransportError>;
    
    fn send_append_entries(&self, target: NodeId, req: AppendEntries) -> Result<AppendEntriesResponse, TransportError>;
}