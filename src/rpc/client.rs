use std::collections::HashMap;

use tokio::runtime::Runtime;
use tonic::transport::Channel;

use crate::raft::state::{
    AppendEntries, AppendEntriesResponse, NodeId, RequestVote, RequestVoteResponse,
};
use crate::raft::transport::{RaftTransport, TransportError};
use crate::rpc::convert::{to_proto_log_entry, to_proto_request_vote};
use crate::rpc::raft::raft_rpc_client::RaftRpcClient;
use crate::rpc::raft::AppendEntriesRequest;

pub struct GrpcTransport {
    pub peer_addrs: HashMap<NodeId, String>,
    pub rt: Runtime,
}

impl GrpcTransport {
    pub fn new(peer_addrs: HashMap<NodeId, String>) -> Result<Self, TransportError> {
        let rt = Runtime::new().map_err(|e| TransportError {
            message: format!("failed to create tokio runtime: {}", e),
        })?;
        Ok(Self { peer_addrs, rt })
    }

    fn addr_for(&self, target: NodeId) -> Result<String, TransportError> {
        self.peer_addrs
            .get(&target)
            .cloned()
            .ok_or_else(|| TransportError {
                message: format!("missing peer address for node {}", target),
            })
    }

    fn connect_client(&self, addr: String) -> Result<RaftRpcClient<Channel>, TransportError> {
        self.rt
            .block_on(RaftRpcClient::connect(addr))
            .map_err(|e| TransportError {
                message: format!("connect failed: {}", e),
            })
    }
}

impl RaftTransport for GrpcTransport {
    fn send_request_vote(
        &self,
        target: NodeId,
        req: RequestVote,
    ) -> Result<RequestVoteResponse, TransportError> {
        let addr = self.addr_for(target)?;
        let mut client = self.connect_client(addr)?;
        let proto_req = to_proto_request_vote(&req);

        let resp = self
            .rt
            .block_on(client.request_vote(proto_req))
            .map_err(|e| TransportError {
                message: format!("request_vote rpc failed: {}", e),
            })?
            .into_inner();

        Ok(RequestVoteResponse {
            term: resp.term,
            vote_granted: resp.vote_granted,
        })
    }

    fn send_append_entries(
        &self,
        target: NodeId,
        req: AppendEntries,
    ) -> Result<AppendEntriesResponse, TransportError> {
        let addr = self.addr_for(target)?;
        let mut client = self.connect_client(addr)?;

        let proto_req = AppendEntriesRequest {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req.entries.iter().map(to_proto_log_entry).collect(),
            leader_commit: req.leader_commit,
        };

        let resp = self
            .rt
            .block_on(client.append_entries(proto_req))
            .map_err(|e| TransportError {
                message: format!("append_entries rpc failed: {}", e),
            })?
            .into_inner();

        Ok(AppendEntriesResponse {
            term: resp.term,
            success: resp.success,
            match_index: resp.match_index,
        })
    }
}
