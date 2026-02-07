use std::sync::{Arc, Mutex};

use tonic::{Request, Response, Status};

use crate::raft::state::RaftNode;
use crate::rpc::convert::{
    from_proto_append_entries, from_proto_request_vote, to_proto_append_entries_reply,
    to_proto_request_vote_reply,
};
use crate::rpc::raft::raft_rpc_server::RaftRpc;
use crate::rpc::raft::{
    AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
};

#[derive(Clone)]
pub struct RaftRpcService {
    pub node: Arc<Mutex<RaftNode>>,
}

#[tonic::async_trait]
impl RaftRpc for RaftRpcService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let req = from_proto_request_vote(request.into_inner());

        let mut node = self
            .node
            .lock()
            .map_err(|_| Status::internal("node lock poisoned"))?;
        let resp = node.on_request_vote(req);

        Ok(Response::new(to_proto_request_vote_reply(&resp)))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let req = from_proto_append_entries(request.into_inner());

        let mut node = self
            .node
            .lock()
            .map_err(|_| Status::internal("node lock poisoned"))?;
        let resp = node.on_append_entries(req);

        Ok(Response::new(to_proto_append_entries_reply(&resp)))
    }
}
