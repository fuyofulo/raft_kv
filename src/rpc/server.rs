use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tonic::{Request, Response, Status};

use crate::raft::state::{ClientReadResult, ClientWriteResult, RaftNode, Role};
use crate::raft::storage::save_node_state;
use crate::rpc::convert::{
    from_proto_append_entries, from_proto_client_write, from_proto_request_vote,
    to_proto_append_entries, to_proto_append_entries_reply, to_proto_client_read_reply,
    to_proto_client_write_reply, to_proto_request_vote_reply,
};
use crate::rpc::raft::raft_rpc_client::RaftRpcClient;
use crate::rpc::raft::raft_rpc_server::RaftRpc;
use crate::rpc::raft::{
    AppendEntriesReply, AppendEntriesRequest, ClientReadReply, ClientReadRequest,
    ClientWriteReply, ClientWriteRequest, RequestVoteReply, RequestVoteRequest,
};

#[derive(Clone)]
pub struct RaftRpcService {
    pub node: Arc<Mutex<RaftNode>>,
    pub last_heartbeat: Arc<Mutex<Instant>>,
    pub storage_path: Arc<PathBuf>,
}

#[tonic::async_trait]
impl RaftRpc for RaftRpcService {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let req = from_proto_request_vote(request.into_inner());

        let resp = self
            .node
            .lock()
            .map_err(|_| Status::internal("node lock poisoned"))?
            .on_request_vote(req);

        {
            let node = self
                .node
                .lock()
                .map_err(|_| Status::internal("node lock poisoned"))?;
            persist_locked_node(&node, &self.storage_path)?;
        }

        if resp.vote_granted {
            let mut hb = self
                .last_heartbeat
                .lock()
                .map_err(|_| Status::internal("heartbeat lock poisoned"))?;
            *hb = Instant::now();
        }

        Ok(Response::new(to_proto_request_vote_reply(&resp)))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        {
            let mut hb = self
                .last_heartbeat
                .lock()
                .map_err(|_| Status::internal("heartbeat lock poisoned"))?;
            *hb = Instant::now();
        }

        let req = from_proto_append_entries(request.into_inner());

        let mut node = self
            .node
            .lock()
            .map_err(|_| Status::internal("node lock poisoned"))?;
        let resp = node.on_append_entries(req);
        persist_locked_node(&node, &self.storage_path)?;

        Ok(Response::new(to_proto_append_entries_reply(&resp)))
    }

    async fn client_write(
        &self,
        request: Request<ClientWriteRequest>,
    ) -> Result<Response<ClientWriteReply>, Status> {
        let req = from_proto_client_write(request.into_inner())
            .map_err(|e| Status::invalid_argument(format!("bad client write request: {}", e)))?;

        let (result, term, self_id) = {
            let mut node = self
                .node
                .lock()
                .map_err(|_| Status::internal("node lock poisoned"))?;
            let result = node.on_client_write(req);
            persist_locked_node(&node, &self.storage_path)?;
            (result, node.persistent.current_term, node.id)
        };

        let reply = match result {
            ClientWriteResult::Ok {
                log_index,
                from_cache,
                message,
            } => {
                if from_cache {
                    let node = self
                        .node
                        .lock()
                        .map_err(|_| Status::internal("node lock poisoned"))?;
                    to_proto_client_write_reply(
                        true,
                        node.persistent.current_term,
                        Some(node.id),
                        log_index,
                        node.volatile.commit_index,
                        format!("{} (from cache)", message),
                    )
                } else {
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
                    let final_reply = loop {
                        let (role, term_now, last_applied, commit_index, known_leader) = {
                            let node = self
                                .node
                                .lock()
                                .map_err(|_| Status::internal("node lock poisoned"))?;
                            (
                                node.volatile.role.clone(),
                                node.persistent.current_term,
                                node.volatile.last_applied,
                                node.volatile.commit_index,
                                node.known_leader,
                            )
                        };

                        if role != Role::Leader || term_now != term {
                            break to_proto_client_write_reply(
                                false,
                                term_now,
                                known_leader,
                                log_index,
                                commit_index,
                                "leadership lost before commit".to_string(),
                            );
                        }

                        if last_applied >= log_index {
                            let node = self
                                .node
                                .lock()
                                .map_err(|_| Status::internal("node lock poisoned"))?;
                            persist_locked_node(&node, &self.storage_path)?;
                            break to_proto_client_write_reply(
                                true,
                                node.persistent.current_term,
                                Some(self_id),
                                log_index,
                                node.volatile.commit_index,
                                "committed and applied".to_string(),
                            );
                        }

                        if tokio::time::Instant::now() >= deadline {
                            break to_proto_client_write_reply(
                                false,
                                term_now,
                                Some(self_id),
                                log_index,
                                commit_index,
                                "timed out before commit".to_string(),
                            );
                        }

                        tokio::time::sleep(Duration::from_millis(20)).await;
                    };
                    final_reply
                }
            }
            ClientWriteResult::NotLeader { known_leader } => {
                let node = self
                    .node
                    .lock()
                    .map_err(|_| Status::internal("node lock poisoned"))?;
                to_proto_client_write_reply(
                    false,
                    node.persistent.current_term,
                    known_leader,
                    0,
                    node.volatile.commit_index,
                    "not leader".to_string(),
                )
            }
        };

        Ok(Response::new(reply))
    }

    async fn client_read(
        &self,
        request: Request<ClientReadRequest>,
    ) -> Result<Response<ClientReadReply>, Status> {
        let key = request.into_inner().key;

        let (term, peers, quorum, commit_index, known_leader, is_leader) = {
            let node = self
                .node
                .lock()
                .map_err(|_| Status::internal("node lock poisoned"))?;
            (
                node.persistent.current_term,
                node.peers.clone(),
                (node.peers.len() + 1) / 2 + 1,
                node.volatile.commit_index,
                node.known_leader,
                node.volatile.role == Role::Leader,
            )
        };

        if !is_leader {
            let reply = to_proto_client_read_reply(
                false,
                false,
                String::new(),
                term,
                known_leader,
                commit_index,
                "not leader".to_string(),
            );
            return Ok(Response::new(reply));
        }

        let mut acks = 1usize;
        for peer in peers {
            let req = {
                let node = self
                    .node
                    .lock()
                    .map_err(|_| Status::internal("node lock poisoned"))?;
                if node.volatile.role != Role::Leader || node.persistent.current_term != term {
                    let reply = to_proto_client_read_reply(
                        false,
                        false,
                        String::new(),
                        node.persistent.current_term,
                        node.known_leader,
                        node.volatile.commit_index,
                        "not leader".to_string(),
                    );
                    return Ok(Response::new(reply));
                }
                node.build_append_entries_for_peer(peer)
            };

            let Some(req) = req else {
                continue;
            };

            let addr = format!("http://127.0.0.1:{}", 50050 + peer);
            let Some(resp) = send_append_entries_rpc(addr, req).await else {
                continue;
            };

            if resp.term > term {
                let mut node = self
                    .node
                    .lock()
                    .map_err(|_| Status::internal("node lock poisoned"))?;
                node.become_follower(resp.term);
                persist_locked_node(&node, &self.storage_path)?;
                let reply = to_proto_client_read_reply(
                    false,
                    false,
                    String::new(),
                    node.persistent.current_term,
                    node.known_leader,
                    node.volatile.commit_index,
                    "leadership lost".to_string(),
                );
                return Ok(Response::new(reply));
            }

            if resp.success {
                acks += 1;
            }

            let mut node = self
                .node
                .lock()
                .map_err(|_| Status::internal("node lock poisoned"))?;
            node.on_append_entries_response(peer, resp);
            persist_locked_node(&node, &self.storage_path)?;
        }

        if acks < quorum {
            let node = self
                .node
                .lock()
                .map_err(|_| Status::internal("node lock poisoned"))?;
            let reply = to_proto_client_read_reply(
                false,
                false,
                String::new(),
                node.persistent.current_term,
                Some(node.id),
                node.volatile.commit_index,
                "leadership not confirmed by quorum".to_string(),
            );
            return Ok(Response::new(reply));
        }

        let (result, term_now, commit_now, leader_now) = {
            let node = self
                .node
                .lock()
                .map_err(|_| Status::internal("node lock poisoned"))?;
            (
                node.on_client_read(&key),
                node.persistent.current_term,
                node.volatile.commit_index,
                Some(node.id),
            )
        };

        let reply = match result {
            ClientReadResult::Value(Some(value)) => to_proto_client_read_reply(
                true,
                true,
                value,
                term_now,
                leader_now,
                commit_now,
                "ok".to_string(),
            ),
            ClientReadResult::Value(None) => to_proto_client_read_reply(
                true,
                false,
                String::new(),
                term_now,
                leader_now,
                commit_now,
                "key not found".to_string(),
            ),
            ClientReadResult::NotLeader { known_leader } => to_proto_client_read_reply(
                false,
                false,
                String::new(),
                term_now,
                known_leader,
                commit_now,
                "not leader".to_string(),
            ),
        };

        Ok(Response::new(reply))
    }
}

async fn send_append_entries_rpc(
    addr: String,
    req: crate::raft::state::AppendEntries,
) -> Option<crate::raft::state::AppendEntriesResponse> {
    let mut client = RaftRpcClient::connect(addr).await.ok()?;
    let proto = to_proto_append_entries(&req);
    let resp = client.append_entries(proto).await.ok()?.into_inner();
    Some(crate::raft::state::AppendEntriesResponse {
        term: resp.term,
        success: resp.success,
        match_index: resp.match_index,
    })
}

fn persist_locked_node(node: &RaftNode, storage_path: &PathBuf) -> Result<(), Status> {
    save_node_state(storage_path.as_path(), node)
        .map_err(|e| Status::internal(format!("persist failed: {}", e)))
}
