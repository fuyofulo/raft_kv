use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use raft_kv::raft::state::{
    AppendEntries, AppendEntriesResponse, PersistentState, RaftNode, RequestVote,
    RequestVoteResponse, Role, VolatileState,
};
use raft_kv::rpc::raft::raft_rpc_server::RaftRpcServer;
use raft_kv::rpc::server::RaftRpcService;
use raft_kv::rpc::convert::{to_proto_append_entries, to_proto_request_vote};
use raft_kv::rpc::raft::raft_rpc_client::RaftRpcClient;

use tonic::transport::Server;

fn build_node(id: u64, peers: Vec<u64>) -> RaftNode {
    RaftNode {
        id,
        peers,
        persistent: PersistentState {
            current_term: 0,
            voted_for: None,
            log: vec![],
        },
        volatile: VolatileState {
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
        },
        leader_state: None,
        known_leader: None,
        state_machine: std::collections::HashMap::new(),
        dedup_table: std::collections::HashMap::new(),
    }
}

fn parse_peers(raw: &str, self_id: u64) -> Vec<u64> {
    raw.split(',')
        .filter_map(|s| s.trim().parse::<u64>().ok())
        .filter(|id| *id != self_id)
        .collect()
}

fn seed_for_node(id: u64) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    now ^ id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn next_election_timeout(seed: &mut u64) -> Duration {
    // xorshift64 for lightweight per-round jitter.
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    let jitter_ms = *seed % 400;
    Duration::from_millis(700 + jitter_ms)
}

async fn send_request_vote_rpc(
    addr: String,
    req: RequestVote,
) -> Option<RequestVoteResponse> {
    let mut client = RaftRpcClient::connect(addr).await.ok()?;
    let proto = to_proto_request_vote(&req);
    let resp = client.request_vote(proto).await.ok()?.into_inner();
    Some(RequestVoteResponse {
        term: resp.term,
        vote_granted: resp.vote_granted,
    })
}

async fn send_append_entries_rpc(
    addr: String,
    req: AppendEntries,
) -> Option<AppendEntriesResponse> {
    let mut client = RaftRpcClient::connect(addr).await.ok()?;
    let proto = to_proto_append_entries(&req);
    let resp = client.append_entries(proto).await.ok()?.into_inner();
    Some(AppendEntriesResponse {
        term: resp.term,
        success: resp.success,
        match_index: resp.match_index,
    })
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // usage: cargo run --bin node -- <id> <listen_addr> <peer_ids_csv>
    // example: cargo run --bin node -- 1 127.0.0.1:50051 2,3,4,5
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: node <id> <listen_addr> <peer_ids_csv>");
        std::process::exit(1);
    }

    let id: u64 = args[1].parse()?;
    let addr: SocketAddr = args[2].parse()?;
    let peers = parse_peers(&args[3], id);
    let peers_for_election = peers.clone();

    let node = Arc::new(Mutex::new(build_node(id, peers)));
    let last_heartbeat = Arc::new(Mutex::new(Instant::now()));
    let svc = RaftRpcService {
        node: Arc::clone(&node),
        last_heartbeat: Arc::clone(&last_heartbeat),
    };

    let node_for_election = Arc::clone(&node);
    let hb_for_election = Arc::clone(&last_heartbeat);
    let node_for_heartbeats = Arc::clone(&node);
    let peers_for_heartbeats = peers_for_election.clone();

    tokio::spawn(async move {
        let mut seed = seed_for_node(id);
        let mut election_timeout = next_election_timeout(&mut seed);

        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let elapsed = {
                let t = hb_for_election.lock().expect("heartbeat lock poisoned");
                t.elapsed()
            };

            let should_elect = {
                let n = node_for_election.lock().expect("node lock poisoned");
                n.volatile.role != Role::Leader && elapsed >= election_timeout
            };

            if !should_elect {
                continue;
            }

            let (term, req) = {
                let mut n = node_for_election.lock().expect("node lock poisoned");
                n.become_candidate();
                let req = RequestVote {
                    term: n.persistent.current_term,
                    candidate_id: n.id,
                    last_log_index: n.last_log_index(),
                    last_log_term: n.last_log_term(),
                };
                (n.persistent.current_term, req)
            };

            let mut votes = 1;
            for peer in &peers_for_election {
                let addr = format!("http://127.0.0.1:{}", 50050 + *peer);
                if let Some(resp) = send_request_vote_rpc(addr, req.clone()).await {
                    if resp.term > term {
                        let mut n = node_for_election.lock().expect("node lock poisoned");
                        n.become_follower(resp.term);
                        break;
                    }
                    if resp.vote_granted {
                        votes += 1;
                    }
                }
            }

            let quorum = (peers_for_election.len() + 1) / 2 + 1;
            if votes >= quorum {
                let mut n = node_for_election.lock().expect("node lock poisoned");
                if n.volatile.role == Role::Candidate && n.persistent.current_term == term {
                    n.become_leader();
                    println!("node {} became LEADER term {}", n.id, n.persistent.current_term);
                }
            }

            let mut hb = hb_for_election.lock().expect("heartbeat lock poisoned");
            *hb = Instant::now();
            election_timeout = next_election_timeout(&mut seed);
        }
    });

    tokio::spawn(async move {
        let heartbeat_interval = Duration::from_millis(200);

        loop {
            tokio::time::sleep(heartbeat_interval).await;

            let is_leader = {
                let n = node_for_heartbeats.lock().expect("node lock poisoned");
                n.volatile.role == Role::Leader
            };

            if !is_leader {
                continue;
            }

            for peer in &peers_for_heartbeats {
                let req_for_peer = {
                    let n = node_for_heartbeats.lock().expect("node lock poisoned");
                    n.build_append_entries_for_peer(*peer)
                };

                let Some(req) = req_for_peer else {
                    continue;
                };

                let addr = format!("http://127.0.0.1:{}", 50050 + *peer);
                if let Some(resp) = send_append_entries_rpc(addr, req).await {
                    let mut n = node_for_heartbeats.lock().expect("node lock poisoned");
                    n.on_append_entries_response(*peer, resp);
                }
            }
        }
    });

    println!("node {} listening on {}", id, addr);

    Server::builder()
        .add_service(RaftRpcServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
