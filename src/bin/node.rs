use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use raft_kv::raft::events::{RaftEvent, install_event_sender, new_event, emit_event};
use raft_kv::raft::state::{
    AppendEntries, AppendEntriesResponse, PersistentState, RaftNode, RequestVote,
    RequestVoteResponse, Role, VolatileState,
};
use raft_kv::raft::storage::{load_durable_state, save_node_state};
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

fn persist_best_effort(node: &RaftNode, path: &Path) {
    if let Err(e) = save_node_state(path, node) {
        eprintln!(
            "node {} failed to persist durable state to {}: {}",
            node.id,
            path.display(),
            e
        );
    }
}

fn parse_data_and_control(id: u64, args: &[String]) -> (PathBuf, Option<String>) {
    let mut data_path = PathBuf::from(format!("data/node-{}.json", id));
    let mut control_url = env::var("CONTROL_PLANE_URL").ok();

    if let Some(a4) = args.get(4) {
        if a4.starts_with("http://") || a4.starts_with("https://") {
            control_url = Some(a4.clone());
        } else {
            data_path = PathBuf::from(a4);
        }
    }
    if let Some(a5) = args.get(5) {
        control_url = Some(a5.clone());
    }

    (data_path, control_url)
}

async fn post_events_task(mut rx: tokio::sync::mpsc::UnboundedReceiver<RaftEvent>, control_url: String) {
    let client = reqwest::Client::new();
    let endpoint = format!("{}/events", control_url.trim_end_matches('/'));

    while let Some(event) = rx.recv().await {
        let _ = client.post(&endpoint).json(&event).send().await;
    }
}

async fn poll_enabled_task(
    id: u64,
    control_url: String,
    enabled: Arc<AtomicBool>,
) {
    let client = reqwest::Client::new();
    let endpoint = format!(
        "{}/nodes/{}/enabled",
        control_url.trim_end_matches('/'),
        id
    );

    loop {
        let next = match client.get(&endpoint).send().await {
            Ok(resp) => {
                let json = resp.json::<serde_json::Value>().await.ok();
                json.and_then(|v| v.get("enabled").and_then(|x| x.as_bool()))
                    .unwrap_or(true)
            }
            Err(_) => true,
        };
        enabled.store(next, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn is_link_allowed(
    client: &reqwest::Client,
    control_url: Option<&str>,
    from: u64,
    to: u64,
) -> bool {
    let Some(url) = control_url else {
        return true;
    };
    let endpoint = format!(
        "{}/allow/{}/{}",
        url.trim_end_matches('/'),
        from,
        to
    );
    match client.get(endpoint).send().await {
        Ok(resp) => {
            let json = resp.json::<serde_json::Value>().await.ok();
            json.and_then(|v| v.get("allow").and_then(|x| x.as_bool()))
                .unwrap_or(true)
        }
        Err(_) => true,
    }
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
    // optional args:
    //   [data_path]
    //   [control_plane_url]
    // examples:
    //   cargo run --bin node -- 1 127.0.0.1:50051 2,3,4,5 data/node-1.json
    //   cargo run --bin node -- 1 127.0.0.1:50051 2,3,4,5 http://127.0.0.1:7000
    //   cargo run --bin node -- 1 127.0.0.1:50051 2,3,4,5 data/node-1.json http://127.0.0.1:7000
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: node <id> <listen_addr> <peer_ids_csv> [data_path|control_plane_url] [control_plane_url]");
        std::process::exit(1);
    }

    let id: u64 = args[1].parse()?;
    let addr: SocketAddr = args[2].parse()?;
    let peers = parse_peers(&args[3], id);
    let peers_for_election = peers.clone();
    let (storage_path, control_plane_url) = parse_data_and_control(id, &args);

    let mut initial_node = build_node(id, peers);
    if let Some(durable) = load_durable_state(&storage_path)? {
        initial_node.persistent = durable.persistent;
        initial_node.volatile.commit_index = durable
            .commit_index
            .min(initial_node.persistent.log.len() as u64);
        initial_node.volatile.last_applied = durable
            .last_applied
            .min(initial_node.volatile.commit_index);
        initial_node.state_machine = durable.state_machine;
        initial_node.dedup_table = durable.dedup_table;
        initial_node.volatile.role = Role::Follower;
        initial_node.known_leader = None;
        initial_node.leader_state = None;
        println!(
            "node {} loaded durable state from {} (term={}, log_len={}, commit_index={}, last_applied={})",
            id,
            storage_path.display(),
            initial_node.persistent.current_term,
            initial_node.persistent.log.len(),
            initial_node.volatile.commit_index,
            initial_node.volatile.last_applied
        );
    }

    let node = Arc::new(Mutex::new(initial_node));
    let last_heartbeat = Arc::new(Mutex::new(Instant::now()));
    let node_enabled = Arc::new(AtomicBool::new(true));
    let storage_path = Arc::new(storage_path);
    let svc = RaftRpcService {
        node: Arc::clone(&node),
        last_heartbeat: Arc::clone(&last_heartbeat),
        storage_path: Arc::clone(&storage_path),
        node_enabled: Arc::clone(&node_enabled),
    };

    let node_for_election = Arc::clone(&node);
    let hb_for_election = Arc::clone(&last_heartbeat);
    let node_for_heartbeats = Arc::clone(&node);
    let peers_for_heartbeats = peers_for_election.clone();
    let storage_for_election = Arc::clone(&storage_path);
    let storage_for_heartbeats = Arc::clone(&storage_path);
    let enabled_for_election = Arc::clone(&node_enabled);
    let enabled_for_heartbeats = Arc::clone(&node_enabled);
    let control_for_election = control_plane_url.clone();
    let control_for_heartbeats = control_plane_url.clone();

    if let Some(url) = control_plane_url.clone() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        install_event_sender(tx);
        tokio::spawn(post_events_task(rx, url.clone()));
        tokio::spawn(poll_enabled_task(id, url, Arc::clone(&node_enabled)));
    }

    tokio::spawn(async move {
        let control_http = reqwest::Client::new();
        let mut seed = seed_for_node(id);
        let mut election_timeout = next_election_timeout(&mut seed);

        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            if !enabled_for_election.load(Ordering::Relaxed) {
                continue;
            }

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
                persist_best_effort(&n, storage_for_election.as_ref().as_path());
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
                if !is_link_allowed(
                    &control_http,
                    control_for_election.as_deref(),
                    id,
                    *peer,
                )
                .await
                {
                    emit_event(new_event(
                        id,
                        "network_drop",
                        format!("request_vote {} -> {} dropped by control plane", id, peer),
                        term,
                        0,
                        0,
                        0,
                    ));
                    continue;
                }
                let addr = format!("http://127.0.0.1:{}", 50050 + *peer);
                if let Some(resp) = send_request_vote_rpc(addr, req.clone()).await {
                    if resp.term > term {
                        let mut n = node_for_election.lock().expect("node lock poisoned");
                        n.become_follower(resp.term);
                        persist_best_effort(&n, storage_for_election.as_ref().as_path());
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
        let control_http = reqwest::Client::new();
        let heartbeat_interval = Duration::from_millis(200);

        loop {
            tokio::time::sleep(heartbeat_interval).await;

            if !enabled_for_heartbeats.load(Ordering::Relaxed) {
                continue;
            }

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

                if !is_link_allowed(
                    &control_http,
                    control_for_heartbeats.as_deref(),
                    id,
                    *peer,
                )
                .await
                {
                    emit_event(new_event(
                        id,
                        "network_drop",
                        format!("append_entries {} -> {} dropped by control plane", id, peer),
                        req.term,
                        req.leader_commit,
                        req.leader_commit,
                        req.prev_log_index + req.entries.len() as u64,
                    ));
                    continue;
                }

                let addr = format!("http://127.0.0.1:{}", 50050 + *peer);
                if let Some(resp) = send_append_entries_rpc(addr, req).await {
                    let mut n = node_for_heartbeats.lock().expect("node lock poisoned");
                    n.on_append_entries_response(*peer, resp);
                    persist_best_effort(&n, storage_for_heartbeats.as_ref().as_path());
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
