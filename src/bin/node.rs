use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use raft_kv::raft::state::{PersistentState, RaftNode, Role, VolatileState};
use raft_kv::rpc::raft::raft_rpc_server::RaftRpcServer;
use raft_kv::rpc::server::RaftRpcService;
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
    }
}

fn parse_peers(raw: &str, self_id: u64) -> Vec<u64> {
    raw.split(',')
        .filter_map(|s| s.trim().parse::<u64>().ok())
        .filter(|id| *id != self_id)
        .collect()
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

    let node = Arc::new(Mutex::new(build_node(id, peers)));
    let svc = RaftRpcService { node };

    println!("node {} listening on {}", id, addr);

    Server::builder()
        .add_service(RaftRpcServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
