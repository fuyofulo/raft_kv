use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

use raft_kv::rpc::raft::raft_rpc_client::RaftRpcClient;
use raft_kv::rpc::raft::{ClientReadRequest, ClientWriteRequest};

fn normalize_addr(raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.to_string()
    } else {
        format!("http://{}", raw)
    }
}

fn host_from_addr(addr: &str) -> String {
    let no_scheme = addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let host_port = no_scheme.split('/').next().unwrap_or(no_scheme);
    host_port
        .rsplit_once(':')
        .map(|(host, _)| host.to_string())
        .unwrap_or_else(|| host_port.to_string())
}

fn addr_for_leader(seed_addr: &str, leader_id: u64) -> Option<String> {
    if leader_id == 0 {
        return None;
    }
    let host = host_from_addr(seed_addr);
    Some(format!("http://{}:{}", host, 50050 + leader_id))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // usage:
    // cargo run --bin client -- <addr> put <key> <value> [client_id] [request_id]
    // cargo run --bin client -- <addr> update <key> <value> [client_id] [request_id]
    // cargo run --bin client -- <addr> delete <key> [client_id] [request_id]
    // cargo run --bin client -- <addr> get <key>
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("usage:");
        eprintln!("  client <addr> put <key> <value> [client_id] [request_id]");
        eprintln!("  client <addr> update <key> <value> [client_id] [request_id]");
        eprintln!("  client <addr> delete <key> [client_id] [request_id]");
        eprintln!("  client <addr> get <key>");
        std::process::exit(1);
    }

    let addr = normalize_addr(&args[1]);
    let op = args[2].to_lowercase();
    let key = args[3].clone();

    let (value, client_id_pos, request_id_pos, is_read) = if op == "put" || op == "update" {
        if args.len() < 5 {
            eprintln!("put/update requires a value");
            std::process::exit(1);
        }
        (args[4].clone(), 5, 6, false)
    } else if op == "delete" {
        (String::new(), 4, 5, false)
    } else if op == "get" {
        (String::new(), 4, 5, true)
    } else {
        eprintln!("op must be one of: put, update, delete, get");
        std::process::exit(1);
    };

    let client_id = args
        .get(client_id_pos)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(1);
    let request_id = args
        .get(request_id_pos)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        });

    let mut target_addr = addr.clone();
    let max_attempts = 3;
    let mut attempt = 0;

    if is_read {
        let req = ClientReadRequest { key };
        let reply = loop {
            attempt += 1;
            let mut client = RaftRpcClient::connect(target_addr.clone()).await?;
            let reply = client.client_read(req.clone()).await?.into_inner();

            if reply.success || attempt >= max_attempts {
                break reply;
            }

            let Some(next_addr) = addr_for_leader(&addr, reply.leader_id) else {
                break reply;
            };
            if next_addr == target_addr {
                break reply;
            }

            eprintln!(
                "redirect: node is not leader, retrying leader_id={} at {}",
                reply.leader_id, next_addr
            );
            target_addr = next_addr;
        };

        println!(
            "success={} found={} value={} term={} leader_id={} commit_index={} message={}",
            reply.success,
            reply.found,
            reply.value,
            reply.term,
            reply.leader_id,
            reply.commit_index,
            reply.message
        );
    } else {
        let req = ClientWriteRequest {
            op,
            key,
            value,
            client_id,
            request_id,
        };

        let reply = loop {
            attempt += 1;
            let mut client = RaftRpcClient::connect(target_addr.clone()).await?;
            let reply = client.client_write(req.clone()).await?.into_inner();

            if reply.accepted || attempt >= max_attempts {
                break reply;
            }

            let Some(next_addr) = addr_for_leader(&addr, reply.leader_id) else {
                break reply;
            };
            if next_addr == target_addr {
                break reply;
            }

            eprintln!(
                "redirect: node is not leader, retrying leader_id={} at {}",
                reply.leader_id, next_addr
            );
            target_addr = next_addr;
        };

        println!(
            "accepted={} term={} leader_id={} log_index={} commit_index={} message={}",
            reply.accepted,
            reply.term,
            reply.leader_id,
            reply.log_index,
            reply.commit_index,
            reply.message
        );
    }

    Ok(())
}
