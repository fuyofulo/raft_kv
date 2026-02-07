use std::collections::HashMap;

use raft_kv::raft::state::{AppendEntries, Command, LogEntry, RequestVote};
use raft_kv::raft::transport::RaftTransport;
use raft_kv::rpc::client::GrpcTransport;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut peer_addrs = HashMap::new();
    peer_addrs.insert(2, "http://127.0.0.1:50052".to_string());
    peer_addrs.insert(3, "http://127.0.0.1:50053".to_string());
    peer_addrs.insert(4, "http://127.0.0.1:50054".to_string());
    peer_addrs.insert(5, "http://127.0.0.1:50055".to_string());

    let transport = GrpcTransport::new(peer_addrs)?;

    // Simulate node1 candidacy in term 1 with empty log.
    let vote_req = RequestVote {
        term: 1,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
    };

    let mut votes = 1;
    for peer in [2, 3, 4, 5] {
        let resp = transport.send_request_vote(peer, vote_req.clone())?;
        println!(
            "request_vote -> node {}: term={} granted={}",
            peer, resp.term, resp.vote_granted
        );
        if resp.vote_granted {
            votes += 1;
        }
    }
    println!("total votes for node1 = {}", votes);

    let append_req = AppendEntries {
        term: 1,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![LogEntry {
            term: 1,
            command: Command::Put {
                key: "x".to_string(),
                value: "10".to_string(),
            },
        }],
        leader_commit: 0,
    };

    for peer in [2, 3, 4, 5] {
        let resp = transport.send_append_entries(peer, append_req.clone())?;
        println!(
            "append_entries -> node {}: term={} success={} match_index={}",
            peer, resp.term, resp.success, resp.match_index
        );
    }

    Ok(())
}
