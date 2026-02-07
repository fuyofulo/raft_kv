use raft_kv::raft::memory_transport::InMemoryTransport;
use raft_kv::raft::state::{
    AppendEntries,
    Command,
    LogEntry,
    PersistentState,
    RaftNode,
    RequestVote,
    Role,
    VolatileState,
};
use raft_kv::raft::transport::RaftTransport;

fn build_node(id: u64, all_ids: &[u64]) -> RaftNode {
    let peers = all_ids.iter().copied().filter(|n| *n != id).collect();
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
        known_leader: None
    }
}

fn main() {
    let transport = InMemoryTransport::default();
    let ids = vec![1,2,3,4,5];
    
    for id in &ids {
        transport.register_nodes(build_node(*id, &ids));
    }
    
    let vote_request = {
        let node1_arc = {
            let map = transport.nodes.lock().unwrap();
            map.get(&1).unwrap().clone()
        };
        
        let mut node1 = node1_arc.lock().unwrap();
        node1.become_candidate();
        
        RequestVote {
            term: node1.persistent.current_term,
            candidate_id: 1,
            last_log_index: node1.last_log_index(),
            last_log_term: node1.last_log_term()
        }
    };
    
    let mut votes = 1;
    for peer in [2,3,4,5] {
        let response = transport.send_request_vote(peer, vote_request.clone()).unwrap();
        println!("vote from {} => granted = {}", peer, response.vote_granted);
        if response.vote_granted {
            votes += 1;
        }
    }
    
    if votes >= 3 {
        let node1_arc = {
            let map = transport.nodes.lock().unwrap();
            map.get(&1).unwrap().clone()
        };
        node1_arc.lock().unwrap().become_leader();
    }
    
    let leader_term = {
        let node1_arc = {
            let map = transport.nodes.lock().unwrap();
            map.get(&1).unwrap().clone()
        };
        let mut node1 = node1_arc.lock().unwrap();
        let t = node1.persistent.current_term;
        node1.persistent.log.push(LogEntry {
            term: t,
            command: Command::Put {
                key: "x".to_string(),
                value: "10".to_string(),
            },
        });
        t
    };
    
    let request = AppendEntries {
        term: leader_term,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![LogEntry {
            term: leader_term,
            command: Command::Put {
                key: "x".to_string(),
                value: "10".to_string(),
            },
        }],
        leader_commit: 0,
    };
    
    let mut acks = 1;
    for peer in [2,3,4,5] {
        let response = transport.send_append_entries(peer, request.clone()).unwrap();
        {
            let node1_arc = {
                let map = transport.nodes.lock().unwrap();
                map.get(&1).unwrap().clone()
            };
            let mut leader = node1_arc.lock().unwrap();
            leader.on_append_entries_response(peer, response.clone());
        }
        println!("append to {} => success= {}", peer, response.success);
        if response.success {
            acks += 1;
        }
    }
    println!("replication acks = {}", acks);
    
    for id in [1,2,3,4,5] {
        let node_arc = {
            let map = transport.nodes.lock().unwrap();
            map.get(&id).unwrap().clone()
        };
        let n = node_arc.lock().unwrap();
        println!(
            "node = {} role = {:?} term = {} log_len = {} commit_index = {}",
            n.id, n.volatile.role, n.persistent.current_term, n.persistent.log.len(), n.volatile.commit_index
        );
    }
    
    let (leader_term_now, leader_commit_now, prev_idx, prev_term) = {
        let node1_arc = {
            let map = transport.nodes.lock().unwrap();
            map.get(&1).unwrap().clone()
        };
        let leader = node1_arc.lock().unwrap();
        (
            leader.persistent.current_term,
            leader.volatile.commit_index,
            leader.last_log_index(),
            leader.last_log_term(),
        )
    };

    let heartbeat = AppendEntries {
        term: leader_term_now,
        leader_id: 1,
        prev_log_index: prev_idx,
        prev_log_term: prev_term,
        entries: vec![],
        leader_commit: leader_commit_now,
    };

    for peer in [2, 3, 4, 5] {
        let resp = transport.send_append_entries(peer, heartbeat.clone()).unwrap();
        println!("heartbeat to {} => success={}", peer, resp.success);
    }
    
    {
        let node1_arc = {
            let map = transport.nodes.lock().unwrap();
            map.get(&1).unwrap().clone()
        };
        let leader = node1_arc.lock().unwrap();
        println!("leader commit_index after responses = {}", leader.volatile.commit_index);
    }

    
}