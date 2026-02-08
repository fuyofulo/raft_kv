use std::collections::HashMap;

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Put { key: String, value: String },
    Update { key: String, value: String },
    Delete { key: String },
    Noop
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub command: Command,
    pub client_id: Option<u64>,
    pub request_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>
}

#[derive(Clone, Debug)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub role: Role
}

#[derive(Clone, Debug, Default)]
pub struct LeaderState {
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>
}

#[derive(Clone, Debug)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term
}

#[derive(Clone, Debug)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool
}

#[derive(Clone, Debug)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex
}

#[derive(Clone, Debug)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex
}

#[derive(Clone, Debug)]
pub struct ClientWriteRequest {
    pub command: Command,
    pub client_id: u64,
    pub request_id: u64
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientWriteResult {
    Ok {
        log_index: LogIndex,
        from_cache: bool,
        message: String,
    },
    NotLeader { known_leader: Option<NodeId> }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientReadResult {
    Value(Option<String>),
    NotLeader { known_leader: Option<NodeId> },
}

#[derive(Clone, Debug)]
pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub leader_state: Option<LeaderState>,
    pub known_leader: Option<NodeId>,
    pub state_machine: HashMap<String, String>,
    pub dedup_table: HashMap<u64, CachedClientReply>,
}

#[derive(Clone, Debug)]
pub struct CachedClientReply {
    pub request_id: u64,
    pub log_index: LogIndex,
    pub message: String,
}

impl RaftNode {
    pub fn last_log_index(&self) -> LogIndex {
        self.persistent.log.len() as LogIndex
    }
    
    pub fn last_log_term(&self) -> Term {
        self.persistent.log.last().map(|e| e.term).unwrap_or(0)
    }
    
    pub fn log_term_at(&self, index: LogIndex) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.persistent.log.get((index-1) as usize).map(|e| e.term)
    } 
    
    pub fn is_candidate_log_up_to_date(&self, candidate_last_log_term: Term, candidate_last_log_index: LogIndex) -> bool {
        let my_term = self.last_log_term();
        let my_index = self.last_log_index();
        
        if candidate_last_log_term != my_term {
            candidate_last_log_term > my_term
        } else {
            candidate_last_log_index >= my_index
        }
    }
    
    pub fn become_follower(&mut self, new_term: Term) {
        if self.persistent.current_term > new_term {
            return;
        }
        
        if new_term > self.persistent.current_term {
            self.persistent.current_term = new_term;
            self.persistent.voted_for = None;
        }
        
        self.volatile.role = Role::Follower;
        self.leader_state = None;
        self.known_leader = None;
    }
    
    pub fn become_candidate(&mut self) {
        self.volatile.role = Role::Candidate;
        self.persistent.current_term += 1;
        self.persistent.voted_for = Some(self.id);
        self.leader_state = None;
        self.known_leader = None;
    }
    
    pub fn become_leader(&mut self) {
        self.volatile.role = Role::Leader;
        self.known_leader = Some(self.id);
        
        let mut leader_state = LeaderState::default();
        let next = self.last_log_index() + 1;
        
        for &peer in &self.peers {
            leader_state.next_index.insert(peer, next);
            leader_state.match_index.insert(peer, 0);
        }
        
        self.leader_state = Some(leader_state);
    }
    
    pub fn on_request_vote(&mut self, req: RequestVote) -> RequestVoteResponse {
        
        if req.term < self.persistent.current_term {
            return RequestVoteResponse {
                term: self.persistent.current_term,
                vote_granted: false
            };
        }
        
        if req.term > self.persistent.current_term {
            self.become_follower(req.term);
        }
        
        let voted_for_other = matches!(
            self.persistent.voted_for,
            Some(voted_for) if voted_for != req.candidate_id
        );
        
        if voted_for_other {
            return RequestVoteResponse {
                term: self.persistent.current_term,
                vote_granted: false
            };
        }
        
        if !self.is_candidate_log_up_to_date(req.last_log_term, req.last_log_index) {
            return RequestVoteResponse {
                term: self.persistent.current_term,
                vote_granted: false
            }
        }
        
        self.persistent.voted_for = Some(req.candidate_id);
        RequestVoteResponse {
            term: self.persistent.current_term,
            vote_granted: true,
        }
    }
    
    pub fn on_append_entries(&mut self, req: AppendEntries) -> AppendEntriesResponse {
        
        if req.term < self.persistent.current_term {
            return AppendEntriesResponse {
                term: self.persistent.current_term,
                success: false,
                match_index: self.last_log_index()
            };
        }
        
        self.become_follower(req.term);
        self.known_leader = Some(req.leader_id);
        
        if self.log_term_at(req.prev_log_index) != Some(req.prev_log_term) {
            return AppendEntriesResponse {
                term: self.persistent.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }
        
        let mut log_changed = false;
        let mut insert_index = req.prev_log_index + 1;
        for entry in req.entries {
            
            let vec_idx = (insert_index - 1) as usize;
            
            match self.persistent.log.get(vec_idx) {
                Some(existing) if existing.term == entry.term => {
                    insert_index += 1;
                }
                Some(_) => {
                    self.persistent.log.truncate(vec_idx);
                    self.persistent.log.push(entry);
                    log_changed = true;
                    insert_index += 1;
                }
                None => {
                    self.persistent.log.push(entry);
                    log_changed = true;
                    insert_index += 1;
                }
            }
        }

        if log_changed {
            self.print_log_dump("append_entries updated log");
        }
        
        let new_commit = std::cmp::min(req.leader_commit, self.last_log_index());
        if new_commit > self.volatile.commit_index {
            self.volatile.commit_index = new_commit;
            self.apply_committed_entries();
        }
        
        AppendEntriesResponse {
            term: self.persistent.current_term,
            success: true,
            match_index: self.last_log_index()
        }  
    } 
    
    pub fn advance_commit_index(&mut self) {
        if self.volatile.role != Role::Leader {
            return;
        }
        
        let Some(leader_state) = self.leader_state.as_ref() else {
            return;
        };
        
        let quorum = (self.peers.len() + 1)/ 2 + 1;
        let mut new_commit = self.volatile.commit_index;
        
        for n in (self.volatile.commit_index + 1)..=self.last_log_index() {
            if self.log_term_at(n) != Some(self.persistent.current_term) {
                continue;
            }
            
            let mut replicated = 1;
            for peer in &self.peers {
                let m = leader_state.match_index.get(peer).copied().unwrap_or(0);
                if m >= n {
                    replicated += 1;
                }
            }
            
            if replicated >= quorum {
                new_commit = n;
            }
        }
        self.volatile.commit_index = new_commit;
        self.apply_committed_entries();
    }
    
    pub fn on_append_entries_response(&mut self, from: NodeId, response: AppendEntriesResponse) {
        
        if response.term > self.persistent.current_term {
            self.become_follower(response.term);
            return;
        }
        
        let default_next = self.last_log_index() + 1;
        
        if self.volatile.role != Role::Leader {
            return;
        }
        
        let Some(leader_state) = self.leader_state.as_mut() else {
            return;
        };
        
        if response.success {
            leader_state.match_index.insert(from, response.match_index);
            leader_state.next_index.insert(from, response.match_index + 1);
        } else {
            let current = leader_state.next_index.get(&from).copied().unwrap_or(default_next);
            leader_state.next_index.insert(from, std::cmp::max(1, current.saturating_sub(1)));
        }
        
        self.advance_commit_index();
    }

    pub fn on_client_write(
        &mut self,
        req: ClientWriteRequest,
    ) -> ClientWriteResult {
        if self.volatile.role != Role::Leader {
            return ClientWriteResult::NotLeader {
                known_leader: self.known_leader,
            };
        }

        if let Some(cached) = self.dedup_table.get(&req.client_id) {
            if cached.request_id >= req.request_id {
                return ClientWriteResult::Ok {
                    log_index: cached.log_index,
                    from_cache: true,
                    message: cached.message.clone(),
                };
            }
        }

        if let Some(existing_idx) = self.find_request_in_log(req.client_id, req.request_id) {
            return ClientWriteResult::Ok {
                log_index: existing_idx,
                from_cache: false,
                message: "request already present in log".to_string(),
            };
        }

        let entry = LogEntry {
            term: self.persistent.current_term,
            command: req.command,
            client_id: Some(req.client_id),
            request_id: Some(req.request_id),
        };
        self.persistent.log.push(entry);
        self.print_log_dump("client_write appended entry");

        ClientWriteResult::Ok {
            log_index: self.last_log_index(),
            from_cache: false,
            message: "accepted by leader".to_string(),
        }
    }

    pub fn on_client_read(&self, key: &str) -> ClientReadResult {
        if self.volatile.role != Role::Leader {
            return ClientReadResult::NotLeader {
                known_leader: self.known_leader,
            };
        }
        ClientReadResult::Value(self.state_machine.get(key).cloned())
    }

    pub fn build_append_entries_for_peer(&self, peer: NodeId) -> Option<AppendEntries> {
        if self.volatile.role != Role::Leader {
            return None;
        }

        let leader_state = self.leader_state.as_ref()?;
        let next_index = leader_state
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(self.last_log_index() + 1);
        let prev_log_index = next_index.saturating_sub(1);
        let prev_log_term = self.log_term_at(prev_log_index)?;

        let start = next_index.saturating_sub(1) as usize;
        let entries = if start < self.persistent.log.len() {
            self.persistent.log[start..].to_vec()
        } else {
            vec![]
        };

        Some(AppendEntries {
            term: self.persistent.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.volatile.commit_index,
        })
    }

    fn find_request_in_log(&self, client_id: u64, request_id: u64) -> Option<LogIndex> {
        self.persistent
            .log
            .iter()
            .enumerate()
            .find_map(|(idx, entry)| {
                if entry.client_id == Some(client_id) && entry.request_id == Some(request_id) {
                    Some((idx as LogIndex) + 1)
                } else {
                    None
                }
            })
    }

    fn apply_committed_entries(&mut self) {
        while self.volatile.last_applied < self.volatile.commit_index {
            let index = self.volatile.last_applied + 1;
            let vec_idx = (index - 1) as usize;
            let Some(entry) = self.persistent.log.get(vec_idx).cloned() else {
                break;
            };

            self.apply_entry(index, entry);
            self.volatile.last_applied = index;
        }
    }

    fn apply_entry(&mut self, index: LogIndex, entry: LogEntry) {
        if let (Some(client_id), Some(request_id)) = (entry.client_id, entry.request_id) {
            if let Some(cached) = self.dedup_table.get(&client_id) {
                if cached.request_id >= request_id {
                    return;
                }
            }

            let message = self.apply_command(&entry.command);
            self.dedup_table.insert(
                client_id,
                CachedClientReply {
                    request_id,
                    log_index: index,
                    message,
                },
            );
            return;
        }

        self.apply_command(&entry.command);
    }

    fn apply_command(&mut self, command: &Command) -> String {
        match command {
            Command::Put { key, value } => {
                self.state_machine.insert(key.clone(), value.clone());
                "ok".to_string()
            }
            Command::Update { key, value } => {
                if self.state_machine.contains_key(key) {
                    self.state_machine.insert(key.clone(), value.clone());
                    "ok".to_string()
                } else {
                    "key not found".to_string()
                }
            }
            Command::Delete { key } => {
                self.state_machine.remove(key);
                "ok".to_string()
            }
            Command::Noop => "noop".to_string(),
        }
    }

    fn print_log_dump(&self, reason: &str) {
        println!("---");
        println!("log");
        println!(
            "node={} term={} role={:?} reason={}",
            self.id, self.persistent.current_term, self.volatile.role, reason
        );
        if self.persistent.log.is_empty() {
            println!("(empty)");
        } else {
            for (idx, entry) in self.persistent.log.iter().enumerate() {
                println!(
                    "[{}] term={} cmd={} client_id={:?} request_id={:?}",
                    idx + 1,
                    entry.term,
                    Self::command_as_string(&entry.command),
                    entry.client_id,
                    entry.request_id
                );
            }
        }
        println!(
            "commit_index={} last_applied={}",
            self.volatile.commit_index, self.volatile.last_applied
        );
        println!("---");
    }

    fn command_as_string(cmd: &Command) -> String {
        match cmd {
            Command::Put { key, value } => format!("put {}={}", key, value),
            Command::Update { key, value } => format!("update {}={}", key, value),
            Command::Delete { key } => format!("delete {}", key),
            Command::Noop => "noop".to_string(),
        }
    }
}
