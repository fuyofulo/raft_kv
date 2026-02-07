use std::collections::HashMap;

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Put { key: String, value: String },
    Delete { key: String },
    Noop
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub command: Command
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
    Ok,
    NotLeader { known_leader: Option<NodeId> }
}

#[derive(Clone, Debug)]
pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub leader_state: Option<LeaderState>,
    pub known_leader: Option<NodeId>
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
                    insert_index += 1;
                }
                None => {
                    self.persistent.log.push(entry);
                    insert_index += 1;
                }
            }
        }
        
        let new_commit = std::cmp::min(req.leader_commit, self.last_log_index());
        if new_commit > self.volatile.commit_index {
            self.volatile.commit_index = new_commit;
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
}