use crate::raft::state::{
    AppendEntries, AppendEntriesResponse, Command, LogEntry, RequestVote, RequestVoteResponse,
};
use crate::rpc::raft::{
    AppendEntriesReply, AppendEntriesRequest, LogEntry as ProtoLogEntry, RequestVoteReply,
    RequestVoteRequest,
};

pub fn to_proto_request_vote(req: &RequestVote) -> RequestVoteRequest {
    RequestVoteRequest {
        term: req.term,
        candidate_id: req.candidate_id,
        last_log_index: req.last_log_index,
        last_log_term: req.last_log_term,
    }
}

pub fn from_proto_request_vote(req: RequestVoteRequest) -> RequestVote {
    RequestVote {
        term: req.term,
        candidate_id: req.candidate_id,
        last_log_index: req.last_log_index,
        last_log_term: req.last_log_term,
    }
}

pub fn to_proto_request_vote_reply(resp: &RequestVoteResponse) -> RequestVoteReply {
    RequestVoteReply {
        term: resp.term,
        vote_granted: resp.vote_granted,
    }
}

pub fn from_proto_append_entries(req: AppendEntriesRequest) -> AppendEntries {
    AppendEntries {
        term: req.term,
        leader_id: req.leader_id,
        prev_log_index: req.prev_log_index,
        prev_log_term: req.prev_log_term,
        entries: req.entries.into_iter().map(from_proto_log_entry).collect(),
        leader_commit: req.leader_commit,
    }
}

pub fn to_proto_append_entries_reply(resp: &AppendEntriesResponse) -> AppendEntriesReply {
    AppendEntriesReply {
        term: resp.term,
        success: resp.success,
        match_index: resp.match_index,
    }
}

pub fn to_proto_log_entry(entry: &LogEntry) -> ProtoLogEntry {
    ProtoLogEntry {
        term: entry.term,
        command: encode_command(&entry.command),
    }
}

pub fn from_proto_log_entry(entry: ProtoLogEntry) -> LogEntry {
    LogEntry {
        term: entry.term,
        command: decode_command(&entry.command),
    }
}

fn encode_command(cmd: &Command) -> String {
    match cmd {
        Command::Noop => "noop".to_string(),
        Command::Put { key, value } => format!("put:{}:{}", key, value),
        Command::Delete { key } => format!("del:{}", key),
    }
}

fn decode_command(raw: &str) -> Command {
    if raw == "noop" {
        return Command::Noop;
    }

    if let Some(rest) = raw.strip_prefix("put:") {
        let mut parts = rest.splitn(2, ':');
        let key = parts.next().unwrap_or("").to_string();
        let value = parts.next().unwrap_or("").to_string();
        return Command::Put { key, value };
    }

    if let Some(rest) = raw.strip_prefix("del:") {
        return Command::Delete {
            key: rest.to_string(),
        };
    }

    // Temporary fallback; we'll harden this later.
    Command::Noop
}
