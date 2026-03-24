use crate::raft::state::{
    AppendEntries, AppendEntriesResponse, ClientWriteRequest as StateClientWriteRequest, Command,
    LogEntry, RequestVote, RequestVoteResponse,
};
use crate::rpc::raft::{
    AppendEntriesReply, AppendEntriesRequest, ClientWriteReply as ProtoClientWriteReply,
    ClientWriteRequest as ProtoClientWriteRequest, ClientReadReply as ProtoClientReadReply,
    LogEntry as ProtoLogEntry, RequestVoteReply, RequestVoteRequest,
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

pub fn to_proto_append_entries(req: &AppendEntries) -> AppendEntriesRequest {
    AppendEntriesRequest {
        term: req.term,
        leader_id: req.leader_id,
        prev_log_index: req.prev_log_index,
        prev_log_term: req.prev_log_term,
        entries: req.entries.iter().map(to_proto_log_entry).collect(),
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

pub fn from_proto_client_write(
    req: ProtoClientWriteRequest,
) -> Result<StateClientWriteRequest, String> {
    let command = match req.op.as_str() {
        "put" => Command::Put {
            key: req.key,
            value: req.value,
        },
        "update" => Command::Update {
            key: req.key,
            value: req.value,
        },
        "delete" => Command::Delete { key: req.key },
        _ => return Err("op must be 'put', 'update', or 'delete'".to_string()),
    };

    Ok(StateClientWriteRequest {
        command,
        client_id: req.client_id,
        request_id: req.request_id,
    })
}

pub fn to_proto_client_write_reply(
    accepted: bool,
    term: u64,
    leader_id: Option<u64>,
    log_index: u64,
    commit_index: u64,
    message: String,
) -> ProtoClientWriteReply {
    ProtoClientWriteReply {
        accepted,
        term,
        leader_id: leader_id.unwrap_or(0),
        log_index,
        commit_index,
        message,
    }
}

pub fn to_proto_client_read_reply(
    success: bool,
    found: bool,
    value: String,
    term: u64,
    leader_id: Option<u64>,
    commit_index: u64,
    message: String,
) -> ProtoClientReadReply {
    ProtoClientReadReply {
        success,
        found,
        value,
        term,
        leader_id: leader_id.unwrap_or(0),
        commit_index,
        message,
    }
}

pub fn to_proto_log_entry(entry: &LogEntry) -> ProtoLogEntry {
    ProtoLogEntry {
        term: entry.term,
        command: encode_command(&entry.command),
        client_id: entry.client_id.unwrap_or(0),
        request_id: entry.request_id.unwrap_or(0),
    }
}

pub fn from_proto_log_entry(entry: ProtoLogEntry) -> LogEntry {
    LogEntry {
        term: entry.term,
        command: decode_command(&entry.command),
        client_id: if entry.client_id == 0 {
            None
        } else {
            Some(entry.client_id)
        },
        request_id: if entry.request_id == 0 {
            None
        } else {
            Some(entry.request_id)
        },
    }
}

fn encode_command(cmd: &Command) -> String {
    match cmd {
        Command::Noop => "noop".to_string(),
        Command::Put { key, value } => format!("put:{}:{}", key, value),
        Command::Update { key, value } => format!("upd:{}:{}", key, value),
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

    if let Some(rest) = raw.strip_prefix("upd:") {
        let mut parts = rest.splitn(2, ':');
        let key = parts.next().unwrap_or("").to_string();
        let value = parts.next().unwrap_or("").to_string();
        return Command::Update { key, value };
    }

    if let Some(rest) = raw.strip_prefix("del:") {
        return Command::Delete {
            key: rest.to_string(),
        };
    }

    Command::Noop
}
