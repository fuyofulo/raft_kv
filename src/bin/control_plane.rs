use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_stream::stream;
use axum::extract::{Path, State};
use axum::http::Method;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, broadcast};
use tower_http::cors::{Any, CorsLayer};

use raft_kv::raft::events::RaftEvent;
use raft_kv::rpc::raft::raft_rpc_client::RaftRpcClient;
use raft_kv::rpc::raft::{ClientReadRequest, ClientWriteRequest};

#[derive(Clone)]
struct AppState {
    node_enabled: Arc<RwLock<HashMap<u64, bool>>>,
    blocked_links: Arc<RwLock<HashSet<(u64, u64)>>>,
    events: Arc<RwLock<VecDeque<RaftEvent>>>,
    tx: broadcast::Sender<RaftEvent>,
}

#[derive(Debug, Serialize)]
struct EnabledReply {
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct AllowReply {
    allow: bool,
}

#[derive(Debug, Serialize)]
struct GenericReply {
    ok: bool,
    message: String,
}

#[derive(Debug, Serialize)]
struct ClusterStateReply {
    enabled_nodes: HashMap<u64, bool>,
    blocked_links: Vec<(u64, u64)>,
    events_count: usize,
}

#[derive(Debug, Deserialize)]
struct PartitionRequest {
    group_a: Vec<u64>,
    group_b: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct ClientCommandRequest {
    target_node: u64,
    op: String,
    key: String,
    value: Option<String>,
    client_id: Option<u64>,
    request_id: Option<u64>,
    host: Option<String>,
}

#[derive(Debug, Serialize)]
struct ClientCommandReply {
    ok: bool,
    message: String,
    target_node: u64,
    final_node: u64,
    response: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let addr: SocketAddr = if args.len() >= 2 {
        args[1].parse()?
    } else {
        "127.0.0.1:7000".parse()?
    };

    let (tx, _) = broadcast::channel(2048);
    let state = AppState {
        node_enabled: Arc::new(RwLock::new(HashMap::new())),
        blocked_links: Arc::new(RwLock::new(HashSet::new())),
        events: Arc::new(RwLock::new(VecDeque::new())),
        tx,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(health))
        .route("/events", post(post_event))
        .route("/events/history", get(events_history))
        .route("/events/stream", get(events_stream))
        .route("/nodes/:id/enabled", get(get_node_enabled))
        .route("/nodes/:id/start", post(start_node))
        .route("/nodes/:id/stop", post(stop_node))
        .route("/allow/:from/:to", get(get_allow))
        .route("/partition", post(apply_partition))
        .route("/heal", post(heal_network))
        .route("/client/command", post(client_command))
        .route("/state", get(get_state))
        .layer(cors)
        .with_state(state);

    println!("control-plane listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<GenericReply> {
    Json(GenericReply {
        ok: true,
        message: "ok".to_string(),
    })
}

async fn post_event(State(state): State<AppState>, Json(event): Json<RaftEvent>) -> Json<GenericReply> {
    {
        let mut events = state.events.write().await;
        if events.len() >= 5000 {
            events.pop_front();
        }
        events.push_back(event.clone());
    }
    {
        let mut enabled = state.node_enabled.write().await;
        enabled.entry(event.node_id).or_insert(true);
    }
    let _ = state.tx.send(event);
    Json(GenericReply {
        ok: true,
        message: "event accepted".to_string(),
    })
}

async fn events_history(State(state): State<AppState>) -> Json<Vec<RaftEvent>> {
    let events = state.events.read().await;
    Json(events.iter().cloned().collect())
}

async fn events_stream(
    State(state): State<AppState>,
) -> Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.tx.subscribe();
    let out = stream! {
        loop {
            let next = rx.recv().await;
            match next {
                Ok(event) => {
                    let payload = serde_json::to_string(&event).unwrap_or_else(|_| "{}".to_string());
                    yield Ok(Event::default().event("raft_event").data(payload));
                }
                Err(_) => {
                    break;
                }
            }
        }
    };
    Sse::new(out).keep_alive(KeepAlive::default())
}

async fn get_node_enabled(
    Path(id): Path<u64>,
    State(state): State<AppState>,
) -> Json<EnabledReply> {
    let enabled = {
        let map = state.node_enabled.read().await;
        map.get(&id).copied().unwrap_or(true)
    };
    Json(EnabledReply { enabled })
}

async fn start_node(
    Path(id): Path<u64>,
    State(state): State<AppState>,
) -> Json<GenericReply> {
    {
        let mut map = state.node_enabled.write().await;
        map.insert(id, true);
    }
    Json(GenericReply {
        ok: true,
        message: format!("node {} started", id),
    })
}

async fn stop_node(
    Path(id): Path<u64>,
    State(state): State<AppState>,
) -> Json<GenericReply> {
    {
        let mut map = state.node_enabled.write().await;
        map.insert(id, false);
    }
    Json(GenericReply {
        ok: true,
        message: format!("node {} stopped", id),
    })
}

async fn get_allow(
    Path((from, to)): Path<(u64, u64)>,
    State(state): State<AppState>,
) -> Json<AllowReply> {
    let blocked = {
        let links = state.blocked_links.read().await;
        links.contains(&(from, to))
    };
    Json(AllowReply { allow: !blocked })
}

async fn apply_partition(
    State(state): State<AppState>,
    Json(req): Json<PartitionRequest>,
) -> Json<GenericReply> {
    let set_a: HashSet<u64> = req.group_a.into_iter().collect();
    let set_b: HashSet<u64> = req.group_b.into_iter().collect();
    let mut links = state.blocked_links.write().await;
    links.clear();
    for a in &set_a {
        for b in &set_b {
            links.insert((*a, *b));
            links.insert((*b, *a));
        }
    }
    Json(GenericReply {
        ok: true,
        message: "partition applied".to_string(),
    })
}

async fn heal_network(State(state): State<AppState>) -> Json<GenericReply> {
    let mut links = state.blocked_links.write().await;
    links.clear();
    Json(GenericReply {
        ok: true,
        message: "network healed".to_string(),
    })
}

async fn client_command(
    Json(req): Json<ClientCommandRequest>,
) -> Json<ClientCommandReply> {
    let mut current_node = req.target_node;
    let host = req.host.unwrap_or_else(|| "127.0.0.1".to_string());
    let op = req.op.to_lowercase();
    let max_attempts = 3usize;

    for _ in 0..max_attempts {
        let addr = node_addr(&host, current_node);
        let connect = RaftRpcClient::connect(addr.clone()).await;
        let mut client = match connect {
            Ok(c) => c,
            Err(e) => {
                return Json(ClientCommandReply {
                    ok: false,
                    message: format!("connect failed to node {} at {}: {}", current_node, addr, e),
                    target_node: req.target_node,
                    final_node: current_node,
                    response: serde_json::json!({}),
                })
            }
        };

        if op == "get" {
            let rpc = client
                .client_read(ClientReadRequest {
                    key: req.key.clone(),
                })
                .await;
            let reply = match rpc {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    return Json(ClientCommandReply {
                        ok: false,
                        message: format!("client_read failed: {}", e),
                        target_node: req.target_node,
                        final_node: current_node,
                        response: serde_json::json!({}),
                    })
                }
            };

            let payload = serde_json::json!({
                "success": reply.success,
                "found": reply.found,
                "value": reply.value,
                "term": reply.term,
                "leader_id": reply.leader_id,
                "commit_index": reply.commit_index,
                "message": reply.message,
            });

            if !reply.success && reply.leader_id != 0 && reply.leader_id != current_node {
                current_node = reply.leader_id;
                continue;
            }

            return Json(ClientCommandReply {
                ok: reply.success,
                message: "client read completed".to_string(),
                target_node: req.target_node,
                final_node: current_node,
                response: payload,
            });
        }

        let request_id = req.request_id.unwrap_or_else(now_nanos_u64);
        let rpc = client
            .client_write(ClientWriteRequest {
                op: op.clone(),
                key: req.key.clone(),
                value: req.value.clone().unwrap_or_default(),
                client_id: req.client_id.unwrap_or(1),
                request_id,
            })
            .await;

        let reply = match rpc {
            Ok(r) => r.into_inner(),
            Err(e) => {
                return Json(ClientCommandReply {
                    ok: false,
                    message: format!("client_write failed: {}", e),
                    target_node: req.target_node,
                    final_node: current_node,
                    response: serde_json::json!({}),
                })
            }
        };

        let payload = serde_json::json!({
            "accepted": reply.accepted,
            "term": reply.term,
            "leader_id": reply.leader_id,
            "log_index": reply.log_index,
            "commit_index": reply.commit_index,
            "message": reply.message,
        });

        if !reply.accepted && reply.leader_id != 0 && reply.leader_id != current_node {
            current_node = reply.leader_id;
            continue;
        }

        return Json(ClientCommandReply {
            ok: reply.accepted,
            message: "client write completed".to_string(),
            target_node: req.target_node,
            final_node: current_node,
            response: payload,
        });
    }

    Json(ClientCommandReply {
        ok: false,
        message: "max retries exhausted".to_string(),
        target_node: req.target_node,
        final_node: current_node,
        response: serde_json::json!({}),
    })
}

async fn get_state(State(state): State<AppState>) -> Json<ClusterStateReply> {
    let enabled_nodes = state.node_enabled.read().await.clone();
    let blocked_links = state.blocked_links.read().await.iter().copied().collect();
    let events_count = state.events.read().await.len();
    Json(ClusterStateReply {
        enabled_nodes,
        blocked_links,
        events_count,
    })
}

fn node_addr(host: &str, node_id: u64) -> String {
    format!("http://{}:{}", host, 50050 + node_id)
}

fn now_nanos_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
