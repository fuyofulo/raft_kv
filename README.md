# raft-kv

A small distributed key-value store built in Rust on top of the Raft consensus algorithm.

This project runs multiple Raft nodes over gRPC and supports leader election, log replication, and client operations through a CLI.

## Stack

- Rust
- Tokio (async runtime)
- Tonic + Prost (gRPC + protobuf)
- Serde + serde_json (durable on-disk state)

## What it currently supports

- Automated Raft leader election
- Heartbeat-based leader maintenance
- Log replication to followers
- Commit and apply flow
- Durable node state on disk (log + term/vote + applied state)
- Client write operations:
  - `put`
  - `update`
  - `delete`
- Client read operation:
  - `get`
- Follower-to-leader redirect behavior in the client
- Request dedup/caching using `client_id` + `request_id` metadata stored in replicated log entries
- Write response contract: success means committed and applied (or duplicate served from cache)
- Control-plane backend (in progress) for:
  - live Raft event collection
  - node start/stop toggles
  - network partition/heal rules

## Project layout

- `src/bin/node.rs`: Raft node process (run one per node)
- `src/bin/client.rs`: CLI client for `put/update/delete/get`
- `src/raft/state.rs`: Core Raft state machine and protocol logic
- `src/raft/storage.rs`: Durable state load/save helpers
- `src/raft/events.rs`: Raft event model + node-local event emitter
- `src/rpc/server.rs`: gRPC service implementation
- `src/rpc/convert.rs`: Core <-> protobuf conversion helpers
- `src/proto/raft.proto`: protobuf API definition
- `src/bin/control_plane.rs`: HTTP control plane + event stream
- `ui/`: Vite + React terminal dashboard

## Run

From project root:

```bash
cargo build
```

Start 5 nodes (use separate terminals):

```bash
cargo run --bin node -- 1 127.0.0.1:50051 2,3,4,5
cargo run --bin node -- 2 127.0.0.1:50052 1,3,4,5
cargo run --bin node -- 3 127.0.0.1:50053 1,2,4,5
cargo run --bin node -- 4 127.0.0.1:50054 1,2,3,5
cargo run --bin node -- 5 127.0.0.1:50055 1,2,3,4
```

Each node stores state in `data/node-<id>.json` by default.

Optional custom data path:

```bash
cargo run --bin node -- 1 127.0.0.1:50051 2,3,4,5 data/custom-node-1.json
```

Run node with control-plane URL:

```bash
cargo run --bin node -- 1 127.0.0.1:50051 2,3,4,5 data/node-1.json http://127.0.0.1:7000
```

or set:

```bash
export CONTROL_PLANE_URL=http://127.0.0.1:7000
```

Run client commands:

```bash
cargo run --bin client -- 127.0.0.1:50054 put x 100
cargo run --bin client -- 127.0.0.1:50055 update x 200
cargo run --bin client -- 127.0.0.1:50052 get x
cargo run --bin client -- 127.0.0.1:50051 delete x
```

Optional explicit idempotency fields:

```bash
cargo run --bin client -- 127.0.0.1:50051 put x 100 42 9001
```

## Write semantics

- `accepted=true` means the write entry reached commit and apply on the leader before reply.
- If the node loses leadership or times out before commit/apply, the response is `accepted=false`.
- Duplicate client requests (`client_id`, `request_id`) are served from dedup cache.

## Durability

- Node state is persisted as JSON per node.
- Restarting a node restores:
  - Raft persistent state (`current_term`, `voted_for`, `log`)
  - `commit_index`, `last_applied`
  - applied key/value state
  - dedup cache

## Control Plane (in progress)

Start control plane:

```bash
cargo run --bin control_plane -- 127.0.0.1:7000
```

Useful endpoints:

- `GET /state`
- `GET /events/history`
- `GET /events/stream` (SSE)
- `POST /nodes/{id}/stop`
- `POST /nodes/{id}/start`
- `POST /partition` with body: `{"group_a":[1,2], "group_b":[3,4,5]}`
- `POST /heal`
- `POST /client/command`

## UI Dashboard

The UI is a retro terminal-style dashboard for:

- viewing all 5 nodes at once
- live event/log feed
- start/stop node controls
- partition/heal controls
- client command panel (`put/update/delete/get`)

Run:

```bash
cd ui
npm install
npm run dev
```

Default UI URL: `http://127.0.0.1:5173`  
Default control-plane URL used by UI: `http://127.0.0.1:7000`

Optional override:

```bash
VITE_API_BASE=http://127.0.0.1:7000 npm run dev
```

## Notes

- Address mapping assumes node `N` listens on port `50050 + N`.
- This is a learning/engineering project and not production-hardened yet.
