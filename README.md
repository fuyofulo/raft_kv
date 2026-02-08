# raft-kv

A small distributed key-value store built in Rust on top of the Raft consensus algorithm.

This project runs multiple Raft nodes over gRPC and supports leader election, log replication, and client operations through a CLI.

## Stack

- Rust
- Tokio (async runtime)
- Tonic + Prost (gRPC + protobuf)

## What it currently supports

- Automated Raft leader election
- Heartbeat-based leader maintenance
- Log replication to followers
- Commit and apply flow
- Client write operations:
  - `put`
  - `update`
  - `delete`
- Client read operation:
  - `get`
- Follower-to-leader redirect behavior in the client
- Request dedup/caching using `client_id` + `request_id` metadata stored in replicated log entries

## Project layout

- `src/bin/node.rs`: Raft node process (run one per node)
- `src/bin/client.rs`: CLI client for `put/update/delete/get`
- `src/raft/state.rs`: Core Raft state machine and protocol logic
- `src/rpc/server.rs`: gRPC service implementation
- `src/rpc/convert.rs`: Core <-> protobuf conversion helpers
- `src/proto/raft.proto`: protobuf API definition

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

## Notes

- Address mapping assumes node `N` listens on port `50050 + N`.
- This is a learning/engineering project and not production-hardened yet.
