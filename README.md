# jotun

Jotun is a Rust Raft library split into a pure consensus engine, a deterministic simulation harness, a tokio runtime, and a small replicated KV example.

The workspace is aimed at two kinds of users:

- people who want a reusable Raft state machine with no I/O baked in
- people who want to stand up a practical replicated service on tokio without writing the engine/runtime glue themselves

## Crates

- `jotun-core`
  Pure Raft engine. No network, disk, or async runtime assumptions. The main API is `Engine::step(Event<C>) -> Vec<Action<C>>`.
- `jotun-sim`
  Deterministic cluster simulator for `jotun-core`. It drives crash/recover, partitions, drops, reorderings, partial flushes, and snapshots, then checks Raft safety invariants after every step.
- `jotun`
  Batteries-included runtime. Ships `Node`, `Storage`, `Transport`, `DiskStorage`, and `TcpTransport`.
- `jotun-examples`
  A small replicated KV server built on `jotun`, plus `run-three-node.sh` for local bring-up.

## What Ships Today

- leader election, log replication, crash recovery, and membership changes
- snapshot install/restore in the engine and runtime
- segmented on-disk log storage
- protobuf wire format and a TCP transport
- deterministic simulation with safety checks
- runtime APIs for bootstrap mode selection, proposal backpressure, status inspection, and graceful shutdown

## Current Caveats

- no linearizable read helper / `ReadIndex` API yet
- no leadership-transfer API yet
- snapshot creation in the `jotun` runtime is still application-driven
- snapshot transfer is not chunked yet; large snapshots still need follow-up work
- proposal batching and async state-machine apply are queued, not implemented

## Quick Start

Run the example cluster:

```bash
./jotun-examples/run-three-node.sh
```

Or run the workspace tests directly:

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

## Layout

```text
jotun-core/      pure consensus engine
jotun-sim/       deterministic simulator + invariants
jotun/           tokio runtime, storage, transport
jotun-examples/  replicated KV demo
```

## License

MIT
