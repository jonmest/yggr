# jotun

Jotun is a Rust Raft library split into a pure consensus engine, a deterministic simulation harness, a tokio runtime, and a small replicated KV example.

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
- chunked snapshot transfer plus runtime auto-compaction hints
- segmented on-disk log storage
- protobuf wire format and a TCP transport
- deterministic simulation with safety checks
- linearizable reads via `Node::read_linearizable` (Raft §8 `ReadIndex`)
- leadership transfer via `Node::transfer_leadership_to` (`TimeoutNow` RPC)
- async state-machine apply — slow `apply` no longer stalls heartbeats
- opt-in leader proposal batching (`Config::max_batch_delay_ticks`)
- runtime APIs for bootstrap mode selection, proposal backpressure, status inspection, and graceful shutdown

Compression is a host-side concern: compress inside `StateMachine::snapshot`, decompress inside `StateMachine::restore`. jotun treats the bytes as opaque through the engine, disk, and wire.

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
