# jotun

A Raft library in Rust. Four crates: a pure protocol engine, a deterministic simulator, a tokio runtime, and an example KV service.

[Guide](https://jonmest.github.io/jotun/guide/) &middot; [API docs](https://jonmest.github.io/jotun/api/jotun/)

```rust
use jotun::{Config, Node, DiskStorage, TcpTransport};

let config    = Config::new(my_id, peer_ids);
let storage   = DiskStorage::open(&data_dir).await?;
let transport = TcpTransport::start(my_id, listen_addr, peer_addrs).await?;
let node      = Node::start(config, MyStateMachine::default(), storage, transport).await?;

// Replicate a command. Returns when it has committed and applied.
let response = node.propose(MyCmd::Inc(5)).await?;

// Linearizable read (ReadIndex). No log append, no fsync.
let value = node.read_linearizable(|sm: &MyStateMachine| sm.value).await?;
```

## Crates

- `jotun-core` is the protocol. One type, `Engine<C>`, one method: `step(Event<C>) -> Vec<Action<C>>`. No sockets, no disk, no async.
- `jotun-sim` is a deterministic cluster simulator. It drives drops, reorderings, partitions, crashes, and partial fsync against the engine and checks safety invariants after every step.
- `jotun` is the tokio runtime. It provides `Node`, `DiskStorage`, `TcpTransport`, and a length-prefixed protobuf wire format.
- `jotun-examples` has a three-node replicated KV service.

The split exists so the engine is usable without the runtime, and so the simulator can run the engine deterministically without tokio or real I/O.

## What's implemented

- Leader election, log replication, single-server membership changes (§4.3)
- §9.6 Pre-vote, on by default — flapping nodes can't force the rest of the cluster to step down
- Linearizable reads via ReadIndex (§8), with an opt-in §9 leader-lease fast path that skips the heartbeat round when the leader has a fresh majority ack
- Leadership transfer via `TimeoutNow`
- Snapshotting: chunked `InstallSnapshot`, applied-entries and live-log guardrails (`max_log_entries`), and non-blocking `snapshot()` — a slow serializer cannot stall the driver
- Fallible `StateMachine::snapshot` so transient disk-space errors retry cleanly
- Segmented on-disk log with atomic file writes and crash-mid-rename cleanup
- State machine apply on its own task; slow `apply()` does not stall heartbeats
- Opt-in proposal batching on the leader
- Pull-model metrics via `Node::metrics()` — counters for elections, replication, reads, and snapshots plus the usual gauges
- Structured `tracing` spans and events with stable field names. OpenTelemetry wiring is a few lines in your `main`; see the [observability guide](https://jonmest.github.io/jotun/guide/runtime/observability.html).

## Quick start

Requires Rust 1.85+ and `protoc`.

```bash
# Three-node KV demo.
./jotun-examples/run-three-node.sh

# Workspace checks.
just test
just clippy
just coverage
just fuzz-check
```

## Testing

The library is tested in four layers.

1. Unit tests. Pure-engine tests covering every `Event`/`Action` path.
2. Property tests. 1024-case proptests on term and commit monotonicity, vote uniqueness, and recover idempotence. Scheduled CI dials the engine invariant cases up further. Wire-format decoders are fuzzed through `Message::try_from` for arbitrary bytes.
3. Sim chaos. 128 seeds, 1500 steps, on 3-, 5-, and 7-node clusters, under drops, reorderings, partitions, crashes, and partial fsync. Election Safety, Log Matching, Leader Completeness, and State Machine Safety are checked after every step.
4. Runtime chaos. Real `Node` instances (driver task, apply task, storage) connected through an in-process chaos transport. PRs run a 16-case smoke sweep; scheduled CI runs a heavier seeded sweep. The same safety invariants are asserted at the full-stack level.

CI also enforces a workspace line-coverage floor, runs diff-scoped `cargo-mutants` smoke on PRs with full scheduled sweeps on the correctness-critical crates, and keeps scheduled `cargo-fuzz` corpus-building jobs for the wire codec, the engine, and disk recovery.

## Status

Pre-1.0. The public API is stable enough to build on, but expect changes.

## License

MIT. See [LICENSE.md](./LICENSE.md).
