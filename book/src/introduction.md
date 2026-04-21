# Introduction

yggr is a Raft implementation in Rust. It is split into four crates:

| crate | what it is |
|---|---|
| `yggr-core` | The protocol. `Engine<C>` with one method, `step(Event<C>) -> Vec<Action<C>>`. No sockets, no disk, no async. |
| `yggr-sim` | A deterministic cluster simulator. Drives crashes, partitions, drops, reorderings, and partial flushes against `yggr-core` and checks safety invariants after every step. |
| `yggr` | The tokio runtime. `Node`, `DiskStorage`, `TcpTransport`. |
| `yggr-examples` | A three-node replicated KV service. |

The split exists so the engine can be embedded in a non-tokio runtime (or driven by a custom transport or storage) without dragging in the runtime, and so the simulator can run the engine deterministically.

## What's implemented

- §5.2 leader election with randomized timeouts
- §5.3 log replication via `AppendEntries`
- §5.4.1 election restriction
- §5.4.2 current-term commit rule (election no-op)
- §4.3 single-server membership changes
- §7 snapshotting with chunked `InstallSnapshot`
- §8 linearizable reads via `ReadIndex`
- Leadership transfer via `TimeoutNow`
- Async apply (slow `apply()` does not stall heartbeats)
- Opt-in proposal batching
- Protobuf wire format, TCP transport
- Segmented on-disk log with atomic file writes

## Who this is for

- You want a Raft-backed Rust service and would prefer to not write the engine, transport, and storage plumbing yourself. Use `yggr`.
- You already have an async runtime, a transport, or a storage layer you want to reuse. Use `yggr-core` directly; see [Writing a custom host](./engine/custom-host.md).
- You want to test your own changes to the engine against adversarial schedules. Use `yggr-sim` as a test library.

## Status

Pre-1.0. The public API is stable enough to build on, but expect changes. Safety invariants are checked in the simulator (many chaos seeds per CI run) and at the full-stack level under real tokio tasks with an in-process chaos transport (see [The sim harness](./sim/overview.md)).
