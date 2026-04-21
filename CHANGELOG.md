# Changelog

All notable changes to this project will be documented here.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows [SemVer](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-04-21

Initial public release of `yggr` (Raft runtime) and `yggr-core` (protocol engine).

### Protocol

- Leader election, log replication, and single-server membership changes (§4.3).
- §5.4.2 leader-no-op on election so prior-term entries become committable.
- Chunked `InstallSnapshot` for followers whose `nextIndex` falls below the leader's log floor.
- Leadership transfer via `TimeoutNow`.
- Linearizable reads via `ReadIndex` (§8).
- **§9.6 Pre-vote**, on by default. A disrupted follower probes peers before bumping its term, so flapping nodes don't force the cluster to step down on every reconnect.
- **§9 Leader-lease fast path**, opt-in via `Config::lease_duration_ticks`. When the leader has received a majority AE ack within the lease window, linearizable reads return immediately — no heartbeat round-trip required. `Config::validate` enforces the required `lease < election_timeout_min - heartbeat_interval` constraint.

### Runtime (`yggr`)

- Tokio-based `Node` with a dedicated driver task, apply task, transport task, and periodic tick.
- TCP transport with length-prefixed protobuf wire format.
- Segmented on-disk log with atomic `*.tmp` + rename writes.
- Crash-mid-rename cleanup: `DiskStorage::open()` sweeps orphaned `.tmp` files on startup.
- **Non-blocking snapshot creation**: a slow `StateMachine::snapshot()` runs on its own task and streams back via `DriverInput::SnapshotReady`, so ticks, heartbeats, inbound RPCs, and `status()` / `metrics()` stay responsive.
- **Fallible `StateMachine::snapshot`** — returns `Result<Vec<u8>, SnapshotError>`. An `Err` is logged; the engine re-hints on the next threshold crossing.
- **Disk-space guardrails**: `Config::max_log_entries` forces a `SnapshotHint` once the live log exceeds the threshold, independent of apply progress.
- Opt-in leader proposal batching (`max_batch_delay_ticks`, `max_batch_entries`).
- Graceful shutdown flushes in-flight proposals before tearing down the transport.

### Observability

- **`Node::metrics()` / `Engine::metrics()`** — pull-model `EngineMetrics` with counters for elections, votes, pre-votes, AEs, commits, applies, reads, snapshots, and higher-term stepdowns; gauges for current term, commit index, last-applied, log length, and role code. `#[non_exhaustive]` so new fields are additive.
- Structured `tracing` spans and events with stable targets (`yggr::engine`, `yggr::node`) and field names. OpenTelemetry wiring lives in the consumer's `main`, not as a library dep.

### Engine (`yggr-core`)

- One type, `Engine<C>`; one method, `step(Event<C>) -> Vec<Action<C>>`. No sockets, no disk, no async.
- `Env` trait for election-timeout randomization; `StaticEnv` for deterministic tests.
- `RoleState::{Follower, PreCandidate, Candidate, Leader}` enum with per-role state payloads.

### Simulation and testing

- Deterministic chaos simulator (`yggr-sim`, unpublished): drops, reorderings, partitions, crashes, partial fsync. Election Safety, Log Matching, Leader Completeness, and State Machine Safety asserted after every step.
- In-process runtime chaos harness: real `Node` instances connected through a chaos transport, same invariants checked at the full-stack level.
- Fixed a sim-internal Persist→Send ordering bug that could let a peer observe a vote the sender still hadn't durably recorded.
- `cargo-fuzz` targets for the wire codec, engine events, and disk recovery.
- `cargo-mutants` smoke on PRs, full scheduled sweeps on the correctness-critical crates.

### Documentation

- mdBook guide at `book/src/` covering engine, runtime, and sim.
- Rustdoc for all public types. Top-level crate docs describe the engine / runtime split.

[0.1.0]: https://github.com/jonmest/yggr/releases/tag/v0.1.0
