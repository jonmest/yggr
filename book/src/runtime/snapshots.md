# Snapshots

Snapshots (§7) let a node discard the prefix of its log by capturing the state machine's state and the log position it reflects. The library handles the protocol end-to-end. Fast followers catch up via `AppendEntries`. Followers whose `nextIndex` has fallen below the snapshot floor catch up via `InstallSnapshot`.

## Two triggers

1. Host-initiated. Your application calls `Event::SnapshotTaken { last_included_index, bytes }`. In the default runtime this happens automatically via `Action::SnapshotHint` when enough entries have applied past the current floor.
2. Follower catch-up. A leader whose peer's `nextIndex <= snapshot_floor` sends `InstallSnapshot` instead of `AppendEntries`. Chunks are `Config::snapshot_chunk_size_bytes` bytes each.

## Auto-compaction hints

The engine emits `Action::SnapshotHint { last_included_index }` in two cases:

1. Applied-entries band — every time the applied-entries count past the current floor crosses `Config::snapshot_hint_threshold_entries`. Set to `0` to disable.
2. Live-log guardrail — whenever entries above the floor exceed `Config::max_log_entries` (disk-space backstop for a stuck apply path). Set to `0` to disable.

The default runtime reacts by calling `StateMachine::snapshot()` on its own task and feeding the bytes back via `Event::SnapshotTaken`. The driver does not block: `status()`, ticks, heartbeats, and inbound RPCs stay responsive even when `snapshot()` takes seconds to run. Overlapping hints during an in-flight snapshot are coalesced; the engine re-hints after the next threshold crossing.

## Fallibility

`StateMachine::snapshot` returns `Result<Vec<u8>, SnapshotError>`. Return `Err` for transient failures (ENOSPC, backpressure); the runtime logs and drops this attempt. The engine re-hints when the next threshold crossing fires, so the caller gets automatic retry without any bookkeeping.

## Compression

The library treats snapshot bytes as opaque. Compress inside `StateMachine::snapshot` and decompress inside `restore`:

```rust
fn snapshot(&self) -> Result<Vec<u8>, SnapshotError> {
    zstd::encode_all(&self.serialized[..], 3)
        .map_err(|e| SnapshotError::new(e.to_string()))
}
fn restore(&mut self, bytes: Vec<u8>) {
    let decoded = zstd::decode_all(&bytes[..]).unwrap();
    self.deserialize(&decoded);
}
```

## Membership and snapshots

A snapshot carries the cluster membership as of `last_included_index`. Without this, committed `AddPeer` / `RemovePeer` entries that got snapshotted would be lost on restart, and the node would compute the wrong majority. The runtime stores the peer list alongside the snapshot bytes via `StoredSnapshot::peers`.
