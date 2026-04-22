# Operations

Running a yggr cluster in anger: bring-up, rolling upgrades, graceful shutdown, failure recovery.

## Bring-up

See [Bootstrap modes](./bootstrap.md) for the full decision tree. Short version:

- **First boot of a new cluster.** Every node starts with `Bootstrap::NewCluster { initial_peers }`. Exactly one bootstrap per cluster lifetime.
- **Subsequent boots.** Always `Bootstrap::Recover` (the default). The node reloads its persisted state and catches up via replication.

Do not re-bootstrap a cluster that already has state. It creates a second history at term 0 and diverges from every other node.

## Rolling upgrades

A rolling upgrade replaces binaries one node at a time without downtime. The recipe:

1. Pick a node to upgrade (ideally a follower; use `transfer_leadership` to move leadership away if needed).
2. Drain: call `node.shutdown().await`. The driver flushes in-flight proposals and stops cleanly.
3. Swap the binary. On-disk state (log segments, snapshots, hard state) is compatible across patch versions.
4. Start the new binary with the same `Config` and `Bootstrap::Recover`.
5. Wait until `node.status().membership.voters` matches every peer and `status.leader` is stable.
6. Repeat for each node.

Always upgrade voters one at a time. Upgrading two at once can leave the cluster without a quorum of voters on the new version.

## Graceful shutdown

```rust
node.shutdown().await?;
```

This:

- rejects new `write()` calls with `Shutdown`,
- drains proposals already accepted into the driver,
- stops the transport,
- joins the tick / apply / driver tasks,
- returns when everything has stopped.

If the caller holds multiple clones of `Node`, only the call on the last clone that hasn't been cloned further actually runs the drain — `shutdown(self)` consumes its receiver. Other clones get `Shutdown` or `DriverDead` from subsequent calls.

## Triggering a snapshot

Snapshots are automatic when `Config::snapshot_hint_threshold_entries` or `max_log_entries` fires. To force one:

```rust
// Planned:  admin.trigger_snapshot().await?;
```

Today the engine's hint is observed by the driver; there's no public "snapshot now" button. If you need one, either lower the threshold or enqueue enough writes to trip it.

## Handling leader loss

Expected during normal operation: election timeouts, transient partitions, node restarts. The runtime handles it — clients see `NotLeader` or `NoLeader` briefly, retry logic kicks in, traffic resumes on the new leader.

If a loss persists more than an election-timeout cycle, check:

- `status.role` on each node — is anyone a candidate or pre-candidate stuck?
- `status.current_term` — is the term diverging across nodes? (Unusual; expect it to converge upward.)
- transport connectivity between voters — a partition between any two voters can deadlock election if the partition touches the majority.

## Disk-space management

Two knobs on `Config`:

- `snapshot_hint_threshold_entries` — compact when applied-entries past the floor crosses this. Default 1024.
- `max_log_entries` — compact when the live log (entries above the floor) crosses this, regardless of apply progress. Default 0 (off). Set it if you see slow or stuck apply producing unbounded log growth.

`DiskStorage` stores one `snapshot.bin` + `snapshot_meta.bin` at the data-dir root, plus segmented log files in `log/`. Crash-safe: atomic-rename writes for the snapshot, append-only segments for the log. `.tmp` files left by a crash mid-rename are swept on `DiskStorage::open`.

## Capacity planning

Per-node disk at steady state: snapshot bytes + live log bytes + overhead. Live log is bounded by `snapshot_hint_threshold_entries × average-entry-size` plus in-flight replication. The snapshot is whatever your `StateMachine::snapshot()` produces.

Per-node memory: engine state (small), applied-but-not-yet-responded proposals (bounded by `max_pending_proposals`), apply queue (bounded by `max_pending_applies`), plus your state machine.

Per-node CPU: dominated by the state machine's `apply()`. The engine itself is cheap.

## Observability

- `node.status()` — point-in-time view of role, term, commit, last-applied, leader, membership.
- `node.metrics()` — raw engine counters (elections, AEs, commits, reads, snapshots).
- `node.node_metrics()` — runtime-facing wrapper: engine metrics plus membership and status fields.
- `tracing` — structured events under targets `yggr::engine` and `yggr::node`. See [Observability](./observability.md).

Wire these into your monitoring system at cluster bring-up. Cross-cluster visibility into `current_term`, `commit_index`, and `leader` catches most operational issues before they become outages.

## Backups

A snapshot file plus the live log segments at a point in time is a complete backup of one node. For cluster backups:

1. Take a snapshot on each node (wait for `snapshots_installed` to tick).
2. Copy `snapshot.bin`, `snapshot_meta.bin`, and `log/` to durable off-cluster storage.
3. Include the node's `Config` (peers, node_id) alongside — recovery needs it.

A single node's backup is sufficient to rebuild the cluster: restore that node, re-bootstrap the others via `add_learner` + snapshot install, promote.
