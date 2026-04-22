# Writes

A write submits a command to the cluster. When it returns, the command has committed on a majority and applied on the local leader.

```rust
let response = node.write(CountCmd::Inc(5)).await?;
```

`write()` is the preferred name. `propose()` is kept as an alias for the 0.1 migration window — same semantics, same errors.

## Contract

When `write(cmd).await?` returns `Ok(response)`:

- `cmd` is the next Raft log entry at some committed index `i`.
- `state_machine.apply(cmd)` has run on the local node; `response` is what it returned.
- Every node in the cluster will replay `cmd` at index `i` on recovery.
- `commit_index >= i` on the leader; followers converge as replication completes.

Only a leader accepts a write. Followers reject with `NotLeader { leader_hint }` when they know who the leader is, `NoLeader` otherwise.

## What a write does not guarantee

- **Not a read barrier.** Reads issued before the write may see pre-write state. Use [`read_linearizable`](./read-index.md) if a subsequent read must observe the write.
- **Not exactly-once on retry.** If a write errors with `Shutdown` or `DriverDead`, the command may or may not have been appended. Your application is responsible for idempotency if this matters — embed a client token in the command and have the state machine de-duplicate.
- **Not exactly-once across failover.** A proposal submitted to a leader that loses leadership mid-flight surfaces `NotLeader`; the entry may still have been replicated and committed by the new leader. Same idempotency advice applies.

## Batching

Multiple concurrent `write()` calls are interleaved by the driver and, with batching enabled, packed into a single leader broadcast:

```rust
let mut config = Config::new(my_id, peers);
config.max_batch_delay_ticks = 1;  // hold for up to 1 tick
config.max_batch_entries = 64;     // flush sooner if the buffer fills
```

Off by default. Enable it when you have many concurrent producers — N writers can commit in one fsync + one broadcast.

Each caller still gets their own response when their entry applies. Batching is transparent to the contract above.

## Errors

| Error | What happened | Caller action |
|---|---|---|
| `NotLeader { leader_hint }` | A follower with a known leader. | Retry against `leader_hint`. |
| `NoLeader` | Follower without a leader, or candidate/pre-candidate. | Back off and retry later. |
| `Busy` | Too many proposals already in flight. | Back off and retry. |
| `Shutdown` | The runtime is stopping. | Stop calling. |
| `DriverDead` | The driver task exited without a clean shutdown. | Fatal; restart the process. |
| `Fatal` | A non-recoverable storage or transport fault. | Fatal; restart the process. |

`write_batch` (future) follows the same error shape with all-or-error semantics.

## Best practices

- **Idempotency keys in the command.** Even a mid-flight crash can double-apply if your retry logic isn't careful; bake de-duplication into the state machine.
- **Keep commands small and deterministic.** The engine treats commands as opaque bytes; the state machine runs `apply` on every node. Non-determinism in `apply` diverges replicas silently.
- **Don't block `apply()`.** A slow apply stalls every subsequent write. Push heavy work downstream of apply; the engine's apply task already runs concurrently with the driver.
