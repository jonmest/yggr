# Linearizable reads

`Node::read_linearizable` runs a closure against the state machine at a point where the read sees every committed write. No log append, no fsync. This is §8 ReadIndex.

```rust
let value = node.read_linearizable(|sm: &MyState| sm.value).await?;
```

## Protocol

1. The leader records `commit_index` as the read's `read_index`.
2. The leader triggers a heartbeat round. When a majority of peers ack, the leader knows it was still authoritative as of `read_index`.
3. When `last_applied >= read_index` on the leader, the closure runs.

## Leader-lease fast path (§9)

With `Config::lease_duration_ticks > 0`, the leader may skip the heartbeat round. If it has received a majority AE ack within the last `lease_duration_ticks`, no other leader can have been elected during that window (§9 proof), so the current `commit_index` is still authoritative. `read_linearizable` returns as soon as `last_applied >= commit_index`.

Safety constraint: `lease_duration_ticks < election_timeout_min_ticks - heartbeat_interval_ticks`. `Config::validate` rejects violations. Lease off (`0`, the default) always uses the classic protocol.

## Errors

- `NotLeader { leader_hint }` — retry against `leader_hint`.
- `NotReady` — the leader hasn't committed an entry in its current term yet (the §5.4.2 no-op takes a round to commit after election). Retry in a moment.
- `SteppedDown` — the leader lost its role before the read could serve.

## Why a closure

The state machine lives on a dedicated apply task. The closure is shipped to that task and runs in FIFO order with applies, so the read observes post-apply state, not a partially-applied view.
