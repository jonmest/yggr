# Errors

Every fallible runtime method returns a typed error. The types are narrow on purpose — each variant maps to one caller action.

## Write / propose errors

```rust
pub enum ProposeError {
    NotLeader { leader_hint: NodeId },
    NoLeader,
    Busy,
    Shutdown,
    DriverDead,
    Fatal,
}
```

| Variant | Meaning | What to do |
|---|---|---|
| `NotLeader` | A follower that knows who the leader is. | Retry against `leader_hint`. |
| `NoLeader` | Follower without a current leader, or pre-candidate / candidate. | Back off, retry. |
| `Busy` | `max_pending_proposals` reached. | Back off, retry. |
| `Shutdown` | The runtime is stopping. | Stop calling. |
| `DriverDead` | Driver task exited unexpectedly. | Fatal, restart process. |
| `Fatal` | Unrecoverable storage / transport fault. | Fatal, restart process. |

## Read errors

```rust
pub enum ReadError {
    NotLeader { leader_hint: NodeId },
    NoLeader,
    NotReady,
    SteppedDown,
    Shutdown,
    DriverDead,
    Fatal,
}
```

Beyond the write-error shape, reads add:

- `NotReady` — the leader hasn't committed an entry in its current term (§5.4.2 no-op hasn't applied yet). Retry in a few ms.
- `SteppedDown` — leadership lost between when the read was queued and when it would have served. Retry on the new leader.

## Transfer leadership errors

```rust
pub enum TransferLeadershipError {
    NotLeader { leader_hint: NodeId },
    NoLeader,
    InvalidTarget,
    Shutdown,
    DriverDead,
    Fatal,
}
```

`InvalidTarget` — the target isn't a voter, or is self, or doesn't exist. Non-retriable without changing the target.

## Status / metrics errors

Status and metrics are informational; they shouldn't get mixed up with write-oriented error variants. Future: a narrower `StatusError { Shutdown | DriverDead | Fatal }`.

## Start-up errors

```rust
pub enum NodeStartError<E> {
    Config(ConfigError),
    Storage(E),
}
```

`ConfigError` is an enum of pre-flight validation failures (invalid timeout range, peer contains self, lease too large, etc.). `Storage(E)` is whatever error type your `Storage` impl surfaces — typically `io::Error` for `DiskStorage`.

## Membership errors (planned)

Once the admin handle exposes richer semantics, membership calls will return:

```rust
pub enum MembershipError {
    NotLeader { leader_hint: NodeId },
    NoLeader,
    Busy,
    ChangeInProgress,
    UnknownNode(NodeId),
    AlreadyVoter(NodeId),
    AlreadyLearner(NodeId),
    NotLearner(NodeId),
    InvalidTargetMembership,
    Shutdown,
    DriverDead,
    Fatal,
}
```

Today `add_peer` / `remove_peer` / `add_learner` / `promote_learner` return `ProposeError`. The richer enum arrives when membership becomes a first-class surface rather than a thin forward over `propose`.

## How to retry

The general pattern:

```rust
loop {
    match node.write(cmd.clone()).await {
        Ok(response) => return Ok(response),
        Err(ProposeError::NotLeader { leader_hint }) => {
            // Redirect to leader_hint and try there (your transport-level concern).
            return Err("redirect");
        }
        Err(ProposeError::NoLeader) | Err(ProposeError::Busy) => {
            tokio::time::sleep(Duration::from_millis(50)).await;
            // and retry
        }
        Err(e) => return Err(e),  // Shutdown / DriverDead / Fatal are terminal
    }
}
```

Never retry indefinitely without a ceiling. If `Busy` persists, the system is backpressured and retrying harder makes it worse.

## Logging

Error variants are `#[non_exhaustive]`, so downstream matches should always include a `_ => ...` arm to stay forward-compatible with new variants.
