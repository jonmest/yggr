# The Node API

`Node<S>` is the handle to a running node. Cloning it is cheap; every clone shares the driver task.

## Starting

```rust
let node = Node::start(config, state_machine, storage, transport).await?;
```

`Node::start` recovers persisted state, builds the engine, and spawns the driver, ticker, and apply tasks. See [Configuration](./config.md) and [Bootstrap modes](./bootstrap.md).

## Methods

| Method | Behavior |
|---|---|
| `write(cmd)` | Preferred client-write API. Returns when the command commits and applies. |
| `propose(cmd)` | Compatibility alias for `write(cmd)`. |
| `add_peer(id)` / `remove_peer(id)` | §4.3 single-server membership change. |
| `read_linearizable(closure)` | Run `closure` against the state machine at a linearizable read point (ReadIndex, §8). |
| `admin()` | Return an operator-facing handle for membership and lifecycle operations. |
| `current_leader()` | Return the current leader hint, if known. |
| `transfer_leadership_to(peer)` | Hand leadership to a follower. |
| `status()` | Current role, term, commit index, known leader. |
| `node_metrics()` | Runtime-facing metrics wrapper around the raw engine counters and gauges. |
| `shutdown()` | Drain and stop every background task. |

See [rustdoc for `Node`](../api/yggr/struct.Node.html) for full signatures.

## Error types

- `ProposeError`: `NotLeader`, `NoLeader`, `Busy`, `Shutdown`, `DriverDead`, `Fatal`.
- `ReadError`: `NotLeader`, `NotReady`, `SteppedDown`, `Shutdown`, `DriverDead`, `Fatal`.
- `TransferLeadershipError`: `NotLeader`, `NoLeader`, `InvalidTarget`, `Shutdown`, `DriverDead`, `Fatal`.
- `NodeStartError<E>`: `Config(ConfigError)`, `Storage(E)`.
