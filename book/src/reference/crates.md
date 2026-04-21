# Crate layout

| Crate | Purpose |
|---|---|
| `yggr-core` | The engine. `Engine`, `Event`, `Action`, wire types, protobuf mapping. |
| `yggr-sim` | Sim harness. `Cluster`, `Network`, `SafetyChecker`, chaos proptests. |
| `yggr` | Runtime. `Node`, `Config`, `DiskStorage`, `TcpTransport`, `StateMachine` trait. |
| `yggr-examples` | Demos. `kv` three-node replicated KV server. |

## Dependency graph

```
yggr-examples → yggr → yggr-core
                            ↑
                       yggr-sim
```

`yggr-core` does not depend on `yggr`, `yggr-sim`, or any async runtime. `yggr-sim` pulls in `yggr-core` only. `yggr` adds tokio and prost on top of `yggr-core`.

## Why the split

Two reasons.

1. Testability. The sim relies on `yggr-core` being free of tokio, real sockets, and the filesystem. Otherwise we couldn't run thousands of deterministic chaos steps per second.
2. Embeddability. Users who want Raft inside something that's already async take `yggr-core` and write a small host. See [Writing a custom host](../engine/custom-host.md).
