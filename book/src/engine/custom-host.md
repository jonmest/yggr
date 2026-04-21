# Writing a custom host

The default `yggr` runtime assumes tokio, TCP, and disk. If those don't fit — you already have an async runtime, you want to replicate over QUIC, or you need to persist into an existing storage engine — you can drive `yggr-core` yourself.

A minimal host loop:

```rust
use yggr_core::{Engine, Event, Action};

let mut engine: Engine<MyCmd> = Engine::new(my_id, peers, env, heartbeat_ticks);

loop {
    let event = next_event().await?;  // your ticker, transport, app
    let actions = engine.step(event);

    for action in actions {
        match action {
            Action::PersistHardState { current_term, voted_for } => {
                my_storage.persist_hard_state(current_term, voted_for).await?;
            }
            Action::PersistLogEntries(entries) => {
                my_storage.append_log(entries).await?;
            }
            Action::PersistSnapshot { bytes, .. } => {
                my_storage.persist_snapshot(bytes).await?;
            }
            Action::Send { to, message } => {
                my_transport.send(to, message).await?;
            }
            Action::Apply(entries) => {
                for entry in entries { my_sm.apply(entry).await; }
            }
            Action::ApplySnapshot { bytes } => {
                my_sm.restore(bytes).await;
            }
            _ => {}
        }
    }
}
```

## Rules

1. Respect action order. See [Events and actions](./events-and-actions.md). Fsync `Persist*` before any subsequent `Send`.
2. Feed `Tick` at a steady cadence. The engine's election and heartbeat timers are tick-based. The default runtime uses 50ms per tick.
3. Hydrate the engine on restart. Read persisted hard state, snapshot (if any), and post-snapshot log, then call `Engine::recover_from(RecoveredHardState { .. })` before feeding any real events.

`yggr/src/node.rs` and `yggr-sim/src/harness.rs` are two working reference hosts.
