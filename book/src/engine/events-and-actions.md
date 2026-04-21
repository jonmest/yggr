# Events and actions

See the [rustdoc for `Event`](../api/yggr_core/enum.Event.html) and [`Action`](../api/yggr_core/enum.Action.html) for the full definitions.

## Action ordering

`step()` returns `Vec<Action<C>>` in causal order. Every action that must reach stable storage before any subsequent network send appears earlier in the vector. Hosts must:

- Process actions in order.
- Flush `PersistHardState` / `PersistLogEntries` / `PersistSnapshot` to disk before performing any `Send` that follows them.

This is Figure 2's "respond to RPCs only after updating stable storage". The engine enforces it on the host side of the contract.

## Actions that don't need fsync

- `Send { .. }` — best-effort. The engine retries on its own cadence.
- `Apply(entries)` — feed to the state machine. The engine advances `last_applied` when emitting this; the host must not skip it.
- `ApplySnapshot { bytes }` — restore the state machine.
- `Redirect { leader_hint }` — fail the user's pending reply with `NotLeader`.
- `SnapshotHint { .. }` — advisory. Ignore, debounce, or act.
- `ReadReady { id }` — the read with this id is now safe to serve.
- `ReadFailed { id, reason }` — surface the reason to the caller.
