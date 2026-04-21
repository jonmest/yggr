# The pure engine

`yggr-core` is the Raft protocol. One type, one method:

```rust
pub struct Engine<C> { /* ... */ }

impl<C: Clone> Engine<C> {
    pub fn step(&mut self, event: Event<C>) -> Vec<Action<C>>;
}
```

Every forward motion is an `Event`:

- `Tick` — abstract time advanced one step
- `Incoming(msg)` — a peer sent us an RPC
- `ClientProposal(cmd)` — application wants to replicate something
- `ClientProposalBatch(cmds)` — same, many at once
- `ProposeConfigChange(change)` — §4.3 membership change
- `TransferLeadership { target }` — initiate leadership transfer
- `ProposeRead { id }` — §8 linearizable read
- `SnapshotTaken { last_included_index, bytes }` — host cut a snapshot

Every effect is an `Action`:

- `PersistHardState`, `PersistLogEntries`, `PersistSnapshot`
- `Send { to, message }`
- `Apply(entries)` — commit to the state machine
- `ApplySnapshot { bytes }` — restore the state machine
- `Redirect { leader_hint }` — we are not the leader
- `ReadReady { id }` / `ReadFailed { id, reason }`
- `SnapshotHint { last_included_index }` — advisory compaction

## No I/O

The engine does not touch a socket, a file, or the clock. The host (either `yggr` or your own code) translates `Action`s into I/O.

## No async

`step` is synchronous. It returns `Vec<Action<C>>`. The host decides how to dispatch.

## Testable

Because the engine has no I/O, it runs at memory speed under the deterministic chaos harness in `yggr-sim`. Many seeds, thousands of steps each, in a second.
