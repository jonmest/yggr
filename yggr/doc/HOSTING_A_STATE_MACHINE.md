# Hosting a state machine on yggr

`yggr::StateMachine` is the trait you implement to plug your application logic into a `Node`. The runtime calls `apply` once per committed log entry, in order, with a deterministic context. Everything else — replication, persistence, leader election, snapshotting — yggr handles.

This guide covers three patterns that come up in non-trivial state machines.

## 1. Command enums with per-variant responses

Real state machines usually have multiple operations. Wrap them in an enum:

```rust
#[derive(Debug, Clone)]
enum MyCommand {
    Write { key: String, value: Bytes },
    Delete { key: String },
    CreateIndex { name: String },
}

#[derive(Debug)]
enum MyResponse {
    WriteOk { prev: Option<Bytes> },
    DeleteOk { prev: Option<Bytes> },
    IndexCreated,
}

impl StateMachine for MyStateMachine {
    type Command = MyCommand;
    type Response = MyResponse;

    fn apply(&mut self, cmd: MyCommand, ctx: ApplyContext) -> MyResponse {
        match cmd {
            MyCommand::Write { key, value } => {
                let prev = self.map.insert(key, value);
                MyResponse::WriteOk { prev }
            }
            MyCommand::Delete { key } => {
                let prev = self.map.remove(&key);
                MyResponse::DeleteOk { prev }
            }
            MyCommand::CreateIndex { name } => {
                self.indexes.insert(name, Index::default());
                MyResponse::IndexCreated
            }
        }
    }

    // encode_command / decode_command / snapshot / restore …
}
```

Client code pattern-matches on the response variant after `node.write(cmd).await?`. For state machines with many operations, a `#[serde(tag = "op")]` enum serializable via bincode is a common shape.

## 2. Using `ApplyContext.log_index` as a monotonic id

If you need a globally-unique monotonic id for each applied command (event offset, transaction id, audit sequence number), use `ctx.log_index` directly. It's:

- **Strictly increasing** across successive `apply` calls on any one node.
- **Identical** across all nodes for the same logical command.
- **Gap-free** over committed Command entries, but note that raw log indices are _not_ gap-free — the engine also assigns indices to `Noop` and `ConfigChange` entries that don't surface as apply calls. If you need a gap-free sequence over Commands only, maintain your own counter incremented inside `apply`.

```rust
fn apply(&mut self, cmd: AppendCmd, ctx: ApplyContext) -> u64 {
    let offset = ctx.log_index.get();  // durable, replicated, monotonic
    self.segments.record(offset, cmd.payload);
    offset
}
```

This is how `yggr-streams` assigns event-log offsets: the Raft log index _is_ the stream offset, for free.

## 3. Deduplicating on idempotency keys

Clients that retry after a timeout can submit the same command twice. Dedup at the state-machine level:

```rust
struct MyState {
    recent_keys: LruCache<String, u64>,  // key → assigned offset
    // ...
}

fn apply(&mut self, cmd: MyCommand, ctx: ApplyContext) -> MyResponse {
    if let Some(key) = cmd.idempotency_key() {
        if let Some(&prev_offset) = self.recent_keys.peek(key) {
            return MyResponse::DuplicateAccepted { offset: prev_offset };
        }
    }
    let offset = ctx.log_index.get();
    // … apply the command …
    if let Some(key) = cmd.idempotency_key() {
        self.recent_keys.put(key.clone(), offset);
    }
    MyResponse::Ok { offset }
}
```

The LRU bound is your retry window. Document it for callers: "retries must complete within N commands of the original, where N is the cache capacity."

## What you **cannot** do in `apply`

- **Touch the network.** `apply` is synchronous and runs in an apply task. External I/O makes the state machine non-deterministic and will diverge replicas.
- **Read the system clock.** Different replicas apply at different wall-clock times. If you need a timestamp, put it in the command — the leader assigns one when proposing.
- **Use random numbers.** Same reason. Pass entropy through the command.
- **Panic on user-controlled input.** A panic on one replica is an inconsistent state on the others. Decode errors are handled at the boundary (see `DecodeError`); within `apply`, any input is already committed and must be handled.

## What you **should** do in `apply`

- Keep it fast. Long applies starve later commands. If you need slow work, do it after `apply` returns, driven by the applied state.
- Keep it panic-free.
- Return a response that the client call site actually needs. If clients don't use the response, return `()` and save the allocation.
