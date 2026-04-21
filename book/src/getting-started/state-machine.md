# Writing a state machine

Your `StateMachine` is the application-level thing Raft replicates. Three required methods, two optional ones for snapshots.

```rust
use yggr::{DecodeError, StateMachine};

#[derive(Debug, Default)]
struct Counter { value: u64 }

#[derive(Debug, Clone)]
enum CountCmd { Inc(u64) }

impl StateMachine for Counter {
    type Command = CountCmd;
    type Response = u64;

    fn encode_command(c: &CountCmd) -> Vec<u8> {
        match c {
            CountCmd::Inc(n) => n.to_le_bytes().to_vec(),
        }
    }

    fn decode_command(bytes: &[u8]) -> Result<CountCmd, DecodeError> {
        let arr: [u8; 8] = bytes.try_into()
            .map_err(|_| DecodeError::new("expected 8 bytes"))?;
        Ok(CountCmd::Inc(u64::from_le_bytes(arr)))
    }

    fn apply(&mut self, cmd: CountCmd) -> u64 {
        match cmd {
            CountCmd::Inc(n) => { self.value += n; self.value }
        }
    }
}
```

## Rules

- `apply` must be deterministic. The same command on any node must produce the same response and the same state mutation. If it doesn't, the cluster diverges.
- `encode_command` and `decode_command` must round-trip. The library stores the bytes in the log and on the wire; the decoder has to accept its own encoder's output.
- `apply` runs on its own tokio task. Slow work doesn't stall heartbeats, but the driver blocks on send when the apply channel fills. See [Configuration](../runtime/config.md).

## Snapshots

Override `snapshot` and `restore` if your state takes too long to replay from the log:

```rust
fn snapshot(&self) -> Vec<u8> {
    bincode::serialize(&self.value).unwrap()
}

fn restore(&mut self, bytes: Vec<u8>) {
    self.value = bincode::deserialize(&bytes).unwrap();
}
```

Compression is your decision. Compress inside `snapshot`, decompress inside `restore`. The library treats the bytes as opaque through the engine, disk, and wire.
