# A three-node cluster

The `yggr-examples` crate ships a replicated KV service. To see it running:

```bash
./yggr-examples/run-three-node.sh
```

That brings up three `kv` processes on localhost, has them elect a leader, and exposes a text protocol you can drive with `nc`.

## What the script does

Each node starts with:

```rust
let config    = Config::new(my_node_id, peer_ids);
let storage   = DiskStorage::open(&data_dir).await?;
let transport = TcpTransport::start(my_node_id, listen_addr, peer_addrs).await?;
let node      = Node::start(config, MyStateMachine::default(), storage, transport).await?;
```

The leader accepts `propose(cmd)` from clients. Followers return `NotLeader` with a hint so the client can retry against the right node.

## Next

- [Writing a state machine](./state-machine.md)
- [The Node API](../runtime/node.md)
