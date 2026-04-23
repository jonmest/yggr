# Transport and side channels

The `yggr::Transport` trait carries **Raft protocol messages** between brokers: `AppendEntries`, `RequestVote`, `InstallSnapshot`, etc. It is _not_ designed to carry bulk application payloads.

If your application's command bytes are small (under a few KB), treat them as opaque payload inside the Raft command and ignore this page — your existing `TcpTransport` is fine. This page is for applications that want to replicate large payloads (stream messages, blob writes, file uploads).

## Why not just put payloads in the Raft command

You can. It works correctly. But:

- Every entry flows through the leader's fsync batch. A 1 MiB payload fsync dominates a 100-byte metadata fsync.
- Snapshots carry the whole payload history forever (until compaction).
- Memory pressure on the leader scales with payload size × outstanding commits.
- `AppendEntries` frames become large, bloating the single TCP connection that carries both heartbeats and entries. Head-of-line blocking hurts liveness.

For small payloads this is fine. For large payloads (≥ several KB), a side channel is better.

## Recommended shape: metadata through Raft, payload out-of-band

The pattern:

1. Leader receives the client's `append(topic, payload)` request.
2. Leader writes the **payload bytes** to a local segment file on its own disk, immutably tagged with a local ref.
3. Leader proposes a Raft command carrying just the **metadata** (`topic`, `local_ref`, `size`, `checksum`, `idempotency_key`).
4. Raft replicates the metadata. The engine commits it when a majority acks — _before_ followers have the payload.
5. Each follower, on applying the metadata, issues a **payload-pull** to the leader via its own side channel (separate listener, separate port). The payload-pull is HTTP, gRPC, or a custom framed protocol — your choice.
6. Once the follower has the payload persisted and checksum-verified, it considers the entry "durable" from its perspective.

The commit condition you expose to clients should include payload durability on a majority. This is a layer above Raft: after the metadata commits, wait until N-1 followers have confirmed payload durability (via an explicit `MarkDurable` command or a synchronous follow-up request).

This is what `yggr-streams` uses. See `yggr-streams/docs/context/DESIGN.md` for the specific protocol shape.

## What yggr does and doesn't help with

- **yggr provides**: Raft replication of metadata with the usual guarantees (majority commit, leader-completeness, log-matching, deterministic apply).
- **yggr does not provide**: the payload-pull side channel. You build that, using whatever library you like.
- **yggr does not provide**: the coordination between "metadata committed" and "payload durable on a majority." That's application logic. Typically a `MarkDurable { local_ref }` command that the leader proposes once enough followers have pulled the payload.

## Running two listeners

The natural deployment: one port for Raft (the `TcpTransport` default), one port for the application's client-facing + payload-pull surface. Example:

```text
broker config:
  node_id      = 1
  raft_addr    = 0.0.0.0:9445   # TcpTransport listens here
  client_addr  = 0.0.0.0:9443   # your gRPC / HTTP listener here
  peers:
    2 = other-broker:9445       # peer raft addrs
    3 = third-broker:9445
```

The Raft port is broker-to-broker only. Client traffic and payload-pull both go through the client port (TLS recommended — see the broader defaults discussion).

## Don't multiplex bulk on the Raft connection

`TcpTransport` uses a single TCP connection per peer. Sending a 10 MiB payload on it blocks every `AppendEntries` heartbeat until the payload finishes transmitting, which can trip election timers. Don't do that. Keep Raft's transport lean; carry bulk traffic elsewhere.

## Small-payload applications

If your commands are under ~4 KiB each, none of this matters. Put your payload in the command, let Raft handle it, move on. Most `StateMachine` users don't need a side channel.
