# Learners

A learner is a cluster member that receives replication but doesn't count toward quorum and never campaigns. Raft thesis §4.2.1.

Learners exist so that adding a fresh node to a cluster doesn't destabilise it. Without learners, the new node joins the voting set immediately — quorum math shifts before the node has caught up, and the operator has no handle to delay the shift. With learners, a node can:

- receive `AppendEntries` and `InstallSnapshot`,
- catch up on its own schedule,
- stay out of quorum and elections while it does,
- be promoted only when it's ready.

## Lifecycle

```
(unknown) --add_learner--> learner --promote_learner--> voter
                           learner <--remove_peer----- (removed)
                           voter   <--remove_peer----- (removed)
```

The three operations are all single-server membership changes (§4.3): at most one in flight at a time. The leader rejects a new configuration proposal if any previous one is still uncommitted.

## Operations

All three go through [`Node::admin()`](./node.md):

```rust
let admin = node.admin();

// Add a new node as a non-voting learner. Begins receiving replication
// immediately.
admin.add_learner(NodeId::new(4).unwrap()).await?;

// Wait for the learner to catch up (inspect via status/metrics below),
// then promote.
admin.promote_learner(NodeId::new(4).unwrap()).await?;

// Remove either a learner or a voter.
admin.remove_peer(NodeId::new(4).unwrap()).await?;
```

Each call returns once the configuration change has committed. It does not wait for catch-up.

## Checking readiness before promotion

Promotion is always safe for correctness — the engine enforces Leader Completeness — but promoting a badly-lagging learner reduces cluster availability: every subsequent commit now needs an ack from the lagging node. Before promoting, check that the learner is caught up.

The runtime exposes per-peer replication progress via metrics:

```rust
let m = node.node_metrics().await?;
// m.replication.followers is a BTreeMap<NodeId, FollowerProgress>.
// Each FollowerProgress has `matched`, `next_index`, `is_learner`.
```

A simple rule of thumb: promote when `leader_last_log_index - followers[learner].matched <= 1`. Document-level advice, not a library invariant.

## What learners cannot do

- **Grant votes.** A learner that receives a `RequestVote` or `RequestPreVote` responds with a rejection.
- **Campaign.** A learner whose election timer fires resets the timer and does nothing else.
- **Count toward quorum.** Leader commit-advance uses `majority_index_over_voters`, which excludes learners. A learner's ack cannot unblock a commit.

These three rules are enforced in the engine; the runtime just forwards configuration changes.

## Constraints

- **One membership change in flight at a time.** If you call `add_learner` and immediately call `promote_learner` on the same node, the second call will return `ChangeInProgress` (or equivalent `ProposeError`) until the `add_learner` entry commits.
- **Learners still persist state.** Like voters, they carry a durable log and hard state. A crashed learner recovers by replaying the log and resuming catch-up.
- **Snapshots carry full membership.** An `InstallSnapshot` restores both voters and learners, so a recovered node correctly identifies its own role.

## Anti-patterns

- **Auto-promotion on "caught up enough".** The runtime does not auto-promote. Readiness signals are surfaced; the decision is the operator's. A hidden policy would make operations unpredictable.
- **Promoting every new node.** If a read-only replica is all you need (e.g. a follower that just drives a secondary index), leave it as a learner. Quorum remains tight.
- **Treating learners as read-only for linearizable reads.** Learners see committed entries but do not serve `read_linearizable` — that path requires leadership. Use stale reads on a learner if eventual consistency is acceptable.
