# Membership

A yggr cluster has two classes of members:

- **Voters** count toward quorum and can campaign. The majority of voters must ack a commit for it to advance.
- **Learners** receive replication but stay out of quorum and never campaign. See [Learners](./learners.md) for the dedicated chapter.

Self is always a member: either implicitly a voter (the default) or a learner (once `AddLearner(self)` commits locally).

## Operations

All membership changes go through `node.admin()`:

```rust
let admin = node.admin();
admin.add_peer(id).await?;           // add a voter
admin.add_learner(id).await?;        // add a non-voting member
admin.promote_learner(id).await?;    // learner → voter
admin.remove_peer(id).await?;        // remove either flavour
```

Each call returns once the configuration change has committed.

## Single-server discipline (§4.3)

yggr implements single-server changes: exactly one membership delta per log entry, and at most one uncommitted change in flight. This keeps the old and new majorities overlapping and rules out the split-quorum window that joint-consensus would need to handle.

Consequence: `admin.add_peer` and `admin.remove_peer` can't run concurrently. The second call returns an error (`Busy` / `ChangeInProgress`) until the first commits.

## Recommended workflow for growing a cluster

```text
  state     operator action         cluster effect
  ─────     ───────────────         ──────────────
  N voters  add_learner(new)        new node gets replication, no quorum
            (wait for catch-up)     monitor via node_metrics()
            promote_learner(new)    N+1 voters, quorum adjusts
```

Adding a voter in one step (`add_peer`) is supported but operationally awkward: quorum math shifts before the node has caught up, and a cold node can slow commit until it catches up. Prefer the learner path.

## Recommended workflow for shrinking a cluster

```text
  state     operator action         cluster effect
  ─────     ───────────────         ──────────────
  N voters  transfer_leadership     optional, if leader is the one leaving
            remove_peer(going)      N-1 voters, quorum adjusts
```

If the node being removed is the leader, it steps down once `RemovePeer(self)` commits. Either transfer leadership first (cleaner) or tolerate the brief election gap.

## Snapshot-carried membership

Snapshots store the voter and learner sets as of `last_included_index`. A node that recovers from a snapshot sees the correct membership without replaying the full history. An `InstallSnapshot` from the leader similarly restores both sets on the follower.

This is why snapshot metadata is versioned on disk — the 0.1 format carries voters only for backward compatibility with pre-learner snapshots; 0.2 carries both.

## Observability

`node.status()` surfaces the current membership view:

```rust
let status = node.status().await?;
println!("{} voters, {} learners", status.membership.voters.len(), status.membership.learners.len());
```

`node.node_metrics()` exposes per-peer replication progress (future: a `followers` map with `matched`, `next_index`, `is_learner`). Use it to decide when a learner is ready for promotion.

## Errors

See [Errors](./errors.md). Membership-specific variants include `ChangeInProgress` (another CC uncommitted), `UnknownNode`, `AlreadyVoter`, `AlreadyLearner`, `NotLearner`.
