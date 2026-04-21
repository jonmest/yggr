//! The scheduler's event alphabet and the helpers it uses to pick one
//! per [`crate::Cluster::step`].
//!
//! The scheduler is pure — it inspects cluster/network state and draws
//! from the shared RNG to decide. Keeping this logic in one place
//! makes it straightforward to swap the policy (pure-random,
//! happy-path, partition-biased) without touching the core types.

use std::collections::BTreeSet;

use yggr_core::{ConfigChange, NodeId};

use crate::network::MessageId;

/// One scheduler decision. Applied to the cluster in
/// [`crate::Cluster::step`].
#[derive(Debug, Clone)]
pub(crate) enum Event<C> {
    /// Advance node `id`'s logical clock by one tick.
    Tick(NodeId),
    /// Deliver the in-flight message with the given id. If the sender
    /// and receiver are on opposite sides of the current partition,
    /// the message is dropped instead.
    Deliver(MessageId),
    /// Drop the in-flight message. Exercises message loss.
    Drop(MessageId),
    /// Swap two adjacent queued messages — cheap reorder.
    Reorder(usize, usize),
    /// Offer a client proposal to `id`. Followers redirect; only
    /// leaders will replicate.
    Propose(NodeId, C),
    /// Crash `id`: drop the engine and any unflushed pending writes.
    Crash(NodeId),
    /// Recover `id` from its durable snapshot.
    Recover(NodeId),
    /// Flush the first `n` pending writes on `id` to the durable
    /// snapshot. `usize::MAX` means "flush everything".
    Flush(NodeId, usize),
    /// Install a partition. The given set sits on one side, the rest
    /// on the other.
    Partition(BTreeSet<NodeId>),
    /// Heal any current partition.
    Heal,
    /// Offer a §4.3 single-server membership change to `id`. Followers
    /// redirect; only leaders append (and only if no other CC is in
    /// flight). The harness doesn't randomly inject these — the
    /// scheduler picks them only in scripted scenarios — but they ride
    /// the same machinery as `Propose` once chosen.
    ProposeConfigChange(NodeId, ConfigChange),
    /// Tell node `id` to take a snapshot covering everything up to
    /// the given index, with the supplied bytes. Scripted scenarios
    /// drive these directly via [`crate::Cluster::snapshot_to`].
    Snapshot(NodeId, yggr_core::LogIndex, Vec<u8>),
}
