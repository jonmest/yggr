//! In-flight message queue + partition bookkeeping.
//!
//! The scheduler picks messages by id, so the network keeps every
//! in-flight `Send` addressable until the scheduler either delivers or
//! drops it. Partitions are stored as a disjoint set of `NodeId`s on
//! one side; any message crossing the partition is silently discarded
//! at delivery time.

use std::collections::BTreeSet;
use std::collections::VecDeque;

use jotun_core::{Message, NodeId};

/// A message in flight. The scheduler refers to it by `id`; FIFO
/// order is preserved unless the scheduler explicitly picks `Reorder`.
#[derive(Debug, Clone)]
pub(crate) struct InFlight<C> {
    pub(crate) id: MessageId,
    pub(crate) from: NodeId,
    pub(crate) to: NodeId,
    pub(crate) message: Message<C>,
}

/// Stable, strictly increasing id assigned on enqueue so the scheduler
/// can target a specific in-flight message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MessageId(pub(crate) u64);

/// The simulated network.
#[derive(Debug)]
pub(crate) struct Network<C> {
    queue: VecDeque<InFlight<C>>,
    next_id: u64,
    /// Nodes on the "A" side of the current partition. Empty means no
    /// partition. Any message between A-side and non-A-side nodes is
    /// dropped on delivery.
    partition_a: BTreeSet<NodeId>,
}

impl<C> Default for Network<C> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            next_id: 0,
            partition_a: BTreeSet::new(),
        }
    }
}

impl<C: Clone> Network<C> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn enqueue(&mut self, from: NodeId, to: NodeId, message: Message<C>) -> MessageId {
        let id = MessageId(self.next_id);
        self.next_id += 1;
        self.queue.push_back(InFlight {
            id,
            from,
            to,
            message,
        });
        id
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn peek_ids(&self) -> Vec<MessageId> {
        self.queue.iter().map(|m| m.id).collect()
    }

    /// Remove and return the message with the given id. `None` if no
    /// such message is in flight.
    pub(crate) fn take(&mut self, id: MessageId) -> Option<InFlight<C>> {
        let pos = self.queue.iter().position(|m| m.id == id)?;
        self.queue.remove(pos)
    }

    /// Swap two in-flight messages — the scheduler's "reorder" knob.
    pub(crate) fn swap(&mut self, a: usize, b: usize) {
        if a < self.queue.len() && b < self.queue.len() {
            self.queue.swap(a, b);
        }
    }

    pub(crate) fn set_partition(&mut self, a_side: BTreeSet<NodeId>) {
        self.partition_a = a_side;
    }

    pub(crate) fn heal(&mut self) {
        self.partition_a.clear();
    }

    /// True iff a message between `from` and `to` would be severed by
    /// the current partition (one side A, the other not).
    pub(crate) fn partitioned(&self, from: NodeId, to: NodeId) -> bool {
        if self.partition_a.is_empty() {
            return false;
        }
        self.partition_a.contains(&from) != self.partition_a.contains(&to)
    }
}
