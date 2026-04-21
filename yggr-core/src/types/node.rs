use std::fmt;

/// A unique identifier for a node in the Raft cluster.
///
/// Node ids are non-zero so that `0` can serve as an unambiguous sentinel
/// in the protobuf wire format (the default for `uint64`). The validating
/// mapping layer rejects messages carrying `node_id == 0`.
///
/// Ids are assumed to be stable across restarts and unique within a
/// cluster. The library does not assign them; callers do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(std::num::NonZeroU64);

impl NodeId {
    /// Construct a `NodeId` from a raw `u64`. Returns `None` if `id == 0`.
    #[must_use]
    pub fn new(id: u64) -> Option<Self> {
        Some(Self(std::num::NonZeroU64::new(id)?))
    }

    /// The raw `u64` underlying this id. Always non-zero.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node:{}", self.0)
    }
}
