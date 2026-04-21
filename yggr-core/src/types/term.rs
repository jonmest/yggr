use std::fmt;

/// Logical clock for the cluster (§5.1).
///
/// Time in Raft is divided into terms numbered with consecutive integers.
/// Each term begins with an election; the term identifies which leader is
/// in charge. Every node tracks the highest term it has ever observed in
/// its `current_term`, and that value is monotonically non-decreasing —
/// any RPC carrying a higher term forces immediate catch-up and a
/// step-down to follower.
///
/// Terms are the foundation of Raft's safety: vote records, log entries,
/// and leadership are all scoped by term, so stale messages are detectable
/// by comparing terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[must_use]
pub struct Term(u64);

impl Term {
    /// The initial term, before any election has occurred.
    pub const ZERO: Self = Self(0);

    /// Construct a `Term` from a raw `u64`.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw `u64` underlying this term.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }

    /// The next term. Used when starting a new election (§5.2).
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "term:{}", self.0)
    }
}
