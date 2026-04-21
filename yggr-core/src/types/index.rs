use std::fmt;

/// Position of an entry in the replicated log.
///
/// Indices are 1-based per Raft convention. [`LogIndex::ZERO`] is a
/// pre-log sentinel meaning "before any entry" — it is never the index
/// of a real entry. The first appended entry is always at index 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[must_use]
pub struct LogIndex(u64);

impl LogIndex {
    /// The pre-log sentinel. Returned when "before any entry" is the
    /// honest answer (e.g., as `prev_log_index` for the very first
    /// `AppendEntries` against an empty log).
    pub const ZERO: Self = Self(0);

    /// Construct a `LogIndex` from a raw `u64`.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw `u64` underlying this index.
    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }

    /// The next index. Used when assigning a new entry's position.
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "log_index:{}", self.0)
    }
}
