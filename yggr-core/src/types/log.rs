use crate::types::{index::LogIndex, term::Term};

/// Identifier of a log entry as `(index, term)`.
///
/// The pair is what makes the **Log Matching Property** (§5.3) work: if
/// two logs contain entries with the same `LogId`, then the logs are
/// identical in all entries up to that index. This means a single
/// `LogId` is enough for `AppendEntries` to certify the entire prefix
/// matches — no whole-log comparison needed.
///
/// `LogId` also drives the §5.4.1 election restriction: `RequestVote`
/// carries the candidate's last `LogId`, and voters compare last-entry
/// ids to decide whether a candidate's log is at least as up-to-date as
/// their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogId {
    /// Position in the log (1-based).
    pub index: LogIndex,
    /// Term in which the entry was created by its leader.
    pub term: Term,
}

impl LogId {
    /// Construct a `LogId` from its index and term.
    #[must_use]
    pub fn new(index: LogIndex, term: Term) -> Self {
        Self { index, term }
    }
}
