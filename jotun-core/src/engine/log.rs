// log indices are u64 but we back the log with a Vec; `u64 as usize` is safe
// on any 64-bit target and a log larger than 2^32 won't fit in RAM on 32-bit.
#![allow(clippy::cast_possible_truncation)]

use crate::records::log_entry::LogEntry;
use crate::types::{index::LogIndex, log::LogId, term::Term};

/// indices are 1-based (Raft convention). `LogIndex::ZERO` is a sentinel
/// meaning "before any entry" and is never the index of a real entry.
#[derive(Debug)]
pub struct Log<C> {
    entries: Vec<LogEntry<C>>,
}

impl<C> Default for Log<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> Log<C> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// the id of the last entry, or `None` if the log is empty.
    /// this is what vote and append-entries predicates compare against.
    #[must_use]
    pub fn last_log_id(&self) -> Option<LogId> {
        self.entries.last().map(|e| e.id)
    }

    /// the entry at a 1-based index, if it exists.
    #[must_use]
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry<C>> {
        let i = index.get().checked_sub(1)?;
        self.entries.get(i as usize)
    }

    /// the term of the entry at `index`, if it exists.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.entry_at(index).map(|e| e.id.term)
    }

    /// all entries with index `>= index`. empty slice if the log is empty
    /// or `index` is past the end.
    #[must_use]
    pub fn entries_from(&self, index: LogIndex) -> &[LogEntry<C>] {
        let i = index.get().saturating_sub(1) as usize;
        self.entries.get(i..).unwrap_or(&[])
    }

    /// append a single entry. caller is responsible for constructing entries
    /// with contiguous indices; in debug builds this is checked.
    pub fn append(&mut self, entry: LogEntry<C>) {
        debug_assert!(
            match self.entries.last() {
                None => entry.id.index == LogIndex::new(1),
                Some(last) => entry.id.index == last.id.index.next(),
            },
            "log entries must have contiguous indices starting at 1"
        );
        self.entries.push(entry);
    }

    /// remove all entries with index `>= index`. no-op if `index` is past the end.
    /// used by followers when an `AppendEntries` RPC conflicts with local state.
    pub fn truncate_from(&mut self, index: LogIndex) {
        let i = index.get().saturating_sub(1) as usize;
        if i < self.entries.len() {
            self.entries.truncate(i);
        }
    }

    #[must_use]
    pub fn is_superseded_by(&self, candidate_last_log: Option<LogId>) -> bool {
        match (self.last_log_id(), candidate_last_log) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(ours), Some(theirs)) => {
                theirs.term > ours.term || (theirs.term == ours.term && theirs.index >= ours.index)
            }
        }
    }
}
