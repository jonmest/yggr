// log indices are u64 but we back the log with a Vec; `u64 as usize` is safe
// on any 64-bit target and a log larger than 2^32 won't fit in RAM on 32-bit.
#![allow(clippy::cast_possible_truncation)]

use crate::records::log_entry::LogEntry;
use crate::types::{index::LogIndex, log::LogId, term::Term};

/// The replicated log — the linear, append-mostly history that consensus
/// is engineered to deliver to every node identically.
///
/// Indices are 1-based (Raft convention). [`LogIndex::ZERO`] is a
/// pre-log sentinel meaning "before any entry" and is never the index
/// of a real entry.
///
/// **Snapshot floor (§7).** After `Engine::install_snapshot` (or an
/// inbound `InstallSnapshot` RPC), entries below the floor are no
/// longer in `entries` — their state lives in the host-persisted
/// snapshot. Reading those indices via [`Log::entry_at`] returns
/// `None`. The single boundary entry's `(index, term)` is preserved
/// in `snapshot_last` so callers above the floor can still validate
/// `prev_log_id` checks against it.
///
/// **Invariants** (debug-checked internally):
///  - In-memory entries have contiguous indices starting at
///    `snapshot_last_index + 1` (or 1 if no snapshot).
///  - Entry terms are non-decreasing across the in-memory log (a
///    leader only appends at its current term, which is monotonic
///    across leadership).
///
/// Followers reconcile against incoming `AppendEntries` by truncating
/// conflicting tails and appending missing entries; leaders use
/// [`Log::entries_from`] to slice out what each peer needs next.
#[derive(Debug)]
pub struct Log<C> {
    entries: Vec<LogEntry<C>>,
    /// `(index, term)` of the entry conceptually at the snapshot's
    /// tail. Both `LogIndex::ZERO` / `Term::ZERO` when no snapshot
    /// has been installed.
    snapshot_last: LogId,
}

impl<C> Default for Log<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> Log<C> {
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            entries: Vec::new(),
            snapshot_last: LogId::new(LogIndex::ZERO, Term::ZERO),
        }
    }

    /// True iff the log holds no in-memory entries. The snapshot floor
    /// is independent — a log can be `is_empty()` while still having
    /// a non-zero snapshot. Callers reasoning about "any history at
    /// all" should use `last_log_id().is_none()` instead.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of in-memory entries. Excludes anything compacted into
    /// the snapshot.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// The smallest index callers can read via [`Log::entry_at`].
    /// Equals `snapshot_last_index + 1`, or 1 if no snapshot.
    pub fn first_index(&self) -> LogIndex {
        LogIndex::new(self.snapshot_last.index.get() + 1)
    }

    /// `(index, term)` at the snapshot's tail. Both fields are zero
    /// when no snapshot has been installed.
    #[must_use]
    pub fn snapshot_last(&self) -> LogId {
        self.snapshot_last
    }

    /// The id of the last entry, or `None` if the log has no history
    /// at all (no in-memory entries AND no snapshot).
    #[must_use]
    pub fn last_log_id(&self) -> Option<LogId> {
        if let Some(last) = self.entries.last() {
            Some(last.id)
        } else if self.snapshot_last.index == LogIndex::ZERO {
            None
        } else {
            Some(self.snapshot_last)
        }
    }

    /// The entry at a 1-based index, if it exists in memory. Returns
    /// `None` for indices ≤ snapshot floor (those are inside the
    /// snapshot and not individually addressable) and for indices
    /// past the in-memory tail.
    #[must_use]
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry<C>> {
        let floor = self.snapshot_last.index.get();
        let i = index.get().checked_sub(floor + 1)?;
        self.entries.get(i as usize)
    }

    /// The term of the entry at `index`, if known. Equals
    /// `snapshot_last.term` for the floor index itself; `None` for
    /// indices below the floor or past the in-memory tail.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == LogIndex::ZERO {
            return None;
        }
        if index == self.snapshot_last.index {
            return Some(self.snapshot_last.term);
        }
        self.entry_at(index).map(|e| e.id.term)
    }

    /// All in-memory entries with index ≥ `index`. Returns an empty
    /// slice if `index` falls below the snapshot floor (caller should
    /// send an `InstallSnapshot` instead) or past the tail.
    #[must_use]
    pub fn entries_from(&self, index: LogIndex) -> &[LogEntry<C>] {
        let floor = self.snapshot_last.index.get();
        if index.get() <= floor {
            // Caller wants entries from below the floor — that's
            // snapshot territory, not addressable here.
            return &[];
        }
        let i = (index.get() - floor - 1) as usize;
        self.entries.get(i..).unwrap_or(&[])
    }

    /// Append a single entry. Caller is responsible for constructing
    /// entries with indices contiguous with whatever's already in the
    /// log (or `first_index()` if empty). Debug-checked.
    pub(crate) fn append(&mut self, entry: LogEntry<C>) {
        debug_assert!(
            match self.entries.last() {
                None => entry.id.index == self.first_index(),
                Some(last) => entry.id.index == last.id.index.next(),
            },
            "log entries must have contiguous indices starting at first_index()"
        );
        self.entries.push(entry);
    }

    /// Remove all entries with index ≥ `index`. No-op if `index` is
    /// past the end OR at/below the snapshot floor (snapshotted
    /// entries are immutable). Used by followers when an
    /// `AppendEntries` RPC conflicts with local state.
    pub(crate) fn truncate_from(&mut self, index: LogIndex) {
        let floor = self.snapshot_last.index.get();
        if index.get() <= floor {
            // Truncating into or below the snapshot is forbidden:
            // §5.4.1 guarantees a correct leader never asks us to.
            return;
        }
        let i = (index.get() - floor - 1) as usize;
        if i < self.entries.len() {
            self.entries.truncate(i);
        }
    }

    /// Install a fresh snapshot floor. Drops every in-memory entry
    /// with index ≤ `last_included_index`. Entries past the floor
    /// survive only when their index > `last_included_index` AND
    /// (for the boundary entry) their term agrees with
    /// `last_included_term` — otherwise the in-memory tail is wiped
    /// because Log Matching no longer holds across the floor.
    pub(crate) fn install_snapshot(
        &mut self,
        last_included_index: LogIndex,
        last_included_term: Term,
    ) {
        // If our existing log already has the snapshot's last entry
        // with the matching term, keep everything past it. Otherwise
        // we have no consistent prefix — wipe the in-memory log.
        let consistent = self
            .entry_at(last_included_index)
            .is_some_and(|e| e.id.term == last_included_term);
        let new_first = LogIndex::new(last_included_index.get() + 1);
        self.snapshot_last = LogId::new(last_included_index, last_included_term);
        if consistent {
            // Keep entries strictly past the snapshot floor.
            self.entries.retain(|e| e.id.index >= new_first);
        } else {
            self.entries.clear();
        }
    }

    /// True iff `candidate_last_log` is at least as up-to-date as
    /// ours per §5.4.1 (last term wins; ties broken by length).
    /// Snapshot floor is implicitly considered — `last_log_id` returns
    /// the snapshot's tail when in-memory log is empty.
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

    /// Check structural invariants. Panics in debug builds when violated,
    /// no-op in release. Intended to run at the end of every state transition.
    ///
    /// §5.3 Log Matching Property requires:
    ///  - in-memory entries have contiguous indices starting at
    ///    `first_index()`,
    ///  - entry terms are non-decreasing across the log (a leader only appends
    ///    at its current term, which is monotonic across leadership).
    #[cfg(debug_assertions)]
    pub(crate) fn check_invariants(&self) {
        let floor = self.snapshot_last.index.get();
        let mut prev_term: Option<Term> = None;
        for (i, entry) in self.entries.iter().enumerate() {
            let expected = LogIndex::new(floor + (i as u64) + 1);
            debug_assert_eq!(
                entry.id.index, expected,
                "log entry at position {i} has non-contiguous index {:?} (expected {expected:?})",
                entry.id.index,
            );
            if let Some(pt) = prev_term {
                debug_assert!(
                    entry.id.term >= pt,
                    "log terms must be non-decreasing (§5.3): {pt:?} -> {:?}",
                    entry.id.term,
                );
            } else if floor > 0 {
                // First in-memory entry's term must be ≥ snapshot's term.
                debug_assert!(
                    entry.id.term >= self.snapshot_last.term,
                    "first in-memory entry's term {:?} must be ≥ snapshot term {:?}",
                    entry.id.term,
                    self.snapshot_last.term,
                );
            }
            prev_term = Some(entry.id.term);
        }
    }

    #[cfg(not(debug_assertions))]
    pub(crate) fn check_invariants(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::records::log_entry::LogPayload;

    fn idx(n: u64) -> LogIndex {
        LogIndex::new(n)
    }
    fn term(n: u64) -> Term {
        Term::new(n)
    }
    fn entry(index: u64, term_n: u64) -> LogEntry<Vec<u8>> {
        LogEntry {
            id: LogId::new(idx(index), term(term_n)),
            payload: LogPayload::Command(Vec::new()),
        }
    }

    // ---------------- is_empty / len ----------------

    #[test]
    fn fresh_log_is_empty() {
        let log: Log<Vec<u8>> = Log::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn log_with_entries_is_not_empty() {
        let mut log: Log<Vec<u8>> = Log::new();
        log.append(entry(1, 1));
        assert!(!log.is_empty());
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn log_is_empty_tracks_in_memory_entries_only() {
        // After a snapshot install that wipes the in-memory tail,
        // is_empty() is true even though the log has "history" via
        // snapshot_last.
        let mut log: Log<Vec<u8>> = Log::new();
        log.install_snapshot(idx(5), term(2));
        assert!(log.is_empty());
        assert_eq!(log.snapshot_last().index, idx(5));
    }

    // ---------------- truncate_from ----------------

    #[test]
    fn truncate_from_drops_entries_at_and_after_index() {
        let mut log: Log<Vec<u8>> = Log::new();
        for i in 1..=5 {
            log.append(entry(i, 1));
        }
        log.truncate_from(idx(3));
        // Entries 1 and 2 survive; 3, 4, 5 are gone.
        assert_eq!(log.len(), 2);
        assert_eq!(log.last_log_id().unwrap().index, idx(2));
        assert!(log.entry_at(idx(3)).is_none());
    }

    #[test]
    fn truncate_from_at_first_index_drops_everything() {
        let mut log: Log<Vec<u8>> = Log::new();
        log.append(entry(1, 1));
        log.append(entry(2, 1));
        log.truncate_from(idx(1));
        assert_eq!(log.len(), 0);
        assert!(log.last_log_id().is_none());
    }

    #[test]
    fn truncate_from_past_tail_is_noop() {
        let mut log: Log<Vec<u8>> = Log::new();
        log.append(entry(1, 1));
        log.append(entry(2, 1));
        log.truncate_from(idx(99));
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn truncate_from_below_snapshot_floor_is_noop() {
        // Snapshot floor at 5, in-memory entries 6..=8. Truncate from
        // index 3 (below floor) must not disturb anything.
        let mut log: Log<Vec<u8>> = Log::new();
        log.install_snapshot(idx(5), term(1));
        log.append(entry(6, 1));
        log.append(entry(7, 1));
        log.append(entry(8, 1));
        log.truncate_from(idx(3));
        assert_eq!(log.len(), 3);
    }

    // ---------------- proptests ----------------

    use proptest::prelude::*;

    proptest! {
        /// truncate_from(k) then appending fresh entries at >= k leaves
        /// a log with contiguous indices starting at first_index() and
        /// terms that don't regress. Pins that truncate doesn't leave
        /// the log in a state where the next append would violate the
        /// debug invariant.
        #[test]
        fn truncate_then_append_reestablishes_contiguous_log(
            initial_terms in proptest::collection::vec(1u64..=5u64, 1..20),
            trunc_from in 1u64..=25u64,
            new_terms in proptest::collection::vec(1u64..=9u64, 0..10),
        ) {
            let mut log: Log<Vec<u8>> = Log::new();
            // Force non-decreasing terms for initial entries — a valid
            // Raft log never has a term regression.
            let mut running = 0u64;
            for (i, t) in initial_terms.iter().enumerate() {
                running = running.max(*t);
                log.append(entry((i as u64) + 1, running));
            }
            let last_term_before = log.last_log_id().map_or(Term::ZERO, |id| id.term);
            log.truncate_from(idx(trunc_from));

            // Append fresh entries at the tail with non-decreasing terms.
            let start = log.last_log_id().map_or(1, |id| id.index.get() + 1);
            let mut t_min = log
                .last_log_id()
                .map_or(last_term_before.get(), |id| id.term.get());
            for (offset, t) in new_terms.iter().enumerate() {
                t_min = t_min.max(*t);
                log.append(entry(start + offset as u64, t_min));
            }

            // Check contiguity and non-decreasing terms.
            let first = log.first_index();
            let mut prev_term: Option<Term> = None;
            for offset in 0..log.len() {
                let i = LogIndex::new(first.get() + offset as u64);
                let e = log.entry_at(i).expect("entry missing after append");
                prop_assert_eq!(e.id.index, i);
                if let Some(pt) = prev_term {
                    prop_assert!(e.id.term >= pt);
                }
                prev_term = Some(e.id.term);
            }
            log.check_invariants();
        }

        /// install_snapshot followed by appends past the floor yields a
        /// well-formed log: first in-memory entry sits at floor+1, terms
        /// are >= snapshot term, and check_invariants is silent.
        #[test]
        fn install_snapshot_then_append_is_well_formed(
            snap_idx in 1u64..20,
            snap_term_n in 1u64..10,
            appends in proptest::collection::vec(1u64..20, 0..10),
        ) {
            let mut log: Log<Vec<u8>> = Log::new();
            log.install_snapshot(idx(snap_idx), term(snap_term_n));
            prop_assert_eq!(log.first_index(), idx(snap_idx + 1));
            prop_assert!(log.is_empty());

            let mut t_min = snap_term_n;
            for (offset, t) in appends.iter().enumerate() {
                t_min = t_min.max(*t);
                log.append(entry(snap_idx + 1 + offset as u64, t_min));
            }
            // All appended entries must respect snapshot floor.
            let first = log.first_index();
            prop_assert_eq!(first, idx(snap_idx + 1));
            for offset in 0..log.len() {
                let i = LogIndex::new(first.get() + offset as u64);
                let e = log.entry_at(i).expect("entry missing past snapshot");
                prop_assert!(e.id.term >= term(snap_term_n));
            }
            log.check_invariants();
        }
    }

    #[test]
    fn truncate_from_just_above_snapshot_floor_drops_everything() {
        // Snapshot floor at 5; entries 6..=8 in memory. Truncating
        // from exactly 6 (floor+1) drops all three.
        let mut log: Log<Vec<u8>> = Log::new();
        log.install_snapshot(idx(5), term(1));
        log.append(entry(6, 1));
        log.append(entry(7, 1));
        log.append(entry(8, 1));
        log.truncate_from(idx(6));
        assert_eq!(log.len(), 0);
        // snapshot_last is unchanged — truncate_from only touches the
        // in-memory tail.
        assert_eq!(log.snapshot_last().index, idx(5));
    }
}
