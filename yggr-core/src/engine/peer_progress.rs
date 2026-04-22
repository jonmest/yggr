//! Per-peer replication bookkeeping for a leader.
//!
//! A leader tracks, for every peer, two indices from §5.3 Figure 2:
//!  - `nextIndex`: the next log entry to send to that peer,
//!  - `matchIndex`: the highest log entry known to be replicated on that peer.
//!
//! This module owns the two maps and the invariants that make their evolution
//! safe for commit-index advancement:
//!  - `matchIndex` is monotonically non-decreasing (stale `Success` responses
//!    must not rewind progress).
//!  - `majority_index` exposes the one non-obvious calculation — the highest
//!    index replicated on a majority of the cluster — which feeds
//!    `commit_index` advancement on the leader.

use std::collections::BTreeMap;

use crate::types::{index::LogIndex, node::NodeId};

/// Tracks `nextIndex` and `matchIndex` for every peer the leader knows about.
/// Self is intentionally absent: the leader's own progress is implicitly its
/// last log index, which gets folded into the internal majority calculation
/// that drives commit-index advancement.
#[derive(Debug, Clone, Default)]
pub struct PeerProgress {
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
}

impl PeerProgress {
    /// Initial state on becoming leader (§5.2):
    ///  - `nextIndex[p] = leader_last_log_index + 1`,
    ///  - `matchIndex[p] = 0`.
    #[must_use]
    pub(crate) fn new(
        peers: impl IntoIterator<Item = NodeId>,
        leader_last_log_index: LogIndex,
    ) -> Self {
        let next = leader_last_log_index.next();
        let mut next_index = BTreeMap::new();
        let mut match_index = BTreeMap::new();
        for peer in peers {
            next_index.insert(peer, next);
            match_index.insert(peer, LogIndex::ZERO);
        }
        Self {
            next_index,
            match_index,
        }
    }

    /// Current `nextIndex` for a peer, if tracked.
    #[must_use]
    pub fn next_for(&self, peer: NodeId) -> Option<LogIndex> {
        self.next_index.get(&peer).copied()
    }

    /// Current `matchIndex` for a peer, if tracked.
    #[must_use]
    pub fn match_for(&self, peer: NodeId) -> Option<LogIndex> {
        self.match_index.get(&peer).copied()
    }

    /// Record a successful `AppendEntries` response. Advances `matchIndex`
    /// monotonically and pushes `nextIndex` forward past it. Stale responses
    /// that would rewind `matchIndex` are silently ignored — this is the
    /// correctness-critical behaviour that makes `majority_index` safe.
    /// No-op on unknown peers (e.g., removed by a config change).
    pub(crate) fn record_success(&mut self, peer: NodeId, last_appended: LogIndex) {
        // §5.3: guard the whole update on matchIndex monotonicity. If a stale
        // response doesn't advance matchIndex, it also must not rewind
        // nextIndex — a later conflict may have already pushed nextIndex
        // below this stale last_appended, and we don't want to undo that.
        if let Some(m) = self.match_index.get_mut(&peer)
            && last_appended > *m
        {
            *m = last_appended;
            if let Some(n) = self.next_index.get_mut(&peer) {
                *n = last_appended.next();
            }
        }
    }

    /// Record a conflict response. The leader jumps `nextIndex` to the hint
    /// the follower supplied (capped at 1 — index 0 is the pre-log sentinel).
    /// `matchIndex` is untouched. No-op on unknown peers.
    pub(crate) fn record_conflict(&mut self, peer: NodeId, hint: LogIndex) {
        if let Some(n) = self.next_index.get_mut(&peer) {
            *n = hint.max(LogIndex::new(1));
        }
    }

    /// Highest index replicated on a majority of the voting cluster
    /// (leader included). Non-voter peers — learners — are excluded
    /// from the tally. `voters` is the set of peer ids that currently
    /// count toward quorum (leader not expected to be in the set;
    /// it's folded in via `leader_last_log`).
    ///
    /// Algorithm (§5.3): sort the voter `matchIndex` values along
    /// with the leader's own last index, pick the median (lower-middle
    /// for even sizes).
    pub(crate) fn majority_index_over_voters(
        &self,
        leader_last_log: LogIndex,
        voters: &std::collections::BTreeSet<NodeId>,
    ) -> LogIndex {
        let mut values: Vec<LogIndex> = voters
            .iter()
            .map(|v| self.match_index.get(v).copied().unwrap_or(LogIndex::ZERO))
            .collect();
        values.push(leader_last_log);
        values.sort_unstable();
        let pos = (values.len() - 1) / 2;
        #[allow(clippy::indexing_slicing)]
        values[pos]
    }

    /// Iterate over all tracked peer ids, sorted.
    pub fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.next_index.keys().copied()
    }

    /// Number of peers tracked (leader excluded).
    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.next_index.len()
    }

    /// Begin tracking replication progress for a newly-added peer (§4.3
    /// `AddPeer`). Same initial values as if the peer had been part of the
    /// cluster when this leader took office: `nextIndex = leader_last + 1`,
    /// `matchIndex = 0` (we haven't proven the peer has anything yet).
    /// No-op if the peer is already tracked.
    pub(crate) fn add_peer(&mut self, peer: NodeId, leader_last_log_index: LogIndex) {
        self.next_index
            .entry(peer)
            .or_insert_with(|| leader_last_log_index.next());
        self.match_index.entry(peer).or_insert(LogIndex::ZERO);
    }

    /// Stop tracking a removed peer (§4.3 `RemovePeer`). The peer's
    /// `matchIndex` is dropped from `majority_index` calculations
    /// immediately. No-op if the peer wasn't tracked.
    pub(crate) fn remove_peer(&mut self, peer: NodeId) {
        self.next_index.remove(&peer);
        self.match_index.remove(&peer);
    }

    /// Advance `nextIndex[peer]` to at least `floor`, idempotently.
    /// Used after an `InstallSnapshot` ack at exactly the snapshot's
    /// tail: `record_success` won't move `matchIndex` past where it
    /// already is, but we still need `nextIndex` to step into the
    /// post-floor range so the leader switches back to `AppendEntries`.
    pub(crate) fn ensure_next_at_least(&mut self, peer: NodeId, floor: LogIndex) {
        if let Some(n) = self.next_index.get_mut(&peer)
            && *n < floor
        {
            *n = floor;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn node(id: u64) -> NodeId {
        NodeId::new(id).unwrap()
    }

    fn idx(n: u64) -> LogIndex {
        LogIndex::new(n)
    }

    /// Treat every tracked peer in `pp` as a voter. Cuts test noise
    /// when a scenario has no learners.
    fn all_as_voters(pp: &PeerProgress) -> std::collections::BTreeSet<NodeId> {
        pp.peers().collect()
    }

    // ---------------- construction ----------------

    #[test]
    fn new_sets_next_index_to_leader_last_plus_one() {
        let pp = PeerProgress::new([node(2), node(3)], idx(5));
        assert_eq!(pp.next_for(node(2)), Some(idx(6)));
        assert_eq!(pp.next_for(node(3)), Some(idx(6)));
    }

    #[test]
    fn new_sets_match_index_to_zero() {
        let pp = PeerProgress::new([node(2), node(3)], idx(5));
        assert_eq!(pp.match_for(node(2)), Some(LogIndex::ZERO));
        assert_eq!(pp.match_for(node(3)), Some(LogIndex::ZERO));
    }

    #[test]
    fn new_with_empty_log_starts_next_at_one() {
        let pp = PeerProgress::new([node(2)], LogIndex::ZERO);
        assert_eq!(pp.next_for(node(2)), Some(idx(1)));
    }

    #[test]
    fn new_with_no_peers_is_empty() {
        let pp = PeerProgress::new(std::iter::empty(), idx(5));
        assert_eq!(pp.peer_count(), 0);
    }

    // ---------------- record_success ----------------

    #[test]
    fn record_success_advances_match_index() {
        let mut pp = PeerProgress::new([node(2)], idx(5));
        pp.record_success(node(2), idx(3));
        assert_eq!(pp.match_for(node(2)), Some(idx(3)));
    }

    #[test]
    fn record_success_advances_next_index_past_match() {
        let mut pp = PeerProgress::new([node(2)], idx(5));
        pp.record_success(node(2), idx(3));
        assert_eq!(pp.next_for(node(2)), Some(idx(4)));
    }

    #[test]
    fn record_success_ignores_stale_responses() {
        let mut pp = PeerProgress::new([node(2)], idx(5));
        pp.record_success(node(2), idx(5));
        pp.record_success(node(2), idx(2)); // stale — out of order
        assert_eq!(
            pp.match_for(node(2)),
            Some(idx(5)),
            "matchIndex must never rewind (§5.3)",
        );
        assert_eq!(pp.next_for(node(2)), Some(idx(6)));
    }

    #[test]
    fn record_success_is_idempotent() {
        let mut pp = PeerProgress::new([node(2)], idx(5));
        pp.record_success(node(2), idx(3));
        pp.record_success(node(2), idx(3));
        assert_eq!(pp.match_for(node(2)), Some(idx(3)));
        assert_eq!(pp.next_for(node(2)), Some(idx(4)));
    }

    #[test]
    fn record_success_ignores_unknown_peer() {
        let mut pp = PeerProgress::new([node(2)], idx(5));
        pp.record_success(node(99), idx(3));
        assert_eq!(pp.match_for(node(99)), None);
    }

    // ---------------- record_conflict ----------------

    #[test]
    fn record_conflict_sets_next_to_hint() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        pp.record_conflict(node(2), idx(4));
        assert_eq!(pp.next_for(node(2)), Some(idx(4)));
    }

    #[test]
    fn record_conflict_does_not_touch_match_index() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        pp.record_success(node(2), idx(3)); // match = 3
        pp.record_conflict(node(2), idx(1));
        assert_eq!(
            pp.match_for(node(2)),
            Some(idx(3)),
            "conflict does not rewind match",
        );
    }

    #[test]
    fn record_conflict_never_goes_below_one() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        pp.record_conflict(node(2), LogIndex::ZERO);
        assert_eq!(
            pp.next_for(node(2)),
            Some(idx(1)),
            "nextIndex floor is 1; 0 is the pre-log sentinel",
        );
    }

    #[test]
    fn record_conflict_ignores_unknown_peer() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        pp.record_conflict(node(99), idx(1));
        assert!(pp.next_for(node(99)).is_none());
    }

    // ---------------- majority_index ----------------

    #[test]
    fn majority_index_three_node_cluster() {
        // Leader + 2 peers. Majority = 2. Position = 1 in sorted of 3.
        let mut pp = PeerProgress::new([node(2), node(3)], idx(7));
        pp.record_success(node(2), idx(5));
        pp.record_success(node(3), idx(3));
        // Sorted values: [3, 5, 7]. Pos 1 = 5.
        assert_eq!(pp.majority_index_over_voters(idx(7), &all_as_voters(&pp)), idx(5));
    }

    #[test]
    fn majority_index_five_node_cluster() {
        // Leader + 4 peers. Majority = 3. Position = 2 in sorted of 5.
        let peers = [node(2), node(3), node(4), node(5)];
        let mut pp = PeerProgress::new(peers, idx(7));
        pp.record_success(node(2), idx(5));
        pp.record_success(node(3), idx(5));
        pp.record_success(node(4), idx(3));
        pp.record_success(node(5), idx(2));
        // Sorted values: [2, 3, 5, 5, 7]. Pos 2 = 5.
        assert_eq!(pp.majority_index_over_voters(idx(7), &all_as_voters(&pp)), idx(5));
    }

    #[test]
    fn majority_index_without_replication_is_zero() {
        let pp = PeerProgress::new([node(2), node(3)], idx(7));
        // Sorted values: [0, 0, 7]. Pos 1 = 0 — leader alone isn't a majority.
        assert_eq!(pp.majority_index_over_voters(idx(7), &all_as_voters(&pp)), LogIndex::ZERO);
    }

    #[test]
    fn majority_index_with_no_peers_returns_leader_last() {
        let pp = PeerProgress::new(std::iter::empty(), idx(5));
        // Only the leader. Cluster of 1, majority of 1 = the leader itself.
        assert_eq!(pp.majority_index_over_voters(idx(5), &all_as_voters(&pp)), idx(5));
    }

    #[test]
    fn majority_index_in_even_cluster_takes_lower_middle() {
        // Leader + 3 peers = 4 total. Majority = 3. Position = 1 in sorted of 4.
        let peers = [node(2), node(3), node(4)];
        let mut pp = PeerProgress::new(peers, idx(7));
        pp.record_success(node(2), idx(5));
        pp.record_success(node(3), idx(5));
        pp.record_success(node(4), idx(2));
        // Sorted values: [2, 5, 5, 7]. Pos 1 = 5. Three of four are ≥ 5. ✓
        assert_eq!(pp.majority_index_over_voters(idx(7), &all_as_voters(&pp)), idx(5));
    }

    #[test]
    fn majority_index_pins_lower_middle_not_upper() {
        // Leader + 3 peers = 4 total, distinct matchIndex values so the
        // lower-middle (pos 1) and upper-middle (pos 2) disagree. Pins
        // the `(len - 1) / 2` indexing against the upper-middle mutant.
        let peers = [node(2), node(3), node(4)];
        let mut pp = PeerProgress::new(peers, idx(7));
        pp.record_success(node(2), idx(3));
        pp.record_success(node(3), idx(5));
        pp.record_success(node(4), idx(2));
        // Sorted values: [2, 3, 5, 7]. Pos 1 = 3 (three of four ≥ 3).
        // Upper-middle would be 5, but only two of four are ≥ 5, so 5
        // is NOT a safe commit point.
        assert_eq!(pp.majority_index_over_voters(idx(7), &all_as_voters(&pp)), idx(3));
    }

    // ---------------- record_success vs stale after conflict ----------------

    #[test]
    fn stale_record_success_does_not_rewind_next_index_after_conflict() {
        // After a successful replication, matchIndex=5, nextIndex=6. A
        // later conflict jumps nextIndex down to 3 to probe the peer's
        // log. A stale in-flight Success with last_appended=5 arrives.
        // matchIndex must stay 5 AND nextIndex must stay 3 — if we
        // accepted the stale ack (as `>=` would), nextIndex would
        // snap back to 6 and we'd re-send entries we're still probing.
        let peer = node(2);
        let mut pp = PeerProgress::new([peer], idx(10));
        pp.record_success(peer, idx(5));
        pp.record_conflict(peer, idx(3));
        assert_eq!(pp.next_for(peer), Some(idx(3)));
        pp.record_success(peer, idx(5));
        assert_eq!(pp.match_for(peer), Some(idx(5)));
        assert_eq!(
            pp.next_for(peer),
            Some(idx(3)),
            "stale Success must not rewind nextIndex past a live conflict",
        );
    }

    // ---------------- ensure_next_at_least ----------------

    #[test]
    fn ensure_next_at_least_raises_when_below_floor() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        pp.record_conflict(node(2), idx(3));
        assert_eq!(pp.next_for(node(2)), Some(idx(3)));
        pp.ensure_next_at_least(node(2), idx(7));
        assert_eq!(pp.next_for(node(2)), Some(idx(7)));
    }

    #[test]
    fn ensure_next_at_least_is_noop_when_already_at_floor() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        // Default nextIndex = 11.
        pp.ensure_next_at_least(node(2), idx(11));
        assert_eq!(pp.next_for(node(2)), Some(idx(11)));
    }

    #[test]
    fn ensure_next_at_least_is_noop_when_above_floor() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        // Default nextIndex = 11. floor=5 is below; next must not rewind.
        pp.ensure_next_at_least(node(2), idx(5));
        assert_eq!(pp.next_for(node(2)), Some(idx(11)));
    }

    #[test]
    fn ensure_next_at_least_ignores_unknown_peer() {
        let mut pp = PeerProgress::new([node(2)], idx(10));
        pp.ensure_next_at_least(node(99), idx(5));
        assert_eq!(pp.next_for(node(99)), None);
    }

    // ---------------- property: matchIndex monotonic ----------------

    proptest! {
        /// Any sequence of `record_success` calls must leave matchIndex
        /// non-decreasing across the whole run (per-peer).
        #[test]
        fn match_index_is_monotonically_non_decreasing(
            writes in proptest::collection::vec(0u64..50, 1..30),
        ) {
            let peer = node(2);
            let mut pp = PeerProgress::new([peer], idx(100));
            let mut last = LogIndex::ZERO;
            for w in writes {
                pp.record_success(peer, idx(w));
                let now = pp.match_for(peer).unwrap();
                prop_assert!(now >= last, "matchIndex went backward: {last:?} -> {now:?}");
                last = now;
            }
        }

        /// `majority_index` is bounded above by `leader_last_log` whenever
        /// inputs are protocol-valid (a peer's `matchIndex` is always ≤ the
        /// leader's log length — the leader only replicates entries it has).
        #[test]
        fn majority_index_never_exceeds_leader_last(
            leader_last in 1u64..20,
            match_ratios in proptest::collection::vec(0u64..=100, 1..6),
        ) {
            let peers: Vec<NodeId> = (2..(2 + match_ratios.len() as u64))
                .map(node)
                .collect();
            let mut pp = PeerProgress::new(peers.iter().copied(), idx(leader_last));
            for (peer, r) in peers.iter().zip(match_ratios.iter()) {
                // Scale each peer's matchIndex into [0, leader_last].
                let m = leader_last * r / 100;
                pp.record_success(*peer, idx(m));
            }
            let majority = pp.majority_index_over_voters(idx(leader_last), &all_as_voters(&pp));
            prop_assert!(majority <= idx(leader_last));
        }

        /// `majority_index` is invariant under permutation of peer
        /// insertion order — the set of (peer, match) pairs, not the
        /// order in which they arrived, determines the result.
        #[test]
        fn majority_index_stable_under_peer_order_permutation(
            leader_last in 1u64..30,
            matches in proptest::collection::vec(0u64..=30u64, 1..7),
            shuffle_seed in any::<u64>(),
        ) {
            // Build a canonical list of (peer_id, match_index) pairs.
            let pairs: Vec<(NodeId, u64)> = matches
                .iter()
                .enumerate()
                .map(|(i, m)| (node((i as u64) + 2), (*m).min(leader_last)))
                .collect();

            // Version A: insertion order is canonical.
            let peers_a: Vec<NodeId> = pairs.iter().map(|(p, _)| *p).collect();
            let mut pp_a = PeerProgress::new(peers_a.iter().copied(), idx(leader_last));
            for (p, m) in &pairs {
                pp_a.record_success(*p, idx(*m));
            }

            // Version B: apply the same pairs in a shuffled order.
            let mut shuffled = pairs.clone();
            // Deterministic shuffle keyed on `shuffle_seed`.
            let mut s = shuffle_seed.max(1);
            for i in (1..shuffled.len()).rev() {
                s ^= s << 13; s ^= s >> 7; s ^= s << 17;
                let j = usize::try_from(s % (i as u64 + 1)).unwrap_or(0);
                shuffled.swap(i, j);
            }
            let peers_b: Vec<NodeId> = shuffled.iter().map(|(p, _)| *p).collect();
            let mut pp_b = PeerProgress::new(peers_b.iter().copied(), idx(leader_last));
            for (p, m) in &shuffled {
                pp_b.record_success(*p, idx(*m));
            }

            prop_assert_eq!(
                pp_a.majority_index_over_voters(idx(leader_last), &all_as_voters(&pp_a)),
                pp_b.majority_index_over_voters(idx(leader_last), &all_as_voters(&pp_b)),
                "majority_index depends on insertion order",
            );
        }

        /// `majority_index` equals the classical definition: the
        /// largest N such that at least (cluster_size / 2 + 1) members
        /// have matchIndex >= N (leader included, leader's matchIndex
        /// = leader_last_log).
        #[test]
        fn majority_index_matches_classical_definition(
            leader_last in 1u64..20,
            matches in proptest::collection::vec(0u64..=20u64, 1..7),
        ) {
            let peers: Vec<NodeId> = (2..(2 + matches.len() as u64)).map(node).collect();
            let mut pp = PeerProgress::new(peers.iter().copied(), idx(leader_last));
            for (p, m) in peers.iter().zip(matches.iter()) {
                pp.record_success(*p, idx((*m).min(leader_last)));
            }

            let cluster_size = peers.len() + 1; // +1 for leader
            let threshold = cluster_size / 2 + 1;

            let majority = pp.majority_index_over_voters(idx(leader_last), &all_as_voters(&pp));

            // Collect all values including leader's.
            let mut all: Vec<u64> = peers
                .iter()
                .map(|p| pp.match_for(*p).unwrap().get())
                .collect();
            all.push(leader_last);

            // Classical: count how many are >= `majority`. Must be >= threshold.
            let count_at_or_above = all.iter().filter(|v| **v >= majority.get()).count();
            prop_assert!(
                count_at_or_above >= threshold,
                "majority {:?} lacks classical threshold ({count_at_or_above} < {threshold}) over {all:?}",
                majority,
            );
            // And `majority + 1` must NOT meet the threshold (else our
            // answer was unnecessarily low).
            let count_above = all.iter().filter(|v| **v > majority.get()).count();
            prop_assert!(
                count_above < threshold,
                "majority {:?} is not the maximum such N: {count_above} values are strictly greater (threshold {threshold})",
                majority,
            );
        }

        /// Under any adversarial interleaving of `record_success` and
        /// `record_conflict` calls, the peer's `matchIndex` is
        /// monotonically non-decreasing — `record_conflict` does not
        /// touch matchIndex at all, and stale success acks are
        /// rejected.
        #[test]
        fn match_index_monotonic_under_success_conflict_interleavings(
            ops in proptest::collection::vec(
                (any::<bool>(), 0u64..50u64),
                1..60,
            ),
        ) {
            let peer = node(2);
            let mut pp = PeerProgress::new([peer], idx(100));
            let mut last_match = LogIndex::ZERO;
            for (is_success, v) in ops {
                if is_success {
                    pp.record_success(peer, idx(v));
                } else {
                    pp.record_conflict(peer, idx(v));
                }
                let now = pp.match_for(peer).expect("peer known");
                prop_assert!(
                    now >= last_match,
                    "matchIndex regressed: {last_match:?} -> {now:?}",
                );
                last_match = now;
            }
        }
    }
}
