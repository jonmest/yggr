//! Tests for [`Engine::on_vote_response`] and the candidate → leader
//! transition. §5.2.

use super::fixtures::{follower, follower_with_env, node, term, vote_response_from};
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::types::index::LogIndex;
use crate::types::term::Term;

/// Drive a fresh follower into Candidate state for the given cluster, at
/// term 1. Uses StaticEnv(1) so the first Tick fires the election.
fn candidate_in_cluster(self_id: u64, peers: &[u64]) -> crate::engine::engine::Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(self_id, peers, Box::new(env));
    engine.step(Event::Tick);
    assert!(
        matches!(engine.role(), RoleState::Candidate(_)),
        "precondition: must be in Candidate state",
    );
    engine
}

// ---------------------------------------------------------------------------
// Vote tally
// ---------------------------------------------------------------------------

#[test]
fn granted_response_records_voter() {
    // 5-node cluster: self + 4 peers. Majority = 3, so we need 2 grants.
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);

    engine.step(vote_response_from(2, 1, true));

    // Still candidate (need one more grant), but vote was recorded.
    match engine.role() {
        RoleState::Candidate(s) => assert!(
            s.votes_granted.contains(&node(2)),
            "voter 2's grant must be recorded",
        ),
        other => panic!("expected Candidate, got {other:?}"),
    }
}

#[test]
fn duplicate_grant_from_same_peer_is_idempotent() {
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);

    engine.step(vote_response_from(2, 1, true));
    engine.step(vote_response_from(2, 1, true));

    match engine.role() {
        RoleState::Candidate(s) => assert_eq!(
            s.votes_granted.len(),
            2,
            "self + one unique peer (set semantics dedupe duplicates)",
        ),
        other => panic!("expected Candidate, got {other:?}"),
    }
}

#[test]
fn rejected_response_does_not_count_toward_majority() {
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);

    engine.step(vote_response_from(2, 1, false));
    engine.step(vote_response_from(3, 1, false));

    match engine.role() {
        RoleState::Candidate(s) => assert_eq!(
            s.votes_granted.len(),
            1,
            "only self's vote; rejections don't count",
        ),
        other => panic!("expected Candidate, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Term handling
// ---------------------------------------------------------------------------

#[test]
fn higher_term_response_steps_down_to_follower() {
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);
    assert_eq!(engine.current_term(), Term::new(1));

    engine.step(vote_response_from(2, 5, false));

    assert_eq!(engine.current_term(), term(5), "term caught up");
    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "candidate must step down on higher-term response",
    );
    assert_eq!(engine.voted_for(), None, "vote cleared on term change");
}

#[test]
fn stale_term_response_is_ignored() {
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);
    // Now bump our term: simulate an unrelated higher-term event.
    engine.step(vote_response_from(99, 7, false));
    // Above stepped us down; rejoin candidacy at a known term.
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);
    // After candidate_in_cluster, term = 1.

    // A stale grant from a previous-term election arrives.
    engine.step(vote_response_from(2, 0, true));

    match engine.role() {
        RoleState::Candidate(s) => assert_eq!(
            s.votes_granted.len(),
            1,
            "stale-term grant must be discarded",
        ),
        other => panic!("expected Candidate, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Late responses (we already left Candidate)
// ---------------------------------------------------------------------------

#[test]
fn late_response_after_becoming_leader_is_ignored() {
    // 3-node cluster: self + 2. Majority = 2, so one peer grant wins.
    let mut engine = candidate_in_cluster(1, &[2, 3]);

    engine.step(vote_response_from(2, 1, true));
    assert!(
        matches!(engine.role(), RoleState::Leader(_)),
        "should be leader after one peer grant in 3-node cluster",
    );

    // Late grant from peer 3.
    engine.step(vote_response_from(3, 1, true));

    assert!(
        matches!(engine.role(), RoleState::Leader(_)),
        "late grant must not perturb leader state",
    );
}

#[test]
fn late_response_after_becoming_follower_is_ignored() {
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);

    // Get knocked back to follower by a higher-term message from a peer.
    engine.step(vote_response_from(5, 5, false));
    assert!(matches!(engine.role(), RoleState::Follower(_)));

    // A grant from the old term arrives late.
    engine.step(vote_response_from(2, 1, true));

    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "follower role must not change on stale grant",
    );
}

// ---------------------------------------------------------------------------
// Majority transition → leader
// ---------------------------------------------------------------------------

#[test]
fn three_node_cluster_needs_one_grant_to_win() {
    let mut engine = candidate_in_cluster(1, &[2, 3]);

    engine.step(vote_response_from(2, 1, true));

    assert!(
        matches!(engine.role(), RoleState::Leader(_)),
        "3-node cluster: self + 1 grant = majority",
    );
}

#[test]
fn five_node_cluster_needs_two_grants_to_win() {
    let mut engine = candidate_in_cluster(1, &[2, 3, 4, 5]);

    engine.step(vote_response_from(2, 1, true));
    assert!(
        matches!(engine.role(), RoleState::Candidate(_)),
        "one grant + self = 2; need 3 for majority",
    );

    engine.step(vote_response_from(3, 1, true));
    assert!(
        matches!(engine.role(), RoleState::Leader(_)),
        "two grants + self = 3 = majority",
    );
}

#[test]
fn becoming_leader_initializes_peer_progress_correctly() {
    // Empty pre-election log; become_leader appends a no-op at index 1
    // (§5.4.2), so peers' nextIndex starts at 2 (last_log_index + 1).
    let mut engine = candidate_in_cluster(1, &[2, 3]);
    engine.step(vote_response_from(2, 1, true));

    match engine.role() {
        RoleState::Leader(s) => {
            assert_eq!(
                s.progress.peer_count(),
                2,
                "progress tracks both non-self peers",
            );
            for peer in [node(2), node(3)] {
                assert_eq!(
                    s.progress.next_for(peer),
                    Some(LogIndex::new(2)),
                    "empty pre-election log + leader noop → nextIndex starts at 2",
                );
                assert_eq!(
                    s.progress.match_for(peer),
                    Some(LogIndex::ZERO),
                    "matchIndex starts at 0",
                );
            }
        }
        other => panic!("expected Leader, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Single-node cluster — self-elects via start_election
// ---------------------------------------------------------------------------

#[test]
fn single_node_cluster_self_elects_on_first_tick() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[], Box::new(env));

    engine.step(Event::Tick);

    assert!(
        matches!(engine.role(), RoleState::Leader(_)),
        "single-node cluster reaches majority immediately on candidacy",
    );
    assert_eq!(engine.current_term(), Term::new(1));
}

// ---------------------------------------------------------------------------
// Defensive: response shouldn't reach handler when not a candidate
// ---------------------------------------------------------------------------

#[test]
fn response_arriving_at_a_fresh_follower_is_ignored() {
    let mut engine = follower(1);

    // Same-term response (term 0). The handler hits the "not Candidate"
    // early-return without any state mutation.
    engine.step(vote_response_from(2, 0, true));

    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "fresh follower must not transition on stray vote response",
    );
    assert_eq!(engine.current_term(), Term::ZERO);
    assert_eq!(engine.voted_for(), None);
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

use proptest::prelude::*;

/// Build the peer id list `[2, 3, ..., peer_count + 1]`.
fn peers_of_size(count: usize) -> Vec<u64> {
    (2..(2 + count as u64)).collect()
}

proptest! {
    /// `cluster_majority` matches the textbook formula `N/2 + 1`
    /// where `N = peers + 1` (cluster size including self).
    #[test]
    fn cluster_majority_is_strict_majority(peer_count in 0usize..20) {
        let env = StaticEnv(10);
        let peers: Vec<u64> = peers_of_size(peer_count);
        let engine = follower_with_env(1, &peers, Box::new(env));
        let cluster_size = peer_count + 1;
        let majority = engine.cluster_majority();

        // A majority is more than half of the cluster.
        prop_assert!(2 * majority > cluster_size,
            "majority {} is not strictly more than half of cluster size {}",
            majority, cluster_size);
        // ... and one less is not.
        prop_assert!(2 * (majority - 1) <= cluster_size,
            "majority - 1 = {} is somehow still a majority of {}",
            majority - 1, cluster_size);
    }

    /// Receiving exactly `majority - 1` distinct peer grants always wins.
    /// Combined with self's implicit vote, that's the majority threshold.
    #[test]
    fn exactly_majority_minus_one_distinct_grants_elects_leader(
        peer_count in 2usize..10,
    ) {
        let peers = peers_of_size(peer_count);
        let mut engine = candidate_in_cluster(1, &peers);
        let majority = engine.cluster_majority();
        let needed_grants = majority - 1;

        // Grant from the first `needed_grants` peers, in order.
        for &peer in peers.iter().take(needed_grants) {
            engine.step(vote_response_from(peer, 1, true));
        }

        prop_assert!(matches!(engine.role(), RoleState::Leader(_)),
            "expected Leader after {} grants in cluster of {}; got {:?}",
            needed_grants, peer_count + 1, engine.role());
    }

    /// Grants below the majority threshold leave us as Candidate.
    #[test]
    fn fewer_than_majority_grants_stays_candidate(
        peer_count in 2usize..10,
    ) {
        let peers = peers_of_size(peer_count);
        let mut engine = candidate_in_cluster(1, &peers);
        let majority = engine.cluster_majority();
        // One fewer grant than required.
        prop_assume!(majority >= 2);
        let too_few = majority - 2;

        for &peer in peers.iter().take(too_few) {
            engine.step(vote_response_from(peer, 1, true));
        }

        prop_assert!(matches!(engine.role(), RoleState::Candidate(_)),
            "expected Candidate with only {} grants in cluster of {}",
            too_few, peer_count + 1);
    }

    /// Duplicate grants from the same peer don't push us over the line.
    /// The set semantics are what makes the majority count safe.
    #[test]
    fn duplicate_grants_cannot_fake_a_majority(
        peer_count in 4usize..10,
        duplicates in 1usize..30,
    ) {
        // 5+ peers ⇒ majority ≥ 3 ⇒ need at least 2 grants. We send 1
        // peer's grant repeatedly and assert we never become leader.
        let peers = peers_of_size(peer_count);
        let mut engine = candidate_in_cluster(1, &peers);
        prop_assume!(engine.cluster_majority() >= 3);

        for _ in 0..duplicates {
            engine.step(vote_response_from(peers[0], 1, true));
        }

        prop_assert!(matches!(engine.role(), RoleState::Candidate(_)),
            "duplicate grants from one peer must not produce a majority");
    }

    /// Any response with term > current_term unconditionally demotes the
    /// engine to Follower at that new term, regardless of prior role.
    #[test]
    fn higher_term_response_always_demotes_candidate(
        peer_count in 2usize..10,
        higher_term in 2u64..100,
    ) {
        let peers = peers_of_size(peer_count);
        let mut engine = candidate_in_cluster(1, &peers);
        // Candidate is at term 1; pick anything strictly greater.
        engine.step(vote_response_from(2, higher_term, false));

        prop_assert!(matches!(engine.role(), RoleState::Follower(_)));
        prop_assert_eq!(engine.current_term(), Term::new(higher_term));
        prop_assert_eq!(engine.voted_for(), None,
            "term catch-up clears the prior vote");
    }
}
