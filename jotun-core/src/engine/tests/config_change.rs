//! Tests for §4.3 single-server membership changes via the
//! `Event::ProposeConfigChange` entry point.

use super::fixtures::{
    append_entries_from, append_entries_request, append_entries_success_from,
    collect_persist_log_entries, collect_redirects, follower, follower_with_env, log_id, node,
    propose_add_peer, propose_remove_peer, term, vote_response_from,
};
use crate::engine::engine::Engine;
use crate::engine::env::StaticEnv;
use crate::engine::event::Event;
use crate::engine::role_state::RoleState;
use crate::records::log_entry::{ConfigChange, LogPayload};
use crate::types::index::LogIndex;

/// 3-node leader (self=1, peers=2,3) at term 1, log = [(1,1)=Noop].
fn elected_leader_3_node() -> Engine<Vec<u8>> {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick);
    engine.step(vote_response_from(2, 1, true));
    assert!(matches!(engine.role(), RoleState::Leader(_)));
    engine
}

// ---------------------------------------------------------------------------
// Leader-side accept path
// ---------------------------------------------------------------------------

#[test]
fn leader_appends_add_peer_at_current_term_and_mutates_active_config() {
    let mut engine = elected_leader_3_node();
    let term_now = engine.current_term();
    let last_before = engine.log().last_log_id().unwrap().index;
    assert!(!engine.peers().contains(&node(4)));

    engine.step(propose_add_peer(4));

    let last_after = engine.log().last_log_id().unwrap();
    assert_eq!(last_after.index, last_before.next());
    assert_eq!(last_after.term, term_now);
    let entry = engine.log().entry_at(last_after.index).unwrap();
    assert!(matches!(
        entry.payload,
        LogPayload::ConfigChange(ConfigChange::AddPeer(id)) if id == node(4)
    ));
    // §4.3: active config mutates immediately, before commit.
    assert!(engine.peers().contains(&node(4)));
}

#[test]
fn leader_appends_remove_peer_and_drops_it_from_active_config() {
    let mut engine = elected_leader_3_node();
    assert!(engine.peers().contains(&node(3)));

    engine.step(propose_remove_peer(3));

    assert!(!engine.peers().contains(&node(3)));
    let last = engine.log().last_log_id().unwrap();
    let entry = engine.log().entry_at(last.index).unwrap();
    assert!(matches!(
        entry.payload,
        LogPayload::ConfigChange(ConfigChange::RemovePeer(id)) if id == node(3)
    ));
}

#[test]
fn leader_persists_config_change_entry_before_broadcast() {
    let mut engine = elected_leader_3_node();
    let actions = engine.step(propose_add_peer(4));

    let persisted = collect_persist_log_entries(&actions);
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].len(), 1);
    assert!(matches!(
        persisted[0][0].payload,
        LogPayload::ConfigChange(_)
    ));
}

#[test]
fn add_peer_immediately_replicates_to_the_new_peer() {
    let mut engine = elected_leader_3_node();
    let actions = engine.step(propose_add_peer(4));
    let appends = super::fixtures::collect_append_entries(&actions);
    let recipients: std::collections::BTreeSet<_> = appends.iter().map(|(p, _)| p.get()).collect();
    assert!(
        recipients.contains(&4),
        "newly-added peer must be in the broadcast: {recipients:?}",
    );
}

#[test]
fn remove_peer_stops_replicating_to_the_removed_peer() {
    let mut engine = elected_leader_3_node();
    let actions = engine.step(propose_remove_peer(3));
    let appends = super::fixtures::collect_append_entries(&actions);
    let recipients: std::collections::BTreeSet<_> = appends.iter().map(|(p, _)| p.get()).collect();
    assert!(
        !recipients.contains(&3),
        "removed peer must not appear in the broadcast: {recipients:?}",
    );
}

// ---------------------------------------------------------------------------
// Leader-side refusal paths
// ---------------------------------------------------------------------------

#[test]
fn leader_refuses_second_config_change_while_first_is_uncommitted() {
    // First Add lands in the log uncommitted. Second Add must be a no-op.
    let mut engine = elected_leader_3_node();
    engine.step(propose_add_peer(4));
    let log_len_before = engine.log().len();

    let actions = engine.step(propose_add_peer(5));

    assert_eq!(
        engine.log().len(),
        log_len_before,
        "second CC must not be appended while first is uncommitted",
    );
    assert!(actions.is_empty(), "refused proposals emit no actions");
}

#[test]
fn leader_accepts_second_config_change_after_first_commits() {
    let mut engine = elected_leader_3_node();
    // Propose Add 4; the leader appends and includes 4 in its progress.
    engine.step(propose_add_peer(4));
    // Both peers (3 and the new 4) ack at the new last index. Need majority
    // of new config (4 nodes, majority = 3): self + 3 + 4 = 3.
    let new_last = engine.log().last_log_id().unwrap().index.get();
    engine.step(append_entries_success_from(3, 1, new_last));
    engine.step(append_entries_success_from(4, 1, new_last));
    // CC should now be committed.
    assert!(engine.commit_index().get() >= new_last);

    // Second proposal should now be accepted.
    let log_len_before = engine.log().len();
    engine.step(propose_remove_peer(4));
    assert_eq!(engine.log().len(), log_len_before + 1);
}

#[test]
fn leader_refuses_add_of_existing_member() {
    let mut engine = elected_leader_3_node();
    let log_len_before = engine.log().len();
    let actions = engine.step(propose_add_peer(2)); // 2 already a peer
    assert_eq!(engine.log().len(), log_len_before);
    assert!(actions.is_empty());
}

#[test]
fn leader_refuses_remove_of_non_member() {
    let mut engine = elected_leader_3_node();
    let log_len_before = engine.log().len();
    let actions = engine.step(propose_remove_peer(99));
    assert_eq!(engine.log().len(), log_len_before);
    assert!(actions.is_empty());
}

#[test]
fn leader_refuses_add_of_self() {
    let mut engine = elected_leader_3_node();
    let log_len_before = engine.log().len();
    let actions = engine.step(propose_add_peer(1)); // self
    assert_eq!(engine.log().len(), log_len_before);
    assert!(actions.is_empty());
}

// ---------------------------------------------------------------------------
// Non-leader paths — redirect / drop
// ---------------------------------------------------------------------------

#[test]
fn follower_with_known_leader_redirects_config_change_proposal() {
    let mut engine = follower(1);
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![], 0),
    ));
    let actions = engine.step(propose_add_peer(4));
    assert_eq!(collect_redirects(&actions), vec![node(2)]);
    assert!(engine.log().is_empty());
}

#[test]
fn follower_without_known_leader_drops_config_change_proposal() {
    let mut engine = follower(1);
    let actions = engine.step(propose_add_peer(4));
    assert!(actions.is_empty());
    assert!(engine.log().is_empty());
}

#[test]
fn candidate_drops_config_change_proposal() {
    let env = StaticEnv(1);
    let mut engine = follower_with_env(1, &[2, 3], Box::new(env));
    engine.step(Event::Tick); // → Candidate
    assert!(matches!(engine.role(), RoleState::Candidate(_)));
    let actions = engine.step(propose_add_peer(4));
    assert!(actions.is_empty());
}

// ---------------------------------------------------------------------------
// Follower-side: receiving a CC entry mutates active config
// ---------------------------------------------------------------------------

#[test]
fn follower_applying_a_config_change_entry_mutates_active_config() {
    let mut engine = follower(1);
    assert!(!engine.peers().contains(&node(4)));

    let entry = crate::records::log_entry::LogEntry {
        id: log_id(1, 1),
        payload: LogPayload::ConfigChange(ConfigChange::AddPeer(node(4))),
    };
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![entry], 0),
    ));
    assert!(engine.peers().contains(&node(4)));
}

#[test]
fn truncating_a_config_change_entry_reverts_active_config() {
    // Follower accepts an Add(4) at index 1 from leader=2 at term 1.
    // peers becomes {2, 3, 4}. Then a fresh leader at term 2 sends a
    // conflicting entry at index 1 (a Noop, say) — we truncate the
    // pending Add and must drop 4 from the active config.
    let mut engine = follower(1);
    let add4 = crate::records::log_entry::LogEntry {
        id: log_id(1, 1),
        payload: LogPayload::ConfigChange(ConfigChange::AddPeer(node(4))),
    };
    engine.step(append_entries_from(
        2,
        append_entries_request(1, 2, None, vec![add4], 0),
    ));
    assert!(engine.peers().contains(&node(4)));

    // New leader at term 2 sends a Noop at index 1.
    let conflicting = crate::records::log_entry::LogEntry {
        id: log_id(1, 2),
        payload: LogPayload::Noop,
    };
    engine.step(append_entries_from(
        3,
        append_entries_request(2, 3, None, vec![conflicting], 0),
    ));
    assert!(
        !engine.peers().contains(&node(4)),
        "truncate-and-replace of the CC entry must revert the active config",
    );
}

// ---------------------------------------------------------------------------
// Self-removal step-down
// ---------------------------------------------------------------------------

#[test]
fn leader_steps_down_when_committing_its_own_removal() {
    let mut engine = elected_leader_3_node();
    // Self-remove. Active config drops to {2, 3} — but progress map drops
    // self of course. Wait: self isn't in `peers` to begin with; removing
    // self only affects the engine's own membership status, not the
    // PeerProgress map.
    engine.step(propose_remove_peer(1));

    // The CC entry is at log index 2 (after the noop). Need majority of
    // the *new* config — with self removed the remaining members are
    // {2, 3}, majority 2, both peers must ack.
    let new_last = engine.log().last_log_id().unwrap().index.get();
    engine.step(append_entries_success_from(2, 1, new_last));
    engine.step(append_entries_success_from(3, 1, new_last));

    // The CC commits → leader steps down.
    assert!(
        matches!(engine.role(), RoleState::Follower(_)),
        "leader must step down on committing its own removal, got {:?}",
        engine.role(),
    );
    assert_eq!(engine.commit_index(), LogIndex::new(new_last));
    // Term unchanged; voted_for unchanged (per same-term step-down rule).
    assert_eq!(engine.current_term(), term(1));
}

#[test]
fn removing_a_non_leader_peer_does_not_step_down_the_leader() {
    let mut engine = elected_leader_3_node();
    engine.step(propose_remove_peer(3));
    let new_last = engine.log().last_log_id().unwrap().index.get();
    // After remove, new config is {1, 2}; majority = 2 = self + peer 2.
    engine.step(append_entries_success_from(2, 1, new_last));
    assert!(
        matches!(engine.role(), RoleState::Leader(_)),
        "removing a non-self peer must not step the leader down",
    );
}
