use super::action::Action;
use super::engine::{Engine, RecoveredHardState};
use super::env::StaticEnv;
use super::event::Event;
use super::incoming::Incoming;
use crate::records::append_entries::RequestAppendEntries;
use crate::records::log_entry::{LogEntry, LogPayload};
use crate::records::message::Message;
use crate::records::vote::{RequestVote, VoteResponse, VoteResult};
use crate::types::index::LogIndex;
use crate::types::log::LogId;
use crate::types::node::NodeId;
use crate::types::term::Term;

fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("hard-coded node ids are non-zero")
}

fn small_peer(selector: u8) -> NodeId {
    if selector & 1 == 0 { node(2) } else { node(3) }
}

fn bounded_term(selector: u8) -> Term {
    Term::new(u64::from(selector % 4))
}

fn bounded_append_entries(selector: u8, aux: u8) -> Event<Vec<u8>> {
    let peer = small_peer(selector);
    let term = bounded_term(selector >> 1);
    let with_entry = aux & 1 == 1;
    let entries = if with_entry {
        let entry_term = if term == Term::ZERO {
            Term::new(1)
        } else {
            term
        };
        vec![LogEntry {
            id: LogId::new(LogIndex::new(1), entry_term),
            payload: LogPayload::Command(vec![aux]),
        }]
    } else {
        Vec::new()
    };
    Event::Incoming(Incoming {
        from: peer,
        message: Message::AppendEntriesRequest(RequestAppendEntries {
            term,
            leader_id: peer,
            prev_log_id: None,
            entries,
            leader_commit: LogIndex::ZERO,
        }),
    })
}

fn bounded_vote_request(selector: u8) -> Event<Vec<u8>> {
    let candidate = small_peer(selector);
    Event::Incoming(Incoming {
        from: candidate,
        message: Message::VoteRequest(RequestVote {
            term: bounded_term(selector >> 1),
            candidate_id: candidate,
            last_log_id: None,
        }),
    })
}

fn bounded_event(kind: u8, selector: u8, aux: u8) -> Event<Vec<u8>> {
    match kind % 3 {
        0 => Event::Tick,
        1 => bounded_vote_request(selector),
        _ => bounded_append_entries(selector, aux),
    }
}

fn granted_vote(actions: &[Action<Vec<u8>>]) -> Option<NodeId> {
    actions.iter().find_map(|action| match action {
        Action::Send {
            to,
            message:
                Message::VoteResponse(VoteResponse {
                    result: VoteResult::Granted,
                    ..
                }),
        } => Some(*to),
        _ => None,
    })
}

#[kani::proof]
fn recover_from_round_trips_hard_state_proof() {
    let term_n: u8 = kani::any();
    let voted_for_selector: u8 = kani::any();
    let voted_for = match voted_for_selector % 3 {
        0 => None,
        1 => Some(node(2)),
        _ => Some(node(3)),
    };

    let mut engine = Engine::new(node(1), vec![node(2), node(3)], Box::new(StaticEnv(10)), 3);
    engine.recover_from(RecoveredHardState {
        current_term: Term::new(u64::from(term_n)),
        voted_for,
        snapshot: None,
        post_snapshot_log: Vec::new(),
    });

    assert_eq!(engine.current_term(), Term::new(u64::from(term_n)));
    assert_eq!(engine.voted_for(), voted_for);
}

#[kani::proof]
fn term_and_commit_are_monotonic_across_two_small_events() {
    let first_kind: u8 = kani::any();
    let first_selector: u8 = kani::any();
    let first_aux: u8 = kani::any();
    let second_kind: u8 = kani::any();
    let second_selector: u8 = kani::any();
    let second_aux: u8 = kani::any();

    let mut engine = Engine::new(node(1), vec![node(2), node(3)], Box::new(StaticEnv(10)), 3);
    let initial_term = engine.current_term();
    let initial_commit = engine.commit_index();

    let _ = engine.step(bounded_event(first_kind, first_selector, first_aux));
    let after_first_term = engine.current_term();
    let after_first_commit = engine.commit_index();

    let _ = engine.step(bounded_event(second_kind, second_selector, second_aux));
    let after_second_term = engine.current_term();
    let after_second_commit = engine.commit_index();

    assert!(after_first_term >= initial_term);
    assert!(after_second_term >= after_first_term);
    assert!(after_first_commit >= initial_commit);
    assert!(after_second_commit >= after_first_commit);
}

#[kani::proof]
fn granted_votes_stay_term_unique_for_two_requests() {
    let term_selector: u8 = kani::any();
    let first_candidate_selector: u8 = kani::any();
    let second_candidate_selector: u8 = kani::any();
    let term = Term::new(u64::from((term_selector % 3) + 1));
    let first_candidate = small_peer(first_candidate_selector);
    let second_candidate = small_peer(second_candidate_selector);

    let mut engine = Engine::new(node(1), vec![node(2), node(3)], Box::new(StaticEnv(10)), 3);

    let first_actions = engine.step(Event::Incoming(Incoming {
        from: first_candidate,
        message: Message::VoteRequest(RequestVote {
            term,
            candidate_id: first_candidate,
            last_log_id: None,
        }),
    }));
    let second_actions = engine.step(Event::Incoming(Incoming {
        from: second_candidate,
        message: Message::VoteRequest(RequestVote {
            term,
            candidate_id: second_candidate,
            last_log_id: None,
        }),
    }));

    let first_grant = granted_vote(&first_actions);
    let second_grant = granted_vote(&second_actions);
    if first_grant.is_some() && second_grant.is_some() {
        assert_eq!(first_grant, second_grant);
    }
}
