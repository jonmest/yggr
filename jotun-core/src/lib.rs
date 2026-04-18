//! Jotun Core — a Raft consensus engine as a pure state machine.
//!
//! The engine takes [`Event`]s and returns [`Action`]s; it never performs I/O
//! directly. The host owns sockets, disk, and time, and is
//! responsible for delivering [`Event`]s in whatever order it sees fit and
//! carrying out the [`Action`]s the engine emits.
//!
//! # Minimum viable setup
//!
//! ```ignore
//! use jotun_core::{Engine, Env, NodeId, StaticEnv};
//!
//! let me = NodeId::new(1).unwrap();
//! let peers = [NodeId::new(2).unwrap(), NodeId::new(3).unwrap()];
//! let env: Box<dyn Env> = Box::new(StaticEnv(15));
//! let mut engine: Engine<Vec<u8>> = Engine::new(me, peers, env, 5);
//! ```
//!
//! See [`Engine::step`] for the core loop.

#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing,)
)]

pub mod engine;
pub mod records;
pub mod transport;
pub mod types;

// Curated flat re-exports. Deep paths still work; these exist so external
// users can `use jotun_core::{Engine, Event, Action, NodeId}` without
// memorising the internal module layout.

pub use engine::action::Action;
pub use engine::engine::Engine;
pub use engine::env::{Env, RandomizedEnv, StaticEnv};
pub use engine::event::Event;
pub use engine::incoming::Incoming;
pub use engine::role_state::{CandidateState, FollowerState, LeaderState, RoleState};

pub use records::append_entries::{
    AppendEntriesResponse, AppendEntriesResult, RequestAppendEntries,
};
pub use records::install_snapshot::{InstallSnapshotResponse, RequestInstallSnapshot};
pub use records::log_entry::{ConfigChange, LogEntry, LogPayload};
pub use records::message::Message;
pub use records::vote::{RequestVote, VoteResponse, VoteResult};

pub use types::index::LogIndex;
pub use types::log::LogId;
pub use types::node::NodeId;
pub use types::term::Term;

pub use transport::ConvertError;
