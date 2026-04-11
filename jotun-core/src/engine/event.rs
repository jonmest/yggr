use crate::engine::incoming::Incoming;
use crate::records::log_entry::ConfigChange;

/// The single input type the engine accepts via
/// [`crate::engine::engine::Engine::step`].
///
/// The four sources of forward motion in Raft, funneled through one
/// dispatch: the abstract clock fires (`Tick`), a peer's RPC arrives
/// (`Incoming`), the application submits a command (`ClientProposal`),
/// or an operator asks for a membership change (`ProposeConfigChange`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event<C> {
    /// One unit of abstract time has elapsed. Drives the election timer
    /// (followers/candidates) and the heartbeat interval (leaders).
    /// The caller decides what "one tick" means in wall-clock terms.
    Tick,
    /// A peer sent us an RPC.
    Incoming(Incoming<C>),
    /// The local application is asking us to replicate a command.
    ///
    /// Behaviour by role:
    ///  - **Leader**: appends at `(last+1, current_term)`, emits
    ///    [`crate::engine::action::Action::PersistLogEntries`] and
    ///    broadcasts `AppendEntries` to all peers immediately.
    ///  - **Follower** with a known leader (set by the most recent
    ///    accepted `AppendEntries`): emits
    ///    [`crate::engine::action::Action::Redirect`] so the host can
    ///    forward the client.
    ///  - **Follower** without a known leader, or **Candidate**: drops
    ///    silently. The host should retry on its own cadence.
    ClientProposal(C),
    /// Operator-initiated single-server membership change (§4.3).
    ///
    /// Same role-by-role behaviour as `ClientProposal`, with one extra
    /// rule: a leader refuses if it already has an uncommitted
    /// `ConfigChange` in its log, or if the change is a no-op (adding
    /// an existing member, removing a non-member). On accept, the
    /// active config mutates immediately (pre-commit) per §4.3.
    ProposeConfigChange(ConfigChange),
}
