use crate::engine::incoming::Incoming;

/// The single input type the engine accepts via
/// [`crate::engine::engine::Engine::step`].
///
/// Three sources of forward motion in Raft, all funneled through one
/// dispatch: the abstract clock fires (`Tick`), a peer's RPC arrives
/// (`Incoming`), or the application submits a command (`ClientProposal`).
/// Anything that doesn't fit one of these three is something the engine
/// shouldn't be involved in.
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
}
