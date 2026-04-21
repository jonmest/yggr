use crate::records::{
    append_entries::{AppendEntriesResponse, RequestAppendEntries},
    install_snapshot::{InstallSnapshotResponse, RequestInstallSnapshot},
    pre_vote::{PreVoteResponse, RequestPreVote},
    timeout_now::TimeoutNow,
    vote::{RequestVote, VoteResponse},
};

/// All inter-node messages the engine speaks. The wire format
/// ([`crate::transport::protobuf::Message`]) maps one-to-one onto these
/// variants — what comes off the wire is decoded into a `Message<C>`,
/// validated, and then handed to the engine wrapped in
/// [`crate::engine::incoming::Incoming`].
///
/// Three request/response pairs:
///  - `VoteRequest` / `VoteResponse` — leader election (§5.2).
///  - `AppendEntriesRequest` / `AppendEntriesResponse` — log replication,
///    heartbeats, and commit-index propagation (§5.3).
///  - `InstallSnapshotRequest` / `InstallSnapshotResponse` — snapshot
///    catch-up for followers whose `nextIndex` falls below the leader's
///    log floor (§7).
///  - `TimeoutNow` — leadership transfer after the leader has caught a
///    chosen follower up to its tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<C> {
    /// A candidate is asking for our vote.
    VoteRequest(RequestVote),
    /// Reply to one of our outgoing `VoteRequest`s.
    VoteResponse(VoteResponse),
    /// A leader is replicating entries (or sending a heartbeat).
    AppendEntriesRequest(RequestAppendEntries<C>),
    /// Reply to one of our outgoing `AppendEntriesRequest`s.
    AppendEntriesResponse(AppendEntriesResponse),
    /// A leader is sending us a snapshot to catch us up past its log
    /// floor (§7).
    InstallSnapshotRequest(RequestInstallSnapshot),
    /// Reply to one of our outgoing `InstallSnapshotRequest`s.
    InstallSnapshotResponse(InstallSnapshotResponse),
    /// Leadership-transfer request: start an election immediately.
    TimeoutNow(TimeoutNow),
    /// Pre-vote probe (§9.6): would you vote for me at `term` if I
    /// asked? Non-disruptive — receipt does not update the peer's
    /// term or `voted_for`.
    PreVoteRequest(RequestPreVote),
    /// Reply to a pre-vote probe.
    PreVoteResponse(PreVoteResponse),
}
