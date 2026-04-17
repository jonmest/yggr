use crate::records::{
    append_entries::{AppendEntriesResponse, RequestAppendEntries},
    vote::{RequestVote, VoteResponse},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<C> {
    VoteRequest(RequestVote),
    VoteResponse(VoteResponse),
    AppendEntriesRequest(RequestAppendEntries<C>),
    AppendEntriesResponse(AppendEntriesResponse),
}
