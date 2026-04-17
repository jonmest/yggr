use crate::{
    records::log_entry::LogEntry,
    types::{index::LogIndex, log::LogId, node::NodeId, term::Term},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestAppendEntries<C> {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_id: Option<LogId>,
    pub entries: Vec<LogEntry<C>>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub result: AppendEntriesResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendEntriesResult {
    Success { last_appended: Option<LogIndex> },
    Conflict { next_index_hint: LogIndex },
}
