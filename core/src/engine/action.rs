use crate::{
    records::{log_entry::LogEntry, message::Message},
    types::{index::LogIndex, node::NodeId},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action<C> {
    Send { to: NodeId, message: Message<C> },
    PersistState,
    AppendLogEntries(Vec<LogEntry<C>>),
    ApplyUpTo(LogIndex),
}
