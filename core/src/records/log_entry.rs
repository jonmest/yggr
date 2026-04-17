use crate::types::log::LogId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry<C> {
    pub id: LogId,
    pub command: C,
}
