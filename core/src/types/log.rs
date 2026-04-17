use crate::types::{index::LogIndex, term::Term};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogId {
    pub index: LogIndex,
    pub term: Term,
}

impl LogId {
    #[must_use]
    pub fn new(index: LogIndex, term: Term) -> Self {
        Self { index, term }
    }
}
