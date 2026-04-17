use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[must_use]
pub struct LogIndex(u64);

impl LogIndex {
    pub const ZERO: Self = Self(0);

    pub fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "log_index:{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use]
pub struct CommitIndex(LogIndex);

impl CommitIndex {
    pub fn new(index: LogIndex) -> Self {
        Self(index)
    }

    pub fn get(self) -> LogIndex {
        self.0
    }
}

impl fmt::Display for CommitIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "commit_index:{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use]
pub struct AppliedIndex(LogIndex);

impl AppliedIndex {
    pub fn new(index: LogIndex) -> Self {
        Self(index)
    }

    pub fn get(self) -> LogIndex {
        self.0
    }
}

impl fmt::Display for AppliedIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "applied_index:{}", self.0)
    }
}
