use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(std::num::NonZeroU64);

impl NodeId {
    #[must_use]
    pub fn new(id: u64) -> Option<Self> {
        Some(Self(std::num::NonZeroU64::new(id)?))
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node:{}", self.0)
    }
}
