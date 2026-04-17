use crate::engine::incoming::Incoming;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event<C> {
    Tick,
    Incoming(Incoming<C>),
    ClientProposal(C),
}
