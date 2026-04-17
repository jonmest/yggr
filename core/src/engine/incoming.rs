use crate::{records::message::Message, types::node::NodeId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incoming<C> {
    pub from: NodeId,
    pub message: Message<C>,
}
