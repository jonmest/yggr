use crate::{records::message::Message, types::node::NodeId};

/// An RPC arriving from a peer, paired with the sender's id.
///
/// The engine never trusts the wire blindly: `from` is the
/// transport-authenticated sender, used in places where the message body
/// doesn't carry it (e.g., [`crate::records::vote::VoteResponse`] doesn't
/// include the voter's id; we get it from `from`). The host transport
/// layer is responsible for ensuring `from` is genuine before handing
/// the `Incoming` to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incoming<C> {
    /// Authenticated sender, supplied by the host transport.
    pub from: NodeId,
    /// The decoded message itself.
    pub message: Message<C>,
}
