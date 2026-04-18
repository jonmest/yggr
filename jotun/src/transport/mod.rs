//! Trait for the network side of a [`crate::Node`].
//!
//! The engine emits `Action::Send { to, message }`; this trait
//! delivers the bytes. Inbound messages flow back through the
//! [`Transport::recv`] stream. Implementations own connection
//! lifecycle, framing, retries, and authentication.
//!
//! The default impl is [`tcp::TcpTransport`]: tokio TCP +
//! length-prefixed prost frames. Users who want gRPC (`tonic`),
//! QUIC (`quinn`), or in-memory testing plug in their own
//! [`Transport`].

pub mod tcp;

pub use tcp::{TcpTransport, TcpTransportError};

use std::future::Future;

use jotun_core::{Incoming, Message, NodeId};

/// A network endpoint capable of sending messages to peers and
/// receiving messages addressed to us.
///
/// `C` is the application command type — the same `C` the engine is
/// generic over, threaded through the wire format.
pub trait Transport<C>: Send + 'static
where
    C: Send + 'static,
{
    /// One-shot error type. The runtime logs transport errors and
    /// continues — a single send failure is not fatal because the
    /// engine retries on its own cadence (heartbeat, conflict reply).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Send `message` to peer `to`. Returns once the message has been
    /// handed off to the underlying transport (NOT once the peer has
    /// acknowledged) — the engine doesn't need delivery guarantees,
    /// just best-effort transmission.
    fn send(
        &self,
        to: NodeId,
        message: Message<C>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Receive the next inbound message. The runtime polls this in a
    /// loop; returning `None` shuts the receive task down. The
    /// transport is responsible for tagging messages with the
    /// authenticated sender — `Incoming::from` is the source of
    /// truth the engine relies on.
    fn recv(&mut self) -> impl Future<Output = Option<Incoming<C>>> + Send;

    /// Best-effort graceful shutdown hook. The runtime calls this
    /// during [`crate::Node::shutdown`] before the driver exits so
    /// transport implementations can stop and join any background
    /// tasks they own. Default: nothing to do.
    fn shutdown(&mut self) -> impl Future<Output = ()> + Send {
        async {}
    }
}
