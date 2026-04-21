#![allow(
    clippy::indexing_slicing, // sliced fixed-size byte buffers, bounds proven by length checks
    clippy::expect_used,      // ditto
)]
//! Default [`crate::Transport`] implementation: tokio TCP +
//! length-prefixed prost frames.
//!
//! Each peer gets a long-lived outgoing connection; inbound
//! connections feed a single mpsc the runtime polls via
//! [`TcpTransport::recv`]. Reconnection is automatic with linear
//! backoff. Send is best-effort — a failed write drops the
//! connection so the next call reopens it.
//!
//! The wire format per frame is `[u8; 4 length_be][prost-encoded
//! Message]`. The length field is the body length only and uses
//! big-endian for portability with anything else that might speak
//! to a yggr cluster (a curl-replay debugger, say).
//!
//! Authentication is the sender id, included in every frame as a
//! prefix `[u8; 8 sender_id_be]` before the length prefix. Real
//! deployments would layer TLS + a peer-cert check on top; this
//! impl trusts whatever the wire claims, which is fine for
//! same-VPC deployments.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use yggr_core::transport::protobuf as proto;
use yggr_core::{Incoming, Message, NodeId};

use crate::transport::Transport;

/// Per-peer outgoing connection task. Reopens on failure.
struct PeerLink {
    /// Address to dial.
    addr: SocketAddr,
    /// Live tx side: hands frames off to the per-peer writer task.
    tx: mpsc::Sender<Vec<u8>>,
}

/// Tokio-TCP-backed transport. Construct with [`Self::start`], which
/// binds the listener and spawns one task per peer.
#[allow(clippy::module_name_repetitions)]
pub struct TcpTransport<C> {
    /// Our own id, included in the prefix of every outgoing frame.
    me: NodeId,
    /// Per-peer dial state. Mutex because [`Transport::send`] takes
    /// `&self` to keep the trait shape compact, but we need to mutate
    /// the map on lazy-reconnect.
    peers: Arc<AsyncMutex<BTreeMap<NodeId, PeerLink>>>,
    /// Receiving half of the inbound channel.
    inbound: mpsc::Receiver<Incoming<C>>,
    /// Held so the listener task lives as long as we do.
    listener: Option<JoinHandle<()>>,
    /// Outbound per-peer writer tasks.
    writers: Vec<JoinHandle<()>>,
    /// Inbound per-connection reader tasks, tracked so drop can abort
    /// them promptly during shutdown.
    readers: Arc<StdMutex<Vec<JoinHandle<()>>>>,
}

impl<C> std::fmt::Debug for TcpTransport<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpTransport")
            .field("me", &self.me)
            .finish_non_exhaustive()
    }
}

impl<C> TcpTransport<C>
where
    C: Send + Clone + Into<Vec<u8>> + From<Vec<u8>> + 'static,
{
    /// Bind a TCP listener on `listen_addr`, start a task per peer
    /// dialing the supplied address, and return the running transport.
    /// `peers` maps every other cluster node to its `SocketAddr`; the
    /// `me` entry, if present, is silently ignored.
    pub async fn start(
        me: NodeId,
        listen_addr: SocketAddr,
        peers: BTreeMap<NodeId, SocketAddr>,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(listen_addr).await?;
        let (inbound_tx, inbound_rx) = mpsc::channel(1024);
        let readers = Arc::new(StdMutex::new(Vec::new()));

        let listener_task =
            tokio::spawn(accept_loop::<C>(listener, inbound_tx, Arc::clone(&readers)));

        let mut peer_map: BTreeMap<NodeId, PeerLink> = BTreeMap::new();
        let mut writers = Vec::new();
        for (id, addr) in peers {
            if id == me {
                continue;
            }
            let (link, writer) = spawn_peer_link::<C>(me, id, addr);
            peer_map.insert(id, link);
            writers.push(writer);
        }

        Ok(Self {
            me,
            peers: Arc::new(AsyncMutex::new(peer_map)),
            inbound: inbound_rx,
            listener: Some(listener_task),
            writers,
            readers,
        })
    }

    async fn shutdown_owned_tasks(&mut self) {
        if let Some(listener) = self.listener.take() {
            abort_and_join(listener).await;
        }

        for writer in std::mem::take(&mut self.writers) {
            abort_and_join(writer).await;
        }

        let readers: Vec<JoinHandle<()>> = {
            let mut readers = self
                .readers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            readers.drain(..).collect()
        };
        for reader in readers {
            abort_and_join(reader).await;
        }
    }
}

async fn abort_and_join(handle: JoinHandle<()>) {
    handle.abort();
    match handle.await {
        Ok(()) => {}
        Err(e) if e.is_cancelled() => {}
        Err(e) => {
            debug!(target = "yggr::transport", error = %e, "background task exited with error during shutdown");
        }
    }
}

/// Dedicated writer task per peer: reads frames from a channel,
/// dials/redials TCP as needed, writes them out.
fn spawn_peer_link<C>(me: NodeId, peer: NodeId, addr: SocketAddr) -> (PeerLink, JoinHandle<()>)
where
    C: Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
    let writer = tokio::spawn(async move {
        let mut conn: Option<TcpStream> = None;
        let mut backoff_ms: u64 = 50;
        let _ = me;
        let _ = peer;
        let _: std::marker::PhantomData<C> = std::marker::PhantomData;
        while let Some(frame) = rx.recv().await {
            loop {
                if conn.is_none() {
                    match TcpStream::connect(addr).await {
                        Ok(s) => {
                            conn = Some(s);
                            backoff_ms = 50;
                        }
                        Err(e) => {
                            warn!(target = "yggr::transport", peer = %peer, %addr, error = %e, "connect failed");
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(2000);
                            continue;
                        }
                    }
                }
                let stream = conn.as_mut().expect("just opened");
                match stream.write_all(&frame).await {
                    Ok(()) => {
                        // Best-effort flush. Error here drops the conn.
                        if let Err(e) = stream.flush().await {
                            debug!(target = "yggr::transport", peer = %peer, error = %e, "flush failed; reopening");
                            conn = None;
                            continue;
                        }
                        break;
                    }
                    Err(e) => {
                        debug!(target = "yggr::transport", peer = %peer, error = %e, "write failed; reopening");
                        conn = None;
                        // Loop re-dials and retries this same frame.
                    }
                }
            }
        }
    });
    (PeerLink { addr, tx }, writer)
}

/// Listener task: accept inbound connections, spawn a reader for each.
async fn accept_loop<C>(
    listener: TcpListener,
    inbound: mpsc::Sender<Incoming<C>>,
    readers: Arc<StdMutex<Vec<JoinHandle<()>>>>,
) where
    C: Send + From<Vec<u8>> + 'static,
{
    loop {
        match listener.accept().await {
            Ok((stream, _peer_addr)) => {
                let inbound = inbound.clone();
                let reader = tokio::spawn(read_frames::<C>(stream, inbound));
                readers
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(reader);
            }
            Err(e) => {
                warn!(target = "yggr::transport", error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Per-connection reader: pulls length-prefixed frames off the wire,
/// decodes them, pushes `Incoming` onto the inbound channel.
async fn read_frames<C>(mut stream: TcpStream, inbound: mpsc::Sender<Incoming<C>>)
where
    C: Send + From<Vec<u8>> + 'static,
{
    loop {
        // Frame layout: [u64 BE sender_id][u32 BE body_len][body...]
        let mut prefix = [0u8; 12];
        if let Err(e) = stream.read_exact(&mut prefix).await {
            if e.kind() != io::ErrorKind::UnexpectedEof {
                debug!(target = "yggr::transport", error = %e, "read prefix failed");
            }
            return;
        }
        #[allow(clippy::indexing_slicing)] // 12-byte fixed buffer
        let sender_bytes: [u8; 8] = prefix[0..8].try_into().expect("8 bytes");
        #[allow(clippy::indexing_slicing)]
        let len_bytes: [u8; 4] = prefix[8..12].try_into().expect("4 bytes");
        let sender_raw = u64::from_be_bytes(sender_bytes);
        let len = u32::from_be_bytes(len_bytes) as usize;
        let Some(sender) = NodeId::new(sender_raw) else {
            warn!(
                target = "yggr::transport",
                "frame sender id is zero; dropping connection"
            );
            return;
        };
        // Frame size cap so a malicious or buggy peer can't OOM us.
        if len > 64 * 1024 * 1024 {
            warn!(
                target = "yggr::transport",
                len, "frame body too large; dropping connection"
            );
            return;
        }
        let mut body = vec![0u8; len];
        if let Err(e) = stream.read_exact(&mut body).await {
            debug!(target = "yggr::transport", error = %e, "read body failed");
            return;
        }
        let proto_msg = match proto::Message::decode(body.as_slice()) {
            Ok(m) => m,
            Err(e) => {
                warn!(target = "yggr::transport", error = %e, "frame decode failed; dropping connection");
                return;
            }
        };
        let message: Message<C> = match proto_msg.try_into() {
            Ok(m) => m,
            Err(e) => {
                warn!(target = "yggr::transport", error = %e, "frame validation failed; dropping connection");
                return;
            }
        };
        if inbound
            .send(Incoming {
                from: sender,
                message,
            })
            .await
            .is_err()
        {
            // Receiver dropped — runtime is shutting down.
            return;
        }
    }
}

impl<C> Transport<C> for TcpTransport<C>
where
    C: Send + Clone + Into<Vec<u8>> + From<Vec<u8>> + 'static,
{
    type Error = TcpTransportError;

    async fn send(&self, to: NodeId, message: Message<C>) -> Result<(), Self::Error> {
        let proto_msg: proto::Message = message.into();
        let body = proto_msg.encode_to_vec();
        let len = u32::try_from(body.len()).map_err(|_| TcpTransportError::FrameTooLarge)?;
        let mut frame = Vec::with_capacity(12 + body.len());
        frame.extend_from_slice(&self.me.get().to_be_bytes());
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&body);

        let peers = self.peers.lock().await;
        let Some(link) = peers.get(&to) else {
            return Err(TcpTransportError::UnknownPeer(to));
        };
        link.tx
            .send(frame)
            .await
            .map_err(|_| TcpTransportError::PeerWriterDead(to))?;
        Ok(())
    }

    async fn recv(&mut self) -> Option<Incoming<C>> {
        self.inbound.recv().await
    }

    async fn shutdown(&mut self) {
        self.shutdown_owned_tasks().await;
    }
}

/// Errors returned by [`TcpTransport`].
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum TcpTransportError {
    /// Send target isn't in the peer map. Likely a bug in the runtime
    /// (or a config-change race during membership churn).
    UnknownPeer(NodeId),
    /// The per-peer writer task died, usually because the runtime is
    /// shutting down.
    PeerWriterDead(NodeId),
    /// Outgoing frame body exceeds 4 GiB. Should never happen in
    /// practice; bigger snapshots need chunking (post-0.1).
    FrameTooLarge,
}

impl std::fmt::Display for TcpTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownPeer(id) => write!(f, "unknown peer {id}"),
            Self::PeerWriterDead(id) => write!(f, "peer writer task dead for {id}"),
            Self::FrameTooLarge => write!(f, "outgoing frame body exceeds 4 GiB"),
        }
    }
}

impl std::error::Error for TcpTransportError {}

impl<C> Drop for TcpTransport<C> {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
        for writer in &self.writers {
            writer.abort();
        }
        let mut readers = self
            .readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for reader in readers.drain(..) {
            reader.abort();
        }
    }
}

// Make PeerLink fields used so dead_code doesn't fire.
#[allow(dead_code)]
impl PeerLink {
    fn addr(&self) -> SocketAddr {
        self.addr
    }
}
