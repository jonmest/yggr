//! The trait users implement to plug their application logic into a
//! [`crate::Node`].
//!
//! Three operations:
//!  - [`StateMachine::apply`] — run a committed command against state,
//!    return the response. Synchronous, infallible, deterministic.
//!  - [`StateMachine::snapshot`] — produce a snapshot of state. Default
//!    is "no snapshot" (returns empty bytes). The runtime calls this
//!    automatically when the engine emits a snapshot hint, unless the
//!    user disables that path via
//!    [`crate::Config::snapshot_hint_threshold_entries`].
//!  - [`StateMachine::restore`] — rebuild state from snapshot bytes.
//!    Default panics; only override if you also override `snapshot`.
//!
//! ## Why apply is synchronous and infallible
//!
//! Replication makes apply effects observable on every node. To stay
//! deterministic across replicas the apply must:
//!  - return the same value for the same command on every node (no
//!    clocks, randomness, or network calls — those go through Command),
//!  - never panic on data the cluster has accepted (a panic on one
//!    replica is a non-deterministic state divergence on the others).
//!
//! If your apply needs async work (e.g., write-through to a downstream
//! store), do it after the apply returns. The runtime has already
//! committed; durability is the engine's problem, not yours.

/// The application logic the runtime feeds committed entries to.
///
/// `Command` is what callers submit via [`crate::Node::propose`]. The
/// engine treats it as opaque bytes; you decide how to serialise it
/// via [`Self::encode_command`] and [`Self::decode_command`]. Pick
/// whatever you like — serde + bincode, prost, hand-rolled bytes,
/// anything stable. The runtime never inspects the contents.
///
/// `Response` is what the propose call returns once the entry commits
/// and applies on the local node.
pub trait StateMachine: Send + 'static {
    /// The command type clients submit.
    type Command: Send + 'static;

    /// What [`crate::Node::propose`] returns once the command applies
    /// locally.
    type Response: Send + 'static;

    /// Serialise a command for the wire / disk. Same input MUST
    /// produce the same bytes on every call (across machines, across
    /// restarts) — otherwise log entries diverge between nodes.
    fn encode_command(command: &Self::Command) -> Vec<u8>;

    /// Inverse of [`Self::encode_command`]. Returning `Err` means
    /// the bytes are malformed; the runtime treats this as a fatal
    /// data-corruption signal and shuts the node down — committed
    /// log entries are not supposed to fail to decode.
    fn decode_command(bytes: &[u8]) -> Result<Self::Command, DecodeError>;

    /// Run a committed command against state. Same `command` on every
    /// node MUST produce the same `Response` and the same state
    /// mutation; otherwise the cluster diverges.
    fn apply(&mut self, command: Self::Command) -> Self::Response;

    /// Serialize the entire state into bytes. Default: empty bytes,
    /// signalling "no snapshot". Override this if you want the runtime
    /// to auto-compact on snapshot hints, or if your host integration
    /// plans to cut snapshots explicitly.
    ///
    /// Return the raw snapshot bytes. If you want compression, compress
    /// here and decompress inside [`Self::restore`] — jotun does not
    /// compress for you and does not assume any particular format. zstd
    /// is a common choice for Raft snapshots; lz4 when CPU is precious.
    fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Rebuild state from a previously-emitted snapshot. Default panics
    /// — only override if you also override [`Self::snapshot`].
    /// Called when the runtime recovers from disk after a crash, or
    /// when an incoming leader `InstallSnapshot` arrives at this node.
    ///
    /// The bytes are whatever [`Self::snapshot`] produced. If you
    /// compressed there, decompress here — the runtime hands the bytes
    /// through untouched.
    fn restore(&mut self, _bytes: Vec<u8>) {
        panic!(
            "StateMachine::restore not implemented; override it if your \
             snapshot() returns non-empty bytes"
        );
    }
}

/// Returned from [`StateMachine::decode_command`] when the bytes off
/// the wire / disk can't be parsed back into a `Command`. The runtime
/// treats this as a corrupt-storage signal and shuts down rather than
/// silently dropping commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    pub reason: String,
}

impl DecodeError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command decode error: {}", self.reason)
    }
}

impl std::error::Error for DecodeError {}
