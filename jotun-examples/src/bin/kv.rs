//! A three-line-protocol replicated key-value server built on jotun.
//!
//! ## Usage
//!
//! ```bash
//! # Terminal 1
//! cargo run --bin kv -- \
//!   --node-id 1 \
//!   --peer-addr 127.0.0.1:7001 \
//!   --peer 2=127.0.0.1:7002 \
//!   --peer 3=127.0.0.1:7003 \
//!   --client-addr 127.0.0.1:8001 \
//!   --data-dir /tmp/jotun-kv-1
//!
//! # Terminal 2
//! cargo run --bin kv -- \
//!   --node-id 2 \
//!   --peer-addr 127.0.0.1:7002 \
//!   --peer 1=127.0.0.1:7001 \
//!   --peer 3=127.0.0.1:7003 \
//!   --client-addr 127.0.0.1:8002 \
//!   --data-dir /tmp/jotun-kv-2
//!
//! # Terminal 3 — same shape
//! ```
//!
//! Then from any other terminal:
//!
//! ```bash
//! # Pick any node; it'll redirect if it isn't the leader.
//! printf 'SET foo bar\n' | nc -q1 127.0.0.1 8001
//! printf 'GET foo\n'     | nc -q1 127.0.0.1 8001
//! ```
//!
//! The wire protocol on the client port is three commands:
//!   `SET <key> <value>` — replicate and apply, responds `OK`.
//!   `GET <key>`         — leader-local read (eventually-consistent
//!                         with respect to other clients on other
//!                         nodes; see the comment on reads below),
//!                         responds `<value>` or `<nil>`.
//!   `DEL <key>`         — replicate and apply, responds the previous
//!                         value or `<nil>`.
//!
//! ## What this demonstrates
//!
//! The entire integration with `jotun` is about 100 lines: implement
//! `StateMachine` for your KV, ship it to [`jotun::Node::start`],
//! accept client connections on a separate socket, marshal
//! commands through `Node::propose`. Everything else — elections,
//! replication, persistence, transport, snapshotting — the runtime
//! handles.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use jotun::{
    Config, DecodeError, DiskStorage, Node, NodeId, ProposeError, StateMachine, TcpTransport,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// A tiny in-memory string-keyed / string-valued map. Shared via
/// `Arc<Mutex<_>>` so the client-port handler can do leader-local
/// reads directly; writes go through `Node::propose`.
#[derive(Debug, Default, Clone)]
struct KvState {
    map: BTreeMap<String, String>,
}

struct KvStateMachine {
    inner: Arc<Mutex<KvState>>,
}

impl KvStateMachine {
    fn new() -> (Self, Arc<Mutex<KvState>>) {
        let inner = Arc::new(Mutex::new(KvState::default()));
        (
            Self {
                inner: Arc::clone(&inner),
            },
            inner,
        )
    }
}

#[derive(Debug, Clone)]
enum KvCmd {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone)]
enum KvResponse {
    Ok,
    Prev(Option<String>),
}

impl StateMachine for KvStateMachine {
    type Command = KvCmd;
    type Response = KvResponse;

    fn encode_command(c: &KvCmd) -> Vec<u8> {
        match c {
            KvCmd::Set { key, value } => format!("S\n{key}\n{value}").into_bytes(),
            KvCmd::Delete { key } => format!("D\n{key}").into_bytes(),
        }
    }

    fn decode_command(bytes: &[u8]) -> Result<KvCmd, DecodeError> {
        let s = std::str::from_utf8(bytes).map_err(|e| DecodeError::new(e.to_string()))?;
        let mut parts = s.splitn(3, '\n');
        let tag = parts.next().ok_or_else(|| DecodeError::new("empty"))?;
        match tag {
            "S" => {
                let key = parts
                    .next()
                    .ok_or_else(|| DecodeError::new("missing key"))?;
                let value = parts
                    .next()
                    .ok_or_else(|| DecodeError::new("missing value"))?;
                Ok(KvCmd::Set {
                    key: key.into(),
                    value: value.into(),
                })
            }
            "D" => {
                let key = parts
                    .next()
                    .ok_or_else(|| DecodeError::new("missing key"))?;
                Ok(KvCmd::Delete { key: key.into() })
            }
            other => Err(DecodeError::new(format!("unknown tag: {other}"))),
        }
    }

    fn apply(&mut self, cmd: KvCmd) -> KvResponse {
        let mut state = self.inner.lock().unwrap();
        match cmd {
            KvCmd::Set { key, value } => {
                state.map.insert(key, value);
                KvResponse::Ok
            }
            KvCmd::Delete { key } => KvResponse::Prev(state.map.remove(&key)),
        }
    }
}

// ---------------------------------------------------------------------------
// CLI arg parsing — no clap, just a hand-rolled walk over args.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct CliConfig {
    node_id: u64,
    peer_addr: SocketAddr,
    peers: BTreeMap<u64, SocketAddr>,
    client_addr: SocketAddr,
    data_dir: PathBuf,
}

fn parse_args() -> Result<CliConfig, String> {
    let mut args = std::env::args().skip(1).peekable();
    let mut node_id: Option<u64> = None;
    let mut peer_addr: Option<SocketAddr> = None;
    let mut peers: BTreeMap<u64, SocketAddr> = BTreeMap::new();
    let mut client_addr: Option<SocketAddr> = None;
    let mut data_dir: Option<PathBuf> = None;

    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("flag {flag} needs a value"))?;
        match flag.as_str() {
            "--node-id" => node_id = Some(value.parse().map_err(|e| format!("--node-id: {e}"))?),
            "--peer-addr" => {
                peer_addr = Some(value.parse().map_err(|e| format!("--peer-addr: {e}"))?);
            }
            "--peer" => {
                let (id_str, addr_str) = value
                    .split_once('=')
                    .ok_or_else(|| format!("--peer needs ID=ADDR, got {value}"))?;
                let id: u64 = id_str.parse().map_err(|e| format!("--peer id: {e}"))?;
                let addr: SocketAddr = addr_str.parse().map_err(|e| format!("--peer addr: {e}"))?;
                peers.insert(id, addr);
            }
            "--client-addr" => {
                client_addr = Some(value.parse().map_err(|e| format!("--client-addr: {e}"))?);
            }
            "--data-dir" => data_dir = Some(PathBuf::from(value)),
            other => return Err(format!("unknown flag: {other}")),
        }
    }

    Ok(CliConfig {
        node_id: node_id.ok_or("--node-id is required")?,
        peer_addr: peer_addr.ok_or("--peer-addr is required")?,
        peers,
        client_addr: client_addr.ok_or("--client-addr is required")?,
        data_dir: data_dir.ok_or("--data-dir is required")?,
    })
}

// ---------------------------------------------------------------------------
// Client-port handler: one task per inbound connection.
// ---------------------------------------------------------------------------

async fn serve_client(
    mut stream: TcpStream,
    node: Node<KvStateMachine>,
    state: Arc<Mutex<KvState>>,
) {
    let (r, mut w) = stream.split();
    let mut reader = BufReader::new(r);
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let line = line.trim();
        let response = match parse_request(line) {
            Ok(Request::Set { key, value }) => {
                match node
                    .propose(KvCmd::Set {
                        key: key.clone(),
                        value,
                    })
                    .await
                {
                    Ok(_) => "OK".to_string(),
                    Err(ProposeError::NotLeader { leader_hint }) => {
                        format!("REDIRECT {leader_hint}")
                    }
                    Err(e) => format!("ERROR {e}"),
                }
            }
            Ok(Request::Get { key }) => {
                // Leader-local read. Not linearizable with respect to
                // concurrent proposes against other nodes, but fine
                // for the demo. See README on linearizable reads.
                let v = state.lock().unwrap().map.get(&key).cloned();
                v.unwrap_or_else(|| "<nil>".to_string())
            }
            Ok(Request::Delete { key }) => match node.propose(KvCmd::Delete { key }).await {
                Ok(KvResponse::Prev(Some(v))) => v,
                Ok(KvResponse::Prev(None)) => "<nil>".to_string(),
                Ok(KvResponse::Ok) => "OK".to_string(),
                Err(ProposeError::NotLeader { leader_hint }) => {
                    format!("REDIRECT {leader_hint}")
                }
                Err(e) => format!("ERROR {e}"),
            },
            Err(e) => format!("PARSE_ERROR {e}"),
        };
        if w.write_all(response.as_bytes()).await.is_err() {
            return;
        }
        if w.write_all(b"\n").await.is_err() {
            return;
        }
    }
}

enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

fn parse_request(line: &str) -> Result<Request, String> {
    let mut parts = line.splitn(3, ' ');
    let cmd = parts.next().ok_or("empty")?;
    match cmd {
        "SET" => {
            let key = parts.next().ok_or("SET needs a key")?;
            let value = parts.next().ok_or("SET needs a value")?;
            Ok(Request::Set {
                key: key.into(),
                value: value.into(),
            })
        }
        "GET" => {
            let key = parts.next().ok_or("GET needs a key")?;
            Ok(Request::Get { key: key.into() })
        }
        "DEL" => {
            let key = parts.next().ok_or("DEL needs a key")?;
            Ok(Request::Delete { key: key.into() })
        }
        other => Err(format!("unknown command: {other}")),
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            eprintln!("see the top of src/bin/kv.rs for usage");
            std::process::exit(2);
        }
    };

    let node_id = NodeId::new(cli.node_id).expect("--node-id must be non-zero");
    let peer_id_set: Vec<NodeId> = cli.peers.keys().filter_map(|&id| NodeId::new(id)).collect();
    let peer_addrs: BTreeMap<NodeId, SocketAddr> = cli
        .peers
        .iter()
        .filter_map(|(&id, &a)| NodeId::new(id).map(|n| (n, a)))
        .collect();

    info!(%node_id, %cli.peer_addr, %cli.client_addr, data_dir = %cli.data_dir.display(), "booting");

    let (sm, state) = KvStateMachine::new();
    let storage = DiskStorage::open(&cli.data_dir)
        .await
        .expect("open storage");
    let transport: TcpTransport<Vec<u8>> = TcpTransport::start(node_id, cli.peer_addr, peer_addrs)
        .await
        .expect("start transport");

    let config = Config::new(node_id, peer_id_set);
    let node = Node::start(config, sm, storage, transport)
        .await
        .expect("start node");

    // Client listener.
    let listener = TcpListener::bind(cli.client_addr)
        .await
        .expect("bind client port");
    info!(%cli.client_addr, "accepting client connections");

    let node_for_clients = node.clone();
    let state_for_clients = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let n = node_for_clients.clone();
                    let s = Arc::clone(&state_for_clients);
                    tokio::spawn(serve_client(stream, n, s));
                }
                Err(e) => {
                    warn!(error = %e, "client accept failed");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    });

    // Wait for Ctrl+C.
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown requested");
    let _ = node.shutdown().await;
}
