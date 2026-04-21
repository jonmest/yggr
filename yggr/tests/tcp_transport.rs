//! End-to-end tests for [`yggr::TcpTransport`]: two transports on
//! loopback, send messages back and forth, prove the wire format
//! round-trips and the per-peer reconnect logic survives a peer
//! restart.

#![allow(
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::missing_const_for_fn,
    clippy::unwrap_used,
    clippy::expect_used
)]

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use yggr::{NodeId, TcpTransport, Transport};
use yggr_core::records::vote::{RequestVote, VoteResponse, VoteResult};
use yggr_core::types::log::LogId;
use yggr_core::{LogIndex, Message, Term};

fn nid(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}

/// Pick a free loopback port by binding briefly.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

async fn restart_transport_on_same_port(
    me: NodeId,
    addr: SocketAddr,
    peers: BTreeMap<NodeId, SocketAddr>,
) -> TcpTransport<Vec<u8>> {
    let start = tokio::time::Instant::now();
    loop {
        match TcpTransport::start(me, addr, peers.clone()).await {
            Ok(transport) => return transport,
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {
                assert!(
                    start.elapsed() < Duration::from_secs(2),
                    "port {addr} stayed busy after restart: {err}"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(err) => panic!("failed to restart transport on {addr}: {err}"),
        }
    }
}

#[tokio::test]
async fn vote_request_round_trips_over_tcp() {
    let port_a = free_port();
    let port_b = free_port();
    let addr_a: SocketAddr = (Ipv4Addr::LOCALHOST, port_a).into();
    let addr_b: SocketAddr = (Ipv4Addr::LOCALHOST, port_b).into();

    let mut peers_a = BTreeMap::new();
    peers_a.insert(nid(2), addr_b);
    let mut peers_b = BTreeMap::new();
    peers_b.insert(nid(1), addr_a);

    let a: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr_a, peers_a).await.unwrap();
    let mut b: TcpTransport<Vec<u8>> = TcpTransport::start(nid(2), addr_b, peers_b).await.unwrap();

    let req = RequestVote {
        term: Term::new(7),
        candidate_id: nid(1),
        last_log_id: Some(LogId::new(LogIndex::new(3), Term::new(7))),
    };
    a.send(nid(2), Message::VoteRequest(req)).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(1), b.recv())
        .await
        .expect("recv timed out")
        .expect("transport closed");

    assert_eq!(received.from, nid(1));
    match received.message {
        Message::VoteRequest(r) => {
            assert_eq!(r.term, Term::new(7));
            assert_eq!(r.candidate_id, nid(1));
            assert_eq!(r.last_log_id.unwrap().index, LogIndex::new(3));
        }
        other => panic!("expected VoteRequest, got {other:?}"),
    }
}

#[tokio::test]
async fn vote_response_round_trips_over_tcp() {
    let port_a = free_port();
    let port_b = free_port();
    let addr_a: SocketAddr = (Ipv4Addr::LOCALHOST, port_a).into();
    let addr_b: SocketAddr = (Ipv4Addr::LOCALHOST, port_b).into();

    let mut peers_a = BTreeMap::new();
    peers_a.insert(nid(2), addr_b);
    let mut peers_b = BTreeMap::new();
    peers_b.insert(nid(1), addr_a);

    let mut a: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr_a, peers_a).await.unwrap();
    let b: TcpTransport<Vec<u8>> = TcpTransport::start(nid(2), addr_b, peers_b).await.unwrap();

    let resp = VoteResponse {
        term: Term::new(7),
        result: VoteResult::Granted,
    };
    b.send(nid(1), Message::VoteResponse(resp)).await.unwrap();

    let received = tokio::time::timeout(Duration::from_secs(1), a.recv())
        .await
        .expect("recv timed out")
        .expect("transport closed");

    assert_eq!(received.from, nid(2));
    match received.message {
        Message::VoteResponse(r) => {
            assert_eq!(r.term, Term::new(7));
            assert_eq!(r.result, VoteResult::Granted);
        }
        other => panic!("expected VoteResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn many_messages_arrive_in_order() {
    let port_a = free_port();
    let port_b = free_port();
    let addr_a: SocketAddr = (Ipv4Addr::LOCALHOST, port_a).into();
    let addr_b: SocketAddr = (Ipv4Addr::LOCALHOST, port_b).into();

    let mut peers_a = BTreeMap::new();
    peers_a.insert(nid(2), addr_b);
    let mut peers_b = BTreeMap::new();
    peers_b.insert(nid(1), addr_a);

    let a: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr_a, peers_a).await.unwrap();
    let mut b: TcpTransport<Vec<u8>> = TcpTransport::start(nid(2), addr_b, peers_b).await.unwrap();

    for i in 1..=20u64 {
        let req = RequestVote {
            term: Term::new(i),
            candidate_id: nid(1),
            last_log_id: None,
        };
        a.send(nid(2), Message::VoteRequest(req)).await.unwrap();
    }
    for expected in 1..=20u64 {
        let r = tokio::time::timeout(Duration::from_secs(1), b.recv())
            .await
            .unwrap()
            .unwrap();
        match r.message {
            Message::VoteRequest(rv) => assert_eq!(rv.term, Term::new(expected)),
            other => panic!("expected VoteRequest, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn peer_restart_on_same_port_is_survived_by_reconnect() {
    // Exercises the writer task's reconnect loop: when the peer drops
    // the TCP connection, the next `write_all` fails, we set `conn =
    // None`, loop back, re-dial, and deliver the frame.
    let port_a = free_port();
    let port_b = free_port();
    let addr_a: SocketAddr = (Ipv4Addr::LOCALHOST, port_a).into();
    let addr_b: SocketAddr = (Ipv4Addr::LOCALHOST, port_b).into();

    let mut peers_a = BTreeMap::new();
    peers_a.insert(nid(2), addr_b);
    let mut peers_b = BTreeMap::new();
    peers_b.insert(nid(1), addr_a);

    let a: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr_a, peers_a).await.unwrap();

    let mut b: TcpTransport<Vec<u8>> = TcpTransport::start(nid(2), addr_b, peers_b.clone())
        .await
        .unwrap();

    // Send once to confirm the path works.
    let req = RequestVote {
        term: Term::new(1),
        candidate_id: nid(1),
        last_log_id: None,
    };
    a.send(nid(2), Message::VoteRequest(req)).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), b.recv())
        .await
        .unwrap()
        .unwrap();

    // Kill b and wait for the port to become bindable again.
    drop(b);

    // Restart b on the same port. The writer in `a` should reconnect
    // once b is back up.
    let mut b2 = restart_transport_on_same_port(nid(2), addr_b, peers_b).await;
    // Send is best-effort: a failed write drops the message. Race
    // send-loop against recv — the writer in `a` will reconnect
    // once b2's listener is up, and the next enqueued frame flushes.
    let req2 = RequestVote {
        term: Term::new(2),
        candidate_id: nid(1),
        last_log_id: None,
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let mut received = None;
    while std::time::Instant::now() < deadline {
        let _ = a.send(nid(2), Message::VoteRequest(req2)).await;
        if let Ok(msg) = tokio::time::timeout(Duration::from_millis(250), b2.recv()).await {
            received = msg;
            if received.is_some() {
                break;
            }
        }
    }
    assert!(
        received.is_some(),
        "reconnect never delivered a message within 10s",
    );
}

#[tokio::test]
async fn malformed_prefix_zero_sender_drops_connection() {
    // Open a raw TCP stream to a TcpTransport and send a frame whose
    // sender id is zero. The reader must drop the connection instead
    // of crashing; we observe the drop via the socket being closed.
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let _rx: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(&0u64.to_be_bytes()); // sender id = 0
    frame.extend_from_slice(&0u32.to_be_bytes()); // body len 0
    s.write_all(&frame).await.unwrap();
    s.flush().await.unwrap();
    // The reader task should close its side; reading returns 0.
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .expect("read timed out")
        .unwrap();
    assert_eq!(n, 0, "expected server to close after zero-sender frame");
}

#[tokio::test]
async fn malformed_prefix_oversize_drops_connection() {
    // Send a frame whose declared body length exceeds the 64 MiB cap.
    // The reader must drop the connection without allocating the body.
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let _rx: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut s = TcpStream::connect(addr).await.unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(&1u64.to_be_bytes());
    frame.extend_from_slice(&(128u32 * 1024 * 1024).to_be_bytes()); // 128 MiB, over the cap
    s.write_all(&frame).await.unwrap();
    s.flush().await.unwrap();
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .expect("read timed out")
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn malformed_body_fails_decode_and_drops_connection() {
    // Valid prefix but a body that isn't a valid protobuf Message.
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let _rx: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let mut s = TcpStream::connect(addr).await.unwrap();
    let garbage: &[u8] = b"\xff\xff\xff\xff\xff\xff\xff\xff";
    let len = garbage.len() as u32;
    let mut frame = Vec::new();
    frame.extend_from_slice(&2u64.to_be_bytes());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(garbage);
    s.write_all(&frame).await.unwrap();
    s.flush().await.unwrap();
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .expect("read timed out")
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn client_disconnect_before_full_prefix_is_clean() {
    // Exercise the UnexpectedEof branch of read_prefix (no debug log).
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let _rx: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();

    let s = TcpStream::connect(addr).await.unwrap();
    // Close immediately, no bytes sent.
    drop(s);
    // The listener should still accept new connections after handling
    // the early EOF path.
    let reconnect = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .expect("listener stopped accepting after client EOF")
        .expect("reconnect after client EOF failed");
    drop(reconnect);
}

#[tokio::test]
async fn shutdown_aborts_owned_tasks() {
    // Calling shutdown() should abort listener, writers, and readers.
    // After shutdown, recv() returns None (the inbound sender in the
    // listener task has been dropped).
    let port_a = free_port();
    let port_b = free_port();
    let addr_a: SocketAddr = (Ipv4Addr::LOCALHOST, port_a).into();
    let addr_b: SocketAddr = (Ipv4Addr::LOCALHOST, port_b).into();
    let mut peers_a = BTreeMap::new();
    peers_a.insert(nid(2), addr_b);

    let mut a: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr_a, peers_a).await.unwrap();

    // Open an inbound connection from a naked TCP client to spawn a
    // reader task we can observe being aborted.
    let _s = TcpStream::connect(addr_a).await.unwrap();

    a.shutdown().await;

    // After shutdown, recv returns None promptly.
    let result = tokio::time::timeout(Duration::from_secs(1), a.recv())
        .await
        .expect("recv didn't return after shutdown");
    assert!(result.is_none());
}

#[tokio::test]
async fn drop_aborts_tasks_without_shutdown() {
    // Construct a transport with an active inbound reader, then drop
    // it without calling shutdown. The Drop impl should abort all
    // tasks cleanly.
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let t: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, BTreeMap::new())
        .await
        .unwrap();
    let _s = TcpStream::connect(addr).await.unwrap();
    drop(t);
    // If Drop panics, this test fails. Otherwise, we're good.
}

#[tokio::test]
async fn debug_impl_prints_node_id() {
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let t: TcpTransport<Vec<u8>> = TcpTransport::start(nid(42), addr, BTreeMap::new())
        .await
        .unwrap();
    let s = format!("{t:?}");
    assert!(s.contains("TcpTransport"));
    assert!(s.contains("42"));
}

#[tokio::test]
async fn self_id_in_peer_map_is_silently_ignored() {
    // `me` in the peers arg should be dropped silently, not produce an
    // error or spawn a self-connecting writer.
    let port = free_port();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
    let mut peers = BTreeMap::new();
    peers.insert(nid(1), addr); // same as `me`
    let a: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr, peers).await.unwrap();
    // Sending to nid(1) should return UnknownPeer — the self entry was
    // filtered out of the map.
    let req = RequestVote {
        term: Term::new(1),
        candidate_id: nid(1),
        last_log_id: None,
    };
    let err = a.send(nid(1), Message::VoteRequest(req)).await.unwrap_err();
    assert!(format!("{err}").contains("unknown peer"));
}

#[tokio::test]
async fn tcp_transport_error_display_covers_all_variants() {
    use yggr::TcpTransportError;
    let e = TcpTransportError::UnknownPeer(nid(5));
    let s = format!("{e}");
    assert!(s.contains("unknown peer") && s.contains('5'), "got: {s}");
    let e = TcpTransportError::PeerWriterDead(nid(6));
    assert!(format!("{e}").contains("peer writer task dead"));
    let e = TcpTransportError::FrameTooLarge;
    assert!(format!("{e}").contains("4 GiB"));
}

#[tokio::test]
async fn send_to_unknown_peer_returns_error() {
    let port_a = free_port();
    let addr_a: SocketAddr = (Ipv4Addr::LOCALHOST, port_a).into();
    let a: TcpTransport<Vec<u8>> = TcpTransport::start(nid(1), addr_a, BTreeMap::new())
        .await
        .unwrap();
    let req = RequestVote {
        term: Term::new(1),
        candidate_id: nid(1),
        last_log_id: None,
    };
    let err = a
        .send(nid(99), Message::VoteRequest(req))
        .await
        .unwrap_err();
    assert!(format!("{err}").contains("unknown peer"));
}
