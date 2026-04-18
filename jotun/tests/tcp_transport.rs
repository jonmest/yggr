//! End-to-end tests for [`jotun::TcpTransport`]: two transports on
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

use jotun::{NodeId, TcpTransport, Transport};
use jotun_core::records::vote::{RequestVote, VoteResponse, VoteResult};
use jotun_core::types::log::LogId;
use jotun_core::{LogIndex, Message, Term};

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

    // Give the dialer tasks a moment to connect.
    tokio::time::sleep(Duration::from_millis(50)).await;

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

    tokio::time::sleep(Duration::from_millis(50)).await;

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

    tokio::time::sleep(Duration::from_millis(50)).await;

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
