//! all tracing in the engine goes through this module so that field names
//! and targets stay consistent. Downstream consumers can filter by the
//! `yggr::engine` target and rely on stable field keys.

use crate::types::{node::NodeId, term::Term};

/// tracing target for all engine events. use this with
/// `target: telemetry::TARGET` so `RUST_LOG=yggr::engine=debug` works.
pub(crate) const TARGET: &str = "yggr::engine";

/// stable field keys. used with `Span::current().record(...)` where
/// the macro syntax doesn't accept constants.
pub(crate) mod fields {
    pub(crate) const DECISION: &str = "decision";
}

pub(crate) fn term_advanced(node_id: NodeId, from: Term, to: Term) {
    tracing::info!(
        target: TARGET,
        node_id = %node_id,
        from_term = %from,
        to_term = %to,
        "term advanced",
    );
}

pub(crate) fn became_follower(node_id: NodeId, term: Term) {
    tracing::info!(
        target: TARGET,
        node_id = %node_id,
        to_term = %term,
        role = "follower",
        "role changed",
    );
}

pub(crate) fn became_pre_candidate(node_id: NodeId, proposed_term: Term) {
    tracing::info!(
        target: TARGET,
        node_id = %node_id,
        proposed_term = %proposed_term,
        role = "pre-candidate",
        "role changed",
    );
}

pub(crate) fn became_candidate(node_id: NodeId, term: Term) {
    tracing::info!(
        target: TARGET,
        node_id = %node_id,
        to_term = %term,
        role = "candidate",
        "role changed",
    );
}

pub(crate) fn became_leader(node_id: NodeId, term: Term) {
    tracing::info!(
        target: TARGET,
        node_id = %node_id,
        to_term = %term,
        role = "leader",
        "role changed",
    );
}
