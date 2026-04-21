//! Jotun Sim — a deterministic simulation harness for [`yggr_core`].
//!
//! Drives `N` [`yggr_core::Engine`]s through arbitrary schedules —
//! partitions, reordering, drops, crashes, clock skew — and asserts
//! Raft safety properties on every step. All nondeterminism flows from
//! a single seeded RNG, so a failing run can be replayed byte-for-byte
//! by re-running with the same seed.
//!
//! # Public surface
//!
//! Intentionally tiny. The crate is a testing tool; internals stay hidden.
//!
//!  - [`Cluster::new`]: build an N-node cluster keyed off a seed.
//!  - [`Cluster::step`]: advance one scheduler choice.
//!  - [`Cluster::run_until`]: drive the cluster until a predicate holds.
//!  - [`SafetyViolation`]: the error type panics carry on invariant failure.
//!
//! Everything else — `Network`, `NodeHarness`, the per-event alphabet
//! — is `pub(crate)` so downstream users can't depend on schedule shape.

// This crate is a testing tool: every `expect` is on an invariant the
// sim itself establishes (a known-existing key, a mutex nothing else
// can poison, a slice we just sized). Blanket-allow here rather than
// scattering per-site allows.
#![allow(
    clippy::cast_possible_truncation,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    // Documented elsewhere why these specific patterns are fine here:
    clippy::doc_markdown,
    clippy::format_push_string,
    clippy::manual_non_exhaustive,
    clippy::map_unwrap_or,
    clippy::missing_fields_in_debug,
    clippy::needless_pass_by_value,
    clippy::struct_excessive_bools,
    clippy::too_many_lines,
    clippy::unused_self,
)]

mod cluster;
mod env;
mod harness;
mod invariants;
mod network;
mod schedule;

#[cfg(test)]
mod tests;

pub use cluster::Cluster;
pub use invariants::SafetyViolation;
