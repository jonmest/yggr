//! Internal tests for the sim itself.
//!
//! Public `Cluster` callers hit only [`Cluster::new`] / [`Cluster::step`]
//! / [`Cluster::run_until`]. Tests in this module poke at `pub(crate)`
//! internals (the scheduler policy in particular) to set up specific
//! scenarios; crate-external users can't.

mod chaos;
mod invariants;
mod membership;
mod proptests;
mod smoke;
