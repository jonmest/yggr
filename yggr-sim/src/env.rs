//! Sim-supplied [`yggr_core::Env`] that sources nondeterminism from
//! the cluster RNG.
//!
//! Each node gets its own [`SimEnv`] handle wired to the same shared
//! [`rand::rngs::StdRng`]. Because every `next_election_timeout` call
//! goes through the same RNG in the same order the scheduler observes,
//! replaying with the same seed yields identical timer draws.
//!
//! `Env: Send` so the RNG lives in an `Arc<Mutex<...>>` rather than
//! an `Rc<RefCell<...>>`. The simulation is single-threaded, so lock
//! contention is never real — the wrapper exists solely to satisfy
//! the core trait bound.

use std::sync::{Arc, Mutex};

use rand::RngExt;
use rand::rngs::StdRng;

/// Shared RNG handle. Single source of nondeterminism for the whole
/// cluster — engines, scheduler, network jitter all draw from this.
pub(crate) type SharedRng = Arc<Mutex<StdRng>>;

/// Inclusive election-timeout range. §5.2 recommends ~10-20 ticks; we
/// pick a wider default to give the scheduler room to interleave ticks
/// without immediately triggering elections.
pub(crate) const ELECTION_TIMEOUT_MIN: u64 = 10;
pub(crate) const ELECTION_TIMEOUT_MAX: u64 = 20;

/// A [`yggr_core::Env`] that draws election timeouts from the cluster's
/// shared RNG. All randomness in the simulation — every timer, every
/// scheduler choice, every network jitter call — pulls from this same
/// stream, so a `(seed, schedule-length)` pair is enough to reproduce
/// any run.
#[derive(Debug)]
pub(crate) struct SimEnv {
    rng: SharedRng,
}

impl SimEnv {
    pub(crate) fn new(rng: SharedRng) -> Self {
        Self { rng }
    }
}

impl yggr_core::Env for SimEnv {
    fn next_election_timeout(&mut self) -> u64 {
        let mut rng = self
            .rng
            .lock()
            .expect("sim RNG mutex poisoned — a previous step panicked");
        rng.random_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX)
    }
}
