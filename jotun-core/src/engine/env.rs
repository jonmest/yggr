//! Injected side-effects for the engine.
//!
//! anything the engine needs that can't be computed deterministically from
//! its own state goes through [`Env`]. Production passes a real
//! implementation (typically RNG-backed); tests pass a controlled one.

/// Side-effect interface for the engine.
///
/// `Send + Debug` bounds: the engine derives `Debug` and may be moved across
/// thread boundaries in a production service, so implementations must too.
pub trait Env: Send + std::fmt::Debug {
    /// Number of ticks until the next election fires. Called on every
    /// timer reset. §5.2 recommends a random value from a fixed interval
    /// (e.g., 10–20 ticks) to stagger elections and avoid split votes.
    fn next_election_timeout(&mut self) -> u64;
}

/// An [`Env`] that always returns the same election timeout. Useful for
/// deterministic tests and single-node production configurations where no
/// election can meaningfully race.
#[derive(Debug, Clone, Copy)]
pub struct StaticEnv(pub u64);

impl Env for StaticEnv {
    fn next_election_timeout(&mut self) -> u64 {
        self.0
    }
}
