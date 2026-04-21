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

/// An [`Env`] that draws each election timeout uniformly from `[min, max)`
/// using a seeded xorshift64 RNG. §5.2 calls for randomized timeouts to
/// stagger elections and avoid split votes; this is the production-
/// default knob.
///
/// The RNG is seeded at construction. Different nodes in the same
/// cluster MUST seed differently (e.g. from `(current_time, node_id)`)
/// or they'll all draw identical timeouts and split votes forever.
#[derive(Debug, Clone)]
pub struct RandomizedEnv {
    state: u64,
    min: u64,
    range: u64,
}

impl RandomizedEnv {
    /// New RNG seeded with `seed`, drawing values from `[min, max)`.
    /// Panics if `min >= max`.
    #[must_use]
    pub fn new(seed: u64, min: u64, max: u64) -> Self {
        assert!(
            min < max,
            "RandomizedEnv: min must be strictly less than max"
        );
        // Avoid the all-zeros xorshift fixed point.
        let state = if seed == 0 {
            0xDEAD_BEEF_CAFE_F00D
        } else {
            seed
        };
        Self {
            state,
            min,
            range: max - min,
        }
    }
}

impl Env for RandomizedEnv {
    fn next_election_timeout(&mut self) -> u64 {
        // xorshift64.
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        self.min + (x % self.range)
    }
}

/// An [`Env`] that returns a scripted sequence of election timeouts, cycling
/// when exhausted. Test tool for verifying that the engine actually re-queries
/// the env on each timer reset.
#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct ScriptedEnv {
    values: Vec<u64>,
    idx: usize,
}

#[cfg(test)]
impl ScriptedEnv {
    #[must_use]
    pub(crate) fn new(values: Vec<u64>) -> Self {
        assert!(!values.is_empty(), "ScriptedEnv needs at least one value");
        Self { values, idx: 0 }
    }
}

#[cfg(test)]
impl Env for ScriptedEnv {
    fn next_election_timeout(&mut self) -> u64 {
        // `new` rejects empty vecs, and `idx` is always taken mod `len`,
        // so `idx < len` holds for every call.
        #[allow(clippy::indexing_slicing)]
        let v = self.values[self.idx];
        self.idx = (self.idx + 1) % self.values.len();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn randomized_env_stays_within_range() {
        let mut env = RandomizedEnv::new(12345, 10, 20);
        for _ in 0..1000 {
            let v = env.next_election_timeout();
            assert!((10..20).contains(&v), "out of range: {v}");
        }
    }

    #[test]
    fn randomized_env_covers_full_range() {
        // Over many draws we should hit every value in [min, max).
        // Pins both the `%` division and the `min + ...` shift.
        let mut env = RandomizedEnv::new(42, 5, 9);
        let mut seen = BTreeSet::new();
        for _ in 0..2000 {
            seen.insert(env.next_election_timeout());
        }
        assert_eq!(seen, BTreeSet::from([5, 6, 7, 8]));
    }

    #[test]
    fn randomized_env_is_not_constant() {
        // Catches a `next_election_timeout -> 0` or `-> 1` mutant.
        let mut env = RandomizedEnv::new(1, 100, 200);
        let first = env.next_election_timeout();
        let mut saw_different = false;
        for _ in 0..50 {
            if env.next_election_timeout() != first {
                saw_different = true;
                break;
            }
        }
        assert!(saw_different, "RNG produced constant output");
    }

    #[test]
    fn randomized_env_uses_seed() {
        // Two different seeds must produce different sequences.
        let mut a = RandomizedEnv::new(1, 0, 1000);
        let mut b = RandomizedEnv::new(2, 0, 1000);
        let seq_a: Vec<u64> = (0..10).map(|_| a.next_election_timeout()).collect();
        let seq_b: Vec<u64> = (0..10).map(|_| b.next_election_timeout()).collect();
        assert_ne!(seq_a, seq_b);
    }

    #[test]
    fn randomized_env_zero_seed_does_not_lock() {
        // xorshift has a fixed point at 0. Seed=0 must be remapped.
        let mut env = RandomizedEnv::new(0, 0, 100);
        let mut seen = BTreeSet::new();
        for _ in 0..200 {
            seen.insert(env.next_election_timeout());
        }
        assert!(seen.len() > 1, "RNG stuck at a single value for seed=0");
    }

    #[test]
    fn randomized_env_is_deterministic_for_seed() {
        let mut a = RandomizedEnv::new(99, 0, 1_000_000);
        let mut b = RandomizedEnv::new(99, 0, 1_000_000);
        for _ in 0..100 {
            assert_eq!(a.next_election_timeout(), b.next_election_timeout());
        }
    }
}
