# Contributing

Thanks for your interest. This document covers how to get set up and what to expect when contributing.

## Setup

You need Rust (stable) and `just`. The toolchain is pinned in `rust-toolchain.toml`, so rustup will install the right version automatically.

For the fast local gate you also need a few cargo tools:

```
cargo install cargo-deny cargo-audit cargo-machete typos-cli cargo-llvm-cov
```

If you want to run the heavier local verification lanes, also install:

```bash
cargo install cargo-mutants cargo-udeps cargo-fuzz --locked
```

Kani is separate from normal cargo tooling. To run the bounded proofs locally:

```bash
cargo install --locked kani-verifier
cargo kani setup
```

If you want to run the GitHub Actions workflow locally, install `act` and Docker.

## Build and test

```
just build
just test
```

Before opening a pull request, run the fast local gate:

```
just ci
```

This runs formatting, clippy, tests, rustdoc with warnings denied, license/advisory checks, unused-dependency detection, and typos.

If you want the closest local approximation of pull-request CI, run:

```bash
just ci-pr
```

That adds coverage enforcement, `cargo-udeps`, a loom model test, and the fuzz smoke targets. GitHub Actions still has extra lanes that are either external-service-backed or intentionally heavier/scheduled (`cargo-mutants`, runtime chaos nightly, Kani, and the longer fuzz/property-test sweeps).

## Pull requests

- Keep changes focused. Separate refactors from behavior changes.
- Add tests for new behavior. For anything in the `transport/mapping` layer, add both a round-trip property test and a negative test.
- Clippy warnings are errors in CI. Fix them or explain why they are wrong with a scoped `#[allow]` and a comment.
- Don't check in commented-out code. If you want to preserve something, put it in the PR description.

## Commit messages

Write imperative, specific commit messages. "Fix panic in append-entries decoder" is good. "Updates" is not.

## License

By contributing, you agree that your contributions are licensed under the Business Source License 1.1, the same as the rest of the project.
