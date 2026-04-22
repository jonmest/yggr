# Testing

This project uses several testing and verification layers.

Each layer checks a different failure mode.

The goal is not only to make tests pass. The goal is to catch incorrect behavior early and from more than one angle.

## Fast local commands

Run the standard local gate:

```bash
just ci
```

This runs formatting, clippy, workspace tests, rustdoc with warnings denied, dependency checks, and typo checks.

Run the heavier local verification gate:

```bash
just ci-pr
```

This adds coverage enforcement, `cargo-udeps`, the loom model test, and the fuzz smoke run.

## Main test layers

Run the workspace tests:

```bash
just test
```

This runs unit tests, integration tests, and doc tests.

These tests check normal behavior and common regressions.

## Coverage

Run coverage locally:

```bash
just coverage
just coverage-check
```

Coverage does not prove correctness.

Coverage shows which code paths the test suite reaches.

The coverage check enforces a minimum workspace line-coverage floor in CI.

## Fuzzing

Run the fuzz smoke suite:

```bash
just fuzz-smoke
```

Run one target longer:

```bash
just fuzz-run fuzz_proto_message 300
```

Fuzzing feeds many unexpected inputs into selected boundaries.

It is useful for decoders, parsers, storage recovery, and other edge-heavy code.

## Mutation testing

Run mutation testing:

```bash
just mutants
```

Mutation testing changes the code on purpose and checks whether the tests notice.

It is useful for finding tests that only exercise code without really constraining behavior.

PR CI runs a diff-scoped mutant smoke.

Scheduled CI runs a larger sweep.

## Loom

Run the loom model test:

```bash
just loom-test
```

Loom checks small concurrent models under many possible interleavings.

It is useful for shutdown, task handoff, and race-sensitive coordination logic.

The current loom test focuses on the transport task-registry shutdown race.

## Kani

Run the bounded proofs for `yggr-core`:

```bash
just kani
```

Kani model-checks small proof harnesses.

It is useful for local safety properties on the pure engine, where the state space is bounded and deterministic.

The current proofs check hard-state recovery, monotonic term and commit behavior, and vote uniqueness.

## Dependency checks

Run unused-dependency analysis:

```bash
just udeps
just unused-deps
```

`cargo-udeps` finds dependencies that are not used by the compiled crate graph.

`cargo-machete` gives a faster unused-dependency pass.

These checks keep the workspace smaller and reduce accidental surface area.

## Rustdoc

Run the documentation check:

```bash
just docs-check
```

This builds rustdoc with warnings denied.

It catches broken docs and public API documentation drift.

## CI shape

GitHub Actions runs the standard checks on pushes and pull requests.

It also runs coverage, `cargo-udeps`, and the loom test.

Scheduled or manual CI runs the heavier lanes.

Those lanes include runtime chaos, heavier property-test counts, mutation testing, fuzzing, and Kani.

## Why the stack is layered

No single tool is enough.

Unit and integration tests check expected behavior.

Property tests check invariants across many generated inputs.

Chaos tests check distributed behavior under adverse scheduling.

Fuzzing checks boundary robustness.

Mutation testing checks whether assertions are strong enough.

Loom checks small concurrent state spaces.

Kani checks bounded engine properties with a model checker.

The overlap is intentional.
