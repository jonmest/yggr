default:
    @just --list

build:
    cargo build

check:
    cargo check --all-targets

test:
    cargo nextest run --workspace --all-targets
    cargo test --workspace --doc

# Alias for the fuller run; kept for callers that reach for it.
test-all: test

# Quick feedback loop: lib tests only, skip integration + doc tests.
test-lib:
    cargo nextest run --lib

fmt:
    cargo fmt

fmt-check:
    cargo fmt --check

clippy:
    cargo clippy --all-targets -- -D warnings

proto:
    cargo build -p core

deny:
    cargo deny check

audit:
    cargo audit

unused-deps:
    cargo machete

typos:
    typos

mutants *args:
    cargo mutants {{args}}

# Code coverage (via cargo-llvm-cov). Opens an HTML report by default;
# pass `--summary-only` for a quick terminal view, or `--lcov` for CI.
coverage *args:
    cargo llvm-cov --lib {{args}}

# Static analysis on GitHub Actions workflow files.
zizmor:
    zizmor .github/workflows

# Generate a CycloneDX SBOM for all workspace crates.
sbom:
    cargo cyclonedx --format json

ci: fmt-check clippy test deny audit unused-deps typos zizmor

# Run the GitHub Actions workflow locally via nektos/act. Requires docker.
act *args:
    act {{args}}

# Run just one job (e.g. `just act-job test`).
act-job job:
    act -j {{job}}

# List workflows that would run.
act-list:
    act -l

clean:
    cargo clean
