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
    cargo llvm-cov --workspace --all-targets {{args}}

# ---------------------------------------------------------------------------
# Fuzzing (cargo-fuzz, requires nightly). Targets live in `fuzz/`.
# Run each for a short smoke with `just fuzz-smoke`; do a longer run
# with `just fuzz-run <target> <seconds>`.
# ---------------------------------------------------------------------------

# List every fuzz target.
fuzz-list:
    cd fuzz && cargo +nightly fuzz list

# Smoke-run every fuzz target for 30 seconds each. CI uses this.
# `--target x86_64-unknown-linux-gnu` avoids cargo-fuzz's default
# musl selection, which conflicts with -Zsanitizer=address.
fuzz-smoke:
    cd fuzz && cargo +nightly fuzz run --target x86_64-unknown-linux-gnu fuzz_proto_message -- -max_total_time=30
    cd fuzz && cargo +nightly fuzz run --target x86_64-unknown-linux-gnu fuzz_engine_events -- -max_total_time=30
    cd fuzz && cargo +nightly fuzz run --target x86_64-unknown-linux-gnu fuzz_disk_storage_recover -- -max_total_time=30

# Run one fuzz target for N seconds: `just fuzz-run fuzz_proto_message 300`.
fuzz-run target seconds="60":
    cd fuzz && cargo +nightly fuzz run --target x86_64-unknown-linux-gnu {{target}} -- -max_total_time={{seconds}}

# Just compile every fuzz target (no execution). Works on stable.
fuzz-check:
    cd fuzz && cargo check --bins

# ---------------------------------------------------------------------------
# Docs: rustdoc + mdBook guide, both published to GH Pages by CI.
# ---------------------------------------------------------------------------

# Build rustdoc for the workspace, no deps.
docs:
    cargo doc --workspace --no-deps --all-features

# Build rustdoc and open it.
docs-open: docs
    cargo doc --workspace --no-deps --all-features --open

# Build the mdBook guide in `book/`.
book:
    cd book && mdbook build

# Serve the mdBook guide locally with live reload.
book-serve:
    cd book && mdbook serve --open

# Build both rustdoc and the book into `target/site/` — matches what CI
# publishes to GH Pages.
site: docs book
    rm -rf target/site
    mkdir -p target/site
    cp -r target/doc target/site/api
    cp -r book/book target/site/guide
    @echo "Site built at target/site/"

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
