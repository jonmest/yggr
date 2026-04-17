default:
    @just --list

build:
    cargo build

check:
    cargo check --all-targets

test:
    cargo test --lib

test-all:
    cargo test

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

ci: fmt-check clippy test deny audit unused-deps typos

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
