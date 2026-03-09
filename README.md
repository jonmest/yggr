# jotun

A Raft consensus implementation in Rust. Early work in progress.

## Status

The core types and the protobuf transport mapping exist. The state machine is scaffolded but not implemented. There is no network layer yet.

## Layout

- `core/` — library crate.
  - `src/types/` — domain primitives: `NodeId`, `Term`, `LogIndex`, `LogId`.
  - `src/records/` — Raft messages: vote, append entries, log entries.
  - `src/engine/` — the state machine. Currently a skeleton.
  - `src/transport/protobuf/` — the wire format, generated from `raft.proto`.
  - `src/transport/mapping/` — conversions between core types and protobuf. This is the validation boundary.

## Build

```
just build
```

## Test

```
just test
```

The mapping layer is covered by property tests. They round-trip every message type through protobuf and back, fuzz arbitrary bytes through the decoder to check for panics, and verify that invalid input returns a `ConvertError` rather than passing through.

## CI

`just ci` runs the full suite locally: formatting, clippy, tests, license and advisory checks, unused-dependency detection, and typos. GitHub Actions runs the same jobs on push. You can run the workflow locally with `just act`.

## License

Business Source License 1.1
