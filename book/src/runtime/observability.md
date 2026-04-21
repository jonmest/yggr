# Observability

Two independent surfaces: pull-model metrics via `Node::metrics()` for dashboards and health checks, structured `tracing` events for correlated per-event detail.

## Metrics

`node.metrics().await` returns a snapshot of counters and gauges for the local node. Counters only move forward; gauges reflect current state. Pulling is cheap — no engine work, just a read of internal counters.

```rust
let m = node.metrics().await?;
println!(
    "term={} role={} commit={} log_len={}",
    m.current_term.get(),
    m.role_code,
    m.commit_index.get(),
    m.log_len,
);
println!(
    "elections={} won={} ae_sent={} committed={} applied={}",
    m.elections_started, m.leader_elections_won,
    m.append_entries_sent, m.entries_committed, m.entries_applied,
);
```

Counter families:

- **Elections** — `elections_started`, `pre_votes_granted`, `pre_votes_denied`, `votes_granted`, `votes_denied`, `leader_elections_won`, `higher_term_stepdowns`.
- **Replication** — `append_entries_sent`, `append_entries_received`, `append_entries_rejected`, `entries_appended`, `entries_committed`, `entries_applied`.
- **Snapshots** — `snapshots_sent` (per chunk), `snapshots_installed` (per completed snapshot).
- **Reads** — `read_index_started`, `reads_completed`, `reads_failed`.

Gauges:

- `current_term`, `commit_index`, `last_applied`, `log_len`, `role_code` (0=follower, 1=precandidate, 2=candidate, 3=leader).

`EngineMetrics` is `#[non_exhaustive]`; new fields are additive. See [rustdoc](../api/jotun_core/engine/metrics/struct.EngineMetrics.html) for the exact set.

## Tracing

The engine and runtime emit structured `tracing` events and spans with stable targets and field names.

### Targets

| Target | What emits |
|---|---|
| `jotun::engine` | Role changes, term advances, vote decisions, AppendEntries accept/reject, commit advances. |
| `jotun::node` | Driver-level events: apply failures, transport errors, shutdown. |

### Fields

- `node_id` — the emitting node
- `term` / `from_term` / `to_term` — term transitions
- `role` — `"follower" | "candidate" | "leader"`
- `decision` — on vote handling, `"granted" | "rejected"`

### OpenTelemetry

The library doesn't depend on `opentelemetry` directly. The OTel crates churn fast, and pinning them in a library creates version conflicts for users. Wire your subscriber in your service's `main`:

```rust
use tracing_subscriber::prelude::*;

let otlp = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(opentelemetry_otlp::new_exporter().tonic())
    .install_batch(opentelemetry::runtime::Tokio)?;

tracing_subscriber::registry()
    .with(tracing_opentelemetry::layer().with_tracer(otlp))
    .with(tracing_subscriber::fmt::layer())
    .with(tracing_subscriber::EnvFilter::from_default_env())
    .init();
```

`jotun::engine` spans and events flow to the collector.

### Log filters

Quick-start `RUST_LOG` values:

- `jotun=debug` — everything jotun emits at debug or above
- `jotun::engine=info` — just protocol decisions
- `jotun::node=debug,jotun::engine=info` — runtime detail, protocol overview
