# SP-F Metrics Slice — Design (Adaptive-Commit Observability, early slice)

**Parent roadmap:** `2026-07-15-adaptive-commit-ga-roadmap-design.md` (SP-F, early slice).
**Status:** Design draft 2026-07-15.

**Goal:** Expose the **durable-frontier lag**, **epoch cadence**, and **recovery incarnation** as
first-class metrics so SP-C (performance) and SP-D (crash/durability) can be *interpreted*, and so
ops can alert on "the durable frontier falling behind." This is the **early** SP-F slice; user-facing
docs/runbook are the **late** slice (separate, GA-polish).

**Why first:** the roadmap makes this the one hard cross-dependency — both Prove-it sub-projects need
frontier-lag instrumentation to read their own results.

## Scope

- OSS core only. Two observability surfaces, matching QuestDB's established split:
  - **Prometheus** — global aggregate gauges/counters (process-level).
  - **`wal_tables()`** — per-table drill-down columns (SQL).
- **Non-goals:** per-table Prometheus labels (cardinality explosion — QuestDB deliberately keeps
  per-table detail in `wal_tables()`); user docs/runbook (late SP-F); Grafana dashboards; shipped
  alerting rules (guidance only).

## Signals (all already exposed by `SeqTxnTracker` — no new state)

| Signal | Derivation | Meaning |
|--------|-----------|---------|
| durable-frontier lag | `getSeqTxn() − getLocalDurableSeqTxn()` | acked txns not yet locally durable = RPO exposure under group-commit W>0 |
| epoch lag / retention | `getSeqTxn() − getDurableEpochSeqTxn()` | recovery replay distance / WAL retention (already surfaced as `walRetentionTxn`) |
| epoch cadence | `getLastEpochTs()` + advance count | how often the durable epoch advances |
| recovery incarnation | `getRecoveryIncarnation()` | number of recoveries (a crash/recover detector) |

## Design

### A. Prometheus global metrics — extend `WalMetrics` (DRY: it already owns the sibling gauges)

`WalMetrics` already registers `wal_apply_seq_txn` and `wal_apply_writer_txn` (global `LongGauge`s
updated on the apply path). Add, alongside them:

- **`wal_apply_local_durable_seq_txn`** (`LongGauge`) — the local durable (adaptive-fsync) frontier,
  updated on the same apply/durability path as its siblings.
- **`wal_adaptive_epoch_advances`** (`Counter`) — incremented once per successful durable-epoch
  advance (in the epoch hook).
- **`wal_adaptive_recovery_incarnation`** (`LongGauge`) — set at boot by the recovery pass.

**Durable-frontier lag is deliberately NOT a separate gauge.** It is
`wal_apply_seq_txn − wal_apply_local_durable_seq_txn`, computed Prometheus/Grafana-side. Rationale:
both operands are already exposed, a third push-site would be a redundant place to get wrong, and the
lag definition stays in exactly one place (the query).

### B. `wal_tables()` per-table columns — extend `WalTableListFunctionFactory`

Current columns include `sequencerTxn`, `writerTxn`, `durableEpochSeqTxn`, `walRetentionTxn`,
`recoveryIncarnation`. Add **two**:

- **`localDurableSeqTxn`** (`LONG`) ← `getLocalDurableSeqTxn()`
- **`lastEpochTs`** (`TIMESTAMP`) ← `getLastEpochTs()`

With `localDurableSeqTxn` added, both lags are queryable per-table
(`sequencerTxn − localDurableSeqTxn` and `sequencerTxn − durableEpochSeqTxn`) with no new derived
column. Plumbing mirrors the existing `durableEpochSeqTxn` column exactly.

## Interfaces

- **`WalMetrics`** gains three members + their updater methods; the local-durable gauge is pushed
  wherever `localDurableSeqTxn` advances (the existing apply-metrics update site / the
  `setLocalDurableSeqTxn` path).
- **Epoch hook** (`ApplyWal2TableJob.maybeAdvanceDurableEpoch`) increments the epoch-advances counter
  after the epoch is published.
- **Recovery pass** (`RecoveryCoordinator`) sets the recovery-incarnation gauge at boot.
- **`WalTableListFunctionFactory`** reads the two new values from the per-table `SeqTxnTracker`,
  mirroring the existing `durableEpochSeqTxn` column.

## Testing (fluent `assertQuery`/`QueryAssertion` per house test style)

- Metric registration + scrape output contains the three new names (mirror existing `WalMetrics`
  test).
- `wal_adaptive_epoch_advances` increments by N across N driven epoch advances on an adaptive table.
- `wal_adaptive_recovery_incarnation` reflects a `recover()` pass.
- `wal_tables()` returns `localDurableSeqTxn` + `lastEpochTs` with correct per-table values.
- **Lag correctness:** under group-commit W>0, after commits `sequencerTxn − localDurableSeqTxn > 0`;
  after a durable flush it returns to 0.

## Acceptance

New Prometheus metrics scrapeable; new `wal_tables()` columns queryable; a short "what these mean +
alert-on-lag" note handed to the SP-C/SP-D work (full user docs = late SP-F). Unblocks SP-C/SP-D.

## Open decisions (resolved in the SP-F plan)

- Recovery-incarnation as **gauge** (last value; aggregated max/sum at scrape) vs counter —
  gauge recommended (it is per-table state, not a process-wide event count).
- Final metric names, aligned to QuestDB's `wal_*` naming conventions.
