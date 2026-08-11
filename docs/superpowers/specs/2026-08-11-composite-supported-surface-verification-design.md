# Composite Partitioning — Verifying the Supported Surface (Sub-project 8) — Design

**Status:** approved 2026-08-11. Sub-project 8 of 8 (built FIRST). Successor doc to
`2026-07-15-composite-partitioning-design.md` (§3 "Phasing"); this is the test-completion sub-project
that precedes Phases 2–4.

## 1. Why this, and why first

Composite partitioning is deep on the ingest/query path and deliberately narrow everywhere else:
~20 operations are refused by loud gates, and everything that is supported is asserted by 319
deterministic, hand-written tests across 44 files.

**There is no composite fuzz test of any kind.** QuestDB's confidence in WAL/O3 correctness comes
from its fuzz harnesses; composite has inherited none of that. Crash/power-loss coverage exists only
for the two fast-append paths.

The master-merge audit (2026-08-10) found three real defects, all of the same shape: behaviour that
was *correct by accident* and pinned by nothing —

- master's WAL block fast-append reached composite and was rejected only by an unrelated guard;
- `LiveViewRecordCursorFactory` inherited a capability default and lied about a concurrent cursor;
- `EntCreateTableOperationImpl` silently dropped the composite spec, so **Enterprise created plain
  tables with no error at all**.

Each was found by a test that happened to exist, or by hand. The lesson is that this feature's
correctness is currently under-instrumented relative to its blast radius. Sub-projects 1–7 will each
modify the write path, the partition-addressing model, or both. Doing them against the present level
of coverage means changing a 157-commit feature with a hand-written safety net.

This sub-project builds the net first. It changes **no production behaviour except adding one gate**.

## 2. Goals and non-goals

**Goals**

1. A seeded, replayable differential fuzz harness whose oracle is the design premise itself:
   a composite table must be indistinguishable from its plain twin.
2. Gates become first-class fuzz outcomes: a refused operation must throw *and leave no damage*.
3. Close deterministic coverage gaps that randomness reaches unreliably.
4. Extend crash/power-loss coverage past fast-append.
5. Close the one live silent-wrong risk: materialized views over a composite base.

**Non-goals**

- Implementing any gated operation — that is sub-projects 1–6.
- Materialized-view *support* — sub-project 7. This lands only the gate.
- Enterprise composite fuzz and replication verification — sub-project 6.
- Performance benchmarking. The existing JMH harness (deferred-issues #2) stands.

## 3. Units

| Unit | Deliverable | Depends on |
|---|---|---|
| U1 | `CompositeFuzzRunner` + `CompositeFuzzTest` — differential twin fuzz | — |
| U2 | Gate-boundary outcomes + `CompositeFuzzOpCoverageTest` | U1 |
| U3 | Deterministic matrix completion | — |
| U4 | Crash/power-loss injection behind a flag | U1 |
| U5 | Mat-view loud gate + silent-skip tests | — |

U3 and U5 are independent of the harness and may be built in parallel with U1.

## 4. U1 — the differential harness

### 4.1 Approach

**Composite-owned, composing the existing generator.** A new `CompositeFuzzRunner` under
`core/src/test/java/io/questdb/test/cairo/fuzz/` uses `FuzzTransactionGenerator` and the existing
`Fuzz*Operation` types, but owns table creation, the apply loop, gate expectations and fault
injection.

Rejected alternatives: extending the shared `FuzzRunner` (composite concepts would leak into
infrastructure many unrelated suites depend on, and a mistake destabilises them); a bespoke harness
(discards a mature generator).

The known drawback of an owned harness — it does not automatically inherit new fuzz operations — is
neutralised by U2's coverage guard, which fails when an unclassified operation appears.

### 4.2 Subject and reference

- **Subject** `<base>_composite`: randomized composite spec, WAL.
- **Reference** `<base>_plain`: identical column set, identical designated timestamp, same time unit,
  plain `PARTITION BY <unit>`, WAL.

One transaction list is generated once and applied to **both** tables. The reference is what
composite is compared against; it is never itself the thing under test.

### 4.3 Randomized axes (all derived from the run seed)

| Axis | Values |
|---|---|
| Dimension count | 1, 2, 3 |
| Dimension kind | `identity`, `hash(col, N)`, `truncate(col, N)`, `(expr) AS name` |
| Layout | `HIVE`, `PLAIN` |
| `ORDER BY` clustering | present / absent |
| Cell cardinality | small (2–4), medium (~16), at cap (64), above cap (96) |
| Fast-append flag | `cairo.wal.composite.fastappend.enabled` true / false |
| Commit shape | inherited from `FuzzTransactionGenerator` (O3, equal-ts, rollback, cancel-rows, replace) |

`hash(col, N)` uses N from {8, 32, 64}; `truncate(col, N)` uses N from {1, 3}. The expression
dimension uses a string-coercible safe-subset expression over a SYMBOL column, matching what
`CompositeExpressionDimTest` already proves supported.

### 4.4 Oracle — compared shapes

After applying all transactions and draining the WAL queue, every one of these must be identical
between subject and reference:

1. Full scan `ORDER BY ts` (forward) and `ORDER BY ts DESC` (backward).
2. `count(*)`, `min(ts)`, `max(ts)`.
3. `LATEST ON ts PARTITION BY <symbol col>`.
4. `SAMPLE BY` over a bucket coarser than the time unit, with a keyed aggregate.
5. Dimension-filtered reads: `WHERE <dim col> = 'v'` and `WHERE <dim col> IN (…)`, for a value known
   to exist and one known not to.
6. A timestamp-bounded interval scan crossing at least one partition boundary.
7. A windowed aggregate with the table as the window-join slave.

Composite-only sanity, not compared to the twin because plain has no equivalent:
`table_partitions()` row count equals the number of distinct `(day, cell)` pairs, and every
partition directory named in it exists on disk.

### 4.5 Anti-vacuity — the harness must prove it exercised what it claims

A differential fuzz that never routes a second cell would pass while testing nothing. This is the
failure mode that made the first probe of the master-merge audit worthless, and it is designed
against explicitly.

Each run accumulates counters and **fails the run** if any floor is unmet:

| Counter | Floor |
|---|---|
| Distinct cellKeys actually routed | ≥ 2 (≥ 1 only when the axis deliberately chose a single-cell shape) |
| Commits that took the composite O3 merge path | ≥ 1 |
| Commits that took the fast-append path (when the flag is on) | ≥ 1 |
| Rows landing in a non-last partition (O3 into an existing cell) | ≥ 1 |
| Gated operations actually attempted (U2) | ≥ 1 |

Counters are read from the existing composite counters where available and from a test-only
`@TestOnly` accessor otherwise. The floors are asserted **after** the run, with a message naming the
seed and the axis choices, so an under-exercising seed is a loud failure rather than a green run.

## 5. U2 — gates as first-class outcomes

### 5.1 Classification

Every operation type is classified `SUPPORTED` or `GATED` in one table owned by the harness:

| Operation | Class | Rationale |
|---|---|---|
| `FuzzInsertOperation`, `FuzzStableInsertOperation`, `DuplicateFuzzInsertOperation` | SUPPORTED | routed ingest is the feature |
| `FuzzQueryOperation`, `FuzzValidateSymbolFilterOperation` | SUPPORTED | read path is supported |
| `FuzzTruncateTableOperation` | SUPPORTED | `CompositeUnsupportedOpsTest#testTruncateStillWorks` |
| `FuzzDropCreateTableOperation` | SUPPORTED | table-level, not partition-level |
| `FuzzAddColumnOperation` | SUPPORTED for non-SYMBOL, GATED for SYMBOL | `ADD COLUMN of type SYMBOL` is gated |
| `FuzzDropColumnOperation` | GATED | `does not yet support DROP COLUMN` |
| `FuzzRenameColumnOperation` | GATED | `does not yet support RENAME COLUMN` |
| `FuzzChangeColumnTypeOperation` | GATED | `does not yet support ALTER COLUMN TYPE` |
| `FuzzAddCoveringIndexOperation` | GATED | `does not yet support ADD INDEX` |
| `FuzzDropPartitionOperation` | GATED | `does not yet support DROP PARTITION` |
| `FuzzConvertPartitionToParquetOperation` | GATED | `CONVERT PARTITION TO PARQUET` |
| `FuzzConvertPartitionToNativeOperation` | GATED | `CONVERT PARTITION TO NATIVE` |
| `FuzzSetTtlOperation` | GATED | `TTL-based partition eviction` |
| `FuzzSetTableFormatOperation`, `FuzzSetParquetEncodingOperation` | GATED | parquet-form ops |
| `FuzzChangeSymbolCapacityOperation` | SUPPORTED | must NOT throw. Composite silently skips the autoscale itself (§8), so the assertion is that the statement succeeds and the table stays twin-equal — not that the capacity actually changed |

### 5.2 Rejection protocol

When a `GATED` operation is applied to the subject:

1. It must throw `CairoException` whose message contains `composite`.
2. The reference table skips the operation, keeping the twin aligned.
3. Post-rejection the subject must be: readable; unchanged in `count(*)`; and **still twin-equal
   across every §4.4 shape**.

Step 3 is the part nothing tests today. A gate that throws after partially mutating `_txn` or the
directory tree would pass every existing test and fail here.

### 5.3 Coverage guard

`CompositeFuzzOpCoverageTest` enumerates the `Fuzz*Operation` implementations by reflection over the
`io.questdb.test.fuzz` package and fails when one is not present in the classification table. Its
message states which class is unclassified and that the decision is supported-vs-gated. When
sub-projects 1–6 implement an operation, the same table is where the classification flips, and the
fuzz immediately begins demanding twin-equality instead of rejection.

## 6. U3 — deterministic matrix completion

Explicit named tests for what randomness reaches unreliably, or where an explicit assertion is worth
more than a probability:

- `LAYOUT PLAIN`: routing, on-disk names, `SHOW CREATE TABLE` round-trip. Today only 2–3 files touch
  it versus 14 for the HIVE default.
- Expression dimensions end-to-end against a plain twin, including a value that changes bucket.
- Fast-append flag **off**: full parity run, proving the feature is not load-bearing for correctness.
- Cell-cap boundary: exactly 64 open cells, and eviction at 96, asserting non-truncating close.
- Day-roll with multiple live cells.
- Never-routed empty composite table: gates must not fire (`testGatesDoNotFireOnNeverRoutedEmptyCompositeTable`
  already covers the DDL side; extend to the read side).

## 7. U4 — crash and power-loss

Reuses the `FilesFacade` fault-injection approach already proven by `CompositeFastAppendCrashTest`,
generalised to the fuzz: behind `-Dcomposite.fuzz.crash=true`, the run selects a random commit and
fails a write at it, then reopens the engine, replays, and asserts twin equality.

Crash points to sample: `_txn` commit, `_cv` commit, cell column append, cell segment open, WAL apply
mid-drain. Each recovery must land on one of exactly two acceptable states — the transaction applied
in full, or not at all — matching the single-linearization-point property the fast-append work
established.

Nightly profile only; a crash-on failure is triaged as a recovery bug, a crash-off failure as a
correctness bug.

## 8. U5 — mat-view gate and the silent skips

`cairo/mv/` contains no composite awareness and no composite mat-view test exists, so a materialized
view over a composite base is neither supported nor refused today. This unit adds a **loud gate** at
materialized-view creation when the base table is composite, plus a rejection test. Whether mat views
eventually gain real per-cell refresh is sub-project 7's decision; this makes the surface safe in the
interim.

Two operations are silently skipped for composite and log it: split-fragment squash and
symbol-capacity autoscale. Each gets a test asserting the skip happens **and** that the table remains
correct and twin-equal afterwards — a silent skip is only acceptable if it is provably harmless.

## 9. Failure ergonomics

Every failure message carries: the run seed, the generated composite DDL, the transaction index that
failed, the axis selections, and whether crash injection was on. A failing run is reproduced with a
single `-Dfuzz.seed=<seed>`. Nightly logs every seed it runs so a failure is replayable from CI
output alone.

## 10. CI policy

- **PR:** `CompositeFuzzTest` over a small fixed seed set, bounded row and transaction counts, no
  crash injection. Budget under two minutes. QuestDB's CI cost has roughly doubled over twelve
  months, so this stays deliberately bounded.
- **Nightly:** random seeds, larger counts, crash injection on, all seeds logged.

## 11. File structure

**New**

- `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzRunner.java` — harness: table creation,
  apply loop, comparison, counters, gate protocol.
- `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzTest.java` — PR-profile entry points.
- `core/src/test/java/io/questdb/test/cairo/fuzz/CompositeFuzzOpCoverageTest.java` — classification guard.
- `core/src/test/java/io/questdb/test/cairo/CompositeLayoutPlainTest.java` — U3 layout coverage.
- `core/src/test/java/io/questdb/test/cairo/CompositeSilentSkipTest.java` — U5 skips.
- `core/src/test/java/io/questdb/test/griffin/CompositeMatViewGateTest.java` — U5 gate.

**Modified**

- `core/src/main/java/io/questdb/griffin/SqlCompilerImpl.java` — the single production change in this
  sub-project. The mat-view composite gate goes in the existing base-table validation block at
  ~`:4576–4588`, immediately alongside `base table must be a WAL table` and `live views are not
  allowed as base tables in V1`, and reads the base table's dimension count from its metadata. It
  throws `SqlException` at `op.getBaseTableNamePosition()`, matching its neighbours, so the error
  carries a caret position like every other CREATE MATERIALIZED VIEW rejection.
- `core/src/main/java/io/questdb/cairo/TableWriter.java` — `@TestOnly` counter accessors only, if the
  existing composite counters prove insufficient for §4.5.

## 12. Risks

- **Flake.** A fuzz in PR CI that flakes is worse than no fuzz. Mitigated by fixed seeds in PR,
  randomness confined to nightly, and the §4.5 floors failing loudly rather than intermittently.
- **Vacuity.** Addressed directly by §4.5; without those counters this design would be worthless.
- **Twin drift.** The reference must stay genuinely equivalent; a divergence in *setup* would produce
  false failures. Mitigated by generating both DDLs from one column model.
- **Harness rot.** Addressed by the U2 coverage guard.

## 13. Relationship to the rest of the roadmap

Sub-projects 1–6 each implement gated operations; every one of them flips an entry in §5.1 from
`GATED` to `SUPPORTED`, at which point this harness becomes their acceptance test. Sub-project 7
(mat views) replaces U5's gate with either support or a permanent refusal. The harness is therefore
built once and amortised across the entire remaining roadmap.
