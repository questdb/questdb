# Composite Partitioning — Scope Closure Index

**Purpose:** the audit artifact for "every gate is owned by exactly one spec". If a gate exists in
the code and is not in this table, the roadmap has a hole.

Generated 2026-08-11 against the gate messages in `TableWriter` and the create-time validation sites.
**Re-audited 2026-08-17 across the WHOLE of `core/src/main`** — the original sweep's scope was itself
the defect: it missed nine gates, including every read-side restriction. The audit is now
reproducible, and any future one must use this command:

```bash
grep -rn -oE '"[^"]*composite[^"]*"' core/src/main/java/ \
  | grep -viE "renderCellSegment|resolveCellKey|must not be called"
```

Every refusal it prints must appear in the ownership table below, or in "deferred by decision".
There is no third category.

## Build order

Sub-project 8 is built **first** — it is the differential test harness every other sub-project is
graded against. Sub-project 1 is next, because its addressing decision propagates into 3, 6 and 7.

```
Wave 0 (earliest-refusal + O3-purge proof)
  → 8 (verify) → 9A (per-cell interval cursors) → 1 (lifecycle) → 2 (column DDL)
  → 9B/9C (index cell-awareness) → 4 (row-level + commit shapes) → 5 (create-time)
  → 3 (parquet) → 9D (native-only cursor gates) → 6 (Enterprise) / 7 (mat views)
```

**Wave 0** fixes refusals that fire later than the statement that caused them, plus the one silent
skip that violates invariant 2. It lifts no capability; it makes the current behaviour honest.

Wave 0 originally had a third item — refusing indexed columns at CREATE on a composite table — which
was **built, measured and WITHDRAWN** on 2026-08-17. An indexed DIMENSION column already works and
delivers cell pruning (the core value of subpartitioning, covered by the ~860-line
`CompositeCellPruningTest`), and the index shapes that ARE refused are refused at the `SELECT` that
used them, which already satisfies invariant 6. The gate cost 35 tests across 6 suites to fix a trap
that did not exist. See the wave-0 plan's Task 3 for the full record.

**9A precedes 1 and 2 deliberately.** Composite reads already ship, and that cursor design has
produced three defects (two silently wrong answers, one returning no rows). Fixing what is broken
outranks adding capability.

3 depends on 1 (a parquet cell must still be addressable). 6 depends on 3 (tiering moves parquet
cells) and on 5 (Enterprise CTAS). 7 depends on 1 (refresh after partition removal).

## Gate ownership

| # | Gate / restriction | Owner spec |
|---|---|---|
| 1 | `DROP PARTITION` — **SUPPORTED 2026-08-18**; whole-day in 1B, per-cell (`LIST '<day>/<cell>'`) in 1C | 1 — partition-lifecycle (done) |
| 2 | `FORCE DROP PARTITION` — **SUPPORTED 2026-08-18 (1D)**; whole days, and its LIST parser already makes a cell-qualified name unreachable | 1 — partition-lifecycle (done) |
| 3 | `DETACH PARTITION` | 1 — partition-lifecycle |
| 4 | `ATTACH PARTITION` | 1 — partition-lifecycle |
| 5 | `SQUASH PARTITIONS` (split-fragment squash; today a silent skip) | 1 — partition-lifecycle |
| 6 | TTL-based partition eviction — **SUPPORTED 2026-08-18 (1D)**; whole days only, per-dimension TTL remains deferred by decision | 1 — partition-lifecycle (done) |
| 7 | `DROP COLUMN` | 2 — column-ddl |
| 8 | `RENAME COLUMN` | 2 — column-ddl |
| 9 | `ALTER COLUMN TYPE` | 2 — column-ddl |
| 10 | `ALTER COLUMN TYPE` → SYMBOL (narrower interner guard) | 2 — column-ddl |
| 11 | `ADD COLUMN` of type SYMBOL | 2 — column-ddl |
| 12 | `ADD INDEX` | 2 — column-ddl |
| 13 | `DROP INDEX` | 2 — column-ddl |
| 14 | REINDEX | 2 — column-ddl |
| 15 | `CONVERT PARTITION TO PARQUET` | 3 — parquet-format |
| 16 | `CONVERT PARTITION TO NATIVE` | 3 — parquet-format |
| 17 | `switchNativePartitionWithParquet` (gate added by the 2026-08-10 merge audit) | 3 — parquet-format |
| 18 | `commitPendingParquetToNativeConversions` (gate added by the 2026-08-10 merge audit) | 3 — parquet-format |
| 19 | POSTING index seal on a partition | 3 — parquet-format |
| 20 | Covering POSTING index reseal on a PARQUET partition | 3 — parquet-format |
| 21 | `UPDATE` | 4 — row-level-ops (**PERMANENT**, decided 2026-08-18: out of scope by design, like non-WAL. Keeps composite column lifetimes tied to partition lifetimes, which is what lets column purge stay cell-aware without migrating a positional system-table schema) |
| 22 | DEDUP (`CREATE` and `ALTER … DEDUP ENABLE`) | 4 — row-level-ops |
| 23 | REPLACE-range commit mode | 4 — row-level-ops |
| 24 | `CREATE TABLE AS SELECT` | 5 — create-time |
| 25 | Non-WAL composite (`requires a WAL table`) | 5 — create-time (**permanent**, by design) |
| 26 | Enterprise cold storage / tiering | 6 — enterprise |
| 27 | Enterprise `PARTITION_SEAL` WAL event | 6 — enterprise |
| 28 | Enterprise checkpoint / backup manifest | 6 — enterprise |
| 29 | Enterprise replication (no gate — **unverified**) | 6 — enterprise |
| 30 | Materialized views over a composite base — **GATED**, added by sub-project 8 in `executeCreateMatView`; refuses at CREATE with a caret. The "no gate today" wording here was stale and misread on 2026-08-18 as an ungated risk | 7 — matview (removing the gate is 7's deliverable) |
| 31 | Multiple sub-day time intervals over a single multi-cell day | 9 — read-surface (9A) |
| 32 | Indexed `WHERE` predicate | 9 — read-surface (9B) |
| 33 | `ORDER BY` on an indexed symbol column | 9 — read-surface (9C) |
| 34 | Cross-cell merge supports native partitions only | 9 — read-surface (9D, needs 3) |
| 35 | Time-frame permutation supports native partitions only | 9 — read-surface (9D, needs 3) |
| 36 | `FORMAT PARQUET` (write-path: DDL accepted, next commit suspends) | 3 — parquet-format (refusal moved to DDL in wave 0) |
| 37 | Interleaved multi-cell commit with a var-size column | 4 — row-level ops **and commit shapes** |
| 38 | Checkpoint/snapshot restore of an indexed column | 2 — column-ddl |

Silent skips, which are not gates but must become real behaviour or provably-harmless skips:

| Skip | Owner spec |
|---|---|
| split-fragment squash ("cell-blind merge, cell-aware squash deferred") | 1 — lifecycle (behaviour), 8 (proof it is harmless meanwhile) |
| symbol-capacity autoscale ("cell-blind reopen") | 8 — proof it is harmless; no behaviour change planned |
| **O3 partition purge** (`O3PartitionPurgeJob:224`) | **RESOLVED 2026-08-18 — and the attribution here was wrong.** The measured leak (composite 1→4 day directories vs plain 1→1) was real, but it was NOT caused by this skip. Running the same churn with the purge job ON and OFF leaves a byte-identical directory set: `TableWriter.openLastPartitionAndSetAppendPosition` opened a day-level "last partition" for a routed composite table, and `openPartition` resolved it cell-blind then `ff.mkdirs`-ed it into existence. Fixed at the producer (`eb539b7ddf`); `CompositeO3PurgeSkipTest` now passes un-ignored. **This skip remains, and remains correct**: the LIVE composite container is the UNVERSIONED day directory, so a naive cell-aware purge that keeps the newest `<day>.<txn>` would delete every live cell. Lifting it is not sub-project 1 work and has no known outstanding cost. |

## Deferred by decision — recorded, not forgotten

These are choices, not oversights. Each is deliberately excluded from the eight sub-projects.

| Item | Decided in | Why deferred |
|---|---|---|
| Per-dimension TTL (keep BTC 90d, others 7d) | 1 §5.6 | needs new DDL + per-cell retention metadata; TTL's current meaning is unchanged |
| Cell-level `CONVERT` granularity questions | 3 | resolved: per-cell |
| Single `(cluster, ts)`-sorted parquet per day | 3 §7 | the high-cardinality-safe form; additive later, would fork the addressing model now |
| Row migration on dimension-column `UPDATE` | 4 §3 | turns UPDATE into cross-partition data movement |
| Cross-cell dedup for keys omitting dimension columns | 4 §2 | deliberately excluded — D1 makes dedup correct by construction instead |
| Non-WAL composite | 5 §3 | **permanent** architectural boundary, not a gap |
| Per-dimension storage policies | 6 §7 | sibling of per-dimension TTL |
| Symbol-aware incremental mat-view refresh (recompute touched cells only) | 7 §7 | performance work that presupposes correctness |
| Composite-partitioned mat-view storage | 7 §D3 | a view's storage table is plain by construction |
| Dimension/cluster column mutability | 2 §D3 | partition-spec evolution = Phase 4 of the original design |
| EXPRESSION dimensions (4 code sites: evaluation, compilation, column reference, writer-side symbol table) | 9 §1 | an unbuilt Phase-4 FEATURE, not a restriction on an action a user can perform today. Plan 4e exists; it is not a gate to lift |
| **Windows-illegal characters in dimension values** | 1 §8 | `putPathSafe` lets `* ? : \| " < >` reach directory names; illegal on Windows. Free to fix while unreleased, an on-disk format break afterwards — **decide before release** |

## Cross-cutting invariants

Every sub-project must preserve these; each spec restates them in its testing section.

1. **Plain byte-identity.** A `dimCount == 0` table's `_txn` and `_meta` bytes are unchanged by any
   of this work.
2. **No silent path.** Composite either behaves as the plain twin or fails loudly. A skip is
   acceptable only with a test proving it harmless.
3. **Atomicity.** Any multi-cell operation is one `_txn` commit — all cells or none.
4. **Values, not ordinals.** `cellKey` is table-local; anything crossing a table boundary (ATTACH,
   backup/restore, replication) re-resolves dimension values through the local registry.
5. **Acceptance is differential.** A sub-project is done when its operations flip from `GATED` to
   `SUPPORTED` in sub-project 8's classification table and pass the twin comparison. Flipping that
   classification enrols the operation in the differential fuzz automatically, so a lifted gate gains
   coverage by construction.
6. **A refusal fires at the statement that caused it**, never at a later one. A gate that surfaces
   during an unrelated operation misattributes the failure.
7. **Every new test is shown to FAIL with its fix reverted**, and the result is recorded in the commit
   message. Added 2026-08-17: three tests written that day passed against a defective build.
8. **Performance is measured and recorded per operation, never gating** (decision 2026-08-17).
   Correctness first; a slower composite does not block a gate being lifted.

## Spec files

| Sub-project | File |
|---|---|
| 8 | `2026-08-11-composite-supported-surface-verification-design.md` |
| 1 | `2026-08-11-composite-partition-lifecycle-design.md` |
| 2 | `2026-08-11-composite-column-ddl-design.md` |
| 3 | `2026-08-11-composite-parquet-format-design.md` |
| 4 | `2026-08-11-composite-row-level-ops-design.md` |
| 5 | `2026-08-11-composite-create-time-design.md` |
| 6 | `2026-08-11-composite-enterprise-design.md` |
| 7 | `2026-08-11-composite-matview-design.md` |
| 9 | `2026-08-17-composite-read-surface-design.md` |


## Appendix — audit keys (machine-checkable)

The ownership table above is prose; this block is the exact set of refusal strings present in
`core/src/main` at the last audit. It exists because the audit must be a **string-set difference**, not
a fuzzy match: a first attempt at automating it reported five false orphans purely from backticks and
camelCase in the prose table.

The finish line for the whole effort is that this block becomes empty — every line either deleted from
the code or moved to "deferred by decision" with a reason.

```
ADD COLUMN of type SYMBOL is not yet supported on composite-partitioned tables
ALTER COLUMN TYPE SYMBOL is not yet supported on composite-partitioned tables
composite cross-cell merge supports native partitions only
composite partitioning does not support pending parquet-to-native conversions
composite partitioning does not yet support ADD INDEX
composite partitioning does not yet support ALTER COLUMN TYPE
composite partitioning does not yet support ATTACH PARTITION
composite partitioning does not yet support CONVERT PARTITION TO NATIVE
composite partitioning does not yet support CONVERT PARTITION TO PARQUET
composite partitioning does not yet support DEDUP UPSERT KEYS
composite partitioning does not yet support DETACH PARTITION
composite partitioning does not yet support DROP COLUMN
composite partitioning does not yet support DROP INDEX
composite partitioning does not yet support FORMAT PARQUET
composite partitioning does not yet support ORDER BY on an indexed symbol column
composite partitioning does not yet support REINDEX TABLE
composite partitioning does not yet support RENAME COLUMN
composite partitioning does not yet support SQUASH PARTITIONS
composite partitioning does not support UPDATE
composite partitioning does not yet support a POSTING index seal on this partition
composite partitioning does not yet support a covering POSTING index reseal on a PARQUET partition
composite partitioning does not yet support an EXPRESSION dimension referencing column '
composite partitioning does not yet support an indexed WHERE predicate
composite partitioning does not yet support checkpoint/snapshot restore of an indexed column
composite partitioning does not yet support switching a native partition to parquet
composite partitioning does not yet support the REPLACE commit mode
composite partitioning is not yet supported with CREATE TABLE AS SELECT
composite partitioning requires a WAL table
composite partitioning: an interleaved multi-cell commit is not yet supported for a table with a var-size column
composite table, skipping O3 partition purge (day-blind walk, cell-aware purge deferred)
composite table, skipping split-fragment squash (cell-blind merge, cell-aware squash deferred)
composite table, skipping symbol capacity autoscale (cell-blind reopen, cell-aware autoscale deferred)
composite time-frame permutation supports native partitions only
materialized views are not yet supported over a composite-partitioned base table
```

Re-generate with:

```bash
grep -rhoE '"[^"]*composite[^"]*"' core/src/main/java/ \
  | grep -viE "renderCellSegment|resolveCellKey|must not be called" \
  | grep -iE "does not (yet )?support|not yet supported|supports native|requires a WAL|skipping" \
  | sed 's/"//g; s/ \[.*//; s/;.*//' | sort -u
```

Compare as a set (the block is the FIRST fenced block in this appendix):

```bash
awk '/^## Appendix/{a=1} a&&/^```$/{n++; next} a&&n==1{print}' \
  docs/superpowers/specs/2026-08-11-composite-scope-closure-index.md | grep -v '^$' | sort -u > /tmp/spec.keys
diff /tmp/code.keys /tmp/spec.keys
```

Any line in the code set that is not in the spec set is a NEW gate, and the roadmap has a new hole.
Verified clean 2026-08-18: 34 refusals in code, 34 known keys, empty diff.

The count moved 38 → 37 → 36 → 35 → 34 over one working session: 9A deleted the multi-sub-day-interval gate,
1B REPLACED the blanket DROP PARTITION gate with a much narrower cell-qualified one (a swap the count
alone would have hidden), and 1D removed the TTL and FORCE DROP gates outright, and 1C removed the
cell-qualified refusal by implementing what it refused. Five of those six changes lifted a real
restriction rather than discovering one.

The count held at 37 across sub-project 1B, but one key was REPLACED rather than removed: the blanket
"does not yet support DROP PARTITION" gave way to the far narrower "does not yet support dropping an
individual cell". Whole-day DROP PARTITION now works on a composite table; only the shape that would
destroy unnamed data still refuses. A stable count can hide a real change, which is why the swap is
recorded here.

The count went 38 -> 37 when sub-project 9A deleted the multi-sub-day-interval gate. That is the
first key this project has removed by LIFTING a restriction rather than by discovering one; every
earlier revision of this list added keys. A key leaving the list is the shape of progress the
roadmap aims at, so it is recorded here rather than silently renumbered.
