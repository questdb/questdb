# Composite Partitioning — Scope Closure Index

**Purpose:** the audit artifact for "every gate is owned by exactly one spec". If a gate exists in
the code and is not in this table, the roadmap has a hole.

Generated and verified 2026-08-11 against the gate messages in `TableWriter` and the create-time
validation sites.

## Build order

Sub-project 8 is built **first** — it is the differential test harness every other sub-project is
graded against. Sub-project 1 is next, because its addressing decision propagates into 3, 6 and 7.

```
8 (verify)  →  1 (lifecycle)  →  2 (column DDL)  →  4 (row-level)  →  5 (create-time)
                    ↓                                                        ↓
                    └──────────→  3 (parquet)  ─────→  6 (Enterprise)  ←─────┘
                                       ↓
                                  7 (mat views)      [gate lands in 8, removed in 7]
```

3 depends on 1 (a parquet cell must still be addressable). 6 depends on 3 (tiering moves parquet
cells) and on 5 (Enterprise CTAS). 7 depends on 1 (refresh after partition removal).

## Gate ownership

| # | Gate / restriction | Owner spec |
|---|---|---|
| 1 | `DROP PARTITION` | 1 — partition-lifecycle |
| 2 | `FORCE DROP PARTITION` | 1 — partition-lifecycle |
| 3 | `DETACH PARTITION` | 1 — partition-lifecycle |
| 4 | `ATTACH PARTITION` | 1 — partition-lifecycle |
| 5 | `SQUASH PARTITIONS` (split-fragment squash; today a silent skip) | 1 — partition-lifecycle |
| 6 | TTL-based partition eviction | 1 — partition-lifecycle |
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
| 21 | `UPDATE` | 4 — row-level-ops |
| 22 | DEDUP (`CREATE` and `ALTER … DEDUP ENABLE`) | 4 — row-level-ops |
| 23 | REPLACE-range commit mode | 4 — row-level-ops |
| 24 | `CREATE TABLE AS SELECT` | 5 — create-time |
| 25 | Non-WAL composite (`requires a WAL table`) | 5 — create-time (**permanent**, by design) |
| 26 | Enterprise cold storage / tiering | 6 — enterprise |
| 27 | Enterprise `PARTITION_SEAL` WAL event | 6 — enterprise |
| 28 | Enterprise checkpoint / backup manifest | 6 — enterprise |
| 29 | Enterprise replication (no gate — **unverified**) | 6 — enterprise |
| 30 | Materialized views over a composite base (no gate today; 8 adds one) | 7 — matview |

Silent skips, which are not gates but must become real behaviour or provably-harmless skips:

| Skip | Owner spec |
|---|---|
| split-fragment squash ("cell-blind merge, cell-aware squash deferred") | 1 — lifecycle (behaviour), 8 (proof it is harmless meanwhile) |
| symbol-capacity autoscale ("cell-blind reopen") | 8 — proof it is harmless; no behaviour change planned |

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
   `SUPPORTED` in sub-project 8's classification table and pass the twin comparison.

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
