# Composite Partitioning — Enterprise (Sub-project 6) — Design

**Status:** drafted 2026-08-11, awaiting review. Sub-project 6 of 8.

## 1. Scope

Everything composite-related on the Enterprise side of the fence:

- **Cold storage / tiering** — `TO PARQUET`, `TO REMOTE`, `DROP LOCAL`, `DROP REMOTE` storage
  policies (`StoragePolicyWriterCommand`, `StoragePolicyJob`)
- **Partition seal** — the `PARTITION_SEAL` WAL event (`EntWalTxnTypeHandler`)
- **Checkpoint / backup manifest** — `CheckpointManifest`
- **Replication** — currently *unverified*, neither supported nor refused

The first three are gated as of 2026-08-10; this sub-project makes them work. The fourth is the one
with no gate and no evidence, which makes it the highest-risk item here.

## 2. Starting position

The merge audit established the ent-side facts, and one of them was a real bug rather than a gap:

- `EntCreateTableOperationImpl` did not delegate `getPartitionSpec()`. Because
  `TableStructure.getPartitionSpec()` defaults to `PartitionSpec.EMPTY`, **every composite
  `CREATE TABLE` issued through Enterprise silently produced a plain table**, with no error at any
  layer. Fixed 2026-08-10; the regression test is the inherited OSS composite SHOW CREATE suite.
- Cold storage computed `_txn` raw indexes as `idx * LONGS_PER_TX_ATTACHED_PARTITION` — hardcoded
  stride 4 against a composite table's stride 8, addressing the **wrong partition record**.
- Partition seal resolved partitions with the cellKey-blind `getPartitionIndexByTimestamp`.
- The checkpoint manifest emitted one entry per partition record, named by timestamp alone, which
  collides sibling cells onto one day name.

Ent's cell-blind surface is small and now known: 2 stride sites, 12 day-path constructions, 7
timestamp-only partition lookups, confined to cold storage, backup and the WAL seal handler. That
measurement must be **re-derived** at implementation time — measuring a month-stale ent main during
the audit reported zero stride sites when current main had two.

## 3. Decisions

**D1 — Tiering is per cell.** A storage policy applies to cells, consistent with sub-project 1's
rule that a predicate selects cells and with sub-project 3's per-cell parquet form. A cell is the
unit that moves to remote storage, is dropped locally, or is converted. Mixed placement within a day
(some cells local, some remote) is permitted, exactly as mixed parquet/native is.

This falls out of the storage model rather than being a new concept: each cell is already its own
`_txn` record carrying its own format bit, its own slot-3 word and — on the ent side — its own
remote/uploaded state.

**D2 — Backup and checkpoint are cell-complete.** The manifest emits one entry per `(day, cell)`,
using the rendered cell-qualified name (`2024-01-01/exchange=BTC`), so a restore can reconstruct the
directory tree exactly. Restoring must also rebuild the composite interners — the dedicated
dictionaries and `_cell` registry `.k/.v/.o` files — which OSS
`TableSnapshotRestore.rebuildCompositeInternerFiles` already does (Plan 4d); the ent path must call
the equivalent rather than reimplement it.

**D3 — Replication is verified, not assumed.** Replication ships WAL segments, and a replica applies
them through the same `TableWriter` routing the primary uses, so composite *should* work end to end
with no ent-side change. That is a hypothesis, and this sub-project's job is to prove or refute it
with a running two-node test, not to reason about it. If it holds, the deliverable is the test and a
documented statement; if it fails, the fix lands here.

**D4 — Ent gates come down only per verified feature.** The three gates added on 2026-08-10 are
removed one at a time, each when its feature is cell-correct and covered, not as a batch.

## 4. Semantics

| Feature | Composite behaviour |
|---|---|
| `TO PARQUET` policy | converts cells (sub-project 3 form) |
| `TO REMOTE` policy | uploads cells; a day may be partly remote |
| `DROP LOCAL` / `DROP REMOTE` | per cell |
| `PARTITION_SEAL` WAL event | seals the addressed cell, resolved by `(ts, cellKey)` |
| checkpoint manifest | one entry per cell, cell-qualified name |
| restore | rebuilds cell dirs **and** composite interners |
| replication | expected transparent; proven by test (D3) |

## 5. Implementation surfaces

| File | Change |
|---|---|
| `cairo/cold/storage/StoragePolicyWriterCommand.java` | `(ts, cellKey)` addressing; dynamic stride; drop the audit gate |
| `cairo/cold/storage/StoragePolicyJob.java` | cell-aware candidate selection and path construction (6 `setPathForNativePartition` sites) |
| `cairo/wal/EntWalTxnTypeHandler.java` | cell-aware seal; drop the audit gate |
| `cairo/backup/CheckpointManifest.java` | per-cell entries; drop the audit gate |
| ent restore path | call the OSS interner rebuild |
| `griffin/ops/EntCreateTableOperationImpl.java` | already fixed; keep the regression test |

The ent-reachable composite detector is
`writer.getTxWriter().getLongsPerAttachedPartition() > TableUtils.LONGS_PER_TX_ATTACHED_PARTITION`,
mirroring OSS `O3PartitionPurgeJob`. `isRoutedComposite()` is private to OSS `TableWriter` and is not
available across the module boundary; if ent needs a richer predicate, OSS should expose one
deliberately rather than ent duplicating the logic.

## 6. Testing

- **Re-derive the hazard surface first** (§2) — do not trust the numbers in this document.
- **Ent composite differential**: the sub-project 8 harness run under an ent context, so cold storage
  and seal participate in twin comparison.
- **Tiering round-trip per cell**: local → remote → dropped local → read (served remotely) → restored,
  differential against the plain twin at each step, including a day with mixed placement.
- **Backup/restore round-trip** of a multi-cell table across instances, asserting the restored table
  is byte-equal in `_txn` shape and that interners were rebuilt — with symbol ordinals deliberately
  differing at the destination, which proves values, not ordinals, drive reconstruction.
- **Replication (D3)**: two-node test, composite table on the primary, asserting the replica's data
  and cell layout match. This is the experiment that decides whether replication needs work at all.
- **Enterprise CTAS and DDL** produce genuinely composite tables — the `getPartitionSpec()`
  regression.
- **Plain byte-identity** on the ent side for all of the above.

## 7. Out of scope

- Per-dimension storage policies (keep BTC local, tier everything else) — a feature, and the natural
  sibling of the per-dimension TTL deferred in sub-project 1.
- The single sorted parquet per day (sub-project 3 §7), which would change what "tier a cell" means.
- OSS-side gates, which their own sub-projects own.
