# DELETE statement — design

**Date:** 2026-07-10
**Status:** Approved design, pre-planning
**Scope:** QuestDB OSS + Enterprise
**Feature:** `DELETE FROM <table> WHERE <predicate>` — delete rows by time range or arbitrary condition, built on the existing replace-commit (`WAL_DEDUP_MODE_REPLACE_RANGE`) machinery.

> Code anchors in this document (file:line) were captured during exploration of OSS `master` @ `cdb0ea073b` and Enterprise `citi_3_3_3`. Treat them as navigational anchors to re-verify during implementation, not as guaranteed-stable line numbers.

---

## 1. Summary & motivation

QuestDB has no row-level `DELETE`. Today the only ways to remove data are `TRUNCATE TABLE` (all rows) and `ALTER TABLE ... DROP PARTITION` (whole partitions by time). Users routinely need to delete a **time range** ("drop everything older than X", "remove this bad window") or an **arbitrary condition** ("delete rows where `sensor_id = 42`").

The engine already contains the primitive to do this cleanly: a **replace commit** — a WAL transaction that atomically replaces every existing row in a timestamp range `[lo, hi)` with a new (possibly empty) set of rows. It powers materialized-view incremental refresh. An **empty** replace commit over `[lo, hi)` deletes everything in that range; a replace commit whose new rows are the *survivors* of a predicate deletes the matched rows. This design wires a SQL `DELETE` statement onto that primitive.

## 2. Goals & non-goals

### Goals (v1)
- `DELETE FROM t WHERE <predicate>` on **WAL tables**, for both pure time-range and arbitrary predicates.
- Atomic and correct under concurrent writers (serial WAL semantics).
- Works on Parquet partitions from day one (via a convert-to-native fallback; see §9).
- Full Enterprise support: a `DELETE` permission, GRANT/REVOKE/SHOW, and replication.

### Non-goals (v1)
- Non-WAL (legacy) tables — clear error.
- `DELETE` with joins / `USING` / multi-table — clear error (mirrors UPDATE's WAL restriction).
- Deleting from a materialized view — clear error (views are derived/read-only).
- Efficient in-place Parquet rewrite (Phase 2) and deletion vectors (Phase 3) — see §9.
- Exact synchronous affected-row count on WAL tables beyond what UPDATE already provides — see §12.

## 3. Background: the replace-commit primitive (what already exists)

- **Dedup mode.** `WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE = 3` (`core/.../cairo/wal/WalUtils.java:~105`). A "replace commit" is an ordinary `WalTxnType.DATA` commit (byte `0`) carrying an extra `[replaceRangeLo, replaceRangeHi)` footer + the dedup-mode byte.
- **Writer API.** `WalWriter.commitWithParams(long replaceRangeLowTs, long replaceRangeHiTs, byte dedupMode)` (`core/.../cairo/wal/WalWriter.java:~350`). `lo` inclusive, `hi` **exclusive**. Emits even with **zero** uncommitted rows (`commit0` at `:~950`) — the pure-delete case.
- **Encoding.** The range + dedup mode are an optional footer in the WAL-E event record (`WalEventWriter.appendData` `:~289-341`; read back by `WalEventCursor` `:~52-56, ~400-427`). Old-format WAL-E without the footer stays readable (defaults to `WAL_DEDUP_MODE_DEFAULT`). Per-txn cache: `WalTxnDetails` (`WAL_TXN_REPLACE_RANGE_TS_LOW/HI` `:~69-70`, getters `:~387-393`); a replace commit forces `COMMIT_TO_TIMESTAMP = FORCE_FULL_COMMIT` `:~747-750` (never buffered in WAL lag).
- **Apply.** `TableWriter.processWalCommit` (`:~10163`) dispatches to `processWalCommitDedupReplace` (`:~10907`) when `dedupMode == REPLACE_RANGE`. Inside `processO3Block` (`:~9404`) the replace loop iterates **every partition** in `[lo, hi)` (`:~9448`), even those with no replacement rows. Downstream (`o3ConsumePartitionUpdateSink` `:~8264-8560`): fully-emptied partitions are **dropped** (`removeAttachedPartitions` `:~8401-8443`); partially-covered partitions are **trimmed** (prefix + new rows + suffix); when all partitions vanish the table is **truncated** (`:~8533-8546`). Unsorted survivor rows are radix-sorted for you (`:~10973-10983`).
- **Constraints.** For a replace commit that *also* writes rows: `lo <= min(rowTs)` and `hi > max(rowTs)` and `lo < hi` (`WalEventWriter.appendData` `:~312-321`). Replace **bypasses UPSERT-key dedup** within the range (survivors written become authoritative). **Parquet partitions are rejected** by a guard in the replace path (`TableWriter.java:~9733`) — see §9.
- **Only current producer:** materialized-view refresh (`MatViewRefreshJob.insertAsSelect` `:~1000-1290` — the canonical read→transform→write→`commitWithParams(REPLACE_RANGE)` loop). Definitive behavior spec/tests: `core/src/test/.../cairo/wal/WalWriterReplaceRangeTest.java`.

## 4. SQL surface & semantics

```sql
DELETE FROM <table> WHERE <predicate>;
```

- **WHERE is mandatory.** A bare `DELETE FROM t` is rejected with a message pointing to `TRUNCATE TABLE t`. (Guards accidental full wipes; deliberate divergence from standard SQL.)
- **Single target table.** No joins, `USING`, or subquery-driven multi-table delete in v1.
- **Predicate:** any boolean expression over the table's columns — same expressiveness as a `SELECT ... WHERE`, including designated-timestamp conditions.
- **Target must be a WAL table** and a **regular table** (not a materialized view). Non-WAL table → clear error; mat view → clear error.
- **Keyword:** `delete` is recognized as a statement-initial keyword (a new `isDeleteKeyword`), and is **not** added to the reserved-word set (mirrors how `truncate` is handled), so `delete` remains usable as an identifier elsewhere.
- **Full-table delete via explicit range** is allowed: `DELETE FROM t WHERE ts >= '1970-01-01'` decomposes to whole-partition drops (cheap, Parquet-safe).

## 5. Architecture overview

**Uniform front-end, dual apply-time strategy** — the front-end is a single deferred WAL transaction (modeled exactly on UPDATE); the "hybrid" (cheap time-range vs. correct arbitrary) lives at apply time.

```
DELETE FROM t WHERE <pred>            (query thread / first compile)
  │  parse → validate (WAL, exists, not mat view) → authorize
  └─▶ WAL: SQL txn, CMD_DELETE_TABLE  (stores the DELETE text + RNG seeds + bind vars)
           │  (ApplyWal2TableJob, single-threaded per table — sees all prior txns applied)
           └─▶ OperationExecutor.executeDelete(writer, sql, seqTxn)
                 recompile predicate (isWalApplication == true), classify, then per affected partition:

                 ── time-range predicate (reduces to designated-timestamp intervals) ──
                    partition fully inside interval → removePartition()            (drop; Parquet-safe)
                    boundary partition (partial)    → replaceRange(subLo, subHi)   (empty replace = trim)

                 ── arbitrary predicate ──
                    replaceRange(partLo, partHi, survivorCursor)                    (survivors = WHERE NOT(pred))
                    zero survivors → drop · no match → no-op (identical-data short-circuit)

                 ── if a partition needing replaceRange is Parquet ──
                    convertPartitionParquetToNative(doCommit=false) first (P2 fallback, §9)
```

**Why deferred (like UPDATE).** On WAL tables, UPDATE stores its raw SQL as a `WalTxnType.SQL` transaction (`CMD_UPDATE_TABLE`) and recompiles + executes it at apply time against current state (`OperationExecutor.executeUpdate` `core/.../cairo/wal/OperationExecutor.java:~139`). This deferral is exactly what gives correct **serial semantics**: the delete's effect is computed at its sequencer position, after all prior transactions are applied, so it can never lose concurrent inserts. DELETE reuses this pattern verbatim, including the RNG-seed / bind-variable capture that makes SQL replay deterministic across the compile→apply gap and across replicas.

**Why partition-by-partition decomposition** (rather than one replace over the whole interval): (1) it keeps whole-partition deletes on the Parquet-safe `removePartition` path instead of the replace path that rejects Parquet; (2) it lets the P2 Parquet fallback act per-partition; (3) it bounds memory for the survivor scan.

## 6. Front-end wiring (OSS)

Modeled directly on UPDATE. New/extended pieces:

| Concern | Anchor | Change |
|---|---|---|
| Keyword recognizer | `griffin/SqlKeywords.java` (`isUpdateKeyword` `:~2457`) | add `isDeleteKeyword`; **do not** add to reserved `KEYWORDS` set |
| Parser dispatch | `griffin/SqlParser.java:~5561` (`parse`) | branch on `isDeleteKeyword` → `parseDelete` → `ExecutionModel.DELETE` |
| Execution model | `griffin/model/ExecutionModel.java:~35-40` | new `DELETE` constant; bump `MAX`; extend `typeNameMap` |
| Model→op | `griffin/SqlCompilerImpl.java:~3891` (`compileUsingModel` switch) | new `case ExecutionModel.DELETE` → build `DeleteOperation` |
| Validation | mirror `SqlOptimiser.optimiseUpdate` `:~12372` | table is WAL + not mat view; predicate columns exist + typecheck |
| Operation object | new `griffin/engine/ops/DeleteOperation.java` (mirror `UpdateOperation`) | `extends AbstractOperation`, `cmdType = CMD_DELETE_TABLE`; `authorize()` → `securityContext.authorizeTableDelete(token)` |
| Command constant | `tasks/TableWriterTask.java:~39` (`CMD_UPDATE_TABLE=3`) | add `CMD_DELETE_TABLE` |
| CompiledQuery type | `griffin/CompiledQuery.java:~54-78` | add `DELETE`; shift derived `EMPTY`/`TYPES_COUNT` |
| CompiledQuery impl | `griffin/CompiledQueryImpl.java` (`ofUpdate` `:~393`, `execute()` `:~150`) | add `ofDelete` + `execute()` case → dispatch via an `OperationDispatcher` |
| WAL write | `cairo/wal/WalWriter.java:~274` (`apply(UpdateOperation)`) | add `apply(DeleteOperation)` → `applyNonStructural` → `events.appendSql(CMD_DELETE_TABLE, sqlText, ctx)` (reuses RNG/bind capture) |
| WAL apply | `cairo/wal/ApplyWal2TableJob.java:~871-892` (`processWalSql`) | add `case CMD_DELETE_TABLE` → `OperationExecutor.executeDelete(...)`; then mat-view `INVALIDATE` (mirror UPDATE `:~889-892`) |
| Apply executor | `cairo/wal/OperationExecutor.java:~139` (`executeUpdate`) | add `executeDelete` (§7); recompiles under root/admin context |
| HTTP (asserted complete) | `cutlass/http/processors/JsonQueryProcessor.java:~123` (asserts `size == TYPES_COUNT+1` `:~150`) | register a `DELETE` executor — **required or the assert fails** |
| PG-wire | `cutlass/pgwire/PGPipelineEntry.java:~3504, ~3830` | add `DELETE` command tag (`DELETE n`) + row-count reporting |

**WAL vs non-WAL routing.** As with UPDATE, first compile on a WAL table discards the factory and stores SQL text; `isWalApplication()` at apply flips to the executing branch. Non-WAL tables are out of scope in v1 → error at compile.

## 7. Apply-time executor & strategies — `OperationExecutor.executeDelete(writer, sql, seqTxn)`

Runs on the `ApplyWal2TableJob` thread holding the `TableWriter` (same context as `executeUpdate`). Steps:

1. **Recompile** the DELETE predicate (now `isWalApplication()==true`) into a filter over the table.
2. **Classify** the predicate:
   - **Time-range**: reduces entirely to one or more **precise** `[lo, hi)` intervals on the designated timestamp — use `SELECT`'s WHERE→interval extraction (`IntervalModel` / where-clause interval parser), because boundary **trims** need exact sub-partition bounds. (`ALTER ... DROP PARTITION WHERE`'s `filterApply`/`filterPartitions` `SqlCompilerImpl.java:~4800-4848` evaluates the predicate at *whole-partition* granularity only — cited as precedent for the fully-covered decision, but it is insufficient for boundary trims, so it is not the classification mechanism.)
   - **Arbitrary**: everything else. If it *also* carries timestamp bounds, use them to **prune** the set of candidate partitions.
3. **Iterate affected partitions**, choosing per partition:
   - **Time-range, fully covered** → `removePartition(partitionTimestamp)` (`TableWriter:~3016` → `dropPartitionByExactTimestamp:~6618`). O(1), **Parquet-safe** (no guard).
   - **Time-range, boundary** → `replaceRange(subLo, subHi)` with **no rows** (empty replace = trim; keeps prefix+suffix).
   - **Arbitrary** → `replaceRange(partLo, partHi, survivorCursor)` where `survivorCursor = SELECT * FROM t WHERE NOT(<pred>)` bounded to `[partLo, partHi)`. Zero survivors → the partition drops; all survivors (nothing matched) → the existing identical-data short-circuit (`O3PartitionJob.checkReplaceCommitIdenticalToPartition` `:~1379-1395`) makes it a cheap no-op.
   - **Parquet partition needing `replaceRange`** → run the **P2 convert-fallback** first (§9).
4. **Accumulate** deleted-row count (see §12) and, after the loop, trigger mat-view invalidation for the affected range.

The whole executor runs inside the single atomic apply of the one `SQL` transaction → atomic and race-free.

## 8. Core new primitive — cursor-sourced / range-only replace

The executor needs `TableWriter.replaceRange(lo, hi, rowSource?)`, callable at apply time, which exposes the existing `WAL_DEDUP_MODE_REPLACE_RANGE` apply logic (`processWalCommitDedupReplace` → `processO3Block` replace mode) as an operation where the row source is:
- **empty** → pure range delete / boundary trim (the `rowLo == rowHi` branch already exists, `TableWriter:~10998-11006`), or
- a **survivor `RecordCursor`** (+ `RecordToRowCopier`) → replace `[lo, hi)` with those rows.

Everything downstream is **reused**: partition drop/trim/split, symbol maps, index rebuild, first/last min/max recompute, truncate-when-empty, radix-sort of unsorted rows.

This is the **main net-new, highest-risk** engine work. Two implementation options were weighed:

- **(a) Direct exposure** — drive `processWalCommitDedupReplace`'s apply flow (its O3 staging / `processWalCommitFinishApply` → `processO3Block` replace mode) straight on the writer.
- **(b) Scratch WAL segment** — stage survivors to a throwaway segment, then feed it through the existing replace-apply unchanged.

**SPIKE DECISION (task 1.8) — (a) direct exposure.** Implemented for the **empty-range path** (`survivorCursor == null`) in `TableWriter.replaceRange` (`core/.../cairo/TableWriter.java`, next to `removePartition`). The empty path needs *no* row source, so (b)'s scratch segment would be pure overhead — nothing to stage. `replaceRange` reproduces `processWalCommitDedupReplace`'s `rowLo >= rowHi` (empty) branch directly: `commit()` up front to flush pending rows; set `dedupMode = WAL_DEDUP_MODE_REPLACE_RANGE`; carry `[lo, hiExcl)` in the tx lag min/max (`setLagMinTimestamp(lo)` / `setLagMaxTimestamp(hiExcl - 1)`); `processWalCommitFinishApply(0,0,0,0, TableWriterPressureControl.EMPTY, true, partitionTimestampHi)` over an empty O3 batch; `finishO3Append(0)` + reset `dedupMode` in `finally`; then persist with `commit00()` + `housekeep()` (the latter reclaims dropped-partition dirs via `processPartitionRemoveCandidates`) — mirroring the WAL apply's own post-commit sequence. Zero new O3 surgery; the whole partition drop/trim/split, symbol-map, index-rebuild, first/last-recompute and truncate-when-empty machinery is reused verbatim. Deleted count is taken as `rowCountBefore − rowCountAfter`. Verified by `core/.../test/cairo/TableWriterReplaceRangeDirectTest` (mid-partition trim, whole-partition drop, boundary-spanning trim; all green, exact counts asserted). **The cursor path (survivor rows) is deferred to task 1.9**, where the (a) vs (b) question genuinely bites — it re-raises the "reading the partition being overwritten" risk below; task 1.8 leaves the cursor branch as an explicit `UnsupportedOperationException`.

**SPIKE DECISION (task 1.9) — (a) direct exposure, extended to the cursor path.** Implemented the `survivorCursor != null` branch in the same `TableWriter.replaceRange`. Survivor rows are produced through TableWriter's **ordinary O3 row API** — `newRow(record.getTimestamp(timestampCursorIndex))` / `copier.copy(null, record, row)` / `row.append()` per survivor, mirroring `MatViewRefreshJob.insertAsSelect`'s REPLACE_RANGE producer — filling **O3 memory**, then the empty-path's *direct drive* of `processWalCommitFinishApply` is reused over a **non-empty, sorted** O3 batch. This is the TableWriter analogue of `processWalCommitDedupReplace`'s `rowLo < rowHi` branch (whose row source is a mmap'd WAL segment; ours is the O3 memory we just filled). Option (b) (staging survivors into a throwaway WAL segment) was rejected — it would re-serialize the survivors through a segment only for the apply to re-read them, pure overhead over filling O3 memory directly. Three details proved load-bearing during the spike:
- **Range is `[lo, hiExcl)`, never the survivors' own min/max.** The ordinary `o3Commit()` passes the *data's* min/max to `processO3Block`, which in replace mode would delete only `[survivorMin, survivorMax]` and leave to-be-deleted rows between a range bound and the nearest survivor alive. We therefore **must not** route through `o3Commit()`; we drive `processWalCommitFinishApply` directly with the true range carried in the tx lag min/max (as the empty path does), feeding survivors as the sorted batch.
- **Force O3 staging before the append loop** (`o3OpenColumns()`; `o3MasterRef = masterRef + 1`; `rowAction = ROW_ACTION_O3`) so *every* survivor lands in O3 memory uniformly — a survivor at/after `maxTimestamp` would otherwise append in order and be dropped by the O3 sort. The `+1` matches `newRowO3`'s post-first-bump `o3MasterRef` convention; an off-by-one here shifts every survivor's payload one row relative to its timestamp (caught and fixed under TDD).
- **Sort is mandatory and handles unordered survivors.** The O3 timestamp column is a 128-bit `(timestamp, row-index)` merge array (`o3TimestampSetter`); we radix/quick-sort it and `dispatchColumnTasks(cthO3SortColumnRef)` + `swapO3ColumnsExcept` to reshuffle the data columns, exactly as `o3Commit`. The caller need not pre-sort; `select *` on a designated-timestamp table happens to arrive ordered, but the primitive does not rely on it.

An empty cursor (no survivors) falls through to the empty-range apply. Deleted count stays `rowCountBefore − rowCountAfter`. Verified by `TableWriterReplaceRangeDirectTest.testReplaceRangeSurvivorsRewritesPartition` (300 rows, delete even-`x`, symbol column included; asserts count = 150 and full-table equality vs a NOT-predicate reference). All 6 direct tests + 58 `WalWriterReplaceRangeTest` tests green.

**Risk — reading the partition being overwritten.** The survivor cursor reads a partition's column files that the same `replaceRange` then rewrites. Survivors must be fully staged (copied out) before the partition surgery overwrites the source. Precedent: `UpdateOperatorImpl.executeUpdate` reads a cursor over the same table it mutates while holding the writer (`griffin/UpdateOperatorImpl.java:~104-240`). The O3 path already copies rows into separate staging buffers; the spike must confirm staging completes before the source files are replaced. **Resolved (task 1.9):** the cursor is fully drained into O3 memory *before* `processWalCommitFinishApply` runs any partition surgery — all reads precede all writes — and the direct test (cursor over `src`, `replaceRange` rewriting `src`) exercises exactly this same-table read-then-overwrite and passes.

## 9. Parquet handling

> **Design revision during execution (2026-07-11).** §5's "partition-by-partition decomposition" and §9's original "fully-covered → `removePartition()`" routing are **superseded for the fast path** by a single-commit design, because a read-only investigation of the apply path proved `removePartition` (and `replaceRange`) each **self-commit and persist the sequencer txn** (`TxWriter.commit` → `putLong(TX_OFFSET_SEQ_TXN_64)`), which directly violates the crash-safety requirement §14.6 already stated ("holds only if `executeDelete` does not force intermediate commits… verify `removePartition` does not self-commit mid-apply" — it does). Revised approach: a pure single-interval time-range delete is applied as **one empty `replaceRange` over the deleted interval** (drops covered partitions + trims the boundary in one commit — Task 1.8's empty path); fully-covered **Parquet** partitions are dropped by **refining the replace-path guard at `~9924` to drop inline** (reusing `removeAttachedPartitions`/`columnVersionWriter.removePartition`/`partitionRemoveCandidates.add`, skipping the Parquet-unsupported async rewrite) rather than routing to a separate `removePartition` commit. This keeps the one-atomic-commit-per-WAL-txn invariant that UPDATE relies on. Boundary trims + arbitrary rewrites of Parquet still need the P2 convert-fallback below (Phase 3 / Task 3.1). See the plan's Phase 2 (Tasks 2.1 + 2.2) and the ledger's Phase 2 design-decision note. Guard line has shifted from the originally-cited `~9733` to `~9924` on the current branch.

Grounding facts (verified during exploration):
- **Partition format** is one bit in `_txn` (`TxReader.PARTITION_MASK_PARQUET_FORMAT_BIT_OFFSET=61`; `isPartitionParquet*`). `PartitionFormat.{NATIVE=0, PARQUET=1}`.
- **Whole-partition drop already works on Parquet** — `dropPartitionByExactTimestamp` has **no** Parquet guard.
- **The replace guard is narrow and overbroad.** `TableWriter.java:~9733` throws for *any* Parquet partition in a replace commit — even a fully-covered one that only needs a drop — because it fires during O3 dispatch, *upstream* of the drop decision at `:~8401`. The general O3/insert path, by contrast, **already rewrites Parquet at row-group granularity** (`PartitionUpdater.copyRowGroup`/`updateRowGroup`, `O3ParquetMergeContext`).
- **All primitives exist:** Parquet↔native conversion **both** directions (`convertPartitionNativeToParquet:~1630`, `convertPartitionParquetToNative:~1761`, decode via `produceNativeFromParquet:~11075`, batched `doCommit=false` + `commitPendingParquetToNativeConversions:~1483`); **positional filtered decode** (`decode_row_group_filtered` / `ParquetPartitionDecoder.decodeRowGroupWithRowFilter`); **row-group pruning** (`ParquetRowGroupFilter.canSkipRowGroup`); verbatim row-group copy (`writeStreamingParquetChunkFromRowGroup`).
- **Object-store tiering is currently a no-op.** `NoOpObjectStoreParquetDispatcher` is the only impl; `TO REMOTE` is parsed but not enforced; reads always mmap a **local** file. **Today every Parquet partition is local**, so a rewrite is a cheap local operation.

### v1 — P2 convert-fallback (ships Parquet support day one)
Because whole-partition drops are already Parquet-safe, only **boundary trims and arbitrary-condition rewrites** touching Parquet need handling. In v1 the executor, before a `replaceRange` on a Parquet partition, converts it to native in the same atomic apply:

```
partition is Parquet AND needs replaceRange
  → convertPartitionParquetToNative(partitionTimestamp, doCommit=false)   // queue
  → commitPendingParquetToNativeConversions()                            // flush (native now)
  → replaceRange(...)                                                     // v1 native path
  → (Enterprise storage policy re-tiers to Parquet on its next pass)
```

Trade-off accepted: this **un-tiers and rewrites the whole partition** to delete even one row, and the partition stays native until the storage policy (Enterprise) re-converts it (in OSS it stays native permanently, as expected — OSS Parquet conversion is manual). Correct and simple; inefficient. This is the same convert-then-mutate pattern `ConvertOperatorImpl` already uses before a column-type change.

### Phase 2 — P1 in-place row-group rewrite (replaces the fallback)
Teach the replace path Parquet natively, reusing the row-group machinery O3/insert already uses:
- Fix the `:~9733` guard so a fully-covered Parquet partition routes to `removePartition` instead of throwing.
- Fully-deleted row groups → **prune** (metadata only, via min/max + the existing skip logic).
- Boundary/partial row groups → **decode survivors** (`decode_row_group_filtered`) and **re-encode**; **copy untouched groups verbatim** (`copyRowGroup`).
- Removes the un-tiering cost; stays Parquet.

### Phase 3 — P3 read-time deletion vectors (gated on `TO REMOTE` shipping)
When partitions actually live on object storage, wholesale re-upload per delete becomes prohibitive. The lakehouse answer (Iceberg-v2 / Delta deletion vectors):
- Write a small per-partition **positional deletion bitmap** at delete time; apply it at the existing scan choke points (`decodeRowGroupWithRowFilter` / `PageFrameFwdRowCursor.next()` — the cursor exposes `frame.getPartitionLo() + relativeRow` absolute positions); **compact lazily** (or when a row group is fully deleted).
- O(#deleted-rows) write cost; never rewrites the Parquet object; composes with the positional decode + pruning that already exist. A genuine competitive differentiator for cold/remote deletes.
- Large feature: net-new on-disk vector format, scan integration on every Parquet read, compaction/GC, Enterprise vector replication, mat-view/dedup/stats interactions. Out of scope until remote tiering is real.

## 10. Materialized views & dedup

- **Mat-view invalidation.** A DELETE on a base table must invalidate/refresh dependent incremental materialized views over the affected range. Reuse UPDATE's hook: `ApplyWal2TableJob` sets `MatViewRefreshTask.INVALIDATE` after `executeUpdate` (`:~889-892`); DELETE does the same after `executeDelete`. (OSS.)
- **Dedup tables.** Survivors are read from already-deduplicated state and written back; replace bypasses UPSERT-key dedup within the range, so writing survivors is authoritative and correct (no duplicate collapse needed). Covered by a test.

## 11. Enterprise

Enterprise overlays OSS by subclassing + `SecurityContext` hooks (OSS is a submodule; `questdb-ent` depends on the OSS jar). Work required:

- **Permission.** Add `Permission.DELETE` in `questdb-ent/.../security/Permission.java` at the next free exponent (above the frozen legacy-62 boundary); register in `namePermissionMap` / `permissionNameMap`; add to the aggregate masks `ALL_TABLE` and `ALL`. It is **table-scoped** (whole-row removal) → belongs with `INSERT`/`TRUNCATE_TABLE`, **not** `ALL_COLUMN`. GRANT/REVOKE/SHOW PERMISSIONS DELETE then work **for free** (the permission parser + models are data-driven off these maps).
- **Authorization.** Add `authorizeTableDelete(TableToken)` to the OSS `SecurityContext` interface (`core/.../cairo/SecurityContext.java`, alongside `authorizeTableTruncate` `:~171` / `authorizeTableUpdate` `:~173`). Implement in:
  - Enterprise `EntSecurityContextBase` → `checkNotProtectedTable(...)` + `checkPermission(DELETE, tableToken)` (mirror `authorizeTableTruncate`).
  - Enterprise `DispatchingSecurityContext` (delegate), `AbstractReplicaSecurityContext` (read-only throw).
  - OSS `AllowAllSecurityContext`, `ReadOnlySecurityContext`.
  - Call it from `DeleteOperation.authorize()`.
- **Replication — free.** DELETE is a `WalTxnType.SQL` transaction re-executed at apply, exactly like UPDATE. **No new WAL txn-type byte** is introduced (the Rust `WalTxnType` enum in `qdb-ent/.../wal/event.rs`, which accepts bytes `0..=5`, is untouched); the replication layer treats WAL segments as opaque bytes. Replica apply runs under the root/`AdminSecurityContext`, so per-user permissions don't block replicated deletes.
- **Determinism note.** SQL replay (compile→apply and primary→replica) relies on the captured RNG seeds + bind variables in the WAL-E SQL record. Predicates using non-deterministic functions (e.g. `now()`) must be pinned at statement time and captured — **verify parity with UPDATE's handling during planning**.

## 12. Concurrency, correctness & affected-row count

- **Serial semantics.** Deferral to apply time makes each DELETE effective at its sequencer position, after all prior transactions are applied. An arbitrary DELETE sequenced at txn *N* computes survivors from state through *N−1*; inserts sequenced after *N* survive. No lost updates. Explicit test: interleave an arbitrary DELETE with an INSERT into the affected range and assert serial ordering.
- **Skip-scan interaction.** `ApplyWal2TableJob`'s replace-range skip optimization keys off footer-carrying replace DATA commits. DELETE performs its removals **inline** during the SQL-txn apply (no footer-carrying DATA commit), so it does not participate in that optimization — correct, just unoptimized (prior inserts into the deleted range are applied, then removed). No correctness impact; note for reviewers.
- **Affected-row count.** DELETE should report rows removed (`DELETE n` for PG-wire). Counts are computable at apply: dropped partitions contribute their row counts; trims/rewrites contribute `oldCount − survivorCount`. **However**, on WAL tables `WalWriter.apply(op)` returns the sequencer txn, not a row count (the effect happens later, at apply). v1 therefore mirrors UPDATE's existing WAL count convention; an exact synchronous count on WAL is a known limitation to confirm against UPDATE during planning, not to solve here.

## 13. Testing strategy

House style: fluent `assertQuery()` / `QueryAssertion`, not raw `printSql` + `TestUtils.assertEquals`.

**OSS (`DeleteTest`, plus targeted `WalWriterReplaceRangeTest`-style unit tests for `replaceRange`):**
- Time-range: full-partition drop; boundary trim; multi-interval; delete-everything → empty table.
- Arbitrary: single/multi column predicate; predicate with timestamp bound (partition pruning); no-match → no-op; predicate matching all → table empties.
- Errors: no WHERE; non-WAL table; target is a mat view.
- Storage: dedup table; O3 / unordered survivors; symbol columns; indexed columns (index rebuild); split partitions.
- Parquet: full-partition drop on Parquet (no convert); boundary/arbitrary rewrite on Parquet → P2 convert-fallback produces correct native result.
- Mat views: DELETE on a base table invalidates/refreshes a dependent incremental view.
- Count: reported deleted count matches expectations where the convention allows.

**Enterprise:**
- GRANT/REVOKE/SHOW PERMISSIONS DELETE; authorize failure without the grant; column-security irrelevance (table-scoped).
- Replica read-only: a direct DELETE against a replica is rejected; a DELETE replicated from the primary applies and converges (primary/replica row-for-row equality).

**Concurrency:** arbitrary DELETE interleaved with concurrent INSERT into the affected range preserves serial order.

## 14. Known limitations & risks

1. **`replaceRange` (cursor-sourced replace) is the risk center** — O3/symbol/index correctness; must fully stage a partition's survivors before overwriting the source. Mitigate with a TDD spike choosing option (a) or (b) in §8 before broad implementation.
2. **P2 Parquet fallback is heavy** — un-tiers and rewrites a whole partition per delete. Acceptable for v1; Phase 2 (P1) removes it.
3. **Affected-row count on WAL** — only known at apply; v1 mirrors UPDATE (§12).
4. **Arbitrary predicate with no timestamp bound** rewrites every partition containing a match (full-table survivor scan) — correct but potentially heavy; document and consider per-partition match-existence short-circuiting.
5. **Determinism of SQL replay** for non-deterministic predicates — inherits UPDATE's machinery; verify (§11).
6. **Intra-apply atomicity / crash-safety.** A single `executeDelete` may mix `removePartition` (drops), `convertPartitionParquetToNative` (P2 fallback), and `replaceRange` (trims/rewrites) across several partitions. Crash-safety relies on the WAL framework's guarantee that the `SQL` txn is not marked applied until `executeDelete` returns, so a crash re-applies the whole DELETE from the pre-delete state (idempotent — survivors recompute identically). This holds **only if `executeDelete` does not force intermediate commits** that persist partial state. Planning must confirm the executor performs a single writer commit at txn end (use the batched `doCommit=false` conversion path; verify `removePartition` does not self-commit mid-apply).
   > **Resolved during execution (2026-07-11).** Investigation confirmed each table commit **does** persist seqTxn (`TxWriter.commit` → `TX_OFFSET_SEQ_TXN_64`), so a multi-commit apply that crashes mid-way marks a partial delete permanently applied — the hazard is real, not hypothetical. Two consequences: (i) Phase 2 keeps the *delete itself* to a **single** commit (one `replaceRange`; per-partition `removePartition` loops are rejected — see §9 revision note); (ii) the Parquet convert-fallback (Task 3.1) is inherently **two physical commits** (convert self-commits: its `bumpPartitionTableVersion` trips `replaceRange`'s front commit, and its housekeeping only flushes via the self-committing `commitPendingParquetToNativeConversions`), so it achieves crash-safety by a **single seqTxn advance**: the convert pre-pass commits at the prior `S-1` (run it *before* `setSeqTxn(S)`), and only `replaceRange` advances to `S`. A crash between the two re-applies `S`; the re-issued convert on a now-native partition is an idempotent no-op. Persisting `S` at the convert commit would silently lose the DELETE on crash — forbidden.
7. **SYNC commit-mode power-loss residual in the Parquet convert-fallback (v1 residual).** The convert pre-pass's commit&nbsp;#1 (`commitPendingParquetToNativeConversions` → `commitTxWriter` → `txWriter.commit`) persists `_txn@S-1` referencing the newly-written **native** partition dirs and deletes the source Parquet (`safeDeletePartitionDir`), but issues **no data fsync** — the reconstructed native column files were closed without fsync (see the `TableWriter.commitPendingParquetToNativeConversions` javadoc). Under `CAIRO_COMMIT_MODE=sync` a power-loss / kernel panic can therefore leave `_txn@S-1` durably pointing at torn / zero-length native files whose Parquet source is already gone, corrupting that partition on re-open. Scope:
   - **Moot under the default `NOSYNC`** (no power-loss durability is promised there) and **under a process crash / `kill -9`** (the OS page cache keeps the un-fsync'd native readable). It bites **only** under SYNC commit mode + a genuine power-loss / OS crash.
   - **Wider than the commit#1→commit#2 window.** For route-a (single time-range) deletes every converted partition is a boundary the replace then rewrites (and fsyncs), so the exposure is bounded by that inter-commit window. For **route-b (arbitrary predicate)** deletes the pre-pass converts *every* Parquet partition, but the whole-range replace skips physically rewriting a partition whose survivors equal its existing rows (`O3PartitionJob`'s identical-partition short-circuit), and `syncColumns0` fsyncs only the *active* partition — so a **converted-but-no-match** partition's un-fsync'd native output is never superseded, and its torn-native exposure extends **past commit&nbsp;#2** until the OS naturally flushes, not just the inter-commit window.
   - **Shared with `ALTER … CONVERT PARTITION`** (the same `commitPendingParquetToNativeConversions` primitive) — not DELETE-introduced.
   - **Deferred fix:** fsync the reconstructed native partition data inside `commitPendingParquetToNativeConversions` when `commitMode != NOSYNC` (a `syncColumns`-equivalent over the just-converted partitions), so `_txn@S-1` never publishes un-fsync'd native under SYNC. This also closes the same latent gap for the ALTER caller.

## 15. Phasing & scope boundaries

- **v1** (this spec): WAL-only `DELETE FROM t WHERE ...`, uniform deferred SQL txn, dual apply-time strategy, P2 Parquet fallback, full Enterprise (permission + replication). Deliverable across OSS + `questdb-ent`.
- **Phase 2 (P1):** efficient in-place Parquet row-group rewrite; retires the P2 fallback.
- **Phase 3 (P3):** read-time deletion vectors; gated on remote object-store tiering (`TO REMOTE`) shipping.

## 16. Key code references (anchor map)

Replace primitive: `WalUtils.java` (dedup modes), `WalWriter.java:~350` (`commitWithParams`), `WalEventWriter.java:~289-341` / `WalEventCursor.java:~52-56` (footer), `WalTxnDetails.java:~69-70,~387-393`, `TableWriter.java:~10163,~10907,~9404,~8264-8560` (apply), `MatViewRefreshJob.java:~1000-1290` (template), `WalWriterReplaceRangeTest.java` (spec/tests).
UPDATE template: `SqlParser.java:~5561`, `SqlCompilerImpl.java:~3891,~4884`, `UpdateOperation.java`, `CompiledQueryImpl.java:~150,~393`, `WalWriter.java:~274,~806`, `ApplyWal2TableJob.java:~871-892`, `OperationExecutor.java:~139`, `UpdateOperatorImpl.java:~104-240`.
Front-end hooks: `SqlKeywords.java:~2457`, `ExecutionModel.java:~35-40`, `CompiledQuery.java:~54-78`, `TableWriterTask.java:~39`, `JsonQueryProcessor.java:~123-150`, `PGPipelineEntry.java:~3504,~3830`.
Time-range/partition: `SqlCompilerImpl.java:~1407` (DROP PARTITION), `filterPartitions:~4814` / `filterApply:~4800`, `TableWriter.java:~3016,~6618` (`removePartition`).
Parquet: `PartitionFormat.java`, `TxReader.java:~54-55`, `TableWriter.java:~1630,~1739,~1761,~11075,~1483,~9733,~8401`, `PartitionEncoder.java`, `PartitionUpdater.java`, `ParquetPartitionDecoder.java`, `ParquetRowGroupFilter.java`, `PageFrameMemoryPool.java:~2144-2188`, `qdbr/.../parquet_read/row_groups.rs` (`decode_row_group_filtered`).
Enterprise: `questdb-ent/.../security/Permission.java`, `EntSecurityContextBase.java`, `DispatchingSecurityContext.java`, `AbstractReplicaSecurityContext.java`; `core/.../cairo/SecurityContext.java`; `qdb-ent/.../wal/event.rs` (Rust `WalTxnType`); `questdb-ent/.../cairo/cold/storage/` (storage policy, `NoOpObjectStoreParquetDispatcher`).

## 17. Open questions for the planning phase

1. `replaceRange` implementation: option (a) refactor vs (b) scratch-segment — resolve with a TDD spike first.
2. Exact affected-row-count convention on WAL — confirm what UPDATE reports and match it.
3. Non-deterministic predicate handling — confirm parity with UPDATE's RNG/bind/`now()` capture.
4. Whether the boundary-trim empty-replace and the arbitrary survivor-replace should share one `replaceRange` entry point or split into `deleteRange(lo,hi)` + `replaceRange(lo,hi,cursor)` for clarity.
5. **Full-table / active-partition emptying.** A range delete covering everything must remove *all* partitions including the active/last one. Confirm the `removePartition` path can empty the table (the replace path has an explicit truncate-when-all-removed branch; `removePartition` may special-case the active partition), or route the all-covering case to the truncate path. Add a dedicated test either way.
6. Confirm `executeDelete` runs as a single writer commit at txn end (see §14.6).
