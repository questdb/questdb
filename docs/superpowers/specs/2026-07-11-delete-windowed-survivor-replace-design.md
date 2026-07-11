# DELETE windowed survivor-replace — design

**Goal:** Bound the memory (and transient disk) of an arbitrary-predicate `DELETE` so it cannot OOM the database on a large table, by rewriting the survivor set in adaptive time-windows instead of one whole-range pass — closing lvl3-production findings **C1** (unbounded native-memory materialization → JVM OOM) and **H1** (whole-range Parquet un-tier + 2–3× transient disk).

**Architecture:** Reuse the materialized-view refresh model — walk the delete range in bounded ~N-row windows, applying a survivor-`replaceRange` per window — but keep DELETE's single-seqTxn crash-safety by deferring the transaction commit to a single terminal `commit00()` after the last window, relying on the idempotence of survivor-replace for crash re-apply.

**Tech stack:** QuestDB core (Java), the existing `TableWriter.replaceRange` + `OperationExecutor.executeDelete` WAL-apply path, `MatViewRefreshJob.estimateBucketsForRows` (already `public static`).

## Global Constraints

- **Single seqTxn advance per WAL txn** — the whole DELETE persists seqTxn exactly once (in the terminal commit). Non-negotiable crash-safety invariant, unchanged from the shipped DELETE.
- **Correctness identical to whole-range delete** — the surviving row set is exactly `NOT(pred)`, byte-for-byte the same as the current whole-range implementation, regardless of window count.
- **Time-range (empty-replace) path unchanged** — it is already O(deleted) with no materialization; windowing applies only to the arbitrary survivor path.
- Java tests use fluent `assertQuery()`/`assertSql` with exact result sets (oracle), never `printSql`+`assertEquals`.
- License header starts with `/*+` (the Apache block used across core).

---

## 1. Problem recap (from the lvl3-production review)

`OperationExecutor.replaceWithSurvivors` calls `TableWriter.replaceRange(minTs, maxTs+1, survivorCursor, …)` once over the whole populated range. `replaceRange`'s cursor loop stages **every** survivor into O3 native memory (`newRow`/`copier.copy`/`row.append`) before sorting + applying. O3 memory is malloc-backed with `getO3MemMaxPages()==Integer.MAX_VALUE`, no per-query tracker, `PressureControl.EMPTY`. For an arbitrary predicate on a large table (even a zero-match delete), peak native memory ≈ the whole table → OOM-kills the JVM. The route-b Parquet pre-pass un-tiers **all** Parquet partitions and the rewrite doubles disk transiently (H1). The code's own javadoc flags the memory concern and defers the bound to "Task 2.1", which became the time-range path — so the bound was never shipped.

## 2. Approach: adaptive windowed survivor-replace

Split the delete range `[minTs, maxTs]` into consecutive windows of ~`rowsPerStep` rows and process each window independently, but commit the whole delete once.

```
executeDelete (arbitrary path):
  tableWriter.setSeqTxn(S)                     // in-memory; durable seqTxn still S-1
  step = windowStepTsWidth(tableWriter, rowsPerStep)   // §3
  tableWriter.beginReplaceRange()              // §5: checkDistressed + begin size-update + dedupMode=REPLACE_RANGE (no commit)
  try:
    for (wLo = minTs; wLo <= maxTs; wLo = wHiExcl):
      wHiExcl = clampAdd(wLo, step, maxTs + 1)         // min(wLo+step, maxTs+1), overflow-safe
      convertParquetPartitionsForWindow(wLo, wHiExcl)  // §7: only this window's Parquet partitions
      bindWindow(survivorFactory, wLo, wHiExcl)        // §4: rebind ts-bound vars
      try (cur = survivorFactory.getCursor(ctx)):
        tableWriter.applyReplaceRangeWindow(wLo, wHiExcl, cur, copier, tsIdx, ctx)  // §5
    tableWriter.finishReplaceRange()           // §5: single commit00 → seqTxn S durable
  catch: rollback + setSeqTxn(S-1) (unchanged catch semantics)
```

Peak O3 memory = one window's survivors (~`rowsPerStep`); peak extra disk = one window's partitions.

## 3. Window sizing (reuse the mat-view estimator)

`MatViewRefreshJob.estimateBucketsForRows(targetRows, tableRows, bucket, partitionDuration, partitionCount)` is already `public static`:
```java
totalBuckets = (partitionDuration / bucket) * partitionCount
return max(1, totalBuckets * targetRows / tableRows)   // double math, overflow-safe
```
Call it with `bucket = 1` to get a **ts-width** (in the table's ts unit) spanning ~`targetRows` rows:
```java
long step = MatViewRefreshJob.estimateBucketsForRows(
        rowsPerStep,                        // config, §9
        tableWriter.size(),                 // total rows (txWriter.getRowCount())
        1,                                  // bucket = 1 ts unit → result is a ts width
        approxPartitionDurationTsUnits,     // e.g. DAY→86400*unit; from partitionBy + driver
        tableWriter.getPartitionCount());
step = Math.max(step, 1);
```
`approxPartitionDurationTsUnits` comes from the table's partition-by granularity via the `TimestampDriver` (mirrors how the mat-view path derives `approxBucketSize`); for `PARTITION BY NONE` use `(maxTs - minTs + 1)` (single window unless rowsPerStep < tableRows, which then still sub-splits by row density). Density is approximate (uniform assumption); a non-uniform window stages more, still bounded by that window's actual row count — acceptable, matching the mat-view estimate's semantics.

DELETE's range is **contiguous**, so it does **not** need `SampleByIntervalIterator` (built for sparse refresh intervals + timezones); a plain stepped loop over `[minTs, maxTs]` suffices — windows over data gaps simply yield zero survivors and are no-ops.

## 4. Per-window survivor cursor (bind-var ts bounds)

The survivor factory is built at apply time in `generateDelete` (only when `isWalApplication`). Extend it from `SELECT * WHERE NOT(pred)` to carry two window-bound bind variables on the designated timestamp:

```
SELECT * FROM t WHERE NOT(pred) AND <ts> >= $wLo AND <ts> < $wHiExcl
```

- One compile; `executeDelete` rebinds `$wLo`/`$wHiExcl` per window (`ctx.getBindVariableService().setTimestamp(...)`) before `getCursor`.
- The ts bounds become an **interval scan** on the designated timestamp, so each window reads only its slice — no full-table scan per window.
- The bounds use `>=`/`<` matching `replaceRange`'s `[lo, hiExcl)` contract, so a survivor's ts is always within `[wLo, wHiExcl)` (the in-range assertion in the cursor loop still holds).
- `NOT(pred)` semantics are unchanged (2-valued; bind-var ts bounds are an additional AND, orthogonal to the predicate).

`generateDelete` change: append the two ts-bound conjuncts (as bind-var expression nodes) to the negated survivor WHERE. The bind variables are apply-local (the factory is rebuilt at apply, not serialized to the WAL — the WAL carries only the DELETE SQL text, §unchanged).

## 5. `replaceRange` refactor: begin / apply-window / finish

Split today's monolithic `replaceRange(lo, hiExcl, cursor, …)` into three methods; the existing single-window `replaceRange` becomes `begin` + one `applyReplaceRangeWindow` + `finish` (a thin back-compat wrapper is kept for the direct-primitive tests and any other caller).

- **`beginReplaceRange()`** — the current method's pre-loop setup, extracted verbatim: `checkDistressed(); beginPartitionSizeUpdate(); dedupMode = WAL_DEDUP_MODE_REPLACE_RANGE;`. Records `rowsBefore = txWriter.getRowCount()`. **No `commit00()` here** — WAL apply hands each txn a clean committed S-1 base, and nothing is persisted until `finishReplaceRange()`. (Confirm against the real method during the spike — copy its exact pre-loop lines, do not paraphrase.)
- **`applyReplaceRangeWindow(lo, hiExcl, cursor, copier, tsIdx, ctx)`** — the current cursor-staging + sort + `processWalCommitFinishApply(…)` body, scoped to `[lo, hiExcl)`. Sets `setLagMinTimestamp(lo)/setLagMaxTimestamp(hiExcl-1)` for that window. Stages only this window's survivors into O3, applies the partition drop/trim/merge for `[lo, hiExcl)`, then `finishO3Append(0)` — releasing this window's O3 memory before the next window (the memory bound). **Does NOT** call `commit00()`.
- **`finishReplaceRange()`** — `dedupMode = DEFAULT; commit00(); housekeep(); shrinkO3Mem();` returns `rowsBefore - txWriter.getRowCount()`. The single `commit00()` is the single seqTxn advance.

Because consecutive windows are **disjoint ts-ranges → disjoint partitions**, each window's surgery touches partitions no other window touches; the partition-size-update bookkeeping opened by `beginPartitionSizeUpdate()` accumulates all windows' changes and is finalized once by `commit00()`. (A window that trims a partition boundary touches only that partition; the next window starts at the previous `wHiExcl`, so it starts at/after that partition's upper edge — no overlap.)

Failure inside the loop unwinds via the existing `finally { finishO3Append(0); … }` + `executeDelete`'s `catch` (rollback + `setSeqTxn(S-1)`), unchanged.

## 6. Crash-safety argument

Two properties, both preserved:

1. **Single durable seqTxn advance.** Only `finishReplaceRange()`'s `commit00()` persists the txn/seqTxn. `beginReplaceRange()` performs setup only (no `commit00()`); the writer is already at the committed S-1 base. Per-window `applyReplaceRangeWindow` writes partition data to disk + mutates in-memory `txWriter` bookkeeping but **never** persists the txn. So a crash at any point before `finishReplaceRange` leaves durable seqTxn = S-1 → `ApplyWal2TableJob` re-runs WAL txn S (the whole DELETE) from scratch. On-disk partition data written by already-applied windows is uncommitted (the txn file still names S-1) and is ignored/overwritten by the re-apply — identical to the risk profile of a crash between the shipped single-window replace's `processWalCommitFinishApply` and its `commit00()`, just repeated across windows.

2. **Idempotent re-apply.** After window `w` is applied, `[wLo_w, wHiExcl_w)` contains only rows satisfying `NOT(pred)`. On re-apply, that window's survivor cursor (`NOT(pred) AND ts ∈ window`) returns exactly those same rows, and replacing the window with them is a no-op. So re-running the full delete after a crash re-applies finished windows as no-ops and completes the unfinished ones → the final state is identical whether or not a crash occurred.

Together: crash-safe with a bounded working set. This mirrors the shipped single-window delete's crash-safety (which already relies on re-apply idempotence) — extended across windows. The Parquet convert pre-pass remains idempotent (no-op on now-native partitions), and moving it inside the window loop (§7) keeps it a no-op on re-apply.

## 6a. Key implementation risks & de-risking spike

The whole design rests on one unproven property of `TableWriter`: that **N per-window `processWalCommitFinishApply` calls accumulate into a single `commit00()`** with no intermediate txn persist and no cross-window corruption. Three concrete unknowns to resolve in a **TDD spike before any production code** (mirroring the original DELETE plan's replaceRange option-a/b spike):

1. **Intermediate persist.** Does `processWalCommitFinishApply` (or anything it calls) advance the durable txn/seqTxn on its own, or is persistence strictly deferred to `commit00()`? If it persists per call, the single-seqTxn invariant breaks and the split is not viable as drawn — fall back to a different bound (e.g. cap survivor staging with a spill, out of scope) or accept per-window commits only if each is independently crash-consistent (it is not, for a mid-delete state). **This is the gate.**
2. **`setLagMin/MaxTimestamp` across windows.** These bound the O3 region a commit sees. Per-window they are `[wLo, wHiExcl-1]`; if `commit00()` derives the txn's committed min/max timestamp from the *last* window's lag values rather than the union across windows, the committed metadata is wrong. Resolve: either (a) they only scope the per-window O3 merge and `commit00()` recomputes true min/max from partition state (preferred — then per-window is correct), or (b) track global `[minSurvivorTs, maxSurvivorTs]` and set once before `finishReplaceRange`.
3. **`beginPartitionSizeUpdate` / size bookkeeping.** Confirm the partition-size-update ledger opened once in `begin` tolerates multiple `processWalCommitFinishApply` surgeries before its `finish`, and that disjoint-partition windows never touch the same size-update slot.

**Spike shape:** extend `TableWriterReplaceRangeDirectTest` — drive `beginReplaceRange` + two `applyReplaceRangeWindow` calls over two disjoint ts-ranges + `finishReplaceRange`, then assert (a) result == two separate whole-range replaces, (b) exactly one seqTxn advance, (c) correct table min/max timestamp, (d) crash-and-reapply (release/reopen mid-sequence) == clean run. Only if the spike is green does the `OperationExecutor` wiring proceed; if unknown #1 fails, escalate to the human with the finding before writing more code.

## 7. Parquet (H1)

`convertParquetPartitionsForDelete` currently converts, up front, **all** Parquet partitions the whole-range replace would touch (route-b = every Parquet partition). Move the conversion **inside the window loop**, scoped to the window: `convertParquetPartitionsForWindow(wLo, wHiExcl)` converts only the Parquet partitions overlapping `[wLo, wHiExcl)` immediately before that window's replace. Peak extra disk = one window's converted partitions, not the whole table. The conversion still self-commits at S-1 durability semantics (as today) and is idempotent on re-apply. Fully-covered Parquet partitions inside a window still drop inline via the existing guard.

## 8. Config

New key `cairo.wal.delete.rows.per.step` (long, default `1_000_000`) — the target rows per window, fed as `rowsPerStep`. Rationale for a dedicated key (vs reusing `cairo.mat.view.rows.per.query.estimate`): DELETE and mat-view refresh have independent tuning and blast radius; a mat-view-named knob governing DELETE memory is a foot-gun. Optionally cap the step ts-width by a max (reuse the "single window if step ≥ range" degenerate — no separate max key needed for v1). Default 1M rows/window ≈ tens–hundreds of MB staged, well under typical heap/RSS.

## 9. Observability (M1) + recovery note (M2)

- **M1:** `executeDelete` logs one INFO line per delete: table, strategy (time-range vs windowed-survivor), predicate row estimate, window count, total rows deleted, and elapsed; per-window progress + Parquet-convert events at DEBUG. Today the whole-table rewrite is silent — an operator cannot see a "small" DELETE doing O(table) work.
- **M2:** Document (spec §14 of the original DELETE design + operator docs) that a DELETE that fails at apply suspends the table, and `RESUME WAL FROM TRANSACTION <n+1>` **skips** the delete (the rows are NOT deleted) — an operator resuming past a poison delete must re-issue it. No code change; a documented, explicit recovery semantic.

## 10. Enterprise cheap items (folded in)

Independent of C1/H1, close the lvl3 Enterprise test gaps:
- `PGWireAclTest`: a `GRANT DELETE` → `DELETE FROM t` succeeds / no-grant → denied / `REVOKE DELETE` → denied round-trip (mirrors the sibling TRUNCATE/UPDATE/INSERT cases) — the SQL/wire layer the unit sweeps don't cover.
- `PermissionTest.testIsTableLevel`/`testIsColumnLevel`: add `DELETE` to the hand-picked lists (two one-liners).

## 11. Edge cases

- **Single window** (small table, `step ≥ range`): the loop runs once → behavior identical to today's whole-range replace (no regression; the direct-primitive tests exercise this via the back-compat wrapper).
- **Empty window / data gap**: survivor cursor returns 0 rows over a gap → `applyReplaceRangeWindow` with an empty batch over a range containing no data → no-op.
- **All-match window** (every row in the window matches `pred`): survivor cursor returns 0 rows → empty replace over the window → all its rows deleted (correct).
- **Zero-match delete** (predicate matches nothing anywhere): every window's survivor set = every row in the window → each window replaces itself with itself (no-op) → table unchanged, not suspended. (Still O(table) work but bounded per window; a future optimisation could short-circuit, out of scope.)
- **Overflow**: `wLo + step` guarded by `clampAdd` (saturates at `maxTs + 1`); `maxTs + 1` uses the existing nanos-safe handling (nanos `Long.MAX_VALUE` is unstorable per #7389, so `maxTs + 1` cannot overflow here).
- **Decimals**: `copier.copy(ctx, …)` keeps the real (non-null) execution context per window (the DECIMAL-copier NPE fix), unchanged.

## 12. Testing

- **Correctness across windows** (`DeleteTest`): a table sized so `rows/step` forces ≥3 windows (set `cairo.wal.delete.rows.per.step` low via `setProperty`); arbitrary predicate; assert the exact survivor set via `assertSqlCursors` against `SELECT * WHERE NOT(pred)` + exact counts — proves windowing == whole-range.
- **Single-window equivalence**: same delete with a high rows-per-step (one window) → identical result (no regression).
- **Crash-safety / idempotent re-apply**: multi-window delete, `engine.releaseInactive()` + `drainWalQueue()` re-apply → unchanged result, table not suspended, one seqTxn advance (assert `getSeqTxn` delta == 1).
- **Parquet per-window**: multi-Parquet-partition arbitrary delete → each window converts only its partitions; assert correctness + not-suspended + `isParquet=false` on touched partitions.
- **Memory-shape proxy**: with a low rows-per-step and a multi-window delete, assert window count > 1 (white-box hook or a debug counter) — direct RSS assertion is impractical; the bound follows from `finishO3Append` per window (verified by the reviewer that O3 memory is released per window).
- **Edge**: zero-match multi-window; all-match window; single `PARTITION BY NONE` table; empty table.
- **Enterprise**: the `PGWireAclTest` DELETE round-trip (§10).

## 13. Non-goals / residuals

- **Zero-match short-circuit** — a delete matching nothing still rewrites every window as a no-op (bounded memory, but O(table) work). Optimisation deferred.
- **Cross-window parallelism** — windows are applied sequentially (disjoint partitions would allow parallel apply, but that complicates the single-commit bookkeeping); deferred.
- **SYNC-mode Parquet-convert fsync residual** — unchanged from the shipped DELETE (documented v1 residual); per-window convert does not worsen it.
- **Enterprise** needs no C1/H1 change (authorization only); just the two folded-in tests.
