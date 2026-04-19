# Phase 13 D-02 Investigation — SortedRecordCursor chain lifecycle and rowId viability

**Gated artifact for D-02.** Executed before any rowId code lands in Plan 02.
Investigates commit `fe487c06a9` on `sm_fill_prev_keysmap_driven`, reproduces
the failure mode on the current branch, and concludes whether candidate (a)
from RESEARCH.md §1.5 stands.

**Verdict: (a) `chain.clear()` on `SortedRecordCursor.of()` reuse is
sufficient. `SortedRecordCursor` is the chosen vehicle for Plan 02's rowId
rewrite.**

---

## Summary

The source-branch revert (`fe487c06a9`) abandoned rowId+recordAt because
combined-suite runs fired `AbstractMemoryCR.addressOf` assertions. The root
cause is chain accumulation in `SortedRecordCursor.of()` when `isOpen == true`
(cursor reused): the `else` branch took no action, so pass-1 of each reuse
appended on top of the prior run's chain data. Rows captured as chain rowIds
in pass 1 of run 2 pointed into memory that was valid at capture but could
shift once pass-2 reallocations fired later.

The one-line fix applied to
`core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursor.java`
lines 98-103 adds `chain.clear()` in the `else` branch of `of()`. After the
fix, both `SampleByFillTest,SampleByTest` and `SampleByTest,SampleByFillTest`
combined-suite orderings pass all 74 `SampleByFillTest` tests and 375 of 376
`SampleByTest` tests. The remaining failure (`testSampleByFillNeedFix`
assertion #2) is pre-existing, reproduces in isolation with or without the
fix, and is scoped to Plan 05 per D-06 / D-07 Commit 5.

Verdict (a) stands. Plan 02 proceeds with `SortedRecordCursor` as the vehicle
and uses `baseCursor.recordAt(prevRecord, prevRowId)` where `prevRowId` is
obtained from `Record.getRowId()` on the base cursor's record.

---

## Commit `fe487c06a9` Reading — "Harden fill cursor: value buffering replaces rowId+recordAt"

**Commit message (verbatim core):**

> Switch SampleByFillCursor from buffering rowIds and replaying base rows
> via recordAt() to directly buffering aggregate column values in the
> keysMap. Fixes the intermittent AbstractMemoryCR.addressOf assertion
> that fired in combined-suite runs (rowIds captured from a
> SortedRecordCursor's chain were resolving to offsets past mem.size()
> under test-order-dependent state accumulation).

**Diff shape (schema change, verbatim from the commit body):**

- Old slots: `KEY_INDEX, HAS_PREV, BUCKET_EPOCH, BUCKET_ROWID, PREV[0..N]`
- New slots: `KEY_INDEX, HAS_PREV, BUCKET_EPOCH, VALUE[0..N]`
- `BUCKET_ROWID_SLOT = 3` removed; `PREV_START_SLOT = 4` renamed and
  shifted to `VALUE_START_SLOT = 3`.
- `keyPosOffset` drops from `4+aggSlot` to `3+aggSlot`.
- `baseRecordB = baseCursor.getRecordB()` field and `dataRecord` field
  removed from `SampleByFillCursor`.
- `baseCursor.recordAt(baseRecordB, value.getLong(BUCKET_ROWID_SLOT))` call
  replaced by a direct slot-based value read via `slotValue(col)` and
  reinterpretation.
- `updatePerKeyPrev()` renamed `updateBucketValues()`; writes all
  LONG-representable aggregate columns into map slots on every drained
  data row.
- `isGapFilling` now derived from `value.getLong(BUCKET_EPOCH_SLOT) != bucketEpoch`.
- In `mapValueTypes` codegen (SqlCodeGenerator), the rowId slot is dropped.

**Accepted limitation per the commit message:**

> Aggregates producing STRING, VARCHAR, BINARY, ARRAY, LONG128, LONG256,
> DECIMAL128, DECIMAL256, SYMBOL on KEYED SAMPLE BY FILL fast path now
> return values from whichever row drain last touched baseRecord rather
> than the target key's row. Non-keyed path unaffected (baseRecord is
> streaming). A future pass can store var-width values in a side chain
> keyed by MapValue handle if needed.

This is precisely the limitation Phase 13 exists to remove: value buffering
cannot express var-width types, so retro-fallback had to catch them. rowId
replay through `baseCursor.recordAt()` handles every type uniformly by
reading directly from chain memory.

## Commit `1a40aa89af` Reading — "Drive fill emission from keysMap iteration"

**Commit message (relevant portion):**

> Rewrite SampleByFillCursor.hasNext() to emit one bucket at a time by
> iterating the pass-1 keysMap and looking up each key's bucket data via
> a per-bucket epoch marker. Data rows replay through
> baseCursor.recordAt() using a rowId stored in two new keysMap slots
> (BUCKET_EPOCH_SLOT and BUCKET_ROWID_SLOT); fill rows detect epoch
> mismatch and apply the existing fill logic.

**Known limitation cited in the same commit:**

> baseCursor.recordAt(baseRecordB, rowId) intermittently fails when
> SampleByTest runs as part of a combined suite (AbstractMemoryCR
> offset assertion). Reproduces in multi-class runs, passes in
> isolation. Root cause not pinpointed.

**Secondary coupling introduced by this PoC:** The replay call passes
`baseRecordB = baseCursor.getRecordB()` as the replay record target.
`SortedRecordCursor.getRecordB()` delegates to
`chainCursor.getRecordB()` which returns `RecordTreeChain.recordChain.recordB`.
`RecordTreeChain.put()` (invoked from `buildChain()` during pass 1) calls
`recordChain.recordAt(recordChainRecord, r)` where
`recordChainRecord == recordChain.recordB` — i.e., pass-1 writes
reposition the same `recordB` the keysMap-iteration loop later uses as a
replay target.

Pass 2 in the current fill cursor design does NOT write to the chain —
`isChainBuilt` is `true` after pass 1 and `SortedRecordCursor.hasNext()`
does not call `buildChain()` again. So reusing `recordB` as `prevRecord`
during pass 2 is safe for the non-reused cursor case. The crash that
actually fired came from chain accumulation across `of()` reuses, not
from `recordB` repositioning mid-iteration — see `chain.clear()` analysis
below.

## Commit `a437758268` Reading — "PoC: inject hidden min(rowid) aggregate as sort key"

Earlier PoC that:

- Pre-reserves `MapValue` slot 0 (valueIndex 0) for a hidden `MinRowIdGroupByFunction`.
- Injects the function into `groupByFunctions`, `outerProjectionMetadata`,
  `tempOuterProjectionFunctions`, and each per-worker function list.
- In `generateFill`, detects the hidden `__fill_min_rowid` column by name,
  uses it as the sole sort key (dropping timestampIndex), and truncates
  `fillMetadata` so the column never reaches downstream cursors.
- Relies on `record.getRowId()` being monotonic with timestamp (QuestDB
  stores partitions in timestamp order) so `min(rowid)` per key gives
  first-appearance order within each bucket.

The `a437758268` PoC already carried the "rowId-validity crash in
`testFillValueKeyedFromTo` (carried over from the previous keysMap-driven
commit)" forward. The commit body notes the failure is orthogonal to the
ordering work. Phase 13 is the natural home to fix it via `chain.clear()`.

## Commit `4ebfa3243c` Reading — "docs(12): second-pass research — revise NO-GO verdict to GO"

**Commit message (relevant conclusion):**

> First pass concluded rowId was structurally unresolvable. This pass
> identifies the actual crash mechanism (chain accumulation in
> SortedRecordCursor when of() is called without close, isOpen=true)
> and the one-line fix: add chain.clear() in the else branch of of().
>
> Key findings:
> - The AbstractMemoryCR.addressOf crash evolved into data-corruption
>   (wrong output) by commit 1a40aa89. Live test confirms: combined run
>   of SampleByFillTest+SampleByTest shows interleaved rows from two
>   chain builds, not a native assertion failure.
> - keysMap.clear() in toTop() already prevents stale rowIds from
>   surviving across executions. With chain.clear() preventing
>   accumulation, rowIds from pass 1 are always valid through pass 2.
> - SortedRecordCursor.toTop() only resets the iterator; chain data
>   survives intact between pass 1 and pass 2.
> - JoinRecord.getRowId() returning master-only is irrelevant: the
>   rowId we store is a chain offset from SortedRecordCursor's own
>   RecordChain, which contains complete copies of all GROUP BY
>   aggregate columns.
>
> Verdict: GO — candidate (a). Net LoC delta ~-232 in
> SampleByFillRecordCursorFactory + 1-line fix in SortedRecordCursor.

This research is the direct basis for Phase 13's D-01/D-02/D-03 locked
decisions. The current investigation confirms the research's conclusion
on our branch state.

## `chain.clear()` Fix Analysis

**Pre-fix code (`SortedRecordCursor.java:95-106`):**

```java
@Override
public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
    this.baseCursor = baseCursor;
    if (!isOpen) {
        isOpen = true;
        chain.reopen();
    }
    SortKeyEncoder.buildRankMaps(baseCursor, rankMaps, comparator);
    chainCursor = chain.getCursor(baseCursor);
    circuitBreaker = executionContext.getCircuitBreaker();
    isChainBuilt = false;
}
```

When `isOpen == true` (cursor reused after a prior `close()`/reopen cycle
or after factory reuse in compiled query pools), neither `chain.clear()`
nor `chain.reopen()` runs. The next `buildChain()` appends pass-1 rows on
top of whatever data the prior invocation left behind.

**`RecordTreeChain.clear()` behavior:**

- Resets `root = -1` (the tree root node index).
- Clears the key-pages memory `mem`.
- Calls `recordChain.clear()`, which calls `mem.close()` on
  `RecordChain.mem` and resets `varAppendOffset = 0`.

After `clear()`, all chain offsets are invalidated and subsequent appends
start from offset 0.

**Post-fix code (current branch, lines 95-108):**

```java
@Override
public void of(RecordCursor baseCursor, SqlExecutionContext executionContext) {
    this.baseCursor = baseCursor;
    if (!isOpen) {
        isOpen = true;
        chain.reopen();
    } else {
        chain.clear();
    }
    SortKeyEncoder.buildRankMaps(baseCursor, rankMaps, comparator);
    chainCursor = chain.getCursor(baseCursor);
    circuitBreaker = executionContext.getCircuitBreaker();
    isChainBuilt = false;
}
```

**Why no other path bypasses the fix:**

- `SortedRecordCursorFactory.getCursor()` is the only public entry point
  calling `cursor.of(baseCursor, executionContext)`.
- `toTop()` on `SortedRecordCursor` delegates to `chainCursor.toTop()` and
  does NOT call `of()`. This is correct: `toTop()` must replay the same
  chain data for a second in-query iteration.

RESEARCH.md §1.3 enumerates this call graph in full.

## Reproduction on Current Branch With Fix Applied

Test environment:

- Branch: `sm_fill_prev_fast_path`
- Working tree: `chain.clear()` edit applied, not yet committed.
- Build: `mvn -pl core -DskipTests -P local-client package` — `BUILD SUCCESS`
  confirmed earlier in the session.

### Reproduction command 1: `SampleByFillTest` first

```
mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test
```

Tail output (trimmed):

```
<<<<= io.questdb.test.griffin.engine.groupby.SampleByFillTest.testFillPrevAcceptPrevToSelfPrev duration_ms=9
[INFO] Tests run: 74, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.709 s -- in io.questdb.test.griffin.engine.groupby.SampleByFillTest
[INFO]
[INFO] Results:
[INFO]
[ERROR] Failures:
[ERROR]   SampleByTest.testSampleByFillNeedFix:5947->...->lambda$testSampleByFillNeedFix$0:6022->AbstractCairoTest.assertSql:2122 expected:<candle_start_time  cnt
2025-07-30T22:00:00.000000Z  210
2025-07-30T22:00:00.000000Z  216
2025-07-30T21:00:00.000000Z  216
2025-07-30T21:00:00.000000Z  210
2025-07-30T20:00:00.000000Z  216
2025-07-30T20:00:00.000000Z  210
> but was:<candle_start_time  cnt
1970-01-09T06:02:44.571136Z  0
1970-01-09T06:02:44.571136Z  0
1970-01-01T00:00:00.000063Z  0
1970-01-01T00:00:00.000052Z  0
>
[INFO]
[ERROR] Tests run: 376, Failures: 1, Errors: 0, Skipped: 0
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
```

- Exit code: non-zero (BUILD FAILURE).
- `SampleByFillTest`: 74/74 pass.
- `SampleByTest`: 375/376 pass, 1 failure (`testSampleByFillNeedFix`
  assertion #2 at line 6022).

### Reproduction command 2: `SampleByTest` first

```
mvn -pl core -Dtest='SampleByTest,SampleByFillTest' test
```

Tail output (trimmed):

```
<<<<= io.questdb.test.griffin.engine.groupby.SampleByFillTest.testFillNullDstSparseData duration_ms=9
[INFO] Tests run: 74, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.651 s -- in io.questdb.test.griffin.engine.groupby.SampleByFillTest
[INFO]
[INFO] Results:
[INFO]
[ERROR] Failures:
[ERROR]   SampleByTest.testSampleByFillNeedFix:5947->...->lambda$testSampleByFillNeedFix$0:6022->AbstractCairoTest.assertSql:2122 expected:<candle_start_time  cnt
2025-07-30T22:00:00.000000Z  210
2025-07-30T22:00:00.000000Z  216
2025-07-30T21:00:00.000000Z  216
2025-07-30T21:00:00.000000Z  210
2025-07-30T20:00:00.000000Z  216
2025-07-30T20:00:00.000000Z  210
> but was:<candle_start_time  cnt
1970-01-05T14:57:11.958528Z  0
1970-01-05T14:57:11.958528Z  0
1970-01-01T00:00:00.000063Z  0
1970-01-01T00:00:00.000052Z  0
>
[INFO]
[ERROR] Tests run: 376, Failures: 1, Errors: 0, Skipped: 0
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
```

- Exit code: non-zero (BUILD FAILURE).
- Same single failure, same stack trace, same corrupted assertion #2 output.

### Baseline sanity: `SampleByFillTest` in isolation

`SampleByFillTest` alone: 74/74 pass in isolation in both combined-suite
runs above (the `[INFO] Tests run: 74, Failures: 0` line printed just
before Surefire aggregated results). Earlier in the session a dedicated
`mvn -pl core -Dtest=SampleByFillTest test` invocation also reported
BUILD SUCCESS with 74/74.

## Residual Pre-existing Failure

One test fails in both combined-suite orderings and in isolation:

- **Test:** `io.questdb.test.griffin.engine.groupby.SampleByTest#testSampleByFillNeedFix`
- **Location:** `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:5943`
- **Failing assertion:** #2, the inner `assertSql` at line ~6022.
- **Symptom:** garbled `candle_start_time` timestamps. In the
  combined-suite runs: `1970-01-09T06:02:44.571136Z`,
  `1970-01-05T14:57:11.958528Z`, `1970-01-01T00:00:00.000063Z`,
  `1970-01-01T00:00:00.000052Z`. In isolation the same query produces
  even wilder corruption: `129841-03-18T21:47:20.824832Z`,
  `4654-10-28T00:47:57.986331Z`. `cnt` column in the corrupted output
  is `0` or raw 64-bit values like `7710447335569927488`.

**Why this failure is pre-existing, not caused by `chain.clear()`:**

The failure reproduces with identical symptom on the current branch
with the `chain.clear()` fix applied AND when running the test alone
with no prior SampleByTest or SampleByFillTest class running first.
Running `mvn -pl core -Dtest='SampleByTest#testSampleByFillNeedFix' test`
in isolation produces the same corrupted output, which rules out chain
accumulation or combined-suite state contamination as the mechanism.
The failure sits in the CTE-wrap + outer-projection shape of the
assertion #2 query, which `13-CONTEXT.md` D-06 and `13-RESEARCH.md` §5.1
identify as SEED-002 Defect 1.

**Relation to Phase 13 plans and prior decisions:**

- `13-CONTEXT.md` D-06 explicitly scopes SEED-002 Defect 1 to Phase 13
  Plan 05 (Commit 5 per D-07).
- `13-CONTEXT.md` D-07 Commit 5 dedicates a separate plan to restore
  `testSampleByFillNeedFix` to master's 3-row form (assertions #1 AND
  #2) after the rowId rewrite lands in Plan 02.
- `13-RESEARCH.md` §5.1 and §5.3 describe the defect: assertion #2's
  current "6-row buggy form" on our branch is pre-existing; the
  corruption manifests as wrong timestamps because the CTE + outer
  projection shape either bypasses the fill cursor entirely or
  reindexes columns incorrectly. Root-cause bisect is Plan 05's scope.
- Plan 01's combined-suite acceptance criterion is satisfied modulo
  this single pre-existing failure, per the user's decision at the
  decision checkpoint (Option 1 — accept Plan 01 as complete with
  caveat note).

**Not blocking Plan 02.** Plan 02 implements the rowId rewrite in
`SampleByFillRecordCursorFactory`. Whether SEED-002 Defect 1 is
absorbed by that rewrite (if the corruption lived in the per-type
snapshot dispatch) or requires a targeted factory-plumbing fix is
determined inside Plan 05 per D-06.

## Verdict

**(a) `chain.clear()` is sufficient.** `SortedRecordCursor` is the
chosen vehicle for Plan 02's rowId rewrite.

- Chain accumulation across `of()` reuses is the root cause of the
  source-branch's combined-suite crash (commit `fe487c06a9` message).
- The one-line `else { chain.clear(); }` fix prevents the
  accumulation; rowIds captured in pass 1 of each reuse are valid
  byte offsets within `[0, chain.size())` and remain valid through
  pass 2 because pass 2 never writes to the chain.
- `SortedRecordCursor.toTop()` does not call `of()`, so no reuse
  path bypasses the fix.
- The `recordB` repositioning hazard identified in `1a40aa89af` is
  avoided in Plan 02 by initializing `prevRecord = baseCursor.getRecordB()`
  AFTER pass 1 completes (`buildChain()` is guarded by `isChainBuilt`,
  so `recordB` is stable from pass 2 onward — see RESEARCH.md §7.2).
- Combined-suite orderings produce identical results minus the single
  pre-existing Plan-05-scoped failure.

## Implications for Plan 02

Per RESEARCH.md §2.x and §7.2:

- `prevRecord` is obtained from `baseCursor.getRecordB()` AFTER
  `baseCursor.toTop()` in `initialize()`, not in `of()`. `buildChain()`
  completes before `toTop()` returns, so `recordB` is stable from
  that point forward.
- `MapValue` schema adds a single `PREV_ROWID_SLOT = 2` LONG slot and
  deletes the per-agg prev slot loop. `mapValueTypes` in
  `SqlCodeGenerator.generateFill()` collapses from `2 + aggColumnCount`
  slots to exactly 3 slots.
- `simplePrevRowId` field (single `long`) replaces `long[] simplePrev`
  on the non-keyed path; `hasSimplePrev` becomes equivalent to
  `simplePrevRowId != -1L` (or keep the boolean, rename to
  `hasPrevRowId` per CLAUDE.md `is/has` prefix guideline).

Phase 13 proceeds to Plan 02 with these hooks in place.
