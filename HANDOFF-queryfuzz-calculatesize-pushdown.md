# Handoff: `QueryFuzzTest#testQueryFuzz` CI failure on PR #6939 (live views)

**Status: confirmed regression introduced by this branch. Not flaky, not pre-existing, not arm64-specific.**
Deterministically reproducible from a seed. Root cause identified and mechanically proven. **Not fixed** — this
document is investigation output only; the working tree is clean and unmodified.

---

## 1. The CI failure

| | |
|---|---|
| PR | #6939 `feat(sql): add live views`, branch `puzpuzpuz_live_view` |
| Branch HEAD investigated | `725358ed3c` ("Reformat nested ternary in LiveViewRefreshJob") |
| Merge base / master baseline | `3c1c3382cc` ("test(core): fix seed-dependent crash in checkpoint fuzz test (#7381)") |
| Azure buildId | 252120 |
| Failed job | `SelfHosted Griffin tests on linux-arm64` (jobId `64e91a12-6e0c-58b8-472a-e1169e99698f`) |
| Failed step | `Run tests` (logId 660) |
| Scale | **1 failed test out of 133,884**; 20 of 21 test runs passed |

Every other check on the build is green (Cairo A/B, Griffin on x64-zfs and x86-graal, Other A/B, coverage jobs,
Rust lint, `build`, `gitleaks`, `Danger`, `enterprise-ci`). Three checks were still pending at analysis time.

Failing assertion:

```
QueryFuzzTest.testQueryFuzz:260 -> ... -> runFuzz:547 -> buildFailure:323
query fuzz found 1 unexpected failure(s):
  [1] CursorCheckException: calculateSize() counted 0 but the cursor materialized 16 rows:
  sql: WITH cte0 AS (SELECT * FROM fuzz_t1 WHERE c2 IS NOT NULL LIMIT 16) SELECT * FROM cte0 LIMIT 40
```

### Why only one platform failed

`QueryFuzzTest` seeds itself from `System.nanoTime()` / `System.currentTimeMillis()`
(`TestUtils.generateRandom(Log)`), so **every CI job draws a different seed**. The other Griffin jobs simply
never generated this query shape against this data. This is *not* an arm64-specific defect — see the local
x86_64 reproduction below. Expect this to recur randomly on any platform until fixed.

---

## 2. Reproduction

The failing run's seed, recovered from the CI log:

```
random seeds: 12756455990579198L, 1784185687166L
fuzz config: tables=2, rows=147, queries=100, diffJit=true, diffShadow=true,
             verifyCursor=true, faults=true, faultPct=15, parallelFaults=true, window=true
fuzz schema fuzz_t1 (parquet=ALL): sym=SYMBOL c0=BOOLEAN c1=STRING c2=BYTE c3=DATE ts=TIMESTAMP
```

Command (reproduces in ~2s of test time on linux x86_64):

```bash
mvn -pl core -Dtest=QueryFuzzTest#testQueryFuzz -DfailIfNoTests=false \
    -Dquestdb.fuzz.s0=12756455990579198 -Dquestdb.fuzz.s1=1784185687166 test
```

Two schema facts matter:
- **`fuzz_t1` is `parquet=ALL`** — every partition is parquet, so parquet row-group pruning is live.
- **`c2` is `BYTE`** — BYTE has no null sentinel, so `c2 IS NOT NULL` folds to constant `true`.

### Verified three-way result (identical seed, identical harness)

The whole `core/src/test/java/io/questdb/test/griffin/fuzz/` directory is byte-identical between master and the
branch, so the same seed generates the same tables and the same 100 queries. Both runs self-report the same
workload (`100 queries, 8 serial, 13 with fault injection (10 fired), 8 with bind variant, 15 skipped`),
confirming the query sets match.

| Variant | Result |
|---|---|
| master `3c1c3382cc` | **0 failures — BUILD SUCCESS** |
| branch `725358ed3c` | **1 failure — BUILD FAILURE** (same assertion as CI) |
| branch + one-line `canClamp` guard (see §4) | **0 failures — BUILD SUCCESS** |

This is the proof of regression: master is green on the exact seed that reddens the branch.

---

## 3. Root cause

### The invariant the branch broke

`PageFrameRecordCursorImpl.skipRows()` (`core/src/main/java/io/questdb/griffin/engine/table/PageFrameRecordCursorImpl.java:213`)
computes a decode clamp, then picks a skip strategy:

```java
final boolean canClamp = filter == null && rowCursorFactory.isEntity() && rowCursorFactory.isForwardScan();
maxRowsAfterSkip = canClamp ? requestedMaxRowsAfterSkip : RecordCursor.UNBOUNDED_ROW_COUNT;
rowsProducedSinceSkip = 0;

if (filter != null || rowCursorFactory.isUsingIndex() || frameCursor.hasActivePushdownFilter()) {
    while (rowCount.get() > 0 && hasNext()) {   // <-- row-by-row slow path
        rowCount.dec();
    }
    rowsProducedSinceSkip = 0;
    return;
}
```

The slow path skips rows by calling `hasNext()`. But `hasNext()` is itself clamped
(`PageFrameRecordCursorImpl.java:119`):

```java
if (rowsProducedSinceSkip >= maxRowsAfterSkip) {
    isExhausted = true;
    return false;
}
```

So the slow-path skip only works when `maxRowsAfterSkip` is `UNBOUNDED`, i.e. when `canClamp == false`.

**Before this branch, that was guaranteed.** Both pre-existing slow-path triggers imply `canClamp == false`:

- `filter != null` → `canClamp` requires `filter == null` → false.
- `rowCursorFactory.isUsingIndex()` → an index/scattered row cursor reports `isEntity() == false` → false.

So the invariant held: **slow path ⇒ `canClamp == false` ⇒ `maxRowsAfterSkip == UNBOUNDED` ⇒ the skip loop is
never clamped.**

Commit **`0365f6bc45` "Fix backfill over-count on pruned parquet skip"** (3 Jul) added a *third* slow-path
trigger — `frameCursor.hasActivePushdownFilter()` — which **does not** imply `canClamp == false`. For a plain
forward `SELECT *` entity scan with the filter folded away, `canClamp` stays `true`, so `maxRowsAfterSkip` is
set to the caller's `requestedMaxRowsAfterSkip`. When that value is `0`, the very first `hasNext()` sees
`0 >= 0`, returns false, and **the skip loop skips nothing**.

That commit's own message states the assumption that fails here:

> *"A plain SELECT ... WHERE ... LIMIT lo,hi is unaffected: it routes the skip through FilteredRecordCursor's
> row-by-row path, which respects pushdown."*

That holds only while a `FilteredRecordCursor` exists. Here it does not, because `c2 IS NOT NULL` on a **BYTE**
column folds to a constant. The full chain, verified link by link:

1. `ExpressionParser.java:1773` desugars `<literal> IS NOT NULL` → `<literal> != NULL`.
2. `EqByteFunctionFactory.newInstance` (`EqByteFunctionFactory.java:49-51`) returns `BooleanConstant.FALSE`
   whenever either side `isNullConstant()` — BYTE has no null sentinel (see `griffin/CLAUDE.md`, "NULL
   Sentinels by Type"). So `c2 = null` is constant `FALSE`, and `c2 != null` is constant `TRUE`.
3. The code generator frees a constant-true filter and leaves `filter == null` (same shape as
   `SqlCodeGenerator.java:11064-11073`).
4. But the pushdown conditions were already extracted from the *expression node* `intrinsicModel.filter`
   **before** it was compiled and folded (`SqlCodeGenerator.java:10799`), so `hasActivePushdownFilter()`
   remains `true`.

Net: `filter == null` **and** pushdown active. The LIMIT skips the raw pushdown-carrying page-frame cursor
directly, with no `FilteredRecordCursor` in between — exactly the case the commit believed unreachable.

> The `canClamp` experiment in §2 independently **proves** `filter == null && hasActivePushdownFilter() == true`
> here: had `filter` been non-null, `canClamp` would already have been false and the one-line guard would have
> been a no-op instead of turning the run green.

### The second ingredient

Commit **`41db1f5446` "Gate cursor size() on active pushdown filter"** (13 Jul) made `size()` return `-1` under
active pushdown. That is what steers `LimitRecordCursor` into the broken path: with `baseSize == -1`,
`isBaseSizeKnown()` is false, so `calculateSize()` takes the `else` branch that delegates to
`base.skipRows(counter, 0)` — passing the fatal `requestedMaxRowsAfterSkip == 0`
(`LimitRecordCursorFactory.java:176-189`):

```java
public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter sizeCounter) {
    ensureReadyToConsume();
    if (isBaseSizeKnown()) {
        sizeCounter.add(remaining);
    } else {
        counter.set(remaining);
        base.skipRows(counter, 0);              // max=0 -> clamps hasNext() to no-op
        sizeCounter.add(remaining - counter.get());
        counter.clear();
    }
    remaining = 0;
}
```

**Both commits are required.** `41db1f5446` routes LIMIT into the `skipRows(counter, 0)` path;
`0365f6bc45` makes that path a no-op under pushdown.

### End-to-end trace (matches the observed `0`)

Query tree: `LIMIT 40` → `LIMIT 16` (cte0) → parquet page-frame scan (`filter == null`, pushdown active).

The fuzz oracle (`QueryRunner.checkCalculateSize`, `QueryRunner.java:1144`) calls `toTop()` then
`calculateSize()` after a clean 16-row first pass:

1. Outer `LIMIT 40`: `remaining = 40`, `baseSize = -1` (inner LIMIT's `size()` is -1, since the scan's `size()`
   now returns -1 under pushdown) → `isBaseSizeKnown() == false` → else branch.
2. `counter.set(40)`; `base.skipRows(counter, 0)`.
3. Inner `LIMIT 16`.`skipRows(skipCounter=40, max=0)`: `rowsToSkip=40`, `excessCount=max(0,40-16)=24`,
   `rowsToSkip=16`, `skipCounter.dec(24)` → `16`; `baseMax = min(0, 16-16) = 0`.
4. Scan.`skipRows(rowCount=16, requestedMax=0)`: `canClamp = true` → `maxRowsAfterSkip = 0`. Pushdown gate
   selects the slow path. First `hasNext()`: `rowsProducedSinceSkip(0) >= maxRowsAfterSkip(0)` → `isExhausted=true`,
   returns false. **Loop body never runs; nothing skipped.** `rowCount` stays `16`.
5. Back in inner LIMIT: `counterAfterSkip = 16 > 0` → `remaining = 0`; `skipCounter.add(24)` → `40`.
6. Outer: `sizeCounter.add(remaining - counter.get()) = 40 - 40 = ` **`0`**.

Reported: `calculateSize() counted 0 but the cursor materialized 16 rows`. Exactly the observed failure.

Note `hasNext()`-based iteration is *correct* throughout — the first pass and `checkToTop`'s re-iteration both
yield 16 rows. Only the `calculateSize()`/`skipRows()` accounting diverges.

---

## 4. Suggested fix direction (NOT applied)

Restore the broken invariant by making the new slow-path trigger imply `canClamp == false`, in
`PageFrameRecordCursorImpl.skipRows()`:

```java
final boolean canClamp = filter == null
        && !frameCursor.hasActivePushdownFilter()
        && rowCursorFactory.isEntity()
        && rowCursorFactory.isForwardScan();
```

Rationale: the clamp decodes only the leading `[0, n)` rows of a frame and is sound only for a scan that yields
a frame's rows 1:1 in ascending order. A pushdown-pruned scan is exactly *not* 1:1 with physical rows — the same
premise that motivated `0365f6bc45` — so the clamp should be off whenever pruning is active. With
`maxRowsAfterSkip == UNBOUNDED`, the slow-path loop skips precisely the rows `hasNext()` yields.

Verified: this one-line change turns the failing seed green (§2). **It has been reverted; the tree is clean.**

Caveats for whoever picks this up:
- This forfeits the decode clamp for pushdown scans — a possible performance regression on
  `LIMIT`-over-pruned-parquet. Worth measuring; correctness-neutral alternatives exist (e.g. have the slow path
  save/restore `maxRowsAfterSkip` around the skip loop, applying the clamp only to post-skip reads, which is
  closer to the clamp's intended semantics and keeps the optimization).
- Consider whether `BwdTableReaderPageFrameCursor` (backward scans) needs the same treatment;
  `isForwardScan()` already excludes it from `canClamp`, so it is likely unaffected, but confirm.
- A targeted regression test belongs alongside `ParquetRowGroupPruningTest` (which `41db1f5446` already
  touched): parquet-backed table + a constant-folding filter (e.g. `byte_col IS NOT NULL`) + nested
  `LIMIT`, asserting `calculateSize()` after `toTop()` equals the materialized count. Note the project's
  `.returns(...)` battery already cross-checks `calculateSize()`, so a plain `assertQuery(sql).returns(...)`
  on that shape should catch it.

---

## 5. Recommendation

This blocks the PR. It is a real correctness bug in core SQL execution (not live-view-specific): any
`LIMIT` over a pruned-parquet scan whose filter constant-folds will under-report `calculateSize()`, and
`calculateSize()` feeds user-visible row counts. The live-view work only *reached* it via the two pushdown
commits it carries.

Per the repo convention of bundling related fixes into the branch already in flight, the fix belongs on
`puzpuzpuz_live_view` as a follow-up commit with its own section in the PR body, not a separate PR.

## Appendix: useful commands

```bash
# CI failed-test list (no auth)
curl -s "https://vstmr.dev.azure.com/questdb/questdb/_apis/testresults/resultsbybuild?buildId=252120&publishContext=CI&outcomes=Failed&\$top=200&api-version=5.2-preview.1"

# aggregate counts (no auth)
curl -s "https://dev.azure.com/questdb/6c9c1a0a-74cf-4f7b-bf65-24ae4f3cd61d/_apis/test/resultsummarybybuild?buildId=252120&api-version=7.0-preview"

# the seed line lives in log 660; grep for "fuzz config:" and take the "random seeds:" line above it
curl -s "https://dev.azure.com/questdb/6c9c1a0a-74cf-4f7b-bf65-24ae4f3cd61d/_apis/build/builds/252120/logs/660?api-version=7.0&startLine=1710000&endLine=1712000"
```
