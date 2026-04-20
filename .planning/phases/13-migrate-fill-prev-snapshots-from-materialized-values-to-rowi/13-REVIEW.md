---
status: issues_found
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
depth: standard
files_reviewed: 10
findings:
  critical: 0
  warning: 1
  info: 3
  total: 4
reviewer: gsd-code-reviewer
---

# Phase 13 Code Review

## Scope

Reviewed source files changed during Phase 13 (FILL(PREV) rowId migration, retro-fallback deletion, reordered-aggregate fill mapping fix, WR-04 + Defect 3 grammar):

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java`
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
- `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursor.java`
- `core/src/main/java/io/questdb/griffin/model/IQueryModel.java`
- `core/src/main/java/io/questdb/griffin/model/QueryModel.java`
- `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java`
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java`
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java`

Depth: standard. No Critical issues found. Focus-area correctness (rowId rewrite, prev lifecycle, SortedRecordCursor.clear, deletion completeness) all pass.

## Findings

### Warning

### WR-01 — Defect 3 grammar check misses single non-null constant broadcast

- **File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3568`
- **Issue:** The new "not enough fill values" check guards with `fillValuesExprs.size() > 1 && fillValuesExprs.size() < aggNonKeyCount`, intentionally excluding the single-element case so that `FILL(PREV)` and `FILL(NULL)` can broadcast across all aggregates. However, a single non-null constant (`FILL(0)`, `FILL(42)`) with multiple aggregates slips through the check. Trace for `SELECT ts, sum(a), sum(b) FROM t SAMPLE BY 1h FILL(0)` with `columnCount=3`, `aggNonKeyCount=2`, `fillValues.size()=1`: defect-3 guard evaluates to `1 > 1 && 1 < 2` = false, no rejection; main loop assigns `FILL_CONSTANT(0)` to `sum(a)`, then falls into the `else` branch at line 3556 for `sum(b)` because `fillIdx(1) < fillValues.size()(1)` is false, producing `FILL_CONSTANT(null)`.
- **Context:** Plan 06's Truth block explicitly claims to fix the silent-NULL-pad bug ("current branch silently pads with NULL"). The fix is complete for `size > 1` but incomplete for `size == 1` of a non-null constant. Plan 06's test (`testFillInsufficientFillValues`) uses 5 fill values for 7 aggregates, so it does not cover this case. Pre-existing behavior had the same shape, so this is not a Phase 13 regression — but it is a gap in Plan 06's stated scope relative to master's legacy `SampleByFillValueRecordCursorFactory`, which throws "insufficient fill values for SAMPLE BY FILL" unconditionally when aggregate-count exceeds fill-count.
- **Fix:** Widen the guard to include the single-constant case while preserving the intentional PREV/NULL keyword broadcast:
  ```java
  if (fillValuesExprs.size() < aggNonKeyCount) {
      final ExpressionNode only = fillValuesExprs.size() == 1 ? fillValuesExprs.getQuick(0) : null;
      final boolean isBroadcastable = only != null
              && (isPrevKeyword(only.token) || isNullKeyword(only.token));
      if (!isBroadcastable) {
          throw SqlException.$(fillValuesExprs.getQuick(0).position, "not enough fill values");
      }
  }
  ```
  Add a matching test covering `FILL(0)` with multi-aggregate to lock the behavior. The fix is outside the exact wording of Plan 06's `size() > 1` spec but matches its intent ("master's behavior").

### Info

### IN-01 — Dead import `FillRangeRecordCursorFactory`

- **File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:171`
- **Issue:** `import io.questdb.griffin.engine.groupby.FillRangeRecordCursorFactory;` has no references after Plan 04 removed the retro-fallback.
- **Fix:** Remove the dead import. Separately, consider whether `FillRangeRecordCursorFactory` itself is dead code that can be deleted (out of Phase 13 scope — flag for a follow-up cleanup).

### IN-02 — Redundant `fillIdx >= 0` checks in per-column fill loop

- **File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3500-3502, 3553`
- **Issue:** Both `ExpressionNode fillExpr = fillIdx >= 0 && fillIdx < fillValuesExprs.size() ? ... : ...` (line 3500) and `if (fillIdx >= 0 && fillIdx < fillValues.size())` (line 3553) include a redundant `fillIdx >= 0` check. The preceding early-return at line 3490-3499 guarantees `fillIdx >= 0` by the time execution reaches line 3500. Harmless but noise.
- **Fix:** Drop the `fillIdx >= 0` conjuncts in both conditions.

### IN-03 — Non-ASCII em dashes in comments

- **Files:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (10 occurrences), `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (8 occurrences)
- **Issue:** CLAUDE.md flags non-ASCII (em dashes, curly quotes) as unreliable for QuestDB's log infrastructure. The project rule text scopes this explicitly to LOG/error messages, and all em dashes in the reviewed files sit in Javadoc or `//` comments, never in `LOG.*()` or `SqlException` text. Still worth noting since the em dash count grew via the Phase 13 commits.
- **Fix:** Optional. If matching the project's ASCII-only stance beyond the letter of the rule is desired, replace `—` with ` - ` in the new comments.

## Notes (No action required)

- **prevRecord lifecycle audit.** Confirmed correct across keyed/non-keyed, data/gap, and toTop-reuse paths. `prevRecord = baseCursor.getRecordB()` is set only after pass 1 completes, ensuring `SortedRecordCursor.buildChain()` has already run and `recordB` is stable. `recordAt(prevRecord, rowId)` is called exactly once per emitted fill row (PI-02 invariant), in two sites: non-keyed gap and keyed gap. `toTop()` correctly resets `hasSimplePrev=false`, `simplePrevRowId=-1L`, and `isInitialized=false`, forcing a fresh `initialize()` that re-assigns `prevRecord`. No dereference of stale `prevRecord` is reachable.
- **Resource cleanup.** The generateFill catch block correctly frees `fillValues`, `constantFillFuncs`, `fillFromFunc`, `fillToFunc`, `groupByFactory` on throw. The `SampleByFillRecordCursorFactory` constructor catch frees only `keysMapLocal`, leaving inputs to the caller's catch — correctly avoiding double-free. The factory `_close()` frees `cursor`, `base`, `fromFunc`, `toFunc`, `constantFills`, `keysMap`.
- **Deletion completeness.** No remaining production references to `FallbackToLegacyException`, `stashedSampleByNode`, `isFastPathPrevSupportedType`, `isUnsupportedPrevType`, `hasPrevWithUnsupportedType`, or `isUnsupportedPrevAggType`. `FallbackToLegacyException.java` file removed. Remaining matches for removed symbols sit in `.planning/` docs only.
- **SortedRecordCursor.of() fix.** `chain.clear()` on reuse correctly drops stale chain data between invocations. The else branch (`isOpen=false`) already called `chain.reopen()`, so the `chain.clear()` addition only fires when the cursor is being reused.
- **isKeyColumn ordering quirk.** The bare FILL(PREV) branch at line 3401-3412 still uses `isKeyColumn(i, bottomUpCols, timestampIndex)` where `i` is a factory index but `bottomUpCols` is user order. Plan 05's rationale acknowledges this ("a wider refactor to alias-based key detection across all branches is out of scope"). The claim holds for the current behavior — no PREV spec is mis-assigned across columns. If `propagateTopDownColumns0` ever reorders bare-PREV targets such that the user-order key vs aggregate classification disagrees with factory-order, the classification would be wrong. Not a current bug; worth tracking.
