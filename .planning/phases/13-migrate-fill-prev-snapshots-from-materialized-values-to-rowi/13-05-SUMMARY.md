---
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
plan: 05
subsystem: sql
tags: [sql, fill, prev, defect, seed-002, toTop, cte, outer-projection, reordering]

requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    plan: 02
    provides: "rowId-based prev snapshot that absorbs Defect 2 (toTop() state leak) by replacing simplePrev[] flag arrays with a single simplePrevRowId reset"
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    plan: 04
    provides: "Retro-fallback deletion unblocks all PREV types on the fast path, which is what exposes the CTE+outer-projection Defect 1 end-to-end in testSampleByFillNeedFix"
provides:
  - "generateFill no longer assigns fill specs positionally over factory metadata; a precomputed factoryColToUserFillIdx lookup walks bottomUpCols in user SELECT order and maps each aggregate to its factory index via alias"
  - "Key and timestamp detection in the lookup walk uses the QueryColumn AST directly (LITERAL vs timestamp_floor) instead of isKeyColumn(factoryIdx, bottomUpCols, ...), which read bottomUpCols at a factory index and produced wrong answers whenever the two orders diverged"
  - "Aggregates dropped by the outer projection still advance userFillIdx, so downstream aggregates land on the correct fill value instead of silently shifting"
  - "testSampleByFillNeedFix assertion #1 and #2 restored to master's 3-row assertQuery form with sizeExpected=true"
  - "Plan 06 unblocked to restore assertion #3 to master's assertException form"
affects:
  - plan-06-seed-001-grammar

tech-stack:
  added: []
  patterns:
    - "Alias-based factory-to-user index lookup as a general pattern for per-aggregate spec assignment in the presence of outer-projection column pruning and DFS-driven column reordering"

key-files:
  created:
    - .planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-05-SUMMARY.md
  modified:
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java

decisions:
  - "Branch C fired in Task 2's branching logic: Defect 1 (CTE+outer-projection corruption) was NOT absorbed by Plan 02's rowId rewrite. Applied the user-approved Option D targeted fix in generateFill rather than deferring to a follow-up phase, honouring CONTEXT.md D-06's 'end of this phase should be a full solution' commitment."
  - "Scoped the fix to generateFill's per-column loop only. Did NOT touch bare FILL(PREV) (line ~3397-3412, no user-fill-idx dependency) and did NOT touch the chain-rejection pass (reads fillModes which stays column-indexed). SqlOptimiser.propagateTopDownColumns0 is the upstream reorder cause but is not modified — the fix is surgical at the consumer."
  - "Key/timestamp detection rewritten to read QueryColumn AST directly in the bottomUpCols walk. isKeyColumn(col, bottomUpCols, timestampIndex) is still called by the bare FILL(PREV) branch where col iterates factory order; that branch does not consume a user fill index so the pre-existing semantics are preserved there (a wider refactor to alias-based key detection across all branches is out of scope for this commit)."
  - "Assertion #3's assertSql snapshot updated in lockstep with the fix. The previous snapshot captured the pre-fix buggy positional assignment and no longer matches the now-correct semantic output. Plan 06 will replace the assertSql with master's assertException form that rejects the query outright."

requirements-completed: [INTERNAL-REFACTOR-PH13]

metrics:
  duration: ~65 min
  completed: 2026-04-19
  tasks: 2
  files: 2
  commits: 1
---

# Phase 13 Plan 05: testSampleByFillNeedFix restoration and CTE+outer-projection fill mapping fix Summary

One-liner: Targeted alias-mapping fix in `SqlCodeGenerator.generateFill` that replaces a positional running counter with a factory-to-user-fill-idx lookup, closing SEED-002 Defect 1 (CTE + outer-projection corruption) and restoring `testSampleByFillNeedFix` assertions #1 and #2 to master's 3-row `assertQuery` form.

## Branch Disposition

Branch C of Task 2 fired. Plan 02's rowId rewrite absorbed SEED-002 Defect 2 (toTop() state leak) — assertion #1 passed as soon as the restoration landed. Defect 1 (CTE + outer-projection corruption) was NOT absorbed: assertion #2 kept producing 6 rows with alternating `cnt` values per bucket, exactly the symptom documented in SEED-002. The user approved Option D — a targeted alias-mapping fix in `generateFill` — and committed the test restoration + production fix as a single commit per D-07.

## Root Cause

`SqlOptimiser.propagateTopDownColumns0` (SqlOptimiser.java ~line 5881-6075) propagates the outer query's column references into inner models via a DFS walk. For `SELECT_MODEL_GROUP_BY` nodes it promotes the outer-requested columns plus all non-aggregate bottom-up columns (keys, timestamp literals) into `topDownColumns`. `QueryModel.getColumns()` returns `topDownColumns` when non-empty (QueryModel.java:716), so `GroupByUtils.assembleGroupByFunctions` iterates the DFS-reordered column list when building `outerProjectionMetadata`. Aggregate columns that the outer projection does NOT reference are not promoted — `first(candle_open_price) AS open` in the CTE, for example, is dropped entirely from the factory metadata when the outer only selects `candle_start_time, cnt`.

`generateFill` (SqlCodeGenerator.java ~line 3253) iterates the factory metadata column-by-column and, in the per-column fill-spec branch, used a running `fillIdx` counter keyed to factory-order position. The counter advanced once per non-key, non-timestamp factory column. Two distinct failures followed:

1. When outer DFS reordered the factory metadata (aggregates moved relative to keys/timestamp), the positional counter assigned user fill values to the wrong factory column. This is the reorder flavour of Defect 1.

2. When outer DFS dropped an aggregate column entirely, the counter silently re-aligned to the next kept aggregate, shifting every downstream fill value by one slot. This is the drop flavour of Defect 1 and is what `testSampleByFillNeedFix` assertion #2 hits (`FILL(PREV, 0)` with `open` dropped from factory metadata — `cnt` then received the `PREV` value that was intended for `open` instead of the `0` the user wrote).

GROUP BY code gen consumed the DFS reorder/drop correctly because it iterates `parentModel.getColumns()` (topDown-reordered) for projection function assembly. Only `generateFill` still used positional factory-order assignment against a user-order fill-value list.

## Fix Location and Line Numbers

`core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`, `generateFill` method, per-column fill-spec build block:

- Lookup construction (new): lines ~3426-3454.
- Per-column loop (modified): lines ~3456-3538.
- Bare `FILL(PREV)` block unchanged (lines ~3397-3412). No positional fill-idx dependency.
- Chain-rejection pass unchanged (lines ~3540-3547). Operates on `fillModes` which stays column-indexed.

## Before / After Diff Excerpt

Before:

```java
// Per-column fill spec — key columns get FILL_KEY and skip fillIdx
int fillIdx = 0;
for (int col = 0; col < columnCount; col++) {
    if (col == timestampIndex) { ...; continue; }
    if (isKeyColumn(col, bottomUpCols, timestampIndex)) { ...; continue; }
    ExpressionNode fillExpr = fillIdx < fillValuesExprs.size()
            ? fillValuesExprs.getQuick(fillIdx)
            : (fillValuesExprs.size() == 1 ? fillValuesExprs.getQuick(0) : null);
    ...
    fillIdx++;
}
```

After:

```java
// Precompute lookup: factoryColIndex -> userFillIdx by walking bottomUpCols in user order.
final IntList factoryColToUserFillIdx = new IntList(columnCount);
for (int i = 0; i < columnCount; i++) factoryColToUserFillIdx.add(-1);
int userFillIdx = 0;
for (int i = 0, n = bottomUpCols.size(); i < n; i++) {
    final QueryColumn qc = bottomUpCols.getQuick(i);
    final ExpressionNode ast = qc.getAst();
    if (ast.type == ExpressionNode.LITERAL) continue;                   // key column
    if (SqlUtil.isTimestampFloorFunction(ast)) continue;                // timestamp
    final CharSequence qcAlias = qc.getAlias();
    if (qcAlias != null) {
        final int factoryIdx = groupByMetadata.getColumnIndexQuiet(qcAlias);
        if (factoryIdx >= 0 && factoryIdx != timestampIndex) {
            factoryColToUserFillIdx.setQuick(factoryIdx, userFillIdx);
        }
    }
    userFillIdx++;   // advance even if dropped by outer projection
}

for (int col = 0; col < columnCount; col++) {
    if (col == timestampIndex) { ...; continue; }
    final int fillIdx = factoryColToUserFillIdx.getQuick(col);
    if (fillIdx < 0) { fillModes.add(FILL_KEY); ...; continue; }
    ExpressionNode fillExpr = fillIdx < fillValuesExprs.size()
            ? fillValuesExprs.getQuick(fillIdx)
            : (fillValuesExprs.size() == 1 ? fillValuesExprs.getQuick(0) : null);
    ...
    // no fillIdx++ — the lookup is the source of truth
}
```

## testSampleByFillNeedFix Before / After

| Assertion | Before (our branch, pre-fix)                                                               | After (this commit)                                                                          |
| --------- | ------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| #1        | assertQuery, 3 rows, gap row `high=0.0`, `candle_volume=0.0`, `candle_usd_volume=0.0`, `cnt=216` (workaround state) | assertQuery, 3 rows, gap row `high=0.00128`, `candle_volume=0.0`, `candle_usd_volume=0.0`, `cnt=0` (matches master exactly) |
| #2        | assertSql, 6 rows with alternating `cnt` per bucket (Defect 1 workaround)                  | assertQuery, 3 rows, gap row `cnt=0` (matches master exactly)                                |
| #3        | assertSql, 3 rows, gap row high=0.0, vol=null, usd=null, cnt=216 (pre-fix buggy snapshot)  | assertSql, 3 rows, gap row high=0.0013, vol=0.0, usd=null, cnt=null (post-fix correct snapshot; Plan 06 converts to assertException) |

Assertion #1 and #2 now use master's 3-row `assertQuery` form with `sizeExpected=true` (which forces `toTop()` double-iteration and verifies state reset). Assertion #3 remains in workaround state until Plan 06; the expected snapshot updates track the now-correct semantic output of the 5-fills-for-7-aggregates case.

## SEED-002 Closure Status

| Defect   | Description                                              | Status                                                  |
| -------- | -------------------------------------------------------- | ------------------------------------------------------- |
| Defect 1 | CTE + outer-projection produces wrong output             | RESOLVED (this commit, alias-mapping fix in generateFill) |
| Defect 2 | toTop() state leak on sizeExpected=true double-iteration | RESOLVED (Plan 02 rowId rewrite, absorbed into simplePrevRowId = -1L single reset) |
| Defect 3 | fill values count < aggregate count silently padded      | Plan 06 (restores master's assertException)             |

SEED-002 Defects 1 and 2 are fully resolved per CONTEXT.md D-06's "end of this phase should be a full solution" commitment. Phase 12 Success Criterion #1, previously deferred via Option A, closes with this commit.

## Plan 06 Unblock Confirmation

Plan 06 is unblocked. Assertion #3 now reads `assertSql` with the post-fix snapshot; Plan 06's scope is to replace it with master's `assertException(..., 554, "not enough fill values")` alongside the matching changes in `testFillInsufficientFillValues` and its underlying grammar-level rejection in `generateFill` / `assembleGroupByFunctions`. No prerequisite work from this plan blocks that replacement — the positional counter that silently padded is gone, but the grammar-level rejection is a separate code path that Plan 06 restores.

## Verification

- `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` — BUILD SUCCESS, 1/1 pass.
- `mvn -pl core -Dtest=SampleByTest test` — BUILD SUCCESS, 302/302 pass.
- `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` — BUILD SUCCESS, 1190/1192 pass, 2 pre-existing skipped (unrelated).

## Commit

Single commit per D-07 combining test restoration + production fix:

- `b8a2bee9ee` — "Fix FILL spec mapping for reordered aggregates" (modifies `SqlCodeGenerator.java` and `SampleByTest.java`).

## Deviations from Plan

- **[Rule 2 - Missing Critical Functionality] Assertion #3 snapshot update.** The plan says "Do NOT touch assertion #3 here — Plan 06 handles it", but my fix changed the OBSERVED output of assertion #3 because the positional counter bug was silently compensating the outer-projection drop case with buggy assignments that happened to land on old-snapshot values. Once the fix assigns fill values in correct user order, assertion #3's actual output changes from `{high=0.0, vol=null, usd=null, cnt=216}` (pre-fix buggy) to `{high=0.0013, vol=0.0, usd=null, cnt=null}` (post-fix correct). The assertSql snapshot had to update in lockstep. The comment block above the assertSql documents the update and the Plan 06 follow-up. This is not a scope expansion — assertion #3 remains in workaround state; its snapshot just now captures semantically correct output rather than incidentally correct output.

## Self-Check: PASSED

- Production change `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`: present in commit `b8a2bee9ee` (verified via `git diff HEAD^ HEAD`).
- Test change `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java`: present in commit `b8a2bee9ee`.
- Commit `b8a2bee9ee` present in `git log --oneline -5`.
- SUMMARY.md at `.planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-05-SUMMARY.md` (this file).
