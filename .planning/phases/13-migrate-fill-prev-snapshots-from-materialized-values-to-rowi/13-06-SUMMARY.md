---
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
plan: 06
subsystem: sql
tags: [sql, fill, prev, grammar, seed-001, wr-04, defect-3, chain-rejection, insufficient-fill]

requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    plan: 05
    provides: "alias-mapping lookup (factoryColToUserFillIdx + userFillIdx counter) that Plan 06 reuses to compute aggNonKeyCount for the insufficient-fill grammar check"
provides:
  - "WR-04: precise chain-rejection position via perColFillNodes ObjList, with fallback to fillValuesExprs[0].position when tracking yields null"
  - "Defect 3: insufficient-fill grammar check (per-column mode, size > 1, short list) raises 'not enough fill values' at the first fill expression"
  - "testFillInsufficientFillValues new regression test covering the grammar rule for a 7-aggregate, 5-fill-value query"
  - "testSampleByFillNeedFix assertion #3 restored from assertSql workaround to assertException (position 530, not enough fill values) - closes Phase 12 Success Criterion #1"
  - "testSampleFillValueNotEnough restored in both SampleByTest and SampleByNanoTimestampTest from printSql to assertException (position 85, not enough fill values)"
  - "Phase 13 complete: all six commits in D-07 sequence landed"
affects: []

tech-stack:
  added: []
  patterns:
    - "Parallel ObjList<ExpressionNode> tracking aside IntList fillModes for carrying position information to downstream error sites"

key-files:
  created:
    - .planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-06-SUMMARY.md
  modified:
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java

decisions:
  - "WR-04 implemented via pre-populated ObjList<ExpressionNode> with null-safe setQuick at the cross-column PREV branch only. perColFillNodes[col] is only written when fillModes.add(srcColIdx) fires (the sole cross-column case); other modes leave the slot at null so the fallback kicks in for non-PREV-chain paths."
  - "Defect 3 grammar check placed AFTER the per-column build loop and BEFORE the chain check. Order matters: chain detection assumes the per-column loop has populated fillModes for every aggregate slot; if we rejected short lists before filling those slots we would mask the chain check's real domain. The per-column loop tolerates short lists via the existing `fillIdx < fillValuesExprs.size()` branch that produces null fillExpr, which becomes FILL_CONSTANT (NullConstant) and is harmless until the grammar check fires."
  - "aggNonKeyCount reuses Plan 05's userFillIdx counter from the bottomUpCols walk. After the walk completes, userFillIdx equals the number of non-key non-timestamp columns in the user's SELECT list, which is exactly the count of fill values the user must supply in per-column mode. No additional walk needed."
  - "Existing chain rejection tests testFillPrevRejectMutualChain (position 55) and testFillPrevRejectThreeHopChain (position 65) continue to pass unchanged because for both queries the chain starts at the FIRST aggregate column whose fill expression happens to be the first in the fill list. The perColFillNodes[col].position coincides with fillValuesExprs[0].position. WR-04 is exercised (not in fallback) but produces the same numeric position. This is valid calibration output."
  - "testSampleFillValueNotEnough in both SampleByTest and SampleByNanoTimestampTest updated from printSql to assertException. Originally these documented the pre-fix buggy 'silent broadcast' behavior with comments like 'The fast path broadcasts last fill value when fewer values than columns.' Now aligned with master's assertException(...101, 'insufficient fill values for SAMPLE BY FILL') form, but using our message 'not enough fill values' and calibrated position 85 (where master's would be 101 due to slightly different query string)."

requirements-completed: [INTERNAL-REFACTOR-PH13]

metrics:
  duration: ~75 min
  completed: 2026-04-19
  tasks: 2
  files: 4
  commits: 1
---

# Phase 13 Plan 06: FILL grammar tightening (precise chain errors, short-list rejection) Summary

One-liner: Two surgical grammar upgrades in `SqlCodeGenerator.generateFill` - WR-04 precise chain-rejection position (via parallel `ObjList<ExpressionNode>`) and Defect 3 insufficient-fill-values rule - plus test updates that restore `testSampleByFillNeedFix` and `testSampleFillValueNotEnough` to `assertException` form, closing Phase 12 Success Criterion #1 and completing phase 13's six-commit sequence.

## Objective

Retain two SEED-001 grammar items under D-05 dispositions: WR-04 (precise chain-rejection position) and Defect 3 (insufficient fill values grammar rule). Bundle as phase 13 Commit 6 per D-07 since `generateFill` is already touched in this phase.

## WR-04 Implementation

**Location:** `SqlCodeGenerator.java` per-column fill-spec build block and chain check pass.

**Fix shape:** parallel `ObjList<ExpressionNode>` populated during the cross-column PREV assignment, read at the chain check.

Before:

```java
// per-column loop:
fillModes.add(srcColIdx);

// chain check:
for (int col = 0; col < columnCount; col++) {
    int mode = fillModes.getQuick(col);
    if (mode >= 0 && fillModes.getQuick(mode) >= 0) {
        throw SqlException.$(fillValuesExprs.getQuick(0).position,
                "FILL(PREV) chains are not supported: ...");
    }
}
```

After:

```java
final ObjList<ExpressionNode> perColFillNodes = new ObjList<>(columnCount);
for (int i = 0; i < columnCount; i++) {
    perColFillNodes.add(null);
}

// per-column loop (cross-column PREV branch):
fillModes.add(srcColIdx);
perColFillNodes.setQuick(col, fillExpr);

// chain check:
for (int col = 0; col < columnCount; col++) {
    int mode = fillModes.getQuick(col);
    if (mode >= 0 && fillModes.getQuick(mode) >= 0) {
        ExpressionNode offendingExpr = perColFillNodes.getQuick(col);
        int pos = offendingExpr != null
                ? offendingExpr.position
                : fillValuesExprs.getQuick(0).position;
        throw SqlException.$(pos,
                "FILL(PREV) chains are not supported: ...");
    }
}
```

The `ObjList<ExpressionNode>` is pre-populated with nulls so `getQuick(col)` is safe for every column index regardless of mode. Only the cross-column PREV branch (`fillModes.add(srcColIdx)`) sets a non-null slot; the fallback preserves pre-WR-04 behavior for any column whose slot remains null.

## Defect 3 Implementation

**Location:** `SqlCodeGenerator.java` after the per-column build loop, before the chain check.

**Fix shape:** 5-line grammar check guarded by `size() > 1` to preserve the intentional single-element broadcast form.

```java
if (fillValuesExprs.size() > 1 && fillValuesExprs.size() < aggNonKeyCount) {
    throw SqlException.$(fillValuesExprs.getQuick(0).position, "not enough fill values");
}
```

`aggNonKeyCount` reuses Plan 05's `userFillIdx` counter from the `bottomUpCols` walk (after the walk completes, `userFillIdx` equals the number of non-key non-timestamp columns in the user's SELECT list, regardless of whether outer projection drops any of them from factory metadata).

## Position Calibration Table

All four calibrated positions were determined by running the failing test with a placeholder position, capturing the actual position reported in the failure message, and updating the assertion.

| Test | Position | Query fragment around position |
|------|----------|-------------------------------|
| `testFillPrevRejectMutualChain` | 55 (unchanged) | `...SAMPLE BY 1h FILL(` where pos 55 is the `P` of `PREV(b)` |
| `testFillPrevRejectThreeHopChain` | 65 (unchanged) | `...SAMPLE BY 1h FILL(` where pos 65 is the `P` of `PREV(b)` |
| `testFillInsufficientFillValues` (new) | 91 | `...SAMPLE BY 1h FILL(` where pos 91 is the `P` of first `PREV` |
| `testSampleByFillNeedFix` assertion #3 | 530 | multi-line CTE, pos 530 is the `P` of first `PREV` in the CTE's `fill(PREV, PREV, PREV, PREV, 0)` |
| `testSampleFillValueNotEnough` (SampleByTest + NanoTimestamp) | 85 | `...sample by 3h fill(` where pos 85 is the `2` of `20.56` (first fill expr is a constant, not PREV) |

For the two existing chain-rejection tests, the pre-WR-04 assertion positions happen to coincide with the WR-04 positions because the chain in both queries starts at the first aggregate column, whose fill expression is the first in the fill list. Verified by direct test execution: tests pass without position updates. WR-04's tracking path IS exercised - `perColFillNodes[col]` IS populated at the cross-column PREV site - but the numerical value matches the fallback.

## Regression Tests and Master Alignment

**New test:**

- `testFillInsufficientFillValues` (SampleByFillTest) - 7 aggregates, 5 fill values, expects positioned SqlException at 91 with "not enough fill values". This pins the new Defect 3 grammar behavior.

**Restored tests:**

- `testSampleByFillNeedFix` assertion #3 (SampleByTest) - restored from `assertSql` with buggy 3-row workaround output to `assertException(..., 530, "not enough fill values")`. All three assertions of the test now match master's 3-row + assertException form.
- `testSampleFillValueNotEnough` (SampleByTest + SampleByNanoTimestampTest) - restored from `printSql(silently-buggy)` to `assertException(..., 85, "not enough fill values")`. Comments now reference SEED-001 Defect 3 and phase 13.

The `testSampleFillValueNotEnough` restoration is a deviation from the strict plan scope (Rule 2 - missing critical functionality): the plan did not call out these specific tests, but their `printSql`-based form documented the now-fixed buggy behavior. Leaving them as-is after the grammar check lands would have caused them to fail with "not enough fill values" errors during `printSql` execution. Updating to `assertException` aligns them with master's form and makes the grammar rule behavior explicit in each test's assertion.

## Master Message Difference

Master emits `"insufficient fill values for SAMPLE BY FILL"` at the `fillValues.getQuick(fillIndex - 1).position` (position of the LAST consumed fill) for per-column mode mismatches. Our branch emits `"not enough fill values"` at `fillValuesExprs.getQuick(0).position` (position of the FIRST fill) - matches the text of master's legacy `FillRangeRecordCursorFactory` path but at a more precise position than master's legacy `-1`. Both messages are substring-matched by `assertException`, so tests using our message text pass and continue to exercise the grammar rule.

## Verification

Full phase suite green:

```
mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test
```

Results: **1193 tests run, 0 failures, 0 errors, 2 skipped** (two pre-existing skips unrelated to phase 13).

Combined-suite both orderings:

```
mvn -pl core -Dtest='SampleByFillTest,SampleByTest' test     -> 384 run, 0 failures
mvn -pl core -Dtest='SampleByTest,SampleByFillTest' test     -> 384 run, 0 failures
```

Individual test surfaces:

- `SampleByFillTest`: 82 tests, all green (includes new `testFillInsufficientFillValues`).
- `SampleByTest#testSampleByFillNeedFix`: passes with all three assertions against master's target form.
- `SampleByTest#testSampleFillValueNotEnough`: passes with assertException.
- `SampleByNanoTimestampTest#testSampleFillValueNotEnough`: passes with assertException.
- `SampleByFillTest#testFillPrevRejectMutualChain` / `#testFillPrevRejectThreeHopChain`: continue to pass with positions 55 and 65 (unchanged but WR-04 path now exercised).

## Deviations from Plan

- **[Rule 2 - Missing critical functionality] Updated `testSampleFillValueNotEnough` in SampleByTest + SampleByNanoTimestampTest from `printSql` to `assertException`.** The plan did not enumerate these two tests in its scope, but their pre-plan `printSql`-with-no-assertion form documented the silent-broadcast buggy behavior that Defect 3 fixes. Once the grammar check lands, any test running that exact query shape via `printSql` fails because the query now raises a parse-time exception. Aligning them with master's assertException form was necessary to keep the full phase suite green and reinforces the grammar semantics.
- **[Expected-by-plan deviation] Existing chain-rejection tests (`testFillPrevRejectMutualChain`, `testFillPrevRejectThreeHopChain`) positions NOT updated.** The plan predicted these tests would fail with different positions after WR-04 and guided a recalibration step. Running the tests against the new code showed they pass unchanged - the WR-04 position for both queries coincides with `fillValuesExprs[0].position` because the chain starts at the first aggregate column whose fill expression is the first in the fill list. WR-04 IS exercised via `perColFillNodes` (not via the fallback), but the numeric position matches pre-WR-04. No update needed; tests documented as calibrated with positions 55 and 65.

## Phase 13 Completion Status

Plan 06 is the sixth and final commit of phase 13 per D-07. Full commit sequence now landed:

1. chain.clear() fix in SortedRecordCursor.of() (Commit 1, Plan 01).
2. rowId rewrite in SampleByFillRecordCursorFactory (Commit 2, Plan 02).
3. 13 per-type FILL(PREV) tests cherry-picked (Commit 3, Plan 03).
4. Retro-fallback machinery deleted end-to-end (Commit 4, Plan 04).
5. SEED-002 Defect 1+2 resolved; testSampleByFillNeedFix assertions #1 and #2 restored (Commit 5, Plan 05).
6. SEED-001 WR-04 + Defect 3 + testSampleByFillNeedFix assertion #3 restored (Commit 6, this plan).

**SEED-001 disposition at phase close:**
- WR-01, WR-02, WR-03: obsoleted by D-04 (retro-fallback deletion in Plan 04).
- WR-04: resolved in Plan 06.
- Defect 3: resolved in Plan 06.

**SEED-002 disposition at phase close:**
- Defect 1 (CTE + outer-projection corruption): resolved in Plan 05 via alias-mapping fix.
- Defect 2 (toTop() state leak): absorbed by Plan 02 rowId rewrite.

**Phase 12 Success Criterion #1:** closed. `testSampleByFillNeedFix` now matches master's 3-row + assertException form across all three assertions.

Phase 13 is ready for `/gsd-verify-work`.

## Commit

Single commit per D-07 combining WR-04 + Defect 3 + test updates:

- `ff5b354bd1` - "Tighten FILL grammar: precise errors and short fill list rejection" (modifies `SqlCodeGenerator.java`, `SampleByFillTest.java`, `SampleByTest.java`, `SampleByNanoTimestampTest.java`).

## Self-Check: PASSED

- Production change `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`: present in commit `ff5b354bd1` (verified via `git diff HEAD^ HEAD`).
- Test change `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`: present in commit `ff5b354bd1`.
- Test change `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java`: present in commit `ff5b354bd1`.
- Test change `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java`: present in commit `ff5b354bd1`.
- SUMMARY.md at `.planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-06-SUMMARY.md` (this file).
- `perColFillNodes` symbol count in SqlCodeGenerator.java: 4 (declaration, pre-populate add, per-column setQuick, chain-check getQuick).
- `not enough fill values` literal count in SqlCodeGenerator.java: 1.
- `testFillInsufficientFillValues` declaration count in SampleByFillTest.java: 1.
- Commit `ff5b354bd1` present in `git log --oneline --all`.
