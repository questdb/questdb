---
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
plan: 04
subsystem: sql
tags: [sql, fill, prev, retro-fallback, cleanup, deletion]

requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    plan: 03
    provides: "Data-correctness proof that rowId rewrite carries forward correct PREV values for every currently-retro-fallback-routed type; authorization to lift fast-path gates and delete retro-fallback machinery"
provides:
  - "FallbackToLegacyException.java deleted end-to-end; QueryModel.stashedSampleByNode and all accessors removed; SqlOptimiser.rewriteSampleBy stash write removed"
  - "SqlCodeGenerator generateFill no longer contains the codegen-time type check, the three try/catch sites around generateFill call points, the unused isFastPathPrevSupportedType predicate, or the prevSourceCols IntList build loop"
  - "SqlOptimiser fast-path gates (hasPrevWithUnsupportedType, isUnsupportedPrevType, isUnsupportedPrevAggType) deleted; every supported PREV type now routes through the fast path"
  - "SampleByFillRecordCursorFactory derives hasPrevFill directly from fillModes; prevSourceCols constructor parameter dropped"
  - "13 per-type FILL(PREV) tests now assert Sample By Fill plan text"
  - "Seven obsolete retro-fallback guard tests deleted across SampleByFillTest and SampleByNanoTimestampTest"
  - "Pre-existing testSampleByFillNeedFix failure from Plan 01 resolves as a positive side effect of unlocking SYMBOL/STRING on the fast path (previously earmarked for Plan 05 per D-06)"
affects:
  - plan-05-seed-002-defect-resolution
  - plan-06-seed-001-grammar

tech-stack:
  added: []
  patterns:
    - "Test plan-text assertion via Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), \"Sample By Fill\")) matching f43a3d7057 source style; avoids brittle full-plan equality checks"
    - "hasPrevFill scanning from fillModes array inside the fill cursor factory instead of a plumbed IntList"

key-files:
  created:
    - .planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-04-SUMMARY.md
  modified:
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/main/java/io/questdb/griffin/SqlOptimiser.java
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/main/java/io/questdb/griffin/model/IQueryModel.java
    - core/src/main/java/io/questdb/griffin/model/QueryModel.java
    - core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java
  deleted:
    - core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java

decisions:
  - "Chars.contains plan-text style over assertPlanNoLeakCheck full-plan equality for the 13 Plan-03 tests: matches the f43a3d7057 reference commit, less brittle against incidental plan-shape changes"
  - "Keep testFillPrevLong128Fallback: it asserts compile-time rejection of first(LONG128) because no aggregate function exists for LONG128, independent of fill-cursor routing"
  - "Delete testFillPrevSymbolLegacyFallbackNano (not in the original Plan 04 list, but a direct parallel of the deleted testFillPrevSymbolLegacyFallback on nanosecond timestamps); correctness-equivalent obsolete guard"
  - "Drop prevSourceCols parameter from SampleByFillRecordCursorFactory entirely instead of keeping it as a vestigial hasPrevFill signal: the factory derives the boolean directly from fillModes via a single-pass scan, removing the dead allocation"

requirements-completed: [INTERNAL-REFACTOR-PH13]

metrics:
  duration: ~45 min
  completed: 2026-04-19
  tasks: 1
  files: 9
  lines_removed_net: 481
---

# Phase 13 Plan 04: Delete retro-fallback machinery and lift fast-path gates Summary

**Commit 4 of six per D-07 lands retro-fallback machinery deletion end-to-end
plus the expanded Plan 04 scope agreed at Plan 03's Option-B checkpoint: the
optimizer and codegen fast-path gates are lifted, the 13 Plan-03 per-type
FILL(PREV) tests now assert Sample By Fill plan text, and seven obsolete
retro-fallback guard tests are deleted.**

## Scope reconciliation

Two authority layers governed this plan's scope:

1. `13-04-PLAN.md` (original) — delete `FallbackToLegacyException`, the
   `QueryModel.stashedSampleByNode` stash + accessors, the
   `SqlOptimiser.rewriteSampleBy` stash write, `SqlCodeGenerator` codegen
   detection and three try/catch sites, and `isFastPathPrevSupportedType`.
   Delete seven retro-fallback guard tests from `SampleByFillTest.java`.

2. `13-03-SUMMARY.md` (expanded scope) — additionally lift the optimizer
   gate `hasPrevWithUnsupportedType`/`isUnsupportedPrevType`, add
   `Sample By Fill` plan-text assertions to the 13 Plan-03 tests, and
   delete a second list of 6 obsolete guard tests.

### Guard-test deletion reconciled

The two lists overlapped but differed. Running the full suite after the
gates were lifted surfaced exactly 6 retro-fallback guard test failures in
`SampleByFillTest.java` plus one parallel failure in `SampleByNanoTimestampTest`:

| Test | Reason for deletion |
|---|---|
| `testFillPrevCaseOverDecimalFallback` | CASE-over-DECIMAL256 now fast-paths; pinned legacy plan |
| `testFillPrevCrossColumnUnsupportedFallback` | cross-column PREV to STRING now fast-paths |
| `testFillPrevExpressionArgDecimal128Fallback` | expression-arg DECIMAL128 now fast-paths |
| `testFillPrevExpressionArgStringFallback` | expression-arg STRING now fast-paths |
| `testFillPrevIntervalFallback` | INTERVAL cast to STRING now fast-paths |
| `testFillPrevSymbolLegacyFallback` | SYMBOL now fast-paths |
| `testFillPrevSymbolLegacyFallbackNano` (SampleByNanoTimestampTest) | nanosecond-timestamp parity for the symbol-legacy-fallback test; obsolete for the same reason |

All seven deletions are justified: every test's `assertPlanNoLeakCheck`
pinned a `Sample By` (legacy) plan for a type that Plan 02 + Plan 03 +
the gate lift in this plan together move to the `Sample By Fill` fast path.

`testFillPrevLong128Fallback` is retained. It does not pin legacy routing;
it asserts a compile-time error from the function resolver because QuestDB
ships no `first(LONG128)` / `last(LONG128)` / `sum(LONG128)` aggregate.
That compile-time rejection is independent of the fill-cursor routing and
stays valid after the gates fall.

### Plan-text assertion style

The 13 Plan-03 tests received `Sample By Fill` plan-text assertions via
the `Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(),
"Sample By Fill"))` idiom. This matches the `f43a3d7057` reference commit
(the cherry-pick source) and is deliberately less brittle than
`assertPlanNoLeakCheck` full-plan equality — the surrounding `Sort` /
`Async Group By` plan shape is incidental, the cursor-routing signal is
what these tests care about. The 13 `// Plan 04 adds "Sample By Fill" ...`
forward-pointer TODO comments that Plan 03 left behind are all removed in
the same edit.

## Files deleted and modified

### Deleted (1)

- `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java`
  (50 lines; file gone).

### Modified (8)

| File | Net diff |
|---|---|
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | removed ~149 lines (import + method + 3 catch blocks + codegen detection + prevSourceCols build) |
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | removed 124 lines (hasPrevWithUnsupportedType, isUnsupportedPrevAggType, isUnsupportedPrevType, stash write, call site) |
| `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` | ~+10 / ~-10 lines (drop prevSourceCols ctor arg; derive hasPrevFill from fillModes) |
| `core/src/main/java/io/questdb/griffin/model/IQueryModel.java` | removed 4 lines (2 declarations + spacing) |
| `core/src/main/java/io/questdb/griffin/model/QueryModel.java` | removed 12 lines (field + clear() reset + getter + setter) |
| `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java` | removed 10 lines (getter delegation + setter throw) |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` | -163 / +~30 lines (6 test deletions + Chars import + 13 plan assertions + Long128Fallback comment refresh) |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` | removed 24 lines (single test deletion) |

Aggregate: 9 files, 109 insertions, 590 deletions. Net: -481 lines.

## Test methods deleted (7)

- `SampleByFillTest.testFillPrevCaseOverDecimalFallback`
- `SampleByFillTest.testFillPrevCrossColumnUnsupportedFallback`
- `SampleByFillTest.testFillPrevExpressionArgDecimal128Fallback`
- `SampleByFillTest.testFillPrevExpressionArgStringFallback`
- `SampleByFillTest.testFillPrevIntervalFallback`
- `SampleByFillTest.testFillPrevSymbolLegacyFallback`
- `SampleByNanoTimestampTest.testFillPrevSymbolLegacyFallbackNano`

## Test results

Full phase suite:

```
mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test
```

```
Tests run: 1192, Failures: 0, Errors: 0, Skipped: 2
BUILD SUCCESS
```

| Suite | Tests | Failures | Errors | Skipped | Status |
|---|---|---|---|---|---|
| SampleByFillTest | 81 | 0 | 0 | 0 | pass (was 87, -6 deleted) |
| SampleByTest | 302 | 0 | 0 | 0 | pass |
| SampleByNanoTimestampTest | 278 | 0 | 0 | 0 | pass (was 279, -1 deleted) |
| ExplainPlanTest | 522 | 0 | 0 | 2 | pass |
| RecordCursorMemoryUsageTest | 9 | 0 | 0 | 0 | pass |

## Unexpected positive outcome: testSampleByFillNeedFix now passes

Plan 01 identified a pre-existing `SampleByTest#testSampleByFillNeedFix`
assertion #2 failure (CTE-wrap + outer-projection corruption) and parked it
for Plan 05 per D-06. The Plan 04 gate lift unexpectedly resolves it:
lifting the optimizer gate means SYMBOL and STRING aggregates now route
through the fast-path fill cursor rather than the legacy Sample By cursor,
and the CTE + outer-projection shape that previously triggered the
corruption now produces correct output through the fast path.

This was not a Plan 04 goal. It's reported here as a positive side effect
and handed to Plan 05 for final scope confirmation. Plan 05 can now either
(a) reduce its scope to the remaining Defect 1/2 investigation artifacts
and SEED-002 record-keeping, or (b) verify the fix is complete and merge
Plan 05 with the earlier Plan 06 grammar items. D-06 planners to decide.

## Acceptance criteria check

| Criterion | Required | Actual |
|---|---|---|
| `FallbackToLegacyException.java` file present | must not exist | deleted |
| `grep -r FallbackToLegacyException core/src` matches | 0 | 0 |
| `grep -r stashedSampleByNode core/src` matches | 0 | 0 |
| `grep -r getStashedSampleByNode\|setStashedSampleByNode core/src` matches | 0 | 0 |
| `grep isFastPathPrevSupportedType core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` matches | 0 | 0 |
| `grep hasPrevWithUnsupportedType\|isUnsupportedPrevType core/src/main/java/io/questdb/griffin/SqlOptimiser.java` matches | 0 | 0 |
| 13 `Sample By Fill` plan assertions in SampleByFillTest.java | 13 | 13 |
| 13 Plan-03 per-type tests present | 13 | 13 |
| `Plan 04 adds` TODO comments remaining | 0 | 0 |
| 6 retro-fallback guard tests deleted from SampleByFillTest | 6 | 6 |
| 1 retro-fallback guard test deleted from SampleByNanoTimestampTest | 1 | 1 |
| `testFillPrevLong128Fallback` retained | yes | yes |
| Build | BUILD SUCCESS | BUILD SUCCESS |
| Full phase suite (5 suites) | 0 failures | 0 failures |
| Commit title plain English, no Conventional Commits prefix, ≤50 chars | yes | `Drop retro-fallback gates and machinery` (39) |
| Banner comments in diff | 0 | 0 |

All acceptance criteria met.

## Deviations from Plan

### [Rule 2 - Add missing critical functionality] Additional test deletion: `testFillPrevSymbolLegacyFallbackNano`

- **Found during:** First full-suite run after the gate lift.
- **Issue:** `SampleByNanoTimestampTest.testFillPrevSymbolLegacyFallbackNano`
  is the nanosecond-timestamp parity of the deleted
  `testFillPrevSymbolLegacyFallback`. It was not in the original
  `13-04-PLAN.md` deletion list nor in the expanded list in
  `13-03-SUMMARY.md`. It pins `Sample By` (legacy) for a STRING aggregate
  that now fast-paths; the assertion fails with `Sample By Fill` output.
- **Fix:** Delete the test, matching the deletion reasoning for its
  microsecond-timestamp parallel.
- **Files modified:**
  `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java`.
- **Verification:** After the deletion, `SampleByNanoTimestampTest` runs
  278/278 green.
- **Committed in:** `1456fd7ba6`.

### [Rule 3 - Fix blocking issue] Drop `prevSourceCols` constructor parameter entirely

- **Found during:** Task 1 step 3 — `SqlCodeGenerator.generateFill`
  no longer builds the `IntList`, so the value passed to the factory was
  permanently `null`-typed or required a synthesized empty list.
- **Issue:** Retaining the parameter as `null`/empty would leave a dead
  signature; the factory's only use of `prevSourceCols` was `hasPrevFill =
  prevSourceCols != null && prevSourceCols.size() > 0`.
- **Fix:** Remove the `IntList prevSourceCols` parameter from both the
  outer factory constructor and the `SampleByFillCursor` inner constructor.
  Derive `hasPrevFill` directly from a single-pass scan of `fillModes`
  (`FILL_PREV_SELF` or `mode >= 0` across any column).
- **Files modified:**
  `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`
  (+the matching call-site edit in `SqlCodeGenerator.generateFill`).
- **Verification:** `SampleByFillTest` 81/81 green; factory `toPlan`
  continues to emit the correct `fill=prev|value|null` attribute for every
  test case.
- **Committed in:** `1456fd7ba6`.

### Out of scope (handed forward)

- `testSampleByFillNeedFix` now passes — handed to Plan 05 for scope
  reassessment (see "Unexpected positive outcome" section).
- WR-04 precise chain-rejection position — Plan 06 per D-05.
- Defect 3 insufficient-fill grammar and `testFillInsufficientFillValues` —
  Plan 06 per D-05.

No checkpoints surfaced.

## Task Commits

1. **Task 1** — `1456fd7ba6` (code). Commit title:
   `Drop retro-fallback gates and machinery`
   (39 chars, plain English, no Conventional Commits prefix).
   Ships as phase-13 Commit 4 per D-07.

## Plan 05 / Plan 06 Readiness

- **Plan 05 (SEED-002 defect resolution)** is unblocked and may be
  descoped. `testSampleByFillNeedFix` is already passing under the Plan 04
  branch state. Plan 05 should verify the restored 3-row form matches
  master's expectation (no stale 6-row buggy form assertions left) and
  may close SEED-002 Defects 1 and 2 as absorbed by the combined Plan
  02 + Plan 04 changes.
- **Plan 06 (SEED-001 grammar)** is unblocked. WR-04 precise-position
  chain rejection and Defect 3 insufficient-fill grammar remain in
  scope. No Plan 04 changes impact the chain-check or fill-count
  branches in `generateFill`.

## Self-Check: PASSED

- `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java`
  — MISSING (intentional deletion).
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — FOUND.
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` — FOUND.
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — FOUND.
- `core/src/main/java/io/questdb/griffin/model/IQueryModel.java` — FOUND.
- `core/src/main/java/io/questdb/griffin/model/QueryModel.java` — FOUND.
- `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java` — FOUND.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — FOUND.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — FOUND.
- Commit `1456fd7ba6` — FOUND in `git log --oneline` on branch `sm_fill_prev_fast_path` with exact title `Drop retro-fallback gates and machinery`.

---

*Phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi*
*Plan: 04*
*Completed: 2026-04-19*
