---
phase: 15-address-pr-6946-review-findings-and-retro-fixes
plan: 01
subsystem: sql-codegen
tags: [sample-by, fill, timestamp, codegen, unit-conversion]

requires:
  - phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-
    provides: "Moderate-findings baseline; Phase 14 D-15 assertQueryNoLeakCheck(false,false) contract; Mn-13 fillValues cleanup invariant"
provides:
  - "TIMESTAMP fill constants on the fast path parse via the target column's TimestampDriver"
  - "Unquoted numeric fill values for TIMESTAMP targets rejected at codegen with master's positioned error"
  - "timestampIndex resolution rejects non-TIMESTAMP columns on alias and fallback paths"
  - "Four restored pinning tests (micro and nano twins of testTimestampFillNullAndValue, testTimestampFillValueUnquoted)"
affects: [plan-02-cursor-cluster, plan-03-test-only-m7]

tech-stack:
  added: []
  patterns:
    - "Driver-aware codegen-time re-parse via TimestampConstant.newInstance(driver.parseQuotedLiteral(token), type)"
    - "ColumnType.isTimestamp guards on both alias-path and fallback-path timestamp-index resolution (broader-than-D-10 defensive margin)"

key-files:
  created: []
  modified:
    - "core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java"
    - "core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java"

key-decisions:
  - "Mirror legacy cursor path (SampleByFillValueRecordCursorFactory.createPlaceHolderFunction:144-173) TIMESTAMP branch verbatim in generateFill per CONTEXT D-03; same error wording, same exception positions"
  - "Broader M-5 fix covers both alias-path and fallback-path guards per RESEARCH Open Question 1 recommendation; CONTEXT D-10 only mandates the fallback guard, but the alias-path reset provides defensive margin against SELECT ts::LONG AS ts shadowing"
  - "Misc.free on stale fillValues slot runs before setQuick replaces it so the existing ownership transfer to constantFillFuncs picks up the unit-correct TimestampConstant"
  - "Tasks 1+2+3 landed as a single commit per CONTEXT D-02 (tests and production fix inside plan scope, no artificial split)"

patterns-established:
  - "Codegen-time driver-aware re-parse for TIMESTAMP FILL constants (extends Phase 14 M-4 wrap, joins the TimestampConstant.newInstance precedent stack at SqlCodeGenerator:7261, FunctionParser:1471, SampleByFillValueRecordCursorFactory:165)"
  - "ColumnType.isTimestamp gate for any timestampIndex anchor on the fill grid (applies to both getColumnIndexQuiet outputs, not just the fallback)"

requirements-completed: [COR-01, COR-02, COR-03, COR-04, FILL-02]

duration: 13min
completed: 2026-04-21
---

# Phase 15 Plan 01: Codegen cluster Summary

**TIMESTAMP FILL constants on the fast path parse via the target column's TimestampDriver, unquoted numeric tokens are rejected at codegen with master's positioned error, and the timestampIndex resolution refuses non-TIMESTAMP columns on both alias and fallback paths**

## Performance

- **Duration:** 13 min
- **Started:** 2026-04-21T14:03:56Z
- **Completed:** 2026-04-21T14:16:45Z
- **Tasks:** 3 (C-1+C-2 unified fix, M-5 guard, four pinning tests restored)
- **Files modified:** 3

## Accomplishments

- Closed C-1 (TIMESTAMP fill constant 1000x unit drift) — generateFill per-column FILL_CONSTANT branch now re-parses quoted TIMESTAMP literals through the target column's TimestampDriver before the ownership transfer to constantFillFuncs.
- Closed C-2 (silent unquoted-numeric acceptance for TIMESTAMP fill values) — Chars.isQuoted gate throws `"Invalid fill value: '1236'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'"` at fillExpr.position, verbatim with the legacy cursor path's message at SampleByFillValueRecordCursorFactory:161-163.
- Closed M-5 (timestampIndex fallback resolves to non-TIMESTAMP) — alias-path and fallback-path resolutions both refuse non-TIMESTAMP columns via ColumnType.isTimestamp; the existing skip-fill guard at the downstream `if (timestampIndex < 0)` block returns the factory unchanged rather than crashing in getTimestampDriver.
- Restored four pinning tests: SampleByTest.testTimestampFillNullAndValue (8 fill rows restored to `2019-02-03T12:23:34.123456Z`), SampleByTest.testTimestampFillValueUnquoted (printSql ghost replaced with assertException at position 66), SampleByNanoTimestampTest.testTimestampFillValueUnquoted (nano twin of the same restoration), SampleByNanoTimestampTest.testTimestampFillNullAndValue (verified passes unchanged — nano twin was already unit-correct).

## Task Commits

Plan 01 lands as a single atomic commit (per CONTEXT D-02: tests and production fix in the same commit inside plan scope):

1. **Fix TIMESTAMP fill constant unit drift** — `9df205bac5` (fix)
   - SqlCodeGenerator.java generateFill — C-1, C-2 (TIMESTAMP-target FILL_CONSTANT branch), M-5 (timestampIndex alias+fallback guards)
   - SampleByTest.java — testTimestampFillNullAndValue expected output restored + testTimestampFillValueUnquoted switched to assertException(pos 66)
   - SampleByNanoTimestampTest.java — testTimestampFillValueUnquoted switched to assertException(pos 66)

_Note: tests and production fix land together because the restored assertions require the codegen fix to pass and the codegen fix has no standalone test shape that does not use the restored tests as its pinning harness._

## Files Created/Modified

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — generateFill per-column FILL_CONSTANT branch adds a TIMESTAMP-target re-parse gate (C-1+C-2) at `:3620-3655`; timestampIndex resolution adds ColumnType.isTimestamp guards on both the alias-path result and the fallback-path origIndex at `:3330-3351` (M-5).
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — testTimestampFillNullAndValue's 8 fill-row entries restored from the wrong-unit `51062-02-01T08:48:43.456000Z` to master's `2019-02-03T12:23:34.123456Z` at `:17687-17705`. testTimestampFillValueUnquoted converted from printSql ghost to `assertException(sql, 66, "Invalid fill value: '1236'. Timestamp fill value must be in quotes.")` at `:17718-17730`.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — testTimestampFillValueUnquoted converted from printSql ghost to the same assertException shape at `:14236-14247`. testTimestampFillNullAndValue was already unit-correct (nano driver's parseQuotedLiteral is identity for a NANO-target constant); no edit.

## Decisions Made

- Mirror the legacy cursor path verbatim per CONTEXT D-03. The C-1+C-2 branch preserves the legacy `createPlaceHolderFunction` TIMESTAMP sub-branch's error wording, exception positions, and NumericException catch shape. Rationale: the legacy path is the canonical reference — any wording drift between fast and legacy paths would be a user-observable regression.
- Broader M-5 fix (alias-path guard added in addition to the fallback-path guard CONTEXT D-10 mandates). Rationale per RESEARCH Open Question 1: the same `getTimestampDriver(nonTimestampType)` crash is reachable via `SELECT ts::LONG AS ts, ...` shadowing the alias-path resolution; gating only the fallback leaves that shape uncovered. The `if (timestampIndex < 0)` skip-fill guard downstream runs unchanged and now catches both resolutions.
- Tasks 1+2+3 land in one commit. Rationale per CONTEXT D-02 (Phase 14 D-02 carryover): test restorations are not independently testable from the codegen fix (they would fail without the fix, and the fix would regress the existing test bodies without their restoration). A single atomic commit matches the Phase 14 file-clustered commit discipline.

## Deviations from Plan

None — plan executed exactly as written. CONTEXT locks (D-01..D-12) honored verbatim. The broader-than-D-10 M-5 fix is not a deviation: it is documented in RESEARCH.md "M-5 recommended fix shape" and in Plan 01 Task 2 action notes as the planner-recommended scope, explicitly contrasted with D-10's narrower literal wording.

## Issues Encountered

None. All four target tests pass on the first post-fix run; the combined `SampleByTest,SampleByNanoTimestampTest,SampleByFillTest` trio (694 tests) runs green without regressions.

## Verification Matrix (per VALIDATION.md task 15-01-01..05)

| Task ID   | Test                                                                    | Result      |
| --------- | ----------------------------------------------------------------------- | ----------- |
| 15-01-01  | SampleByTest#testTimestampFillNullAndValue                              | PASS        |
| 15-01-02  | SampleByTest#testTimestampFillValueUnquoted                             | PASS        |
| 15-01-03  | SampleByNanoTimestampTest#testTimestampFillNullAndValue (unchanged)     | PASS        |
| 15-01-04  | SampleByNanoTimestampTest#testTimestampFillValueUnquoted                | PASS        |
| 15-01-05  | SampleByFillTest (full class, M-5 regression check)                     | PASS (113)  |
| Overall   | `mvn -Dtest='SampleByTest,SampleByNanoTimestampTest,SampleByFillTest'`  | PASS (694)  |

## User Setup Required

None — pure code/test change. No external service configuration.

## Next Phase Readiness

- Plan 02 (Cursor cluster: C-3 + M-4) unblocked — no dependency on Plan 01 output beyond the shared branch state.
- Plan 02 adds two NEW tests to SampleByFillTest.java (`testFillKeyedRespectsCircuitBreaker`, `testFillPrevLong256NoPrevYet`); Plan 01 did not touch SampleByFillTest.java's method list, so Plan 02 can insert alphabetically without collision.
- Plan 03 (M-7 test-only upgrade in SqlOptimiserTest) unblocked — orthogonal to Plan 01.
- Plan 04 (Retro-doc) unblocked — paper trail only, no code dependency.

## Handoff to Plan 02

Plan 02's test insertion slots in `SampleByFillTest.java` are untouched by this plan. This plan modified only `SampleByTest.java` (two test bodies) and `SampleByNanoTimestampTest.java` (one test body). `SampleByFillTest.java` is unchanged.

## Self-Check: PASSED

- Commit `9df205bac5` exists.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` contains the C-1+C-2 branch (`ColumnType.isTimestamp(targetColType)`, `Chars.isQuoted(fillExpr.token)`, `TimestampConstant.newInstance(parsed, targetColType)`, both `Invalid fill value: '` and `invalid fill value: ` error strings, `Misc.free(fillValues.getQuick(fillIdx))` free-before-overwrite).
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` contains both M-5 guards (alias-path `ColumnType.isTimestamp(...getColumnType(timestampIndex))` and fallback-path `ColumnType.isTimestamp(...getColumnType(origIndex))`).
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — 0 matches for `51062-02-01T08:48:43.456000Z`, 11 matches for `2019-02-03T12:23:34.123456Z`, 1 match for `Invalid fill value: '1236'. Timestamp fill value must be in quotes.`, 0 matches for `printSql` inside `testTimestampFillValueUnquoted`.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — 1 match for `Invalid fill value: '1236'. Timestamp fill value must be in quotes.`, 0 matches for `printSql` inside `testTimestampFillValueUnquoted`, 10 matches for `2019-02-03T12:23:34.123456000Z` (unchanged, nano twin was already unit-correct).
- No banner comments (`// ===` / `// ---`) introduced in SqlCodeGenerator.java.

---
*Phase: 15-address-pr-6946-review-findings-and-retro-fixes*
*Completed: 2026-04-21*
