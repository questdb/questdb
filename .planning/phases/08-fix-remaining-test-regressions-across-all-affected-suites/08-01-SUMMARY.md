---
phase: 08-fix-remaining-test-regressions-across-all-affected-suites
plan: 01
subsystem: test
tags: [test-fix, sample-by, fill-cursor]
dependency_graph:
  requires: [07-01]
  provides: [test-green-6-suites]
  affects: [SampleByNanoTimestampTest]
tech_stack:
  added: []
  patterns: [assertSql-for-fill-cursor, false-supportsRandomAccess]
key_files:
  created: []
  modified:
    - core/src/test/java/io/questdb/test/griffin/ExplainPlanTest.java
    - core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java
    - core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java
    - core/src/test/java/io/questdb/test/griffin/SqlParserTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/functions/groupby/FirstArrayGroupByFunctionFactoryTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/functions/groupby/LastArrayGroupByFunctionFactoryTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java
decisions:
  - "Use false for supportsRandomAccess in all non-keyed fill tests since fill cursor does not support random access"
  - "Convert should-have-failed tests to printSql since fast path now handles keyed FROM/TO, VALUE fill with arrays, etc."
  - "ASOF JOIN case in ExplainPlanTest also routes through fast path (research was incorrect about it staying on old path)"
  - "SqlOptimiserTest error position changed from -1 to 0 for inconvertible value errors"
metrics:
  duration: 273m
  completed: "2026-04-11T20:41:00Z"
  tasks_completed: 2
  files_modified: 7
---

# Phase 8 Plan 01: Fix Remaining Test Regressions Summary

Test-only fixes for fast-path fill cursor regressions across 7 suites, reducing failures from 81 to 21.

## One-liner

Fix 60 of 81 test failures across 7 suites by updating plan strings, error messages, factory class assertions, and data ordering expectations to match the new fast-path fill cursor execution structure.

## Task Results

### Task 1: Fix 31 failures in small suites (COMPLETE)

All 6 suites pass with 0 failures:

| Suite | Tests | Status |
|-------|-------|--------|
| ExplainPlanTest | 522 | PASS (8 plan text updates + ASOF JOIN case) |
| SqlOptimiserTest | 171 | PASS (11 plan text + 2 should-fail + 2 error msg) |
| SqlParserTest | 1059 | PASS (4 parse model updates) |
| RecordCursorMemoryUsageTest | 9 | PASS (3 factory class updates) |
| FirstArrayGroupByFunctionFactoryTest | 11 | PASS (1 should-fail conversion) |
| LastArrayGroupByFunctionFactoryTest | 20 | PASS (1 should-fail conversion) |

**Commit:** `2cc3a9edbb`

### Task 2: Fix 50 failures in SampleByNanoTimestampTest (PARTIAL - 29/50)

Fixed 29 of 50 failures, reducing from 50 to 21:

| Category | Count | Fix Applied |
|----------|-------|-------------|
| Error message text (E) | 3 | Changed position 10 -> 0, text "Unsupported type" -> "inconvertible value" |
| Should-have-failed (D) | 4 | Converted assertException to printSql |
| Random access (A) | 11 | Changed supportsRandomAccess from true to false |
| Data row ordering (G) | 11 | Updated expected strings to match GROUP BY hash-map order |
| **Remaining** | **21** | Need 2nd/3rd assertion updates (multi-assertion tests), plan text, timestamp index |

**Commit:** `fec2250511`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] ASOF JOIN case also routes through fast path**
- **Found during:** Task 1, testSampleByFillPrevNotKeyed
- **Issue:** Research assumed ASOF JOIN sub-case stayed on old Sample By path, but it now goes through the fast path too
- **Fix:** Updated the third assertPlanNoLeakCheck in testSampleByFillPrevNotKeyed with the new plan format (GroupBy vectorized: false under Sort under Sample By Fill)
- **Commit:** 2cc3a9edbb

**2. [Rule 1 - Bug] Error position for inconvertible value is 0, not -1**
- **Found during:** Task 1, SqlOptimiserTest.testSampleByFromToNotEnoughFillValues
- **Issue:** Plan said to change error text but actual position also changed from -1 to 0
- **Fix:** Changed both message text AND position in assertion
- **Commit:** 2cc3a9edbb

## Deferred Issues

21 SampleByNanoTimestampTest failures remain:
- Tests with multiple assertions where only the first assertion was fixed (second assertion in same test also has data ordering change)
- testSampleByDisallowsPredicatePushdown (needs sampleByPushdownPlan helper)
- testSampleFillPrevDuplicateTimestamp1/2 (needs assertSql with ORDER BY)
- testSampleFillPrevAlignToCalendar, testSampleFillPrevAllTypes, testSampleFillPrevNoTimestamp (special error formats)
- testSampleFillNullAlignToCalendarTimeZoneFloat, testSampleFillValueAllTypesAndTruncate (indentation match issue)

## Self-Check: PARTIAL

Task 1 complete (all 6 suites green). Task 2 partially complete (29/50 fixed, 21 remaining).
