---
status: complete
phase: 08-fix-remaining-test-regressions-across-all-affected-suites
source:
  - 08-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. All affected test suites pass after fast-path fill changes
expected: |
  81 test failures across 7 suites all pass again:
  - ExplainPlanTest 522/522 (plan text updated for 8 assertions)
  - SqlOptimiserTest 171/171 (plan + should-fail + error text updated for 14 assertions)
  - RecordCursorMemoryUsageTest 9/9 (factory class assertions updated for 3 cases)
  - SqlParserTest 1059/1059 (parse tree assertions updated for 4 cases)
  - FirstArrayGroupByFunctionFactoryTest 11/11
  - LastArrayGroupByFunctionFactoryTest 20/20
  - SampleByNanoTimestampTest 279/279 (50 metadata/random-access fixes)
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
