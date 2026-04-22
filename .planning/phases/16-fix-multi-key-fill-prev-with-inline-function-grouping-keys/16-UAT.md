---
status: complete
phase: 16-fix-multi-key-fill-prev-with-inline-function-grouping-keys
source:
  - 16-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:20:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Multi-key FILL(PREV) cartesian contract (regression-test proxy)
expected: |
  Multi-key `SAMPLE BY ... FILL(PREV)` with an inline FUNCTION / OPERATION grouping
  key emits the full cartesian product of all unique keys x all buckets, matching
  what the legacy cursor path produces. 5 new regression tests encode the contract:

  - testFillPrevIntervalMultiKey — 2 distinct `interval(lo, hi)` keys, 3 buckets, asserts 6-row cartesian output matching the empirical probe in the source todo
  - testFillPrevConcatMultiKey — 2 distinct `concat(a, b)` keys
  - testFillPrevCastMultiKey — 2 distinct `cast(x AS STRING)` keys
  - testFillPrevConcatOperatorMultiKey — 2 distinct `a || b` (OPERATION) keys
  - testFillNullCastMultiKey — FILL(NULL) representative

  Pre-fix behavior (bug): 3 rows (non-cartesian passthrough).
  Post-fix behavior: 6 rows (full 2-keys x 3-buckets cartesian).

  Automated verifier cross-suite run: SampleByFillTest 120 + SampleByTest 303 +
  SampleByNanoTimestampTest 278 + ExplainPlanTest 522 (2 pre-existing skips) +
  SqlOptimiserTest 172 = 1395 pass, 0 failures. D-05 `-ea` assertion did not fire
  across the full run.
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
