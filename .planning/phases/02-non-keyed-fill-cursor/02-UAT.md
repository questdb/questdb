---
status: complete
phase: 02-non-keyed-fill-cursor
source:
  - 02-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. Non-keyed SAMPLE BY FILL correctness on the fast path
expected: |
  `SELECT ts, avg(val) FROM t SAMPLE BY 1h FILL(NULL | PREV | <const>)` produces
  correct, time-ordered gap-filled output with:
  - NULL-valued rows for every missing bucket between first and last data point under FILL(NULL)
  - Previous bucket's aggregate carried forward into gap rows under FILL(PREV)
  - Constant-filled rows under FILL(<const>)
  - Correct leading/trailing fill rows under FROM...TO ranges
  - No duplicated or out-of-order output across DST transitions with ALIGN TO CALENDAR TIME ZONE
  Plan shows `Async Group By` (fast path), no memory leaks.
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
