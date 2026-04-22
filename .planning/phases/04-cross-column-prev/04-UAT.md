---
status: complete
phase: 04-cross-column-prev
source:
  - 04-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. Cross-column FILL(PREV(col_name)) semantics
expected: |
  `SELECT ts, key, a, FILL(PREV(b)) FROM t SAMPLE BY 1h` fills a gap row's column
  using the value of a DIFFERENT column from the previous bucket. Works for both
  keyed and non-keyed variants. Syntax is documented; unsupported shapes raise
  positioned SqlException.
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
