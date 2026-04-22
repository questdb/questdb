---
status: complete
phase: 10-fix-offset-aware-bucket-alignment-in-fill-cursor
source:
  - 10-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. Offset-aware bucket alignment
expected: |
  `SAMPLE BY 5d FROM '2017-12-20' FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '10:00'`
  produces finite output that stops after the last data bucket (bug: previously
  infinite fill because the sampler's setStart ignored the offset).
  The fill cursor's bucket sequence matches the `timestamp_floor_utc` bucket
  boundaries for all offset values. Keyed + non-keyed queries with offset + FROM
  (no TO) produce correct, finite results. All existing tests still pass.
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
