---
status: complete
phase: 03-keyed-fill-cursor
source:
  - 03-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. Keyed SAMPLE BY FILL cartesian emission
expected: |
  `SELECT ts, key, agg(val) FROM t SAMPLE BY 1h FILL(NULL | PREV)` with one or more
  key columns emits the full cartesian product of every unique key x every bucket:
  - A row appears for every (key, bucket) pair, including keys absent from a given bucket
  - Per-key FILL(PREV) carries each key's own prior value independently (no cross-key bleed)
  - Key ordering within a bucket is stable and consistent across buckets
  - Fill-row key values match the actual values discovered during pass 1 (not null/garbage)
  Plan shows `Async Group By` (fast path).
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
