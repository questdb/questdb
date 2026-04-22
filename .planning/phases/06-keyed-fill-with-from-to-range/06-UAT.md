---
status: complete
phase: 06-keyed-fill-with-from-to-range
source:
  - 06-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. Keyed FILL with FROM/TO range, including leading + trailing fill
expected: |
  `SELECT ts, key, avg(val) FROM t SAMPLE BY 1h FROM '...' TO '...' FILL(NULL | PREV | <const>)`
  emits all keys for every bucket in the [FROM, TO) range:
  - Leading fill rows (FROM before first data) include every key with null/const/prev values
  - Trailing fill rows (TO after last data) include every key with correct fill values
  - Per-key FILL(PREV) tracks across the full range including leading buckets before any data
  - No SIGSEGV on the cartesian emission path
  Output matches the legacy cursor path for equivalent queries.
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
