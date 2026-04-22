---
status: complete
phase: 11-hardening-review-findings-fixes-and-missing-test-coverage
source:
  - 11-01-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. Hardening: key dispatch + sentinel correctness + coverage gaps closed
expected: |
  - UUID key columns emit correct values in fill rows via FILL_KEY dispatch in
    getLong128Hi/Lo, getDecimal128/256, getLong256
  - Geo-hash null sentinels use `GeoHashes.*_NULL` (not 0 or `Numbers.*_NULL`)
  - Decimal8/16 null sentinels use `Decimals.*_NULL`
  - New NULL-key test exists (NULL SYMBOL/STRING as GROUP BY key)
  - New CTE/subquery FILL_KEY reclassification test exists
  - New sparse-DST test generates fill rows during the transition
  - `/review-pr` produces no critical or moderate findings on production code
result: pass

## Summary

total: 1
passed: 1
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
