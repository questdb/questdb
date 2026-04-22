---
status: complete
phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and
source:
  - 12-01-SUMMARY.md
  - 12-02-SUMMARY.md
  - 12-03-SUMMARY.md
  - 12-04-SUMMARY.md
started: 2026-04-22T10:12:54Z
updated: 2026-04-22T10:12:54Z
---

## Current Test

[testing complete]

## Tests

### 1. Safety-net reclassification replaced with retro-fallback + Tier 1 gate tightening
expected: |
  - `testSampleByFillNeedFix` matches master's 3-row expected output (no duplicated buckets)
  - Queries whose aggregate output type is DECIMAL128/256, LONG256, UUID, STRING, VARCHAR,
    SYMBOL, BINARY, array, LONG128, or INTERVAL route to the legacy cursor (plan shows
    `Sample By`, not `Sample By Fill`) and produce correct results
  - Safety-net block at SqlCodeGenerator.java:3497-3512 is deleted; retro-fallback
    mechanism (stashed SAMPLE BY node + FallbackToLegacyException + try/catch at the
    three generateFill call sites) replaces it
  - `TO null::timestamp` produces bounded output via the hasExplicitTo LONG_NULL guard
  - `toPlan` emits `fill=null|prev|value` unconditionally for every Sample By Fill node
result: pass

### 2. Six FILL(PREV, PREV(...)) grammar rules with positioned SqlException
expected: |
  Malformed PREV shapes are rejected with positioned error messages:
  - PREV(ts) rejected
  - Chained PREV rejected
  - Non-LITERAL arg rejected
  - paramCount != 1 rejected
  - Bind variables in PREV rejected
  - Type-tag mismatches rejected
  8 negative grammar tests + 3 positive tests pass.
result: pass

### 3. 22 new regression tests land
expected: |
  22 regression tests pass:
  - 5 retro-fallback
  - 11 grammar (3 positive + 8 negative)
  - 5 FILL_KEY dispatch (UUID, Long256, Decimal128, Decimal256, geo-no-prev-yet)
  - 1 TO-null
  Full suite green: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest' test` exits 0.
result: pass

### 4. Code-quality sweep per CONTEXT.md D-01..D-18
expected: |
  - FillRecord getters alphabetized
  - SampleByFillCursor members alphabetized
  - Plain imports replace FQNs; import blocks alphabetized
  - `isKeyColumn` relocated next to `isFastPathPrevSupportedType`
  - `anyPrev` loop removed
  - `Dates.parseOffset` asserts added
  - `fill=` toPlan attribute unconditional
  - ~15 `assertSql` sites converted to `assertQueryNoLeakCheck` per D-10
result: pass

## Summary

total: 4
passed: 4
issues: 0
pending: 0
skipped: 0

## Gaps

(none)
