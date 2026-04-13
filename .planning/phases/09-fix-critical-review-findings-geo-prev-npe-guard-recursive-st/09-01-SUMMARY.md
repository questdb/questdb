---
phase: 09-fix-critical-review-findings
plan: 01
subsystem: griffin/groupby
tags: [bugfix, geohash, stack-overflow, javadoc]
dependency_graph:
  requires: []
  provides: [geo-prev-fill, iterative-fill-loop]
  affects: [SampleByFillRecordCursorFactory]
tech_stack:
  added: []
  patterns: [iterative-loop-replaces-recursion, assert-invariant-guard]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
decisions:
  - Assert guard (not null-check-continue) at findValue() because pass 1 discovers all keys from the same cursor
  - Iterative while(true) loop in emitNextFillRow() replaces recursive hasNext() call
  - hasNext() call sites updated to fall through on emitNextFillRow returning false instead of propagating false
metrics:
  duration: 23m
  completed: 2026-04-13
  tasks: 2
  files: 2
---

# Phase 09 Plan 01: Fix Critical Review Findings Summary

Fix 3 critical bugs and 1 Javadoc inconsistency in SampleByFillRecordCursorFactory; add 3 regression tests confirming each fix.

## Completed Tasks

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Apply all four fixes to SampleByFillRecordCursorFactory | b2e1fc857e | SampleByFillRecordCursorFactory.java |
| 2 | Add regression tests for geo PREV and recursive stack overflow | d024a984f8 | SampleByFillTest.java |

## Changes Made

### Fix 1: Geo PREV fill dispatch

`getGeoByte()`, `getGeoShort()`, `getGeoInt()`, `getGeoLong()` now check `FILL_PREV_SELF` and `mode >= 0` before falling through to `FILL_CONSTANT` or default return. Each getter calls `prevValue(col)` with the appropriate cast. This matches the pattern already used by `getInt()`, `getLong()`, `getShort()`, `getByte()`, and other numeric getters.

### Fix 2: NPE guard at findValue()

Added `assert value != null : "key discovered in pass 1 must exist in keysMap"` after `mapKey.findValue()` at the keyed data-row path. The assert catches invariant violations in debug mode without impacting production performance.

### Fix 3: Recursive hasNext() -> iterative loop

Replaced `return hasNext()` at the end of `emitNextFillRow()` with a `while (true)` loop that advances through consecutive empty buckets iteratively. Stack depth is O(1) regardless of gap count.

Additionally updated all three `emitNextFillRow()` call sites in `hasNext()`:
- The top-of-method re-entry path now falls through to the main loop when `emitNextFillRow()` returns false
- The two gap-detection paths inside the while loop use `continue` to re-enter the main loop when `emitNextFillRow()` returns false
- The baseCursorExhausted path retains `return emitNextFillRow()` since there is nothing more to do

### Fix 4: Javadoc correction

Changed class Javadoc from `followedOrderByAdvice=true` to `followedOrderByAdvice=false -- the outer sort handles ordering`.

### Regression tests

1. `testFillPrevGeoHash`: non-keyed query with GEOHASH(3b), GEOHASH(15b), GEOHASH(6c), GEOHASH(8c) columns and FILL(PREV). Verifies all four geo getters carry forward previous values in fill rows.
2. `testFillPrevGeoHashKeyed`: keyed query with SYMBOL key and GEOHASH(6c) aggregate. Verifies per-key geo PREV tracking does not bleed between keys.
3. `testFillNullSparseDataLargeRange`: keyed query with 2 data points ~1 year apart and 1h stride (8761 buckets). Completes without StackOverflowError.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed emitNextFillRow call sites in hasNext()**
- **Found during:** Task 1
- **Issue:** After replacing the recursive `return hasNext()` in `emitNextFillRow()` with an iterative loop, the three `return emitNextFillRow()` call sites in `hasNext()` would propagate `false` directly back to the caller when `emitNextFillRow()` exhausted gap fills and returned false. This caused premature termination -- the query would stop after the first batch of fill rows instead of continuing to process remaining data rows.
- **Fix:** Updated the top-of-method re-entry path to fall through to the main while loop when `emitNextFillRow()` returns false. Updated the two gap-detection call sites to use `continue` to re-enter the main loop. The baseCursorExhausted path was left unchanged since it correctly terminates.
- **Files modified:** SampleByFillRecordCursorFactory.java
- **Commit:** b2e1fc857e

**2. [Rule 1 - Bug] Fixed geohash test expected values**
- **Found during:** Task 2
- **Issue:** The plan suggested `##010` binary literal syntax and `100` as the 3-bit representation of character 's'. The actual 3-bit representation of 's' (base32 value 24) is `110`, and both 's' and 'u' share the same 3-bit prefix. Changed test to use character '8' (3-bit = `010`) for a distinct non-zero 3-bit geohash value.
- **Fix:** Used `cast('8' AS GEOHASH(3b))` for the first row (output `010`) and `cast('s' AS GEOHASH(3b))` for the second row (output `110`), ensuring the fill row carries a value distinct from both zero and the second data row.
- **Files modified:** SampleByFillTest.java
- **Commit:** d024a984f8

## Verification

- `mvn -pl core -Dtest=SampleByFillTest test`: 35/35 pass (32 existing + 3 new)
- `grep 'return hasNext()'` in SampleByFillRecordCursorFactory.java: 0 matches
- `grep 'followedOrderByAdvice=true'` in SampleByFillRecordCursorFactory.java: 0 matches

## Self-Check: PASSED
