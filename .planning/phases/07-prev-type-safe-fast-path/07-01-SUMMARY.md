---
phase: 07-prev-type-safe-fast-path
plan: 01
subsystem: fill-cursor
tags: [type-safety, prev-fill, optimizer-gate, legacy-fallback]
dependency_graph:
  requires: [04-01]
  provides: [per-column-prev-snapshot, type-support-matrix, optimizer-gate]
  affects: [SampleByFillRecordCursorFactory, SqlCodeGenerator, SqlOptimiser]
tech_stack:
  added: []
  patterns: [per-column-mask-filtering, two-layer-type-defense]
key_files:
  created: []
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/main/java/io/questdb/griffin/SqlOptimiser.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java
decisions:
  - IntList prevSourceCols replaces boolean hasPrevFill, derived in generateFill and passed to factory
  - Cross-column prev adds both target and source columns to prevSourceCols
  - Two-layer defense (optimizer gate best-effort + generateFill safety net)
  - readColumnAsLongBits default branch throws UnsupportedOperationException instead of silent getLong()
metrics:
  duration: 49m
  completed: "2026-04-10T18:11:00Z"
  tasks: 2
  files: 5
---

# Phase 7 Plan 01: PREV Type-Safe Fast Path Summary

Per-column PREV snapshot via IntList prevSourceCols, extended readColumnAsLongBits type matrix, optimizer gate for STRING/SYMBOL/VARCHAR/LONG256/BINARY/UUID/ARRAY PREV fallback to legacy cursor path, and generateFill() safety net.

## Changes Made

### Task 1: Production code (commit e98417942a)

**SampleByFillRecordCursorFactory.java:**
- Constructor parameter `boolean hasPrevFill` replaced with `IntList prevSourceCols` in both factory and inner cursor constructors
- `hasPrevFill` boolean field derived locally: `prevSourceCols != null && prevSourceCols.size() > 0`
- `savePrevValues()` iterates only `prevSourceCols` instead of all columns 0..columnCount-1
- `updatePerKeyPrev()` iterates only `prevSourceCols` instead of all columns
- `readColumnAsLongBits()` gains explicit cases for DATE, LONG, TIMESTAMP (previously lumped in default), GEOLONG (via `getGeoLong()`), and DECIMAL8/16/32/64 (via dedicated accessors)
- `readColumnAsLongBits()` default branch throws `UnsupportedOperationException` with descriptive message instead of silently calling `getLong()`

**SqlCodeGenerator.java:**
- `generateFill()` builds `prevSourceCols` IntList after fill mode loop: columns with `FILL_PREV_SELF` or cross-column prev (`mode >= 0`) are included. For cross-column prev, both target and source columns are added to ensure source column values are snapshotted.
- Safety-net type check: iterates `prevSourceCols`, resolves source column type via `groupByMetadata`, throws `SqlException` if type is unsupported
- `isFastPathPrevSupportedType()` helper method added: returns true for DOUBLE, FLOAT, INT, LONG, SHORT, BYTE, BOOLEAN, CHAR, TIMESTAMP, DATE, IPv4, GEOBYTE, GEOSHORT, GEOINT, GEOLONG, DECIMAL8/16/32/64
- `boolean hasPrevFill` local variable removed; `prevSourceCols` passed to factory constructor

**SqlOptimiser.java:**
- `hasPrevWithUnsupportedType()` method added: inspects fill values and output column types. For bare `FILL(PREV)`, checks all aggregate columns. For per-column fill, maps fill entries to aggregate columns by position, skipping key columns and timestamp.
- `isUnsupportedPrevAggType()` helper: resolves aggregate function argument type from the nested model's alias-to-column map (works for simple LITERAL arguments)
- `isUnsupportedPrevType()` helper: returns true for SYMBOL, STRING, VARCHAR, LONG256, BINARY, UUID, DECIMAL128, DECIMAL256, and ARRAY types
- Wired into `rewriteSampleBy()` condition alongside `!hasLinearFill(sampleByFill)`

### Task 2: Tests (commit c73f10e1ec)

**SampleByFillTest.java (5 new tests, 32 total):**
- `testFillPrevMixedWithSymbol`: PREV on sum(val) + NULL on first(sym) stays on fast path without crash
- `testFillPrevMixedWithSymbolKeyed`: keyed variant with per-key PREV tracking for numeric column and NULL for SYMBOL
- `testFillPrevCrossColumnUnsupportedFallback`: PREV(s) targeting STRING column falls back to legacy path
- `testFillPrevSymbolLegacyFallback`: PREV on first(s) where s is STRING falls back to legacy path
- `testFillPrevNumericWithTimezone`: numeric PREV with timezone stays on fast path

**SampleByNanoTimestampTest.java (3 new tests):**
- `testFillPrevMixedNano`: nanosecond parity for mixed PREV+NULL
- `testFillPrevNumericWithTimezoneNano`: nanosecond parity for numeric PREV with timezone
- `testFillPrevSymbolLegacyFallbackNano`: nanosecond parity for legacy fallback

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Cross-column prev source column missing from prevSourceCols**
- **Found during:** Task 1 verification
- **Issue:** The initial `prevSourceCols` construction only added the target column for cross-column prev (`mode >= 0`). But `prevValue()` reads `simplePrev[sourceCol]`, so the source column must also be snapshotted.
- **Fix:** Added `if (!prevSourceCols.contains(mode)) prevSourceCols.add(mode)` for cross-column prev entries.
- **Files modified:** SqlCodeGenerator.java
- **Commit:** e98417942a

**2. [Rule 1 - Bug] Plan assertion text mismatches in new tests**
- **Found during:** Task 2 verification
- **Issue:** Plan assertions used `Sort light` instead of `Sort`, and legacy path used `fill: prev,null` instead of `fill: value`.
- **Fix:** Corrected all plan assertion strings to match actual engine output.
- **Files modified:** SampleByFillTest.java, SampleByNanoTimestampTest.java
- **Commit:** c73f10e1ec

## Pre-existing Issues (Out of Scope)

**testSampleByFillNeedFix** (SampleByTest.java:5947): This test was already failing before Phase 7 changes. It has `FILL(PREV, ...)` on a query with a SYMBOL key column where the model structure after optimizer rewrite causes `isKeyColumn()` to not recognize the key column. The old code crashed with `UnsupportedOperationException`; the new code throws a descriptive `SqlException`. Root cause documented in `deferred-items.md`.

## Decisions Made

1. **IntList over boolean mask:** `IntList prevSourceCols` chosen over `boolean[] isPrevSource` because it enables iteration without scanning all columns and integrates with existing IntList patterns in the codebase.
2. **Cross-column source inclusion:** Both target and source columns added to `prevSourceCols` for cross-column prev, ensuring `prevValue()` can read the source column's snapshot.
3. **Two-layer type defense:** Optimizer gate catches common cases (simple LITERAL aggregate arguments); generateFill safety net catches complex expressions with a descriptive SqlException.
4. **Explicit readColumnAsLongBits cases:** DATE, LONG, TIMESTAMP moved from implicit default->getLong() to explicit cases for clarity. GEOLONG gets dedicated `getGeoLong()` call. DECIMAL8/16/32/64 get dedicated accessors.

## Self-Check: PASSED

All referenced files exist and all commits are verified:
- e98417942a: Task 1 production code
- c73f10e1ec: Task 2 tests
- 32/32 SampleByFillTest tests pass
- 3/3 new SampleByNanoTimestampTest tests pass
- 301/302 SampleByTest tests pass (1 pre-existing failure)
