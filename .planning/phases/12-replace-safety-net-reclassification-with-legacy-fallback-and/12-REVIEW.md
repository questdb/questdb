---
status: issues_found
phase: 12
reviewed: 2026-04-17
reviewer: gsd-code-reviewer
depth: standard
files_reviewed: 11
findings:
  critical: 0
  warning: 4
  info: 6
  total: 10
---

# Phase 12: Code Review Report

## Summary

Phase 12 replaces the codegen safety-net (silent reclassification of
unsupported PREV sources as FILL_KEY, producing duplicated fill rows) with a
retro-fallback mechanism that re-dispatches to the legacy cursor. It tightens
the optimizer gate (cross-col PREV type resolution, LONG128/INTERVAL), enforces
FILL(PREV) grammar rules (D-05..D-09), adds the `hasExplicitTo` LONG_NULL
guard, and makes `fill=` always appear in `Sample By Fill` plans.

The review surfaces three correctness concerns worth addressing before merge:
- A potentially unrecoverable re-dispatch when a `QueryModelWrapper` sits in
  the nested chain.
- Lost `FROM/TO/OFFSET/TIMEZONE` metadata on the retro-fallback path.
- Three byte-identical 13-line catch blocks that duplicate walk-and-restore
  logic at each call site.

Quality findings follow (redundant same-package import, chain-rejection
position is generic, FallbackToLegacyException inherits SqlException's mutable
`message` sink).

## Warnings

### WR-01: Retro-fallback re-dispatch may lose FROM/TO/OFFSET/TIMEZONE metadata

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8043-8056, 8259-8272, 8307-8320`

When `rewriteSampleBy` triggers (SqlOptimiser lines 8467-8469), it clears
`sampleByOffset`, `sampleByFrom`, `sampleByTo` (plus `sampleBy`) on the
`nested` model. The rewrite copies these values into `fillFrom`, `fillTo`,
`fillOffset`, `fillStride`. The retro-fallback then restores ONLY `sampleBy`
via `curr.setSampleBy(stashed)` — `sampleByFrom`, `sampleByTo`,
`sampleByOffset` stay null. `generateSampleBy` reads these fields from its
`model` parameter (line 7083, 7086, 7148, 7157). For queries that match the
retro-fallback trigger conditions AND specify `FROM`/`TO`/`OFFSET`, the legacy
cursor runs without those bounds, producing results that differ from the
user's intent.

No existing retro-fallback test exercises
`FROM`/`TO`/`TIMEZONE`/`OFFSET` — all five fallback tests use the simplest
`SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR` shape.

**Fix:** Either
1. Extend the stash to capture `sampleByFrom`, `sampleByTo`, `sampleByOffset`,
   `sampleByTimezoneName`, `sampleByFill` on the nested model and restore all
   of them in the re-dispatch catch, or
2. Add a regression test that asserts correct bounded output for `SAMPLE BY
   1h FROM '2024-01-01' TO '2024-01-02' FILL(PREV) ALIGN TO CALENDAR` with
   an aggregate whose output type forces retro-fallback. If the test passes,
   document why; if it fails, implement fix #1.

### WR-02: Retro-fallback throws `UnsupportedOperationException` when stash is on a wrapped model

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8053, 8054, 8269, 8270, 8317, 8318`

The re-dispatch walks `curr.getNestedModel()` looking for
`getStashedSampleByNode() != null`. `QueryModelWrapper.getStashedSampleByNode()`
delegates to `delegate.getStashedSampleByNode()` — so the walk can stop at a
wrapper whose delegate has the stash. But `QueryModelWrapper.setSampleBy(...)`
and `QueryModelWrapper.setStashedSampleByNode(...)` both unconditionally
throw `UnsupportedOperationException` (lines 1191-1192, 1246-1247 of
QueryModelWrapper.java). If the walk lands on a wrapper, the retro-fallback
converts a recoverable condition into an unhandled exception that escapes to
the user.

**Fix:** Unwrap before writing back, or change the walk to descend through
wrappers explicitly. `QueryModelWrapper` already exposes `getDelegate()`.

### WR-03: Duplicated retro-fallback catch handler across three call sites

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8043-8056, 8259-8272, 8307-8320`

The `FallbackToLegacyException` catch handler is byte-identical at all three
`generateFill` call sites (13 lines each). Any future change has to be
applied three times.

**Fix:** Extract a private helper:
```java
private RecordCursorFactory fallbackToLegacySampleBy(IQueryModel model, SqlExecutionContext executionContext) throws SqlException {
    IQueryModel curr = model;
    while (curr != null && curr.getStashedSampleByNode() == null) {
        curr = curr.getNestedModel();
    }
    final ExpressionNode stashed = curr != null ? curr.getStashedSampleByNode() : null;
    assert stashed != null : "stashedSampleByNode not found on any nested model";
    curr.setSampleBy(stashed);
    curr.setStashedSampleByNode(null);
    return generateSampleBy(model, executionContext, stashed, model.getSampleByUnit());
}
```

### WR-04: Chain-rejection position is generic — always points at first PREV

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3513`

The D-07 chain rejection uses `fillValuesExprs.getQuick(0).position`
regardless of which column's chain is malformed. Context acknowledges this as
a deliberate trade-off but it represents a UX degradation over other grammar
rules in this phase, which all point at the specific offending AST node.

**Fix:** Track an `ExpressionNode sourceFillExpr` per cross-col mode during
the fill-spec build loop. Allocates one extra pointer array of length
`columnCount` on the cross-col path.

## Info

### IN-01: Redundant same-package import

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:70`

`import io.questdb.griffin.FallbackToLegacyException;` — same-package.
Remove.

### IN-02: `FallbackToLegacyException.INSTANCE` inherits SqlException's mutable `message` and `position` fields

**File:** `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java:40-49`

If future code accidentally writes `FallbackToLegacyException.INSTANCE.put("...")`,
the singleton's state would mutate and leak between threads. Risk small, but
worth either a doc note or `@Override` stubs.

### IN-03: `testFillPrevRejectNoArg` uses `assertSql` + try/catch instead of `assertExceptionNoLeakCheck`

**File:** `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java:1684-1709`

The permissive `e.getMessage().contains(...)` with multiple disjuncts is too
broad. Tighten to `Assert.assertTrue(e.getMessage().contains("PREV"))`.

### IN-04: `hasAnyConstantFill()` walks `fillModes` on every `toPlan` call

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:227-237`

Answer is invariant for the lifetime of the factory — could be computed once.
Strictly optional.

### IN-05: `fillModes` field is final — consistent

**File:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:87`

No issue. Note for completeness.

### IN-06: `generateFill`'s `Dates.parseOffset` assert message concatenates

**File:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3398-3399`

With `-ea` off, the assert compiles to no-op. With `-ea` on, allocation only
on failure. Correct pattern.

## Passing checks

The review verified the following compliance items from CLAUDE.md and the
phase-12 context, all of which pass:

- **Alphabetization (SampleByFillRecordCursorFactory):** FillRecord getters
  in strict alphabetical order. Fields alphabetical. `readColumnAsLongBits`
  with the static cluster.
- **Alphabetization (SqlCodeGenerator):** `isKeyColumn` grouped with
  `isFastPathPrevSupportedType`, `isHorizonOffsetModel`,
  `isSingleColumnFunction`, `isSingleSymbolJoin`.
- **Imports:** `SampleByFillRecordCursorFactory` ordering alphabetical
  relative to siblings. FQN usages (`io.questdb.std.Decimal128`) now plain
  imports.
- **`fill=` always emitted in plan:** `toPlan` emits
  `fill=prev`/`fill=value`/`fill=null` unconditionally. Test expectations
  updated accordingly.
- **Dates.parseOffset assertion:** Silent drop replaced with assert.
- **`@Override` annotations:** All new IQueryModel methods have `@Override`
  on QueryModel and QueryModelWrapper implementations.
- **Boolean naming:** `hasExplicitTo`, `hasPrevFill`, `hasDataForCurrentBucket`,
  `hasSimplePrev`, `isBaseCursorExhausted`, `isEmittingFills`, `isInitialized`,
  `isGapFilling` — all use `is`/`has` prefix.
- **Resource cleanup on error paths:** `FallbackToLegacyException` throw is
  caught by existing `catch (Throwable e)` which frees `fillValues`,
  `constantFillFuncs`, `fillFromFunc`, `fillToFunc`, `groupByFactory` before
  rethrowing. Retro-fallback path does NOT double-free — `generateSampleBy`
  rebuilds the factory chain from scratch via `generateSubQuery`.
- **Zero-GC / ObjList:** No `java.util.*` collections added. `IntList`,
  `ObjList` used throughout.
- **Positioned SqlExceptions:** Grammar rejections at `fillExpr.position`,
  `fillExpr.rhs.position`, `fillValuesExprs.getQuick(0).position` (chain —
  noted in WR-04).
- **NULL handling:** `hasExplicitTo` demotion on `maxTimestamp == LONG_NULL`.
  `testFillToNullTimestamp` covers bind-var-to-null.
- **Test conventions:** All new tests use `assertMemoryLeak`. Most fast-path
  tests converted to `assertQueryNoLeakCheck` with `"ts", false, false`
  params. `testFillPrevOfSymbolKeyColumn` stays on `assertSql` with an
  explicit comment explaining why (symbol mirror discrepancy pre-existing).

## Key source files

- `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java` — new singleton control-flow exception
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3555` — throw site of `FallbackToLegacyException.INSTANCE`
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:8043-8056, 8259-8272, 8307-8320` — three duplicated catch blocks (WR-03)
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:8461-8464, 8467-8469` — stash site and cleared fields (WR-01 concern)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:593` — `hasExplicitTo` LONG_NULL guard
- `core/src/main/java/io/questdb/griffin/model/QueryModelWrapper.java:1191-1247` — unconditional throws that WR-02 concerns
