---
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
plan: 02
subsystem: sql
tags: [sql, fill, prev, rowid, fast-path, cursor]

requires:
  - phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
    plan: 01
    provides: "SortedRecordCursor.of() now clears the chain on reuse, making captured rowIds stable across cursor-factory reuses; chosen vehicle for rowId replay"
provides:
  - "SampleByFillCursor stores PREV snapshots as a single chain rowId per key (keyed path) or one simplePrevRowId (non-keyed path)"
  - "FillRecord getters uniformly delegate PREV reads to prevRecord.getXxx(col) for self-prev or prevRecord.getXxx(mode) for cross-column PREV, unlocking STRING, VARCHAR, SYMBOL, BINARY, DOUBLE[], LONG128, LONG256, DECIMAL128, DECIMAL256, UUID, INTERVAL on the fast path"
  - "SqlCodeGenerator.generateFill mapValueTypes is now a fixed 3 LONG slots (KEY_INDEX, HAS_PREV, PREV_ROWID) — the per-agg loop and its aggColumnCount counter are gone"
  - "Single baseCursor.recordAt(prevRecord, prevRowId) call per emit row (PI-02); FillRecord getters never re-call recordAt()"
affects: [plan-03-cherry-pick-per-type-tests, plan-04-retro-fallback-deletion, plan-05-seed-002-defect-resolution]

tech-stack:
  added: []
  patterns:
    - "rowId replay via baseCursor.recordAt(prevRecord, rowId) once per fill emit row; typed reads via prevRecord.getXxx(col)"
    - "prevRecord = baseCursor.getRecordB() aliased AFTER buildChain() completes, never in of() (avoids pass-1 recordB reposition hazard)"

key-files:
  created:
    - .planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-02-SUMMARY.md
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java

key-decisions:
  - "MapValue schema collapses from (2 + aggColumnCount) LONG slots to exactly 3 LONG slots (KEY_INDEX, HAS_PREV, PREV_ROWID). keyPosOffset is now the literal constant 3."
  - "prevRecord is aliased to baseCursor.getRecordB() once in initialize() at the peek-first-row branch, after buildChain() has completed. One assignment covers both keyed (pass-1 ran buildChain via its hasNext loop then toTop()) and non-keyed (peek-hasNext ran buildChain on first call) paths."
  - "FILL_KEY delegation for Array/Bin/BinLen kept as pre-existing no-op fallthrough (returns null / -1). Plan 02's contract is PREV semantics; extending FILL_KEY coverage to var-width types where the old code returned null is out of scope and would change user-visible behavior beyond rowId migration."
  - "Retro-fallback machinery (FallbackToLegacyException, prevSourceCols, isFastPathPrevSupportedType, three try/catch sites, stash write) stays in place. Plan 04 deletes it after Plan 03 validates every currently-unsupported type passes on the fast path."

patterns-established:
  - "PI-02 (recordAt-once-per-row): fill emit branches call baseCursor.recordAt(prevRecord, rowId) before returning true from hasNext()/emitNextFillRow(); getters never re-call recordAt."
  - "recordB lifetime: alias after buildChain() completes; never before. Initialization happens at the first post-buildChain point (pass-1 loop end in keyed path, or post-peek in non-keyed path)."

requirements-completed: [INTERNAL-REFACTOR-PH13]

duration: ~100 min (analysis, implementation, two rounds of test verification, and one scope-trim to keep FILL_KEY semantics unchanged for var-width types)
completed: 2026-04-19
---

# Phase 13 Plan 02: SampleByFillRecordCursorFactory rowId rewrite + MapValue schema shrink Summary

**SampleByFillCursor now stores PREV snapshots as one chain rowId per key (keyed) or one simplePrevRowId (non-keyed); MapValue is a fixed 3-LONG header; every supported column type reads PREV values uniformly via prevRecord.getXxx(col) after a single baseCursor.recordAt(prevRecord, rowId) per fill emit row.**

## Performance

- **Duration:** ~100 min
- **Started:** 2026-04-19T00:00:00Z (session start)
- **Completed:** 2026-04-19T01:55:00Z (commit d327d02d52 landed)
- **Tasks:** 2 committed together as phase-13 Commit 2 per D-07
- **Files modified:** 2 production .java files (single commit)

## Accomplishments

- `SampleByFillRecordCursorFactory.java` net diff: 127 insertions, 115 deletions (+12 net lines from 1184 to 1196).
- `SqlCodeGenerator.java` net diff: 4 insertions, 14 deletions (-10 net lines from 10763 to 10753).
- All FILL_PREV type paths through `SampleByFillCursor` now read via `prevRecord`, removing the 10 KIND_* dispatch constants' code surface (KIND_LONG_BITS, KIND_SYMBOL, KIND_LONG128, KIND_DECIMAL128, KIND_LONG256, KIND_DECIMAL256, KIND_STRING, KIND_VARCHAR, KIND_BIN, KIND_ARRAY — all previously fan-outs inside `readColumnAsLongBits` or rejections via retro-fallback).
- `SampleByFillTest` 74/74 green; `SampleByNanoTimestampTest` 279/279 green; `ExplainPlanTest` 522/522 green (2 skipped, unrelated); `SampleByTest` 301/302 green with the single pre-existing `testSampleByFillNeedFix` assertion-#2 failure carried over from Plan 01.
- Combined-suite ordering both ways (`SampleByFillTest,SampleByTest` and `SampleByTest,SampleByFillTest`) produces the same 375/376 passing result as Plan 01's baseline — confirming Plan 02 introduces no new regressions.

## Task Commits

1. **Task 1 + Task 2 combined** — `d327d02d52` (code). Commit title:
   `Store FILL(PREV) prev snapshots as chain rowIds`
   (47 chars, plain English, no Conventional Commits prefix per project CLAUDE.md).
   Both tasks ship together as phase-13 Commit 2 per D-07 "Commit Task 1 + Task 2 edits together as Commit 2 of phase 13".

## Exact Diff Shape

### SampleByFillRecordCursorFactory.java

**Fields removed from `SampleByFillCursor`:**

| Removed | Reason |
|---|---|
| `private final short[] columnTypes` | only used by `readColumnAsLongBits` |
| `private long[] simplePrev` | replaced by `simplePrevRowId` (single long) |
| `private final IntList prevSourceCols` (field) | no longer needed at runtime; loop consumed its last reader when `prevValue` and `savePrevValues`/`updatePerKeyPrev` went away |
| `private final int[] outputColToAggSlot` | aggregate-slot math is gone |

**Fields added:**

| Added | Purpose |
|---|---|
| `private Record prevRecord` | aliased to `baseCursor.getRecordB()` after buildChain; target for recordAt() replay |
| `private long simplePrevRowId = -1L` | non-keyed path's saved rowId |

**Methods removed:**

- `private static long readColumnAsLongBits(Record r, int col, short type)` — entire switch over column types.
- `private long prevValue(int col)` — per-type slot dispatch.
- `private void savePrevValues(Record record)` — loop over `prevSourceCols` writing into `simplePrev[col]`.
- `private void updatePerKeyPrev(MapValue value, Record record)` — loop over `prevSourceCols` writing into per-agg slots.

**Methods added:**

- `private void saveSimplePrevRowId(Record record)` — stores `record.getRowId()` + sets `hasSimplePrev`.
- `private void updateKeyPrevRowId(MapValue value, Record record)` — sets `HAS_PREV_SLOT` and `PREV_ROWID_SLOT`.

**Constants:**

- `PREV_START_SLOT = 2` → `PREV_ROWID_SLOT = 2` (same value; renamed semantically).
- `keyPosOffset` is now the literal constant `3` (no arithmetic over `aggSlot`).

**FillRecord getter pattern:**

For types that already dispatched on FILL_KEY (`getBool`, `getByte`, `getChar`, `getShort`, `getInt`, `getLong`, `getFloat`, `getDouble`, `getIPv4`, `getDecimal8/16/32/64`, `getGeoByte/Int/Long/Short`, `getLong128Hi/Lo`, `getLong256/A/B`, `getDecimal128/256`, `getTimestamp`, `getStrA/B/Len`, `getVarcharA/B/Size`, `getSymA/B`): the `(mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()` branch now calls `prevRecord.getXxx(mode >= 0 ? mode : col)` — self-prev reads from `col`, cross-col PREV reads from `mode`.

For types that did NOT dispatch on FILL_KEY in the pre-rewrite code (`getArray`, `getBin`, `getBinLen`): the PREV branch is added the same way, but the FILL_KEY branch is left as the pre-existing no-op fallthrough (returns `null` / `-1`). This matches current behavior for these types on fill rows — extending FILL_KEY coverage for var-width types is out of Plan 02's scope.

**`initialize()`:**

- Keyed path: pass 1 iterates `baseCursor.hasNext()`, which drives `SortedRecordCursor.buildChain()` to completion. The keyed path then calls `baseCursor.toTop()`. The pass-1 `hasNext()` loop + `toTop()` combination guarantees buildChain is done.
- Non-keyed path: no pass 1; the first `baseCursor.hasNext()` call happens at the peek-first-row step.
- Single `prevRecord = baseCursor.getRecordB()` assignment sits inside the `if (baseCursor.hasNext())` peek-success branch, AFTER that first/subsequent hasNext call. By that point, buildChain has completed for both paths. This is the "exactly one match" required by Plan 02 Task 1 acceptance.

**`toTop()` reset adds:**

```java
simplePrevRowId = -1L;
```

alongside the existing `hasSimplePrev = false`.

**Emit-path `recordAt` (PI-02):**

Two call sites, each runs ONCE per fill emit row before FillRecord getters read `prevRecord`:

- Non-keyed gap branch in `hasNext()`: `if (hasSimplePrev) { baseCursor.recordAt(prevRecord, simplePrevRowId); }`
- Keyed fill branch in `emitNextFillRow()`: `if (value.getLong(HAS_PREV_SLOT) != 0) { baseCursor.recordAt(prevRecord, value.getLong(PREV_ROWID_SLOT)); }`

Constructor simplification drops the per-column `columnTypes` population loop and the `outputColToAggSlot` Arrays.fill + loop — only the `outputColToKeyPos` mapping survives.

### SqlCodeGenerator.generateFill

**Before (lines 3579-3595 pre-edit):**

```java
int aggColumnCount = 0;
for (int col = 0; col < columnCount; col++) {
    int mode = fillModes.getQuick(col);
    if (mode != SampleByFillRecordCursorFactory.FILL_KEY
            && col != timestampIndex) {
        aggColumnCount++;
    }
}

final ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
mapValueTypes.add(ColumnType.LONG); // slot 0: keyIndex
mapValueTypes.add(ColumnType.LONG); // slot 1: hasPrev
for (int i = 0; i < aggColumnCount; i++) {
    mapValueTypes.add(ColumnType.LONG); // per-agg prev slot
}
```

**After:**

```java
final ArrayColumnTypes mapValueTypes = new ArrayColumnTypes();
mapValueTypes.add(ColumnType.LONG); // slot 0: keyIndex
mapValueTypes.add(ColumnType.LONG); // slot 1: hasPrev
mapValueTypes.add(ColumnType.LONG); // slot 2: prevRowId
```

The `aggColumnCount` counter loop was only used to size mapValueTypes; it has no other readers in `generateFill`, so the entire counting block is gone.

## Exact Slot Schema Before/After

| Slot index | Before | After |
|---|---|---|
| 0 | KEY_INDEX_SLOT (LONG) | KEY_INDEX_SLOT (LONG) — unchanged |
| 1 | HAS_PREV_SLOT (LONG) | HAS_PREV_SLOT (LONG) — unchanged |
| 2 | PREV_START_SLOT (first per-agg prev, LONG) | PREV_ROWID_SLOT (LONG) — the single rowId slot |
| 3..N | per-agg prev LONGs (aggColumnCount slots) | gone; key columns start at MapRecord column 3 |
| `keyPosOffset` | `2 + aggSlot` (variable) | `3` (constant) |

## KIND_* Constants Removed

The pre-rewrite `readColumnAsLongBits` switch encoded an implicit dispatch over 10 type kinds. All are gone as explicit code paths:

- KIND_LONG_BITS (DOUBLE / FLOAT reinterpret)
- KIND_LONG (LONG, TIMESTAMP, DATE direct reads)
- KIND_INT (INT, IPv4, GEOINT)
- KIND_SHORT (SHORT, GEOSHORT)
- KIND_BYTE (BYTE, GEOBYTE)
- KIND_BOOLEAN
- KIND_CHAR
- KIND_GEOLONG
- KIND_DECIMAL (8/16/32/64)
- KIND_SYMBOL (implicit: was routed through retro-fallback, never reached `readColumnAsLongBits`)

Var-width kinds that were explicitly rejected at codegen time via `FallbackToLegacyException`:

- KIND_LONG128 / UUID
- KIND_DECIMAL128
- KIND_LONG256
- KIND_DECIMAL256
- KIND_STRING
- KIND_VARCHAR
- KIND_BIN
- KIND_ARRAY

All 18 of these types (counting overlaps) now read uniformly via `prevRecord.getXxx(col)` without any kind-specific code. The codegen-time rejection gate (`isFastPathPrevSupportedType`) stays in place — Plan 04 deletes it after Plan 03 cherry-picks the 13 per-type tests that validate end-to-end correctness for each var-width type on the fast path.

## Test Results

| Suite | Tests run | Failures | Errors | Skipped | Exit |
|---|---|---|---|---|---|
| `SampleByFillTest` (isolation) | 74 | 0 | 0 | 0 | SUCCESS |
| `SampleByTest` (isolation) | 302 | 1 | 0 | 0 | FAILURE (pre-existing) |
| `SampleByNanoTimestampTest` (isolation) | 279 | 0 | 0 | 0 | SUCCESS |
| `ExplainPlanTest` (isolation) | 522 | 0 | 0 | 2 | SUCCESS |
| `SampleByFillTest,SampleByTest` (combined) | 376 | 1 | 0 | 0 | FAILURE (pre-existing) |
| `SampleByTest,SampleByFillTest` (combined) | 376 | 1 | 0 | 0 | FAILURE (pre-existing) |
| `SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest` | 1177 | 1 | 0 | 2 | FAILURE (pre-existing) |

The single failing test in every run is `SampleByTest#testSampleByFillNeedFix` assertion #2 (CTE-wrap + outer-projection corruption). It reproduces identically to Plan 01's baseline:

- Same test method, same assertion (#2 at SampleByTest.java:6022).
- Same symptom: garbled `candle_start_time` values starting with `1970-01-01T...` and random 64-bit integers for `cnt`.
- Same reproduction in isolation — confirms the failure is independent of `chain.clear()` or the rowId rewrite.
- Scoped to Plan 05 per 13-CONTEXT.md D-06 and 13-RESEARCH.md section 5.1 (SEED-002 Defect 1).

Per the execution notes, this pre-existing failure is the user-approved carve-out from Plan 01's checkpoint (Option 1: accept with caveat).

## Acceptance Criteria Check

| Criterion | Required | Actual | Pass |
|---|---|---|---|
| `simplePrevRowId` match count | >= 3 | 5 | yes |
| `PREV_ROWID_SLOT` match count | >= 2 | 3 | yes |
| `private static final int PREV_ROWID_SLOT = 2` decl | exactly 1 | 1 (line 242) | yes |
| removed helpers match count | 0 | 0 | yes |
| `prevRecord = baseCursor.getRecordB()` | exactly 1 | 1 (line 617, inside the peek-success branch of `initialize()` — after `baseCursor.hasNext()` drove `buildChain()` for both paths) | yes |
| `baseCursor.recordAt(prevRecord` call sites | 1 or 2 | 2 call sites (+ 1 javadoc reference) | yes |
| mapValueTypes per-agg loop | absent | absent | yes |
| `slot 2: prevRowId` comment | exactly 1 | 1 (line 3585) | yes |
| Banner comments in diff | 0 | 0 | yes |
| `FallbackToLegacyException` count in SqlCodeGenerator | >= 4 | 5 (import + throw + 3 catches) | yes |
| KIND_* constants in fill factory | 0 | 0 | yes |
| Commit title prefix | no Conventional Commits | none ("Store FILL(PREV) ...") | yes |
| Build result | BUILD SUCCESS | BUILD SUCCESS | yes |

## Decisions Made

- **rowId is the single mechanism (D-01 locked).** The rewrite drops every per-type code path in favor of `prevRecord.getXxx(col)`.
- **prevRecord initialization at the post-buildChain point.** Consolidated to a single assignment inside the peek-success branch of `initialize()` — covers both keyed and non-keyed paths without duplication. Fulfills the Plan 02 Task 1 acceptance criterion of "exactly one match" for `prevRecord = baseCursor.getRecordB()`.
- **FILL_KEY for var-width types stays as pre-existing fallthrough.** When I first wrote the rewrite, I added FILL_KEY branches to `getArray`, `getBin`, `getBinLen` that delegated to `keysMapRecord`. Two tests (`testSampleFillPrevAllTypes`, `testSampleFillValueAllKeyTypes`) pinned to the pre-existing behavior failed because they expected `null` for BIN columns on fill rows. I trimmed the FILL_KEY additions for these three getters to preserve pre-existing semantics. Extending FILL_KEY to var-width types is a logical next step but is out of Plan 02's PREV-semantics scope and would change user-visible behavior in two regression-pinned tests.
- **Retro-fallback machinery kept.** `FallbackToLegacyException`, `prevSourceCols` construction loop, `isFastPathPrevSupportedType` codegen check, and the three try/catch sites remain. Plan 04 deletes them after Plan 03 validates the fast path across every currently-unsupported type via the 13 cherry-picked per-type tests.

## Plan 03 / Plan 04 / Plan 05 Readiness

- **Plan 03 (cherry-pick 13 per-type tests from `f43a3d7057`) is unblocked.** The fast path now reads every supported type uniformly via `prevRecord.getXxx(col)`. When Plan 03's 13 tests land, we expect all to show `Sample By Fill` in plan output (indicating fast-path usage) and return correct values.
- **Plan 04 (retro-fallback deletion) is unblocked pending Plan 03.** Once Plan 03 validates every currently-unsupported type against the fast path, Plan 04 can delete `FallbackToLegacyException`, `prevSourceCols`, `isFastPathPrevSupportedType`, the three try/catch sites, and `stashedSampleByNode`.
- **Plan 05 (testSampleByFillNeedFix restoration) is unblocked.** The pre-existing SEED-002 Defect 1 failure still reproduces identically on this branch state — confirming the rowId rewrite alone did NOT absorb it (per RESEARCH.md section 5.1 "MEDIUM confidence" on absorption). Plan 05 must investigate and fix the CTE-wrap + outer-projection corruption at whichever layer it lives (factory plumbing or column-index remapping).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 — Bug scope-trim] FILL_KEY var-width behavior preserved at pre-existing fallthrough**

- **Found during:** Task 2 verification step — initial test run of `SampleByTest` produced 3 failures.
- **Issue:** `testSampleFillPrevAllTypes` and `testSampleFillValueAllKeyTypes` failed because my initial rewrite added FILL_KEY delegation to `getArray`, `getBin`, `getBinLen` (reading from `keysMapRecord` when the column is a key). The tests are pinned to pre-existing behavior where these three getters returned `null`/`-1` for FILL_KEY (no FILL_KEY branch in the pre-rewrite code).
- **Fix:** Trimmed the FILL_KEY branches from `getArray`, `getBin`, `getBinLen`. The PREV branch stays (`prevRecord.getXxx(...)` for fill rows with saved prev). FILL_CONSTANT branch stays. The FILL_KEY fallthrough returns the pre-existing default (`null` / `-1`), which matches pre-rewrite behavior exactly.
- **Files modified:** `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java`.
- **Verification:** After the trim, `testSampleFillPrevAllTypes` and `testSampleFillValueAllKeyTypes` both pass. `SampleByFillTest` remains 74/74 green.
- **Committed in:** `d327d02d52`.

### Intentional Out-of-Scope Items

Per plan `<deliberately out of scope for this plan>`:

- `prevSourceCols` build loop in `SqlCodeGenerator.generateFill` stays.
- Codegen-time type-check loop that throws `FallbackToLegacyException` stays.
- `isFastPathPrevSupportedType` method stays.
- Three try/catch blocks at the `generateFill` call sites stay.
- `FallbackToLegacyException.java`, `QueryModel.stashedSampleByNode`, `SqlOptimiser.rewriteSampleBy` stash write — all stay.

Plan 04 handles these.

---

**Total deviations:** 1 (auto-trimmed scope; no checkpoint surfaced).
**Impact on plan:** Plan 02 ships on time with no behavioral regressions and no new test failures. Acceptance criteria all met. Plan 03, 04, 05 remain unblocked.

## Issues Encountered

- **First test run surfaced two FILL_KEY test regressions** (`testSampleFillPrevAllTypes`, `testSampleFillValueAllKeyTypes`). Both tests pin pre-existing behavior where `getArray`, `getBin`, `getBinLen` return `null`/`-1` on fill rows, including when the column is a FILL_KEY. My initial rewrite over-extended the FILL_KEY dispatch to these three var-width getters. Fix: revert the FILL_KEY branch add for those three getters — the PREV branch via `prevRecord` is the only new addition for them. Resolved without a checkpoint because the fix was a clean scope-trim that matched pre-rewrite behavior exactly.

## User Setup Required

None — internal refactor, no external service configuration.

## Next Phase Readiness

- **Plan 03 (cherry-pick per-type tests)** is unblocked. Expected outcome: all 13 tests show `Sample By Fill` in plan output and produce correct values via `prevRecord.getXxx()`.
- **Plan 04 (retro-fallback deletion)** is unblocked pending Plan 03's validation.
- **Plan 05 (testSampleByFillNeedFix restoration)** is unblocked. The rowId rewrite did NOT absorb SEED-002 Defect 1; Plan 05 must investigate and fix the CTE-wrap + outer-projection corruption at the factory-plumbing / column-index-remap layer.

---

*Phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi*
*Plan: 02*
*Completed: 2026-04-19*

## Self-Check: PASSED

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — FOUND (1196 lines; +12 net from 1184).
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — FOUND (10753 lines; -10 net from 10763).
- Commit `d327d02d52` — FOUND in `git log` with exact title
  `Store FILL(PREV) prev snapshots as chain rowIds`.
