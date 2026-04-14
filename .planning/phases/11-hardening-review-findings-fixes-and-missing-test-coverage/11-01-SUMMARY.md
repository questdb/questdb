---
phase: 11-hardening-review-findings-fixes-and-missing-test-coverage
plan: 01
subsystem: griffin/fill-cursor
tags: [hardening, review-fixes, sample-by, fill, cleanup]
retroactive: true
dependency_graph:
  requires: [10-fix-offset-aware-bucket-alignment]
  provides: [uuid-fill-key-dispatch, geo-decimal-null-sentinels, cte-fill-reclassification-test]
  affects: [SampleByFillRecordCursorFactory, SqlCodeGenerator, SampleByFillTest, SampleByTest, SampleByNanoTimestampTest]
tech_stack:
  added: []
  patterns: [FILL_KEY-dispatch-128-256, GeoHashes-null-sentinels, Decimals-null-sentinels, Function-ownership-transfer]
key_files:
  modified:
    - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java
    - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java
    - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java
decisions:
  - "Extend FILL_KEY dispatch to all 128/256-bit getters (UUID, Long256, Decimal128/256) — mirrors the pattern already used for getInt/getLong/getSymbol"
  - "Use GeoHashes.*_NULL and Decimals.*_NULL constants instead of 0 or Numbers.*_NULL so null sentinels are distinguishable from legitimate zeros"
  - "Transfer Function ownership from fillValues to constantFillFuncs by nulling the source slot, preventing double-close on error paths"
  - "Keep the late-codegen safety-net reclassification (SYMBOL → FILL_KEY) because the optimizer gate does not cover misidentified key columns inside CTE/subquery contexts"
  - "Preserve assertQuery test structure when only values changed; do not convert to assertSql just to shrink diffs"
metrics:
  duration: "~3.5h (spread across 18:23 → 21:57)"
  completed: 2026-04-13
  tasks_completed: 3
  tasks_total: 4
  files_modified: 5
  commits: 3
---

# Phase 11 Plan 01: Hardening — Review Findings & Missing Test Coverage — Summary

**Note on timeline:** This phase was executed ad-hoc while `/review-pr` findings were in-context during the PR #6946 closing session. The CONTEXT.md was created afterward via `/gsd-add-phase hardening`; this PLAN.md and SUMMARY.md are retroactive back-fills reconciling GSD metadata with the three commits (`2125201f30`, `28b00e8340`, `a6355c3e65`) that actually landed the work. `/gsd-forensics` on 2026-04-14 documented the discrepancy.

## What Changed

### SampleByFillRecordCursorFactory.java

`FillRecord` inner class gained FILL_KEY dispatch on every getter that carries a 128/256-bit value:

- `getLong128Hi`, `getLong128Lo` — UUID keys now emit the real hi/lo halves instead of null.
- `getDecimal128(col, sink)`, `getDecimal256(col, sink)` — dispatch sink writes from `keysMapRecord` for DECIMAL128/DECIMAL256 keys.
- `getLong256(col)`, `getLong256A(col)`, `getLong256B(col)` — Long256 keys reach the caller.

Null sentinels corrected in the same class:

| Getter | Before | After |
|--------|--------|-------|
| `getGeoByte` | `0` | `GeoHashes.BYTE_NULL` |
| `getGeoShort` | `0` | `GeoHashes.SHORT_NULL` |
| `getGeoInt` | `Numbers.INT_NULL` | `GeoHashes.INT_NULL` |
| `getGeoLong` | `Numbers.LONG_NULL` | `GeoHashes.NULL` |
| `getDecimal8` | `0` | `Decimals.DECIMAL8_NULL` |
| `getDecimal16` | `0` | `Decimals.DECIMAL16_NULL` |
| `getDecimal32` | `Numbers.INT_NULL` | `Decimals.DECIMAL32_NULL` |
| `getDecimal64` | `Numbers.LONG_NULL` | `Decimals.DECIMAL64_NULL` |

Cleanup: `baseCursorExhausted` renamed to `isBaseCursorExhausted` (CLAUDE.md boolean convention); `SampleByFillCursor` fields reordered alphabetically within visibility group.

Imports added: `io.questdb.cairo.GeoHashes`, `io.questdb.std.Decimals`.

### SqlCodeGenerator.java

`generateFill()` transfers `Function` ownership from `fillValues` to `constantFillFuncs` by nulling the source slot after the copy, so an exception during fill-generation cannot double-close a native function.

Dead code removed: `guardAgainstFillWithKeyedGroupBy` (21 lines, zero callers since phase 3).

Safety-net reclassification (SYMBOL column → FILL_KEY late in codegen) retained, with the comment extended to explain *why* it is still needed: the optimizer gate blocks genuine aggregates with unsupported PREV types, so the safety net only fires for key columns misidentified as aggregates inside CTE/subquery contexts.

### SampleByFillTest.java

Three new regression tests (visible in the current file at lines 51, 360, 805):

- `testFillNullDstSparseData` — Europe/Riga DST fall-back with sparse data; asserts fill rows generate during the duplicated hour without duplication or gaps.
- `testFillNullKeyedWithNullKey` — NULL SYMBOL key value forms its own fill group (cartesian product includes a NULL row per bucket).
- `testFillPrevKeyedCte` — FILL(PREV) wrapped in a CTE; SYMBOL key column stays classified as FILL_KEY rather than silently reclassified as an aggregate.

### SampleByTest.java and SampleByNanoTimestampTest.java

Value-only updates to match the fixed FillRecord output (`assertQuery` structure preserved):

- Geo PREV null rendering: `"000"` → `""` — reflects `GeoHashes.*_NULL` rendering as empty string.
- Long256 key fill-row rendering: `""` → correct hex values — reflects the new FILL_KEY dispatch reaching `keysMapRecord`.

Nano mirror file got the same value updates so micro/nano timestamp paths stay in lockstep.

## Deviations from Plan

Because this plan is retroactive, "deviations" are recorded against the CONTEXT.md (the only forward-looking artifact that existed while work was in progress).

### Extra scope not declared in CONTEXT.md

1. **Function ownership transfer (Task 2 item 1).** Surfaced as a `/review-pr` finding mid-session; not in CONTEXT.md's six items. Kept in this phase because it is a correctness fix uncovered by the same review pass.
2. **`guardAgainstFillWithKeyedGroupBy` removal (Task 2 item 2).** Ditto — review finding, not in CONTEXT.
3. **Safety-net comment expansion (Task 2 item 3).** Ditto.
4. **`baseCursorExhausted → isBaseCursorExhausted` rename (Task 3).** Convention fix, not in CONTEXT.
5. **SampleByFillCursor field reordering (Task 3).** Style fix, not in CONTEXT.
6. **SampleByNanoTimestampTest mirror of value updates (Task 3).** Consequence of the FILL_KEY dispatch fix; CONTEXT only mentioned SampleByTest-class changes implicitly.

None of these expansions introduced new threats; all are covered by the existing test suites.

### Premature test-expectation sweep

During an interim commit, expectations in `SampleByTest.java` were converted from `assertQuery` to `assertSql` to "shrink the diff." The user flagged this as scope creep. The final commit (`a6355c3e65`) reverted the structural change and kept only value-level updates inside the original `assertQuery` calls. Lesson captured in `feedback_ci_fix_caution.md`.

### Task 4 (re-run /review-pr) not executed

Planned as a manual verification step but not yet run. Captured as an open item below.

## Verification Results

- PR #6946 CI: **green** across all 50 checks (280,124 tests, 0 failures) after the three commits.
- `SampleByFillTest`: 43/43 pass (including the three new tests).
- `SampleByTest`: 302/302 pass with updated expectations.
- `SampleByNanoTimestampTest`: 279/279 pass with mirrored expectations.
- `/review-pr`: **not re-run** — Success criterion #7 from ROADMAP phase 11 remains unverified. Not a correctness blocker (earlier reviews are the ones these fixes address) but flagged as follow-up.

## Commits

| Task(s) | Commit | Description |
|---------|--------|-------------|
| 1 (partial), 3 (tests) | `2125201f30` | Fix fill record null sentinels and key dispatch |
| 2 | `28b00e8340` | Fix review findings: ownership, dead code, safety net |
| 1 (Decimal32/64), 3 | `a6355c3e65` | Fix minor review findings and update test expectations |

All three commits sit atop merge `de4bc9d134` (master merge on the same day).

## Open Items

- Task 4: Run `/review-pr 6946 from scratch, ignoring planning folder` to verify success criterion #7.
- PR #6946 body still references `FillPrevRangeRecordCursorFactory`, which no longer exists — update before merge.
- `.planning/` files are in the PR diff despite being gitignored; decide whether to scrub them or leave them as project-archaeology artifact.
- Add `Performance` label to PR #6946 (missing per handoff note).

## Self-Check: PASSED

All five modified files verified present. Three commits (`2125201f30`, `28b00e8340`, `a6355c3e65`) verified in git history. Three new tests (`testFillNullDstSparseData`, `testFillNullKeyedWithNullKey`, `testFillPrevKeyedCte`) verified in `SampleByFillTest.java` at lines 51, 360, 805.
