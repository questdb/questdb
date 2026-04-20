---
phase: 13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi
verified: 2026-04-19T00:00:00Z
status: passed
score: 8/8 must-haves verified
overrides_applied: 0
---

# Phase 13: Migrate FILL(PREV) Snapshots from Materialized Values to rowId-Based Replay — Verification Report

**Phase Goal:** Replace per-type materialization of FILL(PREV) snapshots in the fast-path fill cursor with a single chain rowId per key, replayed on emit via `baseCursor.recordAt(prevRecord, prevRowId)`. Ship the prerequisite `SortedRecordCursor.chain.clear()` fix as its own standalone commit before the rewrite.

**Verified:** 2026-04-19T00:00:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `SampleByFillRecordCursorFactory` stores exactly one `prevRowId` per key (keyed map value slot) or one `simplePrevRowId` (non-keyed) — no per-type copied-value buffers | VERIFIED | `PREV_ROWID_SLOT = 2` declared at line 252; `private long simplePrevRowId = -1L` at line 289; no `long[] simplePrev` field, no `short[] columnTypes`, no `readColumnAsLongBits`, no `prevValue` method, no `KIND_*` constants — all removed per grep counts returning 0 |
| 2 | Gap-fill emit reads prev values via `baseCursor.recordAt(prevRecord, prevRowId)` then typed getters on `prevRecord`; uniform across all types | VERIFIED | Two recordAt call sites in `SampleByFillRecordCursorFactory.java`: line 446 (non-keyed path, `simplePrevRowId`) and line 523 (keyed path, `value.getLong(PREV_ROWID_SLOT)`). All 40+ FillRecord typed getters read uniformly via `prevRecord.getXxx(mode >= 0 ? mode : col)` — no per-type dispatch, no `KIND_*` switch |
| 3 | `SortedRecordCursor.of()` calls `chain.clear()` on reuse when `isOpen=true` — shipped as an independent commit before the fill rewrite | VERIFIED | `SortedRecordCursor.java:102` contains `chain.clear();` inside `else` branch of the `isOpen` check (lines 98-103). Commit `2bed27fef2` with title `fix(sql): clear chain in SortedRecordCursor.of() on reuse` shipped as phase-13 Commit 1 per D-07 |
| 4 | Retro-fallback mechanism from phase 12 is reassessed: if rowId unlocks all currently-unsupported PREV types, retro-fallback is deleted | VERIFIED | `FallbackToLegacyException.java` file does not exist. `grep -r FallbackToLegacyException core/src` → 0 matches. `grep -r stashedSampleByNode core/src` → 0 matches. `isFastPathPrevSupportedType`, `hasPrevWithUnsupportedType`, `isUnsupportedPrevType` all removed. Commit `1456fd7ba6` `Drop retro-fallback gates and machinery` landed as phase-13 Commit 4 |
| 5 | `recordAt` cached once per emit row (PI-02) | VERIFIED | `baseCursor.recordAt(prevRecord, ...)` called exactly twice in the fill factory (lines 446, 523), each at a single emit-row site (non-keyed gap branch in `hasNext()`, keyed fill branch in `emitNextFillRow()`). FillRecord getters never call `recordAt()` — they only read `prevRecord.getXxx(col)`. PI-02 invariant holds |
| 6 | Existing tests still pass: `SampleByTest`, `SampleByFillTest`, `SampleByNanoTimestampTest`, `ExplainPlanTest`, `RecordCursorMemoryUsageTest` | VERIFIED | Regression gate result provided: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` → 1193 tests, 0 failures, 0 errors, 2 skipped (pre-existing) |
| 7 | Seeds SEED-001 and SEED-002 are revisited and closed | VERIFIED | SEED-001 disposition: WR-01/WR-02/WR-03 obsoleted by retro-fallback deletion (Plan 04); WR-04 resolved in Plan 06 (precise chain-rejection position); Defect 3 resolved in Plan 06 (insufficient-fill grammar). SEED-002 disposition: Defect 1 (CTE+outer-projection corruption) resolved in Plan 05 via alias-mapping fix in `generateFill`; Defect 2 (toTop() state leak) absorbed by Plan 02 rowId rewrite |
| 8 | All 13 per-type FILL(PREV) tests listed in `13-VALIDATION-INPUTS.md` land on this branch and pass on the fast path (plan output shows `Sample By Fill`) | VERIFIED | All 13 tests present in `SampleByFillTest.java` (grep returns 13 matches). Each test body contains `Assert.assertTrue(Chars.contains(getPlanSink(query).getSink(), "Sample By Fill"))` assertion (plan-text assertion added in Plan 04 after gates were lifted). `Sample By Fill` substring grep across the test file returns 18 matches. All 13 tests pass as part of regression gate |

**Score:** 8/8 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursor.java` | `chain.clear()` in else branch of `of()` when `isOpen==true` | VERIFIED | Line 102: `chain.clear();` inside `else` branch (lines 98-103 match expected pre/post structure exactly) |
| `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` | 1196-line factory with `simplePrevRowId`, `PREV_ROWID_SLOT`, uniform `prevRecord` getter delegation | VERIFIED | 1206 lines. `simplePrevRowId` (line 289), `PREV_ROWID_SLOT = 2` (line 252), `Record prevRecord` (line 288), 40+ FillRecord getters uniformly delegate via `prevRecord.getXxx(mode >= 0 ? mode : col)`. No KIND_* constants. No `long[] simplePrev`. No `readColumnAsLongBits`. No `prevValue` method |
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | `generateFill()` with 3-slot MapValue schema, alias-mapping fix, WR-04 perColFillNodes tracking, Defect 3 grammar check | VERIFIED | 10737 lines. Three `mapValueTypes.add(ColumnType.LONG)` calls at lines 3613-3615 with comments `slot 0: keyIndex / slot 1: hasPrev / slot 2: prevRowId`. No per-agg loop. `factoryColToUserFillIdx` and `userFillIdx` alias-mapping built at lines 3426-3460. `perColFillNodes` ObjList at line 3478. `not enough fill values` check at line 3568. Chain-rejection with `perColFillNodes.getQuick(col)` precise-position at lines 3579-3585 |
| `core/src/main/java/io/questdb/griffin/FallbackToLegacyException.java` | DELETED | VERIFIED | File does not exist (verified via `find`) |
| `core/src/main/java/io/questdb/griffin/model/QueryModel.java` | `stashedSampleByNode` field + accessors removed | VERIFIED | `grep -r stashedSampleByNode core/src` returns 0 matches |
| `core/src/main/java/io/questdb/griffin/model/IQueryModel.java` | `getStashedSampleByNode`/`setStashedSampleByNode` declarations removed | VERIFIED | Zero grep matches for `getStashedSampleByNode` or `setStashedSampleByNode` across `core/src` |
| `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` | `rewriteSampleBy` stash write + `hasPrevWithUnsupportedType`/`isUnsupportedPrevType` gates removed | VERIFIED | Zero grep matches for any of the three symbols across `core/src` |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` | 13 new per-type tests + `testFillInsufficientFillValues` + updated chain-rejection tests; 7 retro-fallback guard tests deleted | VERIFIED | All 13 per-type test methods present (line refs: 944, 1058, 1082, 1346, 1770, 1795, 1822, 1847, 1873, 1899, 1924, 1948, 1973). `testFillInsufficientFillValues` at line 144. `testFillPrevRejectMutualChain` at 1640 (position 55), `testFillPrevRejectThreeHopChain` at 1686 (position 65). All 7 retro-fallback guard tests (`testFillPrevSymbolLegacyFallback`, `testFillPrevCrossColumnUnsupportedFallback`, `testFillPrevDecimal128LegacyFallback`, `testFillPrevDecimal256LegacyFallback`, `testFillPrevLong256LegacyFallback`, `testFillPrevUuidLegacyFallback`, `testFillPrevCaseOverDecimalFallback`, `testFillPrevExpressionArgDecimal128Fallback`, `testFillPrevExpressionArgStringFallback`, `testFillPrevIntervalFallback`, `testFillPrevSymbolLegacyFallbackNano`) absent (zero grep matches) |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` | `testSampleByFillNeedFix` with 3-row `assertQuery` form for assertions #1 & #2, `assertException(...530, "not enough fill values")` for assertion #3 | VERIFIED | Lines 5943-6091: assertion #1 (line 5972) is `assertQuery` with 3 rows and `sizeExpected=true`; assertion #2 (line 6018) is `assertQuery` with 3 rows and `sizeExpected=true`; assertion #3 (line 6053) is `assertException(..., 530, "not enough fill values")` |
| `.planning/phases/13-.../13-INVESTIGATION.md` | D-02 investigation report confirming candidate (a) verdict | VERIFIED | 415 lines. Explicit verdict at line 9: `Verdict: (a) chain.clear() on SortedRecordCursor.of() reuse is sufficient. SortedRecordCursor is the chosen vehicle for Plan 02's rowId rewrite.` All four source-branch commits (fe487c06a9, a437758268, 1a40aa89af, 4ebfa3243c) read and quoted |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `SortedRecordCursor.of()` `isOpen==true` branch | `chain.clear() -> RecordTreeChain.clear() -> RecordChain.clear() -> mem.close()` | direct method call on `chain` field | WIRED | `chain.clear()` at SortedRecordCursor.java:102 inside the `else` branch of the `isOpen` check; `chain` is the `RecordTreeChain` field already imported in the file |
| `SampleByFillCursor.emitNextFillRow` / `hasNext` | `baseCursor.recordAt(prevRecord, prevRowId)` | single call before FillRecord getters read prevRecord | WIRED | Two call sites: line 446 (non-keyed gap branch with `simplePrevRowId`), line 523 (keyed fill branch with `value.getLong(PREV_ROWID_SLOT)`). Both positioned before the FillRecord getters fire (PI-02) |
| `SqlCodeGenerator.generateFill` mapValueTypes block | exactly three `mapValueTypes.add(ColumnType.LONG)` calls | no for loop over aggColumnCount for slot sizing | WIRED | Lines 3613-3615: three consecutive `mapValueTypes.add(ColumnType.LONG)` calls with `slot 0: keyIndex`, `slot 1: hasPrev`, `slot 2: prevRowId` comments. No for-loop over `aggColumnCount` for slot sizing. Schema matches `keyPosOffset = 3` constant |
| `generateFill` per-column fill-spec loop | `factoryColToUserFillIdx.getQuick(col)` lookup (Plan 05 Defect 1 fix) | alias-based factory-to-user index map | WIRED | Lines 3426-3460: `factoryColToUserFillIdx` built by walking `bottomUpCols` in user order, advancing `userFillIdx` per non-key-non-timestamp column, recording `factoryColToUserFillIdx.setQuick(factoryIdx, userFillIdx)` when column is in factory metadata. Line 3489: `final int fillIdx = factoryColToUserFillIdx.getQuick(col)` consumes the lookup |
| `generateFill` chain-rejection throw | `perColFillNodes.getQuick(col).position` (WR-04) | per-column ExpressionNode tracking | WIRED | Lines 3478-3481: `perColFillNodes` ObjList pre-populated with nulls. Line 3542: `perColFillNodes.setQuick(col, fillExpr)` populated at the cross-column PREV assignment. Lines 3579-3585: chain check reads `perColFillNodes.getQuick(col)` with null-safe fallback to `fillValuesExprs.getQuick(0).position` |
| `generateFill` Defect 3 grammar check | positioned `SqlException` "not enough fill values" | new guard after per-column build loop | WIRED | Line 3568: `if (fillValuesExprs.size() > 1 && fillValuesExprs.size() < aggNonKeyCount)` guard throws `SqlException.$(fillValuesExprs.getQuick(0).position, "not enough fill values")`. `aggNonKeyCount` computed at line 3470 from the `userFillIdx` counter end-state, giving exact non-key-aggregate count from user SELECT list |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|---------------------|--------|
| `SampleByFillCursor.FillRecord` typed getters | `prevRecord` | `baseCursor.getRecordB()` assigned in `initialize()` after `baseCursor.toTop()` (line 626), repositioned via `baseCursor.recordAt(prevRecord, rowId)` per emit row | Yes — `prevRecord` is a `SortedRecordCursor.recordB` delegate that reads directly from chain memory populated by `RecordTreeChain.put` during pass 1 (buildChain). `chain.clear()` ensures fresh chain on reuse. All 13 per-type tests confirm correct carry-forward values for SYMBOL, STRING, VARCHAR, UUID, Long256, Decimal128, Decimal256, Array | FLOWING |
| `SqlCodeGenerator.generateFill` MapValue schema | `PREV_ROWID_SLOT` content (one LONG per key) | `updateKeyPrevRowId(value, record)` sets `value.putLong(PREV_ROWID_SLOT, record.getRowId())` on every data row during pass-1 drain (line 678). `record` is a chain-record from `SortedRecordCursor` so `getRowId()` returns a valid chain offset | Yes — drained pass-1 records have stable chain rowIds (verified by Plan 01's combined-suite tests). Values flow to the keyed emit branch at line 523 where `baseCursor.recordAt(prevRecord, value.getLong(PREV_ROWID_SLOT))` repositions `prevRecord` | FLOWING |

### Behavioral Spot-Checks

Phase 13 produces library code (a refactor inside the QuestDB server process). The regression gate (provided in the verification prompt) is the definitive behavioral signal.

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Full phase test suite passes | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,RecordCursorMemoryUsageTest' test` | 1193 tests, 0 failures, 0 errors, 2 skipped | PASS |
| 13 per-type FILL(PREV) tests present in test file | grep for 13 named tests in SampleByFillTest.java | Matches 13 | PASS |
| Retro-fallback machinery deleted | `find core/src/main/java -name FallbackToLegacyException.java` | Empty (file gone) | PASS |
| Zero residual references to deleted symbols | grep for `FallbackToLegacyException`, `stashedSampleByNode`, `isFastPathPrevSupportedType`, `hasPrevWithUnsupportedType`, `isUnsupportedPrevType` across `core/src` | 0 total matches | PASS |
| `chain.clear()` present in the correct `of()` branch | grep with context around `chain.clear()` in SortedRecordCursor.java | Line 102 inside `else` branch of `isOpen` check | PASS |

### Requirements Coverage

Phase 13 is an internal refactor — per PLAN frontmatter, all six plans declare `requirements: [INTERNAL-REFACTOR-PH13]`. REQUIREMENTS.md does not include a row for INTERNAL-REFACTOR-PH13 (it is a phase-local placeholder, not a v1 functional requirement). The ROADMAP phase description states: "Internal refactor — builds on PTSF-01..06 and COR-01..04; no new requirement IDs".

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| INTERNAL-REFACTOR-PH13 | 13-01..13-06 PLAN frontmatter | Phase-local placeholder for the rowId migration refactor. Not a v1 functional requirement. | SATISFIED | All 8 phase-13 Success Criteria verified above (1-8). Underlying PTSF-01..06 and COR-01..04 requirements remain SATISFIED under the new implementation (verified via 1193-test regression gate) |
| PTSF-02 (inherited) | Phase 7/12 — type matrix | Explicit source type support matrix — numeric on fast path, unsupported types fell back to legacy | REPLACED BY ROWID | Phase 13 deletes the type-gate (the rowId mechanism unlocks every previously-unsupported type). 13 new per-type tests confirm fast-path routing for SYMBOL, STRING, VARCHAR, UUID, Long256, Decimal128, Decimal256, Array — all previously retro-fallback routed. Requirement semantics expanded, not regressed |
| PTSF-04 (inherited) | Phase 7/12 — unsupported-type fallback | `prev(alias)` referencing unsupported type triggers legacy path fallback | REPLACED BY ROWID | Same mechanism change as PTSF-02. Fast-path now handles all types via `prevRecord.getXxx(col)`; "fallback for unsupported types" is no longer a code path. No existing tests regress |
| COR-01/02/04 (inherited) | Phase 5 — correctness | All 302 existing SampleByTest pass; no memory leaks; output matches cursor path | SATISFIED | Regression gate: 1193 tests across 5 suites, 0 failures, 0 errors |

No requirement IDs were orphaned. The placeholder `INTERNAL-REFACTOR-PH13` covers all of Phase 13's Success Criteria.

### Anti-Patterns Found

Code-review findings from `13-REVIEW.md` (0 critical, 1 warning, 3 info). Reproduced here for goal-backward traceability.

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | 3568 | Defect 3 grammar check `fillValuesExprs.size() > 1 && fillValuesExprs.size() < aggNonKeyCount` misses the single non-null constant broadcast case (e.g. `FILL(0)` with 2 aggregates). | Warning | Scope gap relative to master's legacy `SampleByFillValueRecordCursorFactory`, not a Phase 13 regression. Pre-existing behavior, flagged by reviewer as WR-01 for follow-up (see 13-REVIEW.md WR-01). Does NOT block Phase 13 goal achievement because all eight Success Criteria remain verified. Recommend a follow-up seed/phase |
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | 171 | Dead import `import io.questdb.griffin.engine.groupby.FillRangeRecordCursorFactory;` | Info | Cosmetic — no functional impact. Candidate for follow-up cleanup |
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | 3500-3502, 3553 | Redundant `fillIdx >= 0` conjuncts — preceding early-return guarantees the invariant | Info | Harmless; defensive coding. Cosmetic |
| `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (10 occ.) and `SqlCodeGenerator.java` (8 occ.) | various | Em-dash (non-ASCII) characters in Javadoc/`//` comments (not in `LOG.*()` or `SqlException` text) | Info | CLAUDE.md's ASCII-only rule scopes to log/error messages. Comments are not in the rule's domain. Optional cleanup |

Phase 13 completed successfully with these items noted but not blocking goal achievement. The Warning item (WR-01) is a pre-existing scope gap that Phase 13 did not introduce; the reviewer notes "Pre-existing behavior had the same shape, so this is not a Phase 13 regression".

### Human Verification Required

No items require human verification. All Phase 13 Success Criteria are programmatically verifiable through:

- File existence / non-existence checks (retro-fallback deletion)
- Grep-based symbol counts (13 per-type tests, zero `FallbackToLegacyException` references)
- Content inspection (chain.clear() in the correct branch, recordAt call site count)
- Regression gate (5-suite, 1193 tests, 0 failures)

Visual/UX concerns do not apply — this is a parser/codegen/cursor internal refactor with no user-facing UI surface. The `Sample By Fill` plan-text assertions in the 13 per-type tests provide the "plan output looks right" evidence programmatically.

### Gaps Summary

No gaps. Phase 13 delivered the complete six-commit sequence per D-07:

1. Commit 1 (`2bed27fef2`): chain.clear() fix in SortedRecordCursor.of()
2. Commit 2 (`d327d02d52`): Store FILL(PREV) prev snapshots as chain rowIds
3. Commit 3 (`99240223bf`): Add per-type FILL(PREV) data-correctness tests (13 tests)
4. Commit 4 (`1456fd7ba6`): Drop retro-fallback gates and machinery
5. Commit 5 (`b8a2bee9ee`): Fix FILL spec mapping for reordered aggregates
6. Commit 6 (`ff5b354bd1`): Tighten FILL grammar: precise errors and short fill list rejection

All 8 ROADMAP Success Criteria are verified. The rowId rewrite unlocks every previously retro-fallback-routed type on the fast path, confirmed by the 13 per-type tests. The `chain.clear()` fix is in place and bisectable. SEED-002 Defects 1 and 2 are closed (Defect 1 fixed in Plan 05 via alias-mapping, Defect 2 absorbed by the rowId rewrite). SEED-001 disposition per D-05 is closed (WR-01/WR-02/WR-03 obsoleted by retro-fallback deletion; WR-04 resolved in Plan 06; Defect 3 resolved in Plan 06). Phase 12 Success Criterion #1 is closed (`testSampleByFillNeedFix` matches master's 3-row + assertException form across all three assertions). Full regression gate green (1193 tests, 0 failures).

The code-review Warning (Defect 3 grammar gap for single non-null constant) is a pre-existing scope gap and does not affect Phase 13 goal achievement — all goal-backward truths hold regardless.

---

*Verified: 2026-04-19T00:00:00Z*
*Verifier: Claude (gsd-verifier)*
