# Roadmap: SAMPLE BY Fill on GROUP BY Fast Path

## Overview

QuestDB's SAMPLE BY FILL queries now execute on the parallel GROUP BY fast path instead of the sequential cursor path. The optimizer rewrites SAMPLE BY to GROUP BY with `timestamp_floor_utc`, and a streaming fill cursor inserts gap-filled rows above the sorted group-by output. Phases 1–6 implemented the core cursor (non-keyed, keyed, cross-column PREV, FROM/TO ranges); phases 7–10 hardened it against type-safety issues, plan/test regressions across seven suites, critical review findings (geo PREV, recursion, NPE), and offset-aware bucket alignment; phase 11 closed out the remaining review findings and added missing test coverage. All 11 phases complete on PR #6946.

## Phase Summary

- [x] **Phase 1: Optimizer Gate** — Relax optimizer to rewrite FILL(PREV) and keyed FILL to GROUP BY, preserve ORDER BY
- [x] **Phase 2: Non-keyed Fill Cursor** — Streaming fill cursor for non-keyed FILL(NULL/PREV/VALUE) with DST and FROM/TO support
- [x] **Phase 3: Keyed Fill Cursor** — Map-based key discovery, cartesian product emission, per-key prev tracking
- [x] **Phase 4: Cross-Column Prev** — FILL(PREV) referencing a different column from the previous bucket
- [x] **Phase 5: Verification and Hardening** — Absorbed by phases 7–10 (see 05-01-SUMMARY.md)
- [x] **Phase 6: Keyed Fill with FROM/TO Range** — Cartesian product across [FROM, TO) including leading/trailing fill
- [x] **Phase 7: PREV Type-Safe Fast Path** — Per-column PREV snapshot with type matrix + legacy fallback for unsupported types
- [x] **Phase 8: Fix Remaining Test Regressions** — 81 failures across 7 suites (plan text, factory classes, nano timestamp)
- [x] **Phase 9: Fix Critical Review Findings** — Geo PREV silent null, findValue NPE guard, recursive hasNext stack overflow
- [x] **Phase 10: Fix Offset-Aware Bucket Alignment** — Propagate calendar offset so sampler grid matches timestamp_floor_utc
- [x] **Phase 11: Hardening — Review Findings & Missing Test Coverage** — UUID FILL_KEY dispatch, geo/decimal null sentinels, NULL-key/CTE/sparse-DST tests
- [x] **Phase 12: Replace safety-net reclassification with legacy fallback and tighten optimizer PREV gate** — Retro-fallback mechanism, Tier 1 gate tightening, FILL(PREV, PREV(...)) grammar rules, 19 regression tests, code-quality sweep (completed 2026-04-17)
- [ ] **Phase 13: Migrate FILL(PREV) snapshots from materialized values to rowId-based replay** — Replace per-type snapshot materialization in `SampleByFillRecordCursorFactory` with a single chain rowId per key, read lazily via `recordAt`. Ship prerequisite `SortedRecordCursor.chain.clear()` fix as its own commit. Borrowed from `sm_fill_prev_fast_all_types` branch (research verdict GO, candidate a)

## Phase Details

### Phase 1: Optimizer Gate
**Goal**: The optimizer rewrites FILL(PREV) and keyed FILL queries to the GROUP BY fast path instead of falling back to the cursor path
**Depends on**: Nothing (first phase)
**Requirements**: OPT-01, OPT-02, OPT-03
**Success Criteria** (what must be TRUE):
  1. `SELECT ts, avg(val) FROM t SAMPLE BY 1h FILL(PREV)` produces a query plan with `Async Group By` (not `SampleByFillPrev`)
  2. `SELECT ts, key, avg(val) FROM t SAMPLE BY 1h FILL(NULL)` produces a query plan with `Async Group By` (not `SampleByFillNull`)
  3. ORDER BY on the rewritten GROUP BY model is preserved through `optimiseOrderBy` (the sort node is not dropped)
  4. `FILL(LINEAR)` queries still fall through to the cursor path (optimizer does not rewrite them)
**Plans:** 0/0 plans complete (gate-only phase, landed directly)

### Phase 2: Non-keyed Fill Cursor
**Goal**: Non-keyed SAMPLE BY FILL queries produce correct, time-ordered output with gap-filled rows on the fast path for all fill modes and edge cases
**Depends on**: Phase 1
**Requirements**: FILL-01, FILL-02, FILL-03, FILL-04, FILL-05, GEN-01, GEN-02, GEN-03, GEN-04
**Success Criteria** (what must be TRUE):
  1. `SELECT ts, avg(val) FROM t SAMPLE BY 1h FILL(NULL)` emits null-valued rows for every missing bucket between first and last data point
  2. `SELECT ts, avg(val) FROM t SAMPLE BY 1h FILL(PREV)` carries forward the previous bucket's aggregate value into gap rows
  3. `SELECT ts, avg(val) FROM t SAMPLE BY 1h FILL(0)` emits constant-filled rows (value 0) for missing buckets
  4. `FILL(NULL) FROM '2024-01-01' TO '2024-01-02'` emits leading fill rows before the first data point and trailing fill rows after the last
  5. A FILL query with `ALIGN TO CALENDAR TIME ZONE 'Europe/Berlin'` crossing a DST fall-back transition produces correctly ordered, non-duplicated output
**Plans:** 1/1 plans complete
Plans:
- [x] 02-01-PLAN.md — Fix infinite loop + resource leaks, add assertion tests for NULL/VALUE/FROM-TO/DST

### Phase 3: Keyed Fill Cursor
**Goal**: Keyed SAMPLE BY FILL queries produce the cartesian product of all unique keys and all time buckets, with per-key prev tracking
**Depends on**: Phase 2
**Requirements**: KEY-01, KEY-02, KEY-03, KEY-04, KEY-05
**Success Criteria** (what must be TRUE):
  1. `SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(NULL)` emits a row for every (city, bucket) pair, including cities absent from a given bucket
  2. `FILL(PREV)` with keys carries forward each key's own previous value independently (London's prev does not bleed into Paris)
  3. Key order within each bucket is stable and consistent across all buckets
  4. Key column values in fill rows match the actual key values discovered during pass 1 (not null or garbage)
  5. Fill rows for missing (key, bucket) pairs use the correct fill mode (null, constant, or per-key prev)
**Plans:** 1/1 plans complete
Plans:
- [x] 03-01-PLAN.md — Optimizer gates, keyed fill cursor implementation, and integration tests

### Phase 4: Cross-Column Prev
**Goal**: FILL(PREV) can reference a specific source column from the previous bucket rather than always filling from self
**Depends on**: Phase 3
**Requirements**: XPREV-01, XPREV-02
**Success Criteria** (what must be TRUE):
  1. Syntax for cross-column prev is defined and documented (e.g., `FILL(PREV(col_name))` or equivalent)
  2. A query using cross-column prev fills a gap row's column with the value of a different column from the previous bucket
  3. Cross-column prev works correctly with both keyed and non-keyed queries
**Plans:** 1/1 plans complete
Plans:
- [x] 04-01-PLAN.md — Optimizer gate relaxation, PREV(col_name) detection in generateFill, keyed prevValue fix, cross-column tests

### Phase 5: Verification and Hardening
**Goal**: The fast-path fill implementation passes all existing tests and produces output identical to the cursor path
**Depends on**: Phase 4
**Requirements**: COR-01, COR-02, COR-03, COR-04
**Status**: Absorbed by phases 7–10. The plan's four `must_have.truths` were all satisfied cumulatively by phase 7 (PREV type-safety), phase 8 (plan-text / factory-class sweep across 7 suites), phase 9 (geo PREV, NPE, recursion), and phase 10 (offset alignment). See `05-01-SUMMARY.md`.
**Success Criteria** (what must be TRUE):
  1. All 302 existing SampleByTest tests pass without modification (behavioral parity with cursor path)
  2. `assertMemoryLeak` passes for all fill-related tests (no native memory leaks)
  3. Query plans for eligible FILL queries show `Async Group By` or `GroupBy vectorized` (parallel execution confirmed)
  4. Resource leak in `generateFill` error path is fixed (`sorted` factory and `constantFillFuncs` freed on exception)
**Plans:** 1/1 plans complete (absorbed — see SUMMARY)
Plans:
- [x] 05-01-PLAN.md — Superseded by finer-grained phases 7–10; no direct commits. See 05-01-SUMMARY.md for attribution

### Phase 6: Keyed Fill with FROM/TO Range
**Goal**: Keyed FILL queries with FROM/TO range emit the cartesian product of all keys for every bucket in the range, including leading and trailing fill rows for all keys
**Depends on**: Phase 3
**Requirements**: KFTR-01, KFTR-02, KFTR-03, KFTR-04, KFTR-05
**Success Criteria** (what must be TRUE):
  1. `SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FROM '2024-01-01' TO '2024-01-02' FILL(NULL)` emits all keys for every bucket in the [FROM, TO) range
  2. Leading fill rows (FROM before first data) include all keys with null/constant/prev fill values
  3. Trailing fill rows (TO after last data) include all keys with correct fill values
  4. Per-key FILL(PREV) tracks correctly across the full FROM/TO range (including leading buckets before any key has data)
  5. Architecture validation: output matches the cursor-based path for equivalent queries
**Plans:** 1/1 plans complete
Plans:
- [x] 06-01-PLAN.md — Fix SIGSEGV crash + 8 keyed FROM/TO fill tests

### Phase 7: PREV Type-Safe Fast Path
**Goal**: Make fast-path FILL(PREV/prev(alias)) type-safe by adding a source type support matrix, per-column snapshot tracking, and legacy fallback for unsupported types
**Depends on**: Phase 4
**Requirements**: PTSF-01, PTSF-02, PTSF-03, PTSF-04, PTSF-05, PTSF-06
**Success Criteria** (what must be TRUE):
  1. No UnsupportedOperationException or implicit-cast failures from fast-path PREV
  2. Mixed-fill query (one PREV numeric + one non-PREV string/symbol aggregate) does not crash
  3. `prev(alias)` referencing unsupported type (STRING/SYMBOL/VARCHAR/ARRAY) falls back to legacy path (plan shows `Sample By`, not `Async Group By`)
  4. Numeric `prev(alias)` with calendar + timezone stays on fast path
  5. No behavior regressions for existing FILL(PREV) tests (SampleByTest + SampleByFillTest)
  6. Nanosecond timestamp tests mirror microsecond equivalents
**Plans:** 1/1 plans complete
Plans:
- [x] 07-01-PLAN.md — Per-column snapshot, type matrix, optimizer gate, legacy fallback, mixed-fill and nano tests

### Phase 8: Fix Remaining Test Regressions
**Goal**: Fix 81 test failures across 7 suites caused by plan text changes, factory class changes, and the nano timestamp path
**Depends on**: Phase 7
**Requirements**: COR-01 (extended to all suites)
**Success Criteria** (what must be TRUE):
  1. ExplainPlanTest: 522/522 pass (fix 8 plan text mismatches)
  2. SqlOptimiserTest: 171/171 pass (fix 14 plan + should-fail + error text)
  3. RecordCursorMemoryUsageTest: 9/9 pass (fix 3 factory class assertions)
  4. SqlParserTest: 1059/1059 pass (fix 4 parse tree mismatches)
  5. FirstArrayGroupByFunctionFactoryTest: 11/11 pass
  6. LastArrayGroupByFunctionFactoryTest: 20/20 pass
  7. SampleByNanoTimestampTest: 279/279 pass (fix 50 metadata/random-access)
**Plans:** 1/1 plans complete
Plans:
- [x] 08-01-PLAN.md — Fix 31 small-suite failures (plan text, factory classes, parse models, should-fail conversions) + 50 SampleByNanoTimestampTest failures

### Phase 9: Fix Critical Review Findings
**Goal**: Fix 3 critical bugs from code review: geo PREV silent null, unchecked findValue NPE, recursive hasNext stack overflow
**Depends on**: Phase 8
**Requirements**: CR-01, CR-02, CR-03, CR-04
**Success Criteria** (what must be TRUE):
  1. `FILL(PREV)` with geo aggregate columns carries forward previous values (not silent null)
  2. `findValue()` at line 351 has null guard — no NPE on key mismatch
  3. `emitNextFillRow()` → `hasNext()` recursion replaced with loop — no StackOverflowError on sparse data with large FROM/TO range
  4. All existing tests still pass (329 SampleByFillTest + SampleByTest + SampleByNanoTimestampTest)
  5. Javadoc at line 67 updated to match `followedOrderByAdvice=false`
**Plans:** 1/1 plans complete
Plans:
- [x] 09-01-PLAN.md — Fix geo PREV null, findValue NPE guard, recursive hasNext stack overflow, Javadoc fix + regression tests

### Phase 10: Fix Offset-Aware Bucket Alignment in Fill Cursor
**Goal**: Fix infinite fill when ALIGN TO CALENDAR WITH OFFSET is used without TO — the sampler's setStart() ignores the offset, causing bucket boundaries to never match GROUP BY output
**Depends on**: Phase 9
**Requirements**: Correctness bug fix
**Success Criteria** (what must be TRUE):
  1. `SAMPLE BY 5d FROM '2017-12-20' FILL(NULL) ALIGN TO CALENDAR WITH OFFSET '10:00'` produces finite output (stops after last data bucket)
  2. Fill cursor's bucket sequence matches timestamp_floor_utc bucket boundaries for all offset values
  3. Non-keyed and keyed queries with offset + FROM (no TO) produce correct, finite results
  4. All existing tests still pass
**Plans:** 1/1 plans complete
Plans:
- [x] 10-01-PLAN.md — Propagate calendar offset through optimizer/codegen/factory, fix bucket alignment in initialize(), add 5 offset+fill tests

### Phase 11: Hardening — Review Findings & Missing Test Coverage
**Goal**: Fix remaining code review findings and add missing test coverage
**Depends on**: Phase 10
**Requirements**: Code review findings
**Success Criteria** (what must be TRUE):
  1. UUID key columns emit correct values in fill rows (FILL_KEY dispatch in getLong128Hi/Lo, getDecimal128/256, getLong256)
  2. Geo null sentinels use GeoHashes.*_NULL (not 0 or Numbers.*_NULL)
  3. NULL key value test exists (NULL SYMBOL/STRING as GROUP BY key)
  4. CTE/subquery FILL_KEY reclassification test exists
  5. DST test with sparse data generates fill rows during transition
  6. Decimal8/16 null sentinels use Decimals.*_NULL
  7. /review-pr passes with no critical or moderate findings on production code
**Plans:** 1/1 plans complete (retroactive — work landed before plan was written; see SUMMARY)
Plans:
- [x] 11-01-PLAN.md — FILL_KEY dispatch for UUID/Long256/Decimal128-256, Geo/Decimal null sentinels, ownership transfer, dead code removal, 3 new tests (NULL key, CTE, DST sparse)

### Phase 12: Replace safety-net reclassification with legacy fallback and tighten optimizer PREV gate

**Goal:** Replace the silent codegen safety-net reclassification (SqlCodeGenerator.java:3497-3512) with a retro-fallback mechanism that routes unsupported-type PREV aggregates to the legacy cursor path, close the LONG128/INTERVAL gap and add cross-col PREV(alias) resolution at the optimizer gate (Tier 1), implement six new `FILL(PREV, PREV(...))` grammar rules with positioned SqlException errors (D-05..D-09), restore `testSampleByFillNeedFix` to master's 3-row expectation, add 19 regression tests (5 retro-fallback + 8 grammar + 5 FILL_KEY + 1 TO-null), convert ~15 `assertSql` sites to `assertQueryNoLeakCheck` per D-10, and sweep code-quality items (alphabetize FillRecord getters and SampleByFillCursor members, replace FQNs with plain imports, unconditional `fill=` plan attribute, Dates.parseOffset assert, anyPrev loop removal, isKeyColumn relocation).

**Depends on:** Phase 11
**Requirements**: PTSF-02, PTSF-04, COR-01, COR-02, COR-04 (no new requirement IDs introduced; phase added retroactively via `/gsd-add-phase`)

**Success Criteria** (what must be TRUE — derived from CONTEXT.md success criteria 1-18):
  1. `testSampleByFillNeedFix` matches master's 3-row expected output; no duplicated buckets.
  2. Queries whose aggregate output type is DECIMAL128/256, LONG256, UUID, STRING, VARCHAR, SYMBOL, BINARY, array, LONG128, or INTERVAL route to the legacy cursor (plan shows `Sample By`, not `Sample By Fill`) and produce correct results.
  3. The safety-net block at SqlCodeGenerator.java:3497-3512 is deleted; a retro-fallback mechanism (stashed SAMPLE BY node + FallbackToLegacyException + try/catch at the three generateFill call sites) replaces it.
  4. Grammar rules D-05..D-09 reject malformed PREV shapes (PREV(ts), chains, non-LITERAL args, paramCount != 1, bind variables, type-tag mismatches) with positioned SqlException errors.
  5. 22 new regression tests pass: 5 retro-fallback + 11 grammar (3 positive + 8 negative) + 5 FILL_KEY (UUID, Long256, Decimal128, Decimal256, geo-no-prev-yet) + 1 TO-null.
  6. `TO null::timestamp` produces bounded output via the hasExplicitTo LONG_NULL guard.
  7. toPlan emits `fill=null|prev|value` unconditionally for every Sample By Fill node.
  8. Code-quality items applied: FillRecord getters alphabetical, SampleByFillCursor members alphabetical, plain imports in place of FQNs, alphabetized import blocks, isKeyColumn next to isFastPathPrevSupportedType, anyPrev loop removed, Dates.parseOffset asserts.
  9. Full suite green: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest' test` exits 0.

**Plans:** 4/4 plans complete
Plans:
- [x] 12-01-PLAN.md — Scaffolding: FallbackToLegacyException + QueryModel.stashedSampleByNode + Tier 1 gate (LONG128/INTERVAL + cross-col PREV resolution) + stash write in rewriteSampleBy
- [x] 12-02-PLAN.md — SampleByFillRecordCursorFactory refactor: hasExplicitTo LONG_NULL guard + toPlan fill= unconditional + alphabetize FillRecord getters and SampleByFillCursor members + FQN→plain imports + import block sort
- [x] 12-03-PLAN.md — SqlCodeGenerator overhaul: grammar rules D-05..D-09 + anyPrev loop removal + safety-net deletion + FallbackToLegacyException throw + try/catch at three generateFill call sites + isKeyColumn relocation + SampleByFillRecordCursorFactory import slot + Dates.parseOffset assert
- [x] 12-04-PLAN.md — Tests: 5 retro-fallback + 1 TO-null + testSampleByFillNeedFix restore + 11 grammar tests (8 CONTEXT + 3 positives) + 5 FILL_KEY + ~15 assertSql→assertQueryNoLeakCheck conversions + plan-text assertion refresh across 4 test files

### Phase 13: Migrate FILL(PREV) snapshots from materialized values to rowId-based replay

**Goal:** Replace per-type materialization of FILL(PREV) snapshots in the fast-path fill cursor with a single chain rowId per key, replayed on emit via `baseCursor.recordAt(prevRecord, prevRowId)`. Ship the prerequisite `SortedRecordCursor.chain.clear()` fix (today's root cause of data corruption on cursor reuse) as its own standalone commit before the rewrite. Borrowed from the `sm_fill_prev_fast_all_types` branch: research complete with GO verdict, candidate (a) selected. Scope matches that branch's phase 12 — pure rowId rewrite of `SampleByFillRecordCursorFactory`, deletion of all per-type snapshot dispatch (KIND_LONG_BITS / KIND_SYMBOL / KIND_LONG128 / KIND_DECIMAL128 / KIND_LONG256 / KIND_DECIMAL256 / KIND_STRING / KIND_VARCHAR / KIND_BIN / KIND_ARRAY), plus prev-record lifetime-binding hygiene via a dedicated third record slot (`RecordChain.getRecordC()` or equivalent) to avoid coupling to `SortedRecordCursor.recordB`.

**Depends on:** Phase 12
**Requirements**: Internal refactor — builds on PTSF-01..06 and COR-01..04; no new requirement IDs

**Success Criteria** (what must be TRUE — borrowed from source branch's 12-CONTEXT.md, to be re-validated during planning):
  1. `SampleByFillRecordCursorFactory` stores exactly one `prevRowId` per key (keyed map value slot) or one `simplePrevRowId` (non-keyed) — no per-type copied-value buffers.
  2. Gap-fill emit reads prev values via `baseCursor.recordAt(prevRecord, prevRowId)` then typed getters on `prevRecord`; uniform across all types.
  3. `SortedRecordCursor.of()` calls `chain.clear()` on reuse when `isOpen=true` — shipped as an independent commit before the fill rewrite.
  4. Retro-fallback mechanism from phase 12 is reassessed: if rowId unlocks all currently-unsupported PREV types (UUID/LONG128/LONG256/DECIMAL128/DECIMAL256/STRING/VARCHAR/BINARY/array/INTERVAL), retro-fallback is deleted; if only a subset, WR-01/WR-02/WR-03 from seed SEED-001 are fixed alongside.
  5. `recordAt` cached once per emit row (planner concern PI-02 from source branch).
  6. Existing tests still pass: `SampleByTest`, `SampleByFillTest`, `SampleByNanoTimestampTest`, `ExplainPlanTest`, `RecordCursorMemoryUsageTest`, `OrderBy*` tests.
  7. Seeds SEED-001 and SEED-002 are revisited and closed: bucket 1 follow-ups routed into this phase's scope; cursor defects 1 and 2 absorbed by the rewrite or filed as independent phases.
  8. All 13 per-type FILL(PREV) tests listed in `13-VALIDATION-INPUTS.md` (borrowed from `sm_fill_prev_fast_all_types` commit `f43a3d7057`) land on this branch and pass on the fast path (plan output shows `Sample By Fill`, not `Sample By`).

**Plans:** 4/6 plans executed

Plans:
- [x] 13-01-PLAN.md — chain.clear() fix in SortedRecordCursor.of() on reuse + D-02 investigation report (Commit 1)
- [x] 13-02-PLAN.md — rowId rewrite in SampleByFillRecordCursorFactory + mapValueTypes resize in SqlCodeGenerator.generateFill (Commit 2)
- [x] 13-03-PLAN.md — cherry-pick 13 per-type FILL(PREV) tests from f43a3d7057 (Commit 3)
- [x] 13-04-PLAN.md — delete retro-fallback machinery across 6 production files + 7 retro-fallback tests (Commit 4)
- [ ] 13-05-PLAN.md — restore testSampleByFillNeedFix assertion #1 and #2 to master's 3-row form; SEED-002 Defects 1 and 2 (Commit 5)
- [ ] 13-06-PLAN.md — WR-04 precise chain-rejection position + Defect 3 insufficient-fill grammar + testFillInsufficientFillValues + testSampleByFillNeedFix assertion #3 (Commit 6)

## Progress

**Execution Order:** Phases execute in numeric order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13.

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Optimizer Gate | 0/0 | Complete | 2026-04-09 |
| 2. Non-keyed Fill Cursor | 1/1 | Complete | 2026-04-10 |
| 3. Keyed Fill Cursor | 1/1 | Complete | 2026-04-10 |
| 4. Cross-Column Prev | 1/1 | Complete | 2026-04-10 |
| 5. Verification and Hardening | 1/1 | Absorbed by 7–10 | 2026-04-10 |
| 6. Keyed Fill with FROM/TO Range | 1/1 | Complete | 2026-04-10 |
| 7. PREV Type-Safe Fast Path | 1/1 | Complete | 2026-04-10 |
| 8. Fix Remaining Test Regressions | 1/1 | Complete | 2026-04-12 |
| 9. Fix Critical Review Findings | 1/1 | Complete | 2026-04-13 |
| 10. Fix Offset-Aware Bucket Alignment | 1/1 | Complete | 2026-04-13 |
| 11. Hardening — Review Findings & Missing Test Coverage | 1/1 | Complete (retroactive) | 2026-04-13 |
| 12. Replace safety-net reclassification with legacy fallback | 4/4 | Complete    | 2026-04-17 |
| 13. Migrate FILL(PREV) snapshots to rowId-based replay | 4/6 | In Progress|  |
