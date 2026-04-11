# Roadmap: SAMPLE BY Fill on GROUP BY Fast Path

## Overview

This project moves QuestDB's SAMPLE BY FILL queries from the sequential cursor-based execution path to the parallel GROUP BY fast path. The optimizer rewrites SAMPLE BY to GROUP BY with `timestamp_floor_utc`, then a streaming fill cursor inserts gap-filled rows. Work progresses from optimizer gating (done) through non-keyed fill (in progress), keyed fill with cartesian product semantics, cross-column prev (new feature), and full verification against the existing 302-test suite.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Optimizer Gate** - Relax optimizer to rewrite FILL(PREV) and keyed FILL to GROUP BY, preserve ORDER BY
- [x] **Phase 2: Non-keyed Fill Cursor** - Streaming fill cursor for non-keyed FILL(NULL/PREV/VALUE) with DST and FROM/TO support
- [x] **Phase 3: Keyed Fill Cursor** - Map-based key discovery, cartesian product emission, per-key prev tracking
- [ ] **Phase 4: Cross-Column Prev** - FILL(PREV) referencing a different column from the previous bucket
- [ ] **Phase 5: Verification and Hardening** - All 302 SampleByTest tests pass, resource leak fixes, parity validation
- [x] **Phase 6: Keyed Fill with FROM/TO Range** - Keyed fill with FROM/TO range, architecture validation against cursor path (completed 2026-04-10)

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
**Plans**: TBD

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
**Plans:** 1 plan
Plans:
- [x] 02-01-PLAN.md -- Fix infinite loop + resource leaks, add assertion tests for NULL/VALUE/FROM-TO/DST

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
**Plans:** 1 plan
Plans:
- [x] 03-01-PLAN.md -- Optimizer gates, keyed fill cursor implementation, and integration tests

### Phase 4: Cross-Column Prev
**Goal**: FILL(PREV) can reference a specific source column from the previous bucket rather than always filling from self
**Depends on**: Phase 3
**Requirements**: XPREV-01, XPREV-02
**Success Criteria** (what must be TRUE):
  1. Syntax for cross-column prev is defined and documented (e.g., `FILL(PREV(col_name))` or equivalent)
  2. A query using cross-column prev fills a gap row's column with the value of a different column from the previous bucket
  3. Cross-column prev works correctly with both keyed and non-keyed queries
**Plans:** 1 plan
Plans:
- [x] 04-01-PLAN.md -- Optimizer gate relaxation, PREV(col_name) detection in generateFill, keyed prevValue fix, cross-column tests

### Phase 5: Verification and Hardening
**Goal**: The fast-path fill implementation passes all existing tests and produces output identical to the cursor path
**Depends on**: Phase 4
**Requirements**: COR-01, COR-02, COR-03, COR-04
**Success Criteria** (what must be TRUE):
  1. All 302 existing SampleByTest tests pass without modification (behavioral parity with cursor path)
  2. `assertMemoryLeak` passes for all fill-related tests (no native memory leaks)
  3. Query plans for eligible FILL queries show `Async Group By` or `GroupBy vectorized` (parallel execution confirmed)
  4. Resource leak in `generateFill` error path is fixed (`sorted` factory and `constantFillFuncs` freed on exception)
**Plans:** 1 plan
Plans:
- [ ] 05-01-PLAN.md -- Fix generateFill() metadata timestamp index, resolve all 65 test failures across 8 categories

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Optimizer Gate | 0/0 | Complete | 2026-04-09 |
| 2. Non-keyed Fill Cursor | 1/1 | Complete | 2026-04-10 |
| 3. Keyed Fill Cursor | 1/1 | Complete | 2026-04-10 |
| 4. Cross-Column Prev | 1/1 | Complete | 2026-04-10 |
| 5. Verification and Hardening | 1/1 | Complete | 2026-04-10 |
| 6. Keyed Fill with FROM/TO Range | 1/1 | Complete | 2026-04-10 |
| 7. PREV Type-Safe Fast Path | 1/1 | Complete | 2026-04-10 |
| 8. Fix Remaining Test Regressions | 0/1 | In Progress | — |

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
- [x] 06-01-PLAN.md -- Fix SIGSEGV crash + 8 keyed FROM/TO fill tests

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
**Plans:** 1 plan
Plans:
- [x] 07-01-PLAN.md -- Per-column snapshot, type matrix, optimizer gate, legacy fallback, mixed-fill and nano tests

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
**Plans:** 1 plan
Plans:
- [ ] 08-01-PLAN.md -- Fix 31 small-suite failures (plan text, factory classes, parse models, should-fail conversions) + 50 SampleByNanoTimestampTest failures
