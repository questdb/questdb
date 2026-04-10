# Requirements: SAMPLE BY Fill on GROUP BY Fast Path

**Defined:** 2026-04-09
**Core Value:** SAMPLE BY FILL queries execute on the GROUP BY fast path with identical output to the cursor path, enabling parallel execution.

## v1 Requirements

### Optimizer

- [x] **OPT-01**: SqlOptimiser rewrites keyed SAMPLE BY FILL(NULL/PREV/VALUE) to GROUP BY with timestamp_floor_utc
- [x] **OPT-02**: Optimizer preserves ORDER BY when fill stride is present (prevents optimiseOrderBy from dropping the sort)
- [x] **OPT-03**: Optimizer bails out to cursor path for FILL(LINEAR) (deferred to v2)

### Fill Cursor -- Non-keyed

- [ ] **FILL-01**: Non-keyed FILL(NULL) emits null-filled rows for missing buckets
- [ ] **FILL-02**: Non-keyed FILL(PREV) carries forward previous bucket's aggregate values
- [ ] **FILL-03**: Non-keyed FILL(VALUE) emits constant-filled rows for missing buckets
- [ ] **FILL-04**: FROM/TO range bounds control the fill range (leading/trailing fill rows)
- [ ] **FILL-05**: DST timezone queries produce correctly ordered output during fall-back transitions

### Fill Cursor -- Keyed

- [ ] **KEY-01**: Keyed fill emits the cartesian product of all unique keys x all buckets
- [ ] **KEY-02**: Missing (key, bucket) pairs filled according to fill mode (null/prev/value)
- [ ] **KEY-03**: Per-key prev tracking -- each key independently carries forward its own previous values
- [ ] **KEY-04**: Key order within a bucket is stable (same order for every bucket)
- [ ] **KEY-05**: Key column values in fill rows match the key's actual values (from key discovery)

### Cross-Column Prev (New Feature)

- [x] **XPREV-01**: FILL(PREV) can reference a specific column from the previous bucket (not just self)
- [x] **XPREV-02**: Syntax and semantics defined (TBD -- needs design)

### Keyed Fill with FROM/TO Range

- [x] **KFTR-01**: Keyed FILL with FROM/TO emits the cartesian product of all keys for every bucket in the [FROM, TO) range
- [x] **KFTR-02**: Leading fill rows (FROM before first data) include all keys with correct fill values
- [x] **KFTR-03**: Trailing fill rows (TO after last data) include all keys with correct fill values
- [x] **KFTR-04**: Per-key prev tracking works correctly across FROM/TO range boundaries
- [x] **KFTR-05**: Architecture validation -- keyed FROM/TO fill produces output matching cursor-based path

### PREV Type-Safe Fast Path

- [x] **PTSF-01**: Fast-path PREV snapshot only persists required source columns (not global save-all)
- [x] **PTSF-02**: Explicit source type support matrix — numeric types on fast path, unsupported types fall back to legacy
- [x] **PTSF-03**: Mixed-fill queries (one PREV numeric + one non-PREV string/symbol) do not crash on fast path
- [x] **PTSF-04**: prev(alias) referencing unsupported type triggers legacy path fallback (plan shows Sample By, not Async Group By)
- [x] **PTSF-05**: No behavior regressions for existing FILL(PREV) tests
- [x] **PTSF-06**: Nanosecond timestamp tests mirror microsecond equivalents

### Code Generator

- [ ] **GEN-01**: generateFill() builds per-column fill modes (FILL_CONSTANT, FILL_PREV_SELF) from fill expressions
- [ ] **GEN-02**: generateFill() calls generateOrderBy() to sort GROUP BY output before wrapping with fill cursor
- [ ] **GEN-03**: Fill cursor reports followedOrderByAdvice()=true to skip outer sort
- [ ] **GEN-04**: PREV and NULL keywords handled without function parsing (skip functionParser for keywords)

### Correctness

- [ ] **COR-01**: All 302 existing SampleByTest tests pass
- [ ] **COR-02**: No native memory leaks (assertMemoryLeak passes for all tests)
- [ ] **COR-03**: Query plan shows Async Group By (parallel execution) for fill queries
- [ ] **COR-04**: Fill cursor output matches cursor-path output exactly for all fill modes

## v2 Requirements

### FILL(LINEAR)

- **LIN-01**: Linear interpolation between known data points per key
- **LIN-02**: Deferred emission -- buffer gaps until right endpoint known
- **LIN-03**: Trailing gaps fall back to PREV or NULL (no right endpoint)

### Full Cursor Path Replacement

- **REP-01**: Remove cursor-based fill path entirely once fast path covers all cases
- **REP-02**: Remove SampleByFillNull/Prev/ValueRecordCursor classes

## Out of Scope

| Feature | Reason |
|---------|--------|
| FILL(LINEAR) | Architecture supports it, deferred to v2 for complexity |
| Cursor path removal | Fast path is optimization, cursor path stays as fallback |
| New SQL syntax | Using existing FILL(...) syntax |
## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| OPT-01 | Phase 1: Optimizer Gate | Done |
| OPT-02 | Phase 1: Optimizer Gate | Done |
| OPT-03 | Phase 1: Optimizer Gate | Done |
| FILL-01 | Phase 2: Non-keyed Fill Cursor | In Progress |
| FILL-02 | Phase 2: Non-keyed Fill Cursor | In Progress |
| FILL-03 | Phase 2: Non-keyed Fill Cursor | In Progress |
| FILL-04 | Phase 2: Non-keyed Fill Cursor | Pending |
| FILL-05 | Phase 2: Non-keyed Fill Cursor | Pending |
| GEN-01 | Phase 2: Non-keyed Fill Cursor | In Progress |
| GEN-02 | Phase 2: Non-keyed Fill Cursor | In Progress |
| GEN-03 | Phase 2: Non-keyed Fill Cursor | In Progress |
| GEN-04 | Phase 2: Non-keyed Fill Cursor | In Progress |
| KEY-01 | Phase 3: Keyed Fill Cursor | Pending |
| KEY-02 | Phase 3: Keyed Fill Cursor | Pending |
| KEY-03 | Phase 3: Keyed Fill Cursor | Pending |
| KEY-04 | Phase 3: Keyed Fill Cursor | Pending |
| KEY-05 | Phase 3: Keyed Fill Cursor | Pending |
| XPREV-01 | Phase 4: Cross-Column Prev | Complete |
| XPREV-02 | Phase 4: Cross-Column Prev | Complete |
| KFTR-01 | Phase 6: Keyed Fill with FROM/TO Range | Complete |
| KFTR-02 | Phase 6: Keyed Fill with FROM/TO Range | Complete |
| KFTR-03 | Phase 6: Keyed Fill with FROM/TO Range | Complete |
| KFTR-04 | Phase 6: Keyed Fill with FROM/TO Range | Complete |
| KFTR-05 | Phase 6: Keyed Fill with FROM/TO Range | Complete |
| PTSF-01 | Phase 7: PREV Type-Safe Fast Path | Complete |
| PTSF-02 | Phase 7: PREV Type-Safe Fast Path | Complete |
| PTSF-03 | Phase 7: PREV Type-Safe Fast Path | Complete |
| PTSF-04 | Phase 7: PREV Type-Safe Fast Path | Complete |
| PTSF-05 | Phase 7: PREV Type-Safe Fast Path | Complete |
| PTSF-06 | Phase 7: PREV Type-Safe Fast Path | Complete |
| COR-01 | Phase 5: Verification and Hardening | Pending |
| COR-02 | Phase 5: Verification and Hardening | Pending |
| COR-03 | Phase 5: Verification and Hardening | Pending |
| COR-04 | Phase 5: Verification and Hardening | Pending |
