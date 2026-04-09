# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** SAMPLE BY FILL queries execute on the GROUP BY fast path with identical output to the cursor path, enabling parallel execution.
**Current focus:** Phase 2 - Non-keyed Fill Cursor

## Current Position

Phase: 2 of 5 (Non-keyed Fill Cursor)
Plan: 1 of 1 in current phase (complete)
Status: Phase 2 complete
Last activity: 2026-04-10 -- Plan 02-01 executed. Fixed infinite loop, peek-ahead corruption, unsorted input, and two resource leaks. Added 8 assertion tests. All SampleByFillTest tests pass.

Progress: [####......] 40%

## Performance Metrics

**Velocity:**
- Total plans completed: 1
- Average duration: 43m
- Total execution time: 43m

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 2 | 1 | 43m | 43m |

**Recent Trend:**
- Last 5 plans: 43m
- Trend: baseline

*Updated after each plan completion*

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- [Phase 1]: Unified SampleByFillRecordCursorFactory handles NULL/PREV/VALUE via per-column fillModes
- [Phase 1]: Sort inside generateFill via generateOrderBy(); followedOrderByAdvice=true skips outer sort
- [Phase 2]: Two-pass streaming for keyed fill (pass 1 discovers keys, toTop(), pass 2 streams with fill)
- [Phase 2]: Per-column fillModes (FILL_CONSTANT, FILL_PREV_SELF) with constantFills ObjList
- [Phase 2]: Build SortedRecordCursorFactory explicitly in generateFill — optimizer strips ORDER BY before code generation
- [Phase 2]: No peek-ahead in hasNext dataTs==nextBucketTimestamp branch — corrupts SortedRecordCursor record position
- [Phase 2]: Early-exit guard placed after data fetch (not before) so baseCursorExhausted is set correctly

### Pending Todos

None yet.

### Blockers/Concerns

- [Phase 2]: 14 existing SampleByTest tests fail on factory properties (timestamp index, random access, record type). Phase 5 scope.
- [Phase 2]: FillRecord.readColumnAsLongBits() only covers numeric types up to 64 bits. STRING/VARCHAR/SYMBOL FILL(PREV) needs different storage.

## Session Continuity

Last session: 2026-04-10
Stopped at: Phase 2 Plan 01 complete. All fill cursor bugs fixed, 9 assertion tests pass.
Resume file: None
