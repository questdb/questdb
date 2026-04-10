# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** SAMPLE BY FILL queries execute on the GROUP BY fast path with identical output to the cursor path, enabling parallel execution.
**Current focus:** Phase 3 - Keyed Fill Cursor

## Current Position

Phase: 3 of 5 (Keyed Fill Cursor)
Plan: 1 of 1 in current phase (complete)
Status: Phase 3 complete
Last activity: 2026-04-10 -- Plan 03-01 executed. Removed optimizer gates, implemented OrderedMap-based two-pass keyed fill with per-key prev tracking. 6 keyed fill tests added. All 15 SampleByFillTest tests pass.

Progress: [######....] 60%

## Performance Metrics

**Velocity:**
- Total plans completed: 2
- Average duration: 78m
- Total execution time: 155m

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 2 | 1 | 43m | 43m |
| 3 | 1 | 112m | 112m |

**Recent Trend:**
- Last 5 plans: 43m, 112m
- Trend: increasing (keyed fill was more complex)

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
- [Phase 3]: FILL_KEY = -3 distinguishes key columns from aggregates in fillModes array
- [Phase 3]: OrderedMap stores key combinations with per-key prev in MapValue slots [keyIndex, hasPrev, prevCols...]
- [Phase 3]: keyPosOffset compensates for value columns preceding key columns in MapRecord index space
- [Phase 3]: symbolTableColIndices covers all map columns (value + key) for SYMBOL resolution
- [Phase 3]: Non-keyed PREV preserved via simplePrev fallback when keysMap is null

### Pending Todos

None yet.

### Blockers/Concerns

- [Phase 2]: 14 existing SampleByTest tests fail on factory properties (timestamp index, random access, record type). Phase 5 scope.
- [Phase 2]: FillRecord.readColumnAsLongBits() only covers numeric types up to 64 bits. STRING/VARCHAR/SYMBOL FILL(PREV) needs different storage.

## Session Continuity

Last session: 2026-04-10
Stopped at: Phase 3 Plan 01 complete. Keyed fill cursor implemented with OrderedMap, per-key prev, SYMBOL resolution. All 15 tests pass.
Resume file: None
