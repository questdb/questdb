---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: complete
stopped_at: Phase 11 back-fill complete; PR #6946 CI green
last_updated: "2026-04-14T11:00:00.000Z"
last_activity: 2026-04-14
progress:
  total_phases: 11
  completed_phases: 11
  total_plans: 10
  completed_plans: 10
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-09)

**Core value:** SAMPLE BY FILL queries execute on the GROUP BY fast path with identical output to the cursor path, enabling parallel execution.
**Current focus:** Closed — PR #6946 ready for merge.

## Current Position

Phase: 11 of 11 (Hardening — Review Findings & Missing Test Coverage)
Plan: 1 of 1 in current phase (complete)
Status: All 11 phases complete; PR #6946 CI green (50/50 checks, 280,124 tests, 0 failures)
Last activity: 2026-04-14

Progress: [##########] 100%

## Performance Metrics

**Velocity:**

- Total plans completed: 10
- Total execution time across tracked phases: ~10h

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 02 | 1 | 43m | 43m |
| 03 | 1 | 112m | 112m |
| 04 | 1 | 27m | 27m |
| 06 | 1 | 4m | 4m |
| 07 | 1 | 49m | 49m |
| 08 | 1 | 273m | 273m |
| 09 | 1 | 23m | 23m |
| 10 | 1 | 16m | 16m |
| 11 | 1 | ~210m (retroactive estimate) | ~210m |

Phase 5 absorbed into phases 7–10; no direct execution time attributed.

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table. Recent decisions affecting delivered code:

- [Phase 1]: Unified SampleByFillRecordCursorFactory handles NULL/PREV/VALUE via per-column fillModes
- [Phase 1]: Sort inside generateFill via generateOrderBy(); followedOrderByAdvice=true skips outer sort
- [Phase 2]: Two-pass streaming for keyed fill (pass 1 discovers keys, toTop(), pass 2 streams with fill)
- [Phase 2]: Per-column fillModes (FILL_CONSTANT, FILL_PREV_SELF) with constantFills ObjList
- [Phase 2]: Build SortedRecordCursorFactory explicitly in generateFill — optimizer strips ORDER BY before code generation
- [Phase 2]: No peek-ahead in hasNext dataTs==nextBucketTimestamp branch — corrupts SortedRecordCursor record position
- [Phase 2]: Early-exit guard placed after data fetch (not before) so isBaseCursorExhausted is set correctly
- [Phase 3]: FILL_KEY = -3 distinguishes key columns from aggregates in fillModes array
- [Phase 3]: OrderedMap stores key combinations with per-key prev in MapValue slots [keyIndex, hasPrev, prevCols...]
- [Phase 3]: keyPosOffset compensates for value columns preceding key columns in MapRecord index space
- [Phase 3]: symbolTableColIndices covers all map columns (value + key) for SYMBOL resolution
- [Phase 3]: Non-keyed PREV preserved via simplePrev fallback when keysMap is null
- [Phase 04]: FILL(PREV(col_name)) resolved via ExpressionNode.FUNCTION type with paramCount=1, alias resolved against output metadata
- [Phase 04]: Optimizer gate relaxed from size-1-only to hasLinearFill() predicate, allowing multi-fill specs on fast path
- [Phase 06]: Zero-key guard in initialize() returns early with maxTimestamp=Long.MIN_VALUE to produce empty result
- [Phase 07]: IntList prevSourceCols replaces boolean hasPrevFill for per-column PREV snapshot tracking
- [Phase 07]: Two-layer type defense: optimizer gate (best-effort) + generateFill safety net (SqlException)
- [Phase 08]: supportsRandomAccess=false for all fill cursor tests; ASOF JOIN also routes through fast path
- [Phase 09]: Assert guard (not null-check) at findValue() because pass 1 discovers all keys from the same cursor
- [Phase 09]: Iterative while(true) loop in emitNextFillRow replaces recursive hasNext call; hasNext call sites fall through on false
- [Phase 10]: Propagate fillOffset only for non-timezone queries to avoid double-application of offset
- [Phase 11]: FILL_KEY dispatch extended to all 128/256-bit getters (UUID, Long256, Decimal128/256)
- [Phase 11]: Null sentinels use GeoHashes.*_NULL and Decimals.*_NULL rather than 0 / Numbers.*_NULL so null is distinguishable from legitimate zero
- [Phase 11]: Function ownership transferred from fillValues to constantFillFuncs by nulling source slot to prevent double-close on error path

### Roadmap Evolution

- Phase 6 added: Keyed fill with FROM/TO range (was incorrectly listed as out of scope)
- Phase 11 added retroactively via `/gsd-add-phase hardening`; code landed first (commits `2125201f30`, `28b00e8340`, `a6355c3e65`), paper trail back-filled via `/gsd-forensics` on 2026-04-14
- Phase 5 closed as "Absorbed by 7–10" on 2026-04-14 — its four `must_have.truths` were satisfied cumulatively by the finer-grained phases rather than as a single batch

### Pending Todos

None.

### Blockers/Concerns

None blocking merge. Open pre-merge cleanup items:
- `/review-pr` has not been re-run since the phase 11 commits landed (success criterion #7 unverified)
- PR #6946 body references `FillPrevRangeRecordCursorFactory`, which no longer exists — update before merge
- PR #6946 missing `Performance` label

## Session Continuity

Last session: 2026-04-14T11:00:00Z
Stopped at: Phase 11 back-fill complete; PR #6946 CI green
Resume file: None
