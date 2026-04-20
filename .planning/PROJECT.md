# SAMPLE BY Fill on GROUP BY Fast Path

## What This Is

Move QuestDB's SAMPLE BY FILL queries from the sequential cursor-based execution path to the parallel GROUP BY fast path. The optimizer rewrites SAMPLE BY to GROUP BY with `timestamp_floor_utc`, then a streaming fill cursor adds gap-filled rows. This delivers parallel execution for FILL(NULL), FILL(PREV), FILL(VALUE) queries — both keyed and non-keyed.

## Core Value

SAMPLE BY FILL queries execute on the GROUP BY fast path with identical output to the cursor path, enabling parallel execution via Async Group By.

## Requirements

### Validated

(None yet — ship to validate)

### Active

- [ ] Non-keyed FILL(NULL/VALUE/PREV) on fast path with correct output
- [ ] Keyed FILL(NULL/VALUE/PREV) on fast path with cartesian product semantics (all keys in every bucket)
- [ ] Per-key prev tracking for keyed FILL(PREV)
- [ ] DST timezone handling — correct bucket ordering during fall-back transitions
- [ ] FROM/TO range support with fill
- [ ] FILL(PREV) cross-column reference — fill from any column in the previous bucket (potential new feature)
- [ ] All existing SampleByTest tests pass (302 tests)
- [ ] No native memory leaks (assertMemoryLeak)

### Out of Scope

- FILL(LINEAR) — architecture supports it via deferred emission, but not in this milestone
- Replacing the cursor-based path entirely — fast path is an optimization, cursor path remains as fallback
- New SQL syntax — using existing FILL(...) syntax

## Context

- Branch: `sm_fill_prev_fast_path` — 5 WIP commits on top of master
- PR #6946 (draft) tracks this work
- PR #6858 (merged) fixed `timestamp_floor_utc` DST monotonicity — prerequisite for this work
- The cursor-based path (SampleByFillNull/Prev/ValueRecordCursor) works correctly but is sequential
- The GROUP BY fast path uses Async Group By for parallel execution across worker threads
- Current WIP state: non-keyed FILL(NULL/PREV) works, DST timezone edge case and keyed fill pending

## Constraints

- **Zero-GC**: No allocations on data paths — use QuestDB Map, DirectLongList, pre-allocated buffers
- **No third-party deps**: All data structures implemented from first principles
- **Behavioral parity**: Fast path output must match cursor path output exactly (same rows, same order)
- **Memory**: Fill cursor memory should be O(unique keys × columns), not O(rows)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Two-pass streaming fill | Pass 1: discover keys. Pass 2: stream with fill. O(keys) memory, not O(rows) | — Pending |
| Sort inside generateFill | generateOrderBy() called inside generateFill() for sorted input. followedOrderByAdvice=true skips outer sort | — Pending |
| Unified cursor for all fill modes | Single SampleByFillRecordCursorFactory handles NULL/PREV/VALUE via per-column fillModes | ✓ Good |
| Keep cursor path as fallback | Fast path is opt-in via optimizer rewrite. Cursor path untouched for correctness | ✓ Good |

---
*Last updated: 2026-04-20 — Phase 13 complete: FILL(PREV) snapshots migrated to chain rowIds; retro-fallback machinery deleted; SEED-001 (WR-04 + Defect 3) and SEED-002 (Defect 1 + 2) closed.*
