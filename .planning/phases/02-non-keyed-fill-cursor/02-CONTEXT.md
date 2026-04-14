# Phase 2 Context: Non-keyed Fill Cursor

## Domain Boundary

Non-keyed SAMPLE BY FILL(NULL/VALUE) on the GROUP BY fast path. The fill cursor wraps sorted GROUP BY output and inserts gap-filled rows for missing time buckets. FILL(PREV) remains gated on the optimizer side for this phase.

## Decisions

### DST is NOT an issue for sub-day strides

**Decision**: `timestamp_floor_utc` (after PR #6858) produces **evenly-spaced, monotonic UTC timestamps** for sub-day strides, even across DST transitions. `SimpleTimestampSampler` with the same stride reproduces the exact same bucket sequence. No timezone-aware stepping needed for the fill cursor.

**Evidence**: tested 30m and 1h strides with `Europe/Riga` timezone across 2021-10-31 DST fall-back. Buckets are evenly spaced in UTC: `00:00, 00:30, 01:00, 01:30, ...` (30m) and `00:00, 01:00, 02:00, ...` (1h). With offset `00:40`: `23:40, 00:40, 01:40, ...` — still evenly spaced.

**Root cause of the failing test**: NOT a DST ordering issue. It's an **infinite loop**: when no TO clause, `maxTimestamp = Long.MAX_VALUE`, and after the base cursor is exhausted the fill cursor emits fill rows forever. Fix: stop emitting when base cursor is exhausted and no explicit TO.

### followedOrderByAdvice = true with inner sort

**Decision**: Keep `followedOrderByAdvice() = true`. Call `generateOrderBy()` inside `generateFill()` to sort GROUP BY output. The fill cursor receives sorted input, processes in order, and emits ordered output. The outer sort in `generateQuery0()` is skipped.

**Rationale**: Since UTC buckets are evenly spaced, the sampler reproduces the sequence correctly. Sorted input enables future PREV support (needs ordering). One sort total (inner), no double sort.

### Optimizer gate: PREV stays gated

**Decision**: Keep `!isPrevKeyword(...)` in the optimizer gate (line 7880). Phase 2 only handles NULL and VALUE fills on the fast path. PREV enablement moves to a later phase.

**Rationale**: PREV requires per-key tracking for keyed queries, non-numeric type handling, and careful testing. Better to ship NULL/VALUE first.

### Phase 2 scope (refined)

Only non-keyed FILL(NULL) and FILL(VALUE) on the fast path. This means:
- The optimizer gate already lets these through (no change needed)
- The fill cursor handles constant fills and null fills
- PREV code in the cursor is dead code for now (future-proofing)
- The infinite loop fix is the only remaining bug

### How the old fill works

**FillRangeRecordCursorFactory** (current fast-path fill):
- Two-phase: emits all data rows first, then all fill rows
- Collects timestamps into `presentTimestamps` DirectLongList, sorts them
- Walks the bucket grid via sampler, emits fill for missing buckets
- Relies on outer sort for ordering (doesn't guarantee order)
- Only handles NULL and VALUE fills

**Cursor-based path** (`SampleByFillPrevRecordCursor`, etc.):
- Operates on raw table data (not GROUP BY output)
- Works in local time with explicit DST handling (`adjustDst`, `adjustDstInFlight`)
- Processes one bucket at a time, aggregates into a Map
- Map persists across buckets — PREV works by retaining previous values
- Handles timezone via `TimeZoneRules`

### Sub-day vs day+ sampling

- **Sub-day** (h, m, s): `SimpleTimestampSampler` — fixed microsecond step. Evenly spaced in UTC.
- **Day+** (d, w, M, y): `MonthTimestampSampler`, `WeekTimestampSampler`, etc. — calendar-aware step. Variable bucket size. May need different handling for fill.

## Canonical References

- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:7880` — optimizer gate for fill modes
- `core/src/main/java/io/questdb/griffin/engine/groupby/FillRangeRecordCursorFactory.java` — old fast-path fill
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — new fill cursor (WIP)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SimpleTimestampSampler.java` — sub-day sampler
- `core/src/main/java/io/questdb/griffin/engine/groupby/AbstractNoRecordSampleByCursor.java` — DST handling reference

## Deferred Ideas

- FILL(PREV) on fast path — later phase, after NULL/VALUE is solid
- FILL(PREV) for non-numeric types (STRING, VARCHAR, SYMBOL, LONG256, etc.)
- FILL(LINEAR) look-ahead with deferred emission
- Day+ stride fill (Month, Year, Week) — may need special sampler handling
- Full cursor path replacement
