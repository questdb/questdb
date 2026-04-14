# Phase 6 Context: Keyed Fill with FROM/TO Range

## Domain Boundary

Keyed SAMPLE BY FILL with FROM/TO range on the GROUP BY fast path. The fill cursor emits the cartesian product of all keys for every bucket in [FROM, TO), including leading and trailing fill rows. Includes fixing a SIGSEGV crash discovered during investigation.

## Decisions

### SIGSEGV crash — critical bug (Phase 3 regression)

**Finding**: Keyed FILL with FROM/TO crashes ONLY when FROM is after all data (GROUP BY returns zero rows). Reproduced with:
```sql
SELECT ts, key, sum(val) FROM x SAMPLE BY 1h FROM '2024-01-05' TO '2024-01-06' FILL(NULL) ALIGN TO CALENDAR
```
**Root cause**: `initialize()` runs pass 1 (key discovery) on empty GROUP BY output → `keyCount = 0`, `keysMapRecord` not set → pass 2 tries to use null `keysMapRecord` → SIGSEGV in `Unsafe_GetInt`.

**Impact analysis on previous phases:**
- Phase 2 (non-keyed FROM/TO): NOT affected — works correctly
- Phase 3 (keyed no-FROM/TO): NOT affected — works correctly
- Phase 3 (keyed FROM/TO normal): NOT affected — works correctly (tested: leading fill, data, trailing)
- Phase 3 (keyed FROM after data): CRASHES — this specific edge case

**Fix**: When pass 1 discovers zero keys and FROM/TO is set, return empty result or emit fill-only rows without keysMap. Fix in Phase 6 scope.

### Validation scope — exhaustive comparison

**Decision**: Run a suite of edge-case keyed FROM/TO queries and validate results against hardcoded expected output derived from SQL semantics. Use extensive test coverage rather than direct cursor-path comparison.

**Rationale**: Hardcoded expected output is self-contained and doesn't require a mechanism to force cursor-path execution. Comprehensive test count compensates.

### Approach — test first, fix if broken

**Decision**: Write the keyed FROM/TO tests first. The SIGSEGV must be fixed before tests can execute. After the crash fix, run tests and fix any assertion failures.

### Keys appearing mid-range — fill all keys from FROM

**Decision**: All discovered keys get fill rows from the very first bucket (FROM). PREV starts as null until the first data for that key. This matches the cursor-based path behavior — `initializeMap()` discovers all keys via a full scan, then every bucket iteration emits all keys.

### Empty range (FROM == TO) — zero rows

**Decision**: Empty range produces empty result set. Matches cursor-based path behavior (tested and confirmed).

### Test scenarios — comprehensive (6-8 tests)

**Decision**: Write 6-8 assertion tests with hardcoded expected output covering:
1. Basic keyed FILL(NULL) with FROM/TO (leading + trailing fill for all keys)
2. Keyed FILL(VALUE) with FROM/TO
3. Keyed FILL(PREV) with FROM/TO (per-key prev across range)
4. Keys appearing mid-range with FROM/TO (key C first appears at bucket 5)
5. Empty range (FROM == TO) — zero rows
6. FROM after all data (was the SIGSEGV trigger)
7. FROM before all data, TO within data
8. Multiple aggregates with keyed FROM/TO

## Prior Decisions (carried from Phase 2 + 3)

- DST not an issue for sub-day strides — evenly spaced UTC buckets
- followedOrderByAdvice=true with inner sort
- OrderedMap for key discovery (insertion-order, stable key order)
- FILL_KEY = -3 for key columns in fillModes
- Two-pass streaming: pass 1 keys, toTop(), pass 2 fill
- Per-key prev in MapValue slots

## Canonical References

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — fill cursor (crash is here)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillPrevRecordCursor.java:205-236` — cursor-path initializeMap() (reference for keyed behavior)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — existing tests

## Deferred Ideas

- Direct side-by-side comparison tests (fast path vs cursor path)
- Non-numeric PREV with keyed FROM/TO
- Day+ stride keyed FROM/TO
