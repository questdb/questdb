# Phase 9 Context: Fix Critical Review Findings

## Domain Boundary

Fix 3 critical bugs from code review in SampleByFillRecordCursorFactory. No new features. Also fix minor Javadoc inconsistency.

## Decisions

### Fix 1: Geo column PREV fill returns null instead of carrying forward

**Problem**: `getGeoByte()`, `getGeoShort()`, `getGeoInt()`, `getGeoLong()` at lines 808-841 only check `FILL_KEY` and `FILL_CONSTANT`. They skip `FILL_PREV_SELF` and `mode >= 0`. The prev value IS saved correctly by `readColumnAsLongBits()`, but the getter never reads it.

**Fix**: Add `if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) return (cast) prevValue(col);` to all four geo getters. Same pattern as `getInt()`, `getLong()`, etc.

### Fix 2: Unchecked findValue() NPE

**Problem**: Line 351 — `mapKey.findValue()` dereferenced without null check. If pass 2 produces a key not seen in pass 1, NPE. The DST fallback path at line 401 correctly null-checks.

**Fix**: Add `if (value == null) continue;` or assert after `findValue()` at line 351. Use assert since this invariant should hold (pass 1 discovers all keys from the same cursor).

### Fix 3: Recursive hasNext() stack overflow

**Problem**: `emitNextFillRow()` calls `hasNext()` at line 462 after completing a bucket's fills. Consecutive empty buckets cause recursion proportional to gap count. Sparse data + large range = stack overflow.

**Decision**: Inline loop in `emitNextFillRow()`. Instead of calling `hasNext()`, the method loops over empty buckets internally. When it exhausts fills for one bucket, it advances `nextBucketTimestamp` and checks if the next bucket also needs fills (all keys absent = full gap bucket). It loops until it finds a bucket with data (pending row) or exceeds `maxTimestamp`. Returns `true` if a fill row was emitted, `false` if no more data.

### Fix 4: Javadoc inconsistency (minor)

**Problem**: Class Javadoc at line 67 says `followedOrderByAdvice=true` but implementation returns `false`.

**Fix**: Update Javadoc to say `followedOrderByAdvice=false — the outer sort handles ordering`.

## Canonical References

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — all fixes here
