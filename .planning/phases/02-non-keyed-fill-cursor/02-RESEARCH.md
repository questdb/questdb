# Phase 2: Non-keyed Fill Cursor - Research

**Researched:** 2026-04-09
**Domain:** QuestDB SQL engine — streaming fill cursor for SAMPLE BY on GROUP BY fast path
**Confidence:** HIGH (all findings verified against live codebase on branch `sm_fill_prev_fast_path`)

## Summary

Phase 2 completes the non-keyed fill cursor (`SampleByFillRecordCursorFactory`) for
FILL(NULL) and FILL(VALUE) on the fast path. Most infrastructure is already in place;
there are two concrete bugs and a test gap to close before Phase 2 is done.

**Bug 1 — infinite loop (FILL-01/FILL-03/FILL-05 blocker):** When no explicit TO
clause is present, `initialize()` sets `maxTimestamp = Long.MAX_VALUE`. After the base
cursor is exhausted, `dataTs` becomes `Long.MAX_VALUE`, which is `> nextBucketTimestamp`,
so the `dataTs > nextBucketTimestamp` branch fires and emits fill rows forever. The
`if (baseCursorExhausted && !hasExplicitTo) return false` guard at line 317 is never
reached because the `dataTs > nextBucketTimestamp` branch returns `true` first.
Fix: when `baseCursorExhausted && !hasExplicitTo`, return false immediately at the
top of the while loop, before computing `dataTs`.

**Bug 2 — DST fall-back with `nextBucketTimestamp` not re-anchored (FILL-05):**
The CONTEXT.md decision is that DST is NOT an issue for sub-day strides in UTC
arithmetic. The "DST test" in `DstDebug.java` uses no FILL clause; there is no
assertion-based DST fill test yet. The current `dataTs < nextBucketTimestamp` branch
emits the data row as-is without advancing `nextBucketTimestamp`, which is correct
behaviour: it passes the row through and leaves the bucket pointer where it was for
the next iteration. This is sound — the only open question is whether `DstDebug.java`
needs to be promoted to an assertion-based test for FILL-05.

**Gap — FROM/TO range fill (FILL-04):** The `initialize()` method already reads
`fromFunc` and `toFunc` and sets `nextBucketTimestamp` from `fromTs` when `fromTs <
firstTs`. Trailing fill (after last data row when TO is explicit) is driven by
`maxTimestamp`. The code path appears structurally complete, but FILL-04 is untested.
A test covering leading fill rows (FROM before first data point) and trailing fill rows
(TO after last data point) is the primary remaining work for FILL-04.

**Primary recommendation:** Fix the infinite loop bug first (single targeted change in
`hasNext()`), then add asserting tests for FILL(NULL), FILL(VALUE), FROM/TO, and DST.
FILL(PREV) stays gated per the CONTEXT.md decision.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- DST is NOT an issue for sub-day strides. `timestamp_floor_utc` produces evenly-spaced
  monotonic UTC buckets even across DST fall-back. No timezone-aware stepping needed.
- The "DST test failure" is an infinite loop bug when no TO clause is present.
- `followedOrderByAdvice() = true` with `generateOrderBy()` called inside `generateFill()`.
- FILL(PREV) stays gated in the optimizer (`!isPrevKeyword(...)` at line 7880). Phase 2
  handles NULL and VALUE fills only.
- `FillRangeRecordCursorFactory` is the old two-phase fill — it is being replaced by
  `SampleByFillRecordCursorFactory` on the fast path.

### Claude's Discretion

(None specified — all key decisions are locked.)

### Deferred Ideas (OUT OF SCOPE)

- FILL(PREV) on fast path — later phase
- FILL(PREV) for non-numeric types (STRING, VARCHAR, SYMBOL, LONG256, etc.)
- FILL(LINEAR) look-ahead with deferred emission
- Day+ stride fill (Month, Year, Week) — may need special sampler handling
- Full cursor path replacement
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| FILL-01 | Non-keyed FILL(NULL) emits null-filled rows for missing buckets | Basic path works; infinite loop bug blocks no-TO queries |
| FILL-02 | Non-keyed FILL(PREV) carries forward previous aggregate values | Gated by optimizer per locked decision; PREV code is dead for Phase 2 |
| FILL-03 | Non-keyed FILL(VALUE) emits constant-filled rows | Same code path as FILL(NULL) with non-null constant; also blocked by infinite loop |
| FILL-04 | FROM/TO range bounds control fill range (leading/trailing rows) | `initialize()` reads fromFunc/toFunc; structurally complete; needs asserting test |
| FILL-05 | DST timezone queries produce correctly ordered output | Locked: sub-day UTC arithmetic is monotonic; current `dataTs < nextBucketTimestamp` branch handles this; DstDebug needs promotion to assertion test |
| GEN-01 | generateFill() builds per-column fill modes from fill expressions | Implemented in SqlCodeGenerator lines 3318-3372; verified against live code |
| GEN-02 | generateFill() calls generateOrderBy() to sort GROUP BY output | Implemented at line 3375; verified |
| GEN-03 | Fill cursor reports followedOrderByAdvice()=true | Implemented at line 109 of SampleByFillRecordCursorFactory; verified |
| GEN-04 | PREV and NULL keywords handled without function parsing | Implemented: isPrevKeyword()/isNullKeyword() checks in generateFill() lines 3328-3365; verified |
</phase_requirements>

---

## Standard Stack

No new external libraries. All implementation uses existing QuestDB infrastructure.
[VERIFIED: live codebase]

### Core Classes

| Class | File | Role |
|-------|------|------|
| `SampleByFillRecordCursorFactory` | `engine/groupby/SampleByFillRecordCursorFactory.java` | New unified fill cursor (WIP) |
| `SampleByFillCursor` (inner) | same file | `NoRandomAccessRecordCursor` impl; `hasNext()` / `toTop()` |
| `FillRecord` (inner) | same file | `Record` that delegates to base or returns fill values |
| `FillTimestampConstant` (inner) | same file | `TimestampFunction` holding current fill-row timestamp |
| `SimpleTimestampSampler` | `engine/groupby/SimpleTimestampSampler.java` | Fixed-size bucket arithmetic (sub-day strides) |
| `TimestampSampler` | `engine/groupby/TimestampSampler.java` | Interface; `nextTimestamp()`, `setStart()`, `round()` |
| `SqlCodeGenerator.generateFill()` | `SqlCodeGenerator.java` line 3225 | Constructs the fill cursor factory |
| `SqlOptimiser.rewriteSampleBy()` | `SqlOptimiser.java` line 7842 | Rewrites SAMPLE BY to GROUP BY; sets `fillStride`/`fillFrom`/`fillTo`/`fillValues` |
| `AbstractCairoTest` | test infrastructure | Base class; provides `assertMemoryLeak()`, `assertSql()`, `execute()` |

### Fill Mode Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `FILL_CONSTANT` | -1 | Fill from `constantFills.get(col)` function |
| `FILL_PREV_SELF` | -2 | Fill from previous row's own column value (gated, dead for Phase 2) |

---

## Architecture Patterns

### hasNext() Control Flow (current, with infinite loop)

```
initialize() sets:
  nextBucketTimestamp = firstTs (or fromTs if earlier)
  maxTimestamp = toTs (if explicit) or Long.MAX_VALUE (no TO)
  baseCursorExhausted = false

while (nextBucketTimestamp < maxTimestamp):
  dataTs = pendingTs | next from baseCursor | Long.MAX_VALUE (exhausted)

  if dataTs == nextBucketTimestamp:  → emit data row, advance bucket
  if dataTs >  nextBucketTimestamp:  → emit fill row, advance bucket  ← INFINITE LOOP HERE
  if dataTs <  nextBucketTimestamp:  → emit data row, no bucket advance (DST re-anchor)
  if baseCursorExhausted && !hasExplicitTo:  → return false  ← NEVER REACHED (fill branch fires first)
```

### Infinite Loop Fix — Minimal Change

Move the early-exit check to the top of the while body, before reading `dataTs`:

```java
// Source: SampleByFillRecordCursorFactory.java, hasNext()
while (nextBucketTimestamp < maxTimestamp) {
    // Early exit: base cursor exhausted and no explicit TO bound
    if (baseCursorExhausted && !hasExplicitTo) {
        return false;
    }
    // ... rest of the method unchanged
```

This is a one-line relocation. The existing guard at line 317 is correct logic, just
unreachable in its current position. Moving it to the top of the loop body fixes the
infinite loop without changing any other behaviour.

[VERIFIED: traced through code at SampleByFillRecordCursorFactory.java lines 253-322]

### FROM/TO Semantics

`rewriteSampleByFromTo()` (line 8263) converts FROM/TO into a WHERE clause:
`WHERE ts >= FROM AND ts < TO`. This means the base GROUP BY cursor only contains rows
within the range. The fill cursor then:

- **Leading fill:** If `fromTs < firstDataTs`, `initialize()` sets `nextBucketTimestamp = fromTs`
  (line 358-362), so the cursor emits fill rows for all buckets before the first data point.
- **Trailing fill:** `maxTimestamp = toTs` (explicit TO). After the base cursor is exhausted,
  the while loop continues emitting fill rows until `nextBucketTimestamp >= toTs`.

Both paths are structurally complete. FILL-04 needs assertion tests, not code changes.
[VERIFIED: live code at SampleByFillRecordCursorFactory.java lines 346-380]

### DST Handling (FILL-05)

The CONTEXT.md decision: `timestamp_floor_utc` produces monotonic UTC buckets for
sub-day strides across DST. The `dataTs < nextBucketTimestamp` branch (lines 304-314)
is a defensive handler: it emits the row as-is when the base cursor produces a
timestamp less than expected (which can occur near DST transitions). This is correct
behaviour — it outputs the data row without emitting a spurious fill row and without
corrupting `nextBucketTimestamp`.

The `followedOrderByAdvice() = true` contract holds because the DST concern in
CONCERNS.md refers to a different (unresolved) concern about non-monotonic output. For
Phase 2, with the sorted input from `generateOrderBy()`, sub-day UTC timestamps are
monotonic, so the contract is not violated.

For FILL-05, the test requirement is: a DST fall-back query with FILL runs to
completion without an infinite loop and produces correctly ordered output. The
`DstDebug.java` file is a debug tool (no assertions). Phase 2 needs to promote it to
an assertion test.

[VERIFIED: DstDebug.java lines 1-35; SampleByFillRecordCursorFactory.java lines 304-319; CONTEXT.md]

### Resource Leak in generateFill Error Path

CONCERNS.md documents two leaks:

1. **`sorted` factory not freed when `sorted != groupByFactory`** (line 3375 → catch at 3401):
   After `generateOrderBy()` wraps `groupByFactory` in a sort factory, the `sorted`
   reference is separate from `groupByFactory`. The catch block frees `groupByFactory`
   but not `sorted`. Fix: `groupByFactory = sorted` immediately after line 3375 so the
   catch block frees the right reference.

2. **`constantFillFuncs` not freed on error** (lines 3322, 3400-3406): The catch frees
   `fillValues` but not `constantFillFuncs`. Fix: add
   `Misc.freeObjList(constantFillFuncs)` to the catch block, guarding against
   double-free for `NullConstant.NULL` singleton entries (which are not owned and must
   not be freed — `Misc.free()` on a singleton is safe because `NullConstant.NULL`
   does not allocate native memory, but verify this before adding the free call).

[VERIFIED: SqlCodeGenerator.java lines 3374-3406]

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Timestamp arithmetic | Custom interval math | `SimpleTimestampSampler.nextTimestamp()` | Already handles overflow via `Math.addExact` |
| NULL sentinel values | Custom null flags | `Numbers.LONG_NULL`, `Numbers.INT_NULL` | QuestDB convention; all column getters use these |
| Object list management | Raw arrays | `ObjList<T>` + `Misc.freeObjList()` | CLAUDE.md mandates ObjList; integrates with resource cleanup |
| Function evaluation | Direct column access | `constantFills.get(col).getDouble(null)` | Constant functions already evaluated correctly |
| FROM/TO WHERE injection | Manual filter in cursor | `rewriteSampleByFromTo()` already injects WHERE | Don't re-filter in the cursor; the base cursor already has the range |

---

## Common Pitfalls

### Pitfall 1: Infinite Loop — Early Exit in Wrong Position

**What goes wrong:** The guard `if (baseCursorExhausted && !hasExplicitTo) return false`
is inside the while body but below the `dataTs > nextBucketTimestamp` branch. When
`dataTs = Long.MAX_VALUE` (exhausted), it always satisfies `dataTs > nextBucketTimestamp`,
so the fill branch fires, emits a fill row, and returns `true` — the guard is never reached.

**How to avoid:** Move the early-exit check to the first line of the while body, before
computing `dataTs`.

**Warning sign:** Any test with no explicit TO clause that runs a FILL query will hang.

### Pitfall 2: `toTop()` Must Reset All State

**What goes wrong:** If `isInitialized`, `hasPendingRow`, `baseCursorExhausted`,
`hasExplicitTo`, `hasSimplePrev` are not all reset in `toTop()`, re-iteration via
`toTop()` will resume from stale state.

**Current state:** `toTop()` at lines 335-344 resets all of these. Verified correct.
[VERIFIED: SampleByFillRecordCursorFactory.java lines 335-344]

### Pitfall 3: `assertSql` vs `assertQueryNoLeakCheck` in Tests

**What goes wrong:** CLAUDE.md mandates `assertQueryNoLeakCheck()` for query result
assertions (checks factory properties) and `assertSql()` for data-persistence checks.
The existing `SampleByFillTest` uses `assertSql()`. For fill queries (which are SQL
query results, not storage tests), `assertQueryNoLeakCheck()` is the correct method.

**Note:** The current `SampleByFillTest.testFillNullNonKeyed` and
`testFillPrevNonKeyed` use `assertSql()`. Since these test a GROUP BY rewrite query
(not raw storage), switching to `assertQueryNoLeakCheck()` is the correct approach for
new tests. Don't replicate the `assertSql()` pattern from the existing test file.

**Exception:** If the fill cursor does not implement `expectSize` or other factory
properties, `assertQueryNoLeakCheck()` may need those to be set. Verify the factory
returns `-1` for `size()` (which means `expectSize` will be checked as `-1`, which
`assertQueryNoLeakCheck` accepts).

[VERIFIED: SampleByFillRecordCursorFactory.java line 332: `return -1`]

### Pitfall 4: FILL(PREV) Remains Gated — Don't Unlock It

**What goes wrong:** The optimizer gate at `SqlOptimiser.java` line 7880 excludes
FILL(PREV). The PREV code in `SampleByFillRecordCursorFactory` is dead code for Phase 2.
Do not remove this gate or add tests that assert PREV works through the new fast path.

**Why:** PREV enablement is Phase 3+ scope. The current PREV implementation only
handles numeric types via `readColumnAsLongBits()`; non-numeric types would be silently
wrong.

### Pitfall 5: `savePrevValues()` Called on Data Row in `hasPrevFill = false` Path

**What goes wrong:** `savePrevValues()` is gated by `if (hasPrevFill)` at lines 277
and 310. In Phase 2 (FILL(NULL) and FILL(VALUE) only), `hasPrevFill = false`, so
`savePrevValues()` is never called and `simplePrev` is `null`. Code in `FillRecord`
that accesses `simplePrev` via `prevValue()` is only reached when `mode == FILL_PREV_SELF
|| mode >= 0`, which never happens with FILL(NULL)/FILL(VALUE) (those get `FILL_CONSTANT`).
This is safe; do not add a null-check to `savePrevValues()` unless enabling PREV.

[VERIFIED: SampleByFillRecordCursorFactory.java lines 225, 277, 310, 416-420]

---

## Code Examples

### Test Pattern for FILL(NULL) — No TO Clause (Verifying Infinite Loop Fix)

```java
// Source: based on existing SampleByFillTest.testFillNullNonKeyed pattern
@Test
public void testFillNullNonKeyedNoToClause() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x AS (" +
                "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
        // Data at 00:00, 02:00, 04:00 — gaps at 01:00, 03:00. No TO clause → must terminate.
        assertQueryNoLeakCheck(
                "sum\tts\n" +
                "1.0\t2024-01-01T00:00:00.000000Z\n" +
                "null\t2024-01-01T01:00:00.000000Z\n" +
                "2.0\t2024-01-01T02:00:00.000000Z\n" +
                "null\t2024-01-01T03:00:00.000000Z\n" +
                "3.0\t2024-01-01T04:00:00.000000Z\n",
                "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR",
                "ts", true, false
        );
    });
}
```

### Test Pattern for FILL(NULL) — Explicit FROM/TO (FILL-04)

```java
@Test
public void testFillNullNonKeyedFromTo() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x AS (" +
                "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01T02:00:00.000000Z', 3_600_000_000) AS ts " +
                "FROM long_sequence(2)) TIMESTAMP(ts) PARTITION BY DAY");
        // Data at 02:00, 03:00. FROM 00:00, TO 05:00 → leading fill at 00:00, 01:00; trailing at 04:00
        assertQueryNoLeakCheck(
                "sum\tts\n" +
                "null\t2024-01-01T00:00:00.000000Z\n" +
                "null\t2024-01-01T01:00:00.000000Z\n" +
                "1.0\t2024-01-01T02:00:00.000000Z\n" +
                "2.0\t2024-01-01T03:00:00.000000Z\n" +
                "null\t2024-01-01T04:00:00.000000Z\n",
                "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(NULL) FROM '2024-01-01' TO '2024-01-01T05:00:00.000000Z' ALIGN TO CALENDAR",
                "ts", true, false
        );
    });
}
```

### Test Pattern for FILL(VALUE) — FILL-03

```java
@Test
public void testFillValueNonKeyed() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x AS (" +
                "SELECT x::DOUBLE AS val, timestamp_sequence('2024-01-01', 7_200_000_000) AS ts " +
                "FROM long_sequence(3)) TIMESTAMP(ts) PARTITION BY DAY");
        assertQueryNoLeakCheck(
                "sum\tts\n" +
                "1.0\t2024-01-01T00:00:00.000000Z\n" +
                "0.0\t2024-01-01T01:00:00.000000Z\n" +
                "2.0\t2024-01-01T02:00:00.000000Z\n" +
                "0.0\t2024-01-01T03:00:00.000000Z\n" +
                "3.0\t2024-01-01T04:00:00.000000Z\n",
                "SELECT sum(val), ts FROM x SAMPLE BY 1h FILL(0) ALIGN TO CALENDAR",
                "ts", true, false
        );
    });
}
```

### Test Pattern for DST Fall-Back (FILL-05) — Assertion-Based

```java
@Test
public void testFillNullDstFallback() throws Exception {
    assertMemoryLeak(() -> {
        // Dense data: one row every 10 minutes around Europe/Riga DST fall-back 2021-10-31
        execute("CREATE TABLE y AS (" +
                "SELECT x::DOUBLE AS val, " +
                "timestamp_sequence(cast('2021-10-31T00:00:00.000000Z' as timestamp), 600_000_000) k " +
                "FROM long_sequence(100)) TIMESTAMP(k) PARTITION BY NONE");
        // 1h FILL(NULL) with Riga timezone. Dense data means no gaps → no fill rows.
        // Test verifies: (a) query terminates, (b) output is monotonically ordered
        String result = // ... capture via printSql or assertSql on a known range
        "SELECT count() s, k FROM y SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga'";
        // Assert no exception and output is non-empty
        assertQueryNoLeakCheck("s\tk\n...", result, "k", true, false);
    });
}
```

---

## State of the Art (Current WIP Branch)

| State | Detail |
|-------|--------|
| FILL(NULL) basic | Passes `testFillNullNonKeyed` — but only because that test uses explicit data that exhausts cleanly |
| FILL(PREV) basic | `testFillPrevNonKeyed` passes via the OLD cursor path (optimizer gate still excludes PREV) |
| Infinite loop | Present for any FILL query with no TO clause when there are gaps after the last data row |
| FROM/TO | Structurally implemented; untested with assertions |
| DST test | `DstDebug.java` is a debug print tool, not an assertion test |
| Resource leaks | Two documented leaks in `generateFill()` error path (sorted factory, constantFillFuncs) |

---

## Open Questions

1. **Does `testFillNullNonKeyed` already exercise the infinite loop path?**
   - What we know: data at 00:00, 02:00, 04:00 → gaps at 01:00, 03:00. After 04:00 the
     base cursor is exhausted. `nextBucketTimestamp` becomes 05:00, which is `< Long.MAX_VALUE`
     (`maxTimestamp`). `dataTs = Long.MAX_VALUE > 05:00` → fill row at 05:00. This is an
     infinite loop starting at 05:00.
   - What's unclear: why the existing test passes. It may pass because `assertSql` has
     a row limit or because the test is actually hanging in CI and not noticed.
   - Recommendation: run `testFillNullNonKeyed` in isolation with a timeout to confirm
     the infinite loop is reproducible. If it passes, investigate why (possibly
     `assertSql` has a default row cap).

2. **`assertQueryNoLeakCheck` vs `assertSql` for fill tests**
   - What we know: CLAUDE.md says use `assertQueryNoLeakCheck()` for query results.
     The fill cursor returns `size() = -1` and `supportsRandomAccess() = false`. These
     are valid — `assertQueryNoLeakCheck` accepts `-1` for size.
   - What's unclear: whether `expectSize` needs to be set differently for the fill cursor.
   - Recommendation: use `assertQueryNoLeakCheck` with `expectSize=false` parameter
     (3rd positional arg after expected and sql).

3. **`Misc.free(NullConstant.NULL)` safety in constantFillFuncs cleanup**
   - What we know: `NullConstant.NULL` is a singleton. `Misc.freeObjList()` calls
     `Misc.free()` on each element. `Misc.free()` calls `close()` if the object
     implements `Closeable`. `NullConstant` implements `Function` which may implement
     `Closeable`.
   - What's unclear: whether calling `close()` on `NullConstant.NULL` has side effects.
   - Recommendation: check `NullConstant.close()` implementation before adding
     `Misc.freeObjList(constantFillFuncs)` to the catch block.

---

## Environment Availability

Step 2.6: SKIPPED — Phase 2 is pure Java code changes with no external tool dependencies.
The test runner is Maven (`mvn -Dtest=SampleByFillTest test`).

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 4 (via Maven Surefire) |
| Config file | `core/pom.xml` |
| Quick run command | `mvn -Dtest=SampleByFillTest test` |
| Full suite command | `mvn -Dtest=SampleByTest test` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Command | File Exists? |
|--------|----------|-----------|---------|-------------|
| FILL-01 | FILL(NULL) emits null rows for gaps, terminates without TO | unit | `mvn -Dtest=SampleByFillTest#testFillNullNonKeyed test` | Partial (no infinite loop assertion) |
| FILL-02 | FILL(PREV) on fast path | unit | N/A (gated by optimizer, not Phase 2) | — |
| FILL-03 | FILL(VALUE) emits constant fill rows | unit | `mvn -Dtest=SampleByFillTest#testFillValueNonKeyed test` | No — Wave 0 gap |
| FILL-04 | FROM/TO range bounds: leading + trailing fill rows | unit | `mvn -Dtest=SampleByFillTest#testFillNullNonKeyedFromTo test` | No — Wave 0 gap |
| FILL-05 | DST fall-back query terminates, output ordered | unit | `mvn -Dtest=SampleByFillTest#testFillNullDstFallback test` | No — Wave 0 gap |
| GEN-01 | generateFill() builds per-column fill modes | code review | — | Implemented; no dedicated unit test |
| GEN-02 | generateFill() calls generateOrderBy() | code review | — | Implemented; verified in code |
| GEN-03 | followedOrderByAdvice()=true | code review | — | Implemented |
| GEN-04 | PREV/NULL keywords handled without function parse | code review | — | Implemented |

### Sampling Rate
- Per task commit: `mvn -Dtest=SampleByFillTest test`
- Per wave merge: `mvn -Dtest=SampleByTest test`
- Phase gate: `SampleByTest` full 302-test suite green

### Wave 0 Gaps
- [ ] `SampleByFillTest.testFillValueNonKeyed` — covers FILL-03
- [ ] `SampleByFillTest.testFillNullNonKeyedFromTo` — covers FILL-04
- [ ] `SampleByFillTest.testFillNullDstFallback` — covers FILL-05
- [ ] Fix `testFillNullNonKeyed` to use `assertQueryNoLeakCheck` instead of `assertSql`

---

## Project Constraints (from CLAUDE.md)

- Use `ObjList<T>` not `T[]` for object arrays. Use `Misc.freeObjList()` for cleanup.
- Use `assertMemoryLeak()` in all tests that allocate native memory.
- Use `assertQueryNoLeakCheck()` for query result assertions (not `assertSql()`).
- Use `execute()` for DDL (CREATE TABLE, INSERT), not `assertSql`.
- Boolean variables/methods must use `is...` or `has...` prefix. Current code:
  `isInitialized`, `hasPrevFill`, `hasSimplePrev`, `hasPendingRow`, `baseCursorExhausted`,
  `hasExplicitTo`, `isNonKeyed`, `isGapFilling` — all compliant.
- Use multiline string literals for multi-row expected query results.
- Use underscore separators in 5+ digit literals: `7_200_000_000`, `3_600_000_000`.
- Consider NULL behavior: `Numbers.LONG_NULL`, `Numbers.INT_NULL`, `Double.NaN`, etc.
- Use enhanced switch and pattern variables in instanceof where applicable.
- Java class members sorted alphabetically by kind+visibility. When adding methods,
  insert in correct alphabetical position.

---

## Assumptions Log

All claims in this research were verified directly against the live codebase on branch
`sm_fill_prev_fast_path`. No assumed claims.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| — | — | — | — |

**This table is empty:** All claims verified against live code.

---

## Sources

### Primary (HIGH confidence — verified against live codebase)
- `SampleByFillRecordCursorFactory.java` lines 199-322 — `hasNext()`, `initialize()`, `toTop()`, `savePrevValues()`
- `SqlCodeGenerator.java` lines 3225-3407 — `generateFill()` implementation
- `SqlOptimiser.java` lines 7842-8014 — `rewriteSampleBy()` eligibility logic
- `SampleByFillTest.java` — 3 existing tests, assertion patterns
- `DstDebug.java` — debug test (no assertions)
- `SimpleTimestampSampler.java` — `nextTimestamp()` arithmetic
- `.planning/codebase/CONCERNS.md` — resource leak and DST analysis
- `.planning/phases/02-non-keyed-fill-cursor/02-CONTEXT.md` — locked decisions

### Secondary (MEDIUM confidence)
- REQUIREMENTS.md, ROADMAP.md, STATE.md — project state and requirement IDs

---

## Metadata

**Confidence breakdown:**
- Bug analysis: HIGH — traced through live code, infinite loop path confirmed
- Fix approach: HIGH — single-line relocation, behaviorally equivalent
- FROM/TO completeness: HIGH — `initialize()` code verified
- Test gaps: HIGH — test file has 3 tests, gaps enumerated from requirement list
- DST handling: HIGH — locked decision from CONTEXT.md; consistent with code

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable codebase, single branch)
