# Phase 14: Fix Moderate `/review-pr 6946` findings — Research

**Researched:** 2026-04-20
**Domain:** QuestDB SAMPLE BY FILL — verification of codebase assumptions for Phase 14 plans
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**D-01 — Four plans, grouped by area + one standalone pre-existing fix as final commit.**
- **Plan 01 — Codegen cluster**: M-1, M-3, M-9, Mn-13. All in `SqlCodeGenerator.java`. Share the `generateFill()` neighborhood.
- **Plan 02 — Cursor cluster**: M-2, M-8. All in `SampleByFillRecordCursorFactory.java` `FillRecord` class. Both add missing getter branches; natural to bundle.
- **Plan 03 — Optimiser**: M-4. `SqlOptimiser.java` + fill cursor init path. Cross-file but single concern.
- **Plan 04 — Pre-existing defensive fix**: M-7 standalone. Last commit of the phase.

**D-02 — Commit hygiene strict from commit 1**: CLAUDE.md-compliant titles (no Conventional Commits prefix, under 50 chars, long-form body). No `WIP:` or phase-numbered commits.

**D-03 — Invest in regression triggers** even for hard-to-reproduce cases (M-1 reorder, M-7 constructor throw).

**D-04 — Per-plan verification**: each plan's tests pass in isolation; the combined SAMPLE BY trio passes at phase end (`SampleByTest + SampleByFillTest + SampleByNanoTimestampTest`).

**D-05 — M-1 bare FILL(PREV) reorder-safe classification**: reuse `factoryColToUserFillIdx` alias-mapping machinery from the per-column branch. Walk `bottomUpCols` in user order; decide `FILL_KEY` vs `FILL_PREV_SELF` per factory column via the mapping, not by passing factory-index to user-order `isKeyColumn`.

**D-06 — M-2 ARRAY/BIN getters missing dispatch**: add the missing `FILL_KEY` and cross-column-PREV-to-key branches to `getArray`, `getBin`, `getBinLen` — mirror the exact 4-branch pattern used by every other typed getter.

**D-07 — M-3 broadcast tightening**: at `SqlCodeGenerator.java:3567-3574`, replace `isPrevKeyword(only.token) || isNullKeyword(only.token)` with:
```java
final boolean isBareBroadcastable =
        (isPrevKeyword(only.token) && only.type == ExpressionNode.LITERAL)
     || isNullKeyword(only.token);
```

**D-08 — M-4 TIME ZONE + FROM propagation**: in `SqlOptimiser.rewriteSampleBy`, when `isSubDay && sampleByTimezoneName != null`, pass `createToUtcCall(sampleByFrom, sampleByTimezoneName)` to `nested.setFillFrom` (and TO counterpart to `setFillTo`). `createToUtcCall` already at `SqlOptimiser.java:2725`.

**D-09 — M-4 fill cursor parsing unchanged**: `SqlCodeGenerator.generateFill` continues to call `functionParser.parseFunction(fillFrom, …)`, which evaluates the `to_utc` wrapper naturally. No new cursor state.

**D-10 — M-7 double-free defensive fix**: inside `SortedRecordCursorFactory` constructor, assign `this.base` inside the try block so that on `new RecordTreeChain(...)` throw, the catch block can null it out before calling `close()`. Pick lower-risk option by reading the class; prefer per-class fix (nulling `this.base`) unless hierarchy-wide guard is trivially safe.

**D-11 — M-8 INTERVAL getter**: add `FillRecord.getInterval(int col)` mirroring the 4-branch dispatch. Default null sentinel: use `Interval.NULL`. Then audit `Record` for every `default throw UOE` method; ensure `FillRecord` covers each.

**D-12 — M-9 cross-column PREV full-type match**: replace `tagOf(target) != tagOf(source)` with full-int equality for DECIMAL / GEOHASH / ARRAY. Prefer existing helper (probe `ColumnType` for `isSameType`, `equalsExactly`); otherwise inline with `needsExactTypeMatch` flag on `isDecimal/isGeoHash/ARRAY`. Error message includes both types via `ColumnType.nameOf(...)`.

**D-13 — Mn-13 fillValues success-path cleanup**: `Misc.freeObjList(fillValues)` immediately before the `return new SampleByFillRecordCursorFactory(...)` on the success path. Transferred slots are already null; only residuals are freed. Catch-block semantics unchanged.

**D-14 — Per-type dedicated `FILL(PREV)` tests** for: BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, IPv4, BINARY, INTERVAL. Live in `SampleByFillTest.java`. Each test exercises non-keyed FILL(PREV) for its type with a dense-then-gap pattern. INTERVAL mandatory for M-8.

**D-15 — Default helper**: `assertQueryNoLeakCheck`. Fall back to `assertSql` only when the cursor has a documented factory-property limitation.

**D-16 — Ghost-test cleanup**: `SampleByTest.testSampleByFromToIsDisallowedForKeyedQueries` and nano twin. **Choice: RENAME + assert.** Keep the history of why this case was originally an error in a comment.

**D-17 — Decimal zero-vs-null distinction test**: `FILL(PREV)` query where some buckets have legitimate `0` decimal values and some precede any data. Assert the two render differently (NULL vs `0.00`).

**D-18 — Restore `RecordCursorMemoryUsageTest` direct factory-type assertion**. Update the three tests currently asserting `SelectedRecordCursorFactory` to assert `SampleByFillRecordCursorFactory` is in the base-factory chain.

**D-19 — Add M-5/M-6 to existing `## Trade-offs` section of PR #6946's body.** Do not create a new section.

**D-20 — Claude proposes wording; user edits.** No benchmark prescription (no SAMPLE BY FILL JMH benchmarks exist today, verified). Just flag O(K×B) memory envelope and 3× scan cost honestly. Commit the PR body update as part of Phase 14 (or via `gh pr edit`).

### Claude's Discretion
- M-7 exact fix mechanism (null `this.base` in constructor catch vs. add `isClosed` guard) — pick lower-risk option after reading the factory hierarchy.
- Exact placement of new test methods within `SampleByFillTest.java` — alphabetical per CLAUDE.md.
- Helper method extraction — if the 4-branch dispatch pattern can be factored without per-row method-call overhead, do it.

### Deferred Ideas (OUT OF SCOPE)
- **M-5 architectural fix** — merge-sort wrapper or circuit-breaker. Future phase after benchmark coverage.
- **M-6 architectural fix** — populating `keysMap` during `SortedRecordCursor.buildChain` via callback. Future phase.
- **JMH benchmark harness for SAMPLE BY FILL** — future phase.
- **Tighten `testFillPrevRejectNoArg` fuzzy 4-message assertion** — Mn-11 item, descoped.
- **Mn-1..Mn-10, Mn-12 cleanups** — separate cleanup pass or case-by-case.
</user_constraints>

## Project Constraints (from CLAUDE.md)

**Non-negotiable directives applying to every Phase 14 change:**
- Java class members sorted alphabetically within kind + visibility. **No `// ===` / `// ---` banner comments.**
- Enhanced switch, multiline string literals, `instanceof` pattern variables (Java 17). The `java-questdb-client` module stays Java 11, but Phase 14 does not touch it.
- `is…` / `has…` prefix for all new boolean names.
- Log messages ASCII-only.
- Tests use `assertMemoryLeak` for anything allocating native memory. `assertQueryNoLeakCheck` asserts factory properties; `assertSql` for storage/data-only checks.
- DDL via `execute()`; UPPERCASE SQL keywords; multi-row `INSERT`; multiline strings for longer SQL; `_` thousands-separators in 5+ digit numbers.
- `SqlException.$(position, msg)` — position must point at the specific offending character.
- Commit title ≤ 50 chars, plain English, no Conventional Commits prefix; full long-form body required, lines ≤ 72 chars.
- PR title uses Conventional Commits (`fix(sql): …`), but commits on the branch do NOT.

**Planner MUST verify these when structuring plans**: no banner comments, alphabetical placement of new methods, UPPERCASE SQL in tests, multi-row INSERT grouping.

## Summary

The CONTEXT.md already pins 20 locked decisions covering fix mechanics, test shapes, and commit grouping. This research verifies every codebase fact the four plans depend on and flags discrepancies. **All assumptions confirmed.** One minor enrichment: the Mn-13 success-path cleanup must precede the `return new SampleByFillRecordCursorFactory(...)` at `SqlCodeGenerator.java:3672`, **before** the transferred-slot invariant is consumed, not after — behavior consistent with D-13 wording.

**Primary recommendation:** Proceed to planning immediately. No CONTEXT gaps. The codebase is exactly as described: `factoryColToUserFillIdx` is at lines 3426-3461; `createToUtcCall` is at `SqlOptimiser.java:2725`; `ColumnType.isDecimal`/`isGeoHash`/`tagOf`/`nameOf`/`ARRAY` exist but no `isSameType`/`equalsExactly` helper exists — D-12 must inline. `Interval.NULL` exists as `new Interval(LONG_NULL, LONG_NULL)`. The M-7 bug mechanics match exactly: `this.base` is `private final` and assigned on line 56 BEFORE the try block, so on `RecordTreeChain` throw, `close()` → `_close()` → `Misc.free(base)` double-frees a caller-owned factory.

<phase_requirements>
## Phase Requirements

No new requirement IDs introduced by Phase 14. The phase closes code-review-discovered bugs against requirements already landed:

| ID | Description | Research Support |
|----|-------------|------------------|
| OPT-01 / OPT-02 | Optimizer rewrites SAMPLE BY FILL to fast path | M-4 (D-08) restores parity between fill grid and tsFloor grid under sub-day + TIME ZONE + FROM |
| FILL-02 | Non-keyed FILL(PREV) | M-8 (D-11) fixes INTERVAL crash; M-2 (D-06) fixes ARRAY/BIN dispatch |
| KEY-01..KEY-05 | Keyed fill semantics | M-1 (D-05) prevents mis-classification of keys after optimizer reorder |
| XPREV-01 | Cross-column PREV | M-3 (D-07) stops silent broadcast; M-9 (D-12) closes DECIMAL/GEOHASH/ARRAY precision leak |
| PTSF-01..PTSF-06 | Type-safe fast path | M-9 cross-column full-type match closes residual type leak |
| COR-01..COR-04 | Correctness, no leaks, parity with cursor path | Mn-13 (D-13) closes success-path leak; D-14..D-18 extend regression coverage |

The per-plan verification in D-04 exercises the combined SAMPLE BY trio (`SampleByTest + SampleByFillTest + SampleByNanoTimestampTest`) at phase end; the full suite run is the gate.
</phase_requirements>

## Verification Results

### 1. M-9 ColumnType helpers [VERIFIED: codebase grep]

**No full-int equality helper exists** on `ColumnType`. The helpers confirmed present:

| Helper | Location | Signature |
|--------|----------|-----------|
| `isDecimal(int)` | `ColumnType.java:510` | `public static boolean isDecimal(int type)` |
| `isDecimalType(int)` | `ColumnType.java:515` | `public static boolean isDecimalType(int colType)` (sibling; check during plan which is correct here) |
| `isGeoHash(int)` | `ColumnType.java:541` | `public static boolean isGeoHash(int columnType)` |
| `nameOf(int)` | `ColumnType.java:673` | `public static String nameOf(int columnType)` |
| `tagOf(int)` | `ColumnType.java:723` | `public static short tagOf(int type)` |
| `tagOf(CharSequence)` | `ColumnType.java:730` | overload |

**Constants confirmed:**
- `ColumnType.ARRAY` = 27 (line 108)
- `ColumnType.INTERVAL` = 39 (line 136); plus `INTERVAL_RAW`, `INTERVAL_TIMESTAMP_MICRO`, `INTERVAL_TIMESTAMP_NANO` variants
- `ColumnType.LONG128` = 24 (line 105)
- `ColumnType.UUID` = 19 (line 96)
- `ColumnType.GEOHASH` = 23 (line 104)
- `ColumnType.DECIMAL8..256` = 28..33 (lines 122-127)

**Action for D-12 plan:** inline the type-equality check as CONTEXT specifies. No helper to reuse. Planner MAY optionally extract a private `needsExactTypeMatch(int targetType)` helper in `SqlCodeGenerator` near `isFastPathPrevSupportedType` (CONTEXT allows this under Claude's Discretion — helper extraction without per-row overhead).

**Note on `isDecimal` vs `isDecimalType`:** two similarly named helpers exist. The plan should verify which covers all six DECIMAL width tags (DECIMAL8/16/32/64/128/256) before using it — a casual reader will otherwise pick the wrong one. If unclear at implementation time, prefer explicit `ColumnType.tagOf(t) >= ColumnType.DECIMAL8 && ColumnType.tagOf(t) <= ColumnType.DECIMAL256`.

### 2. M-8 INTERVAL null sentinel [VERIFIED: codebase grep]

**`Interval.NULL` EXISTS.**

```
/Users/sminaev/qdbwt/elegant-cuddling-sprout/core/src/main/java/io/questdb/std/Interval.java:33:
    public static final Interval NULL = new Interval(Numbers.LONG_NULL, Numbers.LONG_NULL);
```

**Action for D-11 plan:** use `Interval.NULL` directly as the fallthrough return. No discovery work needed at implementation time.

`Record.getInterval(int col)` at `Record.java:292-294` has a `default throw new UnsupportedOperationException()` body. `FillRecord` does **NOT** override it today — crash path confirmed.

### 3. FillRecord audit for M-8 completeness [VERIFIED: codebase grep]

**Record interface methods that `default throw UnsupportedOperationException`** (from `Record.java`):

| Line | Method | FillRecord overrides? |
|------|--------|----------------------|
| 68 | `getArray(int, int)` | yes (line 696) |
| 110 | `getBin(int)` | yes |
| 120 | `getBinLen(int)` | yes |
| 130 | `getBool(int)` | yes |
| 140 | `getByte(int)` | yes |
| 150 | `getChar(int)` | yes |
| 164 | `getDecimal128(int, Decimal128)` | yes |
| 174 | `getDecimal16(int)` | yes |
| 178 | `getDecimal256(int, Decimal256)` | yes |
| 188 | `getDecimal32(int)` | yes |
| 198 | `getDecimal64(int)` | yes |
| 208 | `getDecimal8(int)` | yes |
| 218 | `getDouble(int)` | yes |
| 228 | `getFloat(int)` | yes |
| 238 | `getGeoByte(int)` | yes |
| 248 | `getGeoInt(int)` | yes |
| 258 | `getGeoLong(int)` | yes |
| 268 | `getGeoShort(int)` | yes |
| 279 | `getIPv4(int)` | yes |
| 289 | `getInt(int)` | yes |
| **293** | **`getInterval(int)`** | **NO — M-8 target** |
| 303 | `getLong(int)` | yes |
| 307 | `getLong128Hi(int)` | yes |
| 311 | `getLong128Lo(int)` | yes |
| 321 | `getLong256(int, CharSink)` | yes |
| 335 | `getLong256A(int)` | yes |
| 349 | `getLong256B(int)` | yes |
| 371 | `getRecord(int)` | **no — out-of-scope** |
| 380 | `getRowId()` | **no — out-of-scope** |
| 390 | `getShort(int)` | yes |
| 410 | `getStrA(int)` | yes |
| 429 | `getStrB(int)` | yes |
| 439 | `getStrLen(int)` | yes |
| 452 | `getSymA(int)` | yes |
| 466 | `getSymB(int)` | yes |
| 485 | `getUpdateRowId()` | **no — out-of-scope** |
| 516 | `getVarcharA(int)` | yes |
| 536 | `getVarcharB(int)` | yes |
| 546 | `getVarcharSize(int)` | yes |

**Non-throwing defaults that currently delegate correctly** (no override needed):
- `getDate(int)` at `Record.java:159-161` returns `getLong(col)` — FillRecord's `getLong` is overridden, so `getDate` works transparently.
- `getTimestamp(int)` at `Record.java:475` is delegate-by-default to `getLong`, but `FillRecord` explicitly overrides it (line 1140) because timestamp has special `fillTimestampFunc` behavior.
- `getLongIPv4(int)` at `Record.java:360-362` delegates to `getIPv4` which FillRecord overrides.
- `getArrayDouble1d2d` at `Record.java:89-101` delegates to `getArray`.
- `getVarchar(int, Utf16Sink)` at `Record.java:495-497` delegates to `getVarcharA` which FillRecord overrides.

**Gap beyond M-8 (INTERVAL):** `getRecord`, `getRowId`, `getUpdateRowId` are unimplemented. These are internal plumbing (nested record field, table-row ID for UPDATE, filesystem row ID) and are never valid output columns of a SAMPLE BY FILL query. **No action required.** The plan should note this in a comment when landing M-8 so future readers understand the scope boundary.

### 4. M-4 `createToUtcCall` [VERIFIED: codebase grep]

Confirmed at exact CONTEXT-cited lines:

```
SqlOptimiser.java:2725:  private ExpressionNode createToUtcCall(ExpressionNode value, ExpressionNode timezone)
SqlOptimiser.java:2726-2734:  builds FUNCTION node with token=ToUTCTimestampFunctionFactory.NAME, paramCount=2, lhs=value, rhs=timezone
```

Existing call sites:
- `SqlOptimiser.java:8308` (GROUP BY rewrite — tsFloor args)
- `SqlOptimiser.java:8462` (FROM unwrap for FROM/TO range)
- `SqlOptimiser.java:8465` (TO unwrap for FROM/TO range)

The `isSubDay` + `sampleByTimezoneName` guards are at **`SqlOptimiser.java:8285` and `8293`** respectively. Line 8307 wraps the GROUP BY tsFloor's FROM arg with `createToUtcCall` when `isSubDay && sampleByTimezoneName != null`. **Line 8331-8332 then passes the raw unwrapped `sampleByFrom`/`sampleByTo` to `setFillFrom`/`setFillTo`** — this is the exact M-4 asymmetry.

**Action for D-08 plan:** the fix location is `SqlOptimiser.java:8331-8332`. Replace with:
```java
nested.setFillFrom(isSubDay && sampleByTimezoneName != null && sampleByFrom != null
        ? createToUtcCall(sampleByFrom, sampleByTimezoneName) : sampleByFrom);
nested.setFillTo(isSubDay && sampleByTimezoneName != null && sampleByTo != null
        ? createToUtcCall(sampleByTo, sampleByTimezoneName) : sampleByTo);
```

(Exact expression at implementer's discretion — may be cleaner to precompute a `needsToUtcWrap` boolean above and use it for both.)

### 5. M-1 `factoryColToUserFillIdx` machinery [VERIFIED: codebase grep]

Confirmed at `SqlCodeGenerator.java:3426-3461`:
- Line 3426 allocates `IntList factoryColToUserFillIdx`
- Lines 3427-3429 pre-populate `-1`
- Lines 3430-3461 walk `bottomUpCols` in user order, resolve alias via `getColumnIndexQuiet`, record mapping from factory index to user fill idx
- Lines 3470 captures `aggNonKeyCount = userFillIdx` (for Defect 3 grammar)

**Bare-FILL(PREV) branch (M-1 target) at lines 3397-3412:**
```java
if (fillValuesExprs.size() == 1
        && isPrevKeyword(fillValuesExprs.getQuick(0).token)
        && fillValuesExprs.getQuick(0).type == ExpressionNode.LITERAL) {
    for (int i = 0; i < columnCount; i++) {                             // <- factory order
        if (i == timestampIndex) { … }
        else if (isKeyColumn(i, bottomUpCols, timestampIndex)) { … }    // <- BUG: i is factory, bottomUpCols is user
        else { FILL_PREV_SELF … }
    }
} else {
    // per-column branch with factoryColToUserFillIdx (CORRECT)
}
```

**The bug is confirmed at line 3405** — `isKeyColumn(i, bottomUpCols, timestampIndex)` receives factory-indexed `i` but reads user-ordered `bottomUpCols`. After `propagateTopDownColumns0` reorder, factory order diverges from user order, and a key column can be misclassified.

**Action for D-05 plan:** rewrite the bare branch to reuse the `factoryColToUserFillIdx` pattern. Walk `bottomUpCols` in user order to build the same mapping (or hoist it above the if/else), then iterate factory columns deciding FILL_KEY / FILL_PREV_SELF via the mapping. The refactor naturally hoists the mapping-build loop above both branches. Implementation detail left to Plan 01.

**Regression test construction per D-03:** CONTEXT's shape `SELECT a + b, k FROM (SELECT k, sum(x) a, sum(y) b FROM t SAMPLE BY 1h FILL(PREV))` walks the outer DFS and triggers `propagateTopDownColumns0`. The planner should verify the shape actually fires `propagateTopDownColumns0` (print `ExplainPlanTest`-style plan-text to observe column order).

### 6. M-7 SortedRecordCursorFactory close path [VERIFIED: codebase read]

**Confirmed constructor ordering:**

```java
// SortedRecordCursorFactory.java:47-75
public SortedRecordCursorFactory(...) {
    super(metadata);
    this.base = base;                    // LINE 56: assigned BEFORE try
    this.sortColumnFilter = sortColumnFilter;
    RecordTreeChain chain = null;
    try {
        chain = new RecordTreeChain(...); // LINE 60: may throw
        this.cursor = new SortedRecordCursor(chain, comparator, ...);
    } catch (Throwable th) {
        Misc.free(chain);                 // frees the partial chain (OK)
        close();                          // LINE 72: double-free path
        throw th;
    }
}
```

**`AbstractRecordCursorFactory.close()` has NO isClosed guard:**

```java
// AbstractRecordCursorFactory.java:48-51
@Override
public final void close() {
    _close();
}
```

`_close()` in `SortedRecordCursorFactory` unconditionally runs `Misc.free(base); Misc.free(cursor);` — the `cursor` is the final field which is still null when construction failed, so `Misc.free(null)` is a no-op; but **`Misc.free(base)` frees the base cursor factory that the caller is still tracking and will try to free again.**

**Fix mechanism recommendation:** per CONTEXT D-10 and Claude's Discretion, **prefer the per-class fix**. Rationale:

- **103 factories extend `AbstractRecordCursorFactory`**. Adding an `isClosed` guard at the base class would silently mask double-close bugs across the entire hierarchy — could hide regressions in factories where the invariant is "close is called exactly once".
- **The surgical fix is minimal.** Remove `final` from `base`, move `this.base = base;` inside the try block, null it in catch:

  ```java
  public SortedRecordCursorFactory(...) {
      super(metadata);
      this.sortColumnFilter = sortColumnFilter;
      RecordTreeChain chain = null;
      try {
          this.base = base;
          chain = new RecordTreeChain(...);
          this.cursor = new SortedRecordCursor(chain, comparator, ...);
      } catch (Throwable th) {
          this.base = null;    // signal to _close that caller still owns base
          Misc.free(chain);
          close();
          throw th;
      }
  }
  ```

  But **note**: `this.cursor` is `private final` — it can NEVER be assigned inside try in a form that compiles when the constructor throws through. Wait: `final` fields CAN be assigned inside a try block as long as the constructor either completes successfully with assignment or terminates via throw. Java requires definite assignment for successful completion, not for `throw` paths. **The compiler accepts assignment inside try, as long as every normal-completion path assigns.**

  However, reading the current code — `this.base = base` is outside try (line 56) and `this.cursor = …` is inside try (line 69). The existing layout is asymmetric for a reason: historical carelessness. **To move `this.base` inside try, we must either:**
  - (a) Remove `final` from `base` (small API surface concern; `base` is `private`, so no external concern)
  - (b) Keep `final`, use separate boolean flag (uglier)
  - (c) Keep `final`, move assignment inside try with the understanding that `_close()` must be null-safe on its field reads

  **Recommended approach:** (c). The field IS null-safe to read because `_close()` calls `Misc.free(base)` which is null-safe (confirmed at `Misc.java:96-105`). Before moving `this.base = base` inside try, it's null. After success, it's the real reference. After catch throws, the object is unreachable and `_close()` won't be called again.

  Actually — the simpler form: keep `final`, move `this.base = base;` INSIDE try. If `new RecordTreeChain(...)` throws before the line runs, `_close()` sees `base=null` and `Misc.free(null)` is a no-op. If it throws AFTER the line runs, `_close()` frees base twice (because caller also frees). That's the case we need to handle.

  **Simplest correct fix:** Remove `final` from `base`, move `this.base = base;` inside try, null it in catch. Diff is ~3 lines.

- **Test for regression (per D-03):** use a test-scoped `CairoConfiguration` override. `sqlSortKeyPageSize = -1` or similar pathological value to trigger `Unsafe.malloc` / `Numbers.ceilPow2` path failure. Alternative: subclass `SortedRecordCursorFactory` with a constructor that calls super with a throwing `RecordSink`. The simplest reliable trigger is probably to mock the base factory such that calling `.getMetadata()` works normally (for `super(metadata)`) but any downstream method throws — but that's a lot of machinery.

  **Recommendation for Plan 04 test:** use a test-scoped `CairoConfiguration` returning `getSqlSortKeyMaxPages() = -1`. Walking `MemoryPages(pageSize, -1)` constructor → `allocate0(0)` → check `0 > -1` → true → `LimitOverflowException`. This fires before the `cursor` field is assigned. The test asserts (a) the construction throws, (b) `Unsafe.getMemUsed()` before == after (no leaked base factory).

**CONFIDENCE:** HIGH. Mechanism and fix path are unambiguous from direct code read.

### 7. Test trigger for M-7 [VERIFIED: codebase grep]

`CairoConfiguration.getSqlSortKeyMaxPages()` exists at `CairoConfiguration.java:683` (returns `int`). Existing test override pattern at `SecurityTest.java:84` sets it to `2`. **For Phase 14, use `-1`:**

```java
@Override
public int getSqlSortKeyMaxPages() {
    return -1;
}
```

This drives `MemoryPages.allocate0(0)` → `if (0 > -1)` → `LimitOverflowException`. The `new MemoryPages(…, -1)` call at `RecordTreeChain.java:71` propagates the exception out of `new RecordTreeChain(...)` at `SortedRecordCursorFactory.java:60`, triggering the exact constructor-throw path M-7 targets.

**Plan guidance:** the test's SQL shape should be a simple SAMPLE BY with ORDER BY so that `SqlCodeGenerator.generateFill` builds a `SortedRecordCursorFactory` wrapping a `groupByFactory`. The test asserts `Unsafe.getMemUsed()` is unchanged across the failed compile (proving no double-free freed anything it shouldn't have). Reading `RecordCursorMemoryUsageTest.java:75-114` for the assertion pattern — same idiom applies.

### 8. M-3 broadcast AST shape [VERIFIED: codebase grep]

`ExpressionNode.LITERAL = 8` (line 50), `ExpressionNode.FUNCTION = 7` (line 49). Bare `PREV` parses as LITERAL; `PREV(colX)` parses as FUNCTION with `paramCount=1, rhs=<LITERAL>`. Confirmed by existing usage at `SqlCodeGenerator.java`:
- Line 3399: `fillValuesExprs.getQuick(0).type == ExpressionNode.LITERAL` — the test for bare PREV.
- Line 3505: `boolean isBarePrev = fillExpr.type == ExpressionNode.LITERAL;`
- Line 3506-3509: `boolean isPrevWithLiteralArg = fillExpr.type == ExpressionNode.FUNCTION && fillExpr.paramCount == 1 && fillExpr.rhs != null && fillExpr.rhs.type == ExpressionNode.LITERAL;`

The distinction is already in use. D-07's fix at line 3570 is a straightforward extension.

**Action for D-07 plan:** at `SqlCodeGenerator.java:3567-3574` inside the "Defect 3 under-spec" check, replace the `isBroadcastable` computation with the `isBareBroadcastable` formulation from D-07 verbatim. Keep the `throw SqlException.$(fillValuesExprs.getQuick(0).position, "not enough fill values")` unchanged.

**Regression test guidance:** query with two aggregates and `FILL(PREV(col_ref))` where `col_ref` is a valid alias. Pre-fix: silently broadcasts `PREV(col_ref)` to both aggregates (each filled from col_ref's prev). Post-fix: rejects with "not enough fill values" at the user-supplied expression's position.

### 9. Decimal zero-vs-null rendering (D-17) [VERIFIED: codebase grep + existing tests]

**`Decimals.*_NULL` constants** use `MIN_VALUE` sentinels, distinct from `0`:

```
core/src/main/java/io/questdb/std/Decimals.java:56:  public static short DECIMAL16_NULL = Short.MIN_VALUE;
core/src/main/java/io/questdb/std/Decimals.java:61:  public static int DECIMAL32_NULL = Integer.MIN_VALUE;
core/src/main/java/io/questdb/std/Decimals.java:62:  public static long DECIMAL64_NULL = Long.MIN_VALUE;
core/src/main/java/io/questdb/std/Decimals.java:63:  public static byte DECIMAL8_NULL = Byte.MIN_VALUE;
```

**Existing test `testFillPrevDecimal128` at `SampleByFillTest.java:1077-1098`** demonstrates the rendering distinction:

```
2024-01-01T00:00:00.000000Z\t12.34         # data value
2024-01-01T01:00:00.000000Z\t12.34         # PREV fill (non-null)
2024-01-01T02:00:00.000000Z\t              # NULL (empty after tab)
2024-01-01T03:00:00.000000Z\t              # NULL fill
2024-01-01T04:00:00.000000Z\t56.78         # data value
```

The test output harness renders NULL as empty-after-tab and `0.00` as literal `"0.00"`. D-17's assertion is feasible and has precedent — the new test simply needs a row with insert `0.00` early, NULL later, then a gap, and assert both render distinctly.

**Action for D-17 plan:** the test SQL should insert `(0.00, ts1)` followed by data gap followed by `(value, ts3)`, then run `FILL(PREV)` with buckets before ts1 (producing NULL fill) and between ts1 and ts3 (producing `0.00` fill). Assert the multiline string has NULL-rendered-as-empty for pre-ts1 rows and `0.00` for between-ts1-ts3 rows. Factory type DECIMAL64 or DECIMAL32 is sufficient.

## Validation Architecture

`.planning/config.json` has no `workflow.nyquist_validation` key — treat as **enabled** per gsd-researcher protocol.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 4 + QuestDB test harness (`AbstractCairoTest`, `AbstractSqlParserTest`, etc.) |
| Config file | none — QuestDB test classes extend harness base classes providing `CairoConfiguration` |
| Quick run command | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevInterval test` |
| Full suite command | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,RecordCursorMemoryUsageTest,ExplainPlanTest' test` |

### Phase Requirements → Test Map

| Finding | Test | Type | Command | File Exists? |
|---------|------|------|---------|-------------|
| M-1 | `testFillPrevOuterProjectionReorder` (new) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevOuterProjectionReorder test` | Will be added in Plan 01 |
| M-2 | `testFillPrevKeyedArray`, `testFillPrevKeyedBinary` (new) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevKeyedArray test` | New — Plan 02 |
| M-3 | `testFillPrevCrossColumnBroadcastRejection` (new) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevCrossColumnBroadcastRejection test` | New — Plan 01 |
| M-4 | `testFillSubDayTimezoneFrom` (A/B/C per CONTEXT specifics) (new) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillSubDayTimezoneFromEmpty test` | New — Plan 03 |
| M-7 | `testSortedRecordCursorFactoryConstructorThrow` (new) | unit | `mvn -pl core -Dtest=...#testSortedRecordCursorFactoryConstructorThrow test` | New — Plan 04, harness location TBD |
| M-8 | `testFillPrevInterval` (D-14 mandatory) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevInterval test` | New — Plan 02 |
| M-9 | `testFillPrevCrossColumnDecimalPrecisionMismatch`, `…GeoHashWidthMismatch`, `…ArrayDimsMismatch` (new) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevCrossColumnDecimalPrecisionMismatch test` | New — Plan 01 |
| Mn-13 | covered transitively by existing `assertMemoryLeak`-wrapped SAMPLE BY FILL tests | unit | passes silently when residual fill values are freed | Existing |
| D-14 | `testFillPrev{Boolean,Byte,Short,Int,Long,Date,Timestamp,IPv4,Binary,Interval}` (10 new) | unit | per-type | New — Plan 02 (INTERVAL) + Plan 01/02 (rest) |
| D-16 | renamed ghost tests with assertions | unit | per-suite | Rename in Plan 01 or dedicated test plan |
| D-17 | `testFillPrevDecimalZeroVsNull` (new) | unit | same | New — Plan 01 or 02 |
| D-18 | restored `testSampleByFill{None,Null,Prev,Value}*Calendar` assertions | unit | `mvn -pl core -Dtest=RecordCursorMemoryUsageTest test` | Modify existing |

### Sampling Rate
- **Per task commit:** `mvn -pl core -Dtest=<single-class> test` for the class affected by that task.
- **Per wave merge:** the combined SAMPLE BY trio: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest' test`.
- **Phase gate:** full relevant suite including `RecordCursorMemoryUsageTest,ExplainPlanTest` — green before `/gsd-verify-work`.

### Wave 0 Gaps

None. The target test classes already exist with 300+ tests across the SAMPLE BY trio. Plan work is purely additive (new test methods) and fix-by-modifying (ghost test rename, RecordCursorMemoryUsageTest assertion restore).

## Standard Stack

This phase makes no new library choices. It operates inside QuestDB's existing, zero-dependency Java core. Key modules touched:

| Package | Role |
|---------|------|
| `io.questdb.cairo` | Column types, configuration, record infrastructure (`ColumnType`, `CairoConfiguration`, `AbstractRecordCursorFactory`) |
| `io.questdb.cairo.sql` | `Record`, `RecordCursor`, `RecordMetadata` interfaces |
| `io.questdb.griffin` | `SqlCodeGenerator`, `SqlOptimiser`, `SqlException` |
| `io.questdb.griffin.engine.groupby` | `SampleByFillRecordCursorFactory` (FillRecord inner class) |
| `io.questdb.griffin.engine.orderby` | `SortedRecordCursorFactory`, `SortedRecordCursor`, `RecordTreeChain`, `MemoryPages` |
| `io.questdb.griffin.model` | `QueryModel` (`setFillFrom`/`setFillTo`), `ExpressionNode` |
| `io.questdb.std` | `Interval.NULL`, `Decimals.*_NULL`, `Misc.free`/`Misc.freeObjList`, `Numbers.LONG_NULL` |

All helpers and constants confirmed present.

## Architecture Patterns

### FillRecord 4-branch dispatch (established, MUST preserve)

Every typed getter on `FillRecord` (inside `SampleByFillRecordCursorFactory.java`) follows this exact 4-branch order, established in Phase 13:

```java
@Override
public <type> get<Type>(int col) {
    if (!isGapFilling) return baseRecord.get<Type>(col);     // 1. non-gap: delegate to base
    int mode = fillMode(col);
    if (mode == FILL_KEY) return keysMapRecord.get<Type>(outputColToKeyPos[col]);
    if (mode >= 0 && outputColToKeyPos[mode] >= 0)           // 2. cross-column PREV → key
        return keysMapRecord.get<Type>(outputColToKeyPos[mode]);
    if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) // 3. PREV (self or cross to aggregate)
        return prevRecord.get<Type>(mode >= 0 ? mode : col);
    if (mode == FILL_CONSTANT) return constantFills.getQuick(col).get<Type>(null);  // 4. constant
    return <null-sentinel>;
}
```

Plans 01 and 02 MUST preserve this exact order. D-06 (M-2) adds branches 1+2+3+4 to `getArray`/`getBin`/`getBinLen` (currently missing 1 and 2). D-11 (M-8) adds an entirely new `getInterval` following the same pattern.

### factoryColToUserFillIdx alias mapping (Phase 13 pattern, reused by D-05)

The per-column branch at `SqlCodeGenerator.java:3426-3461` builds a lookup from factory metadata column index to user-supplied fill-value index by walking `bottomUpCols` in user order. This pattern is correct under `propagateTopDownColumns0` reorder. D-05 extends this machinery to the bare-FILL(PREV) branch at 3397-3412.

### Ownership transfer (fillValues → constantFillFuncs at line 3555)

Transferred slots are nulled in the source so the catch-block's `Misc.freeObjList(fillValues)` does not double-close transferred functions. D-13 extends this to free residual non-null slots on the success path.

### Retro-fallback machinery (Phase 12 → DELETED Phase 13)

`FallbackToLegacyException`, `QueryModel.stashedSampleByNode`, and the three try/catch blocks at `generateFill` call sites were removed in Phase 13. Phase 14 does NOT reintroduce them. All type-safety checks happen at the optimizer gate (Tier 1) and `generateFill`'s grammar-rule passes.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Column-type equality | ad-hoc switches | `ColumnType.tagOf(x) == ColumnType.tagOf(y)` or full int equality | QuestDB encodes precision/scale/geohash-bits/array-dims in the high bits of the type int; tag-only equality is the existing contract and cross-type comparison MUST match the convention |
| Object-array fields | `Function[]` / `ExpressionNode[]` | `ObjList<Function>` / `ObjList<ExpressionNode>` | CLAUDE.md project rule; integrates with `Misc.freeObjList` for resource cleanup |
| Null-safe resource cleanup | `if (x != null) x.close()` | `Misc.free(x)` / `Misc.freeObjList(list)` | Already null-safe, wraps `Closeable.close` with `FatalError` rewrap, returns null so `x = Misc.free(x)` pattern works |
| Interval null sentinel | `new Interval(LONG_NULL, LONG_NULL)` | `Interval.NULL` | Canonical singleton at `Interval.java:33`; using the singleton avoids allocation and keeps null-check short-circuits working |
| Decimal null sentinels | `0`, `Numbers.LONG_NULL` | `Decimals.DECIMAL{8,16,32,64}_NULL` | `MIN_VALUE` sentinels ensure NULL is distinguishable from legitimate 0/zero-valued decimals |
| Creating test-double CairoConfiguration from scratch | new mock | override the base test harness's `new DefaultCairoConfiguration` | Base test harness already supplies a working configuration; override only the method(s) needed via `@Override` block (pattern: `SecurityTest.java:81-86`) |

**Key insight:** Phase 14 does not introduce new mechanisms. All fixes compose from mechanisms already in the codebase. Planner should resist the temptation to refactor "while we're here" — D-01..D-20 are surgical.

## Common Pitfalls

### Pitfall 1: `final` field assignment ordering in constructors
**What goes wrong:** assigning a final field BEFORE the try block, then calling `close()` in catch, frees a caller-owned resource before the exception propagates.
**Why it happens:** Java allows assigning `private final` fields either before or inside a try block as long as definite-assignment rules are satisfied on successful completion; the compiler accepts either placement.
**How to avoid:** for any resource passed IN and freed in `_close()`, assign inside try (after the potentially-throwing allocation completes) OR null the field in catch before calling `close()`. For Phase 14 M-7, choose the former.
**Warning signs:** `_close()` body has `Misc.free(fieldX)` where `fieldX` is a reference passed in; AND caller may reasonably assume ownership is not transferred unless construction succeeds. Exactly the `SortedRecordCursorFactory` situation.

### Pitfall 2: Bulk changes to AbstractRecordCursorFactory.close()
**What goes wrong:** adding an `isClosed` guard at the base class masks every pre-existing double-close bug across 103 factories.
**Why it happens:** the change looks "defensive and obviously correct", but existing code may rely on close being called twice as part of its error recovery (unlikely but unverified).
**How to avoid:** prefer per-class surgical fixes unless a codebase audit shows no factory has non-idempotent close behavior.

### Pitfall 3: Misclassifying factory-index as user-index
**What goes wrong:** `isKeyColumn(i, bottomUpCols, timestampIndex)` called with factory-indexed `i` but bottomUpCols is user-ordered. After `propagateTopDownColumns0`, same column has different index in the two orderings.
**Why it happens:** early in implementation the two orders matched. Subsequent optimizer passes (top-down column propagation) reorder the inner model. The M-1 branch was written before reorder was common.
**How to avoid:** every `isKeyColumn(factoryIdx, …)` call site is suspect. Only the `factoryColToUserFillIdx` lookup correctly handles reorder.

### Pitfall 4: Ghost tests with `printSql` but no assertion
**What goes wrong:** test compiles, runs, passes — but doesn't actually verify anything. Refactors that break the query silently pass this test.
**Why it happens:** the test was originally written when the query was supposed to error out; after the error became an accepted shape, the test was left with `printSql` (which logs but doesn't assert) instead of an assertion.
**How to avoid:** every test must have either `assertSql`, `assertQueryNoLeakCheck`, `assertException`, or an explicit `Assert.*` call. `printSql` alone is a ghost.

### Pitfall 5: Broadcast rules that conflate keyword-type with parse-tree-type
**What goes wrong:** `isPrevKeyword(only.token)` returns true for both bare `PREV` (LITERAL) and `PREV(colX)` (FUNCTION). Using just the keyword check allows `PREV(colX)` to broadcast when only bare PREV should.
**Why it happens:** the keyword check is at the lexical level; the tree-type check is at the parse level. They're orthogonal.
**How to avoid:** when the broadcast vs non-broadcast distinction matters, AND the AST type with the keyword check: `isPrevKeyword(only.token) && only.type == ExpressionNode.LITERAL`.

## Code Examples

### M-7 recommended fix shape

```java
// core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java
public class SortedRecordCursorFactory extends AbstractRecordCursorFactory {
    private RecordCursorFactory base;             // <-- remove `final`
    private final SortedRecordCursor cursor;
    private final ListColumnFilter sortColumnFilter;

    public SortedRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory base,
            @NotNull RecordSink recordSink,
            @NotNull RecordComparator comparator,
            @NotNull ListColumnFilter sortColumnFilter
    ) {
        super(metadata);
        this.sortColumnFilter = sortColumnFilter;
        RecordTreeChain chain = null;
        try {
            this.base = base;                      // <-- moved inside try
            chain = new RecordTreeChain(
                    metadata, recordSink, comparator,
                    configuration.getSqlSortKeyPageSize(),
                    configuration.getSqlSortKeyMaxPages(),
                    configuration.getSqlSortValuePageSize(),
                    configuration.getSqlSortValueMaxPages()
            );
            this.cursor = new SortedRecordCursor(chain, comparator,
                    SortKeyEncoder.createRankMaps(metadata, sortColumnFilter));
        } catch (Throwable th) {
            this.base = null;                      // <-- null before close()
            Misc.free(chain);
            close();
            throw th;
        }
    }
    // _close() unchanged; Misc.free(null) is a no-op
}
```

### M-1 recommended fix shape

```java
// Rewrite the bare-FILL(PREV) branch at SqlCodeGenerator.java:3397-3412 to reuse
// the factoryColToUserFillIdx walk already present at :3426-3461. Hoist the
// walk above the if/else so both branches share it. The bare branch becomes:

if (isBareFillPrev(fillValuesExprs)) {
    // Build factoryColToUserFillIdx once (shared with per-column branch below)
    // and use it here to decide FILL_KEY vs FILL_PREV_SELF per factory column.
    for (int col = 0; col < columnCount; col++) {
        if (col == timestampIndex) {
            fillModes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
            constantFillFuncs.add(NullConstant.NULL);
        } else if (factoryColToUserFillIdx.getQuick(col) < 0) {
            // Key column — no user fill idx was recorded for this factory slot
            fillModes.add(SampleByFillRecordCursorFactory.FILL_KEY);
            constantFillFuncs.add(NullConstant.NULL);
        } else {
            fillModes.add(SampleByFillRecordCursorFactory.FILL_PREV_SELF);
            constantFillFuncs.add(NullConstant.NULL);
        }
    }
}
```

Exact code shape left to Plan 01 — the point is: the classification decision uses `factoryColToUserFillIdx.getQuick(col) < 0` as the "is key column in factory metadata" predicate, which is correct under reorder. No call to `isKeyColumn(col, bottomUpCols, timestampIndex)` with factory-indexed `col`.

### M-9 recommended fix shape (at SqlCodeGenerator.java:3532-3540)

```java
// Replace:
short targetTag = ColumnType.tagOf(groupByMetadata.getColumnType(col));
short sourceTag = ColumnType.tagOf(groupByMetadata.getColumnType(srcColIdx));
if (targetTag != sourceTag) {
    throw SqlException.$(fillExpr.rhs.position, ...);  // error builder
}
// fillModes.add(srcColIdx); ...

// With:
final int targetType = groupByMetadata.getColumnType(col);
final int sourceType = groupByMetadata.getColumnType(srcColIdx);
final short targetTag = ColumnType.tagOf(targetType);
final boolean needsExactTypeMatch =
        ColumnType.isDecimal(targetType)
     || ColumnType.isGeoHash(targetType)
     || targetTag == ColumnType.ARRAY;
final boolean isCompatible = needsExactTypeMatch
        ? targetType == sourceType
        : targetTag == ColumnType.tagOf(sourceType);
if (!isCompatible) {
    throw SqlException.$(fillExpr.rhs.position,
                    "FILL(PREV(").put(srcAlias).put(")): source type ")
            .put(ColumnType.nameOf(sourceType))
            .put(" cannot fill target column of type ")
            .put(ColumnType.nameOf(targetType));
}
fillModes.add(srcColIdx);
```

### M-4 recommended fix shape (at SqlOptimiser.java:8331-8332)

```java
// Replace:
nested.setFillFrom(sampleByFrom);
nested.setFillTo(sampleByTo);

// With (precompute the wrap predicate for clarity):
final boolean wrapFillRangeWithToUtc = isSubDay && sampleByTimezoneName != null;
nested.setFillFrom(wrapFillRangeWithToUtc && sampleByFrom != null
        ? createToUtcCall(sampleByFrom, sampleByTimezoneName) : sampleByFrom);
nested.setFillTo(wrapFillRangeWithToUtc && sampleByTo != null
        ? createToUtcCall(sampleByTo, sampleByTimezoneName) : sampleByTo);
```

### Mn-13 recommended fix shape (before `return new SampleByFillRecordCursorFactory(...)` at line 3672)

```java
// core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java — just before the return:
Misc.freeObjList(fillValues);  // residual non-transferred functions only; transferred slots are null
return new SampleByFillRecordCursorFactory(
        configuration,
        fillMetadata,
        // … rest unchanged …
);
```

## State of the Art

N/A — this is a defensive bugfix phase against an in-flight PR. No library versions, no external APIs.

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `factoryIdx` passed to `isKeyColumn(factoryIdx, bottomUpCols, …)` in bare branch | `factoryColToUserFillIdx` walk in per-column branch | Phase 13 Plan 05 added for per-column; M-1 extends to bare branch | Reorder-safe classification |
| Retro-fallback machinery for unsupported PREV types | rowId-based fast path handles all types | Phase 13 | Phase 14 assumes retro-fallback DELETED — do not reintroduce |
| Per-type snapshot dispatch (KIND_LONG_BITS etc.) | single `prevRowId` per key, read via `recordAt` | Phase 13 | FillRecord getters now uniformly delegate to `prevRecord` |
| `FILL(PREV)` 4-branch dispatch missing from getArray/getBin/getBinLen | **still missing — M-2 target** | Phase 14 | Fix lands in Plan 02 |
| `FillRecord.getInterval` missing | **still missing — M-8 target** | Phase 14 | Fix lands in Plan 02 |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| — | (none — all claims verified via codebase grep or direct file read) | — | — |

**Every factual claim in this research has been verified against the working tree.** No `[ASSUMED]` tags.

## Open Questions

1. **M-7 trigger harness location.** Should the regression test live in `SortedRecordCursorFactoryTest` (doesn't exist today) or inside `SampleByFillTest` (the consumer)? The bug is in `SortedRecordCursorFactory` but the consumer (fill path) is where the memory-leak matters.
   - What we know: no dedicated `SortedRecordCursorFactoryTest` exists; existing negative-test patterns for similar factory-construction edge cases live in consumer test classes.
   - What's unclear: planner's placement preference.
   - Recommendation: place in `SampleByFillTest` so the test documents the intended use-site behavior. Name: `testSortedFactoryConstructorThrowDoesNotDoubleFreeBase`. Scope: create a `CairoConfiguration` override with `getSqlSortKeyMaxPages() = -1`, execute a simple non-keyed FILL(PREV) query, assert the query throws, assert `Unsafe.getMemUsed()` is unchanged, assert no memory leak via `assertMemoryLeak`.

2. **Helper extraction for the 4-branch FillRecord pattern.** CONTEXT allows factoring into a shared helper "without per-row method-call overhead." The natural approach is inlining a single-line helper, which a modern JIT would inline back. However, the 4 branches require different `sink`-typed or value-returning method bodies per type, so only the predicate logic is extractable.
   - What we know: current code has identical branch structures for ~35 getters; the branch decisions are all based on `mode = fillMode(col)` and `outputColToKeyPos[mode]`.
   - What's unclear: whether a `resolveSourceForFill(col, mode)` helper returning an enum/int dispatch would be clearer without adding cost.
   - Recommendation: KEEP the inlined pattern. The readability gain from a helper is marginal, the refactor risk is non-trivial (must preserve Phase 13's dispatch order), and the existing pattern is well-understood across the codebase. If a helper becomes attractive later, it should be a dedicated follow-up, not a Phase 14 side-quest.

3. **`isDecimal` vs `isDecimalType`.** Two similarly named helpers exist at `ColumnType.java:510` and `:515`. Which covers all DECIMAL width tags (DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256)?
   - What we know: both names suggest "is decimal". Without reading the helper bodies carefully, picking wrong leaks a DECIMAL tag through the exact-type check.
   - Recommendation: plan implementer MUST read both method bodies before choosing. If unclear, prefer explicit tag-range check: `ColumnType.tagOf(t) >= ColumnType.DECIMAL8 && ColumnType.tagOf(t) <= ColumnType.DECIMAL256`. A one-line comment in the fix documents the choice for future readers.

## Environment Availability

Not applicable — Phase 14 is a pure code/test change. No new external tools, runtimes, or services. All fixes operate inside QuestDB's existing Java codebase. Test runs require only `mvn` (already required) and the existing `java-questdb-client` local-install artifact (unchanged since Phase 13).

## Sources

### Primary (HIGH confidence)

**Direct file reads (all verified against current working tree on branch `sm_fill_prev_fast_path`):**

- `core/src/main/java/io/questdb/cairo/ColumnType.java` — helpers (`isDecimal`, `isGeoHash`, `nameOf`, `tagOf`) and constants (`ARRAY`, `INTERVAL`, `LONG128`, `UUID`, `GEOHASH`, `DECIMAL8..256`)
- `core/src/main/java/io/questdb/cairo/sql/Record.java` — complete enumeration of `default throw UOE` methods
- `core/src/main/java/io/questdb/cairo/AbstractRecordCursorFactory.java` — `close()` is `final` and unconditionally cascades to `_close()`; no `isClosed` guard
- `core/src/main/java/io/questdb/std/Interval.java` — `Interval.NULL` constant at line 33
- `core/src/main/java/io/questdb/std/Misc.java:96-105` — `Misc.free` is null-safe
- `core/src/main/java/io/questdb/std/MemoryPages.java:45-117` — constructor `MemoryPages(pageSize, maxPages)` always calls `allocate0(0)` which throws `LimitOverflowException` when `0 > maxPages` (i.e., `maxPages <= -1`)
- `core/src/main/java/io/questdb/std/Decimals.java:56-63` — `DECIMAL*_NULL` sentinels use `MIN_VALUE`
- `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java` — constructor ordering, `_close()` body
- `core/src/main/java/io/questdb/griffin/engine/orderby/RecordTreeChain.java:60-78` — constructor calls `new MemoryPages(keyPageSize, keyMaxPages)`
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java:692-1191` — complete `FillRecord` inner class with 4-branch dispatch pattern; missing `getInterval`; missing FILL_KEY/cross-column-PREV-to-key branches in `getArray`/`getBin`/`getBinLen`
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3253-3700` — full `generateFill` method; bare branch at 3397-3412 (M-1), per-column branch at 3426-3561, `factoryColToUserFillIdx` at 3426-3461, under-spec broadcast at 3567-3574 (M-3), M-9 type check at 3532-3540, success path return at 3672-3691, catch-block cleanup at 3692-3698, Mn-13 location at 3671 (insert before return)
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:2725-2734` — `createToUtcCall` definition; `:8285-8340` — `isSubDay` + `sampleByTimezoneName` guards and `setFillFrom`/`setFillTo` (M-4)
- `core/src/main/java/io/questdb/griffin/model/QueryModel.java:746, 761, 1641, 1656` — `getFillFrom`/`getFillTo`/`setFillFrom`/`setFillTo` signatures
- `core/src/main/java/io/questdb/griffin/model/ExpressionNode.java:44-56` — `LITERAL`, `FUNCTION`, `BIND_VARIABLE` type constants
- `core/src/main/java/io/questdb/cairo/CairoConfiguration.java:683-691` — `getSqlSortKeyMaxPages`, `getSqlSortKeyPageSize`, etc.
- `core/src/test/java/io/questdb/test/griffin/SecurityTest.java:81-87` — override pattern for `getSqlSortKeyMaxPages`
- `core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java:75-143` — current tests asserting `SelectedRecordCursorFactory.class` (D-18 targets)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java:925-960, 1077-1122` — existing per-type FILL(PREV) test patterns
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java:6968-6983` — `testSampleByFromToIsDisallowedForKeyedQueries` ghost test
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java:4960-4965` — nano twin ghost test
- GitHub PR 6946 body (`gh pr view 6946 --json body`) — current Trade-offs section has 4 bullets (D-19 appends 2)

### Secondary (MEDIUM confidence)

None — all verification was done via direct tool calls against the working tree.

### Tertiary (LOW confidence)

None.

## Metadata

**Confidence breakdown:**
- Standard stack / architecture: HIGH — all mechanisms verified against current code.
- Fix mechanics (D-05 through D-13): HIGH — every location and pattern verified.
- Test strategy (D-14 through D-18): HIGH — existing per-type test patterns in place to follow.
- M-7 test trigger approach: HIGH — `sqlSortKeyMaxPages = -1` → `LimitOverflowException` flow walked through `MemoryPages.allocate0` source.
- M-5/M-6 benchmark absence: HIGH — `benchmarks/` grep confirms only `SampleByIntervalIteratorBenchmark` exists.
- Helper extraction for FillRecord dispatch (under Claude's Discretion): MEDIUM — inlining is recommended based on readability tradeoff, but the decision is genuinely subjective.

**Research date:** 2026-04-20
**Valid until:** indefinite — the codebase facts are stable; subsequent Phase 14 commits would invalidate individual line numbers but not the mechanisms. Re-verify if another PR merges into the branch before Plan 01 starts.
