# Phase 15: Address PR #6946 review findings and retro-document post-phase-14 fixes — Research

**Researched:** 2026-04-21
**Domain:** QuestDB SAMPLE BY FILL fast path — codebase verification of Phase 15 fix mechanics
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**D-01 — File-clustered, 4 plans.**
- **Plan 01 — Codegen cluster**: C-1, C-2 (unified fix), M-5. All in `SqlCodeGenerator.generateFill`. Includes test restores for `testTimestampFillNullAndValue` (micro + nano) and `testTimestampFillValueUnquoted` (micro + nano) to master's form. Test-in-same-commit-as-fix per file-clustered convention.
- **Plan 02 — Cursor cluster**: C-3, M-4. Both in `SampleByFillRecordCursorFactory` — C-3 in `SampleByFillCursor` (field + 2 CB check sites + regression test), M-4 in `FillRecord.getLong256(int, CharSink)` (terminal `sink.ofRawNull()` + regression test).
- **Plan 03 — Test-only**: M-7 upgrade of `testSampleByFromToParallelSampleByRewriteWithKeys` in `SqlOptimiserTest`. No production change.
- **Plan 04 — Retro-doc**: consolidated paper trail for 3 already-landed post-Phase-14 commits. No code change.

**D-02 — Commit hygiene from Phase 14 D-02 preserved**: CLAUDE.md-compliant titles (no Conventional Commits prefix, under 50 chars, long-form body). Tests and production fix land in the same commit inside each plan's scope (no artificial split).

**D-03 — C-1 + C-2 unified codegen-side fix**: mirror `SampleByFillValueRecordCursorFactory.createPlaceHolderFunction:144-173` directly in `SqlCodeGenerator.generateFill`. For each TIMESTAMP-target fill constant:
  1. Enforce `Chars.isQuoted(fillExpr.token)` — if false, throw `SqlException.position(fillExpr.position).put("Invalid fill value: '").put(fillExpr.token).put("'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'")`.
  2. Parse via `TimestampConstant.newInstance(timestampDriver.parseQuotedLiteral(fillExpr.token), targetType)`.
  3. `NumericException` maps to `SqlException.position(fillExpr.position).put("invalid fill value: ").put(fillExpr.token)`.
  4. Replace `fillValues.getQuick(fillIdx)` with the new TimestampConstant before the existing `constantFillFuncs.add(...)` transfer at `:3626`, freeing the stale function first.

**D-04 — Hook location**: inside the per-column `FILL_CONSTANT` branch at `SqlCodeGenerator.java:3620-3628`, gated on target column type being TIMESTAMP. Non-TIMESTAMP target columns fall through unchanged.

**D-05 — Test restoration in Plan 01**:
- `SampleByTest.testTimestampFillNullAndValue`: restore expected fill-row output from `51062-02-01T08:48:43.456000Z` to master's `2019-02-03T12:23:34.123456Z`.
- `SampleByNanoTimestampTest.testTimestampFillNullAndValue`: verify nano twin passes or restore if drifted.
- `SampleByTest.testTimestampFillValueUnquoted`: restore from `printSql(...)` to `assertException(..., 66, "Invalid fill value: '1236'. Timestamp fill value must be in quotes.")`.
- `SampleByNanoTimestampTest.testTimestampFillValueUnquoted`: mirror nano twin restoration.

**D-06 — C-3**: `SampleByFillCursor` captures CB via `executionContext.getCircuitBreaker()` inside `of()` at `SampleByFillRecordCursorFactory.java:679`. New field `private SqlExecutionCircuitBreaker circuitBreaker;`.

**D-07 — C-3 check sites**:
  1. Head of `hasNext()` at `SampleByFillRecordCursorFactory.java:379` (before any state transition logic).
  2. Inside `emitNextFillRow()` outer `while(true)` at `:525-565` (top of each outer-loop iteration).

**D-08 — C-3 regression test harness**: use `ParallelGroupByFuzzTest.java:4241-4306` tick-counting `MillisecondClock` + `DefaultSqlExecutionCircuitBreakerConfiguration` + `NetworkSqlExecutionCircuitBreaker` pattern. Query shape: keyed `SAMPLE BY 1s FROM '1970' TO '2100' FILL(NULL)` with ≥2 keys. Assert CB-tripped exception within bounded wall-clock.

**D-09 — M-4**: `FillRecord.getLong256(int col, CharSink<?> sink)` at `SampleByFillRecordCursorFactory.java:1059-1080` adds terminal `sink.ofRawNull()` matching `getDecimal128:821` and `getDecimal256:860`. Regression test in `SampleByFillTest` reads sink after FILL_PREV_SELF miss.

**D-10 — M-5**: `generateFill` timestamp-index fallback at `SqlCodeGenerator.java:3334-3339` gates on `ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(origIndex))`. Non-TIMESTAMP fallback falls through to the `timestampIndex < 0` skip-fill path at `:3343-3346`. No new SqlException.

**D-11 — M-7**: `SqlOptimiserTest.testSampleByFromToParallelSampleByRewriteWithKeys:4879-4955` upgrades the four compile-only `select(...).close()` calls at `:4898-4901` to positive-output assertions against bounded (TO-specified) query variants. `assertQueryNoLeakCheck(..., false, false)` per SAMPLE BY FILL factory contract (Phase 14 D-15).

**D-12 — Plan 04 retro-doc**: `15-04-PLAN.md` lightweight placeholder, `15-04-SUMMARY.md` with three sections covering `6c2c44237c`, `2696df1749`, `a986070e43`.

### Claude's Discretion

- Exact placement of the TIMESTAMP-target detection branch inside the `FILL_CONSTANT` loop at `:3620-3628` — before vs. after the existing `isNull`/`isPrev` branches. Guided by least disruption to existing control flow.
- Whether C-3's CB field is captured inside `of()` or `initialize()` — CONTEXT defaults to `of()`.
- Test placement within `SampleByFillTest.java` — alphabetical per CLAUDE.md.
- Whether `15-04-PLAN.md` has an empty task list or a minimal "write SUMMARY" task — planner's call.
- Whether C-3's test lives in `SampleByFillTest` or `ParallelGroupByFuzzTest`/new file — CONTEXT recommends `SampleByFillTest` with a nested fuzz-test-style config override.

### Deferred Ideas (OUT OF SCOPE)

- **M-6 missing test** for `CairoException.critical("sample by fill: data row timestamp ... precedes next bucket")` at `SampleByFillRecordCursorFactory.java:483-488`. Requires bucket-grid-drift repro. Post-merge phase.
- **M-6b multi-worker parallel test** — all existing test plans set `workers: 1`. Requires `CAIRO_SQL_SHARED_WORKER_COUNT > 1` harness changes. Post-merge phase.
- **Dead `isKeyColumn`** at `SqlCodeGenerator.java:1212-1221` + 2 stale comment references. Merge-time housekeeping via `/gsd-pr-branch`.
- **Non-ASCII em-dashes in comments** — ASCII sweep at merge.
- **PR #6946 title narrowness** — retitle at merge time.
- **`.planning/` diff pollution** in PR — filter via `/gsd-pr-branch`.
- **Broader codegen-side coercion for all fill-value types** (INT/LONG/FLOAT/DOUBLE/SHORT/BYTE/IPv4) per `createPlaceHolderFunction`'s full switch. Future phase if analogous bugs surface.
</user_constraints>

## Project Constraints (from CLAUDE.md)

Both `/Users/sminaev/qdbwt/CLAUDE.md` (parent) and `/Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md` (worktree) apply. Non-negotiable directives for every Phase 15 change:

- Java class members sorted alphabetically within kind + visibility. **No `// ===` / `// ---` banner comments** — not in production, not in tests.
- Java 17 features: enhanced switch, multiline strings, `instanceof` pattern variables. Phase 15 does not touch `java-questdb-client` (Java 11).
- `is…` / `has…` prefix for all new boolean names. C-3 field will be `circuitBreaker` (already-named convention for CB references per `NetworkSqlExecutionCircuitBreaker` codebase precedent — boolean-like accessor uses `checkIfTripped()` / `statefulThrowExceptionIfTripped()`, so no `is/has` naming obligation on the object itself).
- **ASCII-only log/error messages.** Verified: C-2 error message `"Invalid fill value: '... '. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'"` contains only ASCII punctuation (`.`, `'`, `:`, `-`, colon-space). No em-dashes, no curly quotes. Safe.
- `SqlException.$(position, msg)` — the position must point at the specific offending character. C-2 uses `fillExpr.position`, which is the token start.
- `ObjList<T>` instead of `T[]`. `fillValues` is already `ObjList<Function>`; `Misc.free(fillValues.getQuick(fillIdx))` before overwrite follows the established ownership-transfer pattern.
- Tests use `assertMemoryLeak` for anything allocating native memory. `assertQueryNoLeakCheck(..., false, false)` for SAMPLE BY FILL (factory contract: `supportsRandomAccess=false`). `assertException(sql, position, expectedErrorFragment)` for error-path tests.
- DDL via `execute()`; UPPERCASE SQL keywords; multi-row `INSERT`; multiline strings for longer SQL; `_` thousands-separator in 5+ digit numbers.
- Commit title ≤ 50 chars, plain English, no Conventional Commits prefix; full long-form body required (≤ 72 char wraps).
- **`java-questdb-client/` is a separate git repo.** Not touched by Phase 15.

**Planner MUST verify**: no banner comments in new tests, alphabetical placement of new test methods and the new `circuitBreaker` field, UPPERCASE SQL in new test queries, ASCII-only error message literals.

## Summary

CONTEXT.md pins 12 locked decisions (D-01..D-12) covering fix mechanics, plan grouping, and the retro-doc format. This research verifies every codebase fact the four plans depend on — **all assumptions confirmed**. The `createPlaceHolderFunction` pattern at `SampleByFillValueRecordCursorFactory.java:144-173` maps cleanly into `SqlCodeGenerator.generateFill` with zero new imports; every required symbol (`TimestampDriver`, `TimestampConstant`, `ColumnType`, `Chars`, `Misc`, `NumericException`) is already imported. The `ParallelGroupByFuzzTest:4241-4306` CB harness transplants into `SampleByFillTest` as a single `@Test` method plus a handful of new imports — no new test class required.

**One nuance** worth surfacing to the planner: the **nano twin of `testTimestampFillNullAndValue` already reads correctly** (lines 14207-14222 show `2019-02-03T12:23:34.123456000Z` fill values — the correct epoch). The 1000x drift is specific to the MICRO target because `functionParser.parseFunction('...Z')` returns a `TIMESTAMP_NANO`-typed constant (per `ColumnType.getTimestampType(STRING) == TIMESTAMP_NANO` at `ColumnType.java:409`); reading that nano value through `FillRecord.getTimestamp` on a MICRO column produces `nanos * 1` stuffed where `micros * 1000` belonged. The codegen-side fix forces driver-specific parsing, so both micro and nano paths produce unit-correct constants — nano stays green, micro gets restored.

**Primary recommendation:** Proceed to planning. No CONTEXT gaps. All line numbers verified; no conflicting commits on the branch ahead of Phase 15 production changes.

<phase_requirements>
## Phase Requirements

No new requirement IDs introduced by Phase 15. The phase closes second-pass `/review-pr 6946` findings against requirements already landed.

| ID | Description | Phase 15 Research Support |
|----|-------------|---------------------------|
| FILL-02 | Non-keyed FILL(PREV) | M-4 (D-09) adds `sink.ofRawNull()` to `getLong256` matching `getDecimal128`/`getDecimal256`; eliminates sink-residue on no-prev fill rows |
| COR-01 | All 302 existing `SampleByTest` tests pass | C-1 / C-2 (D-03, D-05) restore `testTimestampFillNullAndValue` + `testTimestampFillValueUnquoted` expected outputs to master's form; regression pins the unit-correct parse and the unquoted-numeric rejection |
| COR-02 | No native memory leaks | C-1 fix frees the stale `fillValues.getQuick(fillIdx)` before replacing with the driver-parsed `TimestampConstant`; preserves the Phase 14 `Misc.freeObjList(fillValues)` success-path cleanup |
| COR-03 | Query plan shows `Async Group By` (parallel execution) | Unchanged by Phase 15. C-3's CB checks add cancellation responsiveness; M-5 fallback guard prevents a degenerate non-TIMESTAMP column from crashing codegen |
| COR-04 | Fill cursor output matches cursor-path output exactly | C-1 (D-03) restores parity for TIMESTAMP fill constants; M-4 (D-09) restores parity for Long256 sink contract on no-prev fill rows |

**Keyed-fill safety envelope** (new for Phase 15, no requirement ID): C-3 closes the CB-check gap in `SampleByFillCursor.hasNext` + `emitNextFillRow`, bringing the fast-path keyed fill up to parity with the legacy cursor-path circuit-breaker behavior. Regression test pins the contract: keyed `SAMPLE BY 1s FROM '1970' TO '2100' FILL(NULL)` now honors CB cancellation inside bounded wall-clock instead of iterating through billions of buckets.

**M-7 (D-11)** test upgrade strengthens `OPT-01`/`OPT-02` coverage: the four compile-only `select(...).close()` calls in `testSampleByFromToParallelSampleByRewriteWithKeys` get real output verification against bounded (TO-specified) query variants.
</phase_requirements>

## Verification Results

### 1. C-1 / C-2 codegen-side fix — imports and mechanics [VERIFIED: direct file read]

**Imports already present in `SqlCodeGenerator.java`** (verified):

| Symbol | Line | Status |
|--------|------|--------|
| `io.questdb.cairo.ColumnType` | 34 | present |
| `io.questdb.cairo.TimestampDriver` | 51 | present |
| `io.questdb.griffin.engine.functions.constants.TimestampConstant` | 149 | present |
| `io.questdb.std.Chars` | 353 | present |
| `io.questdb.std.Misc` | 361 | present |
| `io.questdb.std.Numbers` | 363 | present |
| `io.questdb.std.NumericException` | 364 | present |
| `io.questdb.griffin.SqlException` | — | in-package (no import needed) |

**No new imports needed.** The fix is in-place.

**`TimestampConstant.newInstance` signature** at `TimestampConstant.java:44`:
```java
public static ConstantFunction newInstance(long value, int timestampType) {
    return value != Numbers.LONG_NULL
            ? new TimestampConstant(value, timestampType)
            : ColumnType.getTimestampDriver(timestampType).getTimestampConstantNull();
}
```
Returns `ConstantFunction` (interface, `Function` supertype). Caller assigns to `Function` slot in `fillValues` with no cast needed.

**`TimestampDriver.parseQuotedLiteral`** at `TimestampDriver.java:567`:
```java
default long parseQuotedLiteral(@NotNull CharSequence quotedTimestampStr) throws NumericException {
    return parseFloor(quotedTimestampStr, 1, quotedTimestampStr.length() - 1);
}
```
Throws `io.questdb.std.NumericException`. Driver-aware via `parseFloor` override on `MicrosTimestampDriver` vs `NanosTimestampDriver`.

**Hook site** (`SqlCodeGenerator.java:3620-3628`): the per-column FILL_CONSTANT branch currently reads:
```java
} else {
    // Reaching this branch implies isBroadcastMode == false, ...
    fillModes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
    constantFillFuncs.add(fillValues.getQuick(fillIdx));
    fillValues.setQuick(fillIdx, null); // transfer ownership
}
```
C-1 / C-2 insertion happens immediately before `constantFillFuncs.add(...)`, gated on `ColumnType.isTimestamp(groupByMetadata.getColumnType(col))`. The `Misc.free(fillValues.getQuick(fillIdx))` before overwrite is required because the `functionParser.parseFunction` call at `:3293` already constructed a (wrong-unit) TimestampConstant that the ownership transfer path would otherwise pass downstream.

### 2. C-2 token-type check — AST shape [VERIFIED: codebase grep + parser read]

Confirmed at `ExpressionNode.java:47-50`:
- `ExpressionNode.CONSTANT = 3` — quoted strings (`'2019-02-03T...'`), numeric literals (`1236`), booleans, NULL/NaN keyword
- `ExpressionNode.FUNCTION = 5` — `now()`, `PREV(col)`, any parenthesized function call
- `ExpressionNode.LITERAL = 6` — unquoted identifiers (column refs, keywords `PREV`, `NULL` bare)

**For fill values** in `FILL(...)`:

| Input | `fillExpr.type` | `fillExpr.token` | `Chars.isQuoted` |
|-------|-----------------|------------------|------------------|
| `'2019-02-03T12:23:34.123456Z'` | CONSTANT (3) | `'2019-02-03T12:23:34.123456Z'` (includes quotes) | **true** |
| `1236` | CONSTANT (3) | `1236` | **false** |
| `now()` | FUNCTION (5) | `now` | **false** |
| `NULL` | LITERAL (6) | `null` | **false** (handled by existing `isNullKeyword` branch at `:3617`) |
| `PREV` | LITERAL (6) | `prev` | **false** (handled by existing `isPrevKeyword` branch at `:3541`) |

Source trace: `ExpressionParser.java:1652-1699` routes quoted-string / numeric tokens through `SqlUtil.nextConstant(...)` at `:1675` which calls `SqlUtil.java:1641 nextConstant(pool, token, pos) -> nextExpr(pool, ExpressionNode.CONSTANT, token, pos)`. The token retains its raw text including quotes for strings, no quotes for numerics.

**Conclusion:** `Chars.isQuoted(fillExpr.token)` on the `fillExpr` from `fillValuesExprs.getQuick(fillIdx)` (NOT on the parsed `fillValues.getQuick(fillIdx)` function) is the correct AST probe. Both quoted strings and unquoted numeric literals are CONSTANT-typed, so type alone cannot distinguish them — `Chars.isQuoted` on the raw token text is the only reliable check. This matches `SampleByFillValueRecordCursorFactory:161` verbatim.

### 3. C-3 — `SampleByFillCursor.of()` signature and CB accessor [VERIFIED: direct file read]

**`SampleByFillCursor.of` at `SampleByFillRecordCursorFactory.java:679-686`:**
```java
private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
    this.baseCursor = baseCursor;
    this.baseRecord = baseCursor.getRecord();
    Function.init(constantFills, baseCursor, executionContext, null);
    fromFunc.init(baseCursor, executionContext);
    toFunc.init(baseCursor, executionContext);
    toTop();
}
```
`executionContext` is in scope. D-06's `this.circuitBreaker = executionContext.getCircuitBreaker();` plugs in at any point within the method body (before `toTop()` is a natural spot, right after `Function.init(...)`).

**`SqlExecutionContext.getCircuitBreaker()` at `SqlExecutionContext.java:111`:**
```java
SqlExecutionCircuitBreaker getCircuitBreaker();
```
Returns `io.questdb.cairo.sql.SqlExecutionCircuitBreaker`. **Never null** — the interface contract guarantees a usable object; context implementations return `NOOP_CIRCUIT_BREAKER` singleton (`SqlExecutionCircuitBreaker.java:34-112`) when no real breaker is attached.

**Signal-on-trip contract** at `NetworkSqlExecutionCircuitBreaker.java:218-234`:
```java
public void statefulThrowExceptionIfTripped() {
    if (testCount < throttle) {
        testCount++;
    } else {
        statefulThrowExceptionIfTrippedNoThrottle();
    }
}

public void statefulThrowExceptionIfTrippedNoThrottle() {
    testCount = 0;
    testTimeout();        // throws CairoException.queryTimedOut / queryCancelled
    testCancelled();      // throws CairoException.queryCancelled
    if (testConnection(fd)) {
        throw CairoException.nonCritical().put("remote disconnected, query aborted [fd=...]")...
    }
}
```
**Signal is THROW, not return value.** Method is `void`. The call site just calls `circuitBreaker.statefulThrowExceptionIfTripped();` and lets the exception propagate.

**NOOP signal**: `SqlExecutionCircuitBreaker.java:102-103`:
```java
public void statefulThrowExceptionIfTripped() {
}
```
Empty body — no-op when no real CB is attached. Safe to call unconditionally without null checks.

**C-3 check-site code shape:**
```java
// head of SampleByFillCursor.hasNext() at :379
public boolean hasNext() {
    circuitBreaker.statefulThrowExceptionIfTripped();
    if (!isInitialized) { ... }
    ...
}

// top of emitNextFillRow() outer while(true) at :525
private boolean emitNextFillRow() {
    while (true) {
        circuitBreaker.statefulThrowExceptionIfTripped();
        while (keysMapCursor.hasNext()) { ... }
        ...
    }
}
```
Zero-GC: both methods are `void` on a cached field reference, no allocation on the fast path (throttled internally by `testCount < throttle`).

### 4. C-3 test harness transplant [VERIFIED: direct file read of ParallelGroupByFuzzTest + SampleByFillTest]

**Harness pattern at `ParallelGroupByFuzzTest.java:4241-4306`** (summary):

```java
circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
    private final AtomicLong ticks = new AtomicLong();
    @Override @NotNull
    public MillisecondClock getClock() {
        return () -> {
            if (ticks.incrementAndGet() < tripWhenTicks) return 0;
            return Long.MAX_VALUE;
        };
    }
    @Override
    public long getQueryTimeout() { return 1; }
};
final WorkerPool pool = new WorkerPool(() -> 4);
TestUtils.execute(pool, (engine, compiler, sqlExecutionContext) -> {
    final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
    final NetworkSqlExecutionCircuitBreaker circuitBreaker =
            new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, MemoryTag.NATIVE_DEFAULT);
    try {
        engine.execute("CREATE TABLE ...", sqlExecutionContext);
        engine.execute("insert into tab ...", sqlExecutionContext);
        context.with(context.getSecurityContext(), context.getBindVariableService(),
                context.getRandom(), context.getRequestFd(), circuitBreaker);
        TestUtils.assertSql(compiler, context, query, sink, "");
        Assert.fail();
    } catch (CairoException ex) {
        TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
    } finally {
        Misc.free(circuitBreaker);
    }
}, configuration, LOG);
```

**Transplants into `SampleByFillTest` as a single `@Test` method.** The test class already `extends AbstractCairoTest` (line 22), which defines `circuitBreakerConfiguration` as a `protected static` field at `AbstractCairoTest.java:154` — the override pattern works across test classes.

**Imports needed in `SampleByFillTest.java` for the C-3 test** (currently missing — the test is the first CB test in the file):

| Import | Source |
|--------|--------|
| `io.questdb.cairo.CairoException` | for catch block |
| `io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker` | CB instance |
| `io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration` | config override base |
| `io.questdb.griffin.SqlExecutionContextImpl` | `context.with(...)` cast |
| `io.questdb.mp.WorkerPool` | pool construction |
| `io.questdb.std.MemoryTag` | CB allocation tag |
| `io.questdb.std.Misc` | `Misc.free(circuitBreaker)` |
| `io.questdb.std.datetime.millitime.MillisecondClock` | `getClock()` return type |
| `io.questdb.test.tools.TestUtils` | `TestUtils.execute(...)` + `TestUtils.assertContains(...)` + `TestUtils.assertSql(...)` |
| `java.util.concurrent.atomic.AtomicLong` | tick counter |
| `org.jetbrains.annotations.NotNull` | `getClock()` return annotation |

`io.questdb.cairo.sql.RecordCursorFactory`, `io.questdb.std.Chars`, `io.questdb.std.Numbers`, `io.questdb.cairo.sql.Record`, `org.junit.Assert`, `org.junit.Test` are already imported per lines 9-18.

**Query shape per D-08**: `SELECT ts, k, count(*) FROM tab SAMPLE BY 1s FROM '1970-01-01' TO '2100-01-01' FILL(NULL)` with ≥2 distinct keys. Calibration: `tripWhenTicks` around 100 (matching the existing `ParallelGroupByFuzzTest` value) should trip inside the fill-emission outer loop before the test times out. Test wall-clock budget via JUnit default ample for the 5s target.

**Minimum viable transplant scope:** single `@Test` method, no new test class, no `@Before` / `@After` needed (CB override state is reset by `AbstractCairoTest.tearDown()`). Test should live in alphabetical order next to existing tests starting with `testFill...` — recommended name `testFillKeyedRespectsCircuitBreaker` or similar.

### 5. M-4 `getLong256` missing `sink.ofRawNull()` [VERIFIED: direct file read]

**Current code at `SampleByFillRecordCursorFactory.java:1058-1080`:**
```java
@Override
public void getLong256(int col, CharSink<?> sink) {
    if (!isGapFilling) {
        baseRecord.getLong256(col, sink);
        return;
    }
    int mode = fillMode(col);
    if (mode == FILL_KEY) {
        keysMapRecord.getLong256(outputColToKeyPos[col], sink);
        return;
    }
    if (mode >= 0 && outputColToKeyPos[mode] >= 0) {
        keysMapRecord.getLong256(outputColToKeyPos[mode], sink);
        return;
    }
    if ((mode == FILL_PREV_SELF || mode >= 0) && hasKeyPrev()) {
        prevRecord.getLong256(mode >= 0 ? mode : col, sink);
        return;
    }
    if (mode == FILL_CONSTANT) {
        constantFills.getQuick(col).getLong256(null, sink);
    }
    // <-- BUG: no sink.ofRawNull() fallthrough
}
```

**Compare to `getDecimal128` at `:817-822` (correct pattern):**
```java
if (mode == FILL_CONSTANT) {
    constantFills.getQuick(col).getDecimal128(null, sink);
    return;
}
sink.ofRawNull();
```

**Compare to `getDecimal256` at `:856-861` (correct pattern):**
```java
if (mode == FILL_CONSTANT) {
    constantFills.getQuick(col).getDecimal256(null, sink);
    return;
}
sink.ofRawNull();
```

**Fix**: add `return;` to the FILL_CONSTANT branch and append terminal `sink.ofRawNull();`. Minimal 2-line change at `:1077-1080`:
```java
if (mode == FILL_CONSTANT) {
    constantFills.getQuick(col).getLong256(null, sink);
    return;
}
sink.ofRawNull();
```

**Verify `CharSink.ofRawNull()` exists**: referenced at `:821` and `:860` on `Decimal128`/`Decimal256` typed sinks. `CharSink<?>` (the generic supertype) also supports `ofRawNull()` — used identically for Long256 sinks by `NullMemoryCMR` per the Phase 14 research confirmation. Plan verifies at implementation time.

**Regression test** (per D-09): in `SampleByFillTest`, create a Long256 column, `FILL(PREV)` query, trigger a fill row BEFORE any data has been seen (`hasKeyPrev() == false`) AND fall-through the FILL_CONSTANT branch (non-keyed, default mode). Sink read should produce null-rendered Long256, not prior-row bytes. Mirror `testFillPrevCrossColumnNoPrevYet` pattern from `2696df1749` (already landed on this branch).

### 6. M-5 timestamp-index fallback guard [VERIFIED: direct file read]

**Current code at `SqlCodeGenerator.java:3330-3346`:**
```java
int timestampIndex = groupByFactory.getMetadata().getColumnIndexQuiet(alias);
// When the same timestamp column appears multiple times (e.g. SELECT k, k),
// the alias may resolve to a renamed duplicate. Fall back to the original
// timestamp token if it yields a valid, earlier column index.
if (!Chars.equalsIgnoreCase(alias, timestamp.token)) {
    int origIndex = groupByFactory.getMetadata().getColumnIndexQuiet(timestamp.token);
    if (origIndex >= 0 && (timestampIndex < 0 || origIndex < timestampIndex)) {
        timestampIndex = origIndex;
    }
}
// The timestamp column may not be present in the output metadata when
// the fill query is wrapped by an outer GROUP BY that projects
// different columns. In that case skip fill entirely.
if (timestampIndex < 0) {
    Misc.freeObjList(fillValues);
    return groupByFactory;
}
int timestampType = groupByFactory.getMetadata().getColumnType(timestampIndex);
TimestampDriver driver = getTimestampDriver(timestampType);
```

**Problem:** `origIndex` can point to a non-TIMESTAMP column — specifically when the outer projection casts or reinterprets the timestamp column with the same alias (e.g. `SELECT ts::LONG AS ts, ...` shadowing a SAMPLE BY's `ts` with a LONG column). The existing safety is `timestampIndex < 0` guard at `:3343`, but the fallback path at `:3336` can promote a non-TIMESTAMP column to `timestampIndex >= 0`, bypassing the skip-fill check.

**Downstream crash**: `ColumnType.getTimestampDriver(timestampType)` at `ColumnType.java:372-385`:
```java
public static TimestampDriver getTimestampDriver(int timestampType) {
    final short tag = tagOf(timestampType);
    if (tag == NULL || tag == UNDEFINED) return MicrosTimestampDriver.INSTANCE;
    assert tag == TIMESTAMP;  // <-- trips when tag is something else
    return switch (timestampType) {
        case TIMESTAMP_MICRO -> MicrosTimestampDriver.INSTANCE;
        case TIMESTAMP_NANO -> NanosTimestampDriver.INSTANCE;
        default -> throw new UnsupportedOperationException();
    };
}
```
The assertion at `:378` trips under `-ea`; without asserts, the `switch` falls to `default -> throw new UnsupportedOperationException()` when `timestampType` is a non-TIMESTAMP non-null/undefined type.

**Concrete SQL shape that triggers** (best-effort — the exact repro may vary by SqlOptimiser rewrite order):
```sql
SELECT ts::LONG AS ts, avg(x) FROM t SAMPLE BY 1h FILL(NULL)
```
Or with a duplicate alias:
```sql
SELECT s AS ts, avg(x) FROM t SAMPLE BY 1h FILL(NULL)
```
Where `s` is a SYMBOL/STRING column. The alias-resolution path at `:3317-3328` sets `alias = "ts"` (from the floor expression), but `getColumnIndexQuiet("ts")` can resolve to a renamed non-TIMESTAMP column after outer projection.

**The M-5 fix per D-10**:
```java
if (!Chars.equalsIgnoreCase(alias, timestamp.token)) {
    int origIndex = groupByFactory.getMetadata().getColumnIndexQuiet(timestamp.token);
    if (origIndex >= 0
            && ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(origIndex))
            && (timestampIndex < 0 || origIndex < timestampIndex)) {
        timestampIndex = origIndex;
    }
}
```
The `ColumnType.isTimestamp(type)` check gates the fallback. If `origIndex` points to a non-TIMESTAMP column, `timestampIndex` stays at whatever `getColumnIndexQuiet(alias)` returned — which might itself be a non-TIMESTAMP column. So the M-5 guard must **also** cover the alias-path result. Re-reading D-10: "if `origIndex` resolves to a non-TIMESTAMP column, treat as 'no timestamp column found' and fall through to the existing `timestampIndex < 0` path at `:3343-3346`". This requires checking BOTH the alias result and the fallback result.

**Planner note**: the M-5 fix may need to be broader than D-10 literally prescribes. Consider:
```java
int timestampIndex = groupByFactory.getMetadata().getColumnIndexQuiet(alias);
if (timestampIndex >= 0
        && !ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(timestampIndex))) {
    timestampIndex = -1;  // reset; non-TIMESTAMP column cannot anchor a fill grid
}
if (!Chars.equalsIgnoreCase(alias, timestamp.token)) {
    int origIndex = groupByFactory.getMetadata().getColumnIndexQuiet(timestamp.token);
    if (origIndex >= 0
            && ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(origIndex))
            && (timestampIndex < 0 || origIndex < timestampIndex)) {
        timestampIndex = origIndex;
    }
}
if (timestampIndex < 0) {
    Misc.freeObjList(fillValues);
    return groupByFactory;
}
```
The first guard (new) protects against the alias pointing to a non-TIMESTAMP column; the second guard (from D-10) protects the fallback; the existing `timestampIndex < 0` check at `:3343` catches both "not found" and "found non-TIMESTAMP reset to -1" cases.

**ROADMAP Success Criterion 5** explicitly says: "`generateFill` timestamp-index fallback (SqlCodeGenerator.java:3334-3339) guards with `ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(origIndex))` before accepting `origIndex`; if the fallback column is non-TIMESTAMP, codegen returns `groupByFactory` (skips fill) instead of cascading into `UnsupportedOperationException`."

This language covers the fallback column specifically. Whether to extend to the alias-path column is Claude's Discretion at planning time — the conservative read is "yes, extend" to prevent the same crash via the other path, but the planner may choose the narrower read if the alias-path collision is not reachable in practice. Recommend the broader fix for defensive margin; regression test should trigger via the narrower fallback path (to match the ROADMAP wording).

### 7. M-7 test upgrade [VERIFIED: direct file read of SqlOptimiserTest + FROM_TO_DDL]

**Current test at `SqlOptimiserTest.java:4879-4955`** — the four compile-only lines at `:4898-4901`:
```java
select(shouldSucceedKeyed).close();
select(shouldSucceedKeyedWithOffset).close();
select(shouldSucceedKeyedExpr).close();
select(shouldSucceedKeyedExprWithOffset).close();
```

Query strings have `from '2017-12-20' fill(null)` — no TO clause. Comment at `:4896-4897` explicitly states: "results are unbounded without TO, so only the compile step runs here."

**`FROM_TO_DDL` at `SampleByTest.java:81-101`** creates `fromto` with:
- `ts TIMESTAMP` via `timestamp_sequence('2018-01-01T00:00:00', 1800000000L)` — 30 min stride
- 480 rows → 10 days of data (2018-01-01 to 2018-01-10 at 23:30)
- columns: `ts, x, s (varchar), b (byte), e (short), i (int), l (long), f (float), d (double), str (string), a (char), k (symbol), t (boolean), n (timestamp)`

The unique key column used in keyed variants is `s` (varchar), producing 480 distinct values (since `s = x::varchar`). Too many to make a bounded TO-specified output useful without further aggregation — query would emit `480 × N_buckets` rows.

**M-7 implementation strategy per D-11**: upgrade the test to use bounded (TO-specified) query variants. The simplest shape: add `to '2018-01-31'` (10 days past the data end) so leading-data buckets fill normally and trailing buckets fill with NULL. Because `s` has 480 values, the test output would balloon. Recommend also trimming the key cardinality — e.g. project `s` into a low-cardinality bucket:

```java
final String shouldSucceedKeyedBounded =
        "select ts, avg(x), s from fromto\n" +
                "where x <= 4\n" +  // cap key cardinality for output-assertable shape
                "sample by 5d from '2017-12-20' to '2018-01-31' fill(null) ";
```

Alternative: use `k` (symbol, also `= x::symbol`) but add a WHERE clause filtering to a small subset. Planner must choose a shape that produces a readable expected-output table.

Nearby test `testSampleByFromToParallelSampleByRewriteWithKeysWithExpr` (implied but not yet verified) or the existing parallel variant at `:4918-4935` uses `shouldSucceedResult` with 5 rows (no keys). Keyed variant with bounded output will be 5 rows × N_keys. If N_keys=5 (via WHERE clause), assertion is 25 rows — tractable.

**Planner must decide**: (a) upgrade to bounded + trimmed-key output assertion, (b) upgrade to bounded + `count(DISTINCT s)` style reduction to avoid materializing full output, (c) keep compile-only but add plan-text assertion on each query. CONTEXT D-11 prefers (a) via `assertQueryNoLeakCheck(..., false, false)`. Option (a) requires careful expected-output construction.

**Match SAMPLE BY FILL factory contract** per Phase 14 D-15: use `assertQueryNoLeakCheck(expectedOutput, query, "ts", false, false)` — `supportsRandomAccess=false`, `expectSize=false`.

### 8. Retro-doc commit inspection [VERIFIED: git show]

All three retro-doc targets have clean metadata:

**`6c2c44237c` — "Cover narrow-decimal FILL_KEY branches"** (2026-04-20):
- +36 lines in `SampleByFillTest.testFillKeyedDecimalNarrow`
- DECIMAL(2,0), DECIMAL(4,0), DECIMAL(9,0), DECIMAL(18,0) key columns → DECIMAL8/16/32/64 storage tag coverage
- No production change; test-only commit

**`2696df1749` — "Fix decimal128/256 sink null fall-through"** (2026-04-21):
- Production: `FillRecord.getDecimal128/256` terminal `sink.ofRawNull()` (16 lines +, 3 -)
- Production: `-ea` assert in `SampleByFillCursor.hasNext()` for defensive `dataTs < nextBucketTimestamp` branch
- Tests: `testFillKeyedPrimitiveTypes` (BOOLEAN/BYTE/CHAR/FLOAT/SHORT FILL_KEY) + `testFillPrevCrossColumnNoPrevYet` (cross-col PREV fall-through to default null); 64 added lines in `SampleByFillTest.java`
- Commit body explains sink-caller contract mirrors `NullMemoryCMR`

**`a986070e43` — "clean-up SampleByFillRecordCursorFactory"** (2026-04-21):
- 20 insertions / 40 deletions in `SampleByFillRecordCursorFactory.java`
- Dead-code and simplification sweep; post-rowId-migration residuals from Phase 13
- No test changes

**Commits on branch post-retro-target**:
```
06244dfb05 docs(state): record phase 15 context session
929384d705 docs(15): capture phase context
a986070e43 clean-up SampleByFillRecordCursorFactory    <-- retro target
2696df1749 Fix decimal128/256 sink null fall-through   <-- retro target
6c2c44237c Cover narrow-decimal FILL_KEY branches      <-- retro target
```

Only `.planning/` doc commits sit between `a986070e43` and current HEAD. **No Phase 15 production changes are already attempted on the branch.** Clean baseline for planning.

### 9. `testTimestampFillNullAndValue` nano twin verification [VERIFIED: direct file read]

**Micro version (`SampleByTest.java:17682-17716`) — DRIFTED:**
```
2021-03-29T00:00:00.000000Z\t\t51062-02-01T08:48:43.456000Z
```
Should be `2019-02-03T12:23:34.123456Z`. Expected output was manually edited to match the buggy (nano-stored, micros-read) fast-path behavior.

**Nano version (`SampleByNanoTimestampTest.java:14203-14234`) — CORRECT:**
```
2021-03-29T00:00:00.000000000Z\t\t2019-02-03T12:23:34.123456000Z
```
Already shows the unit-correct `2019-02-03T12:23:34.123456000Z` value (nanos precision).

**Why the drift is micro-only**: `functionParser.parseFunction('2019-02-03T12:23:34.123456Z', ...)` yields a TimestampConstant typed as `TIMESTAMP_NANO` because `ColumnType.getTimestampType(STRING) == TIMESTAMP_NANO` (per `ColumnType.java:409`). The constant stores the nanosecond value (`2019-02-03 * 1e9 + ... nanos`). On a MICRO-target column, `FillRecord.getTimestamp` reads the constant's `getTimestamp(null)` which returns the raw nano value with no downconversion — and micros-rendering of `nanos_value` produces `nanos * 1 / 1` treated as micros, which equals `micros * 1000`, hence the `2019 → 51062` year drift. On a NANO-target column, no conversion is needed, so the output is correct.

**Planner decision for D-05 nano twin**: verify the nano `testTimestampFillNullAndValue` passes unchanged after the Plan 01 codegen fix (it should, since the driver-aware parse for the nano driver is identity: `NanosTimestampDriver.parseQuotedLiteral('2019-...') → nanos`). No test edit needed unless actual drift is observed — but the text `2019-02-03T12:23:34.123456000Z` is ALREADY in the nano test file, so NO edit is needed. The planner should confirm the test passes as part of Plan 01 verification.

**`testTimestampFillValueUnquoted`**: both micro and nano versions currently use `printSql(...)` with no assertion (ghost test per Phase 14 Pitfall 4). Restore to `assertException(..., 66, "Invalid fill value: '1236'. Timestamp fill value must be in quotes.")`. Position `66` is measured from the query string:
```sql
select ts, first(ts), last(ts) from trade sample by 1d fill(null, 1236) align to CALENDAR;
                                                              ^ position 66 (where '1236' token starts)
```
Verify position math during planning — may be 65/66/67 depending on exact string layout. The precise position should match `fillExpr.position` for the `1236` token, which is where `SqlException.position(fillExpr.position)` will surface.

### 10. Plan 04 retro-doc skeleton [VERIFIED: Phase 13 / Phase 14 SUMMARY patterns]

Phase 13 and Phase 14 retro-doc plans (12-01, 12-04, 13-04) wrote lightweight `XX-YY-PLAN.md` with minimal task lists plus `XX-YY-SUMMARY.md` with one section per commit. Phase 15 Plan 04 follows the same pattern. No code change; the plan artifact exists purely for audit completeness per ROADMAP Phase 15 Success Criterion #7.

**Recommendation for `15-04-PLAN.md`**: single task "T1: write 15-04-SUMMARY.md" so the execution harness has something to consume. Empty task list works but may trigger harness warnings.

**`15-04-SUMMARY.md` skeleton** per CONTEXT D-12:
```markdown
# Phase 15 Plan 04 Summary: Retro-documented commits

## 6c2c44237c - "Cover narrow-decimal FILL_KEY branches"
Date: 2026-04-20
Scope: +36 lines in SampleByFillTest.testFillKeyedDecimalNarrow (DECIMAL(2,0), DECIMAL(4,0), DECIMAL(9,0), DECIMAL(18,0) key columns — DECIMAL8/16/32/64 storage tags). No production change.
Rationale: Pre-existing suite only exercised DECIMAL128/DECIMAL256 keys; the four narrow-decimal getters in FillRecord (getDecimal8/16/32/64) were uncovered.

## 2696df1749 - "Fix decimal128/256 sink null fall-through"
Date: 2026-04-21
Scope:
- FillRecord.getDecimal128 and getDecimal256 terminal `sink.ofRawNull()` (16 lines +, 3 -)
- `-ea` assert in SampleByFillCursor.hasNext() for defensive `dataTs < nextBucketTimestamp` branch
- 64 new test lines: testFillKeyedPrimitiveTypes + testFillPrevCrossColumnNoPrevYet
Rationale: Sink-caller contract mirrors NullMemoryCMR.ofRawNull; without the terminal call, callers reusing a Decimal128/256 sink saw stale prior-row bytes instead of a clean null. The `-ea` assert makes upstream-contract-violation regressions surface in test and dev builds while keeping the production fallback to avoid corrupting query output.

## a986070e43 - "clean-up SampleByFillRecordCursorFactory"
Date: 2026-04-21
Scope: 20 insertions / 40 deletions in SampleByFillRecordCursorFactory.java. Dead-code and simplification sweep.
Rationale: Post-rowId-migration residuals from Phase 13 (KIND_* dispatch, per-type snapshot code paths) no longer referenced after the single-rowId refactor. Cleanup doesn't change behavior; simplifies the file for future review.
```

## Runtime State Inventory

**Not applicable** — Phase 15 is a pure code/test change phase. No rename, no rebrand, no data-model migration, no external-service configuration change. Re-running the mvn test suite after Plan 01-04 lands is the only "state" consideration, and it is covered by the Validation Architecture section below.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|-------------|-----------|---------|----------|
| Java | build + test | probe via `java --version` at plan time | JDK 17+ | — (blocking) |
| Maven 3 | build + test | probe via `mvn --version` | 3.x | — (blocking) |
| `java-questdb-client` local-install | core module build | Maven cache artifact on this worktree | SNAPSHOT | `cd java-questdb-client && mvn clean install -DskipTests && cd -` |
| `gh` CLI | PR #6946 re-run (Success Criterion #8) | probe via `gh --version` | any recent | — (non-blocking; manual `/review-pr` possible) |

**Missing dependencies with no fallback**: none expected on this development workstation per project conventions.

**Missing dependencies with fallback**: stale `java-questdb-client` artifact — rebuild command in table.

## Validation Architecture

Per Phase 15 `additional_context` step 6: the validation budget for Phase 15 is:
- **(a)** After Plan 01: `mvn -Dtest=SampleByTest#testTimestampFillNullAndValue,SampleByTest#testTimestampFillValueUnquoted,SampleByNanoTimestampTest#testTimestampFillNullAndValue,SampleByNanoTimestampTest#testTimestampFillValueUnquoted test` returns green.
- **(b)** After Plan 02: new regression tests for C-3 and M-4 in `SampleByFillTest` pass.
- **(c)** After Plan 03: `SqlOptimiserTest#testSampleByFromToParallelSampleByRewriteWithKeys` passes with its new output assertions.
- **(d)** Phase close: `/review-pr 6946` re-run on the completed branch reports 0 Critical findings.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 4 + QuestDB test harness (`AbstractCairoTest`) |
| Config file | none — QuestDB test classes extend harness base classes |
| Quick run command | `mvn -pl core -Dtest=SampleByFillTest#testFillKeyedRespectsCircuitBreaker test` |
| Plan-01 verification | `mvn -pl core -Dtest='SampleByTest#testTimestampFillNullAndValue,SampleByTest#testTimestampFillValueUnquoted,SampleByNanoTimestampTest#testTimestampFillNullAndValue,SampleByNanoTimestampTest#testTimestampFillValueUnquoted' test` |
| Full suite command | `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,SqlOptimiserTest,RecordCursorMemoryUsageTest,ExplainPlanTest' test` |

### Phase Requirements → Test Map

| Finding | Test | Type | Automated Command | File Exists? |
|---------|------|------|-------------------|--------------|
| C-1 | `SampleByTest#testTimestampFillNullAndValue` (restore expected) | unit | `mvn -pl core -Dtest=SampleByTest#testTimestampFillNullAndValue test` | Yes — modify |
| C-1 nano twin | `SampleByNanoTimestampTest#testTimestampFillNullAndValue` (verify passes) | unit | `mvn -pl core -Dtest=SampleByNanoTimestampTest#testTimestampFillNullAndValue test` | Yes — verify |
| C-2 | `SampleByTest#testTimestampFillValueUnquoted` (restore assertException) | unit | `mvn -pl core -Dtest=SampleByTest#testTimestampFillValueUnquoted test` | Yes — modify |
| C-2 nano twin | `SampleByNanoTimestampTest#testTimestampFillValueUnquoted` (restore assertException) | unit | `mvn -pl core -Dtest=SampleByNanoTimestampTest#testTimestampFillValueUnquoted test` | Yes — modify |
| C-3 | `SampleByFillTest#testFillKeyedRespectsCircuitBreaker` (new) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillKeyedRespectsCircuitBreaker test` | New — Plan 02 |
| M-4 | `SampleByFillTest#testFillPrevLong256NoPrevYet` or similar (new) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevLong256NoPrevYet test` | New — Plan 02 |
| M-5 | `SampleByFillTest#testFillTimestampFallbackNonTimestampColumn` (new; optional per criterion) | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillTimestampFallbackNonTimestampColumn test` | New — Plan 01 |
| M-7 | `SqlOptimiserTest#testSampleByFromToParallelSampleByRewriteWithKeys` (upgrade assertions) | unit | `mvn -pl core -Dtest=SqlOptimiserTest#testSampleByFromToParallelSampleByRewriteWithKeys test` | Yes — modify |

**M-5 regression test**: CONTEXT and ROADMAP do not explicitly require a new test for M-5 — the D-10 fix is defensive and exists to prevent a downstream crash. The planner may add a small guard-regression test if a reliable repro SQL shape is confirmed during implementation; otherwise the fix is validated by existing SAMPLE BY suite not regressing.

### Sampling Rate

- **Per task commit**: `mvn -pl core -Dtest=<single-class> test` for the class touched by the task (typically `SampleByFillTest` or `SampleByTest`).
- **Per wave merge (plan completion)**: the combined SAMPLE BY quartet: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,SqlOptimiserTest' test`.
- **Phase gate**: full relevant suite: `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,SqlOptimiserTest,RecordCursorMemoryUsageTest,ExplainPlanTest' test` — green before `/gsd-verify-work`.
- **Phase close (Success Criterion #8)**: `/review-pr 6946` re-run; expect 0 Critical findings and Moderate list reduced to explicitly-deferred items (M-6 and the minor housekeeping tracked for merge time).

### Wave 0 Gaps

None. Test infrastructure is mature — `SampleByFillTest` and `SampleByTest` hold 300+ tests collectively, all harness and helpers present. Plan work is additive (new test methods) and fix-by-modifying (restore expected outputs, upgrade assertions). The only new imports are additions to `SampleByFillTest.java` for the first CB test in that file (enumerated in section 4 above).

## Standard Stack

Phase 15 makes no new library choices. Touched modules:

| Package | Role in Phase 15 |
|---------|------------------|
| `io.questdb.cairo` | `ColumnType.isTimestamp`, `ColumnType.getTimestampDriver`, `TimestampDriver.parseQuotedLiteral` — all verified present |
| `io.questdb.cairo.sql` | `SqlExecutionCircuitBreaker`, `NetworkSqlExecutionCircuitBreaker` — C-3 CB |
| `io.questdb.griffin` | `SqlCodeGenerator.generateFill`, `SqlException`, `SqlExecutionContext.getCircuitBreaker`, `DefaultSqlExecutionCircuitBreakerConfiguration` |
| `io.questdb.griffin.engine.functions.constants` | `TimestampConstant.newInstance(long, int)` |
| `io.questdb.griffin.engine.groupby` | `SampleByFillRecordCursorFactory` (cursor + FillRecord) |
| `io.questdb.mp` | `WorkerPool` (test harness) |
| `io.questdb.std` | `Chars.isQuoted`, `Misc.free`, `NumericException`, `MemoryTag`, `datetime.millitime.MillisecondClock` |
| `io.questdb.test` / `io.questdb.test.tools.TestUtils` | `assertQueryNoLeakCheck`, `assertException`, `assertSql`, `execute` |

All symbols verified via direct file read against the branch's working tree.

## Architecture Patterns

### Driver-Aware Parse at Codegen Time (established; C-1 / C-2 extends)

The legacy cursor-path `SampleByFillValueRecordCursorFactory.createPlaceHolderFunction:144-173` parses fill constants through the **target column's** `TimestampDriver.parseQuotedLiteral(...)`, producing a unit-correct `TimestampConstant`. This pattern is the canonical reference for C-1 / C-2: Plan 01 imports it verbatim into `SqlCodeGenerator.generateFill`'s per-column FILL_CONSTANT branch. The fast-path gap was that `functionParser.parseFunction` parses driver-agnostically (via `getTimestampType(STRING) == TIMESTAMP_NANO`), so the resulting constant's unit disagrees with the target column's unit whenever the target is MICRO.

### Codegen-Time Type Rebuild via `TimestampConstant.newInstance(long, int)` (multiple precedents)

Used at:
- `SqlCodeGenerator.java:7261-7275` — SAMPLE BY FROM/TO timezone conversion
- `FunctionParser.java:1471, 1195` — generic coercion
- `SampleByFillValueRecordCursorFactory.java:165` — legacy TIMESTAMP FILL

C-1 fix adds one more call site in `generateFill`'s per-column FILL_CONSTANT branch at `:3620-3628`.

### Circuit-Breaker Check at Cursor Boundaries (established; C-3 extends)

Every SQL cursor in QuestDB that can iterate unbounded-N times per call to `hasNext()` or a sub-loop must call `executionContext.getCircuitBreaker().statefulThrowExceptionIfTripped()` at least once per outer-loop iteration. The method is throttled internally — the default `throttle` field bounds the per-call cost — so adding a check at `hasNext()` head + at the top of any unbounded `while(true)` is zero-GC and sub-microsecond per non-trip iteration. C-3 brings `SampleByFillCursor` into parity with the legacy cursor-path cursors and with every GROUP BY / PARALLEL GROUP BY cursor in `io.questdb.griffin.engine.groupby.vect`.

### Sink-Caller Null Fall-Through (`sink.ofRawNull()`) (established; M-4 extends)

`NullMemoryCMR` resets caller-provided sinks via `sink.ofRawNull()` when the slot is null. `FillRecord.getDecimal128` (`:821`) and `getDecimal256` (`:860`) adopt the same contract. M-4 extends to `getLong256` (`:1059-1080`), which was overlooked in the Phase 13/14 sweep and surfaced as a `/review-pr` finding.

### 4-Branch FillRecord Dispatch Order (Phase 13, preserved)

Every typed getter on `FillRecord` follows:
1. `FILL_KEY` — read from `keysMapRecord` at `outputColToKeyPos[col]`
2. Cross-column PREV to key (`mode >= 0 && outputColToKeyPos[mode] >= 0`) — read from `keysMapRecord` at `outputColToKeyPos[mode]`
3. `FILL_PREV_SELF` or cross-col PREV to aggregate, with `hasKeyPrev()` — read from `prevRecord` at `mode` (or `col` for self)
4. `FILL_CONSTANT` — read from `constantFills.getQuick(col)`
5. Terminal null (default sentinel for the type, or `sink.ofRawNull()` for sink-accepting getters)

M-4 closes the terminal-null gap in `getLong256`. Plan 02 preserves all other dispatch branches unchanged.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Parse a quoted timestamp literal to a long | ad-hoc `Dates.parseOffset`/`Chars`-based substring logic | `TimestampDriver.parseQuotedLiteral(quotedStr)` | Driver-aware: MicrosTimestampDriver parses to micros, NanosTimestampDriver parses to nanos. Exactly matches the legacy cursor path |
| Build a `TimestampConstant` | `new TimestampConstant(value, type)` | `TimestampConstant.newInstance(long, int)` | Handles the `value == LONG_NULL` case by returning the driver's singleton null; avoids leaking a non-null TimestampConstant when the parse yields LONG_NULL |
| Null-safe sink reset | `if (sink != null) sink.ofRawNull()` | unconditional `sink.ofRawNull()` | The `CharSink<?>` and `Decimal128`/`Decimal256` sinks are caller-owned non-null by the method-signature contract |
| CB polling loop | manual `Thread.sleep` or `System.currentTimeMillis` checks | `circuitBreaker.statefulThrowExceptionIfTripped()` | Internally throttled; zero allocation on non-trip path; uniform signal (throw CairoException) across cursor types |
| Column-type isTimestamp check | `colType == ColumnType.TIMESTAMP_MICRO \|\| colType == ColumnType.TIMESTAMP_NANO` | `ColumnType.isTimestamp(colType)` | Covers future timestamp precision additions; matches the codebase convention |
| Build a test `CairoConfiguration` from scratch | new subclass | override `circuitBreakerConfiguration` in `AbstractCairoTest` + use `DefaultSqlExecutionCircuitBreakerConfiguration` as base | Existing `AbstractCairoTest` already provides working configuration; override only the methods needed (pattern: `ParallelGroupByFuzzTest:4241-4306`) |

**Key insight**: Phase 15 reuses mechanisms entirely present in the codebase. The planner should resist side-quests: the 4 plans are surgical, sized to land as 4 cohesive commits (tests included per plan).

## Common Pitfalls

### Pitfall 1: Confusing `LITERAL` vs `CONSTANT` in `ExpressionNode.type`

**What goes wrong**: checking `fillExpr.type == ExpressionNode.LITERAL` to distinguish quoted strings from column references — but quoted strings are `CONSTANT` (3), not LITERAL (6). LITERAL is for column names and bare keywords like `PREV`, `NULL`.
**Why it happens**: the names are intuitively swapped — "literal string" feels like it should map to LITERAL, but the parser categorizes by parse-tree role (identifier vs value).
**How to avoid**: for fill values, use `Chars.isQuoted(fillExpr.token)` on the raw token text. Matches the legacy cursor path's existing check at `SampleByFillValueRecordCursorFactory:161`.
**Warning signs**: any `.type == ExpressionNode.LITERAL` check that also needs to handle numeric literals.

### Pitfall 2: Driver-agnostic vs driver-specific timestamp parsing

**What goes wrong**: using `functionParser.parseFunction(quotedTimestampExpr, ...)` parses the string constant driver-agnostically — specifically, `ColumnType.getTimestampType(STRING) == TIMESTAMP_NANO`, so the resulting constant is NANO-typed. Reading that constant back through a MICRO-target column produces a 1000x unit drift.
**Why it happens**: the fast path inherited the `functionParser` from the generic fill-parse loop that doesn't know target column types yet.
**How to avoid**: after the generic parse, re-parse TIMESTAMP-target fill constants through `timestampDriver.parseQuotedLiteral(...)` with the target column's driver, replacing the constant in `fillValues` before ownership transfer.
**Warning signs**: test output differs between micros-target and nanos-target for the same fill string; test expected output shows an implausibly-far-future year like `51062` (= `2019 * 1000`).

### Pitfall 3: Ghost tests using `printSql(...)` without assertion

**What goes wrong**: test compiles, runs, passes — but doesn't actually verify anything. Regressions sneak through.
**Why it happens**: the test was originally written for a pre-assertion placeholder state; the assertion never got added. CONTEXT mentions this happened to `testTimestampFillValueUnquoted` (both micro and nano).
**How to avoid**: every test must have `assertSql`, `assertQueryNoLeakCheck`, `assertException`, or explicit `Assert.*` calls. `printSql` is diagnostic-only and must never be the terminal assertion.
**Warning signs**: test body ends with `printSql(...)` or similar logging calls; no `assert*` or `Assert.*` or `fail(...)` in the body.

### Pitfall 4: Missing circuit-breaker checks in unbounded cursor loops

**What goes wrong**: a cursor that can iterate N-billion times per `hasNext()` call has no CB check, so a cancelled / timed-out query keeps running until it OOMs or hits a wall-clock budget the orchestrator enforces out-of-band.
**Why it happens**: CB checks are project convention, but new cursor paths get written without reading the cursor-contract guidance; the legacy cursor path has them, the fast path didn't.
**How to avoid**: every cursor's `hasNext()` head AND every inner `while(true)` loop top calls `circuitBreaker.statefulThrowExceptionIfTripped()`. Method is throttled and zero-GC on non-trip; no performance concern.
**Warning signs**: a cursor whose worst-case iteration count scales with `FROM`/`TO` range × keys (keyed fill is the canonical example); no CB field on the cursor class.

### Pitfall 5: `final` field double-free on constructor throw

**What goes wrong**: (covered by Phase 14 M-7) — assigning a `private final` field BEFORE the try block, then calling `close()` in catch, frees a caller-owned resource before the exception propagates. Phase 14 landed the fix for `SortedRecordCursorFactory`; Phase 15 does not introduce new constructor-throw patterns.
**How to avoid**: for Phase 15, preserve Phase 14's fix and ensure no new `_close()`-unsafe field assignments appear. C-1 / C-2 do not add new constructor logic — the `TimestampConstant.newInstance(...)` return is stored in an `ObjList<Function>` slot managed via `Misc.free(...)` / `Misc.freeObjList(...)` patterns with clear ownership transfer.

### Pitfall 6: Non-TIMESTAMP column anchoring a timestamp index

**What goes wrong**: `getColumnIndexQuiet(alias)` or its fallback resolves to a non-TIMESTAMP column (e.g., a renamed SYMBOL or casted LONG). Downstream `ColumnType.getTimestampDriver(nonTimestampType)` either trips an assert under `-ea` or throws `UnsupportedOperationException` at runtime.
**Why it happens**: duplicate column names across inner and outer SELECTs after SqlOptimiser rewrites.
**How to avoid**: gate fallback acceptance on `ColumnType.isTimestamp(groupByMetadata.getColumnType(origIndex))`. If no TIMESTAMP column resolvable, skip fill entirely (return `groupByFactory` unchanged).
**Warning signs**: runtime-only failure on queries with cast-or-rename of timestamp column; `getTimestampDriver` appearing in the stack trace of a crash; no unit-test coverage of a "non-TIMESTAMP shadow column" case.

## Code Examples

### C-1 + C-2 recommended fix shape (at `SqlCodeGenerator.java:3620-3628`)

```java
// Replace the existing `else` branch at :3620:
} else {
    // Reaching this branch implies isBroadcastMode == false,
    // because every broadcastable fill value (NULL or bare PREV)
    // is handled by one of the branches above. Therefore
    // fillIdx < fillValues.size() is guaranteed here.
    final int targetColType = groupByMetadata.getColumnType(col);
    if (ColumnType.isTimestamp(targetColType)) {
        if (!Chars.isQuoted(fillExpr.token)) {
            throw SqlException.position(fillExpr.position)
                    .put("Invalid fill value: '").put(fillExpr.token)
                    .put("'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'");
        }
        final TimestampDriver driver = ColumnType.getTimestampDriver(targetColType);
        try {
            final long parsed = driver.parseQuotedLiteral(fillExpr.token);
            // Free the stale (wrong-unit) function parsed earlier; transfer the
            // unit-correct replacement into the fill-values slot so the existing
            // ownership-transfer at line 3626 moves it into constantFillFuncs.
            Misc.free(fillValues.getQuick(fillIdx));
            fillValues.setQuick(fillIdx, TimestampConstant.newInstance(parsed, targetColType));
        } catch (NumericException e) {
            throw SqlException.position(fillExpr.position)
                    .put("invalid fill value: ").put(fillExpr.token);
        }
    }
    fillModes.add(SampleByFillRecordCursorFactory.FILL_CONSTANT);
    constantFillFuncs.add(fillValues.getQuick(fillIdx));
    fillValues.setQuick(fillIdx, null); // transfer ownership
}
```

Note: the unit-correct `TimestampConstant` replaces the stale slot contents BEFORE the `constantFillFuncs.add(fillValues.getQuick(fillIdx))` line, so the transfer path is unchanged.

### M-5 recommended fix shape (at `SqlCodeGenerator.java:3330-3339`)

```java
// Replace the existing resolution block at :3330:
int timestampIndex = groupByFactory.getMetadata().getColumnIndexQuiet(alias);
// The alias may resolve to a non-TIMESTAMP column after outer projection
// (e.g., SELECT ts::LONG AS ts, ...) — reset so the fallback can try the
// raw timestamp token and the skip-fill guard at :3343 fires cleanly when
// no TIMESTAMP column is reachable.
if (timestampIndex >= 0
        && !ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(timestampIndex))) {
    timestampIndex = -1;
}
// When the same timestamp column appears multiple times (e.g. SELECT k, k),
// the alias may resolve to a renamed duplicate. Fall back to the original
// timestamp token if it yields a valid, earlier, TIMESTAMP-typed column index.
if (!Chars.equalsIgnoreCase(alias, timestamp.token)) {
    int origIndex = groupByFactory.getMetadata().getColumnIndexQuiet(timestamp.token);
    if (origIndex >= 0
            && ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(origIndex))
            && (timestampIndex < 0 || origIndex < timestampIndex)) {
        timestampIndex = origIndex;
    }
}
// The timestamp column may not be present in the output metadata when
// the fill query is wrapped by an outer GROUP BY that projects
// different columns. In that case skip fill entirely.
if (timestampIndex < 0) {
    Misc.freeObjList(fillValues);
    return groupByFactory;
}
```

**Note on D-10 literal wording vs. this broader fix**: CONTEXT D-10 and ROADMAP Success Criterion 5 speak of guarding the **fallback** (`origIndex`) column. The broader fix above additionally resets `timestampIndex` when the **alias** resolves to a non-TIMESTAMP column. Planner must decide whether to include the alias-path guard; recommend YES for defensive margin, but the narrower fix alone satisfies the criterion literally. Tests should target the fallback-path shape (narrower repro) regardless.

### C-3 recommended field + check-site shape

Field at top of `SampleByFillCursor` (alphabetical — between `calendarOffset` and `constantFills`):

```java
private final long calendarOffset;
private /* instance */ SqlExecutionCircuitBreaker circuitBreaker;  // captured in of()
private final ObjList<Function> constantFills;
// ...
```

Assignment in `of()` (add one line between `this.baseRecord = ...` and `Function.init(...)` or equivalent clean spot):
```java
private void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
    this.baseCursor = baseCursor;
    this.baseRecord = baseCursor.getRecord();
    this.circuitBreaker = executionContext.getCircuitBreaker();
    Function.init(constantFills, baseCursor, executionContext, null);
    fromFunc.init(baseCursor, executionContext);
    toFunc.init(baseCursor, executionContext);
    toTop();
}
```

Check at head of `hasNext()`:
```java
@Override
public boolean hasNext() {
    circuitBreaker.statefulThrowExceptionIfTripped();
    if (!isInitialized) {
        initialize();
        isInitialized = true;
    }
    // ... rest unchanged
}
```

Check at top of `emitNextFillRow` outer loop:
```java
private boolean emitNextFillRow() {
    while (true) {
        circuitBreaker.statefulThrowExceptionIfTripped();
        // Inner loop: scan remaining keys in current bucket
        while (keysMapCursor.hasNext()) {
            // ... unchanged
        }
        // ... unchanged
    }
}
```

The `statefulThrowExceptionIfTripped` is internally throttled (`testCount < throttle` fast path), so the per-iteration cost on non-trip is a single integer compare + increment. Zero-GC.

### C-3 regression test shape (lives in `SampleByFillTest.java`)

```java
@Test
public void testFillKeyedRespectsCircuitBreaker() throws Exception {
    // Trip the CB inside the fill-emission outer loop. Without C-3, the keyed
    // FROM='1970' TO='2100' FILL(NULL) query iterates unbounded buckets and
    // would OOM or wall-clock-exceed; with C-3 it throws a CB-tripped
    // CairoException within bounded wall-clock.
    final long tripWhenTicks = 100;
    assertMemoryLeak(() -> {
        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            private final AtomicLong ticks = new AtomicLong();

            @Override
            @NotNull
            public MillisecondClock getClock() {
                return () -> {
                    if (ticks.incrementAndGet() < tripWhenTicks) {
                        return 0;
                    }
                    return Long.MAX_VALUE;
                };
            }

            @Override
            public long getQueryTimeout() {
                return 1;
            }
        };

        final WorkerPool pool = new WorkerPool(() -> 4);
        TestUtils.execute(
                pool,
                (engine, compiler, sqlExecutionContext) -> {
                    final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                    final NetworkSqlExecutionCircuitBreaker cb =
                            new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration, MemoryTag.NATIVE_DEFAULT);
                    try {
                        engine.execute(
                                "CREATE TABLE t (" +
                                        " ts TIMESTAMP," +
                                        " k SYMBOL," +
                                        " x DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO t VALUES" +
                                        " ('2024-01-01T00:00:00.000000Z', 'a', 1.0)," +
                                        " ('2024-01-01T00:00:00.000000Z', 'b', 2.0)",
                                sqlExecutionContext
                        );
                        context.with(
                                context.getSecurityContext(),
                                context.getBindVariableService(),
                                context.getRandom(),
                                context.getRequestFd(),
                                cb
                        );
                        TestUtils.assertSql(
                                compiler,
                                context,
                                "SELECT ts, k, first(x) FROM t " +
                                        "SAMPLE BY 1s FROM '1970-01-01' TO '2100-01-01' FILL(NULL)",
                                sink,
                                ""
                        );
                        Assert.fail("expected CB-tripped exception");
                    } catch (CairoException ex) {
                        TestUtils.assertContains(ex.getFlyweightMessage(), "timeout, query aborted");
                    } finally {
                        Misc.free(cb);
                    }
                },
                configuration,
                LOG
        );
    });
}
```

**Alphabetical placement**: in `SampleByFillTest.java` between existing `testFill...` methods — specifically near the `testFillKeyed...` cluster. Verify exact position at implementation time.

### M-4 regression test shape

```java
@Test
public void testFillPrevLong256NoPrevYet() throws Exception {
    // Before any data row fills simplePrevRowId, the non-keyed fill fallthrough
    // must render Long256 as null rather than leave the caller's sink holding
    // prior-row bytes. Mirrors testFillPrevGeoNoPrevYet + testFillPrevCrossColumnNoPrevYet
    // but for the CharSink-taking getLong256(int, CharSink) variant.
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x (val LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO x VALUES" +
                " (CAST('0x01' AS LONG256), '2024-01-01T02:00:00.000000Z')");
        assertQueryNoLeakCheck(
                """
                        first\tts
                        \t2024-01-01T00:00:00.000000Z
                        \t2024-01-01T01:00:00.000000Z
                        0x01\t2024-01-01T02:00:00.000000Z
                        """,
                "SELECT first(val), ts FROM x " +
                        "SAMPLE BY 1h FROM '2024-01-01T00:00:00.000000Z' TO '2024-01-01T03:00:00.000000Z' " +
                        "FILL(PREV) ALIGN TO CALENDAR",
                "ts", false, false
        );
    });
}
```

(Exact Long256 literal syntax and expected rendering depend on QuestDB's LONG256 output format; planner verifies at implementation time.)

### M-7 recommended test upgrade shape

```java
// Replace lines 4898-4901 with bounded query variants + output assertions.
final String shouldSucceedKeyedBounded =
        "select ts, avg(x), s from fromto\n" +
                "where x <= 4\n" +
                "sample by 5d from '2017-12-20' to '2018-01-31' fill(null) ";

final String shouldSucceedKeyedBoundedResult = """
        ts\tavg\ts
        2017-12-20T00:00:00.000000Z\tnull\t1
        2017-12-20T00:00:00.000000Z\tnull\t2
        2017-12-20T00:00:00.000000Z\tnull\t3
        2017-12-20T00:00:00.000000Z\tnull\t4
        2017-12-25T00:00:00.000000Z\tnull\t1
        ...
        """;
assertQueryNoLeakCheck(shouldSucceedKeyedBoundedResult, shouldSucceedKeyedBounded,
        "ts", false, false);
```

**Planner task**: construct the exact expected-output strings for each of the four query variants. Shape is `N_buckets × N_keys` rows. With `WHERE x <= 4` (4 keys) and `FROM '2017-12-20' TO '2018-01-31'` (8-9 buckets at 5-day stride), table is ~32-36 rows. Verify via a brief `printSql` run at implementation time, then freeze as the assertion. Keep the narrative comment at `:4896-4897` explaining why the test originally compiled-only, for history.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `functionParser.parseFunction` for all TIMESTAMP fill constants | driver-aware `TimestampConstant.newInstance(driver.parseQuotedLiteral(token), targetType)` gated on `ColumnType.isTimestamp(targetType)` | Phase 15 C-1 / C-2 (this phase) | Unit-correct fill constants on both MICRO and NANO targets |
| Silent acceptance of unquoted numeric tokens as TIMESTAMP fill | `Chars.isQuoted(token)` enforcement mirroring legacy cursor path | Phase 15 C-2 (this phase) | Master's error message surfaces at the correct position |
| `SampleByFillCursor` with no circuit-breaker check | `statefulThrowExceptionIfTripped()` at `hasNext()` head + `emitNextFillRow` outer-loop top | Phase 15 C-3 (this phase) | Keyed fill with unbounded FROM/TO respects cancellation |
| `FillRecord.getLong256(int, CharSink)` missing terminal `sink.ofRawNull()` | terminal null-reset matching `getDecimal128`/`getDecimal256` | Phase 15 M-4 (this phase) | Sink-caller contract parity across all Decimal + Long256 getters |
| `generateFill` timestamp-index fallback accepts any `origIndex >= 0` | gated on `ColumnType.isTimestamp(targetType)` | Phase 15 M-5 (this phase) | Prevents `UnsupportedOperationException` cascade on non-TIMESTAMP alias collisions |
| `testSampleByFromToParallelSampleByRewriteWithKeys` compile-only | `assertQueryNoLeakCheck(..., false, false)` on bounded TO-specified variants | Phase 15 M-7 (this phase) | Output-level regression pinning for keyed FROM-TO rewrite |

**Deprecated / outdated**: none. This phase adds checks and gates to the post-Phase-14 baseline; nothing is removed.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| — | (none — all claims verified via codebase grep or direct file read) | — | — |

Every factual claim in this research has been verified against the working tree on branch `sm_fill_prev_fast_path`. No `[ASSUMED]` tags. The one subjective recommendation — broadening M-5 to also guard the alias-path result — is clearly flagged as Claude's Discretion at planning time; CONTEXT D-10 and the ROADMAP wording both cover the narrower fallback-only fix, so the planner has the freedom to choose.

## Open Questions

1. **M-5 fix scope: alias-path guard or fallback-only?**
   - What we know: CONTEXT D-10 and ROADMAP Success Criterion 5 both speak of the **fallback** guard only. The alias-path can also resolve to a non-TIMESTAMP column under SqlOptimiser rewrites.
   - What's unclear: whether the alias-path collision is reachable in practice, or whether the existing `Chars.equalsIgnoreCase(alias, timestamp.token)` branch at `:3334` already excludes the relevant shapes.
   - Recommendation: implement the broader fix (reset `timestampIndex` when alias points non-TIMESTAMP, then apply D-10 guard on fallback). Regression test can target the narrower fallback-only shape to match ROADMAP wording while the broader guard provides defensive margin.

2. **M-5 regression test — concrete repro SQL?**
   - What we know: the M-5 guard is defense-in-depth. CONTEXT does not prescribe a test, and no existing test exercises the non-TIMESTAMP fallback path.
   - What's unclear: whether constructing a reliable repro (`SELECT ts::LONG AS ts, avg(x) FROM t SAMPLE BY 1h FILL(NULL)` or similar) is worth a plan-level task.
   - Recommendation: planner's call. If implementing the broader fix, a small repro test pins the contract. If sticking to the narrower fix, fold the check into existing `SampleByFillTest` smoke coverage.

3. **M-7 expected-output assertion vs. plan-text assertion.**
   - What we know: CONTEXT D-11 prescribes `assertQueryNoLeakCheck` with bounded queries. Bounded variant output for 4 keys × 8-9 buckets = ~32-36 rows per variant × 4 variants = 128-144 total output lines.
   - What's unclear: whether the planner should prefer plan-text-only assertion (lighter, narrow intent: "compiles and produces correct plan") or full data assertion.
   - Recommendation: full data assertion per CONTEXT D-11; 128-144 lines is tractable in a multiline string.

4. **C-3 test worker count (`WorkerPool(() -> 4)` vs `() -> 1`)?**
   - What we know: `ParallelGroupByFuzzTest:4261` uses `() -> 4` (4 workers). Keyed fill is single-threaded post-GROUP BY, but the underlying `Async Group By` IS multi-threaded.
   - What's unclear: whether 4-worker config is load-bearing for the C-3 test, or whether `() -> 1` is sufficient (and easier to reason about).
   - Recommendation: use `() -> 4` to match the reference harness; the CB trip is deterministic via the tick-counting clock regardless of worker count. `() -> 1` would also work.

## Sources

### Primary (HIGH confidence)

**Direct file reads — all verified against the current working tree on branch `sm_fill_prev_fast_path`:**

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — imports (`:27-363`), `generateFill` method (`:3253-3730`), FILL_CONSTANT per-column branch (`:3620-3628`), timestampIndex fallback (`:3330-3346`), `factoryColToUserFillIdx` pattern (`:3407-3442`)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — `SampleByFillCursor.of` (`:679-686`), `hasNext` (`:379-490`), `emitNextFillRow` (`:525-565`), `FillRecord.getLong256(int, CharSink<?>)` (`:1059-1080`), `getDecimal128` reference (`:798-822`), `getDecimal256` reference (`:837-861`), `getTimestamp` (`:1187-1198`)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursorFactory.java` — `createPlaceHolderFunction` TIMESTAMP branch (`:160-166`), full switch body (`:144-173`)
- `core/src/main/java/io/questdb/cairo/TimestampDriver.java` — `parseQuotedLiteral` default (`:567-569`)
- `core/src/main/java/io/questdb/cairo/ColumnType.java` — `getTimestampDriver` (`:372-385`), `getTimestampType` (`:406-414`), `isTimestamp` (referenced)
- `core/src/main/java/io/questdb/griffin/engine/functions/constants/TimestampConstant.java` — `newInstance(long, int)` (`:44-46`)
- `core/src/main/java/io/questdb/griffin/SqlExecutionContext.java` — `getCircuitBreaker()` (`:111`)
- `core/src/main/java/io/questdb/cairo/sql/SqlExecutionCircuitBreaker.java` — `statefulThrowExceptionIfTripped` contract (`:177`, NOOP `:102-103`), interface layout (`:31-189`)
- `core/src/main/java/io/questdb/cairo/sql/NetworkSqlExecutionCircuitBreaker.java` — `statefulThrowExceptionIfTripped` body (`:218-224`), signal-on-trip via `testTimeout`/`testCancelled` throwing `CairoException` (`:245-258`)
- `core/src/main/java/io/questdb/griffin/model/ExpressionNode.java` — type constants (`:43-76`), including `CONSTANT=3`, `FUNCTION=5`, `LITERAL=6`
- `core/src/main/java/io/questdb/griffin/ExpressionParser.java` — quoted-string and numeric-literal path (`:1640-1715`)
- `core/src/main/java/io/questdb/griffin/SqlUtil.java` — `nextConstant` (`:1641-1643`)
- `core/src/test/java/io/questdb/test/cairo/fuzz/ParallelGroupByFuzzTest.java` — CB test harness reference (`:4241-4306`), imports (`:26-62`)
- `core/src/test/java/io/questdb/test/AbstractCairoTest.java` — `circuitBreakerConfiguration` field (`:154`), harness fields (`:145-174`)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — test class declaration (`:22`), imports (`:1-18`), `assertQueryNoLeakCheck(..., false, false)` existing pattern (`:63-72`)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — `FROM_TO_DDL` (`:80-101`), `testTimestampFillNullAndValue` (`:17680-17716`, drift confirmed), `testTimestampFillValueUnquoted` (`:17718-17729`, printSql ghost confirmed)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — `testTimestampFillNullAndValue` correct output (`:14203-14234`), `testTimestampFillValueUnquoted` (`:14236-14246`, printSql ghost)
- `core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java` — `testSampleByFromToParallelSampleByRewriteWithKeys` (`:4879-4955`), four compile-only calls (`:4898-4901`)
- git history: `git log master..HEAD`, `git show --stat` for `6c2c44237c`, `2696df1749`, `a986070e43`

### Secondary (MEDIUM confidence)

None — all verification done via direct tool calls against the working tree.

### Tertiary (LOW confidence)

None.

## Metadata

**Confidence breakdown:**
- C-1 / C-2 fix mechanics (D-03, D-04, D-05): **HIGH** — pattern literally copied from legacy path; all imports verified present.
- C-3 CB integration (D-06, D-07, D-08): **HIGH** — test harness verified on a neighboring test class; signal-on-trip contract walked through source.
- M-4 `sink.ofRawNull()` placement (D-09): **HIGH** — direct comparison to `getDecimal128`/`getDecimal256` verified.
- M-5 timestamp-index guard (D-10): **HIGH** for the narrower fix; the broader alias-guard recommendation is rated MEDIUM because the alias-path collision has no explicit test repro on this branch.
- M-7 test upgrade (D-11): **HIGH** for the approach; **MEDIUM** for the exact expected-output table (planner must run at implementation time to freeze the assertion).
- Retro-doc metadata (D-12): **HIGH** — `git show --stat` for all three commits performed.
- ASCII-only error messages (CLAUDE.md): **HIGH** — C-1 / C-2 error strings audited character-by-character; no em-dashes, no curly quotes.

**Research date:** 2026-04-21
**Valid until:** indefinite — the codebase facts are stable on the current branch. Re-verify if another PR merges into `sm_fill_prev_fast_path` before Plan 01 starts.
