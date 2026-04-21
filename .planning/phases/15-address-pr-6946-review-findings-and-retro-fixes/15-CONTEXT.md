# Phase 15 Context: Address PR #6946 review findings and retro-document post-phase-14 fixes

**Gathered:** 2026-04-21
**Status:** Ready for planning
**Source:** Second-pass `/review-pr 6946` run after Phase 14 closed. Findings locked in ROADMAP.md Phase 15 Success Criteria 1-8.

<domain>
## Phase Boundary

Close the three Critical findings from a second `/review-pr 6946` pass that landed after Phase 14, pull in three well-scoped Moderate findings from the same pass, and retroactively bring three already-landed post-Phase-14 commits under phase ownership for audit completeness.

**In scope — code changes:**

- **C-1** (Critical) — TIMESTAMP fill constant 1000x unit conversion drift hidden by altered `testTimestampFillNullAndValue` expected output.
- **C-2** (Critical) — Silent acceptance of unquoted numeric fill values for TIMESTAMP columns hidden by `testTimestampFillValueUnquoted` losing its `assertException`.
- **C-3** (Critical) — Missing `SqlExecutionCircuitBreaker` check in keyed fill emission path (regression vs. legacy cursor-path cursors).
- **M-4** (Moderate) — `FillRecord.getLong256(int, CharSink<?>)` missing terminal `sink.ofRawNull()` fallthrough (asymmetric with `getDecimal128`/`getDecimal256`).
- **M-5** (Moderate) — `generateFill` timestamp-index fallback at SqlCodeGenerator.java:3334-3339 does not verify the resolved column is actually TIMESTAMP.
- **M-7** (Moderate) — `testSampleByFromToParallelSampleByRewriteWithKeys` lost its output assertion when keyed FROM-TO behavior flipped from reject to succeed (four compile-only `select(...).close()` calls with no output verification).

**In scope — audit completeness:**

- Retro-document `6c2c44237c` (narrow-decimal FILL_KEY coverage).
- Retro-document `2696df1749` (decimal128/256 sink null fall-through fix + `-ea` assert promotion + 2 regression tests).
- Retro-document `a986070e43` (SampleByFillRecordCursorFactory clean-up).

**Out of scope (Future Work — per ROADMAP.md "Not in scope"):**

- M-6 missing test for `CairoException.critical` defensive guard — requires crafting a bucket-grid-drift repro (sub-day TZ + FROM/offset misalignment or multi-worker non-determinism).
- M-6b multi-worker parallel test — requires harness changes (`CAIRO_SQL_SHARED_WORKER_COUNT > 1` under a dedicated suite).
- Minor: dead `isKeyColumn` at SqlCodeGenerator.java:1212-1221, non-ASCII em-dashes, PR title narrowness, `.planning/` diff pollution. Housekeeping for merge time via `/gsd-pr-branch`.

</domain>

<decisions>
## Implementation Decisions

### Plan / commit grouping (D-01..D-02)

- **D-01** — File-clustered, 4 plans:
  - **Plan 01 — Codegen cluster**: C-1, C-2 (unified fix), M-5. All in `SqlCodeGenerator.generateFill`. Includes test restores for `testTimestampFillNullAndValue` (micro + nano) and `testTimestampFillValueUnquoted` (micro + nano) to master's form. Test-in-same-commit-as-fix per file-clustered convention.
  - **Plan 02 — Cursor cluster**: C-3, M-4. Both in `SampleByFillRecordCursorFactory` — C-3 in `SampleByFillCursor` (field + 2 CB check sites + regression test), M-4 in `FillRecord.getLong256(int, CharSink)` (terminal `sink.ofRawNull()` + regression test).
  - **Plan 03 — Test-only**: M-7 upgrade of `testSampleByFromToParallelSampleByRewriteWithKeys` in `SqlOptimiserTest`. No production change.
  - **Plan 04 — Retro-doc**: consolidated paper trail for 3 already-landed post-Phase-14 commits. No code change.
- **D-02** — Commit hygiene from Phase 14 D-02 preserved: CLAUDE.md-compliant titles (no Conventional Commits prefix, under 50 chars, long-form body). Tests and production fix land in the same commit inside each plan's scope (no artificial split).

### C-1 + C-2 unified codegen-side fix (D-03..D-05)

- **D-03** — Mirror the legacy cursor path's `SampleByFillValueRecordCursorFactory.createPlaceHolderFunction` pattern (at `:144-173`) directly in `SqlCodeGenerator.generateFill`. For each fill value whose target column type is TIMESTAMP:
  1. Enforce `Chars.isQuoted(fillExpr.token)` — if false, throw `SqlException.position(fillExpr.position).put("Invalid fill value: '").put(fillExpr.token).put("'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'")`. Message matches master and the legacy path verbatim.
  2. Parse via `TimestampConstant.newInstance(timestampDriver.parseQuotedLiteral(fillExpr.token), targetType)` so the stored constant is already in the column's native unit. `NumericException` from parse maps to `SqlException.position(fillExpr.position).put("invalid fill value: ").put(fillExpr.token)` matching the legacy catch.
  3. Replace `fillValues.getQuick(fillIdx)` with the new TimestampConstant before `constantFillFuncs.add(...)` on the success path. Free/transfer ownership consistent with existing `fillValues.setQuick(fillIdx, null)` pattern at line 3627.

- **D-04** — Hook location: inside the per-column FILL_CONSTANT branch at `SqlCodeGenerator.java:3620-3628`, gated on target column type being TIMESTAMP. Non-TIMESTAMP target columns fall through unchanged — no scope expansion to other fill-value types. (Criterion #2 narrow; matches Phase 14 commit discipline.)

- **D-05** — Test restoration lands in Plan 01 alongside the codegen fix:
  - `SampleByTest.testTimestampFillNullAndValue`: restore expected fill-row output from `51062-02-01T08:48:43.456000Z` back to master's `2019-02-03T12:23:34.123456Z`.
  - `SampleByNanoTimestampTest.testTimestampFillNullAndValue`: verify nano twin already reads correctly or restore if drifted.
  - `SampleByTest.testTimestampFillValueUnquoted`: restore from `printSql(...)` form back to master's `assertException(..., 66, "Invalid fill value: '1236'. Timestamp fill value must be in quotes.")`.
  - `SampleByNanoTimestampTest.testTimestampFillValueUnquoted`: mirror nano twin restoration.

### C-3 circuit breaker (D-06..D-08)

- **D-06** — `SampleByFillCursor` captures the `SqlExecutionCircuitBreaker` from the `SqlExecutionContext` passed to `of()` at `SampleByFillRecordCursorFactory.java:679`. New field `private SqlExecutionCircuitBreaker circuitBreaker;` assigned inside `of()` via `executionContext.getCircuitBreaker()`.
- **D-07** — Two CB check sites per criterion #3:
  1. Head of `hasNext()` at `SampleByFillRecordCursorFactory.java:379` (before any state transition logic).
  2. Inside `emitNextFillRow()` outer `while(true)` at `:525-565` (top of each iteration of the outer loop). The inner `while (keysMapCursor.hasNext())` loop at `:528` exits quickly per bucket; outer-loop check gates unbounded bucket-advance.
- **D-08** — C-3 regression test uses `ParallelGroupByFuzzTest`-style harness at `core/src/test/java/io/questdb/test/cairo/fuzz/ParallelGroupByFuzzTest.java:4241-4259`:
  - Override `circuitBreakerConfiguration` with `DefaultSqlExecutionCircuitBreakerConfiguration` + `AtomicLong ticks` in `MillisecondClock` + `getQueryTimeout() = 1`. Trip after a small N ticks chosen to fire inside fill emission loop.
  - Attach `NetworkSqlExecutionCircuitBreaker` to the `SqlExecutionContext`.
  - Query shape: keyed `SAMPLE BY 1s FROM '1970' TO '2100' FILL(NULL)` with >=2 keys (unbounded FROM/TO forces the fill loop to iterate billions of buckets absent CB).
  - Assert the query throws a CB-tripped exception within bounded wall-clock time (e.g., 5s test budget) rather than running to OOM.

### M-4 getLong256 sink null fallthrough (D-09)

- **D-09** — `FillRecord.getLong256(int col, CharSink<?> sink)` at `SampleByFillRecordCursorFactory.java:1059-1080` adds a terminal `sink.ofRawNull()` after all four branches (FILL_KEY, cross-col PREV to key, FILL_PREV_SELF/cross-col PREV aggregate, FILL_CONSTANT). Mirrors `getDecimal128` at `:821` and `getDecimal256` at `:860`. Regression test in `SampleByFillTest` reads the sink after a `FILL_PREV_SELF` miss (no prior data) and asserts it is not left holding prior-row bytes.

### M-5 timestampIndex guard (D-10)

- **D-10** — `generateFill` timestamp-index fallback at `SqlCodeGenerator.java:3334-3339` gates the fallback acceptance on `ColumnType.isTimestamp(groupByFactory.getMetadata().getColumnType(origIndex))`. If `origIndex` resolves to a non-TIMESTAMP column, treat as "no timestamp column found" and fall through to the existing `timestampIndex < 0` path at `:3343-3346` which returns `groupByFactory` (skip fill). No new SqlException — preserves existing "skip fill when wrapped by outer GROUP BY" behavior for the degenerate case and avoids cascading into `UnsupportedOperationException` in the cursor.

### M-7 test upgrade (D-11)

- **D-11** — `SqlOptimiserTest.testSampleByFromToParallelSampleByRewriteWithKeys` at `:4879-4955` upgrades the four compile-only `select(shouldSucceedKeyed...).close()` calls at `:4898-4901` to positive-output assertions against bounded (TO-specified) query variants:
  - Add `to '2018-01-31'` (or equivalent bounded terminator) to each of the four query strings.
  - Replace `select(...).close()` with `assertQueryNoLeakCheck(..., expectedOutput, ..., false, false)` — matches SAMPLE BY FILL factory contract per Phase 14 D-15.
  - Preserve the narrative intent (verifying keyed FROM-TO compiles on both paths) while adding the output verification the current test lacks.

### Retro-doc paper trail (D-12)

- **D-12** — Plan 04 writes `15-04-PLAN.md` (lightweight "retro-document landed commits" plan with no task list for execution) and `15-04-SUMMARY.md` with three sections:
  1. **6c2c44237c — "Cover narrow-decimal FILL_KEY branches"**: scope (36 lines in `SampleByFillTest.testFillKeyedDecimalNarrow` covering DECIMAL8/16/32/64 key columns), rationale (pre-existing suite only exercised DECIMAL128/DECIMAL256 keys), test coverage notes. No production change.
  2. **2696df1749 — "Fix decimal128/256 sink null fall-through"**: scope (`FillRecord.getDecimal128/getDecimal256` terminal `sink.ofRawNull()`, `-ea` assert in `SampleByFillCursor.hasNext()` for `dataTs < nextBucketTimestamp` defensive branch, 2 regression tests `testFillKeyedPrimitiveTypes` + `testFillPrevCrossColumnNoPrevYet`), rationale (sink-caller contract mirrors `NullMemoryCMR.ofRawNull`).
  3. **a986070e43 — "clean-up SampleByFillRecordCursorFactory"**: scope (20 additions / 40 deletions; dead-code and simplification sweep), rationale (post-rowId-migration residuals from Phase 13).

### Claude's Discretion

- Exact placement of the TIMESTAMP-target detection branch inside the per-column FILL_CONSTANT loop at `:3620-3628` — before vs. after the existing isNull/isPrev branches. Guided by least disruption to the existing control flow.
- Whether C-3's CB field is captured inside `of()` or `initialize()` — criterion #3 says `of()`, so default there unless `initialize()` yields cleaner code.
- Test placement within `SampleByFillTest.java` — alphabetical per CLAUDE.md, next to existing per-type / edge-case tests.
- Whether Plan 04's `15-04-PLAN.md` has an empty task list or a minimal "write SUMMARY" task for audit completeness — planner's call; either is acceptable.
- Whether C-3's test lives in `SampleByFillTest` or `ParallelGroupByFuzzTest`/new file — planner picks based on fixture/harness fit (likely `SampleByFillTest` with a nested fuzz-test-style config override).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project conventions
- `CLAUDE.md` — ASCII-only log/error messages, `is/has` boolean naming, zero-GC data path, commit hygiene (no Conventional Commits prefixes, <50 char titles, long-form body), `assertMemoryLeak`/`assertQueryNoLeakCheck` test helpers, members sorted alphabetically.

### Prior phase CONTEXTs (patterns to respect)
- `.planning/phases/14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-/14-CONTEXT.md` — Phase 14 D-01 (file-clustered plans), D-02 (commit hygiene), D-15 (`assertQueryNoLeakCheck` default with `(false, false)` for SAMPLE BY FILL), D-18 (direct factory-type assertion in RCM).
- `.planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-CONTEXT.md` — FillRecord 4-branch dispatch order + rowId-based FILL(PREV) baseline.
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-CONTEXT.md` — retro-fallback removal context + grammar-rule positioning convention.

### Production files touched by Phase 15

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — Plan 01 hook points:
  - `:3293` — `functionParser.parseFunction(expr, ...)` for fill values (unchanged; output consumed downstream).
  - `:3334-3339` — M-5 timestampIndex fallback guard site.
  - `:3343-3346` — existing "skip fill" return pattern to match for M-5.
  - `:3353-3358` — `coerceRuntimeConstantType` precedent for FROM/TO (validator only, NOT a converter).
  - `:3540-3628` — per-column FILL mode dispatch loop; C-1+C-2 hook inside the FILL_CONSTANT branch at `:3620-3628`.
  - `:7261-7275` — `TimestampConstant.newInstance(driver.<converted>, timestampType)` precedent for SAMPLE BY FROM/TO timezone conversion (codegen-time rebuild pattern).

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillValueRecordCursorFactory.java` — **`createPlaceHolderFunction` at `:144-173` is the canonical reference for the C-1+C-2 unified fix**. The TIMESTAMP branch at `:160-166` already implements (a) `Chars.isQuoted` rejection with master's error message and (b) `TimestampConstant.newInstance(timestampDriver.parseQuotedLiteral(...), type)` unit-correct parsing. Fast path copies this pattern into `generateFill`.

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — Plan 02 hook points:
  - `:286-362` — `SampleByFillCursor` class body + constructor; add `private SqlExecutionCircuitBreaker circuitBreaker;` field for C-3.
  - `:379-490` — `hasNext()`; C-3 check at head of method.
  - `:525-565` — `emitNextFillRow()`; C-3 check at top of outer `while(true)`.
  - `:580-585` — `fromTs`/`toTs` caching pattern (`driver.from(fn.getTimestamp(null), ColumnType.getTimestampType(fn.getType()))`). Kept unchanged; cursor-side Option 1 was discussed then rejected in favor of codegen-side unified fix.
  - `:679-686` — `of(RecordCursor, SqlExecutionContext)`; C-3 captures CB from `executionContext.getCircuitBreaker()` here.
  - `:817-821` — `getDecimal128` terminal `sink.ofRawNull()` reference pattern for M-4.
  - `:856-860` — `getDecimal256` terminal `sink.ofRawNull()` reference pattern for M-4.
  - `:1059-1080` — `FillRecord.getLong256(int col, CharSink<?> sink)` — M-4 fix site.
  - `:1187-1198` — `FillRecord.getTimestamp(int col)` — C-1 read site (no change required under codegen-side fix; stored constant is already unit-correct).

- `core/src/main/java/io/questdb/cairo/TimestampDriver.java:567` — `parseQuotedLiteral(CharSequence)` contract. Throws `NumericException` on malformed input; caller wraps into `SqlException`.

- `core/src/main/java/io/questdb/griffin/FunctionParser.java:1471, 1195` — additional `TimestampConstant.newInstance(..., type)` precedents for codegen-time type coercion.

### Test files touched by Phase 15

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — Plan 01:
  - `:17682-17716` — `testTimestampFillNullAndValue`; restore expected output to master's `2019-02-03T12:23:34.123456Z` on fill rows.
  - `:17718-17729` — `testTimestampFillValueUnquoted`; restore from `printSql(...)` form to `assertException(..., 66, "Invalid fill value: '1236'. Timestamp fill value must be in quotes.")`.

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — Plan 01:
  - `:14203-14234` — nano twin of `testTimestampFillNullAndValue`; verify passes unchanged (nano-target parsing already correct per `parseQuotedLiteral(ns driver)`), adjust if it drifted.
  - `:14236-14246` — nano twin of `testTimestampFillValueUnquoted`; same restoration as micro twin.

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — Plan 02:
  - Add C-3 regression test (keyed SAMPLE BY 1s FROM 1970 TO 2100 FILL(NULL), tick-counting CB harness, tripWhenTicks small).
  - Add M-4 regression test for `getLong256` sink-null fallthrough (mirror the decimal128/256 sink-null test style).

- `core/src/test/java/io/questdb/test/griffin/SqlOptimiserTest.java` — Plan 03:
  - `:4879-4955` — `testSampleByFromToParallelSampleByRewriteWithKeys`; upgrade four compile-only `select(...).close()` calls at `:4898-4901` to bounded TO-variant assertions with `assertQueryNoLeakCheck`.

- `core/src/test/java/io/questdb/test/cairo/fuzz/ParallelGroupByFuzzTest.java:4241-4300` — C-3 test harness reference pattern (tick-counting `MillisecondClock` + `DefaultSqlExecutionCircuitBreakerConfiguration` override + `NetworkSqlExecutionCircuitBreaker` attach).

### Retro-doc commit references (Plan 04, no code touch)

- `6c2c44237c` — Cover narrow-decimal FILL_KEY branches (`SampleByFillTest.testFillKeyedDecimalNarrow`, 36 lines).
- `2696df1749` — Fix decimal128/256 sink null fall-through (FillRecord.getDecimal128/getDecimal256 + `-ea` assert + 2 tests).
- `a986070e43` — clean-up SampleByFillRecordCursorFactory (20 insertions / 40 deletions).

### PR reference

- PR #6946: https://github.com/questdb/questdb/pull/6946 — PR body already has Trade-offs section from Phase 14 D-20. Phase 15 success criterion #8 calls for a `/review-pr 6946` re-run after close to verify 0 Critical findings remain.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `SampleByFillValueRecordCursorFactory.createPlaceHolderFunction` at `:144-173` — the canonical TIMESTAMP fill-value parsing + rejection pattern. Fast path adopts the TIMESTAMP branch verbatim for C-1+C-2 closure.
- `TimestampConstant.newInstance(long, int)` — stateless rebuild of a TimestampConstant with a specific type. Used at `SqlCodeGenerator.java:7261/7271` for SAMPLE BY FROM/TO and `FunctionParser.java:1471/1195` for generic coercion.
- `TimestampDriver.parseQuotedLiteral(CharSequence)` at `:567` — throws `NumericException` on malformed, caller wraps to `SqlException`. Driver-aware, so micros vs. nanos parsing is handled automatically by the target driver.
- `DefaultSqlExecutionCircuitBreakerConfiguration` + `NetworkSqlExecutionCircuitBreaker` + tick-counting `MillisecondClock` — production CB wiring, proven in-repo at `ParallelGroupByFuzzTest.java:4241-4300`.
- `Chars.isQuoted(CharSequence)` — boolean check for `'...'` wrapping.
- `sink.ofRawNull()` — CharSink null-reset contract; mirrors `NullMemoryCMR`'s behavior. Existing use at `FillRecord.getDecimal128` `:821` and `getDecimal256` `:860`.

### Established Patterns

- **FillRecord dispatch order** (Phase 13, preserved): `FILL_KEY` -> cross-column-PREV-to-key (`mode >= 0 && outputColToKeyPos[mode] >= 0`) -> `FILL_PREV_SELF` or cross-col-PREV-to-aggregate -> `FILL_CONSTANT` -> default null. M-4 adds the terminal `sink.ofRawNull()` without reordering.
- **Codegen-time type rebuild** (multiple precedents): `TimestampConstant.newInstance(driver.<convert>, targetType)` replaces parsed Function with a unit-correct constant. Fast path FILL(CONSTANT) for TIMESTAMP was the gap.
- **Cursor's `of(baseCursor, executionContext)`** is the canonical hook for pulling CB, bind vars, etc., from the execution context into cursor state. Used throughout fill cursor (`Function.init(constantFills, baseCursor, executionContext, null)` at `:682`).
- **Test-only config overrides**: `circuitBreakerConfiguration` static field on `AbstractCairoTest` is overridable per-test; tick-counting `MillisecondClock` + low `getQueryTimeout()` is the deterministic trip mechanism.

### Integration Points

- `SqlCodeGenerator.generateFill` at `:3244-3700` — hook for C-1+C-2 (per-column FILL_CONSTANT branch) and M-5 (timestamp-index fallback guard).
- `SampleByFillCursor.of()` / `.hasNext()` / `.emitNextFillRow()` — hooks for C-3 (field + 2 check sites).
- `FillRecord.getLong256(int, CharSink)` — single-line fix site for M-4.
- `SqlOptimiserTest.testSampleByFromToParallelSampleByRewriteWithKeys` — single test method upgrade for M-7.

</code_context>

<specifics>
## Specific Ideas

### C-1 concrete fix sketch

Inside the per-column FILL_CONSTANT branch at `SqlCodeGenerator.java:3620-3628`, before `constantFillFuncs.add(fillValues.getQuick(fillIdx))`:

```java
final int targetColType = groupByFactory.getMetadata().getColumnType(col);
if (ColumnType.isTimestamp(targetColType)) {
    if (!Chars.isQuoted(fillExpr.token)) {
        throw SqlException.position(fillExpr.position)
                .put("Invalid fill value: '").put(fillExpr.token)
                .put("'. Timestamp fill value must be in quotes. Example: '2019-01-01T00:00:00.000Z'");
    }
    final TimestampDriver driver = ColumnType.getTimestampDriver(targetColType);
    try {
        final long parsed = driver.parseQuotedLiteral(fillExpr.token);
        // Free the stale function parsed by functionParser (wrong unit) before replacing.
        Misc.free(fillValues.getQuick(fillIdx));
        fillValues.setQuick(fillIdx, TimestampConstant.newInstance(parsed, targetColType));
    } catch (NumericException e) {
        throw SqlException.position(fillExpr.position)
                .put("invalid fill value: ").put(fillExpr.token);
    }
}
```

Then the existing `constantFillFuncs.add(fillValues.getQuick(fillIdx)); fillValues.setQuick(fillIdx, null);` at `:3626-3627` transfers the new TimestampConstant with correct unit.

### C-3 regression test sketch

```java
@Test
public void testKeyedFillRespectsCircuitBreaker() throws Exception {
    final AtomicLong ticks = new AtomicLong();
    final long tripWhenTicks = 100; // calibrate so we trip inside fill emission
    // ... DefaultSqlExecutionCircuitBreakerConfiguration override with tick-counting clock,
    //     NetworkSqlExecutionCircuitBreaker attached to context, query:
    //     "SELECT ts, k, first(x) FROM t SAMPLE BY 1s FROM '1970-01-01' TO '2100-01-01' FILL(NULL)"
    //     with >= 2 distinct keys in table.
    // Expect CairoException (or specific CB-tripped exception) within 5s wall clock.
}
```

Lives in `SampleByFillTest`. Alphabetical placement; use existing test class's `@Before` / `@After` to reset the CB config override.

### M-5 fallback guard sketch

At `SqlCodeGenerator.java:3334-3339`:

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

Non-TIMESTAMP fallback column falls through to the existing `timestampIndex < 0` skip-fill path at `:3343-3346`.

### M-4 concrete fix

At `SampleByFillRecordCursorFactory.java:1059-1080`, replace the current method body's implicit `return;` fallthrough with an explicit terminal `sink.ofRawNull();` after the last branch — mirrors `getDecimal128` at `:821` and `getDecimal256` at `:860`. Regression test: call `getLong256` on a FILL_PREV_SELF column where `hasKeyPrev() == false` and assert the sink renders null (not prior row's Long256 bytes).

### M-7 test upgrade

At `SqlOptimiserTest.java:4898-4901`, each `select(shouldSucceedKeyed...).close()` becomes `assertQueryNoLeakCheck(bounded, expectedOutput, ..., false, false)`. Add TO clause to each of the four query strings (e.g., `to '2018-01-31'`). Expected output mirrors the existing `shouldSucceedResult` multi-row table but with keyed `s` column included. Plan text assertion unchanged.

### Retro-doc paper trail skeleton

`15-04-PLAN.md`:
```
# Phase 15 Plan 04: Retro-document landed commits

Paper trail only. No code changes. Three post-Phase-14 commits landed before
phase boundary was drawn; this plan brings them under Phase 15 ownership for
audit completeness (ROADMAP.md Success Criterion #7).

Tasks:
- T1: write 15-04-SUMMARY.md with three sections (one per commit hash).
```

`15-04-SUMMARY.md`:
```
# Phase 15 Plan 04 Summary: Retro-documented commits

## 6c2c44237c - "Cover narrow-decimal FILL_KEY branches"
Date: 2026-04-20
Scope: +36 lines in SampleByFillTest.testFillKeyedDecimalNarrow...
Rationale: ...

## 2696df1749 - "Fix decimal128/256 sink null fall-through"
Date: 2026-04-21
Scope: FillRecord.getDecimal128/256 terminal sink.ofRawNull, -ea assert, 2 tests...
Rationale: ...

## a986070e43 - "clean-up SampleByFillRecordCursorFactory"
Date: 2026-04-21
Scope: 20 insertions / 40 deletions dead-code sweep...
Rationale: post-rowId-migration residuals from Phase 13.
```

</specifics>

<deferred>
## Deferred Ideas

- **M-6 missing test** for `CairoException.critical("sample by fill: data row timestamp ... precedes next bucket")` defense-in-depth guard at `SampleByFillRecordCursorFactory.java:483-488`. Requires a bucket-grid-drift repro (sub-day TZ + FROM/offset misalignment, or multi-worker non-determinism). Larger than a single regression test; post-merge phase if triaged.
- **M-6b multi-worker parallel test** — all existing test plans set `workers: 1`. Requires harness changes to set `CAIRO_SQL_SHARED_WORKER_COUNT > 1` under a dedicated suite. Post-merge phase.
- **Dead `isKeyColumn` method** at `SqlCodeGenerator.java:1212-1221` plus 2 stale comment references. Housekeeping; fold in at merge time via `/gsd-pr-branch`.
- **Non-ASCII em-dashes in comments** — ASCII sweep pending at merge.
- **PR #6946 title narrowness** — titled around FILL(PREV) fast path; current scope covers all fill modes. Retitle at merge time.
- **`.planning/` directory pollution** in PR diff — planning artifacts shown in the PR review; filter via `/gsd-pr-branch` when preparing for merge.
- **Broader scope: codegen-side coercion for all fill-value types** (not just TIMESTAMP) — would generalize D-03 to INT/LONG/FLOAT/DOUBLE/SHORT/BYTE/IPv4 per `createPlaceHolderFunction`'s full switch. Rejected from Phase 15 scope (user: "Match legacy broadly" for TIMESTAMP only). Future phase if analogous bugs surface for other fill types.

</deferred>

---

*Phase: 15-address-pr-6946-review-findings-and-retro-fixes*
*Context gathered: 2026-04-21*
