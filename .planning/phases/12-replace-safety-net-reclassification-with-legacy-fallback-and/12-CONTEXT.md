# Phase 12 Context: Replace safety-net reclassification with legacy fallback and tighten optimizer PREV gate

**Gathered:** 2026-04-16
**Status:** Ready for planning
**Supersedes:** prior CONTEXT.md draft. The belt-and-braces framing is retired in favor of a single retro-fallback mechanism backed by a deliberately minimal optimizer gate — see "Design intent" and "Mechanism: retro-fallback" below.

## Domain

Optimizer gate + codegen fill-path re-dispatch for SAMPLE BY FILL(PREV) queries whose output types exceed what the fast-path snapshot buffer can hold, plus the grammar of `FILL(PREV, PREV(...))`. No runtime fill-cursor changes. No changes to the legacy cursor path itself — we change only *when* queries reach it.

## Driving finding

`/review-pr` on PR #6946 tip (`67bf9f2eea`) surfaced a latent correctness bug in the codegen safety-net at `SqlCodeGenerator.java:3497-3512`. The safety-net's intended role was to recover from one narrow case — a key column that `isKeyColumn()` failed to identify in a CTE/subquery context (phase 11 retained it with a comment to that effect). In practice the same block fires on a much broader class of queries and silently rewrites genuine aggregates as hash keys, producing duplicated fill rows.

### Evidence that the bug is reachable and already in-tree

- `testSampleByFillNeedFix` (SampleByTest.java, ~line 5943) was updated during this PR to accept 6 rows for a query that master produces 3 rows for. All aggregate output types in the query are DOUBLE/LONG — both supported by the fast path — yet the output is doubled. The test comment documents the regression as expected behavior:
  > "The fill cursor emits fill rows per unique key combination. Since 'open' (first()) has distinct prev values per bucket, the fill produces two rows per bucket for the gap hour."

  Master's expectation for the same query is one row per (key, bucket).

- `isUnsupportedPrevAggType` in `SqlOptimiser.java:535-549` returns `false` on any aggregate whose argument is not a bare `LITERAL` — e.g. `sum(a*1)`, `first(substr(s, 0, 5))`, `last(concat(a, b))`, CASE expressions, casts. The optimizer gate therefore lets those through to codegen, where the safety-net then reclassifies them as FILL_KEY if their output type is unsupported.

- `isUnsupportedPrevType` (`SqlOptimiser.java:552-559`) and `isFastPathPrevSupportedType` (`SqlCodeGenerator.java:1199-1210`) are not complements. LONG128 and INTERVAL fall in the gap: optimizer accepts them, codegen can't encode them, safety-net fires. Same wrong-result failure mode.

## Design intent

Both external design docs (`/Users/sminaev/projects/questdb/plans/fill-fast-path-overview.md` §9 and `fill-fast-path-design-review_v2.md` §3, §8) state the design explicitly:

- "FILL(PREV) on STRING/SYMBOL/VARCHAR/LONG256/BINARY/UUID/arrays" → falls back to legacy path.
- "The optimizer gate is an optimization (avoids wasted GROUP BY work); the safety net is correctness."
- "When the optimizer gate rejects a query, SAMPLE BY metadata stays on the model and SqlCodeGenerator dispatches to one of the eight legacy factories."

No design doc mentions "fail-loud SqlException" as acceptable. Queries that reach the codegen safety-net today must end up on the legacy cursor path — producing correct results — not error out. Phase 12 restores that invariant.

## Mechanism: retro-fallback

The gate is irreversible at codegen time (by the time `generateFill` runs, `rewriteSampleBy` has already set `model.getSampleBy() == null` and the groupByFactory is built). Retro-fallback requires stashing the pre-rewrite state and restoring it on detection.

### Stash

At `SqlOptimiser.rewriteSampleBy` (around line 8156), before the rewrite clears the SAMPLE BY node from the nested model, save the original `sampleByNode` on the model. Add a `QueryModel.stashedSampleByNode` field (plus getter/setter/clear/copy handling analogous to `fillOffset`).

### Detection

In `SqlCodeGenerator.generateFill`, after `groupByFactory.getMetadata()` is available and per-column `fillModes` and `prevSourceCols` are built, authoritatively check every PREV-sourced column's resolved output type against `isFastPathPrevSupportedType`. The fully-resolved metadata is the source of truth — no aggregate-rule inference is needed at codegen time.

### Re-dispatch

Delete the safety-net block at `SqlCodeGenerator.java:3497-3512`. Replace with: if any `prevSourceCols` entry has an unsupported source type, free the partially-built structures (`constantFillFuncs`, `fillValues`, `prevSourceCols`, `keysMap` if allocated) and throw a new `FallbackToLegacyException` (or a sentinel boolean that the caller inspects — design choice deferred to the plan).

In `generateSelectGroupBy` (around line 7998 / 8171 / 8231 — the three call sites of `generateFill`), wrap the fast-path branch in a try/catch for the fallback signal. On catch: restore `model.setSampleBy(stashedSampleByNode)` (and any other metadata the rewrite cleared) and call `generateSampleBy(model, executionContext, sampleByNode, model.getSampleByUnit())` directly.

The exception mechanism keeps `generateFill`'s signature clean (it already throws `SqlException`; a subclass or sentinel variant fits). The alternative — a return-value signal — avoids exception-for-control-flow but requires touching all three call sites with the same pattern.

Close any resources allocated before the detection point on the throwing path. The caller's catch must not double-free anything the `generateFill` throw path already freed.

### Positioning of the gate

Keep the optimizer gate (`hasPrevWithUnsupportedType` / `isUnsupportedPrevAggType`) as a cheap performance optimization that avoids building-then-discarding the groupBy factory for the common cases. The gate's correctness is no longer load-bearing — the codegen check + retro-fallback are. Tighten the gate minimally (Tier 1 below).

## Optimizer gate scope — Tier 1 only

Since retro-fallback catches the residue, the gate stays intentionally simple. Tier 2 aggregate-rule inference and Tier 3 full expression walking are explicitly out of scope for this phase (pay the fallback cost instead of building a type-inference engine).

Tier 1 changes:

1. **Cross-column PREV resolution at the gate.** Remove the skip at `SqlOptimiser.java:521-525`. When `fillExpr` is `PREV(alias)`, resolve `alias` against the output-column set, look up the source column's type from the nested model, check against `isUnsupportedPrevType`. Block if unsupported. If the alias can't be resolved at gate time, pass through — fallback catches it.

2. **Close the LONG128 / INTERVAL gap.** Add `ColumnType.LONG128` and `ColumnType.INTERVAL` to `isUnsupportedPrevType` in `SqlOptimiser.java:552-559`. One-line change. Matches `isFastPathPrevSupportedType`.

No other gate changes. `isUnsupportedPrevAggType`'s expression-arg branch stays as-is (conservative "can't resolve → pass through"). Expression-arg queries that land on an unsupported output type will be caught by the codegen check and retro-fallback to legacy.

## Grammar resolution for FILL(PREV, PREV(...))

Discussion during `/gsd-discuss-phase 12` locked the accepted/rejected shapes. Implementation lives in `SqlCodeGenerator.generateFill` and surfaces positioned `SqlException` for every rejected shape.

### Accepted shapes

- **Bare `FILL(PREV)`** as the single element of a FILL clause — applies self-prev to every aggregate column.
- **Bare `PREV` inside a per-column list** (e.g. `FILL(PREV, NULL, PREV(s))`) — self-prev for that specific slot. Matches existing `testFillPrevCrossColumnKeyed`. Document in a comment at `SqlCodeGenerator.java:3417-3420` that bare PREV in a per-column list means "self-prev for this column".
- **`PREV(alias)`** where alias is another aggregate's output column. Runtime mirrors the source column's prev snapshot.
- **`PREV(key_col)`** where key_col is a SAMPLE BY key. Runtime reads key value from `keysMapRecord` directly (no snapshot needed — key is constant per group). Fix landed in the recent `PREV(key_col)` commit; stays valid.
- **`PREV(col_name)` where the source type matches the target column type at the tag level.** `ColumnType.tagOf(sourceType) == ColumnType.tagOf(targetType)`.

### Rejected shapes (each raises a positioned `SqlException`)

- **`PREV(timestamp_col)`** — the designated timestamp column. The timestamp getter short-circuits on `col == timestampIndex` so the cross-col path was a dead pointer; reject at compile time. Message: `"PREV cannot reference the designated timestamp column"` at `rhs.position`.
- **`PREV(target_col)` where `target_col` is the same output column** — equivalent to bare PREV (self-prev). Alias internally to `FILL_PREV_SELF` rather than `fillModes[col] = col`, eliminating one dead snapshot slot. No user-visible change, cleaner internal state. (This is not a rejection — it's a compile-time rewrite.)
- **Chain: `PREV(b)` where `b` is itself cross-col PREV** — chains are well-defined at runtime (snapshot contains data values only, every cross-col is one-hop to source's last data value, no cycles at runtime), but the resulting "data semantics" can be surprising when a user expects `LAG`-style "displayed semantics". Keep the grammar narrow until a clear use case emerges. Detection rule: **after `fillModes` is fully built, reject any column `col` where `fillModes[col] >= 0 AND fillModes[fillModes[col]] >= 0`**. Sketch (~6 lines, at the end of the per-column build loop in `SqlCodeGenerator.generateFill`):
  ```java
  for (int col = 0; col < columnCount; col++) {
      int mode = fillModes.getQuick(col);
      if (mode >= 0 && fillModes.getQuick(mode) >= 0) {
          throw SqlException.$(fillValuesExprs.getQuick(0).position,
                  "FILL(PREV) chains are not supported: source column is itself a cross-column PREV");
      }
  }
  ```
  Rule accepts: `PREV(b)` where b is FILL_CONSTANT (NULL or constant), FILL_PREV_SELF (bare PREV), FILL_KEY (key column). Rule rejects: true mutual cycles (`PREV(b), PREV(a)`) and multi-hop chains (`PREV(b), PREV(c), PREV`) when the source is itself cross-col. Position points at the FILL clause (generic); see Claude's Discretion below for precise-position variant.
- **Malformed PREV shapes — single unified rejection:**
  - `PREV(func_call)` — `rhs.type != LITERAL`.
  - `PREV(a, b)` — `paramCount > 1`.
  - `PREV()` — `paramCount == 0`.
  - `PREV($1)` (bind variable) — falls under non-LITERAL rhs since `BIND_VARIABLE != LITERAL` (verified at `ExpressionNode.java:46,50`).
  - Error: `"PREV argument must be a single column name"` at `fillExpr.position`.
- **`PREV(col_name)` with a type tag mismatch** — source and target column types differ at the tag level. The current runtime bit-reinterprets the 8-byte snapshot as whatever the target getter wants, producing garbage. Reject at compile time: `"FILL(PREV(<src>)): source type <X> cannot fill target column of type <Y>"` at the appropriate position.

### Tests for the grammar

For each rejected shape, add one positive and one negative test in `SampleByFillTest.java`. For each accepted shape, ensure at least one existing or new test exercises it. Decisions:

- `testFillPrevRejectTimestamp` (negative) — `FILL(PREV(ts))` raises the documented error.
- `testFillPrevSelfAlias` (positive) — `FILL(PREV(a))` where `a` is the target column — confirm output matches bare `FILL(PREV)` semantics and the plan shows the aliased mode.
- `testFillPrevRejectMutualChain` (negative) — `FILL(PREV(b), PREV(a))` mutual cycle detected and rejected.
- `testFillPrevRejectThreeHopChain` (negative) — `FILL(PREV(b), PREV(c), PREV)` where a→b→c, rejected.
- `testFillPrevAcceptPrevToSelfPrev` (positive) — `FILL(PREV(b), PREV)` where b is bare PREV (FILL_PREV_SELF) — accepted, not a chain under the rule.
- `testFillPrevAcceptPrevToConstant` (positive) — `FILL(PREV(b), NULL)` and `FILL(PREV(b), 42.0)` — accepted, source is FILL_CONSTANT.
- `testFillPrevRejectFuncArg`, `testFillPrevRejectMultiArg`, `testFillPrevRejectNoArg`, `testFillPrevRejectBindVar` — the four shapes of malformed PREV.
- `testFillPrevRejectTypeMismatch` — `FILL(PREV(long_col))` filling a DOUBLE target.

## Specific items in scope

### Correctness block

1. **Close the LONG128 / INTERVAL gap at the gate.** `isUnsupportedPrevType` gets both tags added. (Tier 1.)

2. **Cross-col PREV resolution at the gate.** Remove the skip at `SqlOptimiser.java:521-525`; resolve alias → source-column type, check against `isUnsupportedPrevType`. (Tier 1.)

3. **Retro-fallback at codegen.** Stash original `sampleByNode` in `rewriteSampleBy`, detect unsupported type in `generateFill` via fully-resolved `groupByFactory.getMetadata()`, re-dispatch to `generateSampleBy` on detection via restored model. Delete the safety-net block at `SqlCodeGenerator.java:3497-3512`. Tests: `testFillPrevExpressionArgDecimal128Fallback`, `testFillPrevExpressionArgStringFallback`, `testFillPrevCaseOverDecimalFallback`, `testFillPrevLong128Fallback`, `testFillPrevIntervalFallback` — each asserts the legacy plan is picked and the output matches the legacy cursor's.

4. **Defensive guard for `TO`-returning-LONG_NULL.** `SampleByFillRecordCursorFactory.java:523-526` uses an object-identity check against the `TimestampConstantNull` singleton. A user-supplied `TO` expression that yields `LONG_NULL` (`TO null::timestamp`, bind variable, function returning null at runtime) may not be the singleton yet still evaluates to `LONG_NULL`; `maxTimestamp` gets promoted to `Long.MAX_VALUE` on line 591 while `hasExplicitTo` stays `true`, so the main loop never hits the `isBaseCursorExhausted && !hasExplicitTo` short-circuit at line 362 and emits fill rows up to `Long.MAX_VALUE`. Add one line after 525:
   ```java
   if (maxTimestamp == Numbers.LONG_NULL) hasExplicitTo = false;
   ```
   Regression test: `testFillToNullTimestamp` — `TO null::timestamp` produces bounded output.

5. **Restore `testSampleByFillNeedFix` expectations.** Replace the PR's 6-row expected output with master's 3-row output and drop the "two rows per bucket" comment.

### Grammar block (new — from discussion)

6. **Implement the grammar decisions above** in `SqlCodeGenerator.generateFill`: self-alias detection, timestamp rejection, chain rejection, malformed-shape rejection, type-tag mismatch rejection. Error positions point at the offending AST node.

7. **Grammar regression tests** — the eight tests listed under "Tests for the grammar" above.

### Test coverage additions (previously uncovered production paths)

Phase 11 added FILL_KEY dispatch for 128/256-bit getters and corrected null sentinels for geo/decimal getters, but did not add regression tests for those branches. These paths are fully reachable and would silently break if the phase 11 fixes were reverted — no existing test would fail. Add one test per production branch:

8. **`testFillKeyedUuid`** — keyed `SAMPLE BY FILL(NULL)` with a `UUID` key column and sparse data. Covers `FillRecord.getLong128Hi/Lo` FILL_KEY dispatch (`SampleByFillRecordCursorFactory.java:985-1004`). Assert UUID key value appears in fill rows (not null).

9. **`testFillKeyedLong256`** — keyed `SAMPLE BY FILL(NULL)` with a `LONG256` key column. Covers `getLong256A/B` and `getLong256(col, sink)` FILL_KEY dispatch (`:1007-1046`).

10. **`testFillKeyedDecimal128`** — keyed `SAMPLE BY FILL(NULL)` with a `DECIMAL(25,2)` (DECIMAL128) key column. Covers `getDecimal128(col, sink)` FILL_KEY dispatch (`:837-860`).

11. **`testFillKeyedDecimal256`** — keyed `SAMPLE BY FILL(NULL)` with a `DECIMAL(39,2)` (DECIMAL256) key column. Covers `getDecimal256(col, sink)` FILL_KEY dispatch (`:867-890`).

12. **`testFillPrevGeoNoPrevYet`** — keyed `SAMPLE BY FROM '2024-01-01' TO '2024-01-02' FILL(PREV)` with a GEOHASH column and data only in later buckets. Leading fill rows must emit `GeoHashes.BYTE_NULL` / `GeoHashes.SHORT_NULL` / `GeoHashes.INT_NULL` / `GeoHashes.NULL` (the exact sentinel constants chosen in phase 11) — not `0`, not `Numbers.*_NULL`. Covers the null-sentinel branches at lines 933, 945, 957, 969.

13. **Convert `assertSql` → `assertQueryNoLeakCheck` for all fast-path tests** in `SampleByFillTest.java`. Selection rule: "if the test exercises the new fast-path fill cursor and `supportsRandomAccess=false` / `size()=-1` are the correct expected values, convert." Skip: legacy-plan assertions, multi-step tests with mid-test DDL that have specific reasons to stay on `assertSql`, storage tests in the cairo package. Expected count ~15; exact set determined by applying the rule. Do not bulk-convert tests that explicitly use `assertSql` for a reason.

### Dead code block

14. **Remove the `anyPrev` detection loop** at `SqlCodeGenerator.java:3392-3398`:
   ```java
   boolean anyPrev = false;
   for (int i = 0, n = fillValuesExprs.size(); i < n; i++) {
       if (isPrevKeyword(fillValuesExprs.getQuick(i).token)) {
           anyPrev = true;
           break;
       }
   }
   ```
   `anyPrev` is only read at line 3405 inside `anyPrev && fillValuesExprs.size() == 1 && isPrevKeyword(fillValuesExprs.getQuick(0).token)`. The second and third conjuncts force `anyPrev == true` on their own — the loop is redundant.
   Replace the check with:
   ```java
   if (fillValuesExprs.size() == 1
           && isPrevKeyword(fillValuesExprs.getQuick(0).token)
           && fillValuesExprs.getQuick(0).type == ExpressionNode.LITERAL) {
   ```

### Code quality block

15. **Alphabetize `FillRecord` getters in `SampleByFillRecordCursorFactory`.** Current order is grouped-by-kind. CLAUDE.md requires strict alphabetical ordering within kind + visibility. Reorder to `getArray, getBin, getBinLen, getBool, getByte, getChar, getDecimal128, getDecimal16, getDecimal256, getDecimal32, getDecimal64, getDecimal8, getDouble, getFloat, getGeoByte, getGeoInt, getGeoLong, getGeoShort, getIPv4, getInt, getLong, getLong128Hi, getLong128Lo, getLong256, getLong256A, getLong256B, getShort, getStrA, getStrB, getStrLen, getSymA, getSymB, getTimestamp, getVarcharA, getVarcharB, getVarcharSize`. Pure reordering — no logic change.

16. **Alphabetize `SampleByFillCursor` private members.** `fillMode` and `hasKeyPrev` should precede `initialize`; the static `readColumnAsLongBits` belongs with other statics, not interleaved with instance methods.

17. **Move `isKeyColumn()` into the private-static `is*` cluster in `SqlCodeGenerator`.** Currently at line 3640 between `generateFill` and `generateFilter`; should sit next to `isFastPathPrevSupportedType` around line 1199.

18. **Alphabetize imports.** `SampleByFillRecordCursorFactory.java:27-57` has `GeoHashes` before `CairoConfiguration`/`ColumnType`, `Decimals` after `IntList`. `SqlCodeGenerator.java` line 172 inserts `SampleByFillRecordCursorFactory` between `FillRangeRecordCursorFactory` and `GroupByNotKeyedRecordCursorFactory` — belongs after the `SampleByFill*` block.

19. **Replace fully-qualified type names with plain imports.** `SampleByFillRecordCursorFactory.java` uses `io.questdb.std.Decimal128`, `io.questdb.std.Decimal256`, `io.questdb.std.Long256`, `io.questdb.std.BinarySequence`, `io.questdb.cairo.arr.ArrayView`, `io.questdb.std.str.CharSink`, `io.questdb.std.str.Utf8Sequence` inline (lines 816, 823, 837, 869, 1007, 1027, 1038, 1104, 1115). Sibling factories (`FillRangeRecordCursorFactory`) use plain imports — match that style.

20. **Always emit `fill=` in the explain plan.** `toPlan` at `SampleByFillRecordCursorFactory.java:177-179` only emits `fill=prev` when `hasPrevFill` is true. `FILL(NULL)` and `FILL(<const>)` plans currently elide the fill mode, making them indistinguishable from `FILL(NONE)` in explain output. Emit one of `fill=null`, `fill=prev`, `fill=value` unconditionally. Update affected `assertPlanNoLeakCheck` tests (`SampleByFillTest`, `ExplainPlanTest`) in the same commit.

21. **Surface `Dates.parseOffset` failures.** `SqlCodeGenerator.java:3379-3383` silently drops the error when the offset string fails to parse — the `if` skips, `calendarOffset` stays 0, and the misaligned output has no signal back to the user. If `rewriteSampleBy` has already validated the offset this should be impossible; treat it as a corruption invariant and assert (`assert parsed != Numbers.LONG_NULL : "offset should have been validated in rewriteSampleBy: " + offsetToken`). If we later learn it *can* fail at this stage, the assert flips to a `SqlException.$(fillOffsetNode.position, ...)`.

## Non-goals

- Extend the fast path to new types (STRING/VARCHAR/SYMBOL/LONG256/UUID aggregate PREV). A separate branch already explores that (`sm_fill_prev_fast_all_types` / `f43a3d7057`) and is out of scope here.
- Rewrite the optimizer gate from scratch. The goal is minimal, surgical — not "block everything we can't prove is safe".
- Change semantics of `FILL_KEY` or `OrderedMap` key construction.
- Aggregate-rule type inference at the gate (Tier 2) or full expression walking (Tier 3). Retro-fallback covers the residue at negligible cost — the common unsupported cases are rare, and building-then-discarding a groupBy factory for those queries is an acceptable price.
- Numeric-widening conversion for mismatched `PREV(src)` / target types (INT→LONG, FLOAT→DOUBLE via getter conversion). Deferred — the current decision is tag-match-only.
- PR metadata items (title / body / `.planning/` diff noise) — tracked separately in STATE.md's open items.

## Success criteria

1. `testSampleByFillNeedFix` matches master's 3-row expected output; no duplicated buckets for the single-symbol gap query.
2. Queries with expression-argument aggregates returning DECIMAL128 / DECIMAL256 / LONG256 / UUID / STRING / VARCHAR / SYMBOL / BINARY / array / LONG128 / INTERVAL take the legacy path and produce correct results (no phantom fill rows, no wrong-type crashes). No SqlException is surfaced to the user for any query that master handles correctly.
3. Queries with expression-argument aggregates returning a supported type (`sum(price * qty)` → DOUBLE, `avg(a * 2)` → DOUBLE, `first(int_col + 1)` → INT) still take the fast path.
4. `LONG128` and `INTERVAL` PREV aggregates route to the legacy path via the gate (no fallback needed).
5. Cross-col `PREV(alias)` with an unsupported source type routes to legacy via the gate.
6. The codegen safety-net block at `SqlCodeGenerator.java:3497-3512` is removed; a retro-fallback mechanism (stashed SAMPLE BY node + exception-triggered re-dispatch) replaces it.
7. The grammar rules for `FILL(PREV, PREV(...))` are implemented: `PREV(ts)`, chains, malformed shapes, and type-tag mismatches each raise positioned `SqlException`. `PREV(self)` aliases to `FILL_PREV_SELF` internally. Bare `PREV` inside a per-column list is documented as accepted.
8. All 633 existing tests across `SampleByFillTest`, `SampleByTest`, `SampleByNanoTimestampTest` still pass (after updating `testSampleByFillNeedFix` to match master).
9. Five FILL_KEY regression tests for UUID / Long256 / Decimal128 / Decimal256 key columns, plus `testFillPrevGeoNoPrevYet` — all pass, all would fail without the phase 11 production fixes.
10. Eight grammar regression tests (see "Grammar block" item 7) — all pass.
11. Retro-fallback regression tests (`testFillPrevExpressionArgDecimal128Fallback`, `testFillPrevExpressionArgStringFallback`, `testFillPrevCaseOverDecimalFallback`, `testFillPrevLong128Fallback`, `testFillPrevIntervalFallback`) — each asserts legacy plan is picked and output matches legacy cursor.
12. Fast-path `SampleByFillTest` tests that hit streaming single-pass queries use `assertQueryNoLeakCheck` (not `assertSql`), per the selection rule; a regression in `supportsRandomAccess` or `size()` would be caught.
13. The redundant `anyPrev` detection loop at `SqlCodeGenerator.java:3392-3398` is removed.
14. `TO null::timestamp` (or any runtime-null `TO` expression) produces a bounded result — the `hasExplicitTo` guard demotes to `false` when `maxTimestamp == LONG_NULL`. A regression test (`testFillToNullTimestamp`) exercises this.
15. `FillRecord` getters are in strict alphabetical order; imports in both `SampleByFillRecordCursorFactory.java` and `SqlCodeGenerator.java` are alphabetical; `isKeyColumn` sits next to its sibling `is*` helpers.
16. `FillRecord` method signatures use plain imports rather than fully-qualified type names.
17. `toPlan` emits `fill=null|prev|value` for every plan; existing plan-text assertions updated accordingly.
18. `Dates.parseOffset` failure at `SqlCodeGenerator.java:3379` either asserts (preferred, based on `rewriteSampleBy` having already validated the value) or raises a positioned `SqlException` — never silently drops.

## Canonical references

Downstream agents MUST read these before planning or implementing.

### External design docs (pre-existing, authoritative on intent)

- `/Users/sminaev/projects/questdb/plans/fill-fast-path-overview.md` §9 — explicit list of queries that fall back to legacy cursor; states that the old path is retained as fallback.
- `/Users/sminaev/projects/questdb/plans/fill-fast-path-design-review_v2.md` §3 — type support matrix ("Others are blocked by the optimizer gate and fall back to the old path").
- `/Users/sminaev/projects/questdb/plans/fill-fast-path-design-review_v2.md` §8 "Two-layer type defense" — gate is optimization; safety net is correctness.
- `/Users/sminaev/projects/questdb/plans/fill-fast-path-design-review_v2.md` §9 "Fallback conditions" — concrete mapping of rejected conditions to legacy factories.
- `/Users/sminaev/projects/questdb/plans/fill-fast-path-design-review_v2.md` §10 "Known issues" — documents the wrapped-model / CTE / SYMBOL-key-column issue that phase 11 rationalized the safety-net around.

### Prior phase context

- `.planning/phases/04-cross-column-prev/04-CONTEXT.md` — original `PREV(col_name)` syntax decision; deferred `PREV(expression)` and PREV with type coercion.
- `.planning/phases/07-prev-type-safe-fast-path/07-CONTEXT.md` §25-39, §49-53 — type matrix, gate design, safety-net rationale (originally "throws SqlException" per T-07-03).
- `.planning/phases/07-prev-type-safe-fast-path/07-RESEARCH.md` §437-441 — "column types for aggregate OUTPUTS are not known at optimizer time in general" — fundamental constraint explaining why the gate is argument-type-based.
- `.planning/phases/11-hardening-review-findings-fixes-and-missing-test-coverage/11-CONTEXT.md` — phase 11 retained the safety-net with the SYMBOL-key rationale; phase 12 revisits that decision.

### Source code references

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` §3379-3383 — `Dates.parseOffset` silent-drop site.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` §3392-3398 — `anyPrev` dead-code loop.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` §3405-3470 — fill-spec build including bare-PREV and per-column branches; new grammar rules implement here.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` §3477-3512 — `prevSourceCols` build + safety-net (safety-net block to delete).
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` §7998, 8171, 8231 — three `generateFill` call sites; retro-fallback try/catch goes here.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` §1199-1210 — `isFastPathPrevSupportedType` (unchanged reference).
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` §474-533 — `hasPrevWithUnsupportedType` (Tier 1 changes here).
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` §521-525 — cross-col PREV skip (remove).
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` §535-559 — `isUnsupportedPrevAggType`, `isUnsupportedPrevType` (add LONG128, INTERVAL).
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` §8156 — `rewriteSampleBy` gate invocation site; stash saves original `sampleByNode` near here.
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` §177-179, 523-526, 600-630 — `toPlan`, `hasExplicitTo` singleton check, `prevValue` dispatch.
- `core/src/main/java/io/questdb/griffin/model/ExpressionNode.java` §46, 50 — `BIND_VARIABLE != LITERAL` (used in unified rejection rule).
- `core/src/main/java/io/questdb/griffin/model/QueryModel.java` — `fillOffset` field pattern (model for adding `stashedSampleByNode`).
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` §~5943 — `testSampleByFillNeedFix` (restore to master's 3-row expected output).
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — all new tests land here.

## Files expected to change

- `core/src/main/java/io/questdb/griffin/model/QueryModel.java` — `stashedSampleByNode` field with getter/setter/clear/copy handling.
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` — cross-col resolution in gate, `LONG128`/`INTERVAL` in `isUnsupportedPrevType`, stash original `sampleByNode` before rewrite.
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` — grammar rule enforcement in `generateFill`, codegen-time unsupported-type detection + throw, retro-fallback try/catch in the three `generateFill` call sites, delete safety-net, remove `anyPrev` loop, move `isKeyColumn` into `is*` cluster, reorder imports, assert/throw on `Dates.parseOffset` failure.
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — `hasExplicitTo` guard for `maxTimestamp == LONG_NULL`, alphabetize `FillRecord` getters and `SampleByFillCursor` members, replace FQNs with plain imports, reorder imports, always emit `fill=` in `toPlan`.
- New: `FallbackToLegacyException.java` (or sentinel mechanism — plan to confirm) in `core/src/main/java/io/questdb/griffin/`.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` — new regression tests: retro-fallback (5), FILL_KEY for UUID/Long256/Decimal128/256/geo-no-prev (5), grammar (8), `TO null::timestamp` (1), factory-property conversions per rule (~15). Plus plan-text assertions updated for the new `fill=` attribute.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` — `testSampleByFillNeedFix` restored to master's 3-row expected output; plan assertions refreshed for new `fill=` attribute.
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` — mirror any `SampleByTest` expected-output adjustments.
- `core/src/test/java/io/questdb/test/griffin/ExplainPlanTest.java` — plan-text assertions refreshed for new `fill=` attribute on non-PREV queries.

## Implementation decisions (summary)

### Mechanism
- **D-01:** Retro-fallback via stashed `sampleByNode` + try/catch in `generateSelectGroupBy`. No fail-loud. Preserves no-regression invariant.
- **D-02:** Optimizer gate is Tier 1 only (cross-col resolution + LONG128/INTERVAL). No Tier 2 aggregate-rule inference, no Tier 3 expression walker. Retro-fallback covers residue.
- **D-03:** Codegen-time unsupported-type check uses `groupByFactory.getMetadata().getColumnType(col)` — fully resolved. No aggregate-rule inference at codegen either.

### Grammar (FILL(PREV, PREV(...)))
- **D-04:** Bare `PREV` inside per-column list means self-prev for that slot. Keep and document.
- **D-05:** `PREV(ts)` — reject at compile time with positioned error.
- **D-06:** `PREV(self)` — alias internally to `FILL_PREV_SELF` (not rejection, just internal normalization).
- **D-07:** Chain rejection — reject iff `fillModes[col] >= 0 AND fillModes[fillModes[col]] >= 0` (source column is itself cross-col PREV). Accepts `PREV(b)` where b is FILL_CONSTANT / FILL_PREV_SELF / FILL_KEY. Simplest variant (~6 lines) with generic FILL-clause position. Chains are runtime-safe (no cycles, snapshot is one-hop to source's last data value) but "data semantics" diverge from `LAG`-style "displayed semantics" in rare consecutive-gap cases — keep the grammar narrow until a clear use case emerges.
- **D-08:** Malformed PREV shapes (non-LITERAL rhs, paramCount != 1) — single unified rejection at `fillExpr.position`. Bind variables covered by this rule.
- **D-09:** Type-tag mismatch between PREV source and target — reject at compile time.

### Test conversion
- **D-10:** `assertSql` → `assertQueryNoLeakCheck` conversion uses the rule: "if the test exercises the new fast-path fill cursor and `supportsRandomAccess=false` / `size()=-1` are the correct expected values, convert." Skip legacy-plan, mid-test-DDL, storage tests.

### Claude's discretion
- Exception vs sentinel for the fallback signal mechanism — both work; plan decides based on what's cleanest to thread through the three call sites.
- Error-message wording for rejected grammar shapes — use positioned `SqlException.$(position, "message").put(...)` style, pick clear messages; exact wording at implementation time.
- Chain rejection — precise-position variant (~10 lines, remembers `ExpressionNode` per column so the error points at the specific offending `PREV(...)`) vs the simplest ~6-line variant with generic FILL-clause position. Both produce identical behavior; plan picks based on how much the UX upgrade is worth for a rare error path.

## Deferred ideas

- Numeric-widening conversion for `PREV(src)` / target type mismatches (INT→LONG, FLOAT→DOUBLE via explicit snapshot-getter conversion) — deferred; current decision is tag-match-only.
- PREV for STRING/SYMBOL/VARCHAR — tracked separately on `sm_fill_prev_fast_all_types` branch (f43a3d7057), out of scope for phase 12.
- Tier 2 aggregate-rule inference at the gate — if real workloads show high retro-fallback rate for common expression-arg queries, revisit.
- CASE/function-call expression walker at the gate — same rationale.
- `FILL(LINEAR)` on fast path — per project roadmap, postponed.
- Old cursor path removal — postponed.
- `ALIGN TO FIRST OBSERVATION` on fast path — postponed.
- Two-token stride syntax on fast path — postponed.

## Dependency context

Phase 11 retained the safety-net deliberately — the SUMMARY's decisions list includes:
> "Keep the late-codegen safety-net reclassification (SYMBOL → FILL_KEY) because the optimizer gate does not cover misidentified key columns inside CTE/subquery contexts"

Phase 12 overturns that decision. The stated CTE case is not demonstrated by any existing test (`testFillPrevKeyedCte` does not trigger the safety net — its key column `city` is correctly classified by `isKeyColumn` as a STRING literal, and all aggregate outputs in the query are DOUBLE). If the CTE misclassification case surfaces during phase 12 verification, it is handled the same way as any other safety-net trigger: retro-fallback to legacy, not silent reclassification.

---

*Phase: 12-replace-safety-net-reclassification-with-legacy-fallback-and*
*Context gathered: 2026-04-16*
