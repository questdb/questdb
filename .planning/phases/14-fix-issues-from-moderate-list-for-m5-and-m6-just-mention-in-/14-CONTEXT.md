# Phase 14 Context: Fix Moderate `/review-pr 6946` findings

**Gathered:** 2026-04-20
**Status:** Ready for planning
**Source:** `/review-pr 6946` session on branch `sm_fill_prev_fast_path`, conversation archived — full content preserved in this document so the planner has the complete finding trail after `/clear`.

<domain>
## Phase Boundary

Fix the Moderate-severity code-review findings against PR #6946 that are actionable within this branch:

- **M-1** — bare `FILL(PREV)` misclassification under optimizer reorder
- **M-2** — missing `FILL_KEY` + cross-column-PREV-to-key dispatch in `getArray`/`getBin`/`getBinLen`
- **M-3** — `FILL(PREV(colX))` silent broadcast across aggregates
- **M-4** — sub-day + TIME ZONE + FROM grid divergence
- **M-7** — pre-existing `SortedRecordCursorFactory` constructor double-free
- **M-8** — missing `FillRecord.getInterval` (crashes on INTERVAL columns)
- **M-9** — cross-column PREV type check only uses `tagOf()` (DECIMAL/GEOHASH/ARRAY slip through)
- **Mn-13** — `generateFill` success path leaks `fillValues`

Plus test improvements drawn from Mn-11 (per-type FILL(PREV), ghost-test cleanup, Decimal zero-vs-null, RCM direct factory assertion restoration).

**M-5** (SortedRecordCursor materialization memory) and **M-6** (triple-pass scan) are intentionally out of scope for code changes — they land in the PR #6946 description under Trade-offs instead.
</domain>

<decisions>
## Implementation Decisions

### Plan / commit grouping (D-01..D-04)

- **D-01** — Four plans, grouped by area + one standalone pre-existing fix as the final commit.
  - **Plan 01 — Codegen cluster**: M-1, M-3, M-9, Mn-13. All in `SqlCodeGenerator.java`. Share the `generateFill()` neighborhood.
  - **Plan 02 — Cursor cluster**: M-2, M-8. All in `SampleByFillRecordCursorFactory.java` `FillRecord` class. Both add missing getter branches; natural to bundle.
  - **Plan 03 — Optimiser**: M-4. `SqlOptimiser.java` + fill cursor init path. Cross-file but single concern.
  - **Plan 04 — Pre-existing defensive fix**: M-7 standalone. Last commit of the phase. Outside the fill feature's natural scope but landed defensively here.
- **D-02** — Commit hygiene strict from commit 1: CLAUDE.md-compliant titles (no Conventional Commits prefix, under 50 chars, long-form body). No `WIP:` or phase-numbered commits in Phase 14's output.
- **D-03** — Invest in regression triggers even for hard-to-reproduce cases. M-1 reorder test must construct an outer-projection shape that triggers `propagateTopDownColumns0` reorder. M-7 test must simulate `RecordTreeChain` constructor failure (probe via configuration: shrinking `sqlSortKeyMaxPages` or similar to force allocation failure).
- **D-04** — Per-plan verification: each plan's tests pass with `mvn -Dtest=<class> test` in isolation, and the combined SAMPLE BY trio (`SampleByTest + SampleByFillTest + SampleByNanoTimestampTest`) passes at phase end.

### M-1: bare FILL(PREV) reorder-safe classification (D-05)

- **D-05** — Reuse the `factoryColToUserFillIdx` alias-mapping machinery from the per-column branch. Walk `bottomUpCols` in user order; for each entry derive AST type (`LITERAL` = key, `FUNCTION`/timestamp_floor = skip). Decide `FILL_KEY` vs `FILL_PREV_SELF` per factory column via the mapping, not by passing factory-index to user-order `isKeyColumn`. Don't invent a new detection path — fold into the existing structure.

### M-2: ARRAY/BIN getters missing dispatch (D-06)

- **D-06** — Add the missing `FILL_KEY` and cross-column-PREV-to-key branches to `getArray`, `getBin`, `getBinLen` — mirror the exact 4-branch pattern used by every other typed getter. Do not reject ARRAY/BINARY keys at factory-creation time; the cost of symmetrical dispatch is lower than the cost of restricting key types that `OrderedMap`/`RecordSinkFactory` already support.

### M-3: broadcast tightening (D-07)

- **D-07** — In the under-spec check at `SqlCodeGenerator.java:3567-3574`, replace `isPrevKeyword(only.token) || isNullKeyword(only.token)` broadcast condition with:
  ```java
  final boolean isBareBroadcastable =
          (isPrevKeyword(only.token) && only.type == ExpressionNode.LITERAL)
       || isNullKeyword(only.token);
  ```
  Bare PREV has `type == LITERAL`; `PREV(colX)` has `type == FUNCTION` and therefore does not broadcast. NULL keyword remains broadcastable.

### M-4: TIME ZONE + FROM propagation (D-08, D-09)

- **D-08** — Propagate `to_utc`-wrapped FROM/TO into the fill cursor for sub-day + TIME ZONE queries. In `SqlOptimiser.rewriteSampleBy`, when `isSubDay && sampleByTimezoneName != null`, pass `createToUtcCall(sampleByFrom, sampleByTimezoneName)` to `nested.setFillFrom` (and the TO counterpart to `setFillTo`). `createToUtcCall` already exists at `SqlOptimiser.java:2725` and is already used for the GROUP BY at `:8308`.
- **D-09** — Fill cursor parsing unchanged: `SqlCodeGenerator.generateFill` continues to call `functionParser.parseFunction(fillFrom, …)`, which evaluates the `to_utc` wrapper naturally and returns a `TimestampFunction` yielding the shifted UTC instant. No new cursor state, no timezone-aware logic in the cursor.

### M-7: double-free defensive fix (D-10)

- **D-10** — Fix pattern: inside `SortedRecordCursorFactory` constructor, assign `this.base = base;` **inside** the try block so that on `new RecordTreeChain(...)` throw, the catch block can null it out before calling `close()`. Equivalently, add an `isClosed` guard in `AbstractRecordCursorFactory.close()`. Pick the lower-risk option by reading the class to see which existing factories rely on non-idempotent `_close()` behavior — prefer the per-class fix (nulling `this.base`) unless the guard is trivially safe across the hierarchy. Land as last commit of Phase 14.

### M-8: INTERVAL getter (D-11)

- **D-11** — Add `FillRecord.getInterval(int col)` mirroring the 4-branch dispatch. Default null sentinel: use `Interval.NULL` (verify exact constant when writing; if QuestDB has no singleton null Interval constant, use the codebase's canonical null rendering — `io.questdb.cairo.sql.Interval` contract). Then grep `io.questdb.cairo.sql.Record` for every method that defaults to `throw new UnsupportedOperationException()`; audit `FillRecord` covers each. Close any remaining gap as part of Plan 02.

### M-9: cross-column PREV full-type match (D-12)

- **D-12** — Replace `tagOf(target) != tagOf(source)` with full-int equality for DECIMAL / GEOHASH / ARRAY. Prefer an existing helper if one exists (probe `ColumnType` for `isSameType`, `equalsExactly`, etc.); otherwise inline:
  ```java
  final int targetType = groupByMetadata.getColumnType(col);
  final int sourceType = groupByMetadata.getColumnType(srcColIdx);
  final short targetTag = ColumnType.tagOf(targetType);
  final boolean needsExactTypeMatch =
          ColumnType.isDecimal(targetType)
       || ColumnType.isGeoHash(targetType)
       || targetTag == ColumnType.ARRAY;
  final boolean compatible = needsExactTypeMatch
          ? targetType == sourceType
          : targetTag == ColumnType.tagOf(sourceType);
  ```
  Error message on mismatch includes both types via `ColumnType.nameOf(...)` so the user sees precision/scale/dims.

### Mn-13: fillValues success-path cleanup (D-13)

- **D-13** — `Misc.freeObjList(fillValues)` immediately before the `return new SampleByFillRecordCursorFactory(...)` on the success path. Transferred slots are already null; only residuals are freed. Catch-block semantics unchanged.

### Test coverage (D-14..D-18)

- **D-14** — Per-type dedicated `FILL(PREV)` tests for: BOOLEAN, BYTE, SHORT, INT, LONG, DATE, TIMESTAMP, IPv4, BINARY, INTERVAL. Live in `SampleByFillTest.java`. Each test exercises non-keyed FILL(PREV) for its type with a dense-then-gap pattern. INTERVAL is mandatory for M-8; the others close the gap Mn-11 identified.
- **D-15** — Default helper: `assertQueryNoLeakCheck`. Fall back to `assertSql` only when the cursor has a documented factory-property limitation (e.g., `supportsRandomAccess=false` is the contract here, but the helper already accepts that).
- **D-16** — Ghost-test cleanup: `SampleByTest.testSampleByFromToIsDisallowedForKeyedQueries` and its nano-timestamp twin in `SampleByNanoTimestampTest`. Options: rename to reflect the new passing behavior + add assertion on the output, OR delete outright. Plan-time choice: RENAME + assert. Keep the history of why this case was originally an error (comment in test body).
- **D-17** — Decimal zero-vs-null distinction test: `FILL(PREV)` query where some buckets have legitimate `0` decimal values and some precede any data. Assert the two render differently (NULL vs `0.00`).
- **D-18** — Restore `RecordCursorMemoryUsageTest` direct factory-type assertion. Update the three tests currently asserting `SelectedRecordCursorFactory` to assert `SampleByFillRecordCursorFactory` is in the base-factory chain. Exercises the actual new layer's close-path memory cleanup.

### M-5/M-6 PR description (D-19, D-20)

- **D-19** — Add both to the **existing** `## Trade-offs` section of PR #6946's body. Do not create a new section. M-5 and M-6 are shipping tradeoffs, not deferred future work; they belong next to the random-access and OrderedMap-sizing bullets.
- **D-20** — Claude proposes the wording; user edits. No benchmark prescription (no SAMPLE BY FILL JMH benchmarks exist today, verified via `benchmarks/` grep — `SampleByIntervalIteratorBenchmark` covers materialized-view interval iteration only). Just flag the O(K×B) memory envelope and the 3× scan cost honestly. Commit the PR body update as part of Phase 14 (or via `gh pr edit`).

### Claude's Discretion

- M-7 exact fix mechanism (null `this.base` in constructor catch vs. add `isClosed` guard) — pick lower-risk option after reading the factory hierarchy. Both satisfy D-10 intent.
- Exact placement of new test methods within `SampleByFillTest.java` — alphabetical per CLAUDE.md, next to existing per-type tests.
- Helper method extraction — if the 4-branch dispatch pattern can be factored into a shared helper without per-row method-call overhead, do it. Otherwise keep the inlined pattern.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project conventions
- `CLAUDE.md` — ASCII-only log/error messages, `is/has` boolean naming, zero-GC data path, commit hygiene (no Conventional Commits prefixes, <50 char titles, long-form body), `assertMemoryLeak`/`assertQueryNoLeakCheck` test helpers.

### Prior phase CONTEXTs (patterns to respect)
- `.planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-CONTEXT.md` — rowId migration + chain.clear() fix that enabled this PR.
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-CONTEXT.md` — retro-fallback removal, FillRecord dispatch order established.

### Production files touched by Phase 14
- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (Plan 01: M-1, M-3, M-9, Mn-13)
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` (Plan 02: M-2, M-8)
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` (Plan 03: M-4 — `createToUtcCall` already at :2725)
- `core/src/main/java/io/questdb/griffin/engine/orderby/SortedRecordCursorFactory.java` (Plan 04: M-7)
- `core/src/main/java/io/questdb/cairo/sql/Record.java` (M-8 audit reference — every `default throw UnsupportedOperationException` getter)
- `core/src/main/java/io/questdb/cairo/ColumnType.java` (M-9 probe for `isSameType`/`equalsExactly` helper; `isDecimal`, `isGeoHash`, `tagOf`, `ARRAY` constant)

### Test files touched by Phase 14
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (per-type FILL(PREV), Decimal zero-vs-null, M-1/M-3/M-4/M-8/M-9 regressions)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByTest.java` (ghost-test cleanup)
- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByNanoTimestampTest.java` (ghost-test cleanup, nano twin)
- `core/src/test/java/io/questdb/test/griffin/RecordCursorMemoryUsageTest.java` (factory-type assertion restore)

### PR reference
- PR #6946: https://github.com/questdb/questdb/pull/6946 (description needs M-5/M-6 Trade-offs bullets added as part of this phase)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `SqlOptimiser.createToUtcCall(value, timezone)` at `SqlOptimiser.java:2725` — already in use at `:8308` (GROUP BY), `:8462`, `:8465`. Directly reusable for D-08.
- `factoryColToUserFillIdx` pattern at `SqlCodeGenerator.java:3426-3461` — pre-existing alias-mapping machinery to fold into the bare-FILL(PREV) branch for D-05.
- `ColumnType.isDecimal(type)`, `ColumnType.isGeoHash(type)`, `ColumnType.ARRAY` constant — building blocks for D-12. Need to probe for an existing `isSameType`/`equalsExactly` helper before writing one inline.
- `Misc.freeObjList(ObjList)` — null-safe, per-element close. Used throughout. Directly reusable for D-13.

### Established Patterns
- **FillRecord dispatch order** (Phase 13, preserved): `FILL_KEY` → cross-column-PREV-to-key (mode >= 0 && outputColToKeyPos[mode] >= 0) → `FILL_PREV_SELF` or cross-column-PREV-to-aggregate → `FILL_CONSTANT` → default null. M-2 and M-8 must preserve this exact order.
- **Ownership transfer** (`fillValues.setQuick(fillIdx, null)` at `:3555`): existing pattern for moving a Function from `fillValues` to `constantFillFuncs`. Mn-13 builds on this by freeing residuals that were never transferred.
- **Zero-GC getters**: `fillMode(col)` via `IntList.getQuick` (primitive), `outputColToKeyPos[col]` (int[]), `constantFills.getQuick(col)` (ObjList.getQuick, cached ref). Preserved in new getters.

### Integration Points
- `generateFill` at `SqlCodeGenerator.java:3244-3700` — extension points for D-05, D-07, D-12, D-13.
- `rewriteSampleBy` at `SqlOptimiser.java:~8048-8350` — extension point for D-08.
- `SampleByFillRecordCursorFactory` constructor and `FillRecord` inner class — extension points for D-06, D-11.

</code_context>

<specifics>
## Specific Ideas

### M-1 regression test construction
Build a query shape that triggers `propagateTopDownColumns0` to reorder the inner sample-by model. Shape: `SELECT a + b, k FROM (SELECT k, sum(x) a, sum(y) b FROM t SAMPLE BY 1h FILL(PREV))`. Outer expression `a + b` walks DFS; verify the inner model's column order differs from user-SELECT order by printing the factory metadata (or asserting via plan-text). Then assert the fill output produces the expected per-column behavior: `k` key-carried, `a`/`b` self-prev'd.

### M-4 Case A/B/C concrete numbers (for test authoring)
- **Case A (empty base)**: `SELECT ts, sum(x) FROM t WHERE sym='never_matches' SAMPLE BY 1h FROM '2024-06-01' TO '2024-06-02' TIME ZONE 'Europe/London' FILL(NULL)`. Expected: 24 fill rows at London `{00:00, 01:00, ..., 23:00}` = UTC `{2024-05-31T23:00Z, ..., 2024-06-01T22:00Z}`. Pre-fix: 24 rows at UTC `{2024-06-01T00:00Z, ..., 2024-06-01T23:00Z}` = London `{01:00, ..., 2024-06-02T00:00}`.
- **Case B (sparse)**: data at London 03:00 only; expected 3 leading fills at London `{00:00, 01:00, 02:00}`, then data at `03:00`. Pre-fix: 2 leading fills at London `{01:00, 02:00}`, then data at `03:00` — missing `00:00` bucket.
- **Case C (dense)**: correctness already holds (masked by `firstTs` anchor). Still write the test to lock in the behavior.

### M-7 trigger approach
Option A: inject a `CairoConfiguration` with `sqlSortKeyMaxPages = 0` or similar capped value to force `new RecordTreeChain` to throw on allocation. Option B: use a test-double wrapper around `RecordTreeChain` that throws on the first constructor call. Prefer Option A if a small config tweak is sufficient.

### M-9 test types
`DECIMAL64(10,2)` target, `DECIMAL64(18,6)` source; `DOUBLE[]` target, `DOUBLE[][]` source; `GEOHASH(18b)` target, `GEOHASH(25b)` source. Each rejects with a message containing both type names via `ColumnType.nameOf`.

### M-5/M-6 wording draft placement
The existing Trade-offs section in PR #6946 body has 4 bullets (random-access=false, OrderedMap sizing, FILL(LINEAR) out of scope, unsupported-PREV-type fallback). Add two bullets there:
- Memory envelope: "The forced SortedRecordCursor wrap buffers the full GROUP BY output in native memory — O(K keys × B buckets). At 10K keys × 100K buckets that is 10⁹ rows, which will OOM. Workload envelope is bounded by the K × B product; higher-cardinality keyed fills stay on the cursor path as before."
- Scan multiplier: "Keyed fill walks the sorted output three times (chain build + pass-1 key discovery + pass-2 emission) versus master's single-pass non-keyed fill. The chain-read is O(1) per row, but on aggregation-cheap workloads the constant factor is visible. No JMH benchmark coverage today; empirical stress runs (37 fresh-JVM runs of the SAMPLE BY trio) were correctness-only."

</specifics>

<deferred>
## Deferred Ideas

- **M-5 architectural fix** — merge-sort wrapper that consumes the timestamp-ordered GROUP BY output in streaming fashion, or a circuit-breaker size estimate with cursor-path fallback. Belongs in a future phase after benchmark coverage exists.
- **M-6 architectural fix** — populating `keysMap` during `SortedRecordCursor.buildChain` via a callback to eliminate one pass. Cross-cutting change; future phase.
- **JMH benchmark harness for SAMPLE BY FILL** — no existing coverage. `SampleByFillQueryBenchmark` for shapes 1K×10K, 10K×10K, 100×100K would give the codebase a real quality bar for M-5/M-6 follow-ups. Future phase.
- **Tighten `testFillPrevRejectNoArg` fuzzy 4-message assertion** — Mn-11 item, descoped from Phase 14 (low value, orthogonal to the moderate findings).
- **Mn-1..Mn-10, Mn-12 cleanups** — dead import, redundant conjuncts, unreachable branches, planning-doc comment leaks, `isKeyed` dead variable, `keyColIndices` dead field, `hasPrevFill` overly-broad flag, sink-getter stale-state, empty-keyed-FROM-TO silent-zero release note, commit-hygiene squash of Phase 12/13 history. Handled as a separate cleanup pass or case-by-case during future work.

</deferred>

---

*Phase: 14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-*
*Context gathered: 2026-04-20*
