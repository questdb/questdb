# Phase 16 Context: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys

**Gathered:** 2026-04-21
**Status:** Ready for planning
**Source:** Phase 15 defensive-assertion experiment + `.planning/todos/pending/2026-04-21-fix-multi-key-fill-prev-with-inline-function-grouping-keys.md`.

<domain>
## Phase Boundary

Close the latent correctness gap in `SqlCodeGenerator.generateFill`'s grouping-key classifier at `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3405-3436` where non-aggregate FUNCTION grouping keys (`interval(lo, hi)`, `concat(a, b)`, `cast(x AS STRING)`) and OPERATION grouping keys (`a || b`, `a + b`) fall through into the aggregate arm and get dispatched as `FILL_PREV_SELF` / `FILL_CONSTANT` instead of `FILL_KEY`. Multi-key `SAMPLE BY ... FILL(PREV)` silently drops cartesian fill rows as a result. Single-key fixture `SampleByFillTest.testFillPrevInterval:1678` hides the bug because FILL_PREV_SELF reads the same value FILL_KEY would when there is only one distinct key.

**In scope:**
- Classifier fix in `SqlCodeGenerator.generateFill` only — no optimizer changes.
- Regression suite pinning the multi-key cartesian contract for inline-function / operator grouping keys (FILL(PREV) × 4 variants + FILL(NULL) × 1 representative).
- Defensive `-ea` assertion locking the post-fix three-way partition (LITERAL key / `timestamp_floor` / aggregate FUNCTION) — lands in the same commit as the classifier fix.
- Verification that cursor-side wiring (`keyColIndices` / `outputColToKeyPos[col]`) already dispatches FILL_KEY through `keysMapRecord` for function-key factory indexes; small adjustment only if the verification turns up a gap.

**Out of scope:**
- Upstream canonicalization in `SqlOptimiser.rewriteSampleBy` (Option 2). Rejected after confirming non-FILL SAMPLE BY with inline-function keys has no known latent bug on the same classifier path — the GROUP BY engine handles function-valued keys natively, the classifier is the only place that mis-partitions them, and the classifier only runs for FILL. Optimizer-side lift would change plan text across every fast-path SAMPLE BY with a non-literal key without fixing an additional observable bug.
- FILL(constant) multi-key inline-function regression test. Current failure mode is a SqlException from `aggNonKeyCount` mismatch; the classifier fix restores correctness by default and the defensive assertion flags any future drift.
- Reopening `SampleByFillTest.testFillPrevInterval` single-key fixture — continues to pass unchanged (criterion #4).

</domain>

<decisions>
## Implementation Decisions

### Fix approach (D-01)

- **D-01** — Option 1: classifier fix local to `SqlCodeGenerator.generateFill`. Narrow, reversible, consistent with Phase 12–15 codegen-side bias. Option 2 (upstream canonicalization in `SqlOptimiser.rewriteSampleBy`) rejected: it would restructure every fast-path SAMPLE BY with a non-literal key (fill or no-fill), changing plan text across SqlOptimiserTest / ExplainPlanTest / SampleByTest / SampleByNanoTimestampTest expectations, without fixing any additional observable bug — the classifier is the only place that mis-partitions non-aggregate FUNCTION/OPERATION grouping keys, and the classifier only runs for FILL. Option 2 captured in Deferred Ideas as a possible future hardening phase if similar drift recurs.

### Classifier widening (D-02..D-03)

- **D-02** — Insert a third `continue` branch in the classifier loop at `SqlCodeGenerator.java:3405-3436`, between the `isTimestampFloorFunction` continue (`:3418-3420`) and the aggregate fall-through. Shape:

  ```java
  if ((ast.type == ExpressionNode.FUNCTION || ast.type == ExpressionNode.OPERATION)
          && !functionParser.getFunctionFactoryCache().isGroupBy(ast.token)) {
      assert qc.getAlias() != null
              && groupByMetadata.getColumnIndexQuiet(qc.getAlias()) >= 0
              && groupByMetadata.getColumnIndexQuiet(qc.getAlias()) != timestampIndex
          : "generateFill: non-aggregate FUNCTION/OPERATION in bottomUpCols must resolve to a non-timestamp factory key";
      continue;
  }
  ```

  Rationale: `functionParser.getFunctionFactoryCache().isGroupBy(token)` is the canonical FUNCTION-vs-aggregate predicate used ~20 sites in `SqlOptimiser.java`; negated it safely admits grouping-key functions. The `-ea` alias assert is the belt-and-braces positive check that the resolved alias is in the factory key set. `functionParser` is already a field in `SqlCodeGenerator` (used at `:3275, :3346, :3351` inside `generateFill` itself).

- **D-03** — Cover both FUNCTION and OPERATION. `rewriteSampleBy` at `SqlOptimiser.java:8066-8069` admits LITERAL, FUNCTION, and OPERATION into `maybeKeyed`, so OPERATION grouping keys reach `bottomUpCols` the same way. Regression suite includes one OPERATION variant (`a || b`).

### Regression coverage (D-04)

- **D-04** — New multi-key cartesian-contract tests in `SampleByFillTest`, alphabetically placed per CLAUDE.md member ordering:
  - `testFillPrevIntervalMultiKey` — two distinct `interval(lo, hi)` keys, three buckets, assert 6-row cartesian FILL(PREV) output (matches empirical probe in todo).
  - `testFillPrevConcatMultiKey` — two distinct `concat(a, b)` keys, FILL(PREV), cartesian assertion.
  - `testFillPrevCastMultiKey` — two distinct `cast(x AS STRING)` keys, FILL(PREV), cartesian assertion.
  - `testFillPrevConcatOperatorMultiKey` — two distinct `a || b` (OPERATION) keys, FILL(PREV), cartesian assertion.
  - `testFillNullCastMultiKey` — representative FILL(NULL) multi-key test using `cast(x AS STRING)`. Pins that the classifier fix holds independent of fill mode.

  All tests use `assertQueryNoLeakCheck(..., false, false)` per Phase 14 D-15 (SAMPLE BY FILL factory contract). No FILL(constant) variant — see Out of scope.

### Defensive assertion (D-05)

- **D-05** — Land invariant (I) in the same commit as the classifier fix: at the aggregate-arm entry (immediately before `userFillIdx++` at `SqlCodeGenerator.java:3435`), assert:

  ```java
  assert ast.type == ExpressionNode.FUNCTION
          && functionParser.getFunctionFactoryCache().isGroupBy(ast.token)
      : "generateFill aggregate arm: expected aggregate FUNCTION, got type=" + ast.type + " token=" + ast.token;
  ```

  Locks the post-fix three-way partition (LITERAL key / timestamp_floor / aggregate FUNCTION). QuestDB surefire runs `-ea` per `core/pom.xml:37`, so CI fails immediately if a future AST shape falls through to the aggregate arm without being a genuine aggregate. Phase 15-style same-commit landing (no audit drift from separate lock-in commit).

### Cursor-side wiring verification (D-06)

- **D-06** — Before writing tests, confirm `keyColIndices` passed into `SampleByFillCursor` at `SampleByFillRecordCursorFactory` construction already includes the function-key factory index when the classifier is fixed, so `outputColToKeyPos[col] >= 0` and `FillRecord` dispatches FILL_KEY through `keysMapRecord` for the function-key column. If the verification turns up a gap (e.g., `keyColIndices` is computed from a list that excluded the misclassified column), the small cursor-side adjustment lands in the same plan. Not a separate plan. Research/verify step only — no decision needed from user.

### Claude's Discretion

- Exact test fixtures inside `SampleByFillTest` (table DDL, specific interval / concat / cast / `||` values). Planner picks the smallest diff that produces the 6-row cartesian shape matching the empirical probe in the todo.
- Whether the `-ea` alias assert lives directly inside the new `continue` branch (D-02) or just outside the predicate (pre-`continue`). Cleanliness call.
- Plan count: single plan preferred (classifier fix + assertion + regression tests in one commit per Phase 14/15 D-01 file-clustered convention). Planner may split if the diff exceeds typical plan size, but default is 1 plan.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project conventions
- `CLAUDE.md` — ASCII-only log messages, `is/has` boolean naming, members sorted alphabetically (no banner comments), zero-GC data path, `assertMemoryLeak` / `assertQueryNoLeakCheck` test helpers, commit hygiene (no Conventional Commits prefix, <50 char titles, long-form body).

### Prior phase CONTEXTs (patterns to respect)
- `.planning/phases/15-address-pr-6946-review-findings-and-retro-fixes/15-CONTEXT.md` — Phase 15 D-01 (file-clustered plans), D-02 (commit hygiene), codegen-side bias for fast-path fixes; mentions the defensive-assertion experiment that surfaced this bug.
- `.planning/phases/14-fix-issues-from-moderate-list-for-m5-and-m6-just-mention-in-/14-CONTEXT.md` — D-15 (`assertQueryNoLeakCheck(..., false, false)` default for SAMPLE BY FILL), `factoryColToUserFillIdx` rationale for outer-projection reorder coordination.
- `.planning/phases/13-migrate-fill-prev-snapshots-from-materialized-values-to-rowi/13-CONTEXT.md` — FillRecord 4-branch dispatch order (FILL_KEY → cross-col-PREV-to-key → FILL_PREV_SELF/cross-col-PREV-to-agg → FILL_CONSTANT → null). Unchanged by Phase 16.
- `.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-CONTEXT.md` — retro-fallback mechanism and Tier 1 gate; orients the "fast-path must stay narrow" stance.

### Source of this phase
- `.planning/todos/pending/2026-04-21-fix-multi-key-fill-prev-with-inline-function-grouping-keys.md` — full bug trace, empirical probe (3 rows observed vs 6 expected), both fix options, the draft defensive assertion. Captures context that is NOT duplicated in ROADMAP.md.

### Production file hooks

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`
  - `:3405-3436` — classifier loop. D-02 widens the third arm here.
  - `:3414-3420` — existing `LITERAL` / `isTimestampFloorFunction` continue branches; D-02's new branch slots between these and the aggregate fall-through.
  - `:3421-3436` — aggregate arm. D-05 assertion lands at the entry (immediately before `userFillIdx++` at `:3435`).
  - `:3447-3465` — bare-FILL(PREV) dispatch. Reads `factoryColToUserFillIdx` to decide FILL_KEY vs FILL_PREV_SELF — the visible symptom of the classifier bug.
  - `:3466+` — per-column fill dispatch. Reads `factoryColToUserFillIdx` the same way (FILL(NULL) symptom).
  - `:3275, :3346, :3351` — existing `functionParser.parseFunction(...)` call sites inside `generateFill`; confirm `functionParser` is accessible in this scope.

- `core/src/main/java/io/questdb/griffin/FunctionFactoryCache.java:135` — `isGroupBy(CharSequence name)`. Canonical FUNCTION-vs-aggregate predicate. Used by D-02 and D-05.

- `core/src/main/java/io/questdb/griffin/SqlUtil.java:1256` — `isTimestampFloorFunction(ExpressionNode ast)`. Existing predicate the classifier uses at `:3418`; new predicate in D-02 composes the same way.

- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:8066-8069` — `maybeKeyed.add(ast)` admits LITERAL, FUNCTION, OPERATION into the rewritten SAMPLE BY model. Grounds D-03 (OPERATION must be covered symmetrically with FUNCTION in the classifier).

- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` — cursor-side wiring verified under D-06:
  - `keyColIndices` field / constructor wiring — must include the function-key factory column index post-fix.
  - `outputColToKeyPos[col] >= 0` predicate — gates FILL_KEY dispatch in `FillRecord`.
  - `FillRecord` 4-branch dispatch (Phase 13) — `keysMapRecord` read path for key columns.

- `core/pom.xml:37` — surefire `-ea` flag. Grounds D-05's confidence that the defensive assertion fires in CI.

### Test file hooks

- `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java`
  - `:1678` — `testFillPrevInterval` single-key fixture that currently hides the bug. Remains unchanged (criterion #4).
  - New `testFillPrevIntervalMultiKey`, `testFillPrevConcatMultiKey`, `testFillPrevCastMultiKey`, `testFillPrevConcatOperatorMultiKey`, `testFillNullCastMultiKey` — land alphabetically per CLAUDE.md.

### Empirical probe (from todo)

Query:
```sql
SELECT ts, interval(lo, hi) k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR
```
Data: key A = `('2020-01-01Z','2020-02-01Z')` at bucket 00:00; key B = `('2021-01-01Z','2021-02-01Z')` at bucket 02:00.

Fast-path output observed (3 rows, non-cartesian — BUG):
```
2024-01-01T00:00:00Z  A  10.0
2024-01-01T01:00:00Z  A  10.0
2024-01-01T02:00:00Z  B  20.0
```

Expected output (6 rows, cartesian):
```
2024-01-01T00:00:00Z  A  10.0   (data)
2024-01-01T00:00:00Z  B  <no-prev>
2024-01-01T01:00:00Z  A  10.0   (A's prev)
2024-01-01T01:00:00Z  B  <no-prev>
2024-01-01T02:00:00Z  A  10.0   (A's prev)
2024-01-01T02:00:00Z  B  20.0   (data)
```

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `functionParser.getFunctionFactoryCache().isGroupBy(token)` — canonical FUNCTION-vs-aggregate predicate. 20+ call sites in `SqlOptimiser.java`. Zero allocation, zero state.
- `groupByMetadata.getColumnIndexQuiet(alias)` — alias-to-factory-index lookup. Already used at `SqlCodeGenerator.java:3430` to populate `factoryColToUserFillIdx`. Reusable in D-02's assert.
- `SampleByFillTest` harness — existing multi-key FILL(PREV) regression patterns (Phase 3, 11) serve as a template for the new cartesian-contract tests.

### Established Patterns

- **Three-way classifier partition** — the loop at `SqlCodeGenerator.java:3405-3436` conceptually partitions `bottomUpCols` into {LITERAL key, `timestamp_floor` bucket, aggregate}. D-02 adds a fourth explicit branch (non-aggregate FUNCTION/OPERATION key) and D-05 locks the residual aggregate arm via `-ea` assert.
- **Codegen-side surgical fixes on fast path** (Phase 12–15) — keep optimizer untouched, fix the narrow symptom where the misclassification happens.
- **`-ea` defensive assertions for invariant lock-in** (Phase 15's `SampleByFillCursor.hasNext()` `dataTs < nextBucketTimestamp` assert at `SampleByFillRecordCursorFactory.java`) — same landing pattern (same-commit, under -ea).
- **`factoryColToUserFillIdx` alias-based mapping** (Phase 14 D-15) — the existing mechanism that keeps fill-value assignment correct under outer-projection reorder. D-02's fix keys on the same alias path; D-04 regression tests exercise it under multi-key inline-function grouping.

### Integration Points

- Inside `SqlCodeGenerator.generateFill` classifier loop — isolated 3-line insertion (new `continue` branch) + 1-line assertion at aggregate-arm entry.
- `SampleByFillTest` new test methods — alphabetically placed, each ~30-40 lines following existing FILL multi-key test shape.
- Cursor side (`SampleByFillRecordCursorFactory` keyColIndices wiring) — verify-only under D-06; adjustment lands in the same plan if needed.

</code_context>

<specifics>
## Specific Ideas

- The regression suite must match the empirical probe in the todo: 3 buckets × 2 distinct keys → 6 cartesian rows. The probe uses `interval((ts), (ts))` values; planner picks equivalent minimal-diff fixtures for `concat`, `cast`, and `||` variants.
- The D-05 assertion message must use plain ASCII (per CLAUDE.md log/error message rule), matching the style of the draft assertion in the todo (plain ASCII punctuation, no em dashes or curly quotes).
- `aggNonKeyCount` (computed at `SqlCodeGenerator.java:3445` from `userFillIdx`) is expected to DECREASE post-fix for queries with inline-function grouping keys (those keys no longer count toward aggregates). Verify no downstream fill-spec-count validation assumes the pre-fix value. The `fillValuesExprs.size() < aggNonKeyCount` check at `:3479` is the obvious downstream consumer — post-fix it should accept the tightened count correctly.

</specifics>

<deferred>
## Deferred Ideas

- **Option 2 (upstream canonicalization in `SqlOptimiser.rewriteSampleBy`)** — lift inline-function grouping expressions into virtual column projections so `bottomUpCols` only ever sees LITERAL references. Rejected for Phase 16 (no known additional bug fixed, plan-text ripple across every fast-path SAMPLE BY with non-literal key). Keep as a future hardening candidate if the classifier drifts again despite D-05. File as a standalone phase if pursued.
- **FILL(constant) multi-key inline-function regression test** — the classifier fix restores correctness by default, and the defensive assertion (D-05) would fire on drift before a user ever saw it. Add ad-hoc if a real-world FILL(const) report surfaces.

</deferred>

---

*Phase: 16-fix-multi-key-fill-prev-with-inline-function-grouping-keys*
*Context gathered: 2026-04-21*
