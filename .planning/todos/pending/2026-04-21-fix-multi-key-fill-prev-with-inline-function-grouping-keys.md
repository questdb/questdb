---
created: 2026-04-21T15:33:00.103Z
title: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys
area: sql
files:
  - core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java:3406-3435
  - core/src/main/java/io/questdb/griffin/SqlOptimiser.java (rewriteSampleBy — upstream canonicalization option)
  - core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java (FillRecord.getInterval, keyed emit path)
  - core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java:1674-1707 (testFillPrevInterval — single-key fixture that hides the bug)
---

## Problem

Multi-key `SAMPLE BY ... FILL(PREV)` silently drops cartesian fill rows when the GROUP BY key is an inline non-aggregate FUNCTION (e.g., `interval(lo, hi) k`, `concat(a, b) k`, `cast(x AS STRING) k`). The fast path falls back to non-keyed passthrough semantics instead of emitting the per-key cartesian product the legacy cursor path produces.

**Root cause.** The classifier loop in `SqlCodeGenerator.generateFill` around line 3406-3435 walks `bottomUpCols` and classifies each QueryColumn into:

1. LITERAL key — `continue`
2. `timestamp_floor` FUNCTION — `continue`
3. Everything else — treat as aggregate, record `factoryColToUserFillIdx[alias] = userFillIdx`, increment `userFillIdx`.

Case 3 silently swallows non-aggregate FUNCTION nodes. For `interval(lo, hi) k`:

- `k` gets `factoryColToUserFillIdx[k_factory_idx] = 0` (user fill slot 0).
- `aggNonKeyCount` drifts to 2 (`k` + `first(v)` instead of just `first(v)`).
- In the bare-FILL(PREV) dispatch at `SqlCodeGenerator.java:3454-3465`, `k` gets classified as FILL_PREV_SELF instead of FILL_KEY.
- The keyed cartesian emission path can't honor `k` as a key because the fill-mode wiring never marks it as one.

**Empirical evidence (probed 2026-04-21 during Phase 15):**

Query: `SELECT ts, interval(lo, hi) k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR`
Data: key A (`('2020-01-01Z','2020-02-01Z')`) at bucket 00:00; key B (`('2021-01-01Z','2021-02-01Z')`) at bucket 02:00.

Fast-path output (3 rows, NOT cartesian):
```
2024-01-01T00:00:00Z  A  10.0   (data)
2024-01-01T01:00:00Z  A  10.0   (fill: carries previous row forward)
2024-01-01T02:00:00Z  B  20.0   (data)
```

Expected output (6 rows = 2 keys x 3 buckets, cartesian):
```
2024-01-01T00:00:00Z  A  10.0   (data)
2024-01-01T00:00:00Z  B  <no-prev>   (or omitted; leading fill)
2024-01-01T01:00:00Z  A  10.0   (fill: A's prev)
2024-01-01T01:00:00Z  B  <no-prev>   (fill: B has no prev yet)
2024-01-01T02:00:00Z  A  10.0   (fill: A's prev carry forward)
2024-01-01T02:00:00Z  B  20.0   (data)
```

**Why `SampleByFillTest#testFillPrevInterval` (Phase 14) does not catch this:**

The test uses identical `interval(lo, hi)` values in both data rows (single unique key). With one key, cartesian vs non-cartesian produce the same 3-row output, and FILL_PREV_SELF on the key column reads the same interval value from prevRecord that FILL_KEY would read from keysMap. No observable drift. The test pins that `FillRecord.getInterval` compiles and doesn't throw UnsupportedOperationException — it does NOT prove multi-key keyed semantics hold for FUNCTION-valued grouping keys.

**Diagnostic.** A defensive assertion of the form:

```java
assert ast.type != ExpressionNode.OPERATION
        && (ast.type != ExpressionNode.FUNCTION
                || functionParser.getFunctionFactoryCache().isGroupBy(ast.token))
    : "generateFill expected canonicalized bottomUpCols (LITERAL | timestamp_floor | aggregate FUNCTION), got type=" + ast.type + " token=" + ast.token;
```

placed between the two `continue` statements and `userFillIdx++` fires on `testFillPrevInterval` immediately under `-ea` (QuestDB's surefire runs with `-ea` per `core/pom.xml:37`). This is a useful lock-in AFTER the bug is fixed — it cannot land as-is because the existing test would fail.

## Solution

Two fix options; both need a regression test with two distinct inline-function keys asserting 6-row cartesian output.

**Option 1 — Classifier fix (local to `SqlCodeGenerator.generateFill`).**
Widen the third arm to detect non-aggregate FUNCTION nodes whose alias resolves to a factory key column:
- If `ast.type == FUNCTION`, `isGroupBy(ast.token) == false`, `qc.getAlias() != null`, and `groupByMetadata.getColumnIndexQuiet(qc.getAlias())` points at a column present in the effective key set (not `timestampIndex`, not an aggregate output), classify as a key column (`continue`, treat like LITERAL).
- Otherwise keep the aggregate fallthrough.

Verify cursor-side wiring: ensure `keyColIndices` (passed to `SampleByFillCursor`) includes the function-key factory index so `outputColToKeyPos[k_col] >= 0` and FillRecord dispatches FILL_KEY through keysMapRecord. If it does not, the cursor needs a small adjustment there too.

Land the defensive assertion as a final hardening touch once the classifier is correct.

**Option 2 — Upstream canonicalization (in `SqlOptimiser.rewriteSampleBy` or column propagation).**
Lift inline-function grouping expressions into a virtual column projection so `bottomUpCols` only ever sees LITERAL references. Broader scope but removes the need for any generateFill-side detection. Cleaner long-term but riskier — touches optimizer semantics that impact non-FILL queries too.

**Coverage to add regardless of option:**
- Multi-key `interval(lo, hi)` FILL(PREV) assertion test (cartesian 6-row output).
- Same shape for `concat(a, b)` grouping key.
- Same shape for `cast(x AS STRING)` grouping key.
- FILL(NULL) and FILL(constant) equivalents — the same classifier path feeds the per-column fill loop, so constant/null fills on function-keyed SAMPLE BY may exhibit related drift.

**Deferred alongside fix (per CONTEXT.md style):** land the defensive assertion as a follow-up commit in the same phase to prevent future drift of this invariant class.

**Suggested phase title:** "Fix multi-key FILL(PREV) with inline FUNCTION grouping keys (interval/concat/cast)".

**Scope estimate:** Option 1 is 1-2 plans (classifier fix + regression tests + assertion). Option 2 is 3-4 plans (optimizer change + regression tests + ripple verification across non-FILL SAMPLE BY tests).

**Source:** Surfaced 2026-04-21 during Phase 15 defensive-assertion experiment. See Phase 15 conversation for full trace and probe test.
