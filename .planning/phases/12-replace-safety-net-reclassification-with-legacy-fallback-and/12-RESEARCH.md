# Phase 12: Replace safety-net reclassification with legacy fallback and tighten optimizer PREV gate — Research

**Researched:** 2026-04-16
**Domain:** QuestDB SQL optimizer + codegen for `SAMPLE BY FILL(PREV)` on the GROUP BY fast path
**Confidence:** HIGH

## Summary

Phase 12 is a narrow, surgical correctness fix inside an already-shipped fast path. The work splits into four
independent but intertwined areas: (1) replacing the codegen safety-net at
`SqlCodeGenerator.java:3497-3512` with a retro-fallback mechanism that re-dispatches unsupported queries to the
legacy SAMPLE BY cursor, (2) a minimal Tier-1 tightening of the optimizer gate (`SqlOptimiser.java:474-559`)
that closes the cross-column and LONG128/INTERVAL holes, (3) six new grammar rules for `FILL(PREV, PREV(...))`
that reject nonsense shapes with positioned `SqlException`s, and (4) a cluster of code-quality and
test-coverage items for paths that phase 11 added without regression tests.

The retro-fallback is a stash-and-restore: `rewriteSampleBy` saves the original `sampleByNode` onto a new
`QueryModel.stashedSampleByNode` field before clearing it; `generateFill` checks fully-resolved
`groupByFactory.getMetadata()` against `isFastPathPrevSupportedType`; on mismatch it throws a
`FallbackToLegacyException` which the three `generateFill` call sites in `generateSelectGroupBy` catch,
restore the model's SAMPLE BY state from the stash, and dispatch to `generateSampleBy` directly. The
optimizer gate stays as a performance optimization that avoids building-then-discarding a groupBy factory
for the common rejected cases; correctness is now carried entirely by the codegen check plus retro-fallback.

**Primary recommendation:** Implement retro-fallback as an explicit `FallbackToLegacyException` (a private
subclass of `SqlException` or a sibling runtime exception scoped to this use). Rationale: all three
`generateFill` call sites share the same catch pattern, and the existing `generateFill` try/catch at
`SqlCodeGenerator.java:3624-3631` already frees `fillValues`, `constantFillFuncs`, `fillFromFunc`,
`fillToFunc`, and `groupByFactory` — exactly the resources that must be reclaimed on fallback. A
return-value sentinel requires threading a state-object through three identical branches and offers no
leak-safety advantage. Exception-for-control-flow is justified here because the path is rare (unsupported
types + reachable at codegen) and the alternative is more code.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Design intent (CONTEXT.md "Design intent" section):**
- Both design docs state fallback-to-legacy is the documented behavior. No fail-loud `SqlException` for
  queries that master handles correctly.
- The optimizer gate is an optimization (avoids wasted GROUP BY work); the codegen check + retro-fallback
  are correctness.

**Mechanism (CONTEXT.md "Mechanism: retro-fallback"):**
- **D-01:** Retro-fallback via stashed `sampleByNode` + try/catch in `generateSelectGroupBy`. No fail-loud.
  Preserves no-regression invariant.
- **D-02:** Optimizer gate is Tier 1 only (cross-col resolution + LONG128/INTERVAL). No Tier 2 aggregate-
  rule inference, no Tier 3 expression walker. Retro-fallback covers residue.
- **D-03:** Codegen-time unsupported-type check uses `groupByFactory.getMetadata().getColumnType(col)` —
  fully resolved. No aggregate-rule inference at codegen either.
- Stash field lives on `QueryModel` (pattern mirrors `fillOffset`); save before the `nested.setSampleBy(null)`
  at `SqlOptimiser.java:8438-8441`; restore before calling `generateSampleBy(model, executionContext,
  stashedSampleByNode, model.getSampleByUnit())` in the catch.

**Tier 1 gate changes (CONTEXT.md "Optimizer gate scope — Tier 1 only"):**
1. Remove the skip at `SqlOptimiser.java:521-525`. Resolve `PREV(alias)` against `model.getAliasToColumnMap()`
   and `nested.getAliasToColumnMap()`, check source column type against `isUnsupportedPrevType`. Block if
   unsupported. If the alias can't be resolved (expression-arg source), pass through — fallback catches it.
2. Add `ColumnType.LONG128` and `ColumnType.INTERVAL` to `isUnsupportedPrevType` at
   `SqlOptimiser.java:552-559`.

**Grammar (CONTEXT.md "Grammar resolution for FILL(PREV, PREV(...))"):**
- **D-04:** Bare `PREV` inside per-column list means self-prev for that slot. Keep and document.
- **D-05:** `PREV(ts)` — reject at compile time with positioned error at `rhs.position`.
- **D-06:** `PREV(self)` — alias internally to `FILL_PREV_SELF` (not rejection, internal normalization).
- **D-07:** Chain rejection — reject iff `fillModes[col] >= 0 AND fillModes[fillModes[col]] >= 0`. Simplest
  ~6-line variant with generic FILL-clause position OR the precise-position variant (Claude's discretion).
- **D-08:** Malformed PREV shapes (`paramCount != 1`, non-LITERAL rhs, bind variable) — single unified
  rejection at `fillExpr.position` with message `"PREV argument must be a single column name"`.
- **D-09:** Type-tag mismatch between `PREV(src)` and target — reject at compile time with positioned
  error.

**Test conversion (CONTEXT.md):**
- **D-10:** `assertSql` → `assertQueryNoLeakCheck` conversion rule: "if the test exercises the new fast-path
  fill cursor and `supportsRandomAccess=false` / `size()=-1` are the correct expected values, convert."
  Skip legacy-plan-assertion tests, mid-test-DDL tests that explicitly need `assertSql`, and cairo storage
  tests.

**Defensive fixes explicitly in scope:**
- `hasExplicitTo` guard for `maxTimestamp == LONG_NULL` at `SampleByFillRecordCursorFactory.java:523-526`
  (add one line after 525).
- `testSampleByFillNeedFix` expected output restored to master's 3-row form.
- Remove safety-net block at `SqlCodeGenerator.java:3497-3512`.
- Remove redundant `anyPrev` loop at `SqlCodeGenerator.java:3392-3398`.
- Assert or throw on `Dates.parseOffset` failure at `SqlCodeGenerator.java:3379`.

**Code-quality items explicitly in scope:** alphabetize FillRecord getters, SampleByFillCursor private
members, imports in both SqlCodeGenerator.java and SampleByFillRecordCursorFactory.java; move `isKeyColumn`
next to `isFastPathPrevSupportedType`; replace FQN type refs with plain imports in
SampleByFillRecordCursorFactory.java; emit `fill=null|prev|value` unconditionally in `toPlan`.

**Test additions explicitly in scope:** `testFillKeyedUuid`, `testFillKeyedLong256`,
`testFillKeyedDecimal128`, `testFillKeyedDecimal256`, `testFillPrevGeoNoPrevYet`, eight grammar tests, five
retro-fallback tests, `testFillToNullTimestamp`.

### Claude's Discretion

- **Exception vs sentinel** for the fallback signal mechanism (both work; pick what's cleanest for the three
  call sites).
- **Error-message wording** for rejected grammar shapes (positioned `SqlException.$(position,
  "message").put(...)` pattern; exact wording at implementation time).
- **Chain rejection precision** — simplest ~6-line variant with generic FILL-clause position vs
  ~10-line variant that remembers `ExpressionNode` per column for precise error positioning. Both produce
  identical behavior.

### Deferred Ideas (OUT OF SCOPE)

- Extending the fast path to new types (STRING/VARCHAR/SYMBOL/LONG256/UUID aggregate PREV) — separate
  branch `sm_fill_prev_fast_all_types`.
- Numeric-widening conversion for `PREV(src)`/target type mismatches (INT→LONG, FLOAT→DOUBLE).
- Tier 2 aggregate-rule inference at the gate.
- Tier 3 full expression walker at the gate.
- `FILL(LINEAR)` on fast path.
- Old cursor path removal.
- `ALIGN TO FIRST OBSERVATION` on fast path.
- Two-token stride syntax on fast path.
- PR metadata items (title/body/`.planning/` diff noise) — tracked separately.
</user_constraints>

<phase_requirements>
## Phase Requirements

The CONTEXT.md explicitly states "no mapped IDs" — phase 12 was added retroactively after the PR #6946
scope-creep review surfaced the safety-net correctness bug. Phase 12 is covered by the existing
PTSF-02 / PTSF-04 / COR-04 requirements but does not introduce new ones.

| ID | Description (from REQUIREMENTS.md) | Research Support |
|----|------------------------------------|------------------|
| PTSF-02 | Explicit source type support matrix — numeric types on fast path, unsupported types fall back to legacy | Closed by Tier 1 gate changes (adding LONG128/INTERVAL, cross-col resolution) + retro-fallback for the residue |
| PTSF-04 | `prev(alias)` referencing unsupported type triggers legacy path fallback (plan shows Sample By, not Async Group By) | Now covered by Tier 1 resolution + retro-fallback; five new `testFillPrev*Fallback` tests assert the legacy plan is picked |
| COR-04 | Fill cursor output matches cursor-path output exactly for all fill modes | Retro-fallback restores parity for queries that currently produce duplicated fill rows (e.g. `testSampleByFillNeedFix`) |

**Implicit requirement introduced by this phase:** Grammar correctness — `FILL(PREV, PREV(...))` shapes
that have no well-defined semantics must reject at compile time with positioned errors (D-05 through
D-09). Eight new tests cover this.
</phase_requirements>

## Project Constraints (from CLAUDE.md)

Repository-level (`/Users/sminaev/qdbwt/CLAUDE.md`) and fork-level
(`/Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md`) both apply. Directly relevant to phase 12:

- **Alphabetical member ordering within kind/visibility.** Explicitly named targets: FillRecord getters,
  SampleByFillCursor private members, imports. Never insert `// ===` / `// ---` banners or category-style
  header comments — methods are auto-sorted and categories don't survive. `[VERIFIED: /Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md]`
- **Zero-GC on data paths.** The retro-fallback throw path runs at codegen time, not at query execution —
  acceptable to allocate (`new FallbackToLegacyException()`). But the grammar-rule enforcement in
  `generateFill` also runs at codegen, so allocating `SqlException` there is already the norm.
  `[VERIFIED: /Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md]`
- **Modern Java 17 features** (enhanced switch, multiline strings, `instanceof` pattern variables) —
  already used throughout `SqlCodeGenerator.java`. `isFastPathPrevSupportedType` at line 1199-1210 uses
  enhanced switch; mirror that for `isUnsupportedPrevType`. `[VERIFIED: SqlCodeGenerator.java:1199]`
- **Log messages ASCII only** — no em-dashes, curly quotes, Unicode. Error messages in `SqlException.$`
  must stay ASCII. `[VERIFIED: /Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md]`
- **Is/has prefix for booleans** — `isUnsupportedPrevType` / `isFastPathPrevSupportedType` already comply.
  New predicates like `isChainedPrev` must follow suit. `[VERIFIED: /Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md]`
- **Tests use `assertMemoryLeak`** and, for fast-path streaming queries, `assertQueryNoLeakCheck` (factory
  properties are part of the assertion). `[VERIFIED: /Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md]`
- **Active voice in commits/comments** — "The ring queue passes the factory to the exporter" not "The
  factory is passed through the ring queue". `[VERIFIED: /Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md]`
- **QuestDB runs with assertions enabled** — `assert parsed != LONG_NULL : "..."` at
  `SqlCodeGenerator.java:3379` is the preferred form per CONTEXT.md item 21. `[VERIFIED: review-pr skill SKILL.md:25-28]`

## Standard Stack

This is in-tree work — no new libraries. Everything builds on existing QuestDB infrastructure.

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `io.questdb.std.IntList` | in-tree | Fill-mode array, prevSourceCols, keyColIndices | Zero-GC primitive list, matches `fillModes`, `prevSourceCols` already in use |
| `io.questdb.std.ObjList<T>` | in-tree | `constantFillFuncs`, `fillValues` | QuestDB standard resizable object list; integrates with `Misc.freeObjList` |
| `io.questdb.std.Misc` | in-tree | `Misc.free`, `Misc.freeObjList` on throw paths | Standard cleanup helpers — already used at `SqlCodeGenerator.java:3624-3631` |
| `io.questdb.cairo.ColumnType` | in-tree | `tagOf`, `nameOf`, type constants (LONG128, INTERVAL) | `ColumnType.LONG128 = 24` (line 105), `ColumnType.INTERVAL = 39` (line 136) `[VERIFIED: ColumnType.java]` |
| `io.questdb.griffin.SqlException` | in-tree | Positioned compile-time errors | `SqlException.$(position, msg).put(...)` pattern — already used at `SqlCodeGenerator.java:3276, 3447, 7102` |
| `io.questdb.griffin.model.QueryModel` | in-tree | Gains `stashedSampleByNode` field | Parallels existing `fillOffset` field pattern at line 137 |
| `io.questdb.griffin.model.ExpressionNode` | in-tree | AST inspection in generateFill; `position`, `type`, `paramCount`, `rhs`, `token` | `BIND_VARIABLE = 3`, `LITERAL = 7`, `FUNCTION = 6` `[VERIFIED: ExpressionNode.java:43-56]` |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `io.questdb.std.Chars` | in-tree | `Chars.equalsIgnoreCase(a, b)` for alias matching in cross-col resolution | When comparing alias tokens to output/nested column names |
| `io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory` | in-tree | Fill-mode sentinels (`FILL_CONSTANT = -1`, `FILL_PREV_SELF = -2`, `FILL_KEY = -3`) | Reading/writing `fillModes` IntList |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `FallbackToLegacyException` | Return-value sentinel (null or a state tuple from `generateFill`) | Sentinel is "cleaner" (no exception-for-control-flow) but requires touching all three `generateFill` call sites with an identical wrap. Exception is a 5-line subclass and the existing try/catch at lines 3624-3631 already does the right thing. Fallback is rare (unsupported types + codegen-reachable) so the cost of stack unwinding is immaterial. |
| New `QueryModel.stashedSampleByNode` field | Use `nested.getSampleByFill()` as implicit marker | `sampleByFill` stays populated through the rewrite (nested.setFillValues propagates it). But it doesn't hold the original `sampleByNode` (the stride ExpressionNode needed by `generateSampleBy`). A dedicated stash field is clearer and matches the existing `fillOffset` pattern. `[VERIFIED: SqlOptimiser.java:8427-8441]` |
| Generic `Throwable` catch at call sites | Specific `FallbackToLegacyException` | Catching `Throwable` would swallow actual bugs (NPE, assertion failures). The whole point of the exception mechanism is a typed signal distinct from `SqlException`. |

**Installation:** Nothing to install. `SqlException` extends `Exception`; `FallbackToLegacyException` should
extend `SqlException` (to reuse `.position(...)` if needed) or `RuntimeException` (if we prefer to keep it
outside the checked-exception hierarchy). The plan picks — both work.

## Architecture Patterns

### Recommended Project Structure

No new directories. Changes are confined to:

```
core/src/main/java/io/questdb/griffin/
├── SqlCodeGenerator.java                 -- grammar, codegen check, retro-fallback catch, dead-code removal, import reorder
├── SqlOptimiser.java                     -- Tier 1 gate, stash in rewriteSampleBy
├── FallbackToLegacyException.java        -- NEW (or sentinel, plan decides)
├── engine/groupby/
│   └── SampleByFillRecordCursorFactory.java  -- hasExplicitTo guard, FillRecord getter alphabetization,
│                                              --   SampleByFillCursor member reorder, FQN→plain imports,
│                                              --   toPlan fill= always
└── model/
    └── QueryModel.java                   -- stashedSampleByNode field + getter/setter/clear

core/src/test/java/io/questdb/test/griffin/
├── engine/groupby/
│   ├── SampleByFillTest.java             -- ~19 new tests (5 retro-fallback + 5 FILL_KEY + 8 grammar + 1 TO null)
│   │                                       -- ~15 assertSql → assertQueryNoLeakCheck conversions
│   ├── SampleByTest.java                 -- testSampleByFillNeedFix expected output restored
│   └── SampleByNanoTimestampTest.java    -- mirror SampleByTest adjustments
└── ExplainPlanTest.java                  -- plan assertions refreshed for new `fill=` attribute
```

### Pattern 1: Retro-fallback via stash-and-restore

**What:** Save `sampleByNode` on the QueryModel before `rewriteSampleBy` clears it, so that on a codegen-detected
fallback we can put it back and re-dispatch through `generateSampleBy`.

**When to use:** Any rewriter that destructively clears model state it may need to recover later. The
established precedent in this codebase is `setBackupWhereClause` / `IQueryModel.restoreWhereClause`
(`SqlCodeGenerator.java:3664` and `8025`) — filters that get stolen and must be put back if the caller
restarts codegen. Retro-fallback uses the same idea for SAMPLE BY state.

**Example (shape, not final code):**

```java
// In rewriteSampleBy, SqlOptimiser.java:~8438, before the clear:
nested.setStashedSampleByNode(sampleBy);  // NEW — analogous to setFillStride(sampleBy) two lines above
nested.setSampleBy(null);
nested.setSampleByOffset(null);
nested.setSampleByFromTo(null, null);

// In generateSelectGroupBy, SqlCodeGenerator.java around 7998/8171/8231:
try {
    return generateFill(model, groupByFactory, executionContext);
} catch (FallbackToLegacyException fallback) {
    // generateFill already freed groupByFactory, fillValues, constantFillFuncs,
    // fillFromFunc, fillToFunc via its own catch at line 3624-3631.
    IQueryModel curr = locateCurrModelWithFillStride(model);
    ExpressionNode stashed = curr.getStashedSampleByNode();
    curr.setSampleBy(stashed);  // restores the check at line 7808
    curr.setStashedSampleByNode(null);  // single-shot
    // model.getSampleByOffset/From/To/TimezoneName/Fill stayed on the model because
    // rewriteSampleBy only cleared them at the *nested* level — verify during planning
    return generateSampleBy(model, executionContext, stashed, model.getSampleByUnit());
}
```

**Critical detail to resolve in plan:** `rewriteSampleBy` operates on `nested = model.getNestedModel()`,
not on `model` directly. The stash must live on that same `nested` instance. But the `generateSelectGroupBy`
check at `SqlCodeGenerator.java:7808` reads `model.getSampleBy()` on the model passed IN (which has
`SELECT_MODEL_GROUP_BY`). Verify that the model reaching `generateSelectGroupBy` IS the `nested` model
from `rewriteSampleBy` (they're the same object after the rewrite — only model wrappers above change).
If not, restore must walk the outer chain to find the right model. `[ASSUMED: same-object identity holds
through post-rewrite passes]` — verify by tracing `rewriteSelectClause` / `wrapWithSelectModel`
(SqlOptimiser.java:8479-8493, 11128, 11139) to confirm that `nested` isn't re-parented or replaced.

### Pattern 2: Codegen-time authoritative type check

**What:** After `groupByFactory.getMetadata()` is available and `prevSourceCols` is built (at
`SqlCodeGenerator.java:3477-3495`), iterate over prevSourceCols and throw on the first column whose source
type fails `isFastPathPrevSupportedType`.

**When to use:** Replacing the reclassification loop at lines 3497-3512. The metadata is fully resolved by
this point (`RecordMetadata` reflects actual aggregate output types, not AST-inferred ones), so no
aggregate-rule inference is needed.

**Example (shape):**

```java
// Replaces current lines 3497-3512
for (int i = 0, n = prevSourceCols.size(); i < n; i++) {
    int col = prevSourceCols.getQuick(i);
    int mode = fillModes.getQuick(col);
    int sourceCol = mode >= 0 ? mode : col;
    short tag = ColumnType.tagOf(groupByMetadata.getColumnType(sourceCol));
    if (!isFastPathPrevSupportedType(tag)) {
        throw new FallbackToLegacyException();  // generateFill's catch frees all partial resources
    }
}
```

**Trigger position matters:** This must fire BEFORE the first irreversible resource allocation that
`generateFill` doesn't know how to free in a single catch. Current allocations to watch:

| Allocated at | What | Handled by generateFill catch? |
|--------------|------|--------------------------------|
| line 3279 | `fillValues = new ObjList<>()` | yes — `Misc.freeObjList(fillValues)` at 3625 |
| line 3294 | per-item functionParser.parseFunction | yes (inside fillValues) |
| line 3349 | `fillFromFunc = driver.getTimestampConstantNull()` | yes — `Misc.free(fillFromFunc)` at 3627 |
| line 3352 | `fillFromFunc = parseFunction(fillFrom, ...)` | yes |
| line 3357 | `fillToFunc = parseFunction(fillTo, ...)` | yes |
| line 3390 | `constantFillFuncs = new ObjList<>()` | yes — `Misc.freeObjList(constantFillFuncs)` at 3626 |
| lines 3411-3466 | constantFillFuncs populated with NullConstant.NULL or transferred functions | yes |
| **line 3495** | `prevSourceCols` IntList built | not freed — IntList has no close; fine (GC'd) |
| **line 3512** | (current safety-net exit) | — |
| line 3515 | `keyColIndices` IntList built | not freed — fine |
| line 3523 | `mapKeyTypes = new ArrayColumnTypes()` | not freed — fine (no native resources in ColumnTypes) |
| line 3545 | `mapValueTypes = new ArrayColumnTypes()` | not freed — fine |
| line 3559 | `keySink = RecordSinkFactory.getInstance(...)` | not freed — RecordSink may own ASM state; plan verifies |
| line 3591 | `groupByFactory = new SortedRecordCursorFactory(...)` **wraps the parameter** | yes — `Misc.free(groupByFactory)` at 3629 (frees the wrapper, which cascades) |
| line 3603 | `new SampleByFillRecordCursorFactory(...)` (constructor allocates keysMap) | the factory is the return value — if constructor throws it must free its own partial state |

**Recommendation:** Throw the fallback exception at line 3512 (replacing the reclassification block). At
that point only fillValues, constantFillFuncs, fillFromFunc, fillToFunc, groupByFactory are allocated, and
the existing catch handles all five. `[VERIFIED: SqlCodeGenerator.java:3274-3631]`

### Pattern 3: Grammar-rule enforcement in generateFill

**What:** Reject malformed `FILL(PREV, PREV(...))` shapes at compile time with positioned `SqlException`.

**When to use:** During the per-column fill-spec build loop at `SqlCodeGenerator.java:3423-3470`. Each
rejection fires at the earliest point where the AST shape makes the rule decidable. Chain detection is a
post-pass because it needs every `fillModes[col]` populated first.

**Slot map for the five new grammar rejections:**

| Rule | Fires at | AST preconditions | Message (suggested) |
|------|----------|-------------------|---------------------|
| D-05 `PREV(ts)` | Inside the `fillExpr.type == FUNCTION && paramCount == 1` branch at **3439-3442**, after resolving `srcAlias`. Check `srcColIdx == timestampIndex` (or alias matches timestamp token). | rhs is LITERAL | `"PREV cannot reference the designated timestamp column"` at `fillExpr.rhs.position` |
| D-06 `PREV(self)` | Same branch at **3444-3450**. When `srcColIdx == col`, replace `fillModes.add(srcColIdx)` with `fillModes.add(FILL_PREV_SELF)` — this is a rewrite, not a rejection. | srcColIdx resolved | — (no error, internal normalization) |
| D-08 Malformed PREV (non-LITERAL rhs, paramCount != 1, bind variable) | **Replace the check at 3438** `if (fillExpr != null && isPrevKeyword(fillExpr.token))`. Inside this branch, BEFORE dispatch on `type == FUNCTION`, validate: `fillExpr.type == LITERAL (bare PREV)` OR `(fillExpr.type == FUNCTION && paramCount == 1 && rhs != null && rhs.type == LITERAL)`. Otherwise throw. | any PREV token | `"PREV argument must be a single column name"` at `fillExpr.position` |
| D-09 Type-tag mismatch | Same branch at **3444-3450**, after `srcColIdx` resolves. Compare `ColumnType.tagOf(groupByMetadata.getColumnType(srcColIdx)) == ColumnType.tagOf(groupByMetadata.getColumnType(col))`. Reject on mismatch. | `groupByMetadata` available (yes, line 3387) | `"FILL(PREV(<src>)): source type <X> cannot fill target column of type <Y>"` at `fillExpr.rhs.position` |
| D-07 Chain | Post-pass immediately after the build loop, at **line 3470** (before `prevSourceCols` construction at 3477). | `fillModes` fully populated for all columns | `"FILL(PREV) chains are not supported: source column is itself a cross-column PREV"` — at `fillValuesExprs.getQuick(0).position` (simple variant) or the precise offending `PREV(...)` position (precise variant — requires per-column ExpressionNode tracking) |

**Chain-detection mechanics verification:** The per-column build loop at lines 3424-3470 iterates every
output column and writes exactly one `fillModes.add(...)` per iteration. After the loop completes,
`fillModes.size() == columnCount` and every entry is one of `FILL_CONSTANT`, `FILL_KEY`, `FILL_PREV_SELF`,
or a cross-col index `>= 0`. The consumer at line 3478 only reads fillModes AFTER the loop — so a post-pass
between line 3470 and 3477 has a fully-built table to work with. `[VERIFIED: SqlCodeGenerator.java:3424-3495]`

**Position-tracking for the precise chain-position variant:** The `fillValuesExprs` array at line 3270 is
indexed by fill-slot, not by output column. The per-column loop at line 3423 uses `fillIdx` which skips
timestamp and key columns. To remember which `ExpressionNode` produced a given `fillModes[col]` entry,
introduce an `ObjList<ExpressionNode> perColExpr` (size `columnCount`) alongside `fillModes` in the same
loop, then consult it in the chain post-pass. This is the "precise position" variant (~10 lines).

### Anti-Patterns to Avoid

- **Silent reclassification** — the current safety-net at 3497-3512 is the exact anti-pattern being
  removed. Never change the semantic classification of a column at codegen to "make things work" without
  the user's awareness. Either the fast path handles it correctly or we fall back.
- **Catching `Throwable` at the fallback call site** — masks genuine bugs. Catch the specific
  `FallbackToLegacyException` type.
- **Double-free on the fallback path** — `generateFill`'s catch at 3624-3631 already frees groupByFactory.
  The caller must NOT re-close any of: groupByFactory, fillValues, constantFillFuncs, fillFromFunc,
  fillToFunc. It MAY close other state it owns (e.g., the outer `factory` in `generateSelectGroupBy`
  line 8248-8251 generic catch — but that's the base cursor, unrelated).
- **Widening `isUnsupportedPrevType` without widening `isFastPathPrevSupportedType` in lockstep** — the
  two must remain complements. Adding LONG128+INTERVAL to the former without adjusting the latter
  (already correct by default case at line 1208) is fine because `default -> false` means "unsupported".
  But if future work extends the fast path to new types, BOTH sides must move together.
- **Stashing to a shared pool-reset field** — `QueryModel.clear()` at line 460-505 zeros many fields;
  `stashedSampleByNode` must be added to `clear()` to avoid reuse-after-pool-recycle corruption.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cleanup on throw path in generateFill | A custom resource-tracker or linked-list | The existing try/catch at 3624-3631 | It already frees exactly what we allocate between line 3274 and 3623. Throwing at line 3512 lets that catch do its job. |
| Type complement for `isFastPathPrevSupportedType` | Open-coded `switch` in optimizer | Extend `isUnsupportedPrevType` (already exists, same shape) | Keep the gate and codegen predicates symmetric and trivially reviewable side-by-side |
| Alias resolution in Tier 1 gate | Custom column-lookup traversal | `model.getAliasToColumnMap().get(token)` and `nested.getAliasToColumnMap().get(token)` | Both maps already exist; `isUnsupportedPrevAggType` at line 540 already uses `nested.getAliasToColumnMap()` |
| Position tracking for chain errors | Parallel `IntList` of fill-slot-to-column positions | Add `ObjList<ExpressionNode>` alongside `fillModes` during the build loop | Matches how `constantFillFuncs` already parallels `fillModes` |
| Plan-text emission for `fill=` | Manual conditional chains | Single `sink.attr("fill").val(...)` call per fill mode | `toPlan` at `SampleByFillRecordCursorFactory.java:180-191` already uses the `sink.attr/val` pattern |

**Key insight:** The retro-fallback lives within QuestDB's existing codegen/optimiser pipeline. Every
primitive we need (model stash pattern, exception+position+catch-cleanup, enhanced switch for tag matching,
alias maps) already exists. Fight the temptation to invent — the risk in this phase is introducing a new
leak path, not missing an API.

## Runtime State Inventory

Phase 12 is a pure code change. Nothing stored, registered, or deployed outside the repo.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | None — phase 12 touches codegen/optimiser only; no on-disk format changes | none |
| Live service config | None | none |
| OS-registered state | None | none |
| Secrets/env vars | None | none |
| Build artifacts | None — all changes are in `core/` sources; `mvn clean package` regenerates everything | none |

**Nothing found in any category** — this is a code-only refactor of an in-memory execution path.

## Common Pitfalls

### Pitfall 1: Stash-field survives the wrong rewrite

**What goes wrong:** `rewriteSampleBy` recurses into joined and unioned nested models
(`SqlOptimiser.java:8511-8524`). If a single query has SAMPLE BY at multiple nesting levels (main + union,
or main + join sub-query), setting `stashedSampleByNode` at each level is fine — each `nested` is a
different QueryModel instance. But if a downstream pass like `rewriteSelectClause` REPLACES the nested
model with a new wrapper, the stash goes with the old instance and the new wrapper has no stash.

**Why it happens:** `wrapWithSelectModel` at `SqlOptimiser.java:8479-8493` creates a new outer model that
wraps the existing one. The wrapped (inner) QueryModel is the same object that got the stash — good. But
check that `rewriteSelectClause` at line 11469 and `rewriteOrderBy` at line 11483 don't swap the
fill-bearing nested model out from under us.

**How to avoid:** During the plan, add a `Misc.free`-adjacent verification step — write a targeted unit
test that queries `curr.getStashedSampleByNode()` inside `generateFill` and confirms it's non-null for the
unsupported-type fallback case. If it's null, the stash got lost somewhere and the retro-fallback will
NPE on restore.

**Warning signs:** NPE at `model.setSampleBy(stashedSampleByNode)` in the catch; or `getSampleBy()`
returning null on the restored model when `generateSampleBy` reads it.

### Pitfall 2: groupByFactory double-free

**What goes wrong:** The caller catches `FallbackToLegacyException`, calls `generateSampleBy`, and somehow
the outer try/catch at `SqlCodeGenerator.java:8248-8251` also frees something. Or generateSampleBy itself
throws, the outer catch runs, and closes a factory that generateFill already closed.

**Why it happens:** The groupByFactory passed to generateFill is created inline in the argument list of
the call (e.g. `generateFill(model, new AsyncGroupByRecordCursorFactory(...), executionContext)` at 8171).
If generateFill's catch at 3624-3631 frees it, and the argument expression itself threw, Java's evaluation
order means the constructor already succeeded — so the catch is fine. But the caller doesn't have a local
variable for the groupByFactory, so there's no way to accidentally close it twice from the caller side.

**How to avoid:** Verify by reading 7998-8013, 8171-8214, 8231-8247: none of them store the inline-created
factory in a variable. `factory` in that scope is the base-cursor factory, different object.

**Warning signs:** Double-free assertion at test time, or use-after-free crash on the first fallback test.

### Pitfall 3: Test expectation asymmetry between `assertSql` and `assertQueryNoLeakCheck`

**What goes wrong:** `assertSql` checks only row data. `assertQueryNoLeakCheck` additionally asserts
factory properties: `supportsRandomAccess=false`, `size()=-1`, `supportsPageFrameCursor=false`,
`expectedTimestampIndex`. A test that passes under `assertSql` may fail under `assertQueryNoLeakCheck` if
a mid-stream change altered one of those properties.

**Why it happens:** CONTEXT.md's success criterion #12 requires converting fast-path tests to
`assertQueryNoLeakCheck` so that regressions in factory properties are caught. But the rule has a
carve-out: tests that use `assertSql` for a specific reason (multi-step DDL, legacy plan assertion, storage
tests in the cairo package) must stay as-is. Getting this carve-out wrong means either false failures or
missed coverage.

**How to avoid:** For each of the 42 `assertSql` calls in SampleByFillTest.java, apply the rule
mechanically: does the test (1) exercise the fast-path fill cursor (check the query shape — FILL(NULL) /
FILL(<const>) / FILL(PREV) without unsupported types), AND (2) is `supportsRandomAccess=false` and
`size()=-1` the correct expected? If both yes, convert. If the test asserts legacy plan, has mid-test DDL,
or is a cairo storage test, skip.

**Warning signs:** A converted test that previously passed now fails with a factory-property mismatch
error like `expected supportsRandomAccess=true but was false`. If that happens, the test was
mis-classified — revert the conversion.

### Pitfall 4: Grammar rule D-09 (type-tag mismatch) fires on `PREV(key_col)` incorrectly

**What goes wrong:** The type-tag comparison `ColumnType.tagOf(sourceType) == ColumnType.tagOf(targetType)`
rejects `FILL(PREV(city))` when `city` is VARCHAR and the target aggregate column is DOUBLE. That's
correct. But the existing `PREV(key_col)` commit (CONTEXT.md Accepted Shapes, D-05) routes key-column
prev through a dedicated branch — the runtime reads the key value from `keysMapRecord`, not from a
snapshot. So the rejection must NOT fire when the source column is itself FILL_KEY.

**Why it happens:** The check at `SqlCodeGenerator.java:3486-3488` already special-cases `fillModes.getQuick(mode)
== FILL_KEY` to skip prevSourceCols allocation. The D-09 mismatch check must apply the same special-case:
if the source is a key column, the getter reads from `keysMapRecord` and the type-tag comparison is
against the key-column type, which is the SAME type-tag as stored (so they'd match trivially). Actually
no — key columns retain their ORIGINAL type in the metadata (e.g., VARCHAR key column's type is VARCHAR
in groupByMetadata). Filling a DOUBLE aggregate from a VARCHAR key via `PREV(city)` is still a type
mismatch and should still be rejected. So D-09 applies uniformly — no special-case needed.

**How to avoid:** The `testFillPrevOfVarcharKeyColumn` and `testFillPrevOfSymbolKeyColumn` tests (lines
1185, 1159) today pass — they fill same-typed columns (VARCHAR→VARCHAR, SYMBOL→SYMBOL). Verify these still
pass with D-09 active. Add one explicit cross-type-tag test (e.g., `PREV(int_key)` filling a DOUBLE
aggregate) and confirm the rejection fires.

**Warning signs:** Existing test regressions for `testFillPrevOfIntKeyColumn` or
`testFillPrevOfSymbolKeyColumn` after D-09 lands.

### Pitfall 5: Offset-validation assertion fires for an unexpected case

**What goes wrong:** CONTEXT.md item 21 recommends replacing the silent drop at
`SqlCodeGenerator.java:3379-3382` with an assertion on the premise that `rewriteSampleBy` already
validated the offset. If that premise is wrong, the assertion fires on a real user query.

**Why it happens:** `rewriteSampleBy`'s validation path depends on the exact model state at rewrite time.
Non-trivial CTE nesting or union models may route through a different branch that doesn't validate the
offset before reaching codegen.

**How to avoid:** Audit `rewriteSampleBy` for the validation path of `sampleByOffset`. If the validation
is NOT universal, demote CONTEXT.md item 21 to a positioned `SqlException` (mentioned in CONTEXT.md as
the fallback if the assertion premise doesn't hold).

**Warning signs:** An assertion failure on a query that master accepts. Revert to the SqlException form
and log the failing query.

## Code Examples

### Adding `stashedSampleByNode` to QueryModel (mirrors fillOffset)

```java
// QueryModel.java
// Field declaration (alphabetical among instance fields, e.g. after sampleByUnit)
private ExpressionNode stashedSampleByNode;

// In clear() — add to the nulling block around line 460-479, alphabetical slot
stashedSampleByNode = null;

// Getter (alphabetical among getters)
@Override
public ExpressionNode getStashedSampleByNode() {
    return stashedSampleByNode;
}

// Setter (alphabetical among setters)
@Override
public void setStashedSampleByNode(ExpressionNode stashedSampleByNode) {
    this.stashedSampleByNode = stashedSampleByNode;
}
```

```java
// IQueryModel.java — add the interface methods (alphabetical)
ExpressionNode getStashedSampleByNode();

void setStashedSampleByNode(ExpressionNode stashedSampleByNode);
```

### Tier 1 gate change — cross-col resolution

```java
// SqlOptimiser.java — replace the skip at lines 521-525
if (fillExpr != null && isPrevKeyword(fillExpr.token)) {
    if (fillExpr.type == ExpressionNode.FUNCTION && fillExpr.paramCount == 1
            && fillExpr.rhs != null && fillExpr.rhs.type == LITERAL) {
        // PREV(alias) — resolve alias against the output model, then walk to source type
        QueryColumn srcOutput = model.getAliasToColumnMap().get(fillExpr.rhs.token);
        if (srcOutput != null) {
            ExpressionNode srcAst = srcOutput.getAst();
            if (srcAst.type == FUNCTION && srcAst.rhs != null && srcAst.rhs.type == LITERAL) {
                // aggregate on LITERAL — resolve base column
                QueryColumn srcBase = nested.getAliasToColumnMap().get(srcAst.rhs.token);
                if (srcBase != null && srcBase.getColumnType() >= 0
                        && isUnsupportedPrevType(ColumnType.tagOf(srcBase.getColumnType()))) {
                    return true;
                }
            } else if (srcAst.type == LITERAL) {
                // key or timestamp column
                QueryColumn srcBase = nested.getAliasToColumnMap().get(srcAst.token);
                if (srcBase != null && srcBase.getColumnType() >= 0
                        && isUnsupportedPrevType(ColumnType.tagOf(srcBase.getColumnType()))) {
                    return true;
                }
            }
            // expression-arg source — can't resolve at gate; fall through (fallback catches it)
        }
        fillIdx++;
        continue;
    }
    if (isUnsupportedPrevAggType(ast, nested)) {
        return true;
    }
}
```

### Tier 1 gate change — LONG128 / INTERVAL

```java
// SqlOptimiser.java — add to isUnsupportedPrevType at line 552-559
private static boolean isUnsupportedPrevType(short tag) {
    return switch (tag) {
        case ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR,
             ColumnType.LONG256, ColumnType.BINARY, ColumnType.UUID,
             ColumnType.DECIMAL128, ColumnType.DECIMAL256,
             ColumnType.LONG128, ColumnType.INTERVAL -> true;  // NEW
        default -> ColumnType.isArray(tag);
    };
}
```

### Grammar rule D-08 — malformed PREV rejection

```java
// SqlCodeGenerator.java — new guard around line 3438, before the existing type==FUNCTION dispatch
if (fillExpr != null && isPrevKeyword(fillExpr.token)) {
    // Validate PREV shape up front — single unified rejection
    boolean isBarePrev = fillExpr.type == ExpressionNode.LITERAL;
    boolean isPrevWithLiteralArg = fillExpr.type == ExpressionNode.FUNCTION
            && fillExpr.paramCount == 1
            && fillExpr.rhs != null
            && fillExpr.rhs.type == ExpressionNode.LITERAL;
    if (!isBarePrev && !isPrevWithLiteralArg) {
        throw SqlException.$(fillExpr.position, "PREV argument must be a single column name");
    }
    // ... existing PREV(col) / bare-PREV branches
}
```

### Grammar rule D-07 — chain detection (simple variant)

```java
// SqlCodeGenerator.java — at the END of the per-column build loop, line 3470
// Reject chains: a column referencing another column that itself is cross-col PREV.
for (int col = 0; col < columnCount; col++) {
    int mode = fillModes.getQuick(col);
    if (mode >= 0 && fillModes.getQuick(mode) >= 0) {
        throw SqlException.$(fillValuesExprs.getQuick(0).position,
                "FILL(PREV) chains are not supported: source column is itself a cross-column PREV");
    }
}
```

### `toPlan` — emit fill= unconditionally

```java
// SampleByFillRecordCursorFactory.java — replace lines 187-189
// Old:
if (hasPrevFill) {
    sink.attr("fill").val("prev");
}
// New: emit fill= for every fill mode so FILL(NULL) and FILL(<const>) plans distinguish from FILL(NONE)
if (hasPrevFill) {
    sink.attr("fill").val("prev");
} else if (hasAnyConstantFill()) {
    sink.attr("fill").val("value");
} else {
    sink.attr("fill").val("null");
}
// hasAnyConstantFill() — new private helper: iterates fillModes, returns true if any mode ==
// FILL_CONSTANT with a non-null constant, false otherwise. Plan decides naming.
```

### FillRecord getter alphabetization (pure reorder, no logic change)

Current order (lines 705-1134): getDouble, getFloat, getInt, getLong, getShort, getByte, getBool, getChar,
getTimestamp, getArray, getBin, getBinLen, getDecimal128, getDecimal16, getDecimal256, getDecimal32,
getDecimal64, getDecimal8, getGeoByte, getGeoInt, getGeoLong, getGeoShort, getIPv4, getLong128Hi,
getLong128Lo, getLong256, getLong256A, getLong256B, getStrA, getStrB, getStrLen, getSymA, getSymB,
getVarcharA, getVarcharB, getVarcharSize.

Target alphabetical order: getArray, getBin, getBinLen, getBool, getByte, getChar, getDecimal128,
getDecimal16, getDecimal256, getDecimal32, getDecimal64, getDecimal8, getDouble, getFloat, getGeoByte,
getGeoInt, getGeoLong, getGeoShort, getIPv4, getInt, getLong, getLong128Hi, getLong128Lo, getLong256,
getLong256A, getLong256B, getShort, getStrA, getStrB, getStrLen, getSymA, getSymB, getTimestamp,
getVarcharA, getVarcharB, getVarcharSize. `[VERIFIED: SampleByFillRecordCursorFactory.java:705-1134 —
counted all 36 getters]`

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Safety-net reclassifies unsupported-type PREV as FILL_KEY at codegen | Codegen detects unsupported type, throws `FallbackToLegacyException`, caller re-dispatches to legacy `generateSampleBy` | Phase 12 (this phase) | Fixes latent correctness bug where genuine aggregates were treated as hash keys, producing duplicated fill rows |
| Optimizer gate skips `PREV(alias)` cross-col references (`SqlOptimiser.java:521-525`) | Gate resolves alias → source column type; blocks unsupported source types at rewrite time | Phase 12 | Avoids building-then-discarding groupBy factory for common cross-col unsupported cases |
| `isUnsupportedPrevType` omits LONG128, INTERVAL | Added to the switch — matches `isFastPathPrevSupportedType` complement | Phase 12 | Closes one-line gap; LONG128/INTERVAL aggregates route to legacy via gate, no fallback needed |
| `FILL(PREV(expression))` and `FILL(PREV, PREV)` chain grammar — undefined behavior at runtime | Positioned `SqlException` with clear error message for each rejected shape | Phase 12 | Compile-time rejection of shapes the runtime can't handle deterministically |
| `toPlan` emits `fill=prev` only when PREV is present, elides for FILL(NULL) / FILL(<const>) | Emits `fill=null\|prev\|value` unconditionally | Phase 12 | Explain plans distinguish FILL(NULL) from FILL(NONE); better observability |
| `testSampleByFillNeedFix` accepts 6-row duplicated output as "expected" | Restored to master's 3-row expected output | Phase 12 | Undoes PR-side expected-output relaxation that encoded the bug as spec |

**Deprecated/outdated:**

- **Phase 11's SYMBOL-key-column rationale for retaining the safety-net** — superseded by the finding that
  no existing test actually triggers the CTE/wrapped-model case that rationale was built around
  (`testFillPrevKeyedCte` at SampleByFillTest.java:960 doesn't trigger the safety-net; `city` is a STRING
  literal correctly classified by `isKeyColumn`). Phase 12 overturns phase 11's decision. If the
  misclassification case surfaces during verification, retro-fallback handles it the same way as any
  other unsupported-type case. `[VERIFIED: CONTEXT.md "Dependency context" section, test source at
  SampleByFillTest.java:960]`

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The `nested` QueryModel instance that receives `setStashedSampleByNode(sampleBy)` in `rewriteSampleBy` survives all subsequent optimiser passes (rewriteSelectClause, wrapWithSelectModel, rewriteOrderBy, etc.) with the stash intact | Mechanism: retro-fallback | If the nested model is replaced/re-parented, stash is lost; catch handler NPEs on restore. Mitigation: add targeted unit test that logs `curr.getStashedSampleByNode()` in generateFill; if null for a known-unsupported query, trace the path and move the stash to a model that survives. |
| A2 | `Dates.parseOffset` validation in `rewriteSampleBy` is universal (not just one branch) so the assertion at `SqlCodeGenerator.java:3379` can replace silent-drop | Common Pitfalls: Pitfall 5; CONTEXT.md item 21 | If a non-validating branch exists, the assertion fires on real user queries. Mitigation: CONTEXT.md explicitly says "if we later learn it can fail at this stage, the assert flips to a `SqlException.$(fillOffsetNode.position, ...)`". |
| A3 | The `outer` vs `nested` dispatch in `generateSelectGroupBy` at line 7808 — `model.getSampleBy()` reads from the same QueryModel that `rewriteSampleBy` writes to | Pattern 1: Retro-fallback | If they're different QueryModel instances, the stash is stored on one and read from the other. Restoring must walk the chain to find the right one. Mitigation: verify during planning by tracing `generateSelect` → `generateSelectGroupBy` (SqlCodeGenerator.java:7581-7594) with a sample unsupported query; log `System.identityHashCode(model)` at both points. |
| A4 | 15 tests in SampleByFillTest.java qualify for `assertSql → assertQueryNoLeakCheck` conversion per the D-10 rule | CONTEXT.md Test conversion | If the count is off, either false failures (too many converted) or missed coverage (too few). Mitigation: CONTEXT.md says "exact set determined by applying the rule" — treat ~15 as an estimate, not a target. Measure when applying. |
| A5 | Existing `testFillPrevOfIntKeyColumn`, `testFillPrevOfSymbolKeyColumn`, `testFillPrevOfVarcharKeyColumn` tests currently pass because each fills a same-type-tag column — D-09 type-mismatch rejection will NOT regress them | Pitfall 4 | If one of them actually fills a different-tag target, D-09 rejects a query that master accepts. Mitigation: read each test's query text during planning and verify source vs target types before landing D-09. |

## Open Questions (RESOLVED)

1. **RESOLVED: Where does `generateSelectGroupBy` observe the model — outer select vs rewritten nested?** (Answer: trust same-identity assumption; add diagnostic assert in plan 12-03 Task 2.)
   - What we know: The SAMPLE BY fields are on `nested`, cleared by `rewriteSampleBy`. After the rewrite,
     `model` (outer SELECT) wraps `nested`. Later optimiser passes (rewriteSelectClause, moveOrderBy...)
     may re-wrap. `generateSelectGroupBy` is invoked on the model with `SELECT_MODEL_GROUP_BY`, which is
     the nested one.
   - What's unclear: Whether any pass between `rewriteSampleBy` and `generateSelectGroupBy` replaces
     the nested instance. `[ASSUMED: no replacement]`
   - Recommendation: During planning, add a printf-style diagnostic during the first retro-fallback test
     to confirm `System.identityHashCode(modelAtRewrite) == System.identityHashCode(modelAtGenerate)`.
   - **RESOLVED:** Trust the same-identity assumption on the first pass. Plan 12-03 Task 2 adds a
     diagnostic `assert stashed != null` at the retro-fallback catch site so a mismatch would fire
     loudly under `-ea`. A full nested-model chain walk is NOT required for the first pass; if the
     assert trips during plan 12-04 test runs, the remediation (walk `model.getNestedModel()` until
     `getStashedSampleByNode() != null`) is documented inline in plan 12-03 Task 2 as contingency
     prose — but with the resolution below from this file, that remediation is now the default
     two-step action rather than conditional prose.

2. **RESOLVED: Should `FallbackToLegacyException` extend `SqlException` or `RuntimeException`?** (Answer: extend `SqlException` with a singleton `INSTANCE`; `super(0)` keeps position retention simple.)
   - What we know: `generateFill` already throws `SqlException`. Extending it means the existing catch
     block signature at 3624 (`catch (Throwable e)`) still catches it; the call-site catch needs to be
     specific to avoid false capture. But `SqlException` carries a position which we don't need for
     fallback.
   - What's unclear: Whether there's a convention in QuestDB for "control-flow exceptions" that already
     extend one or the other.
   - Recommendation: Extend `SqlException` with a private constructor that returns a singleton (no
     position, no message — it's internal control flow). Or use a static field. Plan decides.
   - **RESOLVED:** Extend `SqlException` with a singleton `INSTANCE` (public static final), private
     constructor calling `super(0)`, `fillInStackTrace()` overridden to skip stack-walking. Position
     is carried as `0` and ignored — the exception never surfaces to users. Plan 12-01 Task 1
     implements this exact shape.

3. **RESOLVED: Chain rule: simple (~6 line) vs precise (~10 line) variant?** (Answer: simple ~6-line variant with generic FILL-clause position per CONTEXT D-07.)
   - What we know: Both are functionally equivalent. Simple uses `fillValuesExprs.getQuick(0).position`
     (the whole FILL clause); precise tracks per-column `ExpressionNode` and points at the offending
     `PREV(...)`.
   - What's unclear: How often users hit this error in practice. If rare (which it likely is — chains are
     an obscure grammar), the precise variant's UX upgrade is worth 4 lines.
   - Recommendation: Precise variant. 4 extra lines for a better error message is a clear win.
   - **RESOLVED:** Simple ~6-line variant with generic FILL-clause position per CONTEXT D-07 locked
     decision ("Simplest variant (~6 lines) with generic FILL-clause position"). Plan 12-03 Task 1
     Step F implements the simple variant. If real users hit the error often enough that UX warrants
     the precise variant, a follow-up phase can upgrade — 4 lines of savings today, 4 lines of work
     tomorrow if demand appears.

4. **RESOLVED: What's the right exception subclass name?** (Answer: keep `FallbackToLegacyException`.)
   - What we know: CONTEXT.md suggests `FallbackToLegacyException`.
   - What's unclear: Whether a shorter name like `LegacyFallbackSignal` or `UseCursorPath` is preferred.
   - Recommendation: Stick with `FallbackToLegacyException` unless the plan picks otherwise. It's
     self-documenting.
   - **RESOLVED:** Keep `FallbackToLegacyException`. Self-documenting, matches CONTEXT.md
     terminology, no shorter alternative won enough support to justify divergence.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Java 17 JDK | core module compilation | ✓ | (assumed, per CLAUDE.md build prerequisites) | — |
| Maven 3 | `mvn clean package -DskipTests` | ✓ | (assumed) | — |
| `mvn -Dtest=SampleByFillTest test` | Running the tests this phase adds | ✓ | — | — |
| `cmake` + C/C++ toolchain | Native binaries (not required for phase 12 — pure Java change) | not needed | — | — |

**Missing dependencies with no fallback:** None — phase 12 is pure Java changes in the core module.

**Missing dependencies with fallback:** None.

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 4 + QuestDB test harness (AbstractGriffinTest, assertMemoryLeak, assertQueryNoLeakCheck, assertSql, assertExceptionNoLeakCheck, assertPlanNoLeakCheck) |
| Config file | `core/pom.xml` (surefire plugin) |
| Quick run command | `mvn -pl core -Dtest=SampleByFillTest test` |
| Full suite command | `mvn -pl core test` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| D-01 fallback: unsupported-type PREV routes to legacy | DECIMAL128 aggregate via `sum(decimal128_col * 1)` → legacy plan | unit | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevExpressionArgDecimal128Fallback test` | ❌ Wave 0 |
| D-01 fallback | STRING aggregate via `first(substr(s, 0, 5))` → legacy plan | unit | `...#testFillPrevExpressionArgStringFallback test` | ❌ Wave 0 |
| D-01 fallback | CASE over DECIMAL → legacy plan | unit | `...#testFillPrevCaseOverDecimalFallback test` | ❌ Wave 0 |
| D-01 fallback | `first(long128_col)` → legacy plan | unit | `...#testFillPrevLong128Fallback test` | ❌ Wave 0 |
| D-01 fallback | `first(interval_col)` → legacy plan | unit | `...#testFillPrevIntervalFallback test` | ❌ Wave 0 |
| D-02 Tier 1 gate: LONG128 | LONG128 aggregate routes via gate (plan shows Sample By not Sample By Fill) | unit | part of `testFillPrevLong128Fallback` — plan assertion | ❌ Wave 0 |
| D-02 Tier 1 gate: INTERVAL | INTERVAL aggregate routes via gate | unit | part of `testFillPrevIntervalFallback` | ❌ Wave 0 |
| D-02 Tier 1 gate: cross-col unsupported | `PREV(string_alias)` gate rejects → legacy plan | unit | extend existing `testFillPrevCrossColumnUnsupportedFallback` at line 807 | ✓ exists |
| D-05 reject PREV(ts) | `FILL(PREV(ts))` → SqlException at `rhs.position`, message contains "timestamp" | unit | `...#testFillPrevRejectTimestamp test` | ❌ Wave 0 |
| D-06 self-alias | `FILL(PREV(a))` where `a` is target → same output as bare `FILL(PREV)` | unit | `...#testFillPrevSelfAlias test` | ❌ Wave 0 |
| D-07 chain: mutual | `FILL(PREV(b), PREV(a))` → SqlException "chains are not supported" | unit | `...#testFillPrevRejectMutualChain test` | ❌ Wave 0 |
| D-07 chain: 3-hop | `FILL(PREV(b), PREV(c), PREV)` → SqlException | unit | `...#testFillPrevRejectThreeHopChain test` | ❌ Wave 0 |
| D-07 accept: source is FILL_PREV_SELF | `FILL(PREV(b), PREV)` → accepted | unit | `...#testFillPrevAcceptPrevToSelfPrev test` | ❌ Wave 0 |
| D-07 accept: source is FILL_CONSTANT | `FILL(PREV(b), NULL)` and `FILL(PREV(b), 42.0)` → accepted | unit | `...#testFillPrevAcceptPrevToConstant test` | ❌ Wave 0 |
| D-08 malformed PREV: function arg | `FILL(PREV(foo()))` → SqlException "single column name" | unit | `...#testFillPrevRejectFuncArg test` | ❌ Wave 0 |
| D-08 malformed PREV: multi arg | `FILL(PREV(a, b))` → SqlException | unit | `...#testFillPrevRejectMultiArg test` | ❌ Wave 0 |
| D-08 malformed PREV: no arg | `FILL(PREV())` → SqlException | unit | `...#testFillPrevRejectNoArg test` | ❌ Wave 0 |
| D-08 malformed PREV: bind variable | `FILL(PREV($1))` → SqlException | unit | `...#testFillPrevRejectBindVar test` | ❌ Wave 0 |
| D-09 type-tag mismatch | `FILL(PREV(long_col))` on DOUBLE target → SqlException | unit | `...#testFillPrevRejectTypeMismatch test` | ❌ Wave 0 |
| FILL_KEY UUID | keyed `SAMPLE BY FILL(NULL)` with UUID key → key appears in fill rows | unit | `...#testFillKeyedUuid test` | ❌ Wave 0 |
| FILL_KEY Long256 | keyed with LONG256 key | unit | `...#testFillKeyedLong256 test` | ❌ Wave 0 |
| FILL_KEY Decimal128 | keyed with DECIMAL128 key | unit | `...#testFillKeyedDecimal128 test` | ❌ Wave 0 |
| FILL_KEY Decimal256 | keyed with DECIMAL256 key | unit | `...#testFillKeyedDecimal256 test` | ❌ Wave 0 |
| GEO no-prev null sentinels | keyed FROM/TO with GEOHASH, leading fill rows → GeoHashes.*_NULL | unit | `...#testFillPrevGeoNoPrevYet test` | ❌ Wave 0 |
| TO null::timestamp bounded | `TO null::timestamp` → bounded output (not Long.MAX_VALUE) | unit | `...#testFillToNullTimestamp test` | ❌ Wave 0 |
| testSampleByFillNeedFix restoration | single-symbol gap query returns 3 rows matching master | unit | `mvn -pl core -Dtest=SampleByTest#testSampleByFillNeedFix test` | ✓ exists (needs update) |
| plan-text `fill=` attribute refresh | plan text for FILL(NULL) and FILL(<const>) contains `fill=null` / `fill=value` | unit | existing `assertPlanNoLeakCheck` sites in SampleByFillTest, SampleByTest, SampleByNanoTimestampTest, ExplainPlanTest | ✓ exists (needs update) |

### Sampling Rate

- **Per task commit:** `mvn -pl core -Dtest=SampleByFillTest test` (~5-10 seconds on a warm cache; fast enough
  for every task that touches grammar, codegen, or the fill cursor)
- **Per wave merge:** `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest' test`
  (phase 12 touches all three suites — SampleByTest for `testSampleByFillNeedFix`, NanoTimestamp for
  parallel expectations per CONTEXT.md #8)
- **Phase gate:** `mvn clean package` then `mvn -pl core test` full suite green before `/gsd-verify-work`.
  The 280_124-test suite from the phase-11 CI run is the floor.

### Wave 0 Gaps

- [ ] `FallbackToLegacyException.java` — new source file (or sentinel mechanism, plan decides)
- [ ] `QueryModel.stashedSampleByNode` field + getter/setter + `IQueryModel` interface update + `clear()`
      nulling
- [ ] 19 new test methods listed in the Phase Requirements → Test Map above (5 retro-fallback + 8 grammar
      + 5 FILL_KEY + 1 TO-null)
- [ ] Update `testSampleByFillNeedFix` expected output from 6 rows to 3 rows; drop "two rows per bucket"
      comment
- [ ] Plan-text assertion refresh: all existing `assertPlanNoLeakCheck` call sites whose plan includes a
      `Sample By Fill` node must add the `fill=null|prev|value` line (SampleByFillTest, SampleByTest,
      SampleByNanoTimestampTest, ExplainPlanTest)

*(No framework install gaps — JUnit 4 and the QuestDB test harness are already in `core/pom.xml`.)*

## Security Domain

`security_enforcement` is not configured in `.planning/config.json` (absent; treat as enabled). However,
phase 12 makes no changes to authentication, authorization, network protocols, input validation of
user-supplied data, cryptography, or session management. It is a purely internal refactor of a SQL query
execution path.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | — (no authn change) |
| V3 Session Management | no | — |
| V4 Access Control | no | — |
| V5 Input Validation | partial | SqlException positioned errors for malformed `FILL(PREV, ...)` shapes are a form of input validation at the SQL parser/codegen layer. The pattern is already in use throughout `SqlCodeGenerator.java`. |
| V6 Cryptography | no | — |

### Known Threat Patterns for this change

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| DoS via malformed `FILL(PREV(...))` causing unbounded CPU/memory | Denial of Service | Compile-time rejection with positioned SqlException; query never reaches the cursor. Grammar rules D-05 through D-09 implement this. |
| Information disclosure via error messages leaking internal structure | Information disclosure | Error messages include column names and types from the user's own query — already public to the caller. No internal state (pointers, memory addresses, file paths) in any grammar-rule error. `[VERIFIED: CONTEXT.md suggested messages]` |
| Resource leak on fallback path producing slow-burn OOM | Denial of Service | `generateFill`'s try/catch at 3624-3631 frees all partial allocations. Retro-fallback relies on that invariant — covered by A3 verification step in planning. |

## Sources

### Primary (HIGH confidence)

- **CONTEXT.md** (`/Users/sminaev/qdbwt/elegant-cuddling-sprout/.planning/phases/12-replace-safety-net-reclassification-with-legacy-fallback-and/12-CONTEXT.md`) — the authoritative spec for this phase
- **SqlCodeGenerator.java** line-number references verified via Read/Grep (lines 1199, 3253-3631, 3640,
  3392-3398, 7049, 7808, 7998, 8171, 8231)
- **SqlOptimiser.java** line-number references verified (lines 474-559, 521-525, 535-559, 8116-8524, 8438-8441)
- **SampleByFillRecordCursorFactory.java** line-number references verified (lines 27-57, 72-74, 180-191,
  213-229, 523-526, 650-670, 705-1134)
- **QueryModel.java** — `fillOffset` field pattern at lines 137, 471, 751, 1646 confirmed as the template
  for `stashedSampleByNode`
- **IQueryModel.java** — interface methods for fillOffset at 287, 561 (template for stashedSampleByNode)
- **ExpressionNode.java** — type constants verified at 43-56: BIND_VARIABLE=3, FUNCTION=6, LITERAL=7,
  confirming BIND_VARIABLE != LITERAL for D-08 rule
- **ColumnType.java** — LONG128=24 at line 105, INTERVAL=39 at line 136; `tagOf(int)` at 723; `nameOf(int)`
  at 673
- **/Users/sminaev/projects/questdb/plans/fill-fast-path-overview.md** — §8 architectural decisions, §9
  "What stays on the old cursor path" (explicit fallback-to-legacy design)
- **/Users/sminaev/projects/questdb/plans/fill-fast-path-design-review_v2.md** — §8 "Two-layer type
  defense" (gate = optimization; safety net = correctness), §9 "Fallback conditions" (mapping to legacy
  factories), §10 "Known issues" (SYMBOL-key CTE case that phase 11 rationalized safety-net around)
- **CLAUDE.md** at `/Users/sminaev/qdbwt/CLAUDE.md` and
  `/Users/sminaev/qdbwt/elegant-cuddling-sprout/CLAUDE.md` — coding conventions, test style, git/PR
  conventions

### Secondary (MEDIUM confidence)

- **review-pr skill SKILL.md** — QuestDB runs with assertions enabled (`-ea`), assertions are preferred
  for invariants
- **SampleByFillTest.java** — existing test style patterns (assertMemoryLeak, assertSql,
  assertQueryNoLeakCheck, assertPlanNoLeakCheck, assertExceptionNoLeakCheck); verified counts: 42
  assertSql calls, 5 assertQueryNoLeakCheck, 5 assertPlanNoLeakCheck, 1 assertExceptionNoLeakCheck
- **SampleByFill*RecordCursorFactory.java** (legacy factories) — all emit `sink.type("Sample By")` at
  various lines; fast-path factory alone emits `sink.type("Sample By Fill")`. Clean plan-text
  discrimination for assertion.

### Tertiary (LOW confidence)

- None — every factual claim in this research is sourced from source code, the CONTEXT.md decisions, or
  the external design docs referenced above.

## Metadata

**Confidence breakdown:**

- **User Constraints:** HIGH — verbatim copy of CONTEXT.md decisions, no interpretation
- **Standard Stack:** HIGH — all references verified via Read/Grep on actual source files
- **Architecture:** HIGH — patterns traced through actual call graphs (rewriteSampleBy →
  generateSelectGroupBy → generateFill)
- **Pitfalls:** MEDIUM — Pitfall 1 (stash-field survival) and Pitfall 5 (offset validation universality)
  both rest on assumptions that should be verified during planning/implementation
- **Code Examples:** HIGH — all examples derived from actual surrounding code, line-accurate
- **Validation Architecture:** HIGH — framework and commands match CLAUDE.md build instructions
- **Security Domain:** HIGH — phase 12 is internal refactor with no external attack surface

**Research date:** 2026-04-16
**Valid until:** 2026-05-16 (30 days — the codebase moves steadily but the relevant files have been
stable in their current shape since phase 11 landed ~2 weeks ago)
