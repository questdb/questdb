# Phase 16: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys - Research

**Researched:** 2026-04-21
**Domain:** SAMPLE BY FILL fast-path classifier + cursor-side key wiring (QuestDB griffin/engine/groupby)
**Confidence:** HIGH

## Summary

This is a surgical classifier fix in `SqlCodeGenerator.generateFill`. The classifier loop at `SqlCodeGenerator.java:3405-3436` recognizes only LITERAL and `timestamp_floor` FUNCTION as non-aggregate grouping-key shapes. Non-aggregate FUNCTION nodes (`interval(lo, hi)`, `concat(a, b)`, `cast(x AS STRING)`) and OPERATION nodes (`a || b`, `a + b`) fall through to the aggregate arm, get mapped to `FILL_PREV_SELF`, and as a result are absent from `keyColIndices` passed to `SampleByFillRecordCursorFactory`. The cursor then treats them as aggregates — keyed cartesian emission drops them from the effective key set. Single-key fixture `testFillPrevInterval` hides the bug because FILL_PREV_SELF reads the same value FILL_KEY would when there is only one key. Empirical probe with two distinct `interval(lo, hi)` keys produces 3 rows instead of 6. [VERIFIED: direct read of SqlCodeGenerator.java:3405-3436, 3447-3465, 3668-3673]

**Cursor-side verdict (D-06): NO cursor change needed.** `keyColIndices` at line 3668-3673 is built by iterating the final `fillModes` array and collecting every column whose mode is `FILL_KEY`. Post-fix, function-key columns' `factoryColToUserFillIdx[col]` stays at its initialized `-1`, which drives both the bare-FILL(PREV) branch (line 3458) and the per-column branch (line 3520) to assign `FILL_KEY`. `keyColIndices` picks them up automatically. The cursor's `outputColToKeyPos[]` inside `SampleByFillCursor`'s constructor (line 358-362) then assigns them a valid `KEY_POS_OFFSET + i` slot, and `FillRecord`'s per-type getters (e.g., `getInterval` at line 1012) correctly dispatch through `keysMapRecord.getX(outputColToKeyPos[col])`. GROUP BY materializes function-valued keys as regular output columns in `groupByMetadata`, and `keySink`'s RecordSinkFactory build (line 3699-3701) indexes them positionally — function vs literal origin is invisible at that layer. [VERIFIED: SampleByFillRecordCursorFactory.java:119-183, 324-363, 1012-1020]

**Primary recommendation:** Single plan, classifier fix + 5 regression tests + same-commit defensive assertion per CONTEXT.md D-01/D-05 and Phase 14/15 file-clustered convention. No cursor-side changes. No plan-text impact. ~25-line production diff plus ~5 test methods totaling ~200 test lines.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01** Fix approach = Option 1 (classifier fix local to `SqlCodeGenerator.generateFill`). Option 2 (upstream canonicalization in `SqlOptimiser.rewriteSampleBy`) rejected and deferred.
- **D-02** Widened 3rd arm uses `(FUNCTION || OPERATION) && !functionParser.getFunctionFactoryCache().isGroupBy(ast.token)` as the predicate. Under `-ea`, assert that `qc.getAlias()` resolves to a non-timestamp factory column index via `groupByMetadata.getColumnIndexQuiet(alias)`. Shape from CONTEXT.md:
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
- **D-03** Cover BOTH `ExpressionNode.FUNCTION` and `ExpressionNode.OPERATION` AST types.
- **D-04** Regression suite = FILL(PREV) x 4 variants (interval, concat, cast, `a || b`) + FILL(NULL) x 1 representative (cast). No FILL(constant) test.
- **D-05** Defensive assertion (variant I) lands in the SAME commit as the classifier fix, at the aggregate-arm entry (immediately before `userFillIdx++` at `SqlCodeGenerator.java:3435`). Form:
  ```java
  assert ast.type == ExpressionNode.FUNCTION
          && functionParser.getFunctionFactoryCache().isGroupBy(ast.token)
      : "generateFill aggregate arm: expected aggregate FUNCTION, got type=" + ast.type + " token=" + ast.token;
  ```
- **D-06** Cursor-side wiring (`keyColIndices`, `outputColToKeyPos[col] >= 0`, `FillRecord` dispatch through `keysMapRecord`) must be verified. If already handled post-fix, no cursor change. If a gap exists, a small cursor-side adjustment lands in the same plan (not a separate plan). **Verdict below: NO cursor change needed.**

### Claude's Discretion

- Exact test fixtures inside `SampleByFillTest` (table DDL, specific interval / concat / cast / `||` values). Planner picks the smallest diff that produces the 6-row cartesian shape matching the empirical probe in the todo.
- Whether the `-ea` alias assert lives directly inside the new `continue` branch (D-02) or just outside the predicate (pre-`continue`). Cleanliness call.
- Plan count: single plan preferred (classifier fix + assertion + regression tests in one commit per Phase 14/15 D-01 file-clustered convention). Planner may split if the diff exceeds typical plan size, but default is 1 plan.

### Deferred Ideas (OUT OF SCOPE)

- **Option 2 (upstream canonicalization in `SqlOptimiser.rewriteSampleBy`)** — lift inline-function grouping expressions into virtual column projections so `bottomUpCols` only ever sees LITERAL references. Rejected for Phase 16 (no known additional bug fixed, plan-text ripple across every fast-path SAMPLE BY with non-literal key). Keep as a future hardening candidate if the classifier drifts again despite D-05. File as a standalone phase if pursued.
- **FILL(constant) multi-key inline-function regression test** — the classifier fix restores correctness by default, and the defensive assertion (D-05) would fire on drift before a user ever saw it. Add ad-hoc if a real-world FILL(const) report surfaces.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| COR-01..04 (strengthened) | Fast-path SAMPLE BY FILL output must match legacy cursor path | Classifier fix ensures function-key FILL_KEY dispatch matches cursor-path cartesian semantics |
| KEY-01..05 (strengthened) | Key columns classified correctly across all AST shapes in `bottomUpCols` | D-02 widens partition to cover FUNCTION + OPERATION non-aggregate nodes |
| XPREV-01 (strengthened) | FILL(PREV) per-key cartesian emission preserves per-key state | Fix closes silent drop of function-valued keys from `keyColIndices` |
| FILL-02 (strengthened) | FILL(NULL) and FILL(PREV) behave consistently under multi-key grouping | D-04 includes one FILL(NULL) cast variant as representative |

No new requirement IDs; this is a latent-bug fix that strengthens existing `COR`, `KEY`, `XPREV`, `FILL` families per ROADMAP Phase 16 scope.
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- Java class members grouped by kind (static vs. instance) + visibility, sorted alphabetically. Insert new test methods in the correct alphabetical position.
- Never insert `// ===` or `// ---` banner comments as section headings (not in production, not in tests).
- Java 17 features: enhanced switch, multiline string literals, pattern variables in `instanceof`. Use them where appropriate.
- Log/error/assertion messages: strictly ASCII. Plain hyphen-minus only. No em dashes, no curly quotes, no unicode.
- Boolean naming: `is...` / `has...` prefix.
- Tests use `assertMemoryLeak()`. Use `assertQueryNoLeakCheck(..., false, false)` for SAMPLE BY FILL (Phase 14 D-15 factory contract).
- Commits: NO Conventional Commits prefix; title <= 50 chars, descriptive plain English; always include long-form body.
- Number literals with 5+ digits use underscore separators (`1_000_000`).

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| AST classification of `bottomUpCols` into {key, timestamp bucket, aggregate} | griffin codegen (`SqlCodeGenerator.generateFill`) | — | Codegen is the only place that partitions `bottomUpCols` into fill-dispatch roles. Optimizer (`SqlOptimiser.rewriteSampleBy`) admits all three AST shapes uniformly into the model and leaves classification to codegen. |
| Key column copy into `keysMap` during pass 1 | griffin engine (`SampleByFillCursor.hasNext` via `keySink.copy(baseRecord, mapKey)`) | cairo (`RecordSinkFactory`) | `keySink` indexes `groupByMetadata` positionally; function-valued keys are regular columns after GROUP BY materializes them. No per-shape logic. |
| Typed FILL_KEY dispatch through `keysMapRecord` | griffin engine (`FillRecord` per-type getters) | — | Each getter already handles `FILL_KEY` via `keysMapRecord.getX(outputColToKeyPos[col])`. No tier-specific branching needed for function-origin keys. |
| Cartesian gap-row emission per bucket | griffin engine (`SampleByFillCursor.emitNextFillRow`) | — | Cursor iterates `keysMapCursor` independent of key origin. Function-key correctness depends solely on `keyColIndices` being correct at construction time. |

**Key insight:** The bug lives entirely at the classifier tier. All downstream tiers (RecordSinkFactory, keysMap, FillRecord, SampleByFillCursor) operate on factory column indices and are blind to whether the key was originally a LITERAL or a FUNCTION/OPERATION in the SQL. Fixing the classifier's `factoryColToUserFillIdx` output automatically flows correctness through every downstream tier.

## Cursor-Side Wiring Verification (D-06)

**Verdict: NO cursor-side change required.** The fix flows automatically from classifier to cursor through existing plumbing.

### End-to-end trace

1. **Classifier (post-fix) — `SqlCodeGenerator.java:3405-3436`:**
   Function-key / operation-key columns hit the new third `continue` branch. `factoryColToUserFillIdx.getQuick(col)` stays at its initialized `-1` for those factory indices. `userFillIdx` is NOT incremented for them. `aggNonKeyCount` ends up smaller by the count of such columns.

2. **Bare-FILL(PREV) branch — `SqlCodeGenerator.java:3454-3465`:**
   ```java
   for (int col = 0; col < columnCount; col++) {
       if (col == timestampIndex) { ... }
       else if (factoryColToUserFillIdx.getQuick(col) < 0) {
           fillModes.add(SampleByFillRecordCursorFactory.FILL_KEY);
       } else {
           fillModes.add(SampleByFillRecordCursorFactory.FILL_PREV_SELF);
       }
   }
   ```
   Function-key columns land in the `< 0` branch -> `FILL_KEY`.

3. **Per-column branch — `SqlCodeGenerator.java:3513-3528`:**
   ```java
   final int fillIdx = factoryColToUserFillIdx.getQuick(col);
   if (fillIdx < 0) {
       fillModes.add(SampleByFillRecordCursorFactory.FILL_KEY);
       ...
       continue;
   }
   ```
   Same `< 0 -> FILL_KEY` logic as the bare branch. Function-key columns land on FILL_KEY regardless of fill mode (FILL(PREV), FILL(NULL), FILL(const), per-column mixed).

4. **`keyColIndices` construction — `SqlCodeGenerator.java:3668-3673`:**
   ```java
   final IntList keyColIndices = new IntList();
   for (int col = 0; col < columnCount; col++) {
       if (fillModes.getQuick(col) == SampleByFillRecordCursorFactory.FILL_KEY) {
           keyColIndices.add(col);
       }
   }
   ```
   This reads from the final `fillModes` array — purely a post-classification derivation. Function-key columns with `FILL_KEY` are included automatically. **No classifier-aware code path here.**

5. **`keySink` construction — `SqlCodeGenerator.java:3693-3702`:**
   ```java
   if (keyColIndices.size() > 0) {
       final ListColumnFilter keyColFilter = new ListColumnFilter();
       for (int i = 0, n = keyColIndices.size(); i < n; i++) {
           keyColFilter.add(keyColIndices.getQuick(i) + 1); // 1-based
       }
       keySink = RecordSinkFactory.getInstance(configuration, asm, groupByMetadata, keyColFilter);
   }
   ```
   `keySink` is a value-copier built from `groupByMetadata` and positional column filter. It reads factory column VALUES — indifferent to whether the value originated from a LITERAL or a FUNCTION AST. GROUP BY has already materialized function-valued keys as regular output columns in `groupByMetadata`.

6. **`SampleByFillCursor` constructor — `SampleByFillRecordCursorFactory.java:358-362`:**
   ```java
   this.outputColToKeyPos = new int[metadata.getColumnCount()];
   Arrays.fill(outputColToKeyPos, -1);
   for (int i = 0, n = keyColIndices.size(); i < n; i++) {
       outputColToKeyPos[keyColIndices.getQuick(i)] = KEY_POS_OFFSET + i;
   }
   ```
   Function-key factory indices get valid `KEY_POS_OFFSET + i` slots. `outputColToKeyPos[col] >= 0` is TRUE for them — exactly the predicate `FillRecord` getters use to dispatch FILL_KEY reads.

7. **`FillRecord` typed getters — `SampleByFillRecordCursorFactory.java:~705-1200`:**
   Every typed getter (confirmed for `getInterval` at line 1012, `getLong` at line 1028, `getVarcharA` / `getStrA` / `getSymbol` etc.) dispatches:
   ```java
   if (mode == FILL_KEY) return keysMapRecord.getX(outputColToKeyPos[col]);
   ```
   No AST-origin branching. Works identically for function-valued keys.

### Why `testFillPrevInterval` still passes post-fix

Single-key fixture: both data rows have identical `interval(lo, hi)` values so pass-1 discovers exactly one key. Pre-fix FILL_PREV_SELF reads the interval from `prevRecord` (snapshot via `recordAt(rowId)`) — same byte-identical interval. Post-fix FILL_KEY reads from `keysMapRecord.getInterval(outputColToKeyPos[col])` — same byte-identical interval stored during `keySink.copy()`. Output is unchanged. Test remains a regression anchor that the INTERVAL getter plumbing compiles and doesn't throw `UnsupportedOperationException` in either dispatch mode.

### Why the bug manifests only with multiple distinct keys

Under FILL_PREV_SELF, the cursor emits gap rows driven by `hasPrevForCurrentGap` / `simplePrevRowId` OR the per-key `PREV_ROWID_SLOT` in the map. But the cartesian emission walks `keysMapCursor` and expects every key column to live in `keyColIndices` so `outputColToKeyPos[col] >= 0` — otherwise the getter falls through to either `prevRecord` (FILL_PREV_SELF branch) or default-null. Pre-fix, function-key factory indices have `outputColToKeyPos[col] == -1`, so the cartesian emission never sees a "key dimension" for them; only the originally-populated rows are emitted.

### Environment verified

- `functionParser` is a field on `SqlCodeGenerator` (line 474) — accessible from `generateFill`.
- `functionParser.parseFunction(...)` already called 3 times inside `generateFill` scope (lines 3275, 3346, 3351). Adding `functionParser.getFunctionFactoryCache().isGroupBy(token)` introduces no new dependency.
- `-ea` surefire flag confirmed at `core/pom.xml:37` — D-05 assertion fires in CI.

[VERIFIED: SqlCodeGenerator.java:474, 3275, 3346, 3351, 3405-3436, 3454-3465, 3513-3528, 3668-3673, 3693-3702] [VERIFIED: SampleByFillRecordCursorFactory.java:119-183, 324-363, 705-1200] [VERIFIED: core/pom.xml:37]

## Downstream Consumers of `aggNonKeyCount`

Only ONE consumer after line 3445:

| Line | Usage | Post-fix behavior |
|------|-------|-------------------|
| 3479 | `if (fillValuesExprs.size() < aggNonKeyCount) { ... throw "not enough fill values" }` | Post-fix `aggNonKeyCount` DECREASES for queries with inline-function grouping keys. The user-supplied fill-value count was previously over-required; post-fix the grammar correctly requires one fill per TRUE aggregate only. This FIXES a latent misbehavior: pre-fix, `FILL(const)` with inline-function grouping key + N aggregates required N+1 fills (rejecting correct user input). Post-fix, it correctly requires N. |
| 3532 (comment only) | "`aggNonKeyCount <= fillValuesExprs.size() guarantees fillIdx in range`" | Comment still accurate post-fix; bound tightens rather than loosens. |

No other `aggNonKeyCount` references in the codebase. [VERIFIED: grep across `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java`]

The CONTEXT.md deferred "FILL(constant) multi-key inline-function regression test" note matches this finding: pre-fix FILL(const) silently threw `"not enough fill values"`; post-fix it works by default. No regression test required because (a) no user-facing regression against pre-fix behavior (pre-fix was error, post-fix is correctness), and (b) D-05 assertion locks the invariant against future drift.

## Standard Stack

### Core

| Component | Location | Purpose | Why Standard |
|-----------|----------|---------|--------------|
| `functionParser.getFunctionFactoryCache().isGroupBy(token)` | `FunctionFactoryCache.java:135` | Canonical FUNCTION-vs-aggregate predicate | 20+ call sites in `SqlOptimiser.java`; zero-alloc lookup in the groupBy name set; the project-wide convention for "is this token an aggregate factory?" |
| `SqlUtil.isTimestampFloorFunction(ast)` | `SqlUtil.java:1256` | Detects `timestamp_floor` / `timestamp_floor_from_offset_utc` | Already used at `SqlCodeGenerator.java:3418` in the classifier's 2nd `continue` branch; D-02 slots its new branch immediately after this one |
| `groupByMetadata.getColumnIndexQuiet(alias)` | base `RecordMetadata` API | Alias-to-factory-index lookup (returns -1 if absent) | Already used at `SqlCodeGenerator.java:3430` to populate `factoryColToUserFillIdx`; reusable in D-02's `-ea` alias assert |
| `assertQueryNoLeakCheck(..., false, false)` | `AbstractCairoTest` | Regression assertion for SAMPLE BY FILL tests | Phase 14 D-15 convention; the two `false, false` positions are `supportsRandomAccess` and `expectSize` — SAMPLE BY FILL cursors advertise neither |

### Test Fixtures (template)

`SampleByFillTest.testFillPrevKeyedIndependent:1785-1813` provides the canonical 2-key x 3-bucket cartesian template shape. New tests follow the same DDL + INSERT + `assertQueryNoLeakCheck` structure. `testFillPrevKeyedNoPrevYet:1878-1898` shows the "leading bucket with no prev yet" rendering convention: missing keys at the first bucket render as `null` (not omitted).

### Installation

No new dependencies. No build changes.

### Version verification

Not applicable — pure source-code change to existing classes.

## Architecture Patterns

### System Architecture Diagram

```
SQL: SELECT ts, interval(lo, hi) k, first(v) FROM t SAMPLE BY 1h FILL(PREV)
         |
         v
  [SqlOptimiser.rewriteSampleBy :8066-8069]
         |  admits LITERAL | FUNCTION | OPERATION into bottomUpCols
         v
  [SqlCodeGenerator.generateFill :3405-3436]  <-- THE CLASSIFIER (bug lives here)
         |
         +--> LITERAL                                -> continue (skip; -1 in factoryColToUserFillIdx)
         +--> timestamp_floor FUNCTION               -> continue (skip; -1)
         +--> [POST-FIX NEW] non-groupBy FUNCTION    -> continue (skip; -1)  <-- D-02
         +--> [POST-FIX NEW] OPERATION               -> continue (skip; -1)  <-- D-02
         +--> aggregate FUNCTION                     -> record userFillIdx mapping, increment   <-- D-05 asserts this arm
         v
  [SqlCodeGenerator.generateFill :3447-3528]  build fillModes[]
         |  -1 in factoryColToUserFillIdx -> FILL_KEY
         |  >=0 mapping -> FILL_PREV_SELF / FILL_CONSTANT / cross-col mode
         v
  [SqlCodeGenerator.generateFill :3668-3673]  keyColIndices = {col : fillModes[col] == FILL_KEY}
         v
  [SampleByFillRecordCursorFactory constructor :119-183]
         |  MapFactory.createOrderedMap(mapKeyTypes, mapValueTypes)
         |  keySink = RecordSinkFactory.getInstance(groupByMetadata, keyColFilter)
         v
  [SampleByFillCursor constructor :324-363]  outputColToKeyPos[keyColIndices[i]] = KEY_POS_OFFSET + i
         v
  [SampleByFillCursor.hasNext :381+]
         |  pass 1: keySink.copy(baseRecord, mapKey) -> keysMap populated per unique key
         |  pass 2 cartesian: iterate (bucket, key) -> FillRecord synthesizes row
         v
  [FillRecord per-type getters ~705-1200]
         if (mode == FILL_KEY) return keysMapRecord.getX(outputColToKeyPos[col]);
```

### Recommended Project Structure

No structural changes. File-level scope:

```
core/src/main/java/io/questdb/griffin/
  SqlCodeGenerator.java              # classifier fix + D-05 assertion (~25 line diff)

core/src/test/java/io/questdb/test/griffin/engine/groupby/
  SampleByFillTest.java              # 5 new @Test methods (~200 line diff)
```

### Pattern 1: Three-way classifier partition -> four-way classifier partition

**What:** `bottomUpCols` loop partitions each column into disjoint roles. Pre-fix: 3 roles (LITERAL key, timestamp_floor bucket, aggregate FUNCTION). Post-fix: 4 roles, with non-aggregate FUNCTION/OPERATION keys added as an explicit branch.

**When to use:** Any code that partitions AST nodes by role in the fast-path must explicitly enumerate every admitted shape rather than relying on a catch-all "else". The admission set in `rewriteSampleBy` (LITERAL, FUNCTION, OPERATION) defines the closed partition universe.

**Example (post-fix classifier):**
```java
// Source: SqlCodeGenerator.java:3405-3436 (post-D-02 shape)
for (int i = 0, n = bottomUpCols.size(); i < n; i++) {
    final QueryColumn qc = bottomUpCols.getQuick(i);
    final ExpressionNode ast = qc.getAst();
    if (ast.type == ExpressionNode.LITERAL) {
        continue;
    }
    if (SqlUtil.isTimestampFloorFunction(ast)) {
        continue;
    }
    // D-02 NEW branch: non-aggregate FUNCTION/OPERATION grouping key
    if ((ast.type == ExpressionNode.FUNCTION || ast.type == ExpressionNode.OPERATION)
            && !functionParser.getFunctionFactoryCache().isGroupBy(ast.token)) {
        assert qc.getAlias() != null
                && groupByMetadata.getColumnIndexQuiet(qc.getAlias()) >= 0
                && groupByMetadata.getColumnIndexQuiet(qc.getAlias()) != timestampIndex
            : "generateFill: non-aggregate FUNCTION/OPERATION in bottomUpCols must resolve to a non-timestamp factory key";
        continue;
    }
    // Aggregate fall-through. D-05 lock-in assertion at this entry.
    assert ast.type == ExpressionNode.FUNCTION
            && functionParser.getFunctionFactoryCache().isGroupBy(ast.token)
        : "generateFill aggregate arm: expected aggregate FUNCTION, got type=" + ast.type + " token=" + ast.token;
    final CharSequence qcAlias = qc.getAlias();
    if (qcAlias != null) {
        final int factoryIdx = groupByMetadata.getColumnIndexQuiet(qcAlias);
        if (factoryIdx >= 0 && factoryIdx != timestampIndex) {
            factoryColToUserFillIdx.setQuick(factoryIdx, userFillIdx);
        }
    }
    userFillIdx++;
}
```
[VERIFIED: SqlCodeGenerator.java:3405-3436 baseline, CONTEXT.md D-02/D-05 shape]

### Pattern 2: Same-commit invariant lock-in assertion (Phase 15 convention)

**What:** Land a defensive `-ea` assertion in the SAME commit as the fix it protects. Under `-ea` surefire, any future drift that violates the invariant fails CI immediately.

**When to use:** Every time a narrow classifier fix closes a latent-partition bug. The assertion pays for itself on the first future refactor that touches the classifier.

**Example:** See D-05 assertion in Pattern 1 above.

[CITED: `.planning/phases/15-address-pr-6946-review-findings-and-retro-fixes/15-CONTEXT.md` D-01/D-02 (file-clustered plans + same-commit lock-in)]

### Anti-Patterns to Avoid

- **Upstream canonicalization (Option 2)** — rewriting inline-function grouping keys into LITERAL virtual column projections in `SqlOptimiser.rewriteSampleBy` would fix the classifier bug by eliminating non-LITERAL shapes from `bottomUpCols`. Rejected: changes plan text across every fast-path SAMPLE BY with a non-literal key (fill or no-fill), rippling into SqlOptimiserTest / ExplainPlanTest / SampleByTest expectations without fixing any additional observable bug.
- **Positional / index-based classification** — attempting to classify `bottomUpCols` entries via their factory index or metadata position risks drift after `propagateTopDownColumns0` reorders columns based on outer-query DFS. The existing alias-based path at line 3430 (`groupByMetadata.getColumnIndexQuiet(qcAlias)`) is the correct mechanism.
- **Banner comments** (`// ===`, `// ---`) as section headings in the new test methods. Java members auto-sort alphabetically so banners wander. Per CLAUDE.md.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Is this token an aggregate? | Hard-coded name list, `equalsIgnoreCase` chain | `functionParser.getFunctionFactoryCache().isGroupBy(token)` | Canonical; 20+ existing call sites in `SqlOptimiser.java`; auto-updates when new aggregate factories register |
| Is this function the timestamp bucket? | AST-shape matching on paramCount + arg types | `SqlUtil.isTimestampFloorFunction(ast)` | Covers both `timestamp_floor` and `timestamp_floor_from_offset_utc` factory names; already in use at line 3418 |
| Multi-key cartesian regression assertion | Row-count-only assertion + hand-parsed CSV | `assertQueryNoLeakCheck(expected, sql, "ts", false, false)` | Phase 14 D-15 convention; matches `testFillPrevKeyedIndependent` and `testFillPrevKeyedNoPrevYet` templates exactly |

**Key insight:** Every primitive needed for D-02/D-03/D-05 already exists as a reusable, zero-alloc field on `SqlCodeGenerator`. No new helper methods, no new utility classes.

## Common Pitfalls

### Pitfall 1: Assuming `qc.getAlias()` is non-null for all bottomUpCols entries

**What goes wrong:** If the alias is null for a function-key entry (hypothetical malformed AST path), `groupByMetadata.getColumnIndexQuiet(null)` could throw or return a misleading index. The D-02 alias assert catches this under `-ea`.

**Why it happens:** `bottomUpCols` entries carry an alias when the SELECT list element has an explicit name (either written by the user or auto-generated during parsing). Current admission paths in `SqlOptimiser` always populate aliases, but a future refactor could break the invariant.

**How to avoid:** D-02's alias assert explicitly tests `qc.getAlias() != null` before the `getColumnIndexQuiet` call. Belt-and-braces: the aggregate path at line 3428-3433 already uses the same `!= null` guard.

**Warning signs:** NPE in `groupByMetadata.getColumnIndexQuiet` under `-ea` with "non-aggregate FUNCTION/OPERATION in bottomUpCols must resolve to a non-timestamp factory key" in the stack trace.

### Pitfall 2: Dropping D-05 assertion message into a non-ASCII char

**What goes wrong:** `ast.token` inside the string concatenation is a `CharSequence` that may contain arbitrary user SQL text (e.g., `"cast"`, `"||"`, etc.). QuestDB's logger reliably handles ASCII. Since assertion messages go through normal `Throwable.getMessage()` rather than the logger, this is strictly a CLAUDE.md style rule — the assertion itself is correct. Keep the fixed parts of the message ASCII.

**Why it happens:** Easy to accidentally use an em dash (U+2014) in a "got type=X - token=Y" message.

**How to avoid:** Use plain hyphen (`-`) / comma / space. Both draft assertions in CONTEXT.md D-02 and D-05 are already ASCII-clean.

### Pitfall 3: Expected output row order

**What goes wrong:** Phase 3 established that SAMPLE BY FILL fast-path output order is `(bucket_ts ASC, key_discovery_order)` where `key_discovery_order` is pass-1 discovery order from `baseCursor.hasNext()`. The probe output in CONTEXT.md lists buckets A before B, but the cursor may emit B after A OR A after B depending on which row the base cursor yields first within a bucket.

**Why it happens:** `keysMap` is an OrderedMap (hash-bucketed LinkedHashMap semantics); pass-1 insertion order defines `keyPresent[]` iteration order. For cartesian gap rows, `keysMapCursor.toTop()` walks in insertion order.

**How to avoid:** Write regression tests with expected output matching pass-1 discovery order. When in doubt, run the test under the fixed code, capture actual output, and verify the semantic contract (6 rows, correct values per (bucket, key)) rather than force-rearranging rows. `testFillPrevKeyedIndependent:1800-1811` and `testFillPrevKeyedNoPrevYet:1888-1893` show the empirical row order — mirror those shapes.

### Pitfall 4: OPERATION `a || b` vs FUNCTION `concat(a, b)`

**What goes wrong:** The two parse differently: `a || b` becomes `ExpressionNode.OPERATION` with `token == "||"` and `paramCount == 2`; `concat(a, b)` becomes `ExpressionNode.FUNCTION` with `token == "concat"` and `paramCount == 2`. Both resolve to the same runtime `concat` function at execution, but the classifier sees different AST type tags. D-03 requires covering BOTH to close the bug for either syntax.

**Why it happens:** QuestDB's parser preserves the surface syntax as the AST node type. The SqlOptimiser admission at `:8066-8069` explicitly lists `LITERAL || FUNCTION || OPERATION`.

**How to avoid:** D-02 predicate `(ast.type == ExpressionNode.FUNCTION || ast.type == ExpressionNode.OPERATION) && !isGroupBy(token)` covers both. `testFillPrevConcatMultiKey` pins the FUNCTION branch; `testFillPrevConcatOperatorMultiKey` pins the OPERATION branch.

### Pitfall 5: Non-determinism from `SYMBOL` key cache

**What goes wrong:** If the fixture uses a `SYMBOL` column as an argument to `concat` or `cast`, the symbol ID ordering affects map iteration order which can affect output row order.

**How to avoid:** Use `STRING`, `VARCHAR`, `LONG`, `DOUBLE`, or `TIMESTAMP` column types for key-function arguments in the new tests. Only `testFillPrevKeyedIndependent` style (plain STRING key) is needed as a template.

## Code Examples

Verified patterns from the codebase:

### Classifier third-arm insertion (D-02)

```java
// Source: SqlCodeGenerator.java (applying D-02 to the existing classifier at :3405-3436)
if (ast.type == ExpressionNode.LITERAL) {
    continue;
}
if (SqlUtil.isTimestampFloorFunction(ast)) {
    continue;
}
// D-02: non-aggregate FUNCTION/OPERATION grouping key (interval, concat, cast, ||)
if ((ast.type == ExpressionNode.FUNCTION || ast.type == ExpressionNode.OPERATION)
        && !functionParser.getFunctionFactoryCache().isGroupBy(ast.token)) {
    assert qc.getAlias() != null
            && groupByMetadata.getColumnIndexQuiet(qc.getAlias()) >= 0
            && groupByMetadata.getColumnIndexQuiet(qc.getAlias()) != timestampIndex
        : "generateFill: non-aggregate FUNCTION/OPERATION in bottomUpCols must resolve to a non-timestamp factory key";
    continue;
}
```

### Aggregate-arm lock-in (D-05)

```java
// Source: SqlCodeGenerator.java (inserting at the aggregate arm entry, before userFillIdx++ at :3435)
assert ast.type == ExpressionNode.FUNCTION
        && functionParser.getFunctionFactoryCache().isGroupBy(ast.token)
    : "generateFill aggregate arm: expected aggregate FUNCTION, got type=" + ast.type + " token=" + ast.token;
final CharSequence qcAlias = qc.getAlias();
if (qcAlias != null) {
    final int factoryIdx = groupByMetadata.getColumnIndexQuiet(qcAlias);
    if (factoryIdx >= 0 && factoryIdx != timestampIndex) {
        factoryColToUserFillIdx.setQuick(factoryIdx, userFillIdx);
    }
}
userFillIdx++;
```

### Test template — multi-key interval FILL(PREV) cartesian

```java
// Source: new test, modeled on SampleByFillTest.testFillPrevKeyedIndependent:1785-1813 + testFillPrevInterval:1677-1707
@Test
public void testFillPrevIntervalMultiKey() throws Exception {
    // Two distinct interval(lo, hi) keys across three buckets should produce
    // 2 x 3 = 6 cartesian rows. Pre-fix classifier dropped the function-valued
    // key from keyColIndices and collapsed output to 3 rows.
    assertMemoryLeak(() -> {
        execute("""
                CREATE TABLE t (
                    lo TIMESTAMP,
                    hi TIMESTAMP,
                    v DOUBLE,
                    ts TIMESTAMP
                ) TIMESTAMP(ts) PARTITION BY DAY""");
        execute("""
                INSERT INTO t VALUES
                    ('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 10.0, '2024-01-01T00:00:00.000000Z'),
                    ('2021-01-01T00:00:00.000Z'::TIMESTAMP, '2021-02-01T00:00:00.000Z'::TIMESTAMP, 20.0, '2024-01-01T02:00:00.000000Z')""");
        assertQueryNoLeakCheck(
                """
                        ts\tk\tfirst
                        2024-01-01T00:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                        2024-01-01T00:00:00.000000Z\t('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')\tnull
                        2024-01-01T01:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                        2024-01-01T01:00:00.000000Z\t('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')\tnull
                        2024-01-01T02:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                        2024-01-01T02:00:00.000000Z\t('2021-01-01T00:00:00.000Z', '2021-02-01T00:00:00.000Z')\t20.0
                        """,
                "SELECT ts, interval(lo, hi) k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                "ts", false, false
        );
    });
}
```

Row order matches `testFillPrevKeyedIndependent`'s observed shape (pass-1 discovery order: key A discovered at bucket 00:00, key B discovered at bucket 02:00). The fixture uses two DIFFERENT `(lo, hi)` pairs to ensure two distinct `interval` values — the discriminant that exposes the bug.

### Test fixture shapes for the other four variants

| Test | Key expression | Key values | Notes |
|------|---------------|-----------|-------|
| `testFillPrevIntervalMultiKey` | `interval(lo, hi)` | `('2020-01-01Z','2020-02-01Z')` at 00:00, `('2021-01-01Z','2021-02-01Z')` at 02:00 | INTERVAL FILL_KEY dispatch (FillRecord.getInterval) |
| `testFillPrevConcatMultiKey` | `concat(a, b) k` (FUNCTION form) | `a='Lon', b='don'` at 00:00, `a='Par', b='is'` at 02:00 (STRING columns) | Produces STRING-valued k column; FillRecord.getStrA FILL_KEY dispatch |
| `testFillPrevCastMultiKey` | `cast(x AS STRING) k` | `x=1` at 00:00, `x=2` at 02:00 (INT column) | Produces STRING k; exercises FILL_KEY on cast-originated column |
| `testFillPrevConcatOperatorMultiKey` | `a \|\| b k` (OPERATION form) | same data as testFillPrevConcatMultiKey | Pins the OPERATION branch of D-02 predicate |
| `testFillNullCastMultiKey` | `cast(x AS STRING) k` with `FILL(NULL)` | same data as testFillPrevCastMultiKey; aggregate column fills with null on gap rows | Representative FILL(NULL) case; key column still dispatches via FILL_KEY |

Each fixture: 2 distinct key values x 3 buckets = 6 expected cartesian rows. For FILL(PREV) variants, the second key discovered at bucket 02:00 produces `null` at bucket 00:00 (no prev yet) and fills forward from 02:00. For FILL(NULL) variant, every gap aggregate is `null`.

## Runtime State Inventory

N/A — greenfield correctness-fix phase. No rename/refactor/migration. Classifier fix is pure compile-time AST dispatch; no stored data, live service config, OS-registered state, secrets/env vars, or build artifacts are affected.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| 3-way classifier partition (LITERAL / timestamp_floor / aggregate) | 4-way partition (+ non-aggregate FUNCTION/OPERATION key) | Phase 16 (this) | Closes silent cartesian-row drop for inline-function grouping keys |
| Runtime safety-net reclassification | Codegen fast-path + explicit -ea lock-in + retro-fallback deleted | Phase 12-13 | Phase 16 builds on the cleaned codegen dispatch surface; no legacy-fallback escape hatch to fall back on |
| Materialized PREV value snapshots per-key | rowId-based replay via `baseCursor.recordAt(prevRecord, rowId)` | Phase 13 | Phase 16 function-key FILL_KEY dispatch reads from `keysMapRecord`, orthogonal to PREV snapshot storage |

**Deprecated/outdated:** None directly invalidated by this phase. The only prior convention affected is the implicit "only LITERAL and timestamp_floor count as non-aggregate" classifier assumption, which this phase explicitly broadens.

## Environment Availability

Not applicable — code-only change to existing Java module. No new runtime dependencies, no external tooling, no CLI utilities beyond the existing Maven build chain (Java 17, mvn 3). Running the new tests requires:

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Java 17+ | Source compilation | Assumed per pom.xml `javac.target=17` | — | — |
| Maven 3 | Build + test runner | Assumed per project convention | — | — |
| `-ea` (assertions enabled) | D-05 defensive assert fires in CI | ✓ | core/pom.xml:37 | Manual `java -ea` when running locally |

[VERIFIED: core/pom.xml:37 surefire `argLine=-ea -Dfile.encoding=UTF-8 -XX:+UseParallelGC`]

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | JUnit 4 (QuestDB uses `org.junit.Test` imports + `AbstractCairoTest` superclass harness) |
| Config file | `core/pom.xml` surefire plugin config (existing) |
| Quick run command | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevIntervalMultiKey test` |
| Full suite command | `mvn -pl core -Dtest=SampleByFillTest test` |

### Phase Requirements -> Test Map

| Req | Behavior | Test Type | Automated Command | File Exists? |
|-----|----------|-----------|-------------------|--------------|
| COR/KEY/XPREV (interval) | 2 distinct `interval(lo,hi)` keys, FILL(PREV), 6-row cartesian | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevIntervalMultiKey test` | Wave 0 |
| COR/KEY/XPREV (concat FUNCTION) | 2 distinct `concat(a,b)` keys, FILL(PREV), 6-row cartesian | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevConcatMultiKey test` | Wave 0 |
| COR/KEY/XPREV (cast) | 2 distinct `cast(x AS STRING)` keys, FILL(PREV), 6-row cartesian | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevCastMultiKey test` | Wave 0 |
| COR/KEY/XPREV (`\|\|` OPERATION) | 2 distinct `a\|\|b` keys, FILL(PREV), 6-row cartesian | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevConcatOperatorMultiKey test` | Wave 0 |
| FILL-02 (NULL + cast) | `cast(x AS STRING)` key + FILL(NULL), 6-row cartesian with null aggregates | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillNullCastMultiKey test` | Wave 0 |
| Regression anchor | Single-key `interval(lo,hi)` + FILL(PREV) still works | integration | `mvn -pl core -Dtest=SampleByFillTest#testFillPrevInterval test` | ✅ exists at line 1678 |
| D-05 drift lock-in | Aggregate-arm assertion fires on any future AST-shape drift | -ea runtime assert | Fires inside any FILL test under `-ea` | ✅ inherent via surefire `-ea` |
| Plan-text stability | ExplainPlanTest / SqlOptimiserTest unchanged (codegen-only fix) | integration | `mvn -pl core -Dtest=ExplainPlanTest test; mvn -pl core -Dtest=SqlOptimiserTest test` | ✅ existing suites |
| Existing FILL suite | Every other FILL test still passes (no regression) | integration | `mvn -pl core -Dtest=SampleByFillTest test` | ✅ entire class |

### Sampling Rate

- **Per task commit:** `mvn -pl core -Dtest=SampleByFillTest test` (the entire SAMPLE BY FILL class — ~2-3 min; covers both the 5 new tests and every existing FILL regression).
- **Per wave merge:** `mvn -pl core -Dtest='SampleByFillTest,SampleByTest,SampleByNanoTimestampTest,ExplainPlanTest,SqlOptimiserTest' test` (surrounding surfaces to catch plan-text drift or cross-file regression).
- **Phase gate:** Full `mvn -pl core test` suite green before `/gsd-verify-work`.

### Wave 0 Gaps

- [ ] `SampleByFillTest#testFillPrevIntervalMultiKey` — covers the interval FILL(PREV) cartesian
- [ ] `SampleByFillTest#testFillPrevConcatMultiKey` — covers `concat(a,b)` FUNCTION form
- [ ] `SampleByFillTest#testFillPrevCastMultiKey` — covers `cast(x AS STRING)`
- [ ] `SampleByFillTest#testFillPrevConcatOperatorMultiKey` — covers `a || b` OPERATION form
- [ ] `SampleByFillTest#testFillNullCastMultiKey` — covers FILL(NULL) variant
- No framework install needed; existing JUnit 4 + AbstractCairoTest harness covers everything.

## Plan Count Recommendation

**Recommend: single plan.**

Rationale:
1. CONTEXT.md Claude's Discretion explicitly names single-plan as the default per Phase 14/15 file-clustered convention.
2. The production diff is ~25 lines confined to one method (`SqlCodeGenerator.generateFill`). The test diff is ~200 lines confined to one class (`SampleByFillTest`). Total: two files, one commit.
3. Splitting classifier fix from tests would land the classifier fix with no regression coverage in its own commit — bisect-hostile.
4. Splitting the assertion (D-05) from the classifier fix violates CONTEXT.md D-05 explicitly: "lands in the SAME commit as the classifier fix."
5. The defensive assertion CANNOT land before the fix (the existing `testFillPrevInterval` single-key test would currently fail under `-ea` due to the bug's classification of `interval` as aggregate — confirmed by the diagnostic probe in the todo at line 57-65).

Single plan, ~5 tasks:
1. Classifier widening + D-02 alias assert + D-05 aggregate-arm assert (production change)
2. `testFillPrevIntervalMultiKey` (new regression test)
3. `testFillPrevConcatMultiKey` + `testFillPrevConcatOperatorMultiKey` (FUNCTION + OPERATION concat variants)
4. `testFillPrevCastMultiKey` (cast variant)
5. `testFillNullCastMultiKey` (FILL(NULL) representative)

All five land in a single commit with a ~50-char title plus long-form body.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Expected row order in regression tests matches pass-1 discovery order (key A at bucket 00:00 before key B at bucket 02:00, then every subsequent bucket emits A then B) | Test template, Pitfall 3 | Test asserts wrong row order; planner should run each fixture once in Wave 0 to capture the actual observed order and freeze. `testFillPrevKeyedIndependent:1800-1811` and `testFillPrevKeyedNoPrevYet:1888-1893` show this pattern is the established convention, but OrderedMap internal hash order is sensitive to key byte content. Mitigation: planner captures observed output under the fixed code before committing expected strings. |
| A2 | The "leading bucket with no prev yet" cell renders as `null` (not empty, not omitted) | Test template | Partial — pinned by `testFillPrevKeyedNoPrevYet:1890` for DOUBLE `avg` output. Other types (INTERVAL, STRING) may render differently. Mitigation: capture actual output during task execution. For INTERVAL specifically, `FillRecord.getInterval` returns `Interval.NULL` on the default branch (verified at SampleByFillRecordCursorFactory.java:1020), and `Interval.NULL` renders as empty text per Phase 14 D-15 conventions — may differ from the "null" literal. Planner should probe first for INTERVAL variant. |
| A3 | The `functionParser.getFunctionFactoryCache().isGroupBy("concat")` returns `false` (concat is not registered as a groupBy function) | D-02 predicate | If `isGroupBy("concat")` returns `true`, the D-02 predicate would misclassify `concat(a,b)` as an aggregate and the bug would persist. `concat` is a string function factory, not a groupBy factory — extremely unlikely to be registered in `groupByFunctionNames`. Mitigation: the `testFillPrevConcatMultiKey` test will fail loudly if this assumption is wrong. |

## Open Questions (RESOLVED)

1. **Exact output row order for INTERVAL `null` rendering** — RESOLVED: `16-01-PLAN.md` Tasks 2-5 apply the probe-and-freeze protocol (run test under fixed classifier, capture actual output verbatim, freeze as expected string). Interval.NULL renders differently from DOUBLE null; OrderedMap hash-dependent row ordering forbids guessing. Phase 15 Plan 03 precedent.

## Security Domain

Not applicable at phase scope. This is a query-engine correctness fix with no authentication, authorization, input-validation, cryptography, or data-at-rest implications. No user-supplied inputs change trust boundaries. Pre-fix, the bug silently under-reports query results — a correctness issue but not an info-disclosure or privilege-escalation vector. [CITED: `.planning/config.json` `security_enforcement` default policy]

## Existing FILL Tests with Inline-Function Keys (Safety Check)

Grep across `core/src/test/java/io/questdb/test/griffin/engine/groupby/` for `SAMPLE BY.*interval(`, `SAMPLE BY.*concat(`, `SAMPLE BY.*cast(` produces four matches:

| Location | Usage | Post-fix impact |
|----------|-------|-----------------|
| `SampleByFillTest.java:82, 104, 126` | `FILL(cast('42' AS DECIMAL(...))` — cast is in the FILL VALUE, not in the grouping key | No impact. FILL-value casts parse independently of the classifier loop. |
| `SampleByFillTest.java:1705` (`testFillPrevInterval`) | `interval(lo, hi)` as the grouping key, single-key fixture | No output change (single-key fixture; FILL_PREV_SELF and FILL_KEY produce identical output). Test remains the INTERVAL FillRecord plumbing anchor. |

Grep across `core/src/test/java/io/questdb/test/griffin/ExplainPlanTest.java` and `SqlOptimiserTest.java` for the same patterns: **zero matches**. No plan-text assertion currently covers inline-function grouping keys + SAMPLE BY, so D-02's codegen-only change cannot drift any existing plan expectation.

Grep across `SampleByTest.java` / `SampleByNanoTimestampTest.java`: **zero SAMPLE BY matches with inline-function keys**.

[VERIFIED: grep across `core/src/test/java/io/questdb/test/griffin`]

## Sources

### Primary (HIGH confidence)

- Direct read of production code: `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` lines 474, 3275, 3346, 3351, 3405-3436, 3447-3528, 3668-3763
- Direct read of cursor code: `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` lines 84-183, 287-363, 705-1200
- Direct read of helper: `core/src/main/java/io/questdb/griffin/FunctionFactoryCache.java:135` (`isGroupBy`)
- Direct read of helper: `core/src/main/java/io/questdb/griffin/SqlUtil.java:1256-1260` (`isTimestampFloorFunction`)
- Direct read of admission: `core/src/main/java/io/questdb/griffin/SqlOptimiser.java:8066-8069` (`maybeKeyed` construction)
- Direct read of build config: `core/pom.xml:37` (surefire `-ea`)
- Direct read of fixture templates: `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java:1677-1898`
- CONTEXT.md decisions (D-01..D-06) + Canonical References section
- Phase 14/15 CONTEXT.md (file-clustered plans, D-15 factory-property convention, same-commit lock-in precedent)
- TODO at `.planning/todos/pending/2026-04-21-fix-multi-key-fill-prev-with-inline-function-grouping-keys.md` (empirical probe + root-cause trace)

### Secondary (MEDIUM confidence)

- Grep-based survey of existing inline-function SAMPLE BY tests across `core/src/test/java/io/questdb/test/griffin` (negative result — no plan-text risk)
- STATE.md progress narrative for Phase 12-15 decisions (confirms codegen-side bias, -ea lock-in pattern)

### Tertiary (LOW confidence)

- None. All research conducted directly against the source tree; no WebSearch / Context7 invocation required — QuestDB internal architecture is the sole domain.

## Metadata

**Confidence breakdown:**
- Classifier fix mechanics (D-02/D-03): HIGH — direct code read + prior-phase precedent
- Cursor-side wiring automatic flow-through (D-06): HIGH — traced end-to-end from `factoryColToUserFillIdx[-1]` through `fillModes[FILL_KEY]` -> `keyColIndices` -> `outputColToKeyPos[col] >= 0` -> `FillRecord.getX(keysMapRecord.getX(outputColToKeyPos[col]))`
- `aggNonKeyCount` downstream impact: HIGH — only one consumer at line 3479, behavior change is strictly a correctness improvement
- Plan-text safety: HIGH — zero matches in ExplainPlanTest / SqlOptimiserTest for the affected query shapes
- Test fixture row order (A1): MEDIUM — pattern matches established Phase 3/14 fixtures but OrderedMap hash order is data-content sensitive; probe-and-freeze recommended in task 0
- INTERVAL null rendering (A2): MEDIUM — `Interval.NULL` default return at line 1020 but exact terminal rendering may differ from DOUBLE null

**Research date:** 2026-04-21
**Valid until:** 2026-05-21 (30 days) — underlying `SqlCodeGenerator.generateFill` and `SampleByFillRecordCursorFactory` are stable post-Phase-15 close.
