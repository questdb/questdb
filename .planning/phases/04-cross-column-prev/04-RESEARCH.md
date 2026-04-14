# Phase 4: Cross-Column Prev — Research

**Researched:** 2026-04-09
**Domain:** SQL fill expression parsing, optimizer gate, generateFill() code generation, fill cursor dispatch
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **Syntax**: `PREV(col_name)` inside a FILL clause references a named output column alias.
- **Reference resolution**: `col_name` in `PREV(col_name)` resolves to the output column alias via `metadata.getColumnIndexQuiet(alias)` in `generateFill()`.
- **Fill mode encoding**: `fillModes[col] >= 0` means cross-column prev referencing column at that index. Already supported by `prevValue()` non-keyed path.
- **Scope**: Cross-column prev works for both keyed and non-keyed queries.

### Claude's Discretion

(none specified)

### Deferred Ideas (OUT OF SCOPE)

- `PREV(expression)` — reference an arbitrary expression, not just a column alias
- `PREV` with type coercion — filling a DOUBLE column from an INT column's prev
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| XPREV-01 | FILL(PREV) can reference a specific column from the previous bucket (not just self) | `prevValue()` non-keyed already handles `mode >= 0`; keyed needs fix |
| XPREV-02 | Syntax and semantics defined (FILL(PREV(col_name)) referencing output alias) | ExpressionNode FUNCTION type with paramCount=1 distinguishes from bare PREV |
</phase_requirements>

---

## Summary

Phase 4 adds `FILL(PREV(col_name))` syntax where a gap-fill column's value comes from a different column's previous bucket value rather than its own. Three code areas require changes: (1) the optimizer gate must allow multi-expression fill lists through the GROUP BY fast path, (2) `generateFill()` must detect `PREV(col_name)` and resolve the alias to a fill mode index, and (3) `prevValue()` in the fill cursor must apply the source column index for the keyed path.

The CONTEXT.md claims "SampleByFillRecordCursorFactory — zero changes" and "The gate at SqlOptimiser.java:7880 was already removed in Phase 3. No change needed." Both claims are incorrect. The gate still restricts fill list size to 0 or 1, which blocks multi-expression fill specs. The keyed `prevValue()` ignores fill mode and always uses `outputColToAggSlot[col]`, so it will read its own prev instead of the cross-column source.

**Primary recommendation**: Make three targeted changes — optimizer gate, `generateFill()` detection, `prevValue()` keyed path — and add four tests (non-keyed cross-column, keyed cross-column, bad alias error, multi-fill with PREV(x) mixed with NULL).

---

## Project Constraints (from CLAUDE.md)

- Use `ObjList<T>` instead of `T[]` for object arrays.
- Boolean variable and method names must use `is...` or `has...` prefix.
- Use `assertMemoryLeak()` for all tests that allocate native memory. Narrow unit tests that don't allocate native memory do not need it.
- Use `assertQueryNoLeakCheck()` to assert results of queries (validates factory properties). Use `assertSql()` for storage/cursor tests where factory properties are irrelevant.
- Use `execute()` for non-queries (DDL).
- Use a single `INSERT INTO ... VALUES (...)` for multiple rows.
- Use multiline strings for complex queries and expected results.
- Use underscore to separate thousands in numbers with 5+ digits.
- Error positions: `SqlException.$(position, msg)` should point at the offending character, not the start of expression.
- Alphabetical ordering of class members by kind and visibility.

---

## Standard Stack

No new libraries. All changes are in existing Java source files.

### Files Modified

| File | Location | Change Required |
|------|----------|-----------------|
| `SqlOptimiser.java` | `griffin/` | Relax fill-size gate to allow multi-fill specs (line 7880) |
| `SqlCodeGenerator.java` | `griffin/` | Detect `PREV(col_name)` in `generateFill()`, resolve alias to index |
| `SampleByFillRecordCursorFactory.java` | `griffin/engine/groupby/` | Fix `prevValue()` keyed path for cross-column mode |
| `SampleByFillTest.java` | `test/.../engine/groupby/` | Add cross-column prev tests |

---

## Architecture Patterns

### ExpressionNode Type for PREV vs PREV(col_name)

[VERIFIED: codebase grep + ExpressionParser.java lines 949-952]

The expression parser converts a LITERAL to a FUNCTION when it encounters following parentheses. Therefore:

| Fill expression | `type` | `paramCount` | `rhs` |
|----------------|--------|-------------|-------|
| `PREV` (bare) | `ExpressionNode.LITERAL` | `0` | `null` |
| `PREV(col_name)` | `ExpressionNode.FUNCTION` | `1` | LITERAL node with `token=col_name` |
| `NULL` (bare) | `ExpressionNode.LITERAL` | `0` | `null` |
| `0` (number) | `ExpressionNode.CONSTANT` | `0` | `null` |

Detection predicate for bare PREV:
```java
// Source: ExpressionNode.java + SqlKeywords.java + ExpressionParser.java
isPrevKeyword(expr.token) && expr.type == ExpressionNode.LITERAL
```

Detection predicate for PREV(col_name):
```java
// Source: ExpressionNode.java + ExpressionParser.java lines 949-952
isPrevKeyword(expr.token)
    && expr.type == ExpressionNode.FUNCTION
    && expr.paramCount == 1
    && expr.rhs != null
    && expr.rhs.type == ExpressionNode.LITERAL
```

### Fill Expression Parsing Path (SqlParser.java)

[VERIFIED: SqlParser.java line 2889-2904]

The FILL clause is parsed by `expr()` for each comma-separated value inside the parentheses. `expr()` returns the full AST including function calls. No special tokenization for fill keywords — `PREV(col_name)` arrives in `generateFill()` already as a FUNCTION node.

```java
// SqlParser.java lines 2889-2896 (parser for FILL clause)
if (tok != null && isFillKeyword(tok)) {
    expectTok(lexer, '(');
    do {
        final ExpressionNode fillNode = expr(lexer, model, sqlParserCallback, model.getDecls());
        model.addSampleByFill(fillNode);
        tok = tokIncludingLocalBrace(lexer, "',' or ')'");
        if (Chars.equals(tok, ')')) break;
        expectTok(tok, lexer.lastTokenPosition(), ',');
    } while (true);
}
```

### Optimizer Gate (SqlOptimiser.java line 7875-7882)

[VERIFIED: SqlOptimiser.java lines 7875-7882]

Current gate condition (Phase 3 state):
```java
&& (sampleByFillSize == 0
    || (sampleByFillSize == 1 && !isLinearKeyword(sampleByFill.getQuick(0).token)))
```

This passes for:
- `FILL` not present (`sampleByFillSize == 0`)
- Single fill expression that is not LINEAR (`sampleByFillSize == 1`)

This BLOCKS:
- `FILL(PREV(a), NULL)` — `sampleByFillSize == 2`, both conditions false → optimizer bails

The gate must be relaxed to: **allow if no fill expression has LINEAR**. Required change:

```java
// Replace line 7880 with:
&& !hasLinearFill(sampleByFill)
```

where `hasLinearFill` is a new private helper:
```java
private static boolean hasLinearFill(ObjList<ExpressionNode> fill) {
    for (int i = 0, n = fill.size(); i < n; i++) {
        if (isLinearKeyword(fill.getQuick(i).token)) {
            return true;
        }
    }
    return false;
}
```

### generateFill() — fillValues Construction Loop (line 3254-3266)

[VERIFIED: SqlCodeGenerator.java lines 3254-3266]

Current loop treats `isPrevKeyword(expr.token)` as a skip-functionParser signal. This handles both bare PREV and PREV(col_name) correctly because neither should be parsed as a function by `functionParser` (which would fail with "Invalid column: PREV").

No change needed here. The `NullConstant.NULL` placeholder for PREV(col_name) in `fillValues` is harmless — it is never read for PREV modes.

### generateFill() — anyPrev Detection (lines 3332-3337)

[VERIFIED: SqlCodeGenerator.java lines 3332-3337]

`anyPrev` is set true if `isPrevKeyword(expr.token)` for any fill expression. This catches `PREV(col_name)` correctly since the token of the FUNCTION node is still "PREV". No change needed here.

### generateFill() — Bare FILL(PREV) Fast Path (line 3345)

[VERIFIED: SqlCodeGenerator.java line 3345]

Current check:
```java
if (anyPrev && fillValuesExprs.size() == 1 && isPrevKeyword(fillValuesExprs.getQuick(0).token)) {
```

This incorrectly enters the "bare FILL(PREV)" path for `FILL(PREV(col_name))` (single-fill, token is "PREV"). Must add type check:

```java
// PREV with no argument — bare FILL(PREV), all aggregates fill from self
if (anyPrev && fillValuesExprs.size() == 1
        && isPrevKeyword(fillValuesExprs.getQuick(0).token)
        && fillValuesExprs.getQuick(0).type == ExpressionNode.LITERAL) {
```

### generateFill() — Per-Column Fill Mode Assignment (lines 3374-3389)

[VERIFIED: SqlCodeGenerator.java lines 3374-3389]

Current check at line 3377:
```java
if (fillExpr != null && isPrevKeyword(fillExpr.token)) {
    fillModes.add(SampleByFillRecordCursorFactory.FILL_PREV_SELF);
    ...
```

This catches both bare PREV (type=LITERAL) and PREV(col_name) (type=FUNCTION). Must split:

```java
if (fillExpr != null && isPrevKeyword(fillExpr.token)) {
    if (fillExpr.type == ExpressionNode.FUNCTION && fillExpr.paramCount == 1
            && fillExpr.rhs != null && fillExpr.rhs.type == ExpressionNode.LITERAL) {
        // PREV(col_name) — cross-column prev
        CharSequence alias = fillExpr.rhs.token;
        int srcColIdx = groupByMetadata.getColumnIndexQuiet(alias);
        if (srcColIdx < 0) {
            throw SqlException.$(fillExpr.rhs.position,
                    "PREV(col): column not found in output: ").put(alias);
        }
        fillModes.add(srcColIdx);
    } else {
        // bare PREV — self-prev
        fillModes.add(SampleByFillRecordCursorFactory.FILL_PREV_SELF);
    }
    constantFillFuncs.add(NullConstant.NULL);
    hasPrevFill = true;
}
```

`groupByMetadata` is already in scope as `final RecordMetadata groupByMetadata = groupByFactory.getMetadata()` at line 3325. [VERIFIED: SqlCodeGenerator.java line 3325]

`getColumnIndexQuiet()` returns `-1` for unknown alias — use this to generate a user-facing error. Use `fillExpr.rhs.position` for the error position (points at the column name token, consistent with SqlException position convention). [VERIFIED: RecordMetadata.java lines 56-62, 71-73]

### prevValue() — Keyed Cross-Column (SampleByFillRecordCursorFactory.java)

[VERIFIED: SampleByFillRecordCursorFactory.java lines 547-571]

Current keyed path:
```java
private long prevValue(int col) {
    if (keysMap != null) {
        MapValue value = keysMapRecord.getValue();
        boolean hasPrev = value.getLong(HAS_PREV_SLOT) != 0;
        if (!hasPrev) return Numbers.LONG_NULL;
        int aggSlot = outputColToAggSlot[col];   // BUG for cross-column
        if (aggSlot >= 0) return value.getLong(PREV_START_SLOT + aggSlot);
        return Numbers.LONG_NULL;
    }
    // non-keyed
    if (hasSimplePrev) {
        int mode = fillMode(col);
        if (mode == FILL_PREV_SELF) return simplePrev[col];
        if (mode >= 0) return simplePrev[mode];   // already correct
    }
    return Numbers.LONG_NULL;
}
```

The keyed path ignores fill mode and always uses `outputColToAggSlot[col]`. For cross-column prev (`mode >= 0`), it should use `outputColToAggSlot[mode]` (the source column's agg slot).

Required fix:
```java
private long prevValue(int col) {
    if (keysMap != null) {
        MapValue value = keysMapRecord.getValue();
        boolean hasPrev = value.getLong(HAS_PREV_SLOT) != 0;
        if (!hasPrev) return Numbers.LONG_NULL;
        int mode = fillMode(col);
        int sourceCol = mode >= 0 ? mode : col;   // cross-column uses source
        int aggSlot = outputColToAggSlot[sourceCol];
        if (aggSlot >= 0) return value.getLong(PREV_START_SLOT + aggSlot);
        return Numbers.LONG_NULL;
    }
    // non-keyed — unchanged
    if (hasSimplePrev) {
        int mode = fillMode(col);
        if (mode == FILL_PREV_SELF) return simplePrev[col];
        if (mode >= 0) return simplePrev[mode];
    }
    return Numbers.LONG_NULL;
}
```

The `outputColToAggSlot` array is indexed by output column index (0..columnCount-1), with `-1` for key and timestamp columns. For a valid cross-column source alias, `srcColIdx` must not be a key column or timestamp — validate this in `generateFill()` if needed (or let `aggSlot == -1` fall through to `LONG_NULL`).

### Fill Mode Index vs. Constants

[VERIFIED: SampleByFillRecordCursorFactory.java lines 70-72]

```java
public static final int FILL_CONSTANT = -1;   // constant or null
public static final int FILL_PREV_SELF = -2;  // prev of same column
public static final int FILL_KEY = -3;        // key column value
```

Cross-column index `>= 0` does not collide with any named constant. Column indices for a typical query are 0..N-1 where N is small (usually < 20). No ambiguity.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Column alias resolution | Custom name lookup | `RecordMetadata.getColumnIndexQuiet(alias)` | Already handles case-insensitive lookup |
| PREV detection | Custom string comparison | `SqlKeywords.isPrevKeyword(token)` | Case-insensitive, already used throughout |

---

## Common Pitfalls

### Pitfall 1: PREV(col_name) Misidentified as Bare PREV

**What goes wrong:** Both bare `PREV` and `PREV(col_name)` have `token == "PREV"`. If code only checks `isPrevKeyword(expr.token)` without checking `expr.type`, it treats them identically — assigning `FILL_PREV_SELF` instead of the cross-column index.

**Why it happens:** `isPrevKeyword()` is a keyword check that predates function-call syntax for fill expressions.

**How to avoid:** Always check `expr.type == ExpressionNode.LITERAL` for bare PREV and `expr.type == ExpressionNode.FUNCTION && expr.paramCount == 1` for `PREV(col_name)`.

**Warning signs:** Tests with `FILL(PREV(a))` produce values identical to `FILL(PREV)` — the column fills from itself instead of the named source.

### Pitfall 2: Optimizer Gate Blocks Multi-Fill Specs

**What goes wrong:** The gate at SqlOptimiser.java:7880 allows only `sampleByFillSize == 1` (or 0). `FILL(PREV(a), NULL)` has `sampleByFillSize == 2` and silently falls through to the old cursor path instead of the fast path.

**Why it happens:** The gate was originally designed for single-fill forms. Phase 3 only removed the PREV exclusion, not the size restriction.

**How to avoid:** Replace the size check with a "no LINEAR" check across all fill expressions.

**Warning signs:** Query plan shows `SampleByFillPrev` (old cursor path) instead of `Sample By Fill` on top of `Async Group By`.

### Pitfall 3: Keyed prevValue() Ignores Mode

**What goes wrong:** `prevValue(col)` in the keyed path uses `outputColToAggSlot[col]` unconditionally. For cross-column prev (`mode >= 0`), this reads the current column's prev instead of the source column's prev.

**Why it happens:** The keyed path was written before cross-column prev existed. The non-keyed path already has the `mode >= 0` check but the keyed path doesn't.

**How to avoid:** Read `fillMode(col)` inside `prevValue()` for the keyed case and use `outputColToAggSlot[mode]` when `mode >= 0`.

**Warning signs:** Keyed cross-column prev tests produce the same value as self-prev instead of the referenced column's value.

### Pitfall 4: Error Position Points at PREV Keyword, Not Column Name

**What goes wrong:** When `PREV(unknown_col)` is used, the error position points at `PREV` (the function node position) instead of `unknown_col`.

**How to avoid:** Use `fillExpr.rhs.position` (the argument node position) for the error, not `fillExpr.position`.

---

## Code Examples

### Detecting PREV(col_name) in generateFill()

```java
// Source: verified pattern from ExpressionNode.java + ExpressionParser.java
if (fillExpr != null && isPrevKeyword(fillExpr.token)) {
    if (fillExpr.type == ExpressionNode.FUNCTION
            && fillExpr.paramCount == 1
            && fillExpr.rhs != null
            && fillExpr.rhs.type == ExpressionNode.LITERAL) {
        // PREV(col_name) — cross-column prev
        CharSequence alias = fillExpr.rhs.token;
        int srcColIdx = groupByMetadata.getColumnIndexQuiet(alias);
        if (srcColIdx < 0) {
            throw SqlException.$(fillExpr.rhs.position,
                    "PREV(col): column not found in output: ").put(alias);
        }
        fillModes.add(srcColIdx);           // mode >= 0 = cross-column index
    } else {
        // bare PREV — self-prev
        fillModes.add(SampleByFillRecordCursorFactory.FILL_PREV_SELF);
    }
    constantFillFuncs.add(NullConstant.NULL);
    hasPrevFill = true;
}
```

### Bare FILL(PREV) Fast Path Guard (generateFill() line 3345)

```java
// Source: SqlCodeGenerator.java line 3345 — add LITERAL check
if (anyPrev
        && fillValuesExprs.size() == 1
        && isPrevKeyword(fillValuesExprs.getQuick(0).token)
        && fillValuesExprs.getQuick(0).type == ExpressionNode.LITERAL) {
    // bare FILL(PREV) — all aggregate columns fill from self
    ...
}
```

### Optimizer Gate hasLinearFill Helper

```java
// Source: derived from SqlOptimiser.java pattern (isLinearKeyword already exists)
private static boolean hasLinearFill(ObjList<ExpressionNode> fill) {
    for (int i = 0, n = fill.size(); i < n; i++) {
        if (isLinearKeyword(fill.getQuick(i).token)) {
            return true;
        }
    }
    return false;
}
```

Gate condition change at line 7880:
```java
// Before:
&& (sampleByFillSize == 0
    || (sampleByFillSize == 1 && !isLinearKeyword(sampleByFill.getQuick(0).token)))

// After:
&& !hasLinearFill(sampleByFill)
```

### prevValue() Fix in SampleByFillCursor (keyed path)

```java
// Source: SampleByFillRecordCursorFactory.java lines 547-571
private long prevValue(int col) {
    if (keysMap != null) {
        MapValue value = keysMapRecord.getValue();
        boolean hasPrev = value.getLong(HAS_PREV_SLOT) != 0;
        if (!hasPrev) return Numbers.LONG_NULL;
        int mode = fillMode(col);
        int sourceCol = mode >= 0 ? mode : col;   // cross-column: read source
        int aggSlot = outputColToAggSlot[sourceCol];
        if (aggSlot >= 0) return value.getLong(PREV_START_SLOT + aggSlot);
        return Numbers.LONG_NULL;
    }
    if (hasSimplePrev) {
        int mode = fillMode(col);
        if (mode == FILL_PREV_SELF) return simplePrev[col];
        if (mode >= 0) return simplePrev[mode];
    }
    return Numbers.LONG_NULL;
}
```

### Test Pattern (from SampleByFillTest.java)

```java
@Test
public void testFillPrevCrossColumnNonKeyed() throws Exception {
    assertMemoryLeak(() -> {
        execute("CREATE TABLE x (val DOUBLE, ival INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO x VALUES " +
                "(1.0, 10, '2024-01-01T00:00:00.000000Z')," +
                "(3.0, 30, '2024-01-01T02:00:00.000000Z')");
        // s fills from itself (PREV), a fills from s's prev value (PREV(s))
        assertSql(
                """
                        ts\ts\ta
                        2024-01-01T00:00:00.000000Z\t1.0\t1.0
                        2024-01-01T01:00:00.000000Z\t1.0\t1.0
                        2024-01-01T02:00:00.000000Z\t3.0\t30.0
                        """,
                "SELECT ts, sum(val) AS s, sum(ival::DOUBLE) AS a " +
                "FROM x SAMPLE BY 1h FILL(PREV, PREV(s)) ALIGN TO CALENDAR"
        );
    });
}
```

---

## State of the Art

| Old Pattern | Current Pattern | When Changed | Impact |
|-------------|----------------|--------------|--------|
| `isPrevKeyword(token)` only | Check `type == LITERAL` for bare vs `type == FUNCTION` for PREV(x) | Phase 4 | Distinguishes self-prev from cross-column prev |
| Gate allows size 0 or 1 | Gate allows any size without LINEAR | Phase 4 | Enables multi-fill specs on fast path |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `PREV(col_name)` arrives at `generateFill()` as a FUNCTION node (`type=FUNCTION`, `paramCount=1`, `rhs=LITERAL`) | Architecture Patterns | Wrong AST shape would require different detection logic |

Note: A1 is based on direct code inspection of ExpressionParser.java (lines 949-952) showing the LITERAL→FUNCTION conversion. HIGH confidence.

---

## Open Questions

1. **Type mismatch in cross-column prev**
   - What we know: `prevValue()` returns raw long bits; the type is interpreted by the calling `getXxx()` method.
   - What's unclear: If column `s` is DOUBLE and column `a` is INT, `FILL(PREV(s))` on column `a` would return raw long bits of a DOUBLE interpreted as INT bits. This is incorrect.
   - Recommendation: For Phase 4 scope, validate that source and destination column types are compatible in `generateFill()` and throw a `SqlException` if not. The CONTEXT.md explicitly defers "PREV with type coercion" to out-of-scope.

2. **Self-reference in PREV(col_name)**
   - What we know: `FILL(PREV(s))` where `s` is the column being filled produces the same result as `FILL(PREV)`.
   - What's unclear: Whether this should be an error or allowed as a no-op synonym.
   - Recommendation: Allow it silently — no special case needed, it just maps to `fillModes[s_col] = s_col`, and `mode >= 0` with `mode == col` behaves identically to `FILL_PREV_SELF` in `prevValue()`.

---

## Environment Availability

Step 2.6: SKIPPED (no external dependencies — code/config-only changes to existing Java source files)

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | JUnit 4 via Maven Surefire |
| Config file | `core/pom.xml` |
| Quick run command | `mvn -Dtest=SampleByFillTest test` |
| Full suite command | `mvn test` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| XPREV-01 | Non-keyed cross-column prev: gap fills from different column's prev | unit | `mvn -Dtest=SampleByFillTest#testFillPrevCrossColumnNonKeyed test` | Wave 0 |
| XPREV-01 | Keyed cross-column prev: per-key isolation maintained for cross-column source | unit | `mvn -Dtest=SampleByFillTest#testFillPrevCrossColumnKeyed test` | Wave 0 |
| XPREV-02 | Unknown alias in PREV(col_name) throws SqlException with correct position | unit | `mvn -Dtest=SampleByFillTest#testFillPrevCrossColumnBadAlias test` | Wave 0 |
| XPREV-01 | Multi-fill spec with PREV(col) mixed with NULL reaches fast path | unit | `mvn -Dtest=SampleByFillTest#testFillPrevCrossColumnMixedFill test` | Wave 0 |

### Sampling Rate
- **Per task commit:** `mvn -Dtest=SampleByFillTest test`
- **Per wave merge:** `mvn -Dtest=SampleByFillTest,SampleByTest test`
- **Phase gate:** `mvn -Dtest=SampleByFillTest,SampleByTest test` — all tests green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `SampleByFillTest#testFillPrevCrossColumnNonKeyed` — covers XPREV-01 (non-keyed)
- [ ] `SampleByFillTest#testFillPrevCrossColumnKeyed` — covers XPREV-01 (keyed)
- [ ] `SampleByFillTest#testFillPrevCrossColumnBadAlias` — covers XPREV-02 (error handling)
- [ ] `SampleByFillTest#testFillPrevCrossColumnMixedFill` — covers XPREV-01 multi-fill with NULL

---

## Security Domain

Phase 4 is internal SQL query execution — no authentication, sessions, access control, cryptography, or network surface. Security domain: N/A.

---

## Sources

### Primary (HIGH confidence)

- `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` lines 3225-3510 — `generateFill()` implementation, fillModes construction, bare PREV fast path
- `core/src/main/java/io/questdb/griffin/engine/groupby/SampleByFillRecordCursorFactory.java` lines 69-73, 547-613 — fill mode constants, `prevValue()` dispatch
- `core/src/main/java/io/questdb/griffin/SqlOptimiser.java` lines 7875-7882 — optimizer gate condition
- `core/src/main/java/io/questdb/griffin/SqlParser.java` lines 2889-2904 — FILL clause parsing
- `core/src/main/java/io/questdb/griffin/ExpressionParser.java` lines 948-963 — LITERAL→FUNCTION conversion when followed by `(`
- `core/src/main/java/io/questdb/griffin/model/ExpressionNode.java` lines 43-75 — node type constants
- `core/src/main/java/io/questdb/griffin/SqlKeywords.java` lines 1827-1832 — `isPrevKeyword()`
- `core/src/main/java/io/questdb/cairo/sql/RecordMetadata.java` lines 56-84 — `getColumnIndex()`, `getColumnIndexQuiet()`

### Secondary (MEDIUM confidence)

- `.planning/phases/03-keyed-fill-cursor/03-01-SUMMARY.md` — documents what Phase 3 actually changed (vs. what CONTEXT.md claims was removed)

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all files and APIs verified by codebase inspection
- Architecture: HIGH — ExpressionNode types verified by reading ExpressionParser.java and ExpressionNode.java
- Pitfalls: HIGH — all three pitfalls confirmed by reading the actual code paths

**Research date:** 2026-04-09
**Valid until:** Stable until any of the three key files are refactored — check before planning if more than 2 weeks pass
