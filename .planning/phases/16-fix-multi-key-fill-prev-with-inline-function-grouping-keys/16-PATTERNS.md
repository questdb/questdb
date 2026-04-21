# Phase 16: Fix multi-key FILL(PREV) with inline FUNCTION grouping keys - Pattern Map

**Mapped:** 2026-04-21
**Files analyzed:** 2 (1 production + 1 test)
**Analogs found:** 2 / 2 (both in-file, exact role+flow match)

## File Classification

| Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---------------|------|-----------|----------------|---------------|
| `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` | codegen (classifier) | AST-partition / compile-time | In-file: same loop `:3405-3436` (existing `continue` branches at `:3415-3417` and `:3418-3420`) | exact |
| `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` | test (JUnit4 integration) | DDL + INSERT + `assertQueryNoLeakCheck` | In-file: `testFillPrevInterval:1677-1707`, `testFillPrevKeyedIndependent:1785-1813`, `testFillPrevKeyedNoPrevYet:1878-1898` | exact |

## Pattern Assignments

### `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (classifier loop)

**Analog:** the two existing `continue` branches in the same classifier loop. D-02 inserts a third branch immediately after them; D-05 asserts the aggregate fall-through.

**Existing classifier loop** (verbatim, `:3401-3436`):

```java
            final IntList factoryColToUserFillIdx = new IntList(columnCount);
            for (int i = 0; i < columnCount; i++) {
                factoryColToUserFillIdx.add(-1);
            }
            int userFillIdx = 0;
            for (int i = 0, n = bottomUpCols.size(); i < n; i++) {
                final QueryColumn qc = bottomUpCols.getQuick(i);
                // Key columns and the timestamp column do NOT consume a user
                // fill-value index. Detect them directly from the QueryColumn
                // AST (LITERAL = column reference = key, or timestamp_floor
                // function = timestamp column) rather than via isKeyColumn on
                // a factory index - bottomUpCols is indexed in user order
                // and factory order may diverge after propagateTopDownColumns0.
                final ExpressionNode ast = qc.getAst();
                if (ast.type == ExpressionNode.LITERAL) {
                    continue;
                }
                if (SqlUtil.isTimestampFloorFunction(ast)) {
                    continue;
                }
                // This bottomUp column is an aggregate and therefore consumes
                // one slot in the user's fill-value list, regardless of whether
                // the outer projection keeps the column in the factory metadata.
                // When the column IS present in factory metadata, record the
                // mapping from factory index to user fill idx; when it is not,
                // skip the mapping but still advance userFillIdx so subsequent
                // aggregates land on the correct fill value.
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

**Existing LITERAL `continue` branch** (verbatim, `:3415-3417`):

```java
                if (ast.type == ExpressionNode.LITERAL) {
                    continue;
                }
```

**Existing timestamp_floor `continue` branch** (verbatim, `:3418-3420`):

```java
                if (SqlUtil.isTimestampFloorFunction(ast)) {
                    continue;
                }
```

**Style to mimic (D-02):** same indentation, same `if (...) { continue; }` one-liner shape. The new branch slots between `:3420` and `:3421` (immediately before the aggregate fall-through comment at `:3421-3427`).

**Aggregate-arm entry (D-05 assertion site):** `:3421-3435` (comment block + alias lookup + `userFillIdx++`). The assertion goes at the top of this block (before the `// This bottomUp column is an aggregate...` comment at `:3421`, or immediately before `userFillIdx++` at `:3435` per CONTEXT.md D-05).

**Existing alias-based factory-index lookup** (reuse in D-02 alias assert), `:3428-3434`:

```java
                final CharSequence qcAlias = qc.getAlias();
                if (qcAlias != null) {
                    final int factoryIdx = groupByMetadata.getColumnIndexQuiet(qcAlias);
                    if (factoryIdx >= 0 && factoryIdx != timestampIndex) {
                        factoryColToUserFillIdx.setQuick(factoryIdx, userFillIdx);
                    }
                }
```

D-02's `-ea` assert reuses the same `groupByMetadata.getColumnIndexQuiet(qc.getAlias())` call and the same `!= timestampIndex` guard.

---

### `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (new tests)

**Analogs:**
- `testFillPrevInterval:1677-1707` — the single-key INTERVAL fixture the new multi-key tests are derived from.
- `testFillPrevKeyedIndependent:1785-1813` — canonical 2-key x 3-bucket cartesian FILL(PREV) shape + `assertQueryNoLeakCheck(..., "ts", false, false)` signature + pass-1 discovery-order row layout.
- `testFillPrevKeyedNoPrevYet:1878-1898` — "leading bucket with no prev yet" null-rendering convention for the aggregate column on the first-discovery bucket.

**`testFillPrevInterval` body — full verbatim** (`:1677-1707`):

```java
    @Test
    public void testFillPrevInterval() throws Exception {
        // INTERVAL is not a persistable column type and QuestDB has no
        // first(INTERVAL) aggregate, so the only route to an INTERVAL in a
        // SAMPLE BY output column is an inline interval(lo, hi) expression
        // used as the GROUP BY key. Without FillRecord.getInterval, gap rows
        // would fall through to Record.getInterval's default and throw
        // UnsupportedOperationException - this test pins the contract.
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
                        ('2020-01-01T00:00:00.000Z'::TIMESTAMP, '2020-02-01T00:00:00.000Z'::TIMESTAMP, 30.0, '2024-01-01T03:00:00.000000Z')""");
            assertSql(
                    """
                            ts\tk\tfirst
                            2024-01-01T00:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T01:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T02:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t10.0
                            2024-01-01T03:00:00.000000Z\t('2020-01-01T00:00:00.000Z', '2020-02-01T00:00:00.000Z')\t30.0
                            """,
                    "SELECT ts, interval(lo, hi) k, first(v) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR");
        });
    }
```

Notes: single-key fixture uses `assertSql` (not `assertQueryNoLeakCheck`). Multi-key variants MUST switch to `assertQueryNoLeakCheck(..., "ts", false, false)` per Phase 14 D-15 and per `testFillPrevKeyedIndependent`.

**`testFillPrevKeyedIndependent` body — full verbatim** (`:1784-1813`) — the canonical multi-key cartesian template:

```java
    @Test
    public void testFillPrevKeyedIndependent() throws Exception {
        assertMemoryLeak(() -> {
            // Tests that per-key prev tracking does not bleed between keys.
            // London: data at 00:00 (temp=10), gap at 01:00, data at 02:00 (temp=12)
            // Paris:  data at 00:00 (temp=20), data at 01:00 (temp=21), gap at 02:00
            // For FILL(PREV):
            //   London at 01:00 -> prev = 10 (London's own prev, NOT Paris's 21)
            //   Paris at 02:00 -> prev = 21 (Paris's own prev, NOT London's 12)
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 21.0, '2024-01-01T01:00:00.000000Z')," +
                    "('London', 12.0, '2024-01-01T02:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tParis\t21.0
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            2024-01-01T02:00:00.000000Z\tLondon\t12.0
                            2024-01-01T02:00:00.000000Z\tParis\t21.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }
```

Row-order convention (pass-1 discovery order): within each bucket, key discovered first in pass-1 (`London` at 00:00) is rendered first at its own bucket, but at later buckets the order flips to `(data row first, gap row second)` — see bucket 01:00 where `Paris` (data) comes before `London` (gap) and bucket 02:00 where `London` (data) comes before `Paris` (gap).

**`testFillPrevKeyedNoPrevYet` body — full verbatim** (`:1877-1898`) — the "no prev yet" null-rendering template:

```java
    @Test
    public void testFillPrevKeyedNoPrevYet() throws Exception {
        assertMemoryLeak(() -> {
            // London appears at 00:00, Paris first appears at 01:00.
            // At 00:00, Paris is missing and has no prev -> should get null.
            execute("CREATE TABLE weather (city STRING, temp DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO weather VALUES " +
                    "('London', 10.0, '2024-01-01T00:00:00.000000Z')," +
                    "('Paris', 20.0, '2024-01-01T01:00:00.000000Z')");
            assertQueryNoLeakCheck(
                    """
                            ts\tcity\tavg
                            2024-01-01T00:00:00.000000Z\tLondon\t10.0
                            2024-01-01T00:00:00.000000Z\tParis\tnull
                            2024-01-01T01:00:00.000000Z\tParis\t20.0
                            2024-01-01T01:00:00.000000Z\tLondon\t10.0
                            """,
                    "SELECT ts, city, avg(temp) FROM weather SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                    "ts", false, false
            );
        });
    }
```

Note: `avg` (DOUBLE) renders as literal string `null` on the no-prev-yet row. Per RESEARCH.md A2, INTERVAL `Interval.NULL` may render differently (empty text) - probe-and-freeze during task execution.

---

## Shared Patterns

### `assertQueryNoLeakCheck` canonical signature for SAMPLE BY FILL (Phase 14 D-15)

**Source:** `testFillPrevKeyedIndependent:1799-1811`, `testFillPrevKeyedNoPrevYet:1886-1897`
**Apply to:** ALL 5 new test methods (`testFillNullCastMultiKey`, `testFillPrevCastMultiKey`, `testFillPrevConcatMultiKey`, `testFillPrevConcatOperatorMultiKey`, `testFillPrevIntervalMultiKey`).

```java
            assertQueryNoLeakCheck(
                    """
                            <expected tab-separated rows, header line first>
                            """,
                    "<SQL with SAMPLE BY 1h FILL(...) ALIGN TO CALENDAR>",
                    "ts", false, false
            );
```

The two trailing `false, false` are `supportsRandomAccess` and `expectSize`. SAMPLE BY FILL cursors advertise neither — Phase 14 D-15 contract.

### `assertMemoryLeak(() -> { ... })` outer wrap

**Source:** every existing FILL test in the file, e.g., `:1685`, `:1786`, `:1879`.
**Apply to:** All 5 new test methods (bodies already land inside the lambda).

```java
    @Test
    public void testXxxMultiKey() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE ...");
            execute("INSERT INTO ... VALUES ...");
            assertQueryNoLeakCheck(..., "ts", false, false);
        });
    }
```

### Alphabetical member ordering (CLAUDE.md)

**Apply to:** placement of the 5 new test methods inside `SampleByFillTest.java`.
Target positions (alphabetical among existing `testFillPrev*` / `testFillNull*` members):
- `testFillNullCastMultiKey` - among `testFillNull*` methods (search the file for the correct alphabetical slot before `testFillPrev*` region).
- `testFillPrevCastMultiKey` - alphabetically between `testFillPrev...` methods (after any `testFillPrevC...` that sorts before `Cast`).
- `testFillPrevConcatMultiKey` - after `testFillPrevCastMultiKey`, before `testFillPrevConcatOperatorMultiKey`.
- `testFillPrevConcatOperatorMultiKey` - immediately after `testFillPrevConcatMultiKey`.
- `testFillPrevIntervalMultiKey` - immediately after `testFillPrevInterval:1707`.

No banner comments (CLAUDE.md: no `// ===` / `// ---` section headings - members auto-sort).

### ASCII-only assertion / comment messages (CLAUDE.md)

**Apply to:** D-02 and D-05 assertion messages, and all comments in new tests.
Use plain hyphen (`-`), plain apostrophe (`'`), plain quotes. No em dash (U+2014), no curly quotes. The existing `testFillPrevInterval` comment at `:1684` already uses plain `-` in "throw UnsupportedOperationException - this test pins the contract" — mirror that style.

## No Analog Found

None. Both files have exact in-file analogs for both role and data flow.

## Metadata

**Analog search scope:** `core/src/main/java/io/questdb/griffin/SqlCodeGenerator.java` (lines 3400-3445), `core/src/test/java/io/questdb/test/griffin/engine/groupby/SampleByFillTest.java` (lines 1670-1900).
**Files scanned:** 2 (both targeted by the phase scope, both contain exact analogs).
**Pattern extraction date:** 2026-04-21
