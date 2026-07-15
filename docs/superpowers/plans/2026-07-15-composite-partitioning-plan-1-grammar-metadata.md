# Composite Partitioning — Plan 1: Grammar, Carrier & Metadata

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended)
> or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make `CREATE TABLE … PARTITION BY DAY, exchange, hash(symbol,32) [ORDER BY symbol]` parse into a
composite partition carrier, persist it in `_meta` (additively — existing tables byte-identical), and
reconstruct it via `SHOW CREATE TABLE`. **No data routing yet** — a composite table stores exactly like a
normal `PARTITION BY DAY` table until Plans 3–4.

**Architecture:** Keep `PartitionBy` (the static int time-unit enum) untouched; add a `PartitionSpec`
carrier = `{ timeUnit, namingMode, dimensions[], clusterColumns[] }` resolved at CREATE build time and
serialized into a new additive `_meta` trailing block gated by a `META_FORMAT_MINOR_VERSION` bump.

**Tech Stack:** Java 17 (records/switch), QuestDB `griffin` parser, `cairo` metadata (`TableUtils`,
`*Metadata`), `AbstractCairoTest` + fluent `assertQuery().returns(...)`.

## Global Constraints (verbatim from spec)

- OSS core only; WAL tables only (composite requires WAL — enforced like existing `PARTITION BY … WAL`).
- Feature is **additive & backward-compatible**: `META_OFFSET_PARTITION_BY` (offset 4) keeps the **time
  unit**; the composite spec goes in a NEW trailing `_meta` block gated by `META_FORMAT_MINOR_VERSION`
  (bump only — **no `ColumnType.VERSION` bump, no `mig/` migration**). Non-composite tables must be
  byte-identical to today.
- Non-time dimensions must be **SYMBOL columns** or transforms over them (`identity`/`hash`/`truncate`) or
  an **aliased** scalar expression over the current row (`(expr) AS name`). No aggregates/subqueries.
- Time dimension always first; `PARTITION BY DAY` desugars to `timestamp(DAY)`; existing DDL unchanged.
- "sub-partition" is a reserved term (O3 splits) — never use it in identifiers or user-facing text; use
  **cell** / **dimension** / **clustering**.
- Tests use fluent `assertQuery(...).returns(...)`; SHOW-CREATE round-trip uses the capture→re-execute
  pattern (`ShowCreateTableTest.testBypassWal`).

---

### Task 1: `PartitionDimension` + `PartitionSpec` carrier types

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/PartitionDimension.java`
- Create: `core/src/main/java/io/questdb/cairo/PartitionSpec.java`
- Test: `core/src/test/java/io/questdb/test/cairo/PartitionSpecTest.java`

**Interfaces — Produces** (consumed by all later tasks & Plans 2–7):
```java
// PartitionDimension.java
public final class PartitionDimension {
    public static final byte KIND_IDENTITY   = 0; // raw value of a SYMBOL column
    public static final byte KIND_HASH       = 1; // hash(col, N) -> 0..N-1
    public static final byte KIND_TRUNCATE   = 2; // truncate(col, N) -> first N chars
    public static final byte KIND_EXPRESSION = 3; // (expr) AS alias

    private final byte kind;
    private final int columnIndex;      // source SYMBOL column; -1 for KIND_EXPRESSION
    private final int param;            // N for HASH/TRUNCATE; 0 otherwise
    private final String alias;         // dir key label + dimension name (never null)
    private final String exprText;      // serialized expression for KIND_EXPRESSION; null otherwise

    public PartitionDimension(byte kind, int columnIndex, int param, String alias, String exprText);
    public byte getKind();
    public int getColumnIndex();
    public int getParam();
    public String getAlias();
    public String getExprText();
    // normalized token, e.g. "hash(symbol, 32)" / "exchange" / "truncate(symbol, 3)" / "(…) AS asset_class"
    public void toSink(CharSink<?> sink, RecordMetadata columnNames);
    @Override public boolean equals(Object o); @Override public int hashCode();
}

// PartitionSpec.java
public final class PartitionSpec implements Mutable {
    public static final byte MODE_HIVE  = 0; // ts=2023-01-01/exchange=NYSE/
    public static final byte MODE_PLAIN = 1; // 2023-01-01/NYSE/

    public int getTimeUnit();                 // PartitionBy.DAY/HOUR/... (never NONE when composite)
    public byte getNamingMode();
    public int getDimensionCount();
    public PartitionDimension getDimension(int i);
    public int getClusterColumnCount();
    public int getClusterColumn(int i);       // ORDER BY column index
    public boolean isComposite();             // getDimensionCount() > 0 || getClusterColumnCount() > 0
    public void setTimeUnit(int unit);
    public void setNamingMode(byte mode);
    public void addDimension(PartitionDimension d);
    public void addClusterColumn(int columnIndex);
    @Override public void clear();
}
```

- [ ] **Step 1: Write the failing test**
```java
// PartitionSpecTest.java (extends AbstractCairoTest)
@Test
public void testCompositeSpecShape() {
    PartitionSpec s = new PartitionSpec();
    s.setTimeUnit(PartitionBy.DAY);
    s.setNamingMode(PartitionSpec.MODE_HIVE);
    s.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));
    s.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "symbol_hash", null));
    s.addClusterColumn(2);

    Assert.assertTrue(s.isComposite());
    Assert.assertEquals(PartitionBy.DAY, s.getTimeUnit());
    Assert.assertEquals(2, s.getDimensionCount());
    Assert.assertEquals(PartitionDimension.KIND_HASH, s.getDimension(1).getKind());
    Assert.assertEquals(32, s.getDimension(1).getParam());
    Assert.assertEquals("symbol_hash", s.getDimension(1).getAlias());
    Assert.assertEquals(1, s.getClusterColumnCount());
    Assert.assertEquals(2, s.getClusterColumn(0));
}

@Test
public void testEmptySpecIsNotComposite() {
    PartitionSpec s = new PartitionSpec();
    s.setTimeUnit(PartitionBy.DAY);
    Assert.assertFalse(s.isComposite());
    Assert.assertEquals(0, s.getDimensionCount());
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `mvn -q -pl core test -Dtest=PartitionSpecTest` (JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64`)
Expected: FAIL — `PartitionSpec`/`PartitionDimension` do not exist (compile error).

- [ ] **Step 3: Write minimal implementation** — the two classes exactly as the Interfaces block above,
  using `ObjList<PartitionDimension>` for dimensions and `IntList` for clusterColumns (both from
  `io.questdb.std`). `isComposite()` returns `dimensions.size() > 0 || clusterColumns.size() > 0`.
  `clear()` resets timeUnit to `PartitionBy.NONE`, namingMode to `MODE_HIVE`, and both lists to empty.

- [ ] **Step 4: Run test to verify it passes**
Run: `mvn -q -pl core test -Dtest=PartitionSpecTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**
```bash
git add core/src/main/java/io/questdb/cairo/PartitionDimension.java \
        core/src/main/java/io/questdb/cairo/PartitionSpec.java \
        core/src/test/java/io/questdb/test/cairo/PartitionSpecTest.java
git commit -m "feat(cairo): add PartitionSpec/PartitionDimension composite-partition carrier"
```

---

### Task 2: Transform token parser `PartitionTransform.parse`

**Files:**
- Create: `core/src/main/java/io/questdb/griffin/PartitionTransform.java`
- Test: `core/src/test/java/io/questdb/test/griffin/PartitionTransformTest.java`

**Interfaces — Consumes:** `PartitionDimension` (Task 1). **Produces:**
```java
// Resolves a parsed CREATE-TABLE dimension expression node into a PartitionDimension.
// columnNameToIndex resolves a SYMBOL column name to its index (or -1); throws if not a SYMBOL column.
public static PartitionDimension resolve(ExpressionNode node,
                                         Function<CharSequence,Integer> symbolColumnResolver)
        throws SqlException;
```
Accepts these node shapes: bare literal `exchange` → IDENTITY; `hash(col, N)` → HASH; `truncate(col, N)`
→ TRUNCATE; `identity(col)` → IDENTITY; any other function/operator node → must be an aliased
expression (handled by the parser, Task 3, which attaches the alias) → EXPRESSION.

- [ ] **Step 1: Write the failing test**
```java
// resolver maps "exchange"->1, "symbol"->2, anything else -> throws "not a SYMBOL column"
@Test
public void testIdentity() throws Exception {
    PartitionDimension d = PartitionTransform.resolve(lit("exchange"), RES);
    Assert.assertEquals(PartitionDimension.KIND_IDENTITY, d.getKind());
    Assert.assertEquals(1, d.getColumnIndex());
    Assert.assertEquals("exchange", d.getAlias());
}
@Test
public void testHash() throws Exception {
    PartitionDimension d = PartitionTransform.resolve(fn("hash", lit("symbol"), num(32)), RES);
    Assert.assertEquals(PartitionDimension.KIND_HASH, d.getKind());
    Assert.assertEquals(2, d.getColumnIndex());
    Assert.assertEquals(32, d.getParam());
    Assert.assertEquals("symbol_hash", d.getAlias());
}
@Test
public void testTruncatePrefix() throws Exception {
    PartitionDimension d = PartitionTransform.resolve(fn("truncate", lit("symbol"), num(3)), RES);
    Assert.assertEquals(PartitionDimension.KIND_TRUNCATE, d.getKind());
    Assert.assertEquals(3, d.getParam());
    Assert.assertEquals("symbol_trunc", d.getAlias());
}
@Test
public void testHashOnNonSymbolThrows() {
    try { PartitionTransform.resolve(fn("hash", lit("price"), num(4)), RES); Assert.fail(); }
    catch (SqlException e) { TestUtils.assertContains(e.getFlyweightMessage(), "SYMBOL"); }
}
@Test
public void testHashRequiresPositiveN() {
    try { PartitionTransform.resolve(fn("hash", lit("symbol"), num(0)), RES); Assert.fail(); }
    catch (SqlException e) { TestUtils.assertContains(e.getFlyweightMessage(), "bucket count"); }
}
```
(Helpers `lit/fn/num` build `ExpressionNode`s via `ExpressionNode.FACTORY`/`SqlUtil`; `RES` is the
symbol resolver above.)

- [ ] **Step 2: Run test to verify it fails**
Run: `mvn -q -pl core test -Dtest=PartitionTransformTest`  Expected: FAIL — class missing.

- [ ] **Step 3: Write minimal implementation** — switch on node type: `ExpressionNode.LITERAL` →
  IDENTITY (resolve column, alias = column name); function node named `hash`/`truncate`/`identity` →
  validate arity, resolve arg0 as a SYMBOL column, parse arg1 as a positive int (`Numbers.parseInt`;
  throw `SqlException.position(node.position).put("bucket count must be > 0")` for hash, similar for
  truncate), alias = `col + "_hash"` / `col + "_trunc"` / `col`. Non-SYMBOL column → `SqlException …
  put("partition dimension must be a SYMBOL column")`. Unknown function → throw "unsupported partition
  transform" (aliased expressions are routed here already carrying `KIND_EXPRESSION` by Task 3).

- [ ] **Step 4: Run test to verify it passes**
Run: `mvn -q -pl core test -Dtest=PartitionTransformTest`  Expected: PASS (5 tests).

- [ ] **Step 5: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/PartitionTransform.java \
        core/src/test/java/io/questdb/test/griffin/PartitionTransformTest.java
git commit -m "feat(griffin): resolve partition transform tokens to PartitionDimension"
```

---

### Task 3: Parser — comma-list of dimensions + optional `ORDER BY`

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/SqlParser.java:2227-2233` (`parseCreateTablePartition`)
  and the caller at `:1706-1716`.
- Modify: `core/src/main/java/io/questdb/griffin/engine/ops/CreateTableOperationBuilderImpl.java`
  (add a parse-time dimension list + cluster list + naming mode alongside `partitionByExpr` at `:64`).
- Test: `core/src/test/java/io/questdb/test/griffin/CompositePartitionParseTest.java`

**Interfaces — Consumes:** `PartitionTransform` (Task 2). **Produces:** on the builder, a parse-time
`ObjList<ExpressionNode> partitionDimensionExprs`, `ObjList<ExpressionNode> clusterExprs`, and
`byte namingMode` with getters, populated during parse; resolved to `PartitionSpec` in Task 4.

- [ ] **Step 1: Write the failing test**
```java
// Assert the parsed model via the resolved operation (Task 4 wires resolution; until then assert the
// builder's raw lists through a small package-visible test accessor added in this task).
@Test
public void testParseTwoDimsAndOrderBy() throws Exception {
    execute("create table t (ts timestamp, exchange symbol, symbol symbol, price double) " +
            "timestamp(ts) partition by day, exchange, hash(symbol, 32) order by symbol wal");
    // Round-trip through SHOW CREATE is covered in Task 7; here assert no parse error + WAL enabled.
    assertSql("wal_enabled\ntrue\n",
        "select walEnabled as wal_enabled from tables() where table_name = 't'");
}
@Test
public void testBareDayStillWorks() throws Exception {
    execute("create table t2 (ts timestamp, s symbol) timestamp(ts) partition by day wal");
    assertSql("count\n1\n", "select count() from tables() where table_name='t2'");
}
@Test
public void testTimeMustLead() throws Exception {
    assertException(
        "create table t3 (ts timestamp, s symbol) timestamp(ts) partition by s, day wal",
        /*expected position of 's'*/ 62,
        "partition time unit (DAY/HOUR/WEEK/MONTH/YEAR) must come first");
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `mvn -q -pl core test -Dtest=CompositePartitionParseTest`
Expected: FAIL — parser rejects the comma / `ORDER BY` (currently reads exactly one literal at `:2230`).

- [ ] **Step 3: Write minimal implementation**
  In `parseCreateTablePartition`: after `expectTok(lexer, "by")`, read the first element. Accept either a
  bare unit literal (`day`…) **or** `timestamp(<unit>)` (function node → unwrap the unit token). Then,
  while the next token is `,`, parse each following element with `expr(lexer, …)` and append the node to
  `builder`'s `partitionDimensionExprs`. Return the time-unit literal node (so the existing `:1712`
  `PartitionBy.fromString` path is unchanged). Back in the caller, after the TTL/FORMAT/WAL handling,
  if the next token is `order`, `expectTok("by")`, then parse a comma list of column literals into
  `builder.clusterExprs`. Add a guard: if the first element is not a recognized time unit but a later one
  is, throw at that position `"partition time unit (DAY/HOUR/WEEK/MONTH/YEAR) must come first"`.
  Naming mode: default `MODE_HIVE`; parse an optional `LAYOUT PLAIN|HIVE` token (add `isLayoutKeyword`).

- [ ] **Step 4: Run test to verify it passes**
Run: `mvn -q -pl core test -Dtest=CompositePartitionParseTest`  Expected: PASS (3 tests).

- [ ] **Step 5: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/SqlParser.java \
        core/src/main/java/io/questdb/griffin/engine/ops/CreateTableOperationBuilderImpl.java \
        core/src/test/java/io/questdb/test/griffin/CompositePartitionParseTest.java
git commit -m "feat(griffin): parse composite PARTITION BY dimension list + ORDER BY clustering"
```

---

### Task 4: Builder resolves parse-time model → `PartitionSpec` + validation

**Files:**
- Modify: `CreateTableOperationBuilderImpl.java` (`getPartitionByFromExpr` neighborhood `:212`, `build`
  sites `:105/135/149`) — build and attach a resolved `PartitionSpec`.
- Modify: `core/src/main/java/io/questdb/griffin/engine/ops/CreateTableOperationImpl.java` (add
  `PartitionSpec partitionSpec` field + getter; validate in the `:688` block).
- Test: `core/src/test/java/io/questdb/test/griffin/CompositePartitionValidateTest.java`

**Interfaces — Consumes:** parse-time lists (Task 3), `PartitionTransform.resolve` (Task 2).
**Produces:** `CreateTableOperationImpl.getPartitionSpec(): PartitionSpec` (empty/`!isComposite` for
plain tables), consumed by Tasks 5–7 and Plans 2–7.

- [ ] **Step 1: Write the failing test**
```java
@Test
public void testDimMustBeSymbol() throws Exception {
    assertException(
        "create table t (ts timestamp, price double) timestamp(ts) partition by day, price wal",
        /*pos of price*/ 71, "partition dimension must be a SYMBOL column");
}
@Test
public void testExpressionRequiresAlias() throws Exception {
    assertException(
        "create table t (ts timestamp, s symbol) timestamp(ts) partition by day, (s = 'BTC') wal",
        /*pos*/ 71, "partition expression must be aliased with AS");
}
@Test
public void testPartitionRequiresDesignatedTimestamp() throws Exception {
    assertException(
        "create table t (ts timestamp, s symbol) partition by day, s wal",
        40, "partitioning is possible only on tables with designated timestamps");
}
@Test
public void testResolvedSpecOnOperation() throws Exception {
    // Compile (don't execute) and assert the resolved spec via a test hook returning getPartitionSpec().
    PartitionSpec s = compilePartitionSpec(
        "create table t (ts timestamp, exchange symbol, symbol symbol) " +
        "timestamp(ts) partition by day, exchange, hash(symbol, 8) wal");
    Assert.assertEquals(PartitionBy.DAY, s.getTimeUnit());
    Assert.assertEquals(2, s.getDimensionCount());
    Assert.assertEquals(PartitionDimension.KIND_IDENTITY, s.getDimension(0).getKind());
    Assert.assertEquals(PartitionDimension.KIND_HASH, s.getDimension(1).getKind());
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `mvn -q -pl core test -Dtest=CompositePartitionValidateTest`  Expected: FAIL — no resolution/validation yet.

- [ ] **Step 3: Write minimal implementation** — in `build()`, construct a `PartitionSpec`: set timeUnit
  = `getPartitionByFromExpr()`, namingMode from the builder, then for each `partitionDimensionExprs` node
  call `PartitionTransform.resolve(node, name -> columnModelIndexOfSymbol(name))`; for a node that is an
  aliased expression (`node.type == FUNCTION`/operator with an `AS` alias captured in Task 3) build a
  `KIND_EXPRESSION` dimension (columnIndex -1, alias = the AS name, exprText = serialized node); resolve
  `clusterExprs` to column indices. Attach to `CreateTableOperationImpl`. In the `:688` validation block,
  additionally throw if a resolved dimension is non-SYMBOL or an expression lacks an alias (positions from
  the node). Keep the existing designated-timestamp check (it already covers the composite case since
  time leads).

- [ ] **Step 4: Run test to verify it passes**
Run: `mvn -q -pl core test -Dtest=CompositePartitionValidateTest`  Expected: PASS (4 tests).

- [ ] **Step 5: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/engine/ops/CreateTableOperationBuilderImpl.java \
        core/src/main/java/io/questdb/griffin/engine/ops/CreateTableOperationImpl.java \
        core/src/test/java/io/questdb/test/griffin/CompositePartitionValidateTest.java
git commit -m "feat(griffin): resolve+validate composite PartitionSpec on CREATE TABLE operation"
```

---

### Task 5: Persist `PartitionSpec` in an additive `_meta` block + minor-version bump

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableUtils.java` — `writeMetadata` (append after the
  covering-index trailing section, near `:2666-2678`); bump `META_FORMAT_MINOR_VERSION_LATEST` (`:117`)
  and extend `calculateMetaFormatMinorVersionField` (`:283-293`) to raise the minor version **only when
  the spec is composite**.
- Modify: the `TableStructure` interface (add `getPartitionSpec()` default returning an empty spec) and
  `CreateTableOperationImpl` (already carries it from Task 4) so `writeMetadata` can read it.
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeMetaFormatTest.java`

**Interfaces — Consumes:** `PartitionSpec` (Tasks 1/4). **Produces:** the on-disk `_meta` block format:
`[u8 namingMode][vint dimCount]{[u8 kind][vint columnIndex][vint param][str alias][str exprText]}…
[vint clusterCount]{[vint columnIndex]}…` appended after existing metadata; read by Task 6.

- [ ] **Step 1: Write the failing test**
```java
@Test
public void testCompositeMetaRoundTripBytes() throws Exception {
    execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
            "timestamp(ts) partition by day, exchange, hash(symbol, 16) wal");
    // Re-open metadata and assert the persisted spec (uses Task 6 reader once landed; until then read
    // the raw block via a helper that seeks past the covering-index section).
    try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("t"))) {
        PartitionSpec s = m.getPartitionSpec();
        Assert.assertEquals(2, s.getDimensionCount());
        Assert.assertEquals(16, s.getDimension(1).getParam());
    }
}
@Test
public void testPlainTableMetaUnchanged() throws Exception {
    // A non-composite table must NOT raise the minor version and must round-trip as today.
    execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
    try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("p"))) {
        Assert.assertFalse(m.getPartitionSpec().isComposite());
    }
    // Byte-level guard: minor version equals the pre-feature value for a plain table.
    assertMetaMinorVersionForPlainTableUnchanged("p");
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `mvn -q -pl core test -Dtest=CompositeMetaFormatTest`  Expected: FAIL — spec not written/read.

- [ ] **Step 3: Write minimal implementation** — after the covering-index append in `writeMetadata`, if
  `tableStruct.getPartitionSpec().isComposite()`, serialize the block above (use `mem.putByte`,
  `mem.putInt`, and the existing var-string writer used for column names). In
  `calculateMetaFormatMinorVersionField`, when composite, ensure the returned minor version ≥ the new
  `COMPOSITE_PARTITION_MINOR_VERSION`. Do **not** touch `META_OFFSET_PARTITION_BY` (stays the time unit).

- [ ] **Step 4: Run test to verify it passes**
Run: `mvn -q -pl core test -Dtest=CompositeMetaFormatTest`  Expected: PASS (2 tests) — after Task 6 reader
lands; if running Task 5 alone, assert only the write side via a raw-block helper, then delete the helper
in Task 6.

- [ ] **Step 5: Commit**
```bash
git add core/src/main/java/io/questdb/cairo/TableUtils.java \
        core/src/main/java/io/questdb/cairo/TableStructure.java \
        core/src/test/java/io/questdb/test/cairo/CompositeMetaFormatTest.java
git commit -m "feat(cairo): persist composite PartitionSpec in additive _meta block (minor-version gated)"
```

---

### Task 6: Read `PartitionSpec` in the metadata readers

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableReaderMetadata.java` (`readFromMem` near `:314-329`)
  and `TableWriterMetadata.java` (`reload` near `:153-164`) — after existing fields, if the minor version
  ≥ `COMPOSITE_PARTITION_MINOR_VERSION`, deserialize the block into a `PartitionSpec` field + getter.
- Modify: `core/src/main/java/io/questdb/cairo/MetadataCache.java` (`:572` region) + `CairoTable.java`
  (`:47/145/226`) to surface the spec.
- Test: extend `CompositeMetaFormatTest` (Task 5) — the reader assertions now run for real; remove the
  raw-block helper.

**Interfaces — Consumes:** the block format (Task 5). **Produces:**
`TableMetadata.getPartitionSpec(): PartitionSpec` (empty when not composite / minor version too low).

- [ ] **Step 1: Write the failing test** — un-skip the reader assertions in `CompositeMetaFormatTest`
  (`getPartitionSpec()` from a re-opened `TableReaderMetadata` and `TableWriterMetadata`), plus:
```java
@Test
public void testReopenAfterRestartKeepsSpec() throws Exception {
    execute("create table t (ts timestamp, exchange symbol) timestamp(ts) partition by day, exchange wal");
    engine.releaseInactive();               // force re-read of _meta from disk
    try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("t"))) {
        Assert.assertTrue(m.getPartitionSpec().isComposite());
        Assert.assertEquals("exchange", m.getPartitionSpec().getDimension(0).getAlias());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `mvn -q -pl core test -Dtest=CompositeMetaFormatTest`  Expected: FAIL — readers return empty spec.

- [ ] **Step 3: Write minimal implementation** — add the mirror deserializer (guarded by
  `isMetaFormatAtLeast(COMPOSITE_PARTITION_MINOR_VERSION)`, `TableUtils.java:2705-2714`) to both
  `*Metadata` classes; store into a `PartitionSpec` field; expose via `getPartitionSpec()`; wire
  `MetadataCache`/`CairoTable`.

- [ ] **Step 4: Run test to verify it passes**
Run: `mvn -q -pl core test -Dtest=CompositeMetaFormatTest`  Expected: PASS (3 tests).

- [ ] **Step 5: Commit**
```bash
git add core/src/main/java/io/questdb/cairo/TableReaderMetadata.java \
        core/src/main/java/io/questdb/cairo/TableWriterMetadata.java \
        core/src/main/java/io/questdb/cairo/MetadataCache.java \
        core/src/main/java/io/questdb/cairo/CairoTable.java \
        core/src/test/java/io/questdb/test/cairo/CompositeMetaFormatTest.java
git commit -m "feat(cairo): read composite PartitionSpec in table metadata readers"
```

---

### Task 7: `SHOW CREATE TABLE` reconstructs the composite clause + `ORDER BY`

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/engine/table/ShowCreateTableRecordCursorFactory.java`
  — `putPartitionBy()` (`:394`) to append the dimension list (normalized transform syntax) after the
  time unit, then `ORDER BY` and (when non-default) `LAYOUT PLAIN`.
- Test: `core/src/test/java/io/questdb/test/griffin/ShowCreateTableTest.java` (add cases).

**Interfaces — Consumes:** `TableMetadata.getPartitionSpec()` (Task 6) + `PartitionDimension.toSink`
(Task 1).

- [ ] **Step 1: Write the failing test**
```java
@Test
public void testShowCreateComposite() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table trades (ts timestamp, exchange symbol, symbol symbol, price double) " +
                "timestamp(ts) partition by day, exchange, hash(symbol, 32) order by symbol wal");
        assertQuery("show create table trades").noLeakCheck().noRandomAccess().returns("""
                ddl
                CREATE TABLE 'trades' (\s
                \tts TIMESTAMP,
                \texchange SYMBOL,
                \tsymbol SYMBOL,
                \tprice DOUBLE
                ) timestamp(ts) PARTITION BY DAY, exchange, hash(symbol, 32) ORDER BY symbol WAL;
                """);
    });
}
@Test
public void testShowCreateCompositeRoundTrip() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table trades (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, hash(symbol, 32) wal");
        printSql("SHOW CREATE TABLE trades;");
        String ddl = sink.toString().replace("ddl\n", "");
        execute("drop table trades;");
        execute(ddl);                                   // re-create from emitted DDL
        printSql("SHOW CREATE TABLE trades;");
        TestUtils.assertEquals(sink.toString().replace("ddl\n", ""), ddl);
    });
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `mvn -q -pl core test -Dtest=ShowCreateTableTest#testShowCreateComposite+testShowCreateCompositeRoundTrip`
Expected: FAIL — `putPartitionBy` emits only `PARTITION BY DAY`.

- [ ] **Step 3: Write minimal implementation** — in `putPartitionBy`, after the existing
  `PARTITION BY <unit>`, if `table.getPartitionSpec().isComposite()`, for each dimension append
  `", "` + `dimension.toSink(sink, tableColumnNames)`; then if cluster columns exist append
  `" ORDER BY " + comma-joined column names`; then if `namingMode == MODE_PLAIN` append `" LAYOUT PLAIN"`.
  (WAL is already emitted by `putWal()` at `:258`.)

- [ ] **Step 4: Run test to verify it passes**
Run: `mvn -q -pl core test -Dtest=ShowCreateTableTest`  Expected: PASS (existing + 2 new).

- [ ] **Step 5: Commit**
```bash
git add core/src/main/java/io/questdb/griffin/engine/table/ShowCreateTableRecordCursorFactory.java \
        core/src/test/java/io/questdb/test/griffin/ShowCreateTableTest.java
git commit -m "feat(griffin): SHOW CREATE TABLE emits composite partition dimensions + ORDER BY"
```

---

### Task 8: Backward-compatibility guard (existing tables + old-binary rejection)

**Files:**
- Test only: `core/src/test/java/io/questdb/test/cairo/CompositeBackwardCompatTest.java`
- Modify (if a gap is found): the reader guard in Task 6 to fail cleanly on a composite table opened by a
  binary whose `META_FORMAT_MINOR_VERSION` support is lower (simulated by writing a spec then forcing an
  older minor version on read).

**Interfaces — Consumes:** everything above.

- [ ] **Step 1: Write the failing test**
```java
@Test
public void testExistingPlainTableUnaffected() throws Exception {
    execute("create table legacy (ts timestamp, s symbol) timestamp(ts) partition by day wal");
    // Full SHOW CREATE identical to pre-feature output (no composite/ORDER BY/LAYOUT tokens).
    assertQuery("show create table legacy").noLeakCheck().noRandomAccess().returns("""
            ddl
            CREATE TABLE 'legacy' (\s
            \tts TIMESTAMP,
            \ts SYMBOL
            ) timestamp(ts) PARTITION BY DAY WAL;
            """);
}
@Test
public void testCompositeStoresLikePlainForNow() throws Exception {
    // Until Plans 3–4, a composite table writes data into ONE time partition dir (no cell subdirs yet).
    execute("create table t (ts timestamp, exchange symbol, price double) " +
            "timestamp(ts) partition by day, exchange wal");
    execute("insert into t values ('2023-01-01T00:00:00.000000Z','NYSE',1.0)");
    drainWalQueue();
    assertSql("count\n1\n", "select count() from t");
    // exactly one partition, named as today
    assertSql("name\n2023-01-01\n", "select name from table_partitions('t')");
}
```

- [ ] **Step 2: Run test to verify it fails/passes**
Run: `mvn -q -pl core test -Dtest=CompositeBackwardCompatTest`
Expected: `testExistingPlainTableUnaffected` PASS immediately (guard); `testCompositeStoresLikePlainForNow`
PASS (composite is parsed+persisted but routing is deferred — data lands in the single day partition).

- [ ] **Step 3: If either fails**, fix the minimal guard (e.g. ensure `putPartitionBy` emits nothing extra
  when `!isComposite()`; ensure the writer ignores `PartitionSpec` for routing in Plan 1).

- [ ] **Step 4: Re-run**  Run: `mvn -q -pl core test -Dtest=CompositeBackwardCompatTest`  Expected: PASS (2).

- [ ] **Step 5: Commit**
```bash
git add core/src/test/java/io/questdb/test/cairo/CompositeBackwardCompatTest.java
git commit -m "test(cairo): composite partitioning is additive; plain tables unaffected; routing deferred"
```

---

## Self-Review

**Spec coverage (Plan 1 slice):** §4 grammar → Tasks 2–3; §4 validation → Task 4; §6 additive metadata →
Tasks 5–6; SHOW CREATE (§4) → Task 7; §15 backward-compat → Tasks 5,8. Routing/pruning/merge/index/mat-view
are **out of this plan** (Plans 2–7) — intentional.

**Placeholder scan:** no "TBD"/"handle edge cases" — each code step names concrete classes/methods; test
bodies are complete. The two forward-referencing tests (Task 5 asserting via Task 6's reader) are marked
with the interim raw-block helper so Task 5 is runnable alone.

**Type consistency:** `PartitionSpec`/`PartitionDimension` signatures in Task 1 are used verbatim in Tasks
2,4,5,6,7; `getPartitionSpec()` name is consistent across `TableStructure`/`TableMetadata`/`CairoTable`;
`COMPOSITE_PARTITION_MINOR_VERSION` referenced in Tasks 5,6,8.

**Open confirmations folded from spec §17:** default naming mode = HIVE (Task 3 default); `ORDER BY`
within chunk dims persists as clustering columns (Tasks 3–5) — physical effect deferred to Plan 4/Phase 2.

## Build/test notes for the executor

- JDK 25 at `/usr/lib/jvm/java-25-openjdk-amd64` (`JAVA_HOME`); module `core`.
- Run a single test: `mvn -q -pl core test -Dtest=ClassName#method`.
- Before pushing, run the repo IntelliJ formatter parity (`java-lint` CI stage reconstructs a patch from
  the log) and `cargo fmt` is **not** needed here (no Rust touched in Plan 1).
