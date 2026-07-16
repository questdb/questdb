# Composite Partitioning — Plan 2: Dimension Dictionaries & Cell Registry

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give a composite-partitioned table the table-root persistent infrastructure that maps dimension **values ↔ dense ints** (per-dimension dictionaries) and distinct **dimension-tuples ↔ dense cell ordinals** (the cell registry), so later plans can key `_txn`/`_cv` and the on-disk directory tree on a compact `(ts, cellOrdinal)`.

**Architecture:** Both the dedicated dimension dictionaries and the cell registry are ordinary `SymbolMapWriter`/`SymbolMapReaderImpl` instances living at table root, registered into the writer's existing `denseSymbolMapWriters` list with `columnIndex = -1` (the codebase's existing "no owning column" sentinel), so they inherit `_txn`'s crash-safe count persistence and rollback **with zero `TxWriter` format changes**. The cell registry is such an interner whose "symbol" is a **fixed-width injective encoding of the dense-int dimension tuple**; its in-memory dedup cache is disabled (cell cardinality can be large). `identity(symbolCol)` dimensions reuse the source column's existing dictionary (no new interner); `hash(col,N)` needs no dictionary. The ordered set of dedicated interners a table needs is **derived deterministically from the persisted `PartitionSpec`** (Plan 1) identically on the writer and reader, so no new persisted identity is required.

**Tech Stack:** Java 25, QuestDB `core` module. Reuses `SymbolMapWriter`/`SymbolMapReaderImpl`/`MapWriter.createSymbolMapFiles`/`BitmapIndexWriter`. Prebuilt native libs are committed — Java tests need no Rust build.

## Global Constraints

- **JDK 25** at `/usr/lib/jvm/java-25-openjdk-amd64` (`JAVA_HOME`); module `core`. Single-test: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -q -pl core test -Dtest=ClassName`.
- **Plain (non-composite) tables must be byte-identical** in `_txn` and on disk: a non-composite table provisions **zero** dedicated interners, so `denseSymbolMapWriters` and the `_txn` symbol-count region are exactly as today.
- **No `_txn` binary-format change and no `ColumnType.VERSION`/`mig/` change.** Dedicated interners ride the existing per-symbol-count region purely by being extra `denseSymbolMapWriters` entries. Composite tables are already gated from old binaries by Plan 1's `META_FORMAT_MINOR_VERSION_COMPOSITE_PARTITIONING = 3`.
- **Writer and reader must derive the identical interner layout** (order + count) from the `PartitionSpec` + metadata, every open. Order is: `[real SYMBOL columns in metadata order]`, then `[dedicated dim dictionaries, one per truncate/expression dimension in PartitionSpec dimension order]`, then `[the cell registry, last]`.
- **The "has interners" gate is `CompositeInternerLayout.of(spec).hasInterners()` (⟺ `spec.getDimensionCount() > 0`), NOT `spec.isComposite()`.** `isComposite()` is also true for a cluster-only table (`PARTITION BY DAY ORDER BY col`, zero partition dimensions) — such a table has no dimension tuple, so it needs NO cell registry and NO dedicated dictionaries. Every interner-provisioning site (file creation, writer registration, reader registration) MUST gate on `hasInterners()`; a zero-dimension spec yields an EMPTY layout (`registrySlot() == -1`, `dedicatedCount() == 0`).
- **`columnIndex = -1`** for every dedicated interner (dictionaries and registry). Never add them to the sparse, `columnCount`-sized `symbolMapWriters`/`symbolMapReaders` arrays — those are keyed by real writer/column index. Provide a separate composite-dimension-keyed lookup instead.
- **Crash-safety is inherited, not reinvented:** rely on `SymbolMapWriter`'s `_txn`-count-driven truncation on reopen/rollback. Do NOT add a second count store.
- Every commit ends with `git add` of only the named files and the branch's trailer (`Co-Authored-By: Claude ...` / `Claude-Session: ...` — copy from `git log -1`).
- **Scope fence:** Plan 2 builds and persists the interners and their APIs, and registers them on writer/reader open. It does NOT route real row writes through them (Plan 4), does NOT widen `_txn` `attachedPartitions` to 2-D (Plan 3), and does NOT create on-disk cell subdirectories (Plan 4). In Plan 2 the interners are populated only by explicit API calls (and the tests that exercise them).

---

## File Structure

- **Create** `core/src/main/java/io/questdb/cairo/CompositeTupleCodec.java` — pure static fixed-width encode/decode between an `int[]` dense-tuple and a `CharSequence` (the registry "symbol"). No I/O.
- **Create** `core/src/main/java/io/questdb/cairo/CompositeInternerLayout.java` — pure logic: given a `PartitionSpec` + column metadata, produce the ordered list of dedicated-interner descriptors (which dimensions need a dedicated dictionary; the registry slot), the deterministic dense-slot ordering, and the reserved `columnNameTxn` per interner. No I/O.
- **Create** `core/src/main/java/io/questdb/cairo/CellRegistry.java` — thin wrapper over the registry `SymbolMapWriter` (write side) / `SymbolMapReader` (read side) that exposes `internCell(int[])→int`, `getTuple(int, int[] sink)→int[]`, `size()`, using `CompositeTupleCodec`.
- **Create** `core/src/main/java/io/questdb/cairo/CompositeDictionaries.java` — per-table holder, built on writer/reader open, that maps a composite dimension (by its index in `PartitionSpec`) to the `MapWriter`/`SymbolMapReader` it should intern/look-up through (identity → the source column's map; truncate/expr → the dedicated map; hash → none), plus the `CellRegistry`.
- **Modify** `core/src/main/java/io/questdb/cairo/TableUtils.java` — `createTableOrViewOrMatViewFiles` (~`:697-714`): after the per-column symbol-map file creation loop, create the dedicated dictionary + registry symbol-map files for a composite table.
- **Modify** `core/src/main/java/io/questdb/cairo/TableWriter.java` — `configureColumnMemory` (~`:5159-5199`): after the real-column symbol-map construction loop, construct and append the dedicated interners to `denseSymbolMapWriters` (columnIndex `-1`, `valueCountCollector = txWriter`), and build the writer-side `CompositeDictionaries`/`CellRegistry`.
- **Modify** `core/src/main/java/io/questdb/cairo/TableReader.java` — the symbol-map-reader setup (near `getSymbolMapReader` `:559` and the `symbolMapReaders` init): open the dedicated interner readers and build the reader-side `CompositeDictionaries`/`CellRegistry`.
- **Test** `core/src/test/java/io/questdb/test/cairo/CompositeTupleCodecTest.java`, `CompositeInternerLayoutTest.java`, `CellRegistryTest.java`, `CompositeDictionariesTest.java`, `CompositeDictPersistenceTest.java`.

---

### Task 1: Fixed-width injective tuple codec

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/CompositeTupleCodec.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeTupleCodecTest.java`

**Interfaces:**
- Produces: `CompositeTupleCodec.encode(int[] tuple, int len, StringSink sink)` writes a fixed-width string; `CompositeTupleCodec.decode(CharSequence s, int[] sink)` fills `sink` and returns the arity. Encoding is 8 hex chars per int (32-bit, zero-padded), concatenated, no delimiter — so equal tuples ⇒ equal strings and unequal tuples ⇒ unequal strings (injective given fixed arity). Negative ints are allowed (two's-complement hex).

- [ ] **Step 1: Write the failing test**
```java
public class CompositeTupleCodecTest {
    @Test
    public void testRoundTripAndInjective() {
        StringSink a = new StringSink();
        CompositeTupleCodec.encode(new int[]{7, 0, -1}, 3, a);
        Assert.assertEquals(24, a.length());               // 3 ints * 8 hex chars
        int[] out = new int[3];
        Assert.assertEquals(3, CompositeTupleCodec.decode(a, out));
        Assert.assertArrayEquals(new int[]{7, 0, -1}, out);

        StringSink b = new StringSink();
        CompositeTupleCodec.encode(new int[]{7, 0, 0}, 3, b);
        Assert.assertNotEquals(a.toString(), b.toString()); // distinct tuples -> distinct strings
    }
}
```

- [ ] **Step 2: Run test to verify it fails**
Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -q -pl core test -Dtest=CompositeTupleCodecTest`
Expected: FAIL — class does not exist.

- [ ] **Step 3: Write minimal implementation**
```java
public final class CompositeTupleCodec {
    private CompositeTupleCodec() {}

    public static void encode(int[] tuple, int len, CharSink<?> sink) {
        for (int i = 0; i < len; i++) {
            int v = tuple[i];
            for (int shift = 28; shift >= 0; shift -= 4) {
                sink.putAscii(Numbers.hexDigits[(v >>> shift) & 0xF]);
            }
        }
    }

    public static int decode(CharSequence s, int[] sink) {
        int arity = s.length() / 8;
        for (int i = 0; i < arity; i++) {
            int v = 0;
            for (int j = 0; j < 8; j++) {
                v = (v << 4) | hex(s.charAt(i * 8 + j));
            }
            sink[i] = v;
        }
        return arity;
    }

    private static int hex(char c) {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        throw CairoException.nonCritical().put("invalid cell-tuple hex char: ").put(c);
    }
}
```
(Verify `Numbers.hexDigits` exists and is lowercase; if the real helper differs, use the codebase's canonical hex writer. Confirm `CharSink` import path.)

- [ ] **Step 4: Run test to verify it passes**
Run: `JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 mvn -q -pl core test -Dtest=CompositeTupleCodecTest`
Expected: PASS.

- [ ] **Step 5: Commit** — `feat(cairo): fixed-width injective codec for composite cell tuples`

---

### Task 2: Deterministic interner layout from PartitionSpec

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/CompositeInternerLayout.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeInternerLayoutTest.java`

**Interfaces:**
- Consumes: `PartitionSpec` (Plan 1: `getDimensionCount()`, `getDimension(i)` → `PartitionDimension` with `getKind()`/`getColumnIndex()`/`getAlias()`).
- Produces: `CompositeInternerLayout.of(PartitionSpec spec)` returns an immutable descriptor with: `dedicatedCount()`; for each composite dimension index `i`, `needsDedicatedDict(i)` (true iff kind is `KIND_TRUNCATE` or `KIND_EXPRESSION`), `dedicatedDictSlot(i)` (its position among dedicated dicts, or -1), `dictName(i)` (the dimension alias) and `dictColumnNameTxn(i)` (a reserved non-`NONE` txn = `COMPOSITE_DICT_TXN_BASE + i`); and `registrySlot()` (= `dedicatedCount()`, i.e. last). `REGISTRY_NAME = "_cell"`, `REGISTRY_TXN = COMPOSITE_DICT_TXN_BASE - 1` (or another reserved constant distinct from all dict txns).

- [ ] **Step 1: Write the failing test**
```java
@Test
public void testLayoutOrdersDedicatedDictsThenRegistry() {
    // spec: identity(exchange) [no dict], hash(symbol,32) [no dict], truncate(symbol,3) [dict]
    PartitionSpec spec = new PartitionSpec();
    spec.setTimeUnit(PartitionBy.DAY);
    spec.addDimension(new PartitionDimension(PartitionDimension.KIND_IDENTITY, 1, 0, "exchange", null));
    spec.addDimension(new PartitionDimension(PartitionDimension.KIND_HASH, 2, 32, "symbol_hash", null));
    spec.addDimension(new PartitionDimension(PartitionDimension.KIND_TRUNCATE, 2, 3, "symbol_trunc", null));
    CompositeInternerLayout l = CompositeInternerLayout.of(spec);
    Assert.assertEquals(1, l.dedicatedCount());            // only the truncate dim
    Assert.assertFalse(l.needsDedicatedDict(0));
    Assert.assertFalse(l.needsDedicatedDict(1));
    Assert.assertTrue(l.needsDedicatedDict(2));
    Assert.assertEquals(0, l.dedicatedDictSlot(2));        // first (and only) dedicated dict
    Assert.assertEquals(1, l.registrySlot());              // registry after the 1 dedicated dict
    Assert.assertEquals("symbol_trunc", l.dictName(2).toString());
    Assert.assertTrue(l.dictColumnNameTxn(2) > TableUtils.COLUMN_NAME_TXN_NONE);
}
```

- [ ] **Step 2: Run to verify it fails** — `mvn -q -pl core test -Dtest=CompositeInternerLayoutTest` → FAIL (no class).

- [ ] **Step 3: Write minimal implementation** — a builder that walks `spec.getDimensions()`, marks truncate/expression dims as needing a dedicated dict, assigns dedicated slots `0..dedicatedCount-1` in dimension order, sets `registrySlot = dedicatedCount`, and computes the reserved txns. Store parallel `IntList`s. Define `COMPOSITE_DICT_TXN_BASE` (a large reserved constant, e.g. `Long.MAX_VALUE - 1024`, well clear of any real `columnNameTxn`) on `TableUtils` or this class. `of()` returns `EMPTY` (dedicatedCount 0, registrySlot -1) when `!spec.isComposite()`.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `feat(cairo): derive deterministic composite interner layout from PartitionSpec`

---

### Task 3: Create dedicated dictionary + registry files at CREATE TABLE

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableUtils.java` (`createTableOrViewOrMatViewFiles`, ~`:697-714`, where per-column `MapWriter.createSymbolMapFiles(...)` is called).
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeDictPersistenceTest.java` (first case).

**Interfaces:**
- Consumes: `CompositeInternerLayout` (Task 2), `MapWriter.createSymbolMapFiles(ff, mem, path.trimTo(pathSize), name, columnNameTxn, capacity, cacheFlag)` (per Agent A, `MapWriter.java:40-71`).

- [ ] **Step 1: Write the failing test** — create a composite table via SQL, then assert the dedicated dict `.o`/`.c`/`.k`/`.v` and the `_cell.o` registry files exist at table root:
```java
@Test
public void testCompositeCreatesDictAndRegistryFiles() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
        try (Path p = new Path()) {
            TableToken tt = engine.verifyTableName("t");
            p.of(engine.getConfiguration().getDbRoot()).concat(tt);
            int plen = p.size();
            // dedicated dict for truncate(symbol,3), aliased "symbol_trunc"
            Assert.assertTrue(ff().exists(TableUtils.offsetFileName(p.trimTo(plen), "symbol_trunc", firstDedicatedTxn()).$()));
            // cell registry
            Assert.assertTrue(ff().exists(TableUtils.offsetFileName(p.trimTo(plen), "_cell", CompositeInternerLayout.REGISTRY_TXN).$()));
        }
    });
}
```
(Confirm the exact `assertMemoryLeak`/`ff()`/`Path`/`engine` helpers by mirroring an existing `TableUtils`/create test; `firstDedicatedTxn()` = `CompositeInternerLayout.of(spec).dictColumnNameTxn(<truncate dim idx>)` — obtain the spec from the created table's metadata via `engine.getTableMetadata`.)

- [ ] **Step 2: Run to verify it fails** — files absent → FAIL.

- [ ] **Step 3: Write minimal implementation** — in `createTableOrViewOrMatViewFiles`, obtain the `PartitionSpec` from the `TableStructure` (`getPartitionSpec()`, Plan 1), build `CompositeInternerLayout layout = CompositeInternerLayout.of(spec)`, and gate on `layout.hasInterners()` (NOT `spec.isComposite()` — see Global Constraints; a cluster-only table has no interners). When it has interners, for each dedicated dict call `createSymbolMapFiles(ff, mem, path.trimTo(pathSize), layout.dictName(i), layout.dictColumnNameTxn(i), defaultSymbolCapacity, false)`; then create the registry files with name `_cell`, txn `REGISTRY_TXN`, `cacheFlag=false`. Use the same `mem`/`ff`/`path` already in scope.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `feat(cairo): create composite dictionary + cell-registry symbol files on CREATE TABLE`

---

### Task 4: CellRegistry wrapper (intern/getTuple over a SymbolMap)

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/CellRegistry.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CellRegistryTest.java`

**Interfaces:**
- Consumes: a `MapWriter` (write) or `SymbolMapReader` (read) over the `_cell` files; `CompositeTupleCodec` (Task 1).
- Produces: `CellRegistry` with `int internCell(int[] tuple, int arity)` (write: `symbolMap.put(encode(tuple))`), `void getTuple(int ordinal, int[] sink)` (read: `decode(reader.valueOf(ordinal), sink)`), `int size()`. A `CellRegistry` is constructed around either a writer or a reader; write-only methods throw if constructed read-only.

- [ ] **Step 1: Write the failing test** — drive a `CellRegistry` over a real `SymbolMapWriter` created against a temp table-root path (mirror an existing `SymbolMapWriterTest` for setup):
```java
@Test
public void testInternIsStableAndDense() throws Exception {
    // create _cell symbol files in a temp root, open a SymbolMapWriter (cache off), wrap in CellRegistry
    try (CellRegistry reg = openWriterRegistry(/* temp path, arity=2 */)) {
        Assert.assertEquals(0, reg.internCell(new int[]{5, 9}, 2));
        Assert.assertEquals(1, reg.internCell(new int[]{5, 10}, 2));
        Assert.assertEquals(0, reg.internCell(new int[]{5, 9}, 2));   // dedup -> same ordinal
        Assert.assertEquals(2, reg.size());
        int[] out = new int[2];
        reg.getTuple(1, out);
        Assert.assertArrayEquals(new int[]{5, 10}, out);
    }
}
```
(`openWriterRegistry` = create files via `MapWriter.createSymbolMapFiles`, construct a `SymbolMapWriter(config, path, "_cell", REGISTRY_TXN, 0 /*symbolCount*/, 0 /*symbolIndexInTxWriter*/, NOOP collector, -1 /*columnIndex*/)`, wrap. Confirm the exact constructor arity from `SymbolMapWriter.java:71-160` at implementation time and the `SymbolValueCountCollector` NOOP.)

- [ ] **Step 2: Run to verify it fails** — FAIL (no class).

- [ ] **Step 3: Write minimal implementation** — thin wrapper storing a `MapWriter writer` and/or `SymbolMapReader reader` and a reusable `StringSink`. `internCell` encodes then `writer.put(sink)`. `getTuple` reads `reader.valueOf(ordinal)` then decodes. `size()` = `writer.getSymbolCount()` / `reader.getSymbolCount()`.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `feat(cairo): CellRegistry intern/getTuple over the _cell symbol map`

---

### Task 5: Writer-side registration of dedicated interners

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`configureColumnMemory`, ~`:5159-5199`; add a `CompositeDictionaries compositeDicts` field + a `getCompositeDictionaries()` accessor + close/rollback handling).
- Create: `core/src/main/java/io/questdb/cairo/CompositeDictionaries.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeDictionariesTest.java` (writer case).

**Interfaces:**
- Consumes: `CompositeInternerLayout` (Task 2), `CellRegistry` (Task 4), the writer's `denseSymbolMapWriters` and `txWriter` (as `SymbolValueCountCollector`), `getSymbolMapWriter(int columnIndex)` (`:2510`).
- Produces: `CompositeDictionaries` with `MapWriter dictFor(int dimIndex)` (identity → `getSymbolMapWriter(dimension.getColumnIndex())`; truncate/expr → the dedicated writer; hash → null), `CellRegistry cellRegistry()`, and `int internRow(...)` deferred to Plan 4 (not built here). Writer-open builds it.

- [ ] **Step 1: Write the failing test** — open a writer on a composite table and assert the dedicated interners are registered and reachable, and that a plain table registers none:
```java
@Test
public void testWriterRegistersDedicatedInternersInOrder() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
        try (TableWriter w = getWriter("t")) {
            // spec: dim0 = identity(exchange) [reuses column dict, no dedicated];
            //       dim1 = truncate(symbol, 3) [dedicated dict]
            CompositeDictionaries d = w.getCompositeDictionaries();
            Assert.assertNull(d.dictFor(0));                        // identity -> reuses column dict, no dedicated
            Assert.assertNotNull(d.dictFor(1));                     // truncate -> dedicated dict
            Assert.assertNotNull(d.cellRegistry());
            // 2 real SYMBOL columns (exchange, symbol) + 1 dedicated dict + 1 registry = 4 dense maps
            Assert.assertEquals(2 + 2, w.getDenseSymbolMapCount());
        }
    });
}
@Test
public void testPlainTableRegistersNoDedicatedInterners() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
        try (TableWriter w = getWriter("p")) {
            Assert.assertFalse(w.getCompositeDictionaries().isComposite());
        }
    });
}
```
(Adjust dim indices to the actual spec order; add a small `getDenseSymbolMapCount()` test accessor if none exists. `getWriter` mirrors existing writer tests.)

- [ ] **Step 2: Run to verify it fails** — FAIL (no `getCompositeDictionaries`).

- [ ] **Step 3: Write minimal implementation** — after the per-column symbol-map loop in `configureColumnMemory`, build `CompositeInternerLayout layout = CompositeInternerLayout.of(metadata.getPartitionSpec())` and gate on `layout.hasInterners()` (NOT `isComposite()` — see Global Constraints): for each dedicated dict then the registry, construct a `SymbolMapWriter` **using the identical argument shape as the per-column construction at `TableWriter.java:5168-5182`** — that loop sets `symbolIndexInTxWriter = denseSymbolMapWriters.size()` immediately before `denseSymbolMapWriters.add(...)`, passes `txWriter` as the `SymbolValueCountCollector`, and reads the initial symbol count from `_txn` (copy that exact count-source expression verbatim — do NOT invent `unsafeReadSymbolCount`; the per-column loop is the source of truth, and it already yields 0 for a freshly-created table). Pass `columnIndex = -1`; for the registry pass `_cell`/`REGISTRY_TXN`/`cacheFlag=false`. `denseSymbolMapWriters.add(dw)` for each, in layout order (dedicated dicts, then registry). Wrap the registry writer in `CellRegistry`; store everything in a new `CompositeDictionaries`.
  **Why this is crash-safe for free (verified):** `MapWriter extends SymbolCountProvider` (`MapWriter.java:39`), and `txWriter.commit(denseSymbolMapWriters)` sets `symbolColumnCount = denseSymbolMapWriters.size()` and writes one count per dense entry (`TxWriter.java:645,782`); the partition table offset is `getPartitionTableSizeOffset(symbolColumnCount)` (`TxWriter.java:641,670`), so it shifts automatically. `TxReader` derives `symbolColumnCount = symbolsSize / Long.BYTES` from the header (`TxReader.java:634`) — no format/header change. Rollback (`TableWriter.java:13054`), truncate, sync, and close all iterate `denseSymbolMapWriters` — so no extra teardown beyond nulling the holder in the writer's close path.

- [ ] **Step 4: Run to verify it passes** — PASS. Also run `mvn -q -pl core test -Dtest=CompositeMetaFormatTest,CompositeBackwardCompatTest` — plain-table paths (`denseSymbolMapWriters.size()` == symbol-column count) unaffected and `_txn` byte-identical.

- [ ] **Step 5: Commit** — `feat(cairo): register composite dedicated interners on writer open`

---

### Task 6: Reader-side registration of dedicated interners

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableReader.java` (symbol-map-reader init; add `getCompositeDictionaries()`).
- Test: `CompositeDictionariesTest.java` (reader case) + `CompositeDictPersistenceTest.java` (reopen case).

**Interfaces:**
- Consumes: `SymbolMapReaderImpl` (per Agent A, `.of(...)`/constructor + `keyOf`/`valueOf`), `TableReaderMetadata.getPartitionSpec()` (Plan 1), `getDenseSymbolIndex` (`:167`).
- Produces: reader-side `CompositeDictionaries` with `SymbolMapReader dictReaderFor(int dimIndex)` and `CellRegistry cellRegistry()` (read-only).

- [ ] **Step 1: Write the failing test** — create + intern via the writer, `engine.releaseInactive()`, reopen a reader, assert the registry/dict read back the same ordinals:
```java
@Test
public void testReaderReadsRegistryAndDictAfterReopen() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
        int ord;
        try (TableWriter w = getWriter("t")) {
            ord = w.getCompositeDictionaries().cellRegistry().internCell(new int[]{3, 4}, 2);
            w.commit();  // persist symbol count into _txn
        }
        engine.releaseInactive();
        try (TableReader r = getReader("t")) {
            int[] out = new int[2];
            r.getCompositeDictionaries().cellRegistry().getTuple(ord, out);
            Assert.assertArrayEquals(new int[]{3, 4}, out);
        }
    });
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (no reader accessor / registry not opened).

- [ ] **Step 3: Write minimal implementation** — where `symbolMapReaders` are opened (`TableReader.java:1540-1545`, then each via `newSymbolMapReader(metadata.getDenseSymbolIndex(i), i)` at `:1279-1288`), build `CompositeInternerLayout layout = CompositeInternerLayout.of(metadata.getPartitionSpec())` and gate on `layout.hasInterners()` (NOT `isComposite()` — see Global Constraints): open a `SymbolMapReaderImpl` for each dedicated dict then the registry, at dense index `R + slot` where **`R` = the real symbol-column count** (the interners occupy dense indices immediately after the real columns, because the writer added them to `denseSymbolMapWriters` after the per-column loop). Read each count with `txFile.getSymbolValueCount(R + slot)` — the same accessor `newSymbolMapReader` uses (`:1288`). Store them in a **separate** reader-side list inside `CompositeDictionaries` — do NOT place them in the column-indexed, `columnCount`-sized `symbolMapReaders` array (`setPos(columnCount)` at `:1540`); they have no owning column. **Refresh their counts on every txn reload:** mirror `reloadSymbolMapCounts()` (`:1979-1984`, `updateSymbolCount(txFile.getSymbolValueCount(denseIndex))`) for the interner list, and close them in the reader's close path (`:1188-1191`). Compute `R` from metadata (count of SYMBOL columns) — it must equal what the writer used, so derive both from the same rule.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `feat(cairo): open composite dedicated interners on reader open`

---

### Task 7: CompositeDictionaries value-interning API (identity reuse / dedicated / hash)

**Files:**
- Modify: `CompositeDictionaries.java` (add value→int and int→value per dimension).
- Test: `CompositeDictionariesTest.java` (interning semantics).

**Interfaces:**
- Produces: `int internValue(int dimIndex, <rawColumnValue>)` — identity → `dictFor(dim).put(value)`; truncate → `dedicatedDict.put(truncate(value, N))`; hash → `Hash.boundedHash(value, N)` (no dict); expr → `dedicatedDict.put(exprResult)` (expr evaluation itself is Plan 4; here expose the dedicated-dict `put` path). `keyOfValue(dimIndex, value)` (prune) and `valueOfKey(dimIndex, int)` (label) mirror using the reader.

- [ ] **Step 1: Write the failing test** — writer-side: intern the same symbol value on an `identity` dimension and confirm it returns the SAME dense int as the source column's own symbol map (proving reuse), and that a `hash(col,N)` dimension yields a key in `0..N-1` with no dedicated dict:
```java
@Test
public void testIdentityReusesColumnDictAndHashNeedsNone() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, hash(symbol, 8) wal");
        try (TableWriter w = getWriter("t")) {
            CompositeDictionaries d = w.getCompositeDictionaries();
            int viaDim = d.internValue(0, "NYSE");                       // identity(exchange)
            int viaCol = w.getSymbolMapWriter(/*exchange col idx*/ 1).put("NYSE");
            Assert.assertEquals(viaCol, viaDim);                          // same dictionary
            int h = d.internValue(1, "BTC");                             // hash(symbol,8)
            Assert.assertTrue(h >= 0 && h < 8);
            Assert.assertNull(d.dictFor(1));                             // hash has no dedicated dict
        }
    });
}
```

- [ ] **Step 2: Run to verify it fails** — FAIL (no `internValue`).

- [ ] **Step 3: Write minimal implementation** — dispatch on `dimension.getKind()`: IDENTITY → `getSymbolMapWriter(dim.getColumnIndex()).put(value)`; TRUNCATE → apply the truncate transform to the value then `dedicatedDict.put(...)` (truncate on a symbol value = a prefix/bucketing per the transform param; reuse the same transform the resolver documents — keep it a pure helper); HASH → `Hash.boundedHash(value)` bounded to N (match QuestDB's existing symbol-hash if one exists); EXPRESSION → `dedicatedDict.put(exprText-evaluated)` — but since expression evaluation is Plan 4, throw `UnsupportedOperationException("composite expression dimensions land in Plan 4")` here and cover only identity/hash/truncate. Read-side `keyOfValue`/`valueOfKey` mirror via `SymbolMapReader.keyOf`/`valueOf`.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `feat(cairo): composite dimension value interning (identity reuse, hash, truncate)`

---

### Task 8: Crash-safety, rollback, and plain-table byte-identity guard

**Files:**
- Test only: `core/src/test/java/io/questdb/test/cairo/CompositeDictPersistenceTest.java` (add cases); minimal writer guard only if a test reveals a gap.

**Interfaces:** Consumes everything above.

- [ ] **Step 1: Write the failing/guard tests**
```java
@Test
public void testUncommittedInternsRolledBackOnReopen() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
        try (TableWriter w = getWriter("t")) {
            w.getCompositeDictionaries().cellRegistry().internCell(new int[]{1, 2}, 2);
            w.rollback();                                        // discard without commit
            Assert.assertEquals(0, w.getCompositeDictionaries().cellRegistry().size());
        }
        engine.releaseInactive();
        try (TableReader r = getReader("t")) {
            Assert.assertEquals(0, r.getCompositeDictionaries().cellRegistry().size()); // reopen sees nothing
        }
    });
}
@Test
public void testPlainTableTxnByteIdenticalToPreFeature() throws Exception {
    // A plain table provisions no dedicated interners: its dense symbol map count and _txn symbol
    // region are exactly as a pre-feature table. Assert dense symbol map count == number of SYMBOL cols.
    assertMemoryLeak(() -> {
        execute("create table p (ts timestamp, a symbol, b symbol) timestamp(ts) partition by day wal");
        try (TableWriter w = getWriter("p")) {
            Assert.assertEquals(2, w.getDenseSymbolMapCount());  // exactly the 2 SYMBOL columns
            Assert.assertFalse(w.getCompositeDictionaries().isComposite());
        }
    });
}
```

- [ ] **Step 2: Run** — `testUncommittedInternsRolledBackOnReopen` should PASS if crash-safety is correctly inherited from `_txn` (writer `rollback()` iterates `denseSymbolMapWriters` including the registry — Agent A confirmed `rollbackSymbolTables` is columnIndex-agnostic). `testPlainTableTxnByteIdenticalToPreFeature` should PASS (no dedicated interners for plain).

- [ ] **Step 3: If either fails**, add the minimal guard: ensure the dedicated interners are appended to `denseSymbolMapWriters` (so rollback/truncate reach them) and that the layout returns empty for non-composite tables (so plain tables allocate nothing).

- [ ] **Step 4: Re-run** — PASS.

- [ ] **Step 5: Commit** — `test(cairo): composite interner crash-safety + plain-table _txn byte-identity`

---

## Self-Review

**Spec coverage (Plan 2 slice of design §5):** per-dimension dictionaries (identity-reuse / hash-none / truncate-expr dedicated) → Tasks 2,3,5,6,7; table-root cell registry (dim-tuple ↔ dense ordinal) → Tasks 1,3,4,5,6; crash-safety + backward-compat → Task 8. Physical routing, on-disk cell dirs, `_txn` 2-D partition table, and expression-dim evaluation are explicitly **out of this plan** (Plans 3–4).

**Placeholder scan:** each task names concrete classes/methods and gives test code. Integration tasks (3,5,6) cite the exact anchors research surfaced; implementers must confirm the precise `SymbolMapWriter` constructor arity and the `createSymbolMapFiles`/`getSymbolMapWriter` signatures at implementation time (the plan flags this inline).

**Type consistency:** `CompositeInternerLayout` produced by Task 2 is consumed by Tasks 3,5,6; `CellRegistry` (Task 4) by Tasks 5,6,8; `CompositeDictionaries` (Task 5) extended by Tasks 6,7. `columnIndex = -1` and the deterministic order `[real symbols][dedicated dicts][registry]` are used identically in Tasks 5 and 6.

**Risk note for the executor:** Tasks 5–6 add non-column entries to `denseSymbolMapWriters` on the hot writer/reader-open path. Agent-audited safe (the one dense-entry consumer that correlates back to a column, `scaleSymbolCapacities`, already guards `columnIndex > -1`), but WAL-segment-writer / O3-split-squash / mat-view-refresh / backup-checkpoint paths were **not** exhaustively swept — the whole-branch review at the end of this plan MUST sweep those before this is considered routing-ready.
