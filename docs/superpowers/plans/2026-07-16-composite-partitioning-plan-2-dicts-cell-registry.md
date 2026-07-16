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
- Consumes: a `MapWriter` (write side) or a `SymbolMapReader` (read side) over the `_cell` files; `CompositeTupleCodec` (Task 1).
- Produces: `CellRegistry` with `int internCell(int[] tuple, int arity)` (write side: `writer.put(encode(tuple))`), `void getTuple(int ordinal, int[] sink)` (read side: `decode(reader.valueOf(ordinal), sink)`), `int size()`. A `CellRegistry` is constructed around **either** a writer **or** a reader. `internCell` throws `IllegalStateException` when there is no writer; `getTuple` throws when there is no reader. **`getTuple` is inherently a read-side operation** — `SymbolMapWriter` has NO `valueOf(int)` (verified: only `getSymbolCount()`/memory accessors), so reverse lookup requires a `SymbolMapReader`.

- [ ] **Step 1: Write the failing test** — two tests: a write-side intern/dedup/size test, and a read-side round-trip that interns via a writer, closes it, then reopens a `SymbolMapReaderImpl` over the same files to reverse-lookup. Mirror the standalone symbol-map setup in `SymbolMapTest.java:99,184`:
```java
@Test
public void testInternIsStableAndDense() throws Exception {
    // write side: intern tuples -> dense ordinals, stable + dedup
    try (CellRegistry reg = openWriterRegistry()) {            // creates _cell files + opens a SymbolMapWriter
        Assert.assertEquals(0, reg.internCell(new int[]{5, 9}, 2));
        Assert.assertEquals(1, reg.internCell(new int[]{5, 10}, 2));
        Assert.assertEquals(0, reg.internCell(new int[]{5, 9}, 2));   // dedup -> same ordinal
        Assert.assertEquals(2, reg.size());
    }
}

@Test
public void testGetTupleRoundTripViaReader() throws Exception {
    try (CellRegistry w = openWriterRegistry()) {              // intern via the writer, then close it
        w.internCell(new int[]{5, 9}, 2);
        w.internCell(new int[]{5, 10}, 2);
    }
    try (CellRegistry r = openReaderRegistry(2)) {             // reopen a reader over the same _cell files, count=2
        int[] out = new int[2];
        r.getTuple(0, out);
        Assert.assertArrayEquals(new int[]{5, 9}, out);
        r.getTuple(1, out);
        Assert.assertArrayEquals(new int[]{5, 10}, out);
        Assert.assertEquals(2, r.size());
    }
}
```
Helpers (verified signatures): `openWriterRegistry` = create the files via `MapWriter.createSymbolMapFiles(ff, mem, path, "_cell", REGISTRY_TXN, capacity, false)`, then `new SymbolMapWriter(configuration, path, "_cell", REGISTRY_TXN, 0 /*symbolCount*/, 0 /*symbolIndexInTxWriter*/, SymbolValueCountCollector.NOOP, -1 /*columnIndex*/)`, wrap in a `CellRegistry`. `openReaderRegistry(count)` = `new SymbolMapReaderImpl(configuration, path, "_cell", REGISTRY_TXN, count)`, wrap. (`SymbolMapWriter` ctor: `(CairoConfiguration, Path, CharSequence, long, int, int, SymbolValueCountCollector, int)`; `SymbolMapReaderImpl` ctor: `(CairoConfiguration, Path, CharSequence, long, int)`. The writer ctor requires the offset file to pre-exist — always `createSymbolMapFiles` first. Close the writer before opening the reader so its appends are flushed.)

- [ ] **Step 2: Run to verify it fails** — FAIL (no class).

- [ ] **Step 3: Write minimal implementation** — thin wrapper holding a `MapWriter writer` (nullable) OR a `SymbolMapReader reader` (nullable) and a reusable `StringSink`. `internCell(tuple, arity)`: require `writer != null` (else `IllegalStateException`), `CompositeTupleCodec.encode(tuple, arity, sink)`, return `writer.put(sink)`. `getTuple(ordinal, sink)`: require `reader != null`, `CompositeTupleCodec.decode(reader.valueOf(ordinal), sink)`. `size()`: `writer != null ? writer.getSymbolCount() : reader.getSymbolCount()`. `close()` frees whichever it owns (the test's try-with-resources drives it). Reset the `StringSink` before each encode.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `feat(cairo): CellRegistry intern/getTuple over the _cell symbol map`

---

### Task 5: Interners as first-class `_txn` symbol maps (create-path count + writer registration)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableUtils.java` — **Part A**: in `createTableOrViewOrMatViewFiles`, inside the existing `if (compositeLayout.hasInterners()) { … }` block (the one Task 3 added, right after it creates the interner files), add `symbolMapCount += compositeLayout.dedicatedCount() + 1;` so the initial `_txn` written by `createTxn(mem, symbolMapCount, …)` (~`:790`) sizes its symbol-count region to include the dedicated dicts + registry (all counts 0). `symbolMapCount` is declared at ~`:698` and consumed at ~`:790`; the `hasInterners()` block sits between them, so the bump reaches `createTxn`.
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` — **Part B**: `configureColumnMemory` (~`:5159-5199`); add a `CompositeDictionaries compositeDicts` field, a `getCompositeDictionaries()` accessor (returns null when the table has no interners), a `getDenseSymbolMapCount()` test accessor, and null-out `compositeDicts` in the writer's close path.
- Create: `core/src/main/java/io/questdb/cairo/CompositeDictionaries.java`
- Test: `core/src/test/java/io/questdb/test/cairo/CompositeDictionariesTest.java`.

**Why both halves land together (the core invariant):** the interners are first-class `_txn` symbol maps. The create-path `_txn` count (A) and the writer's `denseSymbolMapWriters` registration (B) are two halves of ONE invariant — `_txn.symbolColumnCount == denseSymbolMapWriters.size() == realSymbolCols + dedicatedDicts + 1`. If they landed apart, a writer commit with no registered interners would rewrite `symbolColumnCount` back to `realSymbolCols` (clobber), and a create that counted interners with a non-registering writer would read counts out of range. With BOTH, every existing loop handles interners uniformly, **no special-casing**: commit sets `symbolColumnCount = denseSymbolMapWriters.size()` (`TxWriter.java:645,782`); rollback (`TableWriter.java:13050-13054`) iterates `txWriter.unsafeReadSymbolColumnCount()` — which now includes the interner slots from creation, so uncommitted interns ARE discarded on rollback (Task 8 depends on this); sync/truncate/close iterate `denseSymbolMapWriters`; `TxReader` self-derives `symbolColumnCount = symbolsSize / 8` (`TxReader.java:634`). Plain tables have zero interners → `symbolMapCount` unchanged → `_txn` byte-identical.

**Interfaces:**
- Consumes: `CompositeInternerLayout` (Task 2), `CellRegistry` (Task 4), the writer's `denseSymbolMapWriters` + `txWriter`.
- Produces: `CompositeDictionaries` (holder) with `MapWriter dedicatedDictFor(int dimIndex)` (the dedicated `SymbolMapWriter` for a truncate/expression dim, else null) and `CellRegistry cellRegistry()`. `TableWriter.getCompositeDictionaries()` returns the holder, or **null** for a table with no interners. Value interning (`internValue`, identity-reuse) is Task 7.

- [ ] **Step 1: Write the failing tests**
```java
@Test
public void testInitialTxnCountsInterners() throws Exception {          // Part A
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
        // 2 real SYMBOL cols + 1 dedicated dict (truncate) + 1 registry = 4
        try (TableReader r = getReader("t")) {
            Assert.assertEquals(4, r.getTxFile().unsafeReadSymbolColumnCount());  // or the reader's symbol-column-count accessor
        }
    });
}
@Test
public void testWriterRegistersDedicatedInternersInOrder() throws Exception {   // Part B
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, truncate(symbol, 3) wal");
        try (TableWriter w = getWriter("t")) {
            // dim0 = identity(exchange) [reuses column dict, no dedicated]; dim1 = truncate(symbol,3) [dedicated]
            CompositeDictionaries d = w.getCompositeDictionaries();
            Assert.assertNotNull(d);
            Assert.assertNull(d.dedicatedDictFor(0));               // identity -> no dedicated dict
            Assert.assertNotNull(d.dedicatedDictFor(1));            // truncate -> dedicated dict
            Assert.assertNotNull(d.cellRegistry());
            Assert.assertEquals(2 + 2, w.getDenseSymbolMapCount()); // 2 real symbols + dict + registry
        }
    });
}
@Test
public void testPlainTableRegistersNoInterners() throws Exception {
    assertMemoryLeak(() -> {
        execute("create table p (ts timestamp, s symbol) timestamp(ts) partition by day wal");
        try (TableWriter w = getWriter("p")) {
            Assert.assertNull(w.getCompositeDictionaries());        // no interners for a plain table
            Assert.assertEquals(1, w.getDenseSymbolMapCount());     // exactly the 1 SYMBOL column
        }
        try (TableReader r = getReader("p")) {
            Assert.assertEquals(1, r.getTxFile().unsafeReadSymbolColumnCount()); // _txn unchanged for plain
        }
    });
}
```
(Confirm the exact `_txn` symbol-column-count read accessor by mirroring an existing test that inspects `_txn` — e.g. `TableReader.getTxFile()` then `unsafeReadSymbolColumnCount()`/`getSymbolColumnCount()`. Add the `getDenseSymbolMapCount()` accessor to `TableWriter` if none exists — `return denseSymbolMapWriters.size();`.)

- [ ] **Step 2: Run to verify it fails** — `testInitialTxnCountsInterners` fails (count is 2, not 4, until Part A); `testWriterRegistersDedicatedInternersInOrder` fails (no `getCompositeDictionaries`).

- [ ] **Step 3: Write minimal implementation**
  **Part A** (`TableUtils.createTableOrViewOrMatViewFiles`): inside the existing `if (compositeLayout.hasInterners())` block, after the registry `createSymbolMapFiles(...)` call, add `symbolMapCount += compositeLayout.dedicatedCount() + 1;`.
  **Part B** (`TableWriter.configureColumnMemory`): after the per-column symbol-map loop, build `CompositeInternerLayout layout = CompositeInternerLayout.of(metadata.getPartitionSpec())`; if `layout.hasInterners()`, for each dim `i` with `layout.needsDedicatedDict(i)`, then the registry, construct a `SymbolMapWriter` **using the identical argument shape as the per-column construction at `TableWriter.java:5168-5182`** — set `symbolIndexInTxWriter = denseSymbolMapWriters.size()`, pass `txWriter` as the collector, and read the initial count from `_txn` via `txWriter.getSymbolValueCount(symbolIndexInTxWriter)` (valid because Part A put the slot in `_txn` at count 0). Pass the reserved `columnNameTxn` (`layout.dictColumnNameTxn(i)` / `REGISTRY_TXN`), `columnIndex = -1`, and for the registry `_cell`/`cacheFlag=false`. `denseSymbolMapWriters.add(w)` for each in layout order (dedicated dicts, then registry). Wrap the registry writer in a `CellRegistry`; store the dedicated dicts (keyed by dim index) + the registry in a new `CompositeDictionaries`; assign to `compositeDicts`. (For a table with no interners, leave `compositeDicts == null`.) Rollback/truncate/sync/close already iterate `denseSymbolMapWriters` — no extra teardown beyond nulling `compositeDicts` in close.

- [ ] **Step 4: Run to verify it passes** — PASS (3 tests). Also run `mvn -q -pl core test -Dtest=CompositeMetaFormatTest,CompositeBackwardCompatTest,CompositeDictPersistenceTest` — plain-table paths unaffected, composite create still green.

- [ ] **Step 5: Commit** — `feat(cairo): register composite interners as first-class _txn symbol maps (create count + writer open)`

---

### Task 6: Reader-side registration of dedicated interners

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableReader.java` (symbol-map-reader init; add `getCompositeDictionaries()`).
- Test: `CompositeDictionariesTest.java` (reader case) + `CompositeDictPersistenceTest.java` (reopen case).

**Interfaces:**
- Consumes: `SymbolMapReaderImpl` (constructor `(CairoConfiguration, Path, CharSequence columnName, long columnNameTxn, int symbolCount)` + `keyOf`/`valueOf`/`updateSymbolCount`), `TableReaderMetadata.getPartitionSpec()` (Plan 1), `TableReader.getTxFile()` → `TxReader.getSymbolColumnCount()`.
- Produces: makes `CompositeDictionaries` **dual-mode** (mirrors `CellRegistry`): adds a reader-side constructor `(ObjList<SymbolMapReader> dedicatedDictReaders, CellRegistry readerCellRegistry)` and `SymbolMapReader dictReaderFor(int dimIndex)` (null for non-dedicated dims / on the writer side). `cellRegistry()` returns the read-side `CellRegistry` (built over the registry `SymbolMapReader`). `TableReader.getCompositeDictionaries()` returns the reader-side holder, or null when the table has no interners.

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

- [ ] **Step 3: Write minimal implementation** — where `symbolMapReaders` are opened (`TableReader.java:1540-1545`, each via `newSymbolMapReader(metadata.getDenseSymbolIndex(i), i)` at `:1279-1288`), build `CompositeInternerLayout layout = CompositeInternerLayout.of(metadata.getPartitionSpec())` and gate on `layout.hasInterners()` (NOT `isComposite()` — see Global Constraints). **The interners are the LAST `internerCount = layout.dedicatedCount() + 1` symbol slots** in `_txn` (the writer appends them after all real symbol columns, and symbol-column DROP compacts them but keeps them trailing — this is DROP-robust and does NOT require recomputing the real-symbol count). So with `n = txFile.getSymbolColumnCount()`: the dedicated dict for the `s`-th dedicated slot is at dense index `n - internerCount + s`, and the registry is at dense index `n - 1`. Open a `SymbolMapReaderImpl` for each (name/txn from `layout.dictName(i)`/`dictColumnNameTxn(i)` and `REGISTRY_NAME`/`REGISTRY_TXN`), constructing with the count from `txFile.getSymbolValueCount(denseIndex)` — the same accessor `newSymbolMapReader` uses (`:1288`). Store the interner readers in a **`TableReader`-owned `ObjList<SymbolMapReader>`** (do NOT place them in the column-indexed, `columnCount`-sized `symbolMapReaders` array at `:1540` — they have no owning column); wrap the registry reader in a read-side `CellRegistry` and build the reader-side `CompositeDictionaries` (dual-mode). **Ownership (mirror Task 5's non-owning holder):** `TableReader` owns and frees this list in its close path — mirror the `symbolMapReaders` free loop `:1188-1191` (`Misc.freeIfCloseable`) — while the reader-side `CompositeDictionaries`/`CellRegistry` are NON-OWNING views: on close just null the holder, do NOT call `CellRegistry.close()` or otherwise free the registry reader through the holder (it would double-free against the `TableReader` list). **Refresh counts on every txn reload:** mirror `reloadSymbolMapCounts()` (`:1979-1984`, `updateSymbolCount(txFile.getSymbolValueCount(denseIndex))`) for the interner list, recomputing the dense indices from the current `getSymbolColumnCount()` each reload.

- [ ] **Step 4: Run to verify it passes** — PASS.

- [ ] **Step 5: Commit** — `feat(cairo): open composite dedicated interners on reader open`

---

### Task 7: Dimension value-interning API (identity reuse / hash / truncate)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` — add `public int internDimensionValue(int dimIndex, CharSequence value)`.
- Modify: `core/src/main/java/io/questdb/cairo/TableReader.java` — add `public int keyOfDimensionValue(int dimIndex, CharSequence value)` and `public CharSequence valueOfDimensionKey(int dimIndex, int key)`.
- Create: `core/src/main/java/io/questdb/cairo/CompositeDimensionTransform.java` — a tiny static helper for the truncate prefix, shared by writer + reader (one place for the transform).
- Test: `CompositeDictionariesTest.java` (writer interning) + `CompositeDictPersistenceTest.java` (reader keyOf round-trip).

**Why here, not on `CompositeDictionaries`:** identity dims REUSE the source column's own symbol map — that is `getSymbolMapWriter(columnIndex)` (writer) / `getSymbolMapReader(columnIndex)` (reader), which live on `TableWriter`/`TableReader`. Housing the dispatch there uses existing accessors + `getCompositeDictionaries()` and avoids re-threading source-column maps into the holder.

**Interfaces:**
- Consumes: `metadata.getPartitionSpec().getDimension(dimIndex)` (`getKind()`/`getColumnIndex()`/`getParam()`), `getSymbolMapWriter(int)` (`TableWriter:2510`) / `getSymbolMapReader(int)` (`TableReader:559`), `getCompositeDictionaries().dedicatedDictFor(dimIndex)` (writer) / `.dictReaderFor(dimIndex)` (reader), `io.questdb.std.Hash.boundedHash(CharSequence, int)` (`Hash.java:49`).
- Produces: `TableWriter.internDimensionValue(dimIndex, value) → int`; `TableReader.keyOfDimensionValue(dimIndex, value) → int` (returns `SymbolTable.VALUE_NOT_FOUND` when absent) and `valueOfDimensionKey(dimIndex, key) → CharSequence`; `CompositeDimensionTransform.truncatedPrefix(CharSequence value, int n, StringSink sink) → CharSequence` (first `n` chars — if `value == null` or `value.length() <= n`, return `value` unchanged).

**Dispatch (same shape writer/reader, on `dim.getKind()`):**
- IDENTITY → reuse the source column dict: writer `getSymbolMapWriter(dim.getColumnIndex()).put(value)`; reader `getSymbolMapReader(dim.getColumnIndex()).keyOf(value)` / `.valueOf(key)`. Returns the SAME ordinal as the column itself.
- HASH → `Hash.boundedHash(value, dim.getParam())` — pure, in `[0, param)`, NO dict, identical on writer + reader; `valueOfDimensionKey` for a hash dim has no reverse → return `null`.
- TRUNCATE → intern the first-`param`-char prefix into the dedicated dict: writer `dedicatedDictFor(dimIndex).put(CompositeDimensionTransform.truncatedPrefix(value, dim.getParam(), sink))`; reader `dictReaderFor(dimIndex).keyOf(prefix)` / `.valueOf(key)`.
- EXPRESSION → `throw new UnsupportedOperationException("composite expression dimensions land in Plan 4")`.

- [ ] **Step 1: Write the failing tests**
```java
@Test
public void testInternDimensionValueIdentityReuseAndHash() throws Exception {   // writer
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, exchange symbol, symbol symbol) " +
                "timestamp(ts) partition by day, exchange, hash(symbol, 8) wal");
        try (TableWriter w = getWriter("t")) {
            int viaCol = w.getSymbolMapWriter(1).put("NYSE");         // exchange col idx 1
            int viaDim = w.internDimensionValue(0, "NYSE");           // identity(exchange)
            Assert.assertEquals(viaCol, viaDim);                     // identity reuses the column dict
            int h = w.internDimensionValue(1, "BTC");                // hash(symbol, 8)
            Assert.assertTrue(h >= 0 && h < 8);
        }
    });
}
@Test
public void testTruncateDimInternsPrefixAndReaderKeyOf() throws Exception {     // truncate + reader round-trip
    assertMemoryLeak(() -> {
        execute("create table t (ts timestamp, symbol symbol) " +
                "timestamp(ts) partition by day, truncate(symbol, 3) wal");
        int key;
        try (TableWriter w = getWriter("t")) {
            key = w.internDimensionValue(0, "BTCUSDT");              // truncate dim0 -> prefix "BTC"
            Assert.assertEquals(key, w.internDimensionValue(0, "BTCETH")); // same prefix -> same key
            TableWriter.Row row = w.newRow(0);                       // a real row so commit() persists (see Task 6 nuance)
            row.putSym(1, "BTCUSDT");
            row.append();
            w.commit();
        }
        engine.releaseInactive();
        try (TableReader r = getReader("t")) {
            Assert.assertEquals(key, r.keyOfDimensionValue(0, "BTCZZZ")); // "BTC" prefix -> same key
            TestUtils.assertEquals("BTC", r.valueOfDimensionKey(0, key));
        }
    });
}
```
(Verify the `newRow`/`putSym`/`append` idiom + column indices against an existing WAL-table write test; the row is only to create a transaction so `commit()` persists the dedicated-dict count.)

- [ ] **Step 2: Run to verify it fails** — FAIL (no `internDimensionValue`).

- [ ] **Step 3: Write minimal implementation** — add the two methods + the static helper per the Dispatch table above. Keep a reusable `StringSink` on the writer/reader for the truncate prefix (do not allocate per call). Guard the writer method: `internDimensionValue` requires `getCompositeDictionaries() != null` for truncate dims (a composite table always has it); identity/hash need only the spec.

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
