# Covering Index in Parquet Form — Phase 2C Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a covering POSTING index sealed in Parquet form readable — `ParquetPostingIndexFwdReader` / `ParquetPostingIndexBwdReader` serving the `PostingIndexReader` contract off `<col>.pidx.<indexTxn>._im` plus `<col>.pidx.<indexTxn>.parquet`, dispatched on the partition's on-disk form, with pruning levels 1–3 and `collectDistinctKeys`.

**Architecture:** Phase 2 built the write path and left `TableReader.checkPostingIndexIsReadable` refusing every query over a parquet-form index. 2C replaces that refusal with dispatch. The reader binds an `IndexMetaFileReader` over the `_im` and a `ParquetFileDecoder` over the pidx parquet — **not** `ParquetPartitionDecoder`, which requires a `_pm` the pidx parquet does not have — resolves a key to a row-group range out of the `_im` directory with no parquet read at all, prunes by the row-id zone maps, decodes only the projected columns, and feeds `CoveringRowCursor` exactly as the native cursors do. The five mmap-shaped `IndexReader` methods return `0`; `getKeyCount()` returns the `_im`'s `KEY_SPACE_SIZE`. Two carried-forward write-path defects (the O3 update-mode leak and the `DROP INDEX` token residue) are closed at the end, because a reader is what turns both from leaks into read failures.

**Tech Stack:** Java 17 (zero-GC, `Unsafe`/mmap), Rust (`qdb-parquet-meta`, `qdbr` JNI, `parquet_write`), Maven, Cargo, JUnit 4 with `assertMemoryLeak`.

## Global Constraints

Every task's requirements implicitly include this whole section.

### Sources of truth

- Spec: `docs/superpowers/specs/2026-08-10-covering-index-parquet-design.md`. **It carries five inline `(CORRECTED …)` notes as of `0ba74f829b`; where a note exists, the note is the spec.** Normative on-disk formats: `docs/index-metadata.md` (`_im` v3) and `docs/parquet-metadata.md` (`_pm`). Durable ledger: `.superpowers/sdd/progress.md`. Branch `feat/covering-index-parquet`, worktree `~/claude/wt/pidx-parquet`.
- **The ledger wins on facts about the code; the spec wins on intent.** Where this plan records a conflict, it names it in the task rather than choosing silently.

### House style

- Java members grouped by kind (static vs instance) and visibility, then sorted **alphabetically**. No `// ===` or `// ---` banner comments. Exception and log messages strictly **ASCII**. Underscore separators in numbers with 5+ digits. Booleans use `is`/`has` prefixes. Modern Java 17 (enhanced switch, pattern variables).
- Tests use `assertMemoryLeak()`. Native memory, file descriptors and JNI writer/result pointers must be freed on **every** path including error paths.
- New Java files carry the standard QuestDB Apache-2.0 header, copied from a neighbour in the same package.
- Rust builds under `-D warnings` (`core/pom.xml:52`): an unused variable, import or `mut` is a **build failure**.
- Do **not** run multiple `mvn test` commands in parallel.

### The build and test gate

- `export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR` before every Maven command.
- Java tests that cross JNI **require** `mvn -pl core -Pbuild-rust-library -Dtest=... test` (profile at `core/pom.xml:522`). Without it you test a stale `core/target/classes` and JNI tests die with an `UnsatisfiedLinkError that does not name its cause`.
- `rm -rf core/target/surefire-reports` **before each run**, and read the counts out of the XML afterwards. `mvn -q` suppresses the Tests-run lines, `-DfailIfNoSpecifiedTests=false` makes a pattern matching nothing still report BUILD SUCCESS, and a shell exit code taken after a pipe is the last pipeline stage's, not Maven's. Capture Maven's own exit code into a variable **before** any pipe.
- **Rust gates run per crate.** `qdb-parquet-meta` is a path dependency, NOT a workspace member: `cargo test --lib`, `cargo fmt` and `cargo clippy` run from `core/rust/qdbr` execute **zero** of its tests, reformat none of its files and lint none of its code. Any task touching it must gate from inside `core/rust/qdb-parquet-meta` as well, and additionally with `cargo test --lib --release`, because several guards there are `debug_assert!` with a release fallback and their tests are `cfg(debug_assertions)`-split.
- Any new `PropertyKey` breaks `ServerMainTest#testShowParameters`, which asserts exact set equality on `SHOW PARAMETERS`. Add the line to `expectedProps` in the same commit.

### Process requirements — these are why Phase 2 needed five fix waves

- **Every fix carries a negative control.** Revert **only** the production change, exercise it **through the production entry path**, confirm the test fails, restore, confirm it passes, and report the **exact failure message**. Two controls in Phase 2 were vacuous because they never reached production code — a control that edits a test, a config default, or a code path production does not take proves nothing.
- **Do not assert via `LogCapture.waitForRegex` or `LogCapture.waitFor`.** `core/src/test/java/io/questdb/test/tools/LogCapture.java:75` and `:88` check `elapsed > maxWait` after a loop that exits at `elapsed >= maxWait`, so **a timed-out wait returns silently, asserting nothing**. One Phase 2 control ran 121 s and passed while asserting nothing. 26 call sites across ~11 classes; the fix is its own PR. Assert on a counter, a file, or a query result instead.
- **Verify an invariant before relying on it, including one this plan, the spec, or a reviewer states.** Every Phase 2 fix wave introduced a defect by adopting a stated invariant unchecked, and five agents improved on a suggested fix by refusing it with evidence. If a step's premise is false, say so and report it rather than building on it.
- **Build the world so the two answers differ.** A differential test whose two arms cannot disagree, or an assertion that would pass under the mutation it exists to catch, is a plan failure. Run the negative control before believing a green.

### Hard constraint carried into 2C: do not re-root the `_pm` `prev` chain

`sweepOrphanParquetIndexArtifacts` bounds itself by the **union over the whole `prev` footer chain**. That is sound only because `prev` is written as the **parse anchor** — the committed head resolved from `_txn` — so the chain is the sequence of committed heads and a reader with an older mapping walks a *suffix* of it. **A `_pm` compaction that re-roots the chain reintroduces a Critical that took four review rounds to close.** `_pm` compaction is deferred by design and no 2C task may make it harder: do not add an in-directory `_pm` rewrite, do not truncate `_pm`, and do not weaken `resolvePrevFooter`'s fail-closed behaviour on a malformed link. The constraint is written onto `resolvePrevFooter` (`ed5b17b2d7`); leave it there.

### Facts a 2C implementer will otherwise get wrong

- **The pidx parquet has no `_pm`.** `ParquetPartitionDecoder.of(...)` requires one on every overload and calls `resolveFooter`. Use `ParquetFileDecoder`, which reads a bare parquet file from its own footer — the same decoder `read_parquet()` uses.
- **`ParquetFileDecoder.decodeRowGroup` takes parquet column *indices*, not field ids.** Its `columns` list holds `[parquet_column_index, column_type]` pairs. The `_im`'s descriptor index **is** the parquet column index, because `_im` descriptors are written one per schema column in schema order. The synthetic `-1` field ids therefore never reach a projection on the production read path.
- **Cover slots are not writer indices and not descriptor indices.** `requiredCoverColumns` are cover slots (`0 .. coverCount-1`). The `_im` maps them positionally: `descriptorIndex = FIRST_COVER_COLUMN + coverSlot`, and `IndexMetaFileReader.getCoverColumnIndex(int slot)` does exactly that. Confusing the three spaces silently resolves to the wrong column.
- **`KEY_SPACE_SIZE` is an exclusive upper bound on key ids, not a distinct-key count.** Occupancy is sparse. `getKeyCount()` must return it.
- **`getRowGroupRangeForKey` returns `Numbers.encodeLowHighInts(rgLo, rgHi)`, or `IndexMetaFileReader.KEY_ABSENT == -1L`** which decodes to `-1` for both bounds.
- **The `PostingIndexReader` sentinel is `Numbers.LONG_NULL`, not `-1`** (`3bbdcbe404`). `-1` from `countMatchesClamped` is consumed as a count and silently subtracts one from `count(*)`; `-1` from `selectKthMatch` is consumed as an absolute row id. `getEntryMaxValue` is the exception: there the "no entry" value is a **negative** number and callers branch on the sign.
- **`cairo.posting.index.parquet.payload` no longer exists** (`ecbbcfefdc`). Only payload arm N (`PAYLOAD_KIND = 0`, one row per posting) is written. A reader must still read `PAYLOAD_KIND` off the `_im` and reject `1` explicitly rather than mis-decoding it.
- **`PostingIndexOracleTestEf` / `PostingIndexOracleTestDelta` have been running under `ENCODING_ADAPTIVE`** because `CairoConfigurationWrapper` does not forward `getPostingIndexRowIdEncoding`. That is a separate pre-existing master bug which **must not be fixed on this branch** (fixing it may turn those suites red). Do not build a 2C assertion on those two subclasses' encoding.

---

## File Structure

**Create:**

| file | responsibility |
| --- | --- |
| `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java` | binds `_im` + pidx parquet, owns the key directory lookup, zone-map pruning, column projection and the shared `IndexReader` surface |
| `core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexFwdReader.java` | forward cursor over a key's row-group run |
| `core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexBwdReader.java` | backward cursor over the same run |
| `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java` | unit-level: binding, lookup, pruning, projection, lifecycle |
| `core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexOracleTest.java` | differential: native reader as an exact oracle, plus the negative controls |

**Modify:**

| file | change |
| --- | --- |
| `core/src/main/java/io/questdb/cairo/idx/IndexFactory.java` | `createReader` gains the on-disk index form and its `_im` token, and a parquet branch |
| `core/src/main/java/io/questdb/cairo/TableReader.java` | caches the resolved form per partition/column; `getIndexReader` dispatches on it; `checkPostingIndexIsReadable` deleted |
| `core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetPageFrameCursor.java` | synthetic field ids no longer collide with a real writer index |
| `core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetRecordCursor.java` | `canProjectMetadata` documents and enforces the corrected rule |
| `core/src/main/java/io/questdb/cairo/TableWriter.java` | O3 update-mode artifact retirement; `DROP INDEX` token reclamation |
| `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionUpdater.java` | carries prior-footer covering entries to the O3 worker |

---

## Task 1: Fix the synthetic field-id collision on the parallel page-frame path

The seal's `_im` writer **requires** field id `-1` on the synthetic `key_id` and `row_id` columns (`docs/index-metadata.md`, "Column descriptors"), and `ParquetIndexSeal` writes them. `canProjectMetadata` computes `effectiveId = columnId < 0 ? parquetIndex : columnId`, so `key_id` (parquet index 0, id `-1`) takes `effectiveId = 0` and collides with a real column whose writer index is 0. Phase 2 observed the consequence directly: `key_id` came back as the low 32 bits of each row's designated timestamp.

**Decision: fix it, do not route around it.** The production 2C reader routes around it structurally — `ParquetFileDecoder.decodeRowGroup` takes column *indices*, never field ids — so this defect cannot reach a query served by the new reader. But the spec's **"Fast path against slow path"** oracle requires reading a pidx parquet through the ordinary parquet reader and asserting that the `_im` directory and standard Parquet statistics select the same row groups, and the spec's **"Negative controls"** requirement perturbs a covered value and asserts the differential test fails. Both read a pidx parquet through `read_parquet()`. Routing around would mean pinning those tests to the serial path, i.e. making the oracle depend on `cairo.sql.parallel.read.parquet.enabled`, a config a user can flip — the oracle would then silently stop covering the shape the reader actually serves.

The fix is narrow. There are exactly two call sites:

- `ReadParquetRecordCursor.java:305-312` passes `columnMapping == null`, so the `columnId < 0` branch never executes there;
- `ReadParquetPageFrameCursor.java:175-186` passes a `columnMapping` and is the only site that mis-projects.

The serial/parallel choice is a single compile-time branch at `ReadParquetFunctionFactory.java:95-100` on `context.isParallelReadParquetEnabled()`.

**Files:**
- Modify: `core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetRecordCursor.java` (`canProjectMetadata`)
- Modify: `core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetPageFrameCursor.java:175-186`
- Test: `core/src/test/java/io/questdb/test/griffin/engine/functions/table/ReadParquetTest.java`

**Interfaces:**
- Produces: no signature change. `canProjectMetadata` keeps its current name and parameter list; only its treatment of a negative `columnId` changes. Task 13's oracle relies on `read_parquet()` over a pidx parquet returning correct `key_id` values on **both** the serial and parallel paths.

- [ ] **Step 1: Read the two call sites and confirm the asymmetry before changing anything**

```bash
cd ~/claude/wt/pidx-parquet
sed -n '295,320p' core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetRecordCursor.java
sed -n '165,195p' core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetPageFrameCursor.java
sed -n '90,105p' core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetFunctionFactory.java
grep -rn "canProjectMetadata" core/src/main/java
```

Expected: `grep` reports exactly two call sites plus the declaration. If it reports three or more, STOP and report it — the fix's blast radius is larger than this task assumes, and the extra site must be audited before proceeding.

- [ ] **Step 2: Write the failing test**

Add to `ReadParquetTest`. The fixture must produce a parquet file whose schema carries a **negative field id on a column that is not last**, and a **real column whose field id equals that column's parquet index**, so the two answers differ. The seal's own output is exactly that shape, so build it through the seal rather than by hand:

```java
    @Test
    public void testNegativeFieldIdsDoNotCollideWithARealColumnIndex() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "parquet");
            execute("create table x (sym symbol index type posting include (price), price double, ts timestamp) " +
                    "timestamp(ts) partition by day wal");
            execute("insert into x select rnd_symbol('a','b','c'), 100.0 + x, " +
                    "timestamp_sequence('2024-01-01T00:00:00.000000Z', 1000000) from long_sequence(50000)");
            drainWalQueue();
            execute("alter table x convert partition to parquet where ts >= '2024-01-01'");
            drainWalQueue();

            final String pidx = findSinglePidxParquet("x", "2024-01-01");
            // key_id is parquet column 0 and carries field id -1. The covered
            // designated timestamp has writer index 0. Under the collision the
            // projection served the timestamp's page for key_id, so every
            // key_id came back as the low 32 bits of a timestamp -- far above
            // the symbol cardinality.
            assertSql(
                    "max\tmin\n" +
                            "2\t0\n",
                    "select max(key_id), min(key_id) from read_parquet('" + pidx + "')"
            );
        });
    }
```

Write `findSinglePidxParquet(String table, String partition)` as a helper that lists the partition directory, asserts exactly one entry matching `*.pidx.*.parquet`, and returns its absolute path. Run the test body **twice in the same class** — once with `cairo.sql.parallel.read.parquet.enabled=true` and once `false` — as two `@Test` methods sharing a private body method, so the fix is pinned on both paths and the serial path proves the fixture is not itself broken.

- [ ] **Step 3: Run to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ReadParquetTest test; echo "MVN_EXIT=$?"
```

Expected: the parallel variant FAILS with a `max(key_id)` far above `2` (a truncated timestamp); the serial variant PASSES. If the parallel variant passes, the fixture is not reaching the parallel path — check `isParallelReadParquetEnabled()` actually took effect before changing production code.

- [ ] **Step 4: Fix the projection**

In `ReadParquetPageFrameCursor`, a negative `columnId` means "this parquet column is not a QuestDB table column". It must **not** fall back to the parquet index, because the parquet index lives in the same numeric space as a writer index. Map by position instead, and never by a synthesised id:

```java
                final int columnId = decoder.getColumnId(parquetIndex);
                // A negative field id marks a column that is not a QuestDB table
                // column -- the covering index's synthetic key_id / row_id carry
                // -1, and docs/index-metadata.md requires it. Substituting the
                // parquet index here put it in the same numeric space as a real
                // column's writer index, so key_id (parquet index 0, id -1)
                // collided with the writer index 0 column and the projection
                // served that column's page instead.
                if (columnId < 0) {
                    // Not projectable by id: fall back to positional mapping for
                    // this column rather than inventing an id for it.
                    columnMapping.set(parquetIndex, parquetIndex);
                    continue;
                }
                final int effectiveId = columnId;
```

Adapt the surrounding loop's variable names to the file's actual ones — read the method before editing. Update `canProjectMetadata`'s javadoc in `ReadParquetRecordCursor` to state the rule: **a negative field id is never mapped into the writer-index space**, and the serial path is unaffected only because it passes `columnMapping == null`.

- [ ] **Step 5: Run to verify it passes**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ReadParquetTest,ParquetIndexSealTest' test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, both variants pass, `ParquetIndexSealTest` unchanged at 29 tests.

- [ ] **Step 6: Negative control**

Revert only the production edit in `ReadParquetPageFrameCursor` (restore `effectiveId = columnId < 0 ? parquetIndex : columnId`), leave the test as written, re-run, and confirm the parallel variant fails. Restore, confirm green. **Report the verbatim assertion message including the actual `max(key_id)` value** — the value is the evidence that the timestamp's page was served, and a failure with a plausible small number would mean the control caught something else.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetPageFrameCursor.java \
        core/src/main/java/io/questdb/griffin/engine/functions/table/ReadParquetRecordCursor.java \
        core/src/test/java/io/questdb/test/griffin/engine/functions/table/ReadParquetTest.java
git commit -m "fix(parquet): stop negative field ids colliding with a writer index in the parallel projection"
```

---

## Task 2: Resolve and cache each partition's on-disk index form at partition-open time

`checkPostingIndexIsReadable` (`TableReader.java:872-989`) resolves the `_pm` footer and scans the covering-index section **on every `getIndexReader` call** for a POSTING-indexed column of a parquet partition. Wave 2 already made it read the reader's own `_pm` mapping rather than a fresh one, which removed the open+mmap+munmap; what remains is a footer resolve plus a linear section scan per call. 2C's dispatch needs the same three values (`form`, `indexTxn`, `imFileSize`) and needs them cheaply, so resolve them once when the partition is opened and cache them.

This task **keeps the refusal**. It changes only where the answer comes from. Task 3 deletes the refusal.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java`

**Interfaces:**
- Produces, on `TableReader`, all package-private:
  - `byte getPartitionIndexForm(int partitionIndex, int columnIndex)` returning `PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE` or `PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET`
  - `long getPartitionIndexTxn(int partitionIndex, int columnIndex)` returning the published `index_txn`, or `-1` when the form is native
  - `long getPartitionIndexImFileSize(int partitionIndex, int columnIndex)` returning the published `im_file_size`, or `0` when the form is native
  - `void invalidateIndexFormCache(int partitionIndex)` called wherever a partition's `_pm` mapping is dropped or replaced
- Task 3 consumes all four.

- [ ] **Step 1: Write the failing test**

Add to `ParquetIndexSealTest`. Assert the **syscall count**, not a duration — a stopwatch would pass on a fast machine regardless. Use the test `FilesFacade` counter pattern already in that file (a `TestFilesFacadeImpl` subclass counting `openRO`/`mmap`), and count **footer resolves** by wrapping `ParquetMetaFileReader.resolveFooter` calls through a counter the test can read. If no such counter exists, add a package-private `long getFooterResolveCount()` to `ParquetMetaFileReader` incremented in `resolveFooter`, mirroring `getChecksumVerifications()` which wave 5 added for exactly this purpose:

```java
    @Test
    public void testTheOnDiskIndexFormIsResolvedOncePerPartitionOpenNotOncePerCall() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                reader.openPartition(0);
                final long before = reader.getParquetMetaReaderForTest().getFooterResolveCount();
                for (int i = 0; i < 64; i++) {
                    try {
                        reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                        Assert.fail("the refusal must still fire in this task");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "sealed as parquet and has no reader yet");
                    }
                }
                final long after = reader.getParquetMetaReaderForTest().getFooterResolveCount();
                Assert.assertEquals("the on-disk index form must be resolved at partition-open time, " +
                        "not once per getIndexReader call", 0, after - before);
            }
        });
    }
```

Write `createIndexedParquetTable(String name, int rows)` if the file has no equivalent: create a `sym symbol index type posting include (price)` table partitioned by day, insert `rows` rows into one day, drain the WAL, `alter table ... convert partition to parquet`, drain again, with `cairo.posting.index.parquet.partition.format=parquet` set on `node1` before the DDL. Add `getParquetMetaReaderForTest()` as a package-private accessor on `TableReader` if none exists.

- [ ] **Step 2: Run to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetIndexSealTest#testTheOnDiskIndexFormIsResolvedOncePerPartitionOpenNotOncePerCall test; echo "MVN_EXIT=$?"
```

Expected: FAIL, `expected:<0> but was:<64>`.

- [ ] **Step 3: Add the cache**

Add three parallel `LongList`s on `TableReader`, sized and indexed exactly as the existing per-partition/per-column arrays are — follow whatever `columnBase`/`getPrimaryColumnIndex` scheme the file already uses for `indexes`, and do not invent a second scheme:

```java
    // Resolved once per partition open, from this reader's OWN _pm mapping.
    // A fresh resolve would return the writer's latest index_txn rather than
    // this snapshot's: a token-only publish restates the same data.parquet
    // size, so its footer shadows the prior one and resolveFooter -- which
    // walks back from the mapped tail and returns the newest match -- hands a
    // pinned reader an index_txn its snapshot is not entitled to, naming an
    // _im that may already have been purged. See docs/parquet-metadata.md,
    // "Token-only appends".
    private final LongList indexFormImFileSizes = new LongList();
    private final LongList indexFormIndexTxns = new LongList();
    private final ByteList indexFormKinds = new ByteList();
```

Populate them in the parquet branch of `openPartition0`, immediately after `openParquetMetadata` has taken this snapshot's mapping and resolved its footer, by walking `getCoveringIndexCount()` once and writing every indexed column's entry. Default every slot to `PARQUET_INDEX_FORMAT_NATIVE` / `-1` / `0`. Clear the three lists for a partition wherever the reader closes or replaces that partition's `_pm` mapping — find those points by grepping for the existing `parquetPartitions` / partition-close bookkeeping and mirror it exactly.

Rewrite `checkPostingIndexIsReadable`'s body to consult `getPartitionIndexForm(...)` instead of resolving the footer itself. **Keep its fail-closed behaviour verbatim:** an unreadable `_pm` or an unresolvable footer on a partition `_txn` says is parquet must still throw, and only an unopenable (row-less) partition may return without deciding. Move those throws into the resolve, which now happens at open time.

**Keep the javadoc.** It records why the decision is the published token and not the configured format — the format says what the *next* seal will write and the two disagree in both directions — and that reasoning must survive into Task 3's dispatch. Amend only its last paragraph, which currently says "Remove this when the parquet-form reader lands".

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetIndexSealTest,PostingSealPurgeTest,TableReaderTest,ParquetMetaFileReaderTest' test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`; `ParquetIndexSealTest` 30 (29 + the new one); the other three unchanged.

- [ ] **Step 5: Negative control**

Make the cache read from a **fresh** `_pm` map instead of the snapshot's — i.e. reintroduce the pre-`271f9bffb2` behaviour at the new resolve point — and run `ParquetIndexSealTest`. It MUST fail on the pinned-reader test that proves the snapshot's own `index_txn` is served (`testSecondSealSupersedesTheFirst` and the wave-2 `I5` reload-arm test both exercise this; if neither fails, the cache is being populated somewhere the pin does not reach, and that is a defect in this task, not a missing test). Restore and confirm green. **Report the verbatim message and the two `index_txn` values.**

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/TableReader.java \
        core/src/main/java/io/questdb/cairo/ParquetMetaFileReader.java \
        core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "perf(idx): resolve a partition's on-disk index form once at open time"
```

---

## Task 3: Give `IndexFactory` a parquet seam and `TableReader` the right path

Three structural gaps close together here, and none can close alone:

1. `IndexFactory.createReader`'s POSTING cases hard-construct `PostingIndexFwdReader`/`PostingIndexBwdReader` and its signature carries no `_pm`/`_im` handle at all.
2. `TableReader.getIndexReader` (`:400`) and `createIndexReaderAt` (`:1286`) both pass `pathGenNativePartition(partitionIndex, partitionTxn)`. That helper resolves to the partition **directory** (`setPathForNativePartition` → `setSinkForNativePartition`), and a parquet partition's `pidx` artifacts live in that same directory — so the path is in fact correct for both forms. **Verify this before relying on it**: `pathGenParquetPartition` appends `PARQUET_PARTITION_NAME` and `pathGenParquetPartitionMetadata` appends `PARQUET_METADATA_FILE_NAME`, both to the same directory prefix. The gap is therefore not the path but the *name* — nothing on the call path tells the factory to build `<col>.pidx.<indexTxn>.parquet` rather than `<col>.pk`.
3. This task deletes `checkPostingIndexIsReadable`. Until the parquet readers exist there is nothing to dispatch to, so this task lands **after** Task 4 in a strict reading — but the reader is untestable through production SQL without dispatch, and testing it only by direct construction is exactly the "components correct, system broken" shape this branch has hit repeatedly. **Land the dispatch here with a reader that answers only the empty case, and let Task 4 fill it in.** The refusal is replaced by a reader that returns an empty cursor *and a test that fails*, not by a silent empty result: Step 3 below makes the placeholder throw a distinct, greppable exception, so an unfinished 2C cannot ship as a silent wrong answer.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/IndexFactory.java`
- Modify: `core/src/main/java/io/questdb/cairo/TableReader.java:378-418,1270-1310`
- Create: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Create: `core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexFwdReader.java`
- Create: `core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexBwdReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes from Task 2: `TableReader.getPartitionIndexForm(int, int)`, `getPartitionIndexTxn(int, int)`, `getPartitionIndexImFileSize(int, int)`.
- Produces:

```java
    public static IndexReader createReader(
            byte indexType,
            int direction,
            CairoConfiguration configuration,
            Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long pinnedTableTxn,
            byte indexForm,
            long indexTxn,
            long imFileSize
    )
```

- Produces `AbstractParquetPostingIndexReader implements PostingIndexReader` with:

```java
    public void ofParquet(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long indexTxn,
            long imFileSize
    );
```

  and public no-arg-shaped accessors `long getIndexTxn()` and `long getImFileSize()`. Tasks 4–12 add behaviour to this class and its two subclasses; every one of them consumes `ofParquet` and the three-value token.

- [ ] **Step 1: Verify the two premises this task rests on**

```bash
cd ~/claude/wt/pidx-parquet
sed -n '1376,1400p' core/src/main/java/io/questdb/cairo/TableReader.java
sed -n '2660,2700p' core/src/main/java/io/questdb/cairo/TableUtils.java
```

Confirm that `formatNativePartitionDirName` yields the partition **directory** and that `setPathForParquetPartition` is that same directory plus `PARQUET_PARTITION_NAME`. If `pathGenNativePartition` does **not** yield the directory a parquet partition's `pidx` files live in, STOP: this task's premise is false and the dispatch needs a new path helper, which must be reported before any code is written.

Then confirm the five mmap-shaped methods really have only the two callers the spec's audit claims:

```bash
grep -rn "getKeyBaseAddress\|getValueBaseAddress\|getKeyMemorySize\|getValueMemorySize\|getValueBlockCapacity" core/src/main/java --include=*.java
```

Expected callers: `LatestByAllIndexedRecordCursor` (whose factory is gated on `IndexType.BITMAP` at both construction sites) and `TouchTableFunctionFactory` (whose `touchMemory` guards `baseAddress == 0`). **Read both and confirm.** The spec states this as an audit result; every Phase 2 wave that adopted a stated invariant unchecked introduced a defect. Report what you actually found, including any third caller.

- [ ] **Step 2: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java` with the Apache-2.0 header from a neighbour in `io.questdb.test.cairo.idx`:

```java
    @Test
    public void testAParquetSealedPartitionDispatchesToTheParquetReader() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                Assert.assertTrue(
                        "a parquet-sealed partition must dispatch to the parquet reader, got " +
                                indexReader.getClass().getName(),
                        indexReader instanceof ParquetPostingIndexFwdReader
                );
                Assert.assertTrue(indexReader instanceof PostingIndexReader);
                Assert.assertTrue(indexReader.isOpen());
                // The five mmap-shaped methods have no meaning for a
                // parquet-backed reader. They return 0: LatestByAllIndexed is
                // gated on IndexType.BITMAP so a POSTING reader never reaches
                // it, and TouchTableFunctionFactory.touchMemory guards
                // baseAddress == 0, so a 0 degrades touch_table() to a no-op.
                Assert.assertEquals(0, indexReader.getKeyBaseAddress());
                Assert.assertEquals(0, indexReader.getValueBaseAddress());
                Assert.assertEquals(0, indexReader.getKeyMemorySize());
                Assert.assertEquals(0, indexReader.getValueMemorySize());
                Assert.assertEquals(0, indexReader.getValueBlockCapacity());
            }
        });
    }

    @Test
    public void testANativeSealedParquetPartitionStillDispatchesToTheNativeReader() throws Exception {
        assertMemoryLeak(() -> {
            // Format left at the default 'native': the partition converts to
            // parquet but its index stays a native sidecar set. Dispatch must
            // follow the ON-DISK form, so this must NOT reach the parquet reader.
            createNativeSealedParquetTable("y", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "y")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                Assert.assertTrue(indexReader instanceof PostingIndexFwdReader);
            }
        });
    }

    @Test
    public void testTouchTableOverAParquetBackedCoveringIndexSucceeds() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            assertSql("touch_table\ntrue\n", "select touch_table('x') != null as touch_table");
        });
    }
```

Adapt the `touch_table()` assertion to that function's real result shape — read `TouchTableFunctionFactory` and its existing test before writing it; the requirement the spec states is that the call **succeeds and reports zero index pages**, so assert the page count if the function exposes one.

- [ ] **Step 3: Create the three reader classes, minimal**

`AbstractParquetPostingIndexReader` holds an `IndexMetaFileReader imReader`, a `ParquetFileDecoder decoder`, the mmapped `_im` and pidx-parquet addresses and sizes, and the standard `columnNameTxn`/`partitionTxn`/`columnTop`/`partitionTimestamp`/`pinnedTableTxn`/`frozen` fields the native readers keep. `ofParquet` builds `<col>.pidx.<indexTxn>._im` via `ParquetIndexSeal.indexMetaFileName(path, columnName, indexTxn)`, binds it with `IndexMetaFileReader.openAndMapRO(ff, path, imReader)`, then mmaps `<col>.pidx.<indexTxn>.parquet` via `ParquetIndexSeal.indexParquetFileName(...)` for exactly `imReader.getPidxFileSize()` bytes — **never** `ff.length()` — and calls `decoder.of(parquetAddr, parquetSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)`. Reject `imReader.getPayloadKind() != PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_POSTING` with an explicit `CairoException`; only arm N is written today and a silently mis-decoded arm B is a wrong-answer class.

Implement the whole `IndexReader` surface now, so the class compiles and dispatch is real:

```java
    @Override
    public long getKeyBaseAddress() {
        return 0;
    }

    @Override
    public int getKeyCount() {
        // KEY_SPACE_SIZE: the exclusive upper bound on key ids, equal to the
        // native reader's keyCountIncludingNulls. NOT a distinct-key count --
        // occupancy is sparse, and a distinct count would make every key above
        // the first report absent with no error anywhere.
        return imReader.getKeySpaceSize();
    }

    @Override
    public long getKeyMemorySize() {
        return 0;
    }

    @Override
    public long getValueBaseAddress() {
        return 0;
    }

    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return 0;
    }
```

`getColumnTop`, `getColumnTxn`, `getPartitionTxn`, `isOpen`, `setPinnedTableTxn`, `isFrozen`, `setFrozen` and `close` follow the native readers' shape. `reloadConditionally()` is a no-op while `frozen`; otherwise it rebinds when `indexTxn` has moved. `of(...)` — the nine-argument `IndexReader.of` — must **throw** a `CairoException` naming `ofParquet` as the correct entry point, because the nine-argument form carries no index txn and cannot name the artifacts.

The four `PostingIndexReader` methods and `getCursor` are the placeholders this task ships:

```java
    @Override
    public long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped) {
        // The sentinel is Numbers.LONG_NULL, NOT -1: the sole caller tests
        // c != Numbers.LONG_NULL and then does total += c, so -1 silently
        // subtracts one from a count(*) answer instead of signalling fallback.
        return Numbers.LONG_NULL;
    }

    @Override
    public long getEntryMaxValue() {
        // Negative means "no entry". Unlike the two above, this sentinel IS
        // negative by contract, and AbstractPostingIndexReader spells it -1.
        return -1;
    }

    @Override
    public void populateCacheForKey(int key) {
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        throw CairoException.critical(0)
                .put("parquet-form posting index cursor is not implemented yet [column=")
                .put(columnName).put(", indexTxn=").put(indexTxn).put(']');
    }

    @Override
    public long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k) {
        return Numbers.LONG_NULL;
    }
```

The `getCursor` throw is deliberate and temporary: an empty cursor here would turn every indexed query over a parquet-sealed partition into a silent empty result, which is the exact failure `checkPostingIndexIsReadable` exists to prevent. Task 4 replaces it. **Do not merge this task without Task 4 also landing.**

`ParquetPostingIndexFwdReader` and `ParquetPostingIndexBwdReader` extend it and differ only in `DIR_FORWARD`/`DIR_BACKWARD` and, from Task 4/6 on, cursor direction.

- [ ] **Step 4: Widen `IndexFactory.createReader` and add the parquet branch**

```java
            case IndexType.POSTING, IndexType.POSTING_DELTA, IndexType.POSTING_EF -> {
                if (indexForm == PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET) {
                    final AbstractParquetPostingIndexReader reader = direction == IndexReader.DIR_FORWARD
                            ? new ParquetPostingIndexFwdReader()
                            : new ParquetPostingIndexBwdReader();
                    reader.ofParquet(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop,
                            metadata, columnVersionReader, partitionTimestamp, indexTxn, imFileSize);
                    reader.setPinnedTableTxn(pinnedTableTxn);
                    yield reader;
                }
                yield direction == IndexReader.DIR_FORWARD
                        ? new PostingIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp, pinnedTableTxn)
                        : new PostingIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp, pinnedTableTxn);
            }
```

`indexForm` is keyed off the **published token**, never the configured format. `IndexType.BITMAP` ignores the three new parameters: a bitmap index never becomes parquet.

- [ ] **Step 5: Dispatch from `TableReader` and delete the refusal**

In `createIndexReaderAt`, pass `getPartitionIndexForm(partitionIndex, columnIndex)`, `getPartitionIndexTxn(...)` and `getPartitionIndexImFileSize(...)` through to `IndexFactory.createReader`.

In `getIndexReader`, delete the `checkPostingIndexIsReadable(partitionIndex, columnIndex)` call at `:379` and the method itself. The cached-reader rebind branch at `:398-408` calls the nine-argument `of(...)`, which the parquet reader rejects: change that branch to test the reader's concrete type and call `ofParquet(...)` with the cached token when it is a parquet reader. **Also invalidate the cached reader when `getPartitionIndexTxn` has moved**, not only on `columnNameTxn`/`partitionTxn` — a token-only publish moves neither of those, which is precisely why the publish had to bump the partition table version in the first place.

**Preserve `checkPostingIndexIsReadable`'s javadoc reasoning by moving it onto the dispatch**, specifically: that the decision is the published token and not the configured format, because the format says what the next seal will write and the two disagree in both directions; and that the token is read through this reader's own `_pm` mapping because a fresh open returns the writer's latest `index_txn`. Delete only the last paragraph's "Remove this when the parquet-form reader lands".

- [ ] **Step 6: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,ParquetIndexSealTest,PostingSealPurgeTest,PostingIndex*Test,Covering*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: the three new dispatch tests pass; the whole posting/covering suite stays green because the config still defaults to `native`. Tests in `ParquetIndexSealTest` that asserted the refusal message must be **rewritten to assert dispatch**, not deleted — deleting them loses the coverage that a parquet-sealed partition is not served by a native reader.

- [ ] **Step 7: Negative control**

Force `indexForm` to `PARQUET_INDEX_FORMAT_NATIVE` unconditionally inside `createIndexReaderAt` — a one-line production change simulating the pre-2C mis-dispatch — and run `ParquetPostingIndexReaderTest`. `testAParquetSealedPartitionDispatchesToTheParquetReader` MUST fail naming `PostingIndexFwdReader`. Then force it to `PARQUET_INDEX_FORMAT_PARQUET` unconditionally and confirm `testANativeSealedParquetPartitionStillDispatchesToTheNativeReader` fails. **Both directions are required** — the format-keyed check this replaces was wrong in both, and a control that only proves one direction cannot distinguish dispatch from a constant. Restore, confirm green, report both messages.

- [ ] **Step 8: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/IndexFactory.java \
        core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexFwdReader.java \
        core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexBwdReader.java \
        core/src/main/java/io/questdb/cairo/TableReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java \
        core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "feat(idx): dispatch on a partition's on-disk index form"
```

---

## Task 4: Serve a single key's postings from the `_im` directory (pruning level 1)

Pruning level 1 is the `_im` key directory: `key -> [rg_lo .. rg_hi]`, exact, with no read of the pidx parquet at all. `IndexMetaFileReader.getRowGroupRangeForKey(int)` already implements the lookup and its worked examples are in `docs/index-metadata.md`, "Key lookup". This task turns that range into a `RowCursor` over `row_id`.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Modify: `core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexFwdReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes: `IndexMetaFileReader.getRowGroupRangeForKey(int key)` returning `Numbers.encodeLowHighInts(rgLo, rgHi)` or `IndexMetaFileReader.KEY_ABSENT == -1L`; `IndexMetaFileReader.getKeyIdColumn()`, `getRowIdColumn()`, `getRowGroupNumRows(int rowGroup)`; `ParquetFileDecoder.decodeRowGroup(RowGroupBuffers rowGroupBuffers, DirectIntList columns, int rowGroupIndex, int rowLo, int rowHi)` where `columns` holds `[parquet_column_index, column_type]` pairs and `rowHi` is **exclusive**; `RowGroupBuffers.getChunkDataPtr(int columnIndex)` / `getChunkDataSize(int columnIndex)`.
- Produces: a working `getCursor(int key, long minValue, long maxValue)` on `ParquetPostingIndexFwdReader`, returning ascending absolute partition-local row ids within `[minValue, maxValue]` inclusive, and an empty cursor for an absent key. Tasks 6–12 build on it.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testTheForwardCursorReturnsAKeysPostingsInAscendingOrder() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final SymbolMapReader symbols = reader.getSymbolMapReader(columnIndex);
                final int key = symbols.keyOf("a") + 1; // +1: key 0 is NULL
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                final RowCursor cursor = indexReader.getCursor(key, 0, Long.MAX_VALUE);
                long previous = -1;
                long count = 0;
                while (cursor.hasNext()) {
                    final long rowId = cursor.next();
                    Assert.assertTrue("row ids must ascend, got " + rowId + " after " + previous, rowId > previous);
                    previous = rowId;
                    count++;
                }
                Assert.assertTrue("the key must have postings", count > 0);
                assertSql("count\n" + count + "\n", "select count() from x where sym = 'a'");
            }
        });
    }

    @Test
    public void testAnAbsentKeyReturnsAnEmptyCursorRatherThanThrowing() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                // Above KEY_SPACE_SIZE: absent by the directory's own bound.
                final RowCursor cursor = indexReader.getCursor(indexReader.getKeyCount() + 7, 0, Long.MAX_VALUE);
                Assert.assertFalse(cursor.hasNext());
            }
        });
    }
```

- [ ] **Step 2: Run to verify they fail**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest test; echo "MVN_EXIT=$?"
```

Expected: FAIL with "parquet-form posting index cursor is not implemented yet".

- [ ] **Step 3: Implement the cursor**

On `AbstractParquetPostingIndexReader`, add a reusable `RowGroupBuffers` and a `DirectIntList projection`, both allocated once and closed in `close()`. Add:

```java
    /**
     * Resolves {@code key} to its inclusive index row-group run through the
     * {@code _im} directory. Pruning level 1: exact, and it reads no byte of
     * the index parquet.
     * <p>
     * The directory answers "which row groups COULD hold k", not "does k
     * exist": the key space is dense and occupancy sparse, so a key falling
     * inside a packed group's key range returns a range whether or not it has
     * postings. Confirming absence costs one row-group decode, which the
     * cursor performs anyway.
     */
    protected long rowGroupRangeForKey(int key) {
        return imReader.getRowGroupRangeForKey(key);
    }
```

The forward cursor holds `rgLo`, `rgHi`, the current row group, a decoded `key_id`/`row_id` buffer pair and an offset into it. `hasNext()` advances within the decoded group, then to the next group in `[rgLo, rgHi]`, decoding it on demand. Each decode projects **exactly two** columns — `imReader.getKeyIdColumn()` and `imReader.getRowIdColumn()` — as `[index, ColumnType.INT]` and `[index, ColumnType.LONG]` pairs, with `rowLo = 0` and `rowHi = (int) imReader.getRowGroupNumRows(rg)`.

Within a decoded group, skip rows whose `key_id != key` (a packed group holds other keys) and rows whose `row_id` falls outside `[minValue, maxValue]`. Do **not** yet skip whole row groups by the zone maps — that is Task 6, and adding it here would make Task 6's negative control vacuous.

Free the decode buffers on every path including exceptional ones.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,ParquetIndexSealTest' test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, all pass.

- [ ] **Step 5: Negative control**

Change `rowGroupRangeForKey` to return `Numbers.encodeLowHighInts(rgLo, rgLo)` — dropping the high bound, so a key spanning consecutive dedicated row groups loses everything past the first. The fixture must contain such a key or this control is vacuous: **first assert the fixture has one** by reading `RG_FIRST_KEY` back and finding a repeated entry, and if it does not, raise the row count or lower the symbol cardinality until it does. Confirm `testTheForwardCursorReturnsAKeysPostingsInAscendingOrder` fails on the `count()` comparison against SQL. Restore, confirm green, report the two counts.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexFwdReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): serve a key's postings from the _im directory"
```

---

## Task 5: Prune row groups by the row-id zone maps (pruning level 2)

`minValue`/`maxValue` is already the row-id range an interval scan derives from a timestamp predicate, so time-range pruning has a hook with no planner change. `RG_ROW_ID_MIN` / `RG_ROW_ID_MAX` are written **unconditionally** — under arm B there is no `row_id` column at all, so a reader taking the range from chunk stats would have no time pruning for that payload — and row id is monotone in the designated timestamp within a partition, so the pruning is exact, not conservative.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes: `IndexMetaFileReader.getRowGroupRowIdMin(int i)` and `getRowGroupRowIdMax(int i)`. **Open question for the implementer:** confirm these accessor names against `core/src/main/java/io/questdb/cairo/IndexMetaFileReader.java` before writing — the class exposes the `RG_ROW_ID_MIN`/`RG_ROW_ID_MAX` sections but this plan did not verify the exact spelling. If they differ, use the real names and say so in the commit message.
- Produces: `protected boolean isRowGroupPruned(int rowGroup, long minValue, long maxValue)` and a decode-counter `long getDecodedRowGroupCount()` used by this task's test and by Task 10's.

- [ ] **Step 1: Write the failing test**

Assert the **row groups decoded**, not a duration. A latency assertion passes on warm-up while the pruning misses.

```java
    @Test
    public void testANarrowRowIdRangeDecodesFewerRowGroupsThanTheKeysWholeRun() throws Exception {
        assertMemoryLeak(() -> {
            // A hot key spanning several dedicated row groups, so its run is
            // long enough for a narrow range to exclude most of it. Without
            // that the two arms cannot differ and the test proves nothing.
            createHotKeyParquetTable("x", 400_000, "hot");
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("hot") + 1;
                final AbstractParquetPostingIndexReader indexReader =
                        (AbstractParquetPostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);

                drain(indexReader.getCursor(key, 0, Long.MAX_VALUE));
                final long whole = indexReader.getDecodedRowGroupCount();
                Assert.assertTrue("the fixture must give the hot key more than one row group, got " + whole,
                        whole > 1);

                final long before = indexReader.getDecodedRowGroupCount();
                drain(indexReader.getCursor(key, 0, 999));
                final long narrow = indexReader.getDecodedRowGroupCount() - before;
                Assert.assertTrue("a narrow row-id range must decode fewer row groups: narrow=" + narrow +
                        " whole=" + whole, narrow < whole);
            }
        });
    }
```

Write `drain(RowCursor)` as a private helper that walks the cursor to exhaustion, and `createHotKeyParquetTable(String name, int rows, String hotSymbol)` as a fixture inserting the named symbol for the large majority of rows so it occupies consecutive dedicated row groups.

- [ ] **Step 2: Run to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest#testANarrowRowIdRangeDecodesFewerRowGroupsThanTheKeysWholeRun test; echo "MVN_EXIT=$?"
```

Expected: FAIL — `narrow` equals `whole`, because Task 4 decodes every group in the run.

- [ ] **Step 3: Implement the skip**

```java
    /**
     * Pruning level 2: skip a row group whose row-id extent does not intersect
     * the caller's [minValue, maxValue]. Row id is monotone in the designated
     * timestamp within a partition, so an interval scan's range maps onto this
     * exactly rather than conservatively.
     * <p>
     * RG_ROW_ID_MIN / RG_ROW_ID_MAX are read from the _im's own sections and
     * not from the row_id chunk's stats, because the sections are written
     * unconditionally while the chunk does not exist under PAYLOAD_KIND = 1.
     */
    protected boolean isRowGroupPruned(int rowGroup, long minValue, long maxValue) {
        return imReader.getRowGroupRowIdMax(rowGroup) < minValue
                || imReader.getRowGroupRowIdMin(rowGroup) > maxValue;
    }
```

Consult it in the cursor's group-advance loop, before the decode. Increment the decode counter only where a decode actually happens.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,ParquetIndexSealTest' test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, all pass.

- [ ] **Step 5: Negative control**

Invert one bound — `getRowGroupRowIdMax(rowGroup) < maxValue` — so the skip drops groups it must keep, and confirm `testTheForwardCursorReturnsAKeysPostingsInAscendingOrder` fails on the `count()` comparison against SQL. A pruning bug that only *reduces* the decode count would pass the counter test while losing rows, so the control must be run against the **correctness** test, not the counter one. Restore, confirm green, report the two counts.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): prune index row groups by the _im row-id zone maps"
```

---

## Task 6: The backward reader

`ParquetPostingIndexBwdReader` walks the same `[rg_lo, rg_hi]` run in reverse and emits descending row ids. Row groups within a run are ordered by `row_id` because the file is key-major and `row_id` ascends within a key, so reversing is walking groups from `rg_hi` down and each group's decoded rows from the end.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexBwdReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes: `AbstractParquetPostingIndexReader.rowGroupRangeForKey(int)` and `isRowGroupPruned(int, long, long)` from Tasks 4 and 5.
- Produces: `ParquetPostingIndexBwdReader.getCursor(int key, long minValue, long maxValue)` emitting strictly descending row ids within `[minValue, maxValue]`.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testTheBackwardCursorMirrorsTheForwardOne() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("a") + 1;
                final LongList forward = new LongList();
                final RowCursor fwd = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD)
                        .getCursor(key, 0, Long.MAX_VALUE);
                while (fwd.hasNext()) {
                    forward.add(fwd.next());
                }
                Assert.assertTrue(forward.size() > 0);
                final RowCursor bwd = reader.getIndexReader(0, columnIndex, IndexReader.DIR_BACKWARD)
                        .getCursor(key, 0, Long.MAX_VALUE);
                int i = forward.size();
                while (bwd.hasNext()) {
                    Assert.assertTrue("backward cursor emitted more rows than forward", i > 0);
                    Assert.assertEquals(forward.getQuick(--i), bwd.next());
                }
                Assert.assertEquals("backward cursor emitted fewer rows than forward", 0, i);
            }
        });
    }
```

- [ ] **Step 2: Run to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest#testTheBackwardCursorMirrorsTheForwardOne test; echo "MVN_EXIT=$?"
```

Expected: FAIL with "parquet-form posting index cursor is not implemented yet".

- [ ] **Step 3: Implement the backward cursor**

Mirror the forward cursor: start at `rgHi`, decrement to `rgLo`, and within each decoded group iterate from `numRows - 1` down to `0`, applying the same `key_id == key` and `[minValue, maxValue]` filters and the same `isRowGroupPruned` skip.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,ParquetIndexSealTest,Covering*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, all pass.

- [ ] **Step 5: Negative control**

Walk the row groups from `rgHi` down but each group's rows **forwards**. This yields the right set with the wrong order, so a set-comparison assertion would pass — confirm the test fails on the element-by-element comparison, naming a specific mismatched pair. Restore, confirm green, report the message. If the test passes under this mutation, the assertion is comparing sets and must be strengthened before proceeding.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexBwdReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): add the backward parquet posting index reader"
```

---

## Task 7: Project covered columns into a `CoveringRowCursor`

`getCursor(key, minValue, maxValue, int[] requiredCoverColumns)` already exists on `IndexReader` (`:91`) as a default delegating to the three-argument form. `requiredCoverColumns` maps directly onto a parquet column projection, which is strictly better than `.pc`, where each covered column is a whole separate file read. `CoveredColumnDecoder` remains the single source of truth for the covered byte layout — but it decodes the *native* `.pc` layout, so the parquet reader does not feed it: it serves `CoveringRowCursor`'s accessors directly from the decoded parquet chunks, leaving `PageFrameMemoryPool` and the eager multi-key frame path untouched.

**The three index spaces must not be confused.** `requiredCoverColumns` are **cover slots**. `IndexMetaFileReader.getCoverColumnIndex(int slot)` maps a slot to a **descriptor index**, which is also the **parquet column index**. A descriptor's `ID` is the covered column's **writer index** and is *not* a lookup key here.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Modify: `core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexFwdReader.java`, `ParquetPostingIndexBwdReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes: `IndexMetaFileReader.getCoverColumnIndex(int slot)`, `getFirstCoverColumn()`, `getColumnType(int column)`, `getChunkNullCount(int rowGroup, int column)`, `getChunkNumValues(int rowGroup, int column)`; `CoveringRowCursor` (`core/src/main/java/io/questdb/cairo/idx/CoveringRowCursor.java`) declaring `getCoveredArray(int, int)`, `getCoveredBin`, `getCoveredBinLen`, `getCoveredByte`, `getCoveredDouble`, `getCoveredFloat`, `getCoveredInt`, `getCoveredLong`, `getCoveredLong128Hi`, `getCoveredLong128Lo`, `getCoveredLong256_0..3`, `getCoveredShort`, `getCoveredStrA`, `getCoveredStrB`, `getCoveredVarcharA`, `getCoveredVarcharB`, `isCoveredAvailable(int)` and `seekToLast()`, all keyed by `includeIdx` — **the cover slot**.
- Produces: `getCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns)` on both readers, returning a `CoveringRowCursor`.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testCoveredValuesMatchTheTableForEveryPosting() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            // price is cover slot 0. The values are deliberately a function of
            // the row id, so an off-by-one gather fails on EVERY row rather
            // than coincidentally matching.
            assertSql(
                    "count\n0\n",
                    "select count() from x where sym = 'a' and price != 100.0 + rowid_of_x"
            );
        });
    }

    @Test
    public void testAProjectionDecodesOnlyTheRequestedCoverSlots() throws Exception {
        assertMemoryLeak(() -> {
            createTwoCoverColumnParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("a") + 1;
                final AbstractParquetPostingIndexReader indexReader =
                        (AbstractParquetPostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                final CoveringRowCursor cursor = (CoveringRowCursor)
                        indexReader.getCursor(key, 0, Long.MAX_VALUE, new int[]{0});
                Assert.assertTrue(cursor.hasNext());
                cursor.next();
                Assert.assertTrue(cursor.isCoveredAvailable(0));
                Assert.assertFalse("slot 1 was not requested and must not be decoded",
                        cursor.isCoveredAvailable(1));
            }
        });
    }
```

Replace `rowid_of_x` with whatever the fixture actually makes `price` a function of — the requirement is that a one-row gather offset makes the comparison fail on every row, which the Phase 2 payload assertion already proved non-vacuous by exactly this mutation. Write `createTwoCoverColumnParquetTable` with `include (price, qty)` so slots 0 and 1 both exist.

- [ ] **Step 2: Run to verify they fail**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest test; echo "MVN_EXIT=$?"
```

Expected: FAIL — `getCursor(..., int[])` still delegates to the three-argument form, so the returned cursor is not a `CoveringRowCursor` and the cast throws `ClassCastException`.

- [ ] **Step 3: Implement the projection**

Extend the shared cursor to build its `DirectIntList projection` as `key_id`, `row_id`, then one pair per requested cover slot, each as `[imReader.getCoverColumnIndex(slot), imReader.getColumnType(descriptorIndex)]`. Bounds-check `getCoverColumnIndex` against `getColumnCount()` and reject an out-of-range slot with a `CairoException` naming the slot and the cover count — the `_im` reader deliberately does **not** validate `FIRST_COVER_COLUMN` at open time (`docs/index-metadata.md`, "What the reader does not re-check"), so the check belongs here at the point of use.

Serve each `getCoveredX(includeIdx)` from the decoded chunk for the slot's projection position at the cursor's current in-group row. `isCoveredAvailable(includeIdx)` returns false for a slot not in the projection, and false for an all-null chunk — `getChunkNullCount(rg, col) == getChunkNumValues(rg, col)` means the reader materialises nulls without fetching or decoding anything.

`seekToLast()` positions at the last matching posting of the current key.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,Covering*Test,PostingIndex*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, all pass.

- [ ] **Step 5: Negative control**

Map cover slots by **writer index** instead of by `getCoverColumnIndex(slot)` — the exact v2 defect the `_im` format's v3 revision exists to prevent, and the one that "silently resolves to the wrong column or to a miss". With two cover columns of different types the projection then serves the wrong column's values. Confirm `testCoveredValuesMatchTheTableForEveryPosting` fails, and report the count of mismatching rows: if it is small rather than every row, the fixture's covered values are not distinct enough and must be strengthened before this test can be trusted. Restore, confirm green.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexFwdReader.java \
        core/src/main/java/io/questdb/cairo/idx/ParquetPostingIndexBwdReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): project covered columns out of a parquet-form index"
```

---

## Task 8: `setFrozen` and `getDetachedCursor` for the parallel covered-decode path

`PageFrameAddressCache.freezeCoveredReaders()` / `unfreezeCoveredReaders()` reach every covered posting reader **through `IndexReader.setFrozen`**, which is on the interface — so the parallel path does not need a concrete type for the freeze itself. The concrete-type dependency the audit worried about is `warmForKeys(int[] keys, int[] requiredCoverColumns)`, which lives on `AbstractPostingIndexReader:1727` only. **Verify before relying on this:** `grep -rn "\.warmForKeys(" core/src/main/java` returned nothing when this plan was written — its only callers are in `core/src/test/java/io/questdb/test/cairo/covering/PostingReaderConcurrentReadTest.java`, where the receiver is a concrete native reader. If that grep now returns a production call site, STOP and report it: `warmForKeys` must then go onto `PostingIndexReader`, which is a deliberate widening of a contract Phase 2 Task 1 sized at exactly four methods, and it would break `PostingIndexReaderContractTest#testContractDeclaresOnlyTheSeamMethods` by design.

What the parquet reader **does** owe the parallel path is real: while frozen, `reloadConditionally()` must be a no-op and the `_im` and parquet mappings must stay stable while async workers iterate detached cursors over them concurrently; and `getDetachedCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns)` defaults to throwing `UnsupportedOperationException`.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Produces: `getDetachedCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns)` returning a `CoveringRowCursor` that owns its own decode buffers and shares only the immutable `_im` and parquet mappings, so N workers may hold N cursors over one reader.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testAParallelCoveredScanAgreesWithTheSerialOne() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 200_000);
            node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "false");
            final StringSink serial = new StringSink();
            printSql("select sym, sum(price) from x where sym in ('a','b','c') order by sym", serial);
            node1.setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "true");
            final StringSink parallel = new StringSink();
            printSql("select sym, sum(price) from x where sym in ('a','b','c') order by sym", parallel);
            TestUtils.assertEquals(serial, parallel);
            Assert.assertTrue("the fixture must return rows", serial.length() > 20);
        });
    }

    @Test
    public void testAFrozenReaderDoesNotRebindItsMappings() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final AbstractParquetPostingIndexReader indexReader =
                        (AbstractParquetPostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                final long imAddr = indexReader.getImAddressForTest();
                indexReader.setFrozen(true);
                indexReader.reloadConditionally();
                Assert.assertEquals("a frozen reader must not rebind its _im mapping", imAddr,
                        indexReader.getImAddressForTest());
                indexReader.setFrozen(false);
            }
        });
    }
```

Adapt the parallel-filter property name to the real `PropertyKey` and the exact SQL shape to whichever plan actually reaches the covered frame path — read `GroupByRecordCursorFactory:602-604` and `PageFrameSequence:568-570` and pick a query that provably takes it, then **assert it took it** rather than assuming, by checking the plan text with `explain`.

- [ ] **Step 2: Run to verify they fail**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest test; echo "MVN_EXIT=$?"
```

Expected: the parallel test fails with `UnsupportedOperationException` from the default `getDetachedCursor`.

- [ ] **Step 3: Implement freeze and detached cursors**

Add a `frozen` flag; `reloadConditionally()` returns immediately while set. Add `getImAddressForTest()` package-private. Implement `getDetachedCursor` to allocate a fresh `RowGroupBuffers` and `DirectIntList` per cursor, register them for release, and share only the `_im` mapping and the decoder handle. Every detached cursor must free its own buffers on close **and** on the error path.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,Covering*Test,PostingReaderConcurrentReadTest' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`; `assertMemoryLeak` must be clean, which is the real gate on per-cursor buffer ownership.

- [ ] **Step 5: Negative control**

Make `getDetachedCursor` return the reader's **shared** cursor instead of a fresh one, and run `testAParallelCoveredScanAgreesWithTheSerialOne` repeatedly (`-Dtest=...#testAParallelCoveredScanAgreesWithTheSerialOne -DrerunFailingTestsCount=0`, 20 iterations via a shell loop). It must fail. **If it passes 20 times, report that** rather than declaring the control genuine: it means the query is not actually dispatching multiple workers over one reader, and the test does not cover what it claims. Restore, confirm green.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): support frozen readers and detached cursors on the parquet form"
```

---

## Task 9: The four `PostingIndexReader` metadata primitives

`CoveringIndexRecordCursorFactory` reaches these through the narrowed interface: the cast at `:1354` (widened to `instanceof PostingIndexReader` in Phase 2 Task 1), the `fillFrameForKeyCheap` parameter at `:1399`, and the `instanceof` at `:2640`. Failing the `:2640` guard is **silent**: `count(*) WHERE sym = 'x'` quietly loses its metadata-only answer and falls back to an O(rows) traverse.

The sentinels are contractual and were corrected in `3bbdcbe404`:

- `countMatchesClamped` and `selectKthMatch` return **`Numbers.LONG_NULL`** when the reader cannot answer from metadata alone. `-1` is consumed as a count (silently subtracting one from `count(*)`) or as an absolute row id.
- `getEntryMaxValue` returns a **negative** value when there is no entry, and callers branch on the sign; `AbstractPostingIndexReader` spells it `-1`.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes: `IndexMetaFileReader.getRowGroupRangeForKey`, `getRowGroupNumRows`, `getRowGroupRowIdMin`/`Max`, `getChunkNumValues(int rowGroup, int column)`.
- Produces: real implementations of `countMatchesClamped(int, long, long, long)`, `getEntryMaxValue()`, `populateCacheForKey(int)`, `selectKthMatch(int, long, long, long, long)`.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testCountMatchesClampedAgreesWithTheCursorAndWithSql() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("a") + 1;
                final PostingIndexReader indexReader =
                        (PostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                final long counted = indexReader.countMatchesClamped(key, 0, Long.MAX_VALUE, Long.MAX_VALUE);
                Assert.assertNotEquals("the metadata fast path must answer, not fall back",
                        Numbers.LONG_NULL, counted);
                Assert.assertNotEquals("-1 is NOT the fallback sentinel; it is consumed as a count", -1, counted);
                long walked = 0;
                final RowCursor cursor = indexReader.getCursor(key, 0, Long.MAX_VALUE);
                while (cursor.hasNext()) {
                    cursor.next();
                    walked++;
                }
                Assert.assertEquals(walked, counted);
                assertSql("count\n" + counted + "\n", "select count() from x where sym = 'a'");
            }
        });
    }

    @Test
    public void testSelectKthMatchAgreesWithTheCursorAtEveryK() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("a") + 1;
                final PostingIndexReader indexReader =
                        (PostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                final LongList expected = new LongList();
                final RowCursor cursor = indexReader.getCursor(key, 0, Long.MAX_VALUE);
                while (cursor.hasNext()) {
                    expected.add(cursor.next());
                }
                Assert.assertTrue(expected.size() > 3);
                for (int k = 0, n = expected.size(); k < n; k++) {
                    Assert.assertEquals("k=" + k, expected.getQuick(k),
                            indexReader.selectKthMatch(key, 0, Long.MAX_VALUE, Long.MAX_VALUE, k));
                }
            }
        });
    }
```

- [ ] **Step 2: Run to verify they fail**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest test; echo "MVN_EXIT=$?"
```

Expected: FAIL — both still return `Numbers.LONG_NULL` from Task 3's placeholders.

- [ ] **Step 3: Implement the four primitives**

`countMatchesClamped`: resolve the key's run; for each unpruned row group, if the group is **dedicated** to the key (`RG_FIRST_KEY[i] == key` and the group holds no other key, which the writer's key-alignment check guarantees for a repeated `RG_FIRST_KEY` entry) and its whole row-id extent lies inside `[minValue, maxValueClamped]`, add `getRowGroupNumRows(i)` without decoding. Otherwise decode `key_id`/`row_id` for that group and count. Return the total — never `-1`, and `Numbers.LONG_NULL` only if the reader genuinely cannot answer.

`selectKthMatch`: the same walk, accumulating counts until the group containing the `k`-th match is found, then decoding only that group. Return `Numbers.LONG_NULL` when `k` is out of range.

`getEntryMaxValue`: the largest `RG_ROW_ID_MAX` over the whole index, or `-1` when `getIndexRowGroupCount() == 0`.

`populateCacheForKey`: decode and retain the key's first row group, so a subsequent `countMatchesClamped`/`getCursor` for that key does not re-decode.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,Covering*Test,PostingIndex*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, all pass.

- [ ] **Step 5: Negative control**

Make `countMatchesClamped` return `-1` instead of a count — **the exact defect `3bbdcbe404` documents**. It must fail: the covering factory's `count(*)` answer becomes one short. Run `Covering*Test` as well as the new test and report which of them caught it; if only the new test does, add a `count(*)` assertion through SQL over a parquet-sealed partition, because the production consumer is the one that matters. Restore, confirm green.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): answer the posting metadata primitives from the _im"
```

---

## Task 10: Narrow the decode to a key's row range inside a packed row group (pruning level 3)

**This task delivers pruning level 3's effect, not its stated mechanism, and the difference must be recorded rather than glossed.** The spec's level 3 is "Parquet `ColumnIndex`/`OffsetIndex`, `row_id` sorted ASC within a key, skips pages". Neither `ParquetFileDecoder` nor `ParquetPartitionDecoder` exposes a page-index API in this tree; what they expose is `decodeRowGroup(..., int rowLo, int rowHi)`, and the Rust decoder skips pages that fall outside that range. Bounding the decode to the key's row range therefore achieves page skipping through the available API. **Open question for the implementer:** confirm whether `ParquetIndexSeal` actually writes `ColumnIndex`/`OffsetIndex` — the spec's layout section says the footer carries them, but no Phase 2 commit message mentions writing them. If it does not, record that too: level 3's stated mechanism is then not merely unreachable through the Java API but absent from the file, and Phase 3 must add both.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes: `ParquetFileDecoder.decodeRowGroup(RowGroupBuffers, DirectIntList, int rowGroupIndex, int rowLo, int rowHi)`, `rowHi` **exclusive**.
- Produces: `protected long getDecodedRowCount()` on the reader, counting rows actually decoded, used by this task's assertion.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testAPackedRowGroupDecodesOnlyTheKeysRowsNotTheWholeGroup() throws Exception {
        assertMemoryLeak(() -> {
            // High cardinality so keys pack many-to-a-row-group: the packed
            // case is the only one where a narrowed range can differ from the
            // whole group, and a hot-key fixture would make this vacuous.
            createHighCardinalityParquetTable("x", 300_000, 20_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final AbstractParquetPostingIndexReader indexReader =
                        (AbstractParquetPostingIndexReader) reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                final int key = reader.getSymbolMapReader(columnIndex).keyOf("s0001") + 1;
                final long rangePacked = indexReader.getRowGroupRangeForKeyForTest(key);
                Assert.assertNotEquals(IndexMetaFileReader.KEY_ABSENT, rangePacked);
                final int rg = Numbers.decodeLowInt(rangePacked);
                Assert.assertEquals("the fixture must pack this key with others",
                        Numbers.decodeLowInt(rangePacked), Numbers.decodeHighInt(rangePacked));
                final long groupRows = indexReader.getImReaderForTest().getRowGroupNumRows(rg);

                final long before = indexReader.getDecodedRowCount();
                drain(indexReader.getCursor(key, 0, Long.MAX_VALUE));
                final long decoded = indexReader.getDecodedRowCount() - before;
                Assert.assertTrue("a packed group must not be decoded whole: decoded=" + decoded +
                        " groupRows=" + groupRows, decoded < groupRows);
            }
        });
    }
```

- [ ] **Step 2: Run to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest#testAPackedRowGroupDecodesOnlyTheKeysRowsNotTheWholeGroup test; echo "MVN_EXIT=$?"
```

Expected: FAIL — `decoded == groupRows`.

- [ ] **Step 3: Narrow the decode**

Two-stage decode within a packed group: first project **only** `key_id` over the whole group, binary-search it for the key's `[lo, hi)` run — the file is key-major and `key_id` ascends within a group, so a binary search is valid — then decode `row_id` and the requested cover slots with `rowLo = lo`, `rowHi = hi`. For a **dedicated** group (`RG_FIRST_KEY[rg] == key` and the group holds nothing else) skip the first stage entirely: the run is the whole group.

Additionally narrow by the row-id range: since `row_id` ascends within a key, `[minValue, maxValue]` maps onto a sub-run of `[lo, hi)`, found by a second binary search over the decoded `row_id` values.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,Covering*Test,PostingIndex*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, all pass. Tasks 4, 6, 7 and 9's correctness tests are the real gate here — a narrowing bug loses rows.

- [ ] **Step 5: Negative control**

Make the `key_id` binary search return `[lo, hi - 1)` — a one-row-short run. Confirm `testTheForwardCursorReturnsAKeysPostingsInAscendingOrder` fails on the SQL `count()` comparison, and that `testAPackedRowGroupDecodesOnlyTheKeysRowsNotTheWholeGroup` still **passes** under the mutation. That asymmetry is the point: the counter test cannot detect a narrowing that is too aggressive, only the correctness test can, and a plan that shipped only the counter test would have measured the optimisation while losing rows. Restore, confirm green, report both outcomes.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): decode only a key's row range inside a packed row group"
```

---

## Task 11: `collectDistinctKeys` and `collectDistinctKeysInRange`

`IndexReader:65` documents a `-1` "not supported, caller uses `getCursor` fallback" contract that **its only caller does not honour**: `PostingIndexDistinctRecordCursorFactory:246` does `foundCount += indexReader.collectDistinctKeys(foundKeys)`. Returning `-1` therefore does not trigger a fallback — it silently shortens `SELECT DISTINCT` by one per partition. The parquet reader must answer, not decline.

Answering is cheap: project only the `key_id` column across the index row groups. Every key present in the partition is the `key_id` of at least one index row, and `RG_FIRST_KEY` gives the first key of each group for free.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java`

**Interfaces:**
- Consumes: `IndexReader.collectDistinctKeys(DirectBitSet foundKeys)` and `collectDistinctKeysInRange(DirectBitSet foundKeys, long rowLo, long rowHi)`, both defaulting to `-1`; `IndexMetaFileReader.getIndexRowGroupCount()`, `getRowGroupFirstKey(int i)`, `getKeyIdColumn()`.
- Produces: real overrides on `AbstractParquetPostingIndexReader` returning the number of keys **newly** marked, never `-1`.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testSelectDistinctOverAParquetSealedPartitionMatchesTheTable() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            assertSql(
                    "count\n0\n",
                    "select count() from ((select distinct sym from x) except (select distinct sym from x where true))"
            );
            // The direct comparison: the index-driven DISTINCT against a
            // group-by that cannot use the index at all.
            assertSqlAndPlanEquals(
                    "select distinct sym from x order by sym",
                    "select sym from x group by sym order by sym"
            );
        });
    }

    @Test
    public void testCollectDistinctKeysNeverReturnsMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            try (TableReader reader = newOffPoolReader(configuration, "x")) {
                final int columnIndex = reader.getMetadata().getColumnIndex("sym");
                final IndexReader indexReader = reader.getIndexReader(0, columnIndex, IndexReader.DIR_FORWARD);
                try (DirectBitSet found = new DirectBitSet(indexReader.getKeyCount())) {
                    final int n = indexReader.collectDistinctKeys(found);
                    // -1 does NOT signal a fallback here: the sole caller adds
                    // it to a running total, so -1 shortens SELECT DISTINCT.
                    Assert.assertTrue("collectDistinctKeys must answer, got " + n, n > 0);
                }
            }
        });
    }
```

Replace `assertSqlAndPlanEquals` with the file's real helper for comparing two query results — write one if none exists; the requirement is that the two answers are compared, not that a particular helper is used.

- [ ] **Step 2: Run to verify they fail**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetPostingIndexReaderTest test; echo "MVN_EXIT=$?"
```

Expected: FAIL — the default returns `-1`, so `SELECT DISTINCT` is short.

- [ ] **Step 3: Implement both**

```java
    /**
     * Marks every key present in this partition. Projects ONLY the key_id
     * column across the index row groups, so no covered or row-id bytes are
     * decoded.
     * <p>
     * Never returns -1. IndexReader documents -1 as "not supported, caller
     * falls back", but the sole caller
     * (PostingIndexDistinctRecordCursorFactory) adds the return value to a
     * running total, so -1 silently shortens SELECT DISTINCT by one per
     * partition rather than triggering any fallback.
     */
    @Override
    public int collectDistinctKeys(DirectBitSet foundKeys) {
```

Walk row groups; for each, decode only `key_id` and set a bit per distinct value, counting only bits that were previously clear. `collectDistinctKeysInRange` additionally decodes `row_id` and skips rows outside `[rowLo, rowHi]`, and skips whole row groups via `isRowGroupPruned(rg, rowLo, rowHi)`.

- [ ] **Step 4: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetPostingIndexReaderTest,PostingIndexDistinct*Test,PostingIndex*Test,Covering*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, all pass.

- [ ] **Step 5: Negative control**

Return `-1` from `collectDistinctKeys` and confirm the `SELECT DISTINCT` comparison fails **by exactly one key** rather than throwing. Report the two result sets' sizes: a failure by more than one would mean something other than the sentinel is wrong. Restore, confirm green.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/AbstractParquetPostingIndexReader.java \
        core/src/test/java/io/questdb/test/cairo/idx/ParquetPostingIndexReaderTest.java
git commit -m "feat(idx): answer collectDistinctKeys from the parquet-form index"
```

---

## Task 12: The differential oracle, the fast/slow path cross-check, and the negative controls

The native index is an exact oracle. A partition never carries both forms at once, so the differential test builds the **same source data twice** — once native-sealed, once parquet-sealed — and compares the two readers key by key. The spec additionally requires a fast-path/slow-path cross-check (the `_im` directory against standard Parquet statistics) and three named negative controls.

`_im` carries deliberate redundancy for exactly this: `RG_FIRST_KEY[i] == chunk(i, KEY_ID_COLUMN).MIN_STAT`, and under `PAYLOAD_KIND = 0` the row-id arrays equal the `row_id` chunk stats. That gives the fast path an independent oracle inside the same file.

**Files:**
- Create: `core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexOracleTest.java`

**Interfaces:**
- Consumes: everything Tasks 3–11 produce, plus Task 1's `read_parquet()` fix (the standard-statistics arm reads a pidx parquet through `read_parquet()` and hits the parallel projection).

- [ ] **Step 1: Write the differential test**

Create the file with the Apache-2.0 header. Structure:

1. Build table `native_arm` with `cairo.posting.index.parquet.partition.format = native` and table `parquet_arm` with it set to `parquet`, from an **identical deterministic row sequence** (fixed seed, no `rnd_*` without a seed). Convert both partitions to parquet.
2. Assert the artifacts: `native_arm`'s partition directory contains `sym.pv.*` and no `*.pidx.*`; `parquet_arm`'s contains exactly one `sym.pidx.<txn>.parquet` and one `sym.pidx.<txn>._im` and no `.pv`. **Assert this before comparing anything** — the ledger records that a config-forwarding gap once made a two-arm bake-off pass while running the native arm in both.
3. For every key in `[0, keyCount)`, both directions, and a grid of `[minValue, maxValue]` ranges — at minimum `[0, MAX]`, `[0, 0]`, `[0, n/2]`, `[n/2, n]`, `[n, MAX]` and an empty range — assert the two cursors emit an **identical row-id sequence** and identical covered values for cover slots `{}`, `{0}` and `{0,1}`.
4. Assert `countMatchesClamped` and `selectKthMatch` agree between the two readers over the same grid.

- [ ] **Step 2: Write the fast-path/slow-path cross-check**

```java
    @Test
    public void testTheImDirectoryAndTheParquetStatisticsSelectTheSameRowGroups() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 200_000);
            final String pidx = findSinglePidxParquet("x", "2024-01-01");
            try (IndexMetaFileReader im = openSingleIm("x", "2024-01-01")) {
                for (int rg = 0, n = im.getIndexRowGroupCount(); rg < n; rg++) {
                    // The _im's own documented redundancy: the directory's
                    // first key must equal the key_id chunk's MIN_STAT.
                    Assert.assertEquals("row group " + rg,
                            im.getRowGroupFirstKey(rg),
                            (int) im.getChunkMinStat(rg, im.getKeyIdColumn()));
                }
            }
            // And the external arm: the same file read through read_parquet(),
            // which decodes from the parquet footer rather than from _im.
            assertSql(
                    "min\tmax\n0\t2\n",
                    "select min(key_id), max(key_id) from read_parquet('" + pidx + "')"
            );
        });
    }
```

Write `openSingleIm(String table, String partition)` returning a bound `IndexMetaFileReader` over the partition's single `_im`, opened with `IndexMetaFileReader.openAndMapRO` and closed by the caller.

- [ ] **Step 3: Run and verify green**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetCoveringIndexOracleTest test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

- [ ] **Step 4: The three named negative controls**

The spec requires each explicitly. Run all three, one at a time, restoring between:

1. **Perturb a row-group `row_id` max.** Patch `RG_ROW_ID_MAX[i]` in a written `_im` (repairing the CRC so the file still opens) for one row group, and confirm the differential test fails. This proves the zone-map pruning is load-bearing rather than decorative.
2. **Perturb an `_im` directory entry.** Patch one `RG_FIRST_KEY[i]`, repair the CRC, and confirm the differential test fails.
3. **Perturb a covered value.** Change one covered value in the pidx parquet and confirm the differential test fails on that row.

For each: report the **verbatim failure message**, and state which assertion caught it. A control whose failure lands on a premise assertion rather than on the comparison proves that the premise, not the property, is what the test detects — say so.

- [ ] **Step 5: Run the full branch gate**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib
cd ~/claude/wt/pidx-parquet/core/rust/qdb-parquet-meta
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib && cargo test --lib --release
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='PostingIndex*Test,Covering*Test,Parquet*Test,IndexMetaFileReaderTest,TableReaderTest,O3*Test,ReadParquetTest,PropServerConfigurationTest,ServerMainTest#testShowParameters,CopyExportTest,PartitionEncoderTest,PartitionUpdaterTest' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml | wc -l
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`, zero failures, zero errors, and the class count at least the 53 the Phase 2 broad suite reached. Confirm the classes actually ran — `-DfailIfNoSpecifiedTests=false` makes a pattern matching nothing report BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add core/src/test/java/io/questdb/test/cairo/idx/ParquetCoveringIndexOracleTest.java
git commit -m "test(idx): differential the parquet-form index against the native oracle"
```

---

## Task 13: Retire the artifacts the O3 in-place update path leaks

**Inherited obligation, reproduced and measured.** One out-of-order INSERT leaves two pidx generations in an unchanged `nameTxn` directory with a live footer of exactly one entry, still present after two purge passes. Wave 5 measured it on **ordinary O3 inserts with no rollback anywhere** and the purge job drained each commit: **2 files leaked per O3 commit, counts 2, 12, 22 … 122 over 60 commits**, bounded only by the rewrite period. It was deliberately **not** pinned by a test in Phase 2, because such a test would assert the leak and fail once fixed.

The mechanism, from the ledger: `ARTIFACT_FORM_PARQUET` has exactly one construction site (`purgeSupersededParquetIndexArtifacts`, reached only from the merge loop at `TableWriter:12660`), and that merge reads the **parse anchor's** covering section — which `updateFileMetadata(0, 0, 0)` on the O3 worker (`O3PartitionJob:591`, contract at `PartitionUpdater:227-249`) has **emptied**. So the merge sees nothing to supersede and queues no purge task. **It cannot be fixed from the sweep**: a footer does name the pair, so unlinking it there is the very Critical wave 4 closed.

The fix therefore has to capture the prior footer's entries **on the O3 worker**, before the update empties the section, and thread them through `updateParquetIndexes`.

Until 2C this was a leak; with a reader it becomes worse — the leaked pairs are reachable and a later seal batch's bookkeeping can disagree about which is current.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/O3PartitionJob.java` (capture at ~`:591`, publish at ~`:637`)
- Modify: `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionUpdater.java`
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java` (`updateParquetIndexes`, the merge loop at ~`:12660`)
- Test: `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java`

**Interfaces:**
- Consumes: `PostingSealPurgeTask`'s `artifactForm` discriminator (`ARTIFACT_FORM_UNKNOWN` / `_NATIVE` / `_PARQUET`) landed in `e8b76152aa`; `purgeSupersededParquetIndexArtifacts`.
- Produces: prior-footer covering entries carried from the O3 worker to `updateParquetIndexes` as a flat `LongList` of `(columnId, indexTxn, imFileSize)` triples, and a purge task queued for every superseded pair.

- [ ] **Step 1: Write the failing test**

```java
    @Test
    public void testAnOrdinaryO3InsertDoesNotLeakTheSupersededPidxPair() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            final int before = countPidxArtifacts("x", "2024-01-01");
            Assert.assertEquals("one committed pair before the O3 write", 2, before);
            for (int i = 0; i < 8; i++) {
                // Out of order: lands in the already-parquet partition, which
                // forces the O3 in-place update path and a full index rebuild.
                execute("insert into x values ('a', 1.0, '2024-01-01T00:00:00.000500Z')");
                drainWalQueue();
                drainPurgeJob();
            }
            final int after = countPidxArtifacts("x", "2024-01-01");
            Assert.assertEquals(
                    "an O3 in-place update must retire the pidx pair it supersedes; " +
                            "measured before the fix as 2 files leaked per commit",
                    2, after);
        });
    }
```

Write `countPidxArtifacts(String table, String partition)` listing the partition directory and counting entries matching `*.pidx.*`. Write `drainPurgeJob()` to run `PostingSealPurgeJob` to quiescence — **not** via `LogCapture.waitForRegex`, which can time out silently.

- [ ] **Step 2: Run to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetIndexSealTest#testAnOrdinaryO3InsertDoesNotLeakTheSupersededPidxPair test; echo "MVN_EXIT=$?"
```

Expected: FAIL, `expected:<2> but was:<18>` (8 commits × 2 leaked, plus the live pair). Report the actual number: if it is not `2 + 2 * commits`, the mechanism differs from the ledger's and must be re-diagnosed before fixing.

- [ ] **Step 3: Capture the prior entries on the O3 worker**

In `O3PartitionJob`, **before** `updateFileMetadata(0, 0, 0)` empties the covering section, read the partition's committed covering-index entries through the same resolve `readPublishedParquetIndexColumnIds` uses — `openParquetMetadataOrThrow(..., parquetFileSize)`, i.e. keyed on the **committed** parquet file size, **not** `resolveLastFooter()`. Wave 3's Critical was exactly this distinction: `resolveLastFooter`'s own javadoc forbids the usage, because a rolled-back in-place update can leave an orphaned dead footer at the tail.

Carry the triples on `PartitionUpdater` alongside the existing update state, and hand them to `updateParquetIndexes` on the writer thread.

- [ ] **Step 4: Queue the purge tasks**

In `updateParquetIndexes`, for every captured `(columnId, indexTxn, imFileSize)` that the new publish does not restate, call `purgeSupersededParquetIndexArtifacts` with `ARTIFACT_FORM_PARQUET` and the window bounded by the **replacing seal's index txn** — the txn at which the supersession becomes visible, half-open, so it must be that txn and not the one before it. Do not recompute the bound; carry it explicitly, as `supersededVisibleAtTxns` already does elsewhere.

**Do not touch `sweepOrphanParquetIndexArtifacts`.** Its union-over-`prev`-chain bound is sound and this fix must not narrow it.

- [ ] **Step 5: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetIndexSealTest,PostingSealPurgeTest,ParquetPostingIndexReaderTest,ParquetCoveringIndexOracleTest,O3*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`. Wave 2's `I6` reseal-supersession branch becomes reachable once this lands — the ledger records it as "testable only once the deferred O3 leak is fixed". Add the test it was waiting for: a pinned reader across an O3 reseal must still resolve its own `index_txn`, and the purge must not unlink the pair that names.

- [ ] **Step 6: Negative control**

Revert only the capture — restore `updateFileMetadata(0, 0, 0)` running before the read — and confirm the leak test fails at the same count as Step 2. Then, separately, change the purge window's upper bound to the replacing seal's index txn **minus one** and confirm a pinned-reader test fails with a silent-deletion message. **Both controls are required**: the first proves the capture reaches production, the second proves the window is not one txn too narrow, which is the shape of wave 1's Critical. Restore, confirm green, report both messages verbatim.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/O3PartitionJob.java \
        core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionUpdater.java \
        core/src/main/java/io/questdb/cairo/TableWriter.java \
        core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "fix(idx): retire the pidx pair an O3 in-place update supersedes"
```

---

## Task 14: Reclaim the `DROP INDEX` token residue

**Inherited obligation.** Token reclamation does not exist: the ledger records the residue as **permanent and crash-only to create** (`4be27950cb` states it as permanent; `W2-I1a` established that the "next publish reclaims the stale token" reclamation is not in the merge loop). It arises when `DROP INDEX` commits and a crash intervenes before the retirement — the token is then never reclaimed, and a later `ADD INDEX TYPE POSTING` publishes with a different index txn over it.

**The obvious implementation is unsafe.** `addIndex` calls `setIndexType()` **after** `writeIndex()` returns, so every publish during an `ADD INDEX` sees the column as **not indexed**, and a metadata-keyed reclamation rule would **drop tokens it must keep**.

**The precedent is one line up, and wave 4 confirmed it:** `setCoveringColumnIndices` at `TableWriter.java:927` is already set **before** `writeIndex` at `:931`, under an explicit comment ("so that `configureCoveringIfNeeded` can find them during index rebuild"); `setIndexType` is at `:933`. So moving `setIndexType` ahead of `writeIndex` is smaller than wave 3 assumed. It is **not implemented**, and it is this task's first step.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java:925-935` and the token merge loop at ~`:12660`
- Test: `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java`

**Interfaces:**
- Consumes: `readCommittedParquetIndexTokens` and the `parquetSweepTokensResolved` flag; `purgeSupersededParquetIndexArtifacts`.
- Produces: a metadata-keyed reclamation in the token merge loop that drops a token for a column the committed metadata says is not POSTING-indexed, safe because `setIndexType` now precedes `writeIndex`.

- [ ] **Step 1: Verify the ordering claim before relying on it**

```bash
cd ~/claude/wt/pidx-parquet
sed -n '915,945p' core/src/main/java/io/questdb/cairo/TableWriter.java
```

Confirm: `setCoveringColumnIndices` precedes `writeIndex`, and `setIndexType` follows it. **If the line numbers or the ordering differ from this description, report the real ones and re-derive the safety argument** — wave 3's own note cited `:914-917` when the code was at `:931-934`, and that mis-citation is exactly what this step exists to catch.

- [ ] **Step 2: Write the failing test**

```java
    @Test
    public void testACrashedDropIndexLeavesNoPermanentTokenResidue() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedParquetTable("x", 50_000);
            Assert.assertEquals(1, countPublishedCoveringTokens("x", "2024-01-01"));
            // Commit the drop, then crash before the retirement -- the only
            // route that creates the residue.
            dropIndexCrashingBeforeRetirement("x", "sym");
            reopenEngine();
            Assert.assertEquals(
                    "a token for a column that is no longer POSTING-indexed must be reclaimed",
                    0, countPublishedCoveringTokens("x", "2024-01-01"));
            Assert.assertEquals("and its artifacts retired", 0, countPidxArtifacts("x", "2024-01-01"));
        });
    }

    @Test
    public void testAPublishDuringAddIndexDoesNotDropTheTokenItMustKeep() throws Exception {
        assertMemoryLeak(() -> {
            // The unsafe direction: if setIndexType still ran AFTER writeIndex,
            // a publish inside ADD INDEX would see the column as not indexed
            // and reclaim a token that is live.
            createParquetTableWithoutIndex("y", 50_000);
            execute("alter table y alter column sym add index type posting include (price)");
            drainWalQueue();
            Assert.assertEquals(1, countPublishedCoveringTokens("y", "2024-01-01"));
            Assert.assertEquals(2, countPidxArtifacts("y", "2024-01-01"));
            assertSql("count\n" + expectedRowsForA + "\n", "select count() from y where sym = 'a'");
        });
    }
```

Write `countPublishedCoveringTokens` reading the partition's `_pm` through `ParquetMetaFileReader` at the **committed** parquet size and counting `getCoveringIndexCount()`. Write `dropIndexCrashingBeforeRetirement` using the file's existing fault-injection facade to fail immediately after the drop's commit and before the retirement; do **not** implement it with a log wait.

- [ ] **Step 3: Run to verify they fail**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest=ParquetIndexSealTest test; echo "MVN_EXIT=$?"
```

Expected: the residue test FAILS with `expected:<0> but was:<1>`; the `ADD INDEX` test PASSES already (it is the guard, and it must stay green through Step 4).

- [ ] **Step 4: Move `setIndexType` and add the reclamation**

Move `setIndexType(...)` to sit **beside** `setCoveringColumnIndices` at `:927`, ahead of `writeIndex` at `:931`, and extend the existing comment to say that both are set early for the same reason: a publish that runs during the index build must see the column as indexed, or a metadata-keyed token rule drops a token it must keep.

Then, in the token merge loop, drop a committed token whose `column_id` names a column the committed metadata says is **not** POSTING-indexed, and hand the pair to `purgeSupersededParquetIndexArtifacts` with `ARTIFACT_FORM_PARQUET`.

**Gate the reclamation on a resolved footer.** Reuse the `parquetSweepTokensResolved` discipline: `readCommittedParquetIndexTokens` **clears its output on `CairoException`**, so "publishes nothing" and "could not be read" are indistinguishable without an explicit flag, and treating the latter as the former is an unlicensed deletion. Only a resolved footer may license this drop.

- [ ] **Step 5: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='ParquetIndexSealTest,PostingSealPurgeTest,ParquetPostingIndexReaderTest,ParquetCoveringIndexOracleTest,TableWriterTest,PostingIndex*Test,Covering*Test' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
grep -h "tests=" core/target/surefire-reports/*.xml
```

Expected: `MVN_EXIT=0`. `TableWriterTest` is in the gate because moving `setIndexType` changes the order of two metadata writes on the `ADD INDEX` path.

- [ ] **Step 6: Negative control — both directions**

1. Restore `setIndexType` to its original position after `writeIndex`, keep the reclamation, and confirm `testAPublishDuringAddIndexDoesNotDropTheTokenItMustKeep` **fails** — this is the unsafe implementation the ledger warned about, and if it passes, the reclamation is not running during `ADD INDEX` and the whole safety argument is untested.
2. Remove the `parquetSweepTokensResolved` gate and inject an unreadable `_pm`, and confirm a test fails with an unlicensed-deletion message rather than silently reclaiming.

Restore both, confirm green, report both messages verbatim.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/TableWriter.java \
        core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "fix(idx): reclaim a covering index token for a column that is no longer indexed"
```

---

## Phase 2C Completion Check

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib
cd ~/claude/wt/pidx-parquet/core/rust/qdb-parquet-meta
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib && cargo test --lib --release
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
rm -rf core/target/surefire-reports
mvn -pl core -Pbuild-rust-library -Dtest='PostingIndex*Test,Covering*Test,Parquet*Test,IndexMetaFileReaderTest,TableReaderTest,TableWriterTest,O3*Test,ReadParquetTest,*Purge*Test,PropServerConfigurationTest,ServerMainTest#testShowParameters,CopyExportTest,PartitionEncoderTest,PartitionUpdaterTest' -DfailIfNoSpecifiedTests=false test; echo "MVN_EXIT=$?"
ls core/target/surefire-reports/*.xml | wc -l
grep -h "tests=" core/target/surefire-reports/*.xml
```

Both crate directories are gated **separately**, and `qdb-parquet-meta` additionally in **release** — several guards there are `debug_assert!` with a release fallback, and their tests are `cfg(debug_assertions)`-split, so a debug-only run reports the fallback test as `ok` while it early-returns and asserts nothing.

Read counts from the XML. Confirm `MVN_EXIT=0`, the class count is at least the 53 the Phase 2 broad suite reached, and every expected class actually produced a report — `-DfailIfNoSpecifiedTests=false` makes a pattern matching nothing report BUILD SUCCESS.

## Recorded for Phase 3, not built here

- **Payload arm B and the two-arm bake-off.** Only arm N is written; `ParquetIndexSeal` hard-codes `PAYLOAD_ROW_PER_POSTING`. The `_im` format supports arm B end to end, so what is missing is the writer arm and its selection. `cairo.posting.index.parquet.payload` was removed in `ecbbcfefdc` and must be reintroduced with arm B, not before.
- **Pruning level 4** — per-key covered-column statistics are written and the file is externally prunable, but nothing pushes a general filter into the covering index scan; only `latestByFilter` exists. Planner work.
- **`ColumnIndex` / `OffsetIndex` page skipping** as level 3's stated mechanism (see Task 10's open question).
- **Exact key presence** — an `RG_LAST_KEY` array or a key bloom section behind an optional feature bit. The directory answers "which row groups could hold `k`", not "does `k` exist".
- **A `SORTED_KEY_THEN_ROW_ID` feature bit**, so sortedness is not only in the Parquet footer.
- **`_pm` compaction** — deferred by design, and constrained: see Global Constraints.
- **`LogCapture.waitForRegex` / `waitFor`** — the `elapsed > maxWait` / `elapsed >= maxWait` mismatch, 26 call sites, its own PR.
- **`CairoConfigurationWrapper.getPostingIndexRowIdEncoding`** is still not forwarded, so the EF and delta encodings are effectively untested through their suites. Separate pre-existing master bug; fixing it may turn those suites red, which is why it must not ride on this branch.
- **`PostingSealPurgeJob` / `PostingSealPurgeOperator` have zero `getCheckpointStatus().isInProgress()` checks**, unlike every comparable purge job. Purges can run during a checkpoint. Question for the purge-job owner.
- **Cold-storage reconversion (enterprise).** `StoragePolicyJob` compares the `_pm`-embedded squash tracker against the live `_txn` counter and treats a mismatch as RECONVERT; a token publish writes exactly 8 bytes at offset 0 and never rewrites the header. So an index DDL on a cold-storage-managed parquet partition can trigger a full reconversion whose fresh `_pm` names no covering token while the pidx pairs stay on disk. Unverified end to end; no enterprise branch for this feature exists.
