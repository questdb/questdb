# Covering Index in Parquet Form — Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a covering POSTING index actually become a Parquet artifact when its partition converts to Parquet — produce `<col>.pidx.<indexTxn>.parquet` and its `_im` sidecar, and publish them atomically with the txn.

**Architecture:** Phase 1 delivered the `_im` format and a row-group flush primitive, both dark. Phase 2 adds the enablers those need to be usable (an interface hoist, a row-count flush, a `_pm` footer token), then the write path: `indexParquetColumn`'s seal point emits a key-aligned index Parquet file instead of `.pv`/`.pc*`, with `_im` generated Rust-side from the streaming writer's own row-group metadata. The read path is Phase 2C and is mapped, not detailed, here.

**Tech Stack:** Java 17 (zero-GC, `Unsafe`/mmap), Rust (`qdb-parquet-meta`, `qdbr` JNI, `parquet_write`), Maven, Cargo, JUnit 4 with `assertMemoryLeak`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-10-covering-index-parquet-design.md`. Format: `docs/index-metadata.md` (`_im` v3). Branch `feat/covering-index-parquet`, worktree `~/claude/wt/pidx-parquet`.
- Java members grouped by kind (static vs instance) and visibility, then sorted **alphabetically**. No `// ===` or `// ---` banner comments. Exception and log messages strictly **ASCII**. Underscore separators in numbers with 5+ digits. Booleans use `is`/`has` prefixes. Modern Java 17 (enhanced switch, pattern variables).
- Tests use `assertMemoryLeak()`. Native memory, file descriptors and JNI writer/result pointers must be freed on **every** path including error paths.
- New Java files carry the standard QuestDB Apache-2.0 header, copied from a neighbour in the same package.
- Rust builds under `-D warnings` (`core/pom.xml:52`): an unused variable, import or `mut` is a **build failure**.
- **`qdb-parquet-meta` is a path dependency, NOT a workspace member.** Cargo commands run from `core/rust/qdbr` are completely blind to it — they run none of its tests, reformat none of its files, lint none of its code. Any task touching it must gate from **inside** `core/rust/qdb-parquet-meta` as well.
- Java tests that cross JNI **require** `mvn -pl core -Pbuild-rust-library -Dtest=... test` (profile at `core/pom.xml:522`). Without it you test a stale `core/target/classes`; a reviewer once reproduced an already-fixed crash that way.
- Always `export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR` before Maven.
- Read `core/target/surefire-reports` for real Tests-run counts. A shell exit code taken after a pipe is the last pipeline stage's, not Maven's; `mvn -q` suppresses the Tests-run lines; and `-DfailIfNoSpecifiedTests=false` makes a pattern matching nothing still report BUILD SUCCESS.
- Any new `PropertyKey` breaks `ServerMainTest#testShowParameters`, which asserts exact set equality on `SHOW PARAMETERS`. Add the line to `expectedProps` in the same commit.
- Do **not** run multiple `mvn test` commands in parallel.

## Scope: Phase 2 is three subsystems

The spec's Phase 2 covers the enablers, the write path and the read path. Each produces working, testable software on its own, and the read path's steps depend on artifacts the write path does not yet produce. This document therefore details **2A (enablers)** and **2B (write path)** in full, and maps **2C (read path)**, which gets its own plan once 2B lands.

| part | contents | state |
| --- | --- | --- |
| **2A** | Extract a `PostingIndexReader` contract; `flushRowGroup(ptr, rows)`; `_pm` footer `COVERING_INDEX` section | detailed below |
| **2B** | Rust `_im` generation from the streaming writer; key-aligned `pidx.parquet` at the seal point; config gating; `parquet → native` and superseded-version GC | detailed below |
| **2C** | `ParquetPostingIndexFwd/BwdReader`, `IndexFactory` dispatch on on-disk form, pruning levels 1–3, `collectDistinctKeys` | mapped at the end |

2A ships no behaviour change. 2B ships behaviour only when `cairo.posting.index.parquet.partition.format = parquet`, which defaults to `native`.

## File Structure

**Modify:**

| file | change |
| --- | --- |
| `core/src/main/java/io/questdb/cairo/idx/AbstractPostingIndexReader.java` | declares `implements PostingIndexReader` |
| `core/src/main/java/io/questdb/griffin/engine/table/CoveringIndexRecordCursorFactory.java` | four concrete-type references narrowed to the interface |
| `core/rust/qdbr/src/parquet_write/jni.rs` | `flushRowGroup` takes a row count |
| `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java` | matching Java declaration |
| `core/rust/qdb-parquet-meta/src/types.rs`, `footer.rs` | `COVERING_INDEX_BIT` and its footer section |
| `core/src/main/java/io/questdb/cairo/ParquetMetaFileReader.java` | read the new footer section |
| `core/rust/qdbr/src/parquet_metadata/jni/index_writer.rs` | `generate_index_metadata` entry point |
| `core/src/main/java/io/questdb/cairo/TableWriter.java` | seal point emits `pidx.parquet` + `_im`; `linkPartitionIndexFiles` gated |

**Create:**

| file | responsibility |
| --- | --- |
| `core/rust/qdbr/src/parquet_metadata/index_gen.rs` | build an `_im` from a finished streaming writer's row groups plus caller-supplied key and boundary data |
| `core/src/main/java/io/questdb/cairo/idx/ParquetIndexSeal.java` | drive the streaming Parquet writer over per-key posting runs, key-aligned, and emit both artifacts |
| `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java` | end-to-end: seal a partition, assert artifacts, key alignment, and round trip |
| `core/src/main/java/io/questdb/cairo/idx/PostingIndexReader.java` | the narrow posting contract both readers implement |
| `core/src/test/java/io/questdb/test/cairo/idx/PostingIndexReaderContractTest.java` | pins the contract and guards it against quietly growing |

---

## Task 1: Extract a `PostingIndexReader` contract

`CoveringIndexRecordCursorFactory` reaches the concrete `AbstractPostingIndexReader` in four places, and the cast at `:1354` is unguarded on the primary forward single-key covering path. A Phase 2C Parquet-backed reader cannot satisfy that cast without inheriting ~3100 lines of `.pk` chain and generation machinery it has no analogue for. Extract the narrow contract the factory actually uses instead.

Hoisting these onto `IndexReader` was considered and rejected: bitmap indexes never become Parquet, so widening their contract buys nothing.

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/idx/PostingIndexReader.java`
- Modify: `core/src/main/java/io/questdb/cairo/idx/AbstractPostingIndexReader.java:243,276,395,528`
- Modify: `core/src/main/java/io/questdb/griffin/engine/table/CoveringIndexRecordCursorFactory.java:37,1354,1399,2640`
- Test: `core/src/test/java/io/questdb/test/cairo/idx/PostingIndexReaderContractTest.java`

**Interfaces:**
- Produces: `public interface PostingIndexReader extends IndexReader` declaring exactly `long getEntryMaxValue()`, `long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped)`, `long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k)` and `void populateCacheForKey(int key)`. Phase 2C's Parquet reader implements this instead of extending the abstract class.

**The seam was measured before this task was written — do not widen it.** The only methods called through the concrete type in that factory are the four above (`reader.` inside `fillFrameForKeyCheap`, and `posting.` at the `:2640` site). `warmForKeys` is NOT used there and must stay off the interface. If you find you need a fifth method to make this compile, STOP and report it: that means the seam is wider than measured and the shape needs rethinking, not a bigger interface.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/idx/PostingIndexReaderContractTest.java` with the Apache-2.0 header copied from a neighbour in that package. It asserts the contract is satisfiable without the chain machinery — i.e. that a class implementing `PostingIndexReader` compiles and can be passed where the covering factory expects one:

```java
    @Test
    public void testNativeReaderSatisfiesTheContract() {
        Assert.assertTrue(PostingIndexReader.class.isAssignableFrom(PostingIndexFwdReader.class));
        Assert.assertTrue(PostingIndexReader.class.isAssignableFrom(PostingIndexBwdReader.class));
    }

    @Test
    public void testContractDeclaresOnlyTheSeamMethods() {
        final java.util.Set<String> declared = new java.util.TreeSet<>();
        for (java.lang.reflect.Method m : PostingIndexReader.class.getDeclaredMethods()) {
            declared.add(m.getName());
        }
        Assert.assertEquals("[countMatchesClamped, getEntryMaxValue, populateCacheForKey, selectKthMatch]", declared.toString());
    }
```

The second test is the guard against the interface quietly growing: if a later change adds a method, it fails and forces the decision to be explicit.

- [ ] **Step 2: Run test to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Dtest=PostingIndexReaderContractTest test
```

Expected: compilation failure — `cannot find symbol: class PostingIndexReader`.

- [ ] **Step 3: Create the interface**

```java
package io.questdb.cairo.idx;

/**
 * The posting-index primitives the covering query path reaches through a
 * reader, separated from {@link AbstractPostingIndexReader}'s chain and
 * generation machinery so a Parquet-backed reader can serve them without
 * inheriting it.
 * <p>
 * Deliberately narrow: it declares exactly the methods
 * {@code CoveringIndexRecordCursorFactory} calls through the concrete type.
 * Adding to it couples a new caller to every implementation, so a new method
 * belongs here only when more than one implementation can answer it.
 */
public interface PostingIndexReader extends IndexReader {

    /**
     * Exact count of postings for {@code key} within
     * {@code [minValue, maxValueClamped]}, or {@code -1} when the reader
     * cannot answer from metadata alone and the caller must walk a cursor.
     */
    long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped);

    /**
     * Highest row id the reader's current entry covers.
     */
    long getEntryMaxValue();

    /**
     * Warms any per-key cache the reader keeps.
     */
    void populateCacheForKey(int key);

    /**
     * Absolute row id of the {@code k}-th posting of {@code key} within
     * {@code [minValue, maxValueClamped]}, or {@code -1} when the reader
     * cannot resolve it from metadata alone.
     */
    long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k);
}
```

Declare `AbstractPostingIndexReader implements PostingIndexReader` and add `@Override` to the four implementations at lines 243, 276, 395 and 528. Do not move their bodies.

- [ ] **Step 4: Narrow the covering factory to the interface**

In `CoveringIndexRecordCursorFactory`, change the import at `:37`, the `fillFrameForKeyCheap` parameter type at `:1399`, and the `instanceof` at `:2640` from `AbstractPostingIndexReader` to `PostingIndexReader`. At `:1354`, replace the unguarded cast by widening the enclosing guard:

```java
            if (cheapEligible && prepRowCursor != null && framePostingReader instanceof PostingIndexReader reader) {
```

and delete the now-redundant local declaration on the following line. Falling out of that block is already a supported outcome — it takes the parked traverse — so no new fallback is needed.

- [ ] **Step 5: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Dtest=PostingIndexReaderContractTest test
mvn -pl core -Dtest='PostingIndex*Test,Covering*Test' -DfailIfNoSpecifiedTests=false test
```

Expected: the new tests pass and the 31-class posting/covering suite stays green. This task changes no behaviour.

- [ ] **Step 6: Negative control**

Temporarily make `AbstractPostingIndexReader.countMatchesClamped` return `-1` unconditionally and re-run `Covering*Test`. It MUST fail — proving the covering fast path still reaches the real implementation through the narrowed type rather than silently taking the traverse. If nothing fails, Step 4 has diverted the fast path and the change is wrong. Restore and confirm green. Report exactly what you observed.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/PostingIndexReader.java \
        core/src/main/java/io/questdb/cairo/idx/AbstractPostingIndexReader.java \
        core/src/main/java/io/questdb/griffin/engine/table/CoveringIndexRecordCursorFactory.java \
        core/src/test/java/io/questdb/test/cairo/idx/PostingIndexReaderContractTest.java
git commit -m "refactor(idx): extract a PostingIndexReader contract"
```

---

## Task 2: `flushRowGroup` takes a row count

The Phase 1 signature captures `accumulated_rows` wholesale, so a boundary can only fall at a chunk boundary and a caller must submit one chunk per key run — roughly 110k JNI submissions per partition at the symbol cardinality this design targets. `write_pending_row_group` already supports cutting mid-chunk.

**Files:**
- Modify: `core/rust/qdbr/src/parquet_write/jni.rs`
- Modify: `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java`
- Test: `core/src/test/java/io/questdb/test/griffin/engine/table/parquet/ParquetRowGroupFlushTest.java`

**Interfaces:**
- Produces: `PartitionEncoder.flushRowGroup(long writerPtr, long rows)`. Task 5 calls it once per key-aligned row group.

- [ ] **Step 1: Write the failing test**

Add to `ParquetRowGroupFlushTest`, reusing its existing fixture helpers. The fixture writes 8 rows as a 3-row frame then a 5-row frame. Submit **both** frames before any flush, then flush at 4 and let finish emit the rest — a boundary that falls *inside* the second frame, which the Phase 1 signature cannot express:

```java
    @Test
    public void testFlushAtRowCountCutsInsideAChunk() throws Exception {
        assertMemoryLeak(() -> {
            // Both frames submitted first, so 8 rows are pending. Flushing at 4
            // must cut inside frame 2 -- the whole point of the row-count form.
            final int[] sizes = streamExportFlushingAt(4);
            Assert.assertEquals(2, sizes.length);
            Assert.assertEquals(4, sizes[0]);
            Assert.assertEquals(4, sizes[1]);
        });
    }
```

Write `streamExportFlushingAt(int rows)` as a variant of the file's existing export helper that submits every frame first, then calls `PartitionEncoder.flushRowGroup(writerPtr, rows)` once, then finishes — and returns the row group sizes read back with `ParquetMetaFileReader`, as the file's existing tests do. Pass `rowGroupSize = 1_000_000` so the fixed threshold cannot create a boundary itself.

- [ ] **Step 2: Run test to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest=ParquetRowGroupFlushTest test
```

Expected: compilation failure — `flushRowGroup(long,long)` does not exist.

- [ ] **Step 3: Change the Rust entry point**

In `core/rust/qdbr/src/parquet_write/jni.rs`, `Java_..._flushRowGroup` gains a `rows: jlong` parameter. Reject a negative `rows` with a `CairoException` through the existing `ffi_guard`, as the other entry points reject negative counts. Replace the wholesale capture with:

```rust
    encoder.forced_row_group_rows = Some((rows as usize).min(encoder.accumulated_rows));
```

Keep the existing "do not arm when `accumulated_rows == 0`" guard, and keep the existing behaviour that a second flush while a boundary is already captured retains the earlier capture. `capped_forced_row_count` already filters `> 0` and clamps with `.min(accumulated_rows)`, so no other change is needed.

- [ ] **Step 4: Change the Java declaration**

In `PartitionEncoder.java`, update the native declaration in place (it stays alphabetically between `finishStreamingParquetWrite` and `populateEmptyPartition`):

```java
    public static native void flushRowGroup(long writerPtr, long rows) throws CairoException;
```

Update its javadoc to state that `rows` is clamped to the pending row count, that the row group is emitted by the next drain or by finish, and that a second flush before a drain keeps the first boundary.

- [ ] **Step 5: Update the existing call sites**

Every existing call in `ParquetRowGroupFlushTest` passes the pending row count to preserve its current meaning — the Phase 1 semantics are `flushRowGroup(ptr, accumulated_rows)`. Update each accordingly so the file's existing expectations are unchanged.

- [ ] **Step 6: Run the tests**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt && cargo check --all-targets && cargo clippy --all-targets && cargo test --lib
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest=ParquetRowGroupFlushTest test
mvn -pl core -Pbuild-rust-library -Dtest='CopyExportTest,PartitionEncoderTest,PartitionUpdaterTest' test
```

Expected: all green, including the threshold-path tests added in Phase 1 and the export suites — this must not change the fixed-`rowGroupSize` path.

- [ ] **Step 7: Negative control**

Change the clamp to `Some(rows as usize)` (dropping `.min`), pass a `rows` larger than the pending count, and confirm a test fails. Restore. Report what you observed.

- [ ] **Step 8: Commit**

```bash
git add core/rust/qdbr/src/parquet_write/jni.rs \
        core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java \
        core/src/test/java/io/questdb/test/griffin/engine/table/parquet/ParquetRowGroupFlushTest.java
git commit -m "feat(parquet): flushRowGroup takes an explicit row count"
```

---

## Task 3: `_pm` footer `COVERING_INDEX` section

The index version token must be anchored to the partition's MVCC snapshot. Footer bits 0 (`SEQ_TXN_BIT`) and 1 (`SCRATCHPAD_BIT`) are taken; bit 2 is free. A dedicated bit is used rather than the scratchpad TLV because the scratchpad's update writer silently inherits the previous footer's entries when its setter is not called, which would leave a stale index token pointing at a superseded index.

**Files:**
- Modify: `core/rust/qdb-parquet-meta/src/types.rs`, `core/rust/qdb-parquet-meta/src/footer.rs`
- Modify: `core/src/main/java/io/questdb/cairo/ParquetMetaFileReader.java`
- Test: Rust `mod tests` in `footer.rs`; `core/src/test/java/io/questdb/test/cairo/ParquetMetaFileReaderTest.java`

**Interfaces:**
- Produces: `FooterFeatureFlags::COVERING_INDEX_BIT: u64 = 1 << 2`; footer section `[entry_count u32][(column_id u32, index_txn u64, im_file_size u64)*]`; Rust `FooterBuilder::add_covering_index(&mut self, column_id: u32, index_txn: u64, im_file_size: u64)`; Java `ParquetMetaFileReader.getCoveringIndexCount()`, `getCoveringIndexColumnId(int i)`, `getCoveringIndexTxn(int i)`, `getCoveringIndexImFileSize(int i)`. Task 6 writes it; Phase 2C reads it to dispatch on the on-disk index form.

- [ ] **Step 1: Write the failing Rust test**

In `core/rust/qdb-parquet-meta/src/footer.rs`'s `mod tests`:

```rust
    #[test]
    fn test_covering_index_section_round_trip() {
        let mut fb = FooterBuilder::new(1024, 512);
        fb.add_covering_index(3, 7, 1_180);
        fb.add_covering_index(9, 7, 2_048);
        let bytes = fb.build().unwrap();
        let footer = Footer::parse(&bytes).unwrap();
        assert!(footer.feature_flags().has(FooterFeatureFlags::COVERING_INDEX_BIT));
        assert_eq!(footer.covering_index_count(), 2);
        assert_eq!(footer.covering_index(0), (3, 7, 1_180));
        assert_eq!(footer.covering_index(1), (9, 7, 2_048));
    }

    #[test]
    fn test_no_covering_index_leaves_the_bit_clear() {
        let fb = FooterBuilder::new(1024, 512);
        let bytes = fb.build().unwrap();
        let footer = Footer::parse(&bytes).unwrap();
        assert!(!footer.feature_flags().has(FooterFeatureFlags::COVERING_INDEX_BIT));
        assert_eq!(footer.covering_index_count(), 0);
    }
```

Adapt `FooterBuilder::new`, `build`, `Footer::parse` and the flag-test helper to whatever the file actually names them — read the existing `SEQ_TXN` and `SCRATCHPAD` tests at `footer.rs:1043` and `:1049` and mirror their shape exactly.

- [ ] **Step 2: Run to verify it fails**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdb-parquet-meta
cargo test --lib covering_index
```

Expected: compilation failure — no `COVERING_INDEX_BIT`, no `add_covering_index`.

- [ ] **Step 3: Implement the flag and section**

In `types.rs`, next to the existing bits:

```rust
    /// Per-indexed-column covering index token, stored in the footer feature
    /// sections as `[entry_count u32][(column_id u32, index_txn u64,
    /// im_file_size u64)*]`. Absent when the partition has no Parquet-form
    /// covering index.
    pub const COVERING_INDEX_BIT: u64 = 1 << 2;
```

In `footer.rs`, add the builder method, the section serialisation and the parse side. Feature sections appear **in bit order**, so `COVERING_INDEX` is written after `SEQ_TXN` and `SCRATCHPAD` and before the CRC. Follow the existing sections' framing exactly, and set the bit only when at least one entry exists — matching how `SEQ_TXN` is omitted when its value is `-1`.

- [ ] **Step 4: Run the Rust gates**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdb-parquet-meta
cargo fmt && cargo fmt -- --check && cargo check --all-targets && cargo clippy --all-targets && cargo test --lib
cd ../qdbr
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib
```

Expected: both crates clean, zero warnings.

- [ ] **Step 5: Add the Java reader side plus its test**

Add the four accessors to `ParquetMetaFileReader` in correct alphabetical position, resolving the section from the footer feature flags exactly as the existing bloom-filter section is resolved. Add a `ParquetMetaFileReaderTest` case that builds a `_pm` with two covering-index entries via `ParquetMetaFileWriter` and reads them back, plus one asserting `getCoveringIndexCount() == 0` when the bit is clear.

- [ ] **Step 6: Run the Java tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest=ParquetMetaFileReaderTest test
```

Expected: PASS. Read the surefire report for the count.

- [ ] **Step 7: Negative control**

Change the section's entry stride by 4 bytes in the Java reader and confirm the round-trip test fails; restore. This is the Rust/Java agreement that matters — report what you observed.

- [ ] **Step 8: Commit**

```bash
git add core/rust/qdb-parquet-meta/src/types.rs core/rust/qdb-parquet-meta/src/footer.rs \
        core/src/main/java/io/questdb/cairo/ParquetMetaFileReader.java \
        core/src/test/java/io/questdb/test/cairo/ParquetMetaFileReaderTest.java
git commit -m "feat(parquet): add the _pm covering index footer section"
```

---

## Task 4: Rust `_im` generation from a finished streaming writer

Production `_im` must be built Rust-side from the streaming writer's own thrift metadata, not through `IndexMetaFileWriter` — Java has none of the codec, encoding, byte-range or statistic values, and the analogous `ParquetMetaFileWriter` has no production caller for exactly that reason. `qdb_parquet_meta::convert::build_row_group_block` already returns the `RowGroupBlockBuilder` that `IndexMetaWriter::add_row_group` takes.

**Files:**
- Create: `core/rust/qdbr/src/parquet_metadata/index_gen.rs`
- Modify: `core/rust/qdbr/src/parquet_metadata/mod.rs`, `core/rust/qdbr/src/parquet_metadata/jni/index_writer.rs`
- Test: `mod tests` in `index_gen.rs`

**Interfaces:**
- Consumes: `ChunkedWriter::row_groups()`, `parquet_footer_offset()`, `schema()` from `core/rust/qdbr/src/parquet_write/file.rs`; `qdb_parquet_meta::convert::build_row_group_block`; `IndexMetaWriter` from `core/rust/qdb-parquet-meta/src/index_meta.rs`.
- Produces: JNI `Java_io_questdb_cairo_IndexMetaFileWriter_generateIndexMetadata(writerPtr, firstKeysPtr, firstKeysLen, rowIdMinPtr, rowIdMinLen, rowIdMaxPtr, rowIdMaxLen, dataBoundariesPtr, dataBoundariesLen, count, keySpaceSize, keyIdColumn, rowIdColumn, firstCoverColumn, payloadKind)` returning a result pointer whose bytes are the complete `_im`. Task 5 calls it.

- [ ] **Step 1: Write the failing Rust test**

Create `core/rust/qdbr/src/parquet_metadata/index_gen.rs` with the Apache-2.0 header and only a test module, so it compiles against symbols that do not yet exist:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generates_an_im_matching_the_written_row_groups() {
        // Build a small streaming parquet with three row groups whose first
        // keys are 0, 5 and 5 -- the last two a dedicated run for key 5, which
        // the _im key-alignment invariant permits.
        let written = write_test_index_parquet(&[(0, 0, 9), (5, 10, 19), (5, 20, 29)]);
        let im = generate_index_metadata(
            &written,
            &[0, 5, 5],
            &[0, 10, 20],
            &[9, 19, 29],
            &[0, 15, 30],
            10,
            0,
            1,
            2,
            0,
        )
        .unwrap();
        let reader = qdb_parquet_meta::IndexMetaReader::new(&im).unwrap();
        assert_eq!(reader.index_row_group_count(), 3);
        assert_eq!(reader.key_space_size(), 10);
        assert_eq!(reader.row_group_range_for_key(5), Some((1, 2)));
        assert_eq!(reader.row_id_min(1), 10);
        assert_eq!(reader.row_id_max(2), 29);
        assert_eq!(reader.data_row_group_boundary(2), 30);
        assert!(reader.pidx_footer_offset() > 0);
    }
}
```

Write `write_test_index_parquet` as a test helper driving the existing streaming writer over a `(key_id INT32, row_id INT64, price DOUBLE)` schema, flushing at each key boundary with the row-count form from Task 2, and returning whatever handle `generate_index_metadata` consumes. Adapt accessor names to the real `IndexMetaReader` API.

- [ ] **Step 2: Run to verify it fails**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo test --lib index_gen
```

Expected: compilation failure — `generate_index_metadata` not found.

- [ ] **Step 3: Implement `generate_index_metadata`**

Above the test module, implement a function that:

1. builds an `IndexMetaWriter` with the caller's `payload_kind`, `key_space_size`, `key_id_column`, `row_id_column` and `first_cover_column`;
2. adds one column descriptor per schema column, synthetic columns first then covered columns in cover-slot order, with `ID` set to the covered column's writer index and `-1` for the synthetic ones;
3. for each row group, calls `build_row_group_block` on the writer's thrift metadata and passes the resulting builder to `add_row_group` together with that group's first key, row-id min and row-id max;
4. sets the data row-group boundaries and the pidx footer offset and length from `parquet_footer_offset()`;
5. calls `finish()` and returns the bytes.

Every validation the `_im` writer performs stays in force — in particular the key-alignment invariant, which will reject a file whose row groups split a key across a shared group. That rejection is the desired behaviour; do not weaken it.

Declare the module in `core/rust/qdbr/src/parquet_metadata/mod.rs`.

- [ ] **Step 4: Add the JNI entry point**

In `index_writer.rs`, add `generateIndexMetadata` following that file's established shape: route through `ffi_guard`; guard every count against negative values; and pass each array's **byte length** alongside its element count, since these are typed arrays where count and byte length differ by an element-size multiplier — the two defects already fixed in this file were both of exactly that shape.

- [ ] **Step 5: Run the gates**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt && cargo check --all-targets && cargo clippy --all-targets && cargo test --lib
cd ../qdb-parquet-meta
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib
```

Expected: both clean.

- [ ] **Step 6: Negative control**

Pass a `first_keys` array that splits key 5 across a shared row group (`[0, 5, 6]` with row group 1 actually holding keys 5 and 6) and confirm `generate_index_metadata` returns an error from the `_im` writer's key-alignment validation rather than producing a file. Report what you observed — this proves the invariant reaches the production path, not just the unit tests.

- [ ] **Step 7: Commit**

```bash
git add core/rust/qdbr/src/parquet_metadata/index_gen.rs \
        core/rust/qdbr/src/parquet_metadata/mod.rs \
        core/rust/qdbr/src/parquet_metadata/jni/index_writer.rs
git commit -m "feat(idx): generate _im from a finished streaming parquet writer"
```

---

## Task 5: Emit a key-aligned `pidx.parquet` at the seal point

`indexParquetColumn` (`TableWriter:7915`, called from `:8106`, `:10848`, `:12599`) already decodes every row group of `data.parquet`, builds per-key posting lists in memory, and accumulates covered column values into mmap'd temp files in row order. Only the seal changes: instead of writing `.pv`/`.pc*`, emit one Parquet file whose row groups are key-aligned.

**Files:**
- Create: `core/src/main/java/io/questdb/cairo/idx/ParquetIndexSeal.java`
- Create: `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java`
- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java:7915`

**Interfaces:**
- Consumes: `PartitionEncoder.createStreamingParquetWriter`, `writeStreamingParquetChunk`, `flushRowGroup(ptr, rows)` from Task 2, `finishStreamingParquetWrite`; `IndexMetaFileWriter.generateIndexMetadata` from Task 4.
- Produces: `ParquetIndexSeal.seal(...)` returning the index txn it wrote, and writing `<col>.pidx.<indexTxn>.parquet` plus `<col>.pidx.<indexTxn>._im` into the partition directory. Task 6 calls it and publishes the token.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java`. Build a table with an indexed SYMBOL column and an `INCLUDE` list, convert a partition to Parquet with `cairo.posting.index.parquet.partition.format = parquet`, and assert:

- `<col>.pidx.<indexTxn>.parquet` and `<col>.pidx.<indexTxn>._im` exist in the partition directory;
- the `_im` opens through `IndexMetaFileReader` and its `KEY_SPACE_SIZE` equals the native reader's `getKeyCount()` for the same partition;
- **no row group boundary falls mid-key** — for every row group, the `key_id` chunk's min and max stats satisfy the invariant, which you assert by reading them back rather than by trusting the writer;
- every key present in the partition resolves to a non-empty row group range.

Include a **sparse key set** (symbols chosen so key ids are sparse, e.g. by inserting and deleting symbols, or by a table whose symbol column has many unused ids) so the `KEY_SPACE_SIZE`-vs-distinct-count distinction is exercised end to end.

- [ ] **Step 2: Run to verify it fails**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest=ParquetIndexSealTest test
```

Expected: FAIL — the artifacts do not exist, because nothing produces them.

- [ ] **Step 3: Implement `ParquetIndexSeal`**

Create the class with a single entry point that takes the per-key posting lists and the covered-column temp mmaps `indexParquetColumn` already holds, plus the partition's data row-group boundaries, and:

1. creates a streaming Parquet writer over the schema `key_id INT32, row_id INT64, <covered columns in cover-slot order>`, passing a `rowGroupSize` large enough that the fixed threshold can never fire — the threshold **will** split a key otherwise, and the `_im` writer will then reject the file;
2. iterates keys ascending, appending each key's postings and gathered covered values, and calling `flushRowGroup(writerPtr, rowsInGroup)` at a key boundary once the accumulated rows reach the configured target;
3. records each row group's first key, row-id min and row-id max as it goes;
4. calls `finishStreamingParquetWrite`, then `generateIndexMetadata` with those arrays, and writes both files, patching `IM_FILE_SIZE` last.

The index txn is the writer's current txn. Free the JNI writer and result pointers on every path, including exceptional ones.

- [ ] **Step 4: Call it from the seal point**

In `indexParquetColumn`, when `configuration.getPostingIndexParquetPartitionFormat() == PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET`, call `ParquetIndexSeal.seal(...)` in place of the native seal. Leave the native path untouched otherwise.

- [ ] **Step 5: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest=ParquetIndexSealTest test
mvn -pl core -Pbuild-rust-library -Dtest='PostingIndex*Test,Covering*Test' -DfailIfNoSpecifiedTests=false test
```

Expected: the new test passes; the 31-class posting/covering suite stays green, because the config defaults to `native`.

- [ ] **Step 6: Negative control**

The obvious form of this control does not work, and the reason is worth understanding before you run it. Shrinking `rowGroupSize` so the fixed threshold fires mid-key **does** make the seal fail — but with the wrong error. The threshold emits far more row groups than the caller declared first keys for, so the `_im` writer's directory-count check (`N first keys for M row groups`) fires first and the key-alignment check never runs. The control would look like it passed while testing something else entirely.

Instead, isolate the defect: leave the threshold inert (a `rowGroupSize` large enough that it cannot fire) and break the *caller* — flush on a fixed row stride that ignores key boundaries, so the row-group count still matches the declared directory and a key split across a shared group is the only remaining defect. Confirm the seal fails with the key-alignment rejection specifically, naming the two row groups and the shared key. Then restore and re-verify green.

This is the end-to-end proof that the invariant is enforced in production rather than only in the `_im` writer's unit tests. Report the verbatim message, not just that it failed — the whole point is *which* check caught it.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/idx/ParquetIndexSeal.java \
        core/src/main/java/io/questdb/cairo/TableWriter.java \
        core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "feat(idx): seal a covering index as key-aligned parquet"
```

---

## Task 6: Publish the token, gate the link path, and handle conversion back

The artifacts exist but nothing references them, `linkPartitionIndexFiles` still hard-links native sidecars unconditionally, and `parquet → native` has no path back.

**Files:** publishing the token is a signature change across a JNI boundary, not a setter call. `update_parquet_metadata` takes `seq_txn` as an explicit parameter and constructs the `ParquetMetaUpdateWriter` internally, so no Java caller can reach `set_covering_index` today. The entries (or an explicit clear) must be threaded through every layer:

- Modify: `core/src/main/java/io/questdb/cairo/TableWriter.java:2823,3733` (`linkPartitionIndexFiles` call sites), `:3717` (`_pm` hard-link), `restoreIndexFilesAfterParquetToNative`
- Modify: `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionUpdater.java` — carry the entries to the JNI call
- Modify: `core/rust/qdbr/src/parquet_write/jni.rs` — accept them at the updater entry point
- Modify: `core/rust/qdbr/src/parquet_write/update.rs` — hold them on `ParquetUpdater`, mirroring how `self.seq_txn` is held around `:1536`
- Modify: `core/rust/qdbr/src/parquet_metadata/convert.rs:148-158,219` — widen `update_parquet_metadata`'s signature and call the setter
- Test: `core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java`

**There are TWO Java call sites reaching `updateFileMetadata()`**, not one: `O3PartitionJob.java:581` and `TableUtils.java:2301`. A change that reasons only about the O3 path leaves the other panicking in debug builds, because Task 3's `debug_assert!` fires on any in-place `_pm` update whose prior footer carried the covering-index bit.

**Dropping the token is not retiring the artifact.** `clear_covering_index()` — and the release-mode fallback — removes the *pointer* while leaving `<col>.pidx.<txn>.parquet` and `<col>.pidx.<txn>._im` on disk. The drop decision and the unlink decision must be the same decision point, or every O3 update over an indexed Parquet partition leaks two files.

**Superseded is not sufficient grounds to delete.** A reader pinned to the prior snapshot still resolves the *old* `index_txn`, so purging must be gated on no reader being able to reach the artifact — not on the token having been superseded.

**What keeps that prior entry reachable is the reader's own `_pm` mapping, NOT the MVCC chain.** A token publish restates the same `parquet_footer_offset`/`parquet_footer_length`, so the appended footer derives the same `data.parquet` size as the one it replaces and `find_footer_for_parquet_size` — which returns the *newest* match — shadows the prior entry for every mapping made after the header patch. The prior footer is still in the file and is no longer selectable. A reader is safe only because it maps the `_pm` at the size its own snapshot's header named and resolves from that tail. The purge window is therefore expressed in table txns over the scoreboard, and the publish must restamp something a reloading reader reconciles on, or a reader can advance past the seal's txn still holding the pre-publish mapping. See `docs/parquet-metadata.md`, "Token-only appends".

This is also why Step 3 copies `_pm` rather than hard-linking it: copying keeps the two directories' MVCC chains independent enough for the purge to be decidable per-directory.

Expect test churn as well: existing tests that build a `_pm` carrying a covering index and then run an in-place update will start panicking in debug once this task writes the bit.

**Interfaces:**
- Consumes: `ParquetIndexSeal.seal(...)` from Task 5; the `_pm` covering-index section from Task 3.

- [ ] **Step 1: Write the failing tests**

Add to `ParquetIndexSealTest`:

- the partition's `_pm` footer carries a covering-index entry whose `column_id`, `index_txn` and `im_file_size` match the written `_im`;
- with the config set to `parquet`, **no** `.pk`/`.pv`/`.pci`/`.pc*` files are hard-linked into the partition directory;
- with the config set to `native`, the artifacts are the native ones and no `pidx` files appear — the existing behaviour is untouched;
- converting the partition back with `ALTER TABLE ... CONVERT PARTITION TO NATIVE` produces a working native index and queries return the same rows as before conversion;
- a second seal supersedes the first: the new `index_txn` appears in `_pm` and the previous `pidx`/`_im` pair is removed.

- [ ] **Step 2: Run to verify they fail**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest=ParquetIndexSealTest test
```

Expected: the new cases fail.

- [ ] **Step 3: Publish the token and gate the link path**

Write the covering-index entry into the `_pm` footer as part of the same commit sequence the spec fixes: `pidx.parquet` → `_im` (patch `IM_FILE_SIZE` last) → `_pm` footer (patch `PARQUET_META_FILE_SIZE` last) → `_txn`. Gate both `linkPartitionIndexFiles` call sites so the native sidecars are linked only when the config selects `native`.

**In the index-only regime, copy `_pm` instead of hard-linking it** (`TableWriter:3717` currently calls `ff.hardLink`). A hard link shares the inode, so appending a footer would mutate the file the old partition directory also names, and with `data.parquet` byte-identical both footers derive the same MVCC token — a reader pinned to the old partition would resolve the new `index_txn` and name an `_im` that does not exist in its directory. `_pm` is kilobytes; `data.parquet` still hard-links.

- [ ] **Step 4: Handle conversion back and superseded versions**

`restoreIndexFilesAfterParquetToNative` prefers hard-linking the native index files and falls back to `rebuildColumnIndex` when the key file is absent. With `pidx` artifacts the link branch never fires and the fallback becomes the only path — which is complete (`rebuildColumnIndex` calls `configureCoveringIfNeeded`) but strictly more expensive. Confirm the fallback is reached and works, and add the superseded-version cleanup to the same purge path that removes orphan partition directories.

- [ ] **Step 5: Run the tests**

```bash
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest=ParquetIndexSealTest test
mvn -pl core -Pbuild-rust-library -Dtest='PostingIndex*Test,Covering*Test,IndexMetaFileReaderTest,ParquetRowGroupFlushTest,PropServerConfigurationTest,ServerMainTest#testShowParameters,CopyExportTest,PartitionEncoderTest,PartitionUpdaterTest' -DfailIfNoSpecifiedTests=false test
```

Expected: all green. The second command is the branch's full gate — 38 classes at the end of Phase 1.

- [ ] **Step 6: Negative control**

Revert the `_pm` copy to a hard link, run the superseded-version test, and confirm it fails — proving the shared-inode hazard is real and the test detects it. Restore. Report what you observed.

- [ ] **Step 7: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/TableWriter.java \
        core/src/test/java/io/questdb/test/cairo/ParquetIndexSealTest.java
git commit -m "feat(idx): publish the parquet index token and gate the link path"
```

---

## Phase 2 Completion Check

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib
cd ~/claude/wt/pidx-parquet/core/rust/qdb-parquet-meta
cargo fmt -- --check && cargo clippy --all-targets && cargo test --lib
cargo test --lib --release
export QDB_TEST_TMPDIR=/dev/shm/qdb-test && mkdir -p $QDB_TEST_TMPDIR
cd ~/claude/wt/pidx-parquet
mvn -pl core -Pbuild-rust-library -Dtest='PostingIndex*Test,Covering*Test,IndexMetaFileReaderTest,ParquetIndexSealTest,IndexReaderDefaultsTest,ParquetMetaFileReaderTest,ParquetRowGroupFlushTest,PropServerConfigurationTest,ServerMainTest#testShowParameters,CopyExportTest,PartitionEncoderTest,PartitionUpdaterTest' -DfailIfNoSpecifiedTests=false test
```

Both crate directories must be gated separately, and `qdb-parquet-meta` is additionally run in
**release**. Several guards in that crate are `debug_assert!` with a release fallback, and their
tests are split by `cfg(debug_assertions)` — the guard test compiles only in debug, the
fallback-behaviour test only in release. A debug-only run reports the fallback test as `ok` while
it early-returns and asserts nothing: a vacuous green. The release run takes 0.01s and exercises
both halves of every such guard, `seq_txn` as well as covering-index. Read the surefire reports for real counts and confirm the expected classes actually ran — `-DfailIfNoSpecifiedTests=false` makes a pattern matching nothing report BUILD SUCCESS.

## Phase 2C — mapped, not detailed

2C gets its own plan once 2B lands, because its steps consume signatures 2B fixes and artifacts 2B is the first to produce.

| task | contents |
| --- | --- |
| Reader | `ParquetPostingIndexFwdReader` / `BwdReader` implementing `IndexReader`, serving `getCursor(key, minValue, maxValue, requiredCoverColumns)` by projecting only the needed Parquet columns, and feeding `CoveredColumnDecoder` unchanged |
| Dispatch | `IndexFactory.createReader` gains the partition's on-disk index form, resolved from the `_pm` covering-index section — **not** from configuration, and not from an `exists()` probe, since the write order commits `_im` before the `_pm` footer and a crash leaves an uncommitted orphan |
| Pruning 1–3 | key directory → row-group range; row-id zone maps against the interval scan's `[minValue, maxValue]`; Parquet `ColumnIndex`/`OffsetIndex` for page skipping |
| `collectDistinctKeys` | answer by projecting only the `key_id` column across row groups. `IndexReader:60` documents a `-1` "caller falls back" contract that its only caller does not honour — `PostingIndexDistinctRecordCursorFactory:246` adds it to a counter — so returning `-1` would silently shorten `SELECT DISTINCT` |
| Differential tests | the native index as an exact oracle: same partition data both ways, identical cursor output for every key, both directions, and a grid of row-id ranges |

Deferred beyond 2C, recorded so they are not lost: exact key-presence (an `RG_LAST_KEY` array or a key bloom section behind a feature bit — the directory answers "which row groups could hold `k`", not "does `k` exist"); a `SORTED_KEY_THEN_ROW_ID` feature bit so sortedness is not only in the Parquet footer; and filter pushdown into the covering scan, which is what makes pruning level 4's per-key covered-column statistics reachable.
