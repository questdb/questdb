# Covering Index in Parquet Form — Phase 1 (Foundation) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the foundations for storing covering POSTING indexes as Parquet artifacts — the opt-in configuration, the `_im` sidecar format (Rust writer, Java reader), and the Rust primitive that lets the streaming Parquet writer close a row group on a key boundary — with no behaviour change to any existing query path.

**Architecture:** Configuration is added first and defaults to `native`, so every subsequent phase is dark by default. The `_im` format is implemented in the existing `qdb-parquet-meta` Rust crate next to the `_pm` format it mirrors, with JNI bindings in `qdbr` and a memory-mapped Java reader, following exactly the `ParquetMetaFileWriter` / `ParquetMetaFileReader` split. The row-group flush primitive extends the existing streaming Parquet writer rather than introducing a second writer.

**Tech Stack:** Java 17 (zero-GC style, `Unsafe`/`MemoryR` mmap access), Rust (`qdb-parquet-meta`, `qdbr` JNI), Maven, Cargo, JUnit 4 with `assertMemoryLeak`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-10-covering-index-parquet-design.md`. Branch `feat/covering-index-parquet`, worktree `~/claude/wt/pidx-parquet`, based on `origin/master` @ `12c2934052`.
- Java class members are grouped by kind (static vs. instance) and visibility, then sorted **alphabetically**. Insert new members in the correct alphabetical position.
- Never insert `// ===` or `// ---` banner comments in any Java file.
- Boolean variables, fields and methods use an `is...` or `has...` prefix.
- Log messages must be **strictly ASCII**.
- Tests use `assertMemoryLeak()` unless they are narrow unit tests that allocate no native memory.
- Underscore thousands separators in numbers with 5 or more digits (`100_000`).
- Rust: the crate lives in `core/rust/qdbr/`; `qdb-parquet-meta` is a sibling crate. Before any Rust task is complete, all four of `cargo fmt`, `cargo check --all-targets`, `cargo clippy --all-targets`, `cargo test --lib` must pass with **zero errors and zero warnings**.
- The `_im` format is little-endian only, matching `_pm` (`core/rust/qdb-parquet-meta/src/types.rs` has `compile_error!` on big-endian targets).
- Do **not** run multiple `mvn test` commands in parallel.
- All Java files carry the standard QuestDB Apache-2.0 header comment; copy it verbatim from a neighbouring file in the same package.

## Phase Map

This plan is Phase 1 of three. The phases are sequential, not independent: Phase 2 and Phase 3 consume the exact `_im` byte layout and JNI signatures that Phase 1 fixes. Writing their steps now would mean inventing signatures that Phase 1 has not yet settled, which is precisely the placeholder failure this plan format forbids. Each phase gets its own plan document, written once its predecessor has landed.

| phase | contents | plan |
| --- | --- | --- |
| **1 (this plan)** | Config properties and accessors; `_im` format in Rust; JNI + Java `_im` reader/writer; Rust key-aligned row-group flush primitive | this document |
| **2** | Index write path (arm N, `row_per_posting`): seal replacement in `indexParquetColumn`, `_pm` footer feature section, `linkPartitionIndexFiles` gating, `ParquetPostingIndexFwd/BwdReader`, on-disk-form dispatch in `IndexFactory.createReader`, differential tests against the native oracle | written after Phase 1 lands |
| **3** | Pruning levels 2–3 (row-group zone maps, `ColumnIndex`/`OffsetIndex`), mixed-state and flag-flip tests, `parquet -> native` fallback coverage, arm B (`row_per_key`), the bake-off harness | written after Phase 2 lands |

Phase 1 ships no user-visible behaviour change. Its deliverable is that the configuration parses, the `_im` format round-trips through Rust and Java, and the Parquet writer can close a row group on demand.

## File Structure

**Created:**

| file | responsibility |
| --- | --- |
| `core/rust/qdb-parquet-meta/src/index_meta.rs` | `_im` layout constants, `IndexMetaWriter`, `IndexMetaReader`, Rust round-trip tests |
| `core/rust/qdbr/src/parquet_metadata/jni/index_writer.rs` | JNI bindings for `io.questdb.cairo.IndexMetaFileWriter` |
| `core/src/main/java/io/questdb/cairo/IndexMetaFileWriter.java` | thin JNI wrapper, mirrors `ParquetMetaFileWriter` |
| `core/src/main/java/io/questdb/cairo/IndexMetaFileReader.java` | memory-mapped `_im` reader: commit-signal handling, CRC validation, key directory lookup, zone maps, byte ranges |
| `core/src/test/java/io/questdb/test/cairo/IndexMetaFileReaderTest.java` | Java round-trip and validation tests against the real Rust writer |
| `core/src/test/java/io/questdb/test/griffin/engine/table/parquet/ParquetRowGroupFlushTest.java` | asserts caller-driven row-group boundaries |

**Modified:**

| file | change |
| --- | --- |
| `core/rust/qdb-parquet-meta/src/lib.rs` | declare and re-export `index_meta` |
| `core/rust/qdbr/src/parquet_metadata/jni/mod.rs` | declare `index_writer` |
| `core/rust/qdbr/src/parquet_write/file.rs` | expose an explicit row-group flush on the streaming writer |
| `core/rust/qdbr/src/parquet_write/jni.rs` | JNI entry point for the flush |
| `core/src/main/java/io/questdb/PropertyKey.java` | two new property keys |
| `core/src/main/java/io/questdb/PropServerConfiguration.java` | parse the two properties |
| `core/src/main/java/io/questdb/cairo/CairoConfiguration.java` | two default accessors |
| `core/src/main/java/io/questdb/cairo/idx/PostingIndexUtils.java` | four format constants |
| `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java` | `flushRowGroup` native declaration |
| `core/src/test/java/io/questdb/test/PropServerConfigurationTest.java` | assert defaults and overrides |

---

## Task 1: Configuration properties

Adds `cairo.posting.index.parquet.partition.format` and `cairo.posting.index.parquet.payload`. Both default to today's behaviour, so nothing changes. This task is separable from the rest: a reviewer can approve the configuration surface without judging the `_im` layout.

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/idx/PostingIndexUtils.java`
- Modify: `core/src/main/java/io/questdb/PropertyKey.java`
- Modify: `core/src/main/java/io/questdb/PropServerConfiguration.java`
- Modify: `core/src/main/java/io/questdb/cairo/CairoConfiguration.java`
- Test: `core/src/test/java/io/questdb/test/PropServerConfigurationTest.java`

**Interfaces:**
- Produces: `PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE` / `PARQUET_INDEX_FORMAT_PARQUET` (both `byte`), `PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_POSTING` / `PARQUET_INDEX_PAYLOAD_ROW_PER_KEY` (both `byte`); `CairoConfiguration.getPostingIndexParquetPartitionFormat()` and `CairoConfiguration.getPostingIndexParquetPayload()`, both returning `byte`. Phase 2 reads these in `TableWriter` to choose between hard-linking native sidecars and writing `pidx` artifacts.

- [ ] **Step 1: Write the failing test**

Add to `core/src/test/java/io/questdb/test/PropServerConfigurationTest.java`, in the correct alphabetical position among the existing test methods:

```java
    @Test
    public void testPostingIndexParquetFormatDefaults() throws Exception {
        Properties properties = new Properties();
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE,
                configuration.getCairoConfiguration().getPostingIndexParquetPartitionFormat()
        );
        Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_POSTING,
                configuration.getCairoConfiguration().getPostingIndexParquetPayload()
        );
    }

    @Test
    public void testPostingIndexParquetFormatOverrides() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("cairo.posting.index.parquet.partition.format", "parquet");
        properties.setProperty("cairo.posting.index.parquet.payload", "row_per_key");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET,
                configuration.getCairoConfiguration().getPostingIndexParquetPartitionFormat()
        );
        Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_KEY,
                configuration.getCairoConfiguration().getPostingIndexParquetPayload()
        );
    }

    @Test
    public void testPostingIndexParquetFormatUnknownValueFallsBackToDefault() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("cairo.posting.index.parquet.partition.format", "banana");
        properties.setProperty("cairo.posting.index.parquet.payload", "banana");
        PropServerConfiguration configuration = newPropServerConfiguration(properties);
        Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE,
                configuration.getCairoConfiguration().getPostingIndexParquetPartitionFormat()
        );
        Assert.assertEquals(
                PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_POSTING,
                configuration.getCairoConfiguration().getPostingIndexParquetPayload()
        );
    }
```

Add `import io.questdb.cairo.idx.PostingIndexUtils;` in the correct alphabetical position among the imports. If the class has no `newPropServerConfiguration(Properties)` helper, use whichever construction helper the neighbouring tests in that file already use, unchanged.

- [ ] **Step 2: Run test to verify it fails**

```bash
cd ~/claude/wt/pidx-parquet
mvn -q -pl core -Dtest=PropServerConfigurationTest#testPostingIndexParquetFormatDefaults test
```

Expected: compilation failure — `cannot find symbol: method getPostingIndexParquetPartitionFormat()`.

- [ ] **Step 3: Add the format constants**

In `core/src/main/java/io/questdb/cairo/idx/PostingIndexUtils.java`, add these four constants in the correct alphabetical position among the existing `public static final` fields (they sort just after `MAX_BLOCK_COUNT` and before `PACKED_BATCH_SIZE`):

```java
    public static final byte PARQUET_INDEX_FORMAT_NATIVE = 0;
    public static final byte PARQUET_INDEX_FORMAT_PARQUET = 1;
    public static final byte PARQUET_INDEX_PAYLOAD_ROW_PER_KEY = 1;
    public static final byte PARQUET_INDEX_PAYLOAD_ROW_PER_POSTING = 0;
```

- [ ] **Step 4: Add the property keys**

In `core/src/main/java/io/questdb/PropertyKey.java`, immediately after the existing `CAIRO_POSTING_INDEX_INDEXER_SPILL_BYTES_MAX` entry (around line 108):

```java
    CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT("cairo.posting.index.parquet.partition.format"),
    CAIRO_POSTING_INDEX_PARQUET_PAYLOAD("cairo.posting.index.parquet.payload"),
```

- [ ] **Step 5: Parse the properties**

In `core/src/main/java/io/questdb/PropServerConfiguration.java`, add two fields next to the existing `postingIndexRowIdEncoding` declaration (around line 450), in alphabetical order:

```java
    private final byte postingIndexParquetPartitionFormat;
    private final byte postingIndexParquetPayload;
```

Then, immediately after the `this.postingIndexRowIdEncoding = switch (...)` block (around line 1698), add:

```java
            this.postingIndexParquetPartitionFormat = switch (getString(properties, env, PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PARTITION_FORMAT, "native")) {
                case "parquet" -> PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET;
                default -> PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE;
            };
            this.postingIndexParquetPayload = switch (getString(properties, env, PropertyKey.CAIRO_POSTING_INDEX_PARQUET_PAYLOAD, "row_per_posting")) {
                case "row_per_key" -> PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_KEY;
                default -> PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_POSTING;
            };
```

In the inner `CairoConfiguration` implementation class, next to the existing `getPostingIndexRowIdEncoding()` override (around line 4616), add both overrides in alphabetical position:

```java
        @Override
        public byte getPostingIndexParquetPartitionFormat() {
            return postingIndexParquetPartitionFormat;
        }

        @Override
        public byte getPostingIndexParquetPayload() {
            return postingIndexParquetPayload;
        }
```

- [ ] **Step 6: Add the interface defaults**

In `core/src/main/java/io/questdb/cairo/CairoConfiguration.java`, immediately before the existing `getPostingIndexRowIdEncoding()` default (around line 696):

```java
    default byte getPostingIndexParquetPartitionFormat() {
        return PostingIndexUtils.PARQUET_INDEX_FORMAT_NATIVE;
    }

    default byte getPostingIndexParquetPayload() {
        return PostingIndexUtils.PARQUET_INDEX_PAYLOAD_ROW_PER_POSTING;
    }
```

- [ ] **Step 7: Run the tests to verify they pass**

```bash
cd ~/claude/wt/pidx-parquet
mvn -q -pl core -Dtest=PropServerConfigurationTest test
```

Expected: PASS, no failures.

- [ ] **Step 8: Commit**

```bash
cd ~/claude/wt/pidx-parquet
git add core/src/main/java/io/questdb/PropertyKey.java \
        core/src/main/java/io/questdb/PropServerConfiguration.java \
        core/src/main/java/io/questdb/cairo/CairoConfiguration.java \
        core/src/main/java/io/questdb/cairo/idx/PostingIndexUtils.java \
        core/src/test/java/io/questdb/test/PropServerConfigurationTest.java
git commit -m "feat(idx): add posting index parquet format config properties"
```

---

## Task 2: `_im` format in Rust

Implements the `_im` sidecar layout with a builder and a reader, plus round-trip and corruption tests, in the crate that already owns the sibling `_pm` format.

**Files:**
- Create: `core/rust/qdb-parquet-meta/src/index_meta.rs`
- Modify: `core/rust/qdb-parquet-meta/src/lib.rs`

**Interfaces:**
- Consumes: `crate::error::{ParquetMetaError, ParquetMetaErrorKind, ParquetMetaResult}` and the `parquet_meta_err!` macro, all already present in the crate.
- Produces: `IndexMetaWriter` with `new(payload_kind: u32, key_count: u32) -> Self`, `set_payload(&mut self, payload_kind: u32, key_count: u32)`, `add_row_group(&mut self, first_key: u32, row_id_min: i64, row_id_max: i64, col_ranges: &[(u64, u64)])`, `set_data_row_group_boundaries(&mut self, boundaries: &[i64])`, `finish(&self) -> ParquetMetaResult<Vec<u8>>`; and `IndexMetaReader::new(data: &[u8]) -> ParquetMetaResult<IndexMetaReader<'_>>` with accessors `im_file_size`, `payload_kind`, `key_count`, `index_row_group_count`, `data_row_group_count`, `index_column_count`, `row_group_range_for_key`, `row_id_min`, `row_id_max`, `data_row_group_boundary`, `column_byte_range`. Task 3 calls the writer over JNI; Phase 2's Java reader implements the same accessor semantics.

### The `_im` layout

All integers little-endian. A committed `_im` file is complete and immutable — there is no MVCC chain inside the file, because the spec versions each index by the partition directory or the `_pm` footer token.

```
Header, 48 bytes fixed:
  off  size  field                type  notes
  0    8     IM_FILE_SIZE         u64   total committed size; patched last; NOT CRC-covered
  8    8     FEATURE_FLAGS        u64   bits 0-31 optional, bits 32-63 required
  16   4     FORMAT_VERSION       u32   = 1
  20   4     PAYLOAD_KIND         u32   0 = row_per_posting, 1 = row_per_key
  24   4     INDEX_RG_COUNT       u32   row groups in <col>.pidx.parquet
  28   4     DATA_RG_COUNT        u32   row groups in data.parquet
  32   4     INDEX_COLUMN_COUNT   u32   columns in the index schema
  36   4     KEY_COUNT            u32   distinct symbol keys covered
  40   8     RESERVED             u64   must be 0

Sections, in order, each starting 8-byte aligned:
  RG_FIRST_KEY       u32 * (INDEX_RG_COUNT + 1)   first key id present in each index
                                                  row group; final entry is a sentinel
                                                  equal to KEY_COUNT
  RG_ROW_ID_MIN      i64 * INDEX_RG_COUNT
  RG_ROW_ID_MAX      i64 * INDEX_RG_COUNT
  DATA_RG_BOUNDARY   i64 * (DATA_RG_COUNT + 1)    cumulative data.parquet row counts,
                                                  first entry 0
  RG_COL_RANGE       (u64 offset, u64 length) * INDEX_RG_COUNT * INDEX_COLUMN_COUNT
                                                  row-major by row group

Trailer, 4 bytes at IM_FILE_SIZE - 4:
  CHECKSUM           u32   CRC32 over bytes [8, IM_FILE_SIZE - 4)
```

`RG_FIRST_KEY` is monotonically non-decreasing. A key that spans several consecutive row groups produces repeated entries, which is how the reader recovers a multi-group range. For key `k`:

- `rg_lo` = `lower_bound(RG_FIRST_KEY[0..rg_count], k)`; if that index is past the end or `RG_FIRST_KEY[rg_lo] != k`, use `rg_lo - 1` instead.
- `rg_hi` = `upper_bound(RG_FIRST_KEY[0..rg_count], k) - 1`.

Worked example, `RG_FIRST_KEY = [0, 11_403, 11_403, 11_404, KEY_COUNT]`: key `11_403` gives `rg_lo = 1`, `rg_hi = 2` (spans two groups); key `5` gives `lower_bound = 1`, `RG_FIRST_KEY[1] != 5` so `rg_lo = 0`, and `upper_bound = 1` so `rg_hi = 0` (packed in group 0).

- [ ] **Step 1: Write the failing test**

Create `core/rust/qdb-parquet-meta/src/index_meta.rs` containing only the module doc comment and the test module, so the test compiles against symbols that do not yet exist:

```rust
//! `_im` covering-index metadata file format.
//!
//! Sidecar to `<col>.pidx.parquet`, mirroring the role `_pm` plays for
//! `data.parquet`: it carries the byte ranges needed to fetch index row
//! groups without reading the Parquet footer, the key directory used to
//! locate a key's row groups, and the zone maps used to prune them.
//!
//! The format specification lives in
//! `docs/superpowers/specs/2026-08-10-covering-index-parquet-design.md`.

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sample() -> Vec<u8> {
        let mut w = IndexMetaWriter::new(0, 11_405);
        w.add_row_group(0, 0, 99_999, &[(4, 100), (104, 200)]);
        w.add_row_group(11_403, 100_000, 157_999, &[(304, 50), (354, 60)]);
        w.add_row_group(11_403, 158_000, 240_000, &[(414, 70), (484, 80)]);
        w.add_row_group(11_404, 240_001, 999_999, &[(564, 90), (654, 10)]);
        w.set_data_row_group_boundaries(&[0, 500_000, 1_000_000]);
        w.finish().unwrap()
    }

    #[test]
    fn test_round_trip_header_fields() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.im_file_size(), bytes.len() as u64);
        assert_eq!(r.payload_kind(), 0);
        assert_eq!(r.key_count(), 11_405);
        assert_eq!(r.index_row_group_count(), 4);
        assert_eq!(r.data_row_group_count(), 2);
        assert_eq!(r.index_column_count(), 2);
    }

    #[test]
    fn test_key_spanning_multiple_row_groups() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(11_403), Some((1, 2)));
    }

    #[test]
    fn test_key_packed_into_shared_row_group() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(5), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(0), Some((0, 0)));
        assert_eq!(r.row_group_range_for_key(11_404), Some((3, 3)));
    }

    #[test]
    fn test_key_out_of_range() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_group_range_for_key(11_405), None);
        assert_eq!(r.row_group_range_for_key(u32::MAX), None);
    }

    #[test]
    fn test_zone_maps_and_byte_ranges() {
        let bytes = build_sample();
        let r = IndexMetaReader::new(&bytes).unwrap();
        assert_eq!(r.row_id_min(1), 100_000);
        assert_eq!(r.row_id_max(1), 157_999);
        assert_eq!(r.column_byte_range(2, 1), (484, 80));
        assert_eq!(r.data_row_group_boundary(0), 0);
        assert_eq!(r.data_row_group_boundary(2), 1_000_000);
    }

    #[test]
    fn test_checksum_mismatch_is_rejected() {
        let mut bytes = build_sample();
        let victim = 48;
        bytes[victim] ^= 0xFF;
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(
            err.kind,
            ParquetMetaErrorKind::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_truncated_file_is_rejected() {
        let bytes = build_sample();
        let err = IndexMetaReader::new(&bytes[..40]).unwrap_err();
        assert!(matches!(err.kind, ParquetMetaErrorKind::Truncated));
    }

    #[test]
    fn test_version_mismatch_is_rejected() {
        let mut bytes = build_sample();
        bytes[16..20].copy_from_slice(&99u32.to_le_bytes());
        let err = IndexMetaReader::new(&bytes).unwrap_err();
        assert!(matches!(
            err.kind,
            ParquetMetaErrorKind::VersionMismatch { .. }
        ));
    }
}
```

Declare the module in `core/rust/qdb-parquet-meta/src/lib.rs`, in the existing alphabetical `pub mod` list (between `header` and `infer`):

```rust
pub mod index_meta;
```

and add the re-export next to the other `pub use` lines:

```rust
pub use index_meta::{IndexMetaReader, IndexMetaWriter};
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo test --lib -p qdb-parquet-meta index_meta
```

Expected: compilation failure — `cannot find type IndexMetaWriter in this scope`.

- [ ] **Step 3: Implement the layout constants and the writer**

Insert into `core/rust/qdb-parquet-meta/src/index_meta.rs`, above the `#[cfg(test)]` module:

```rust
use crate::error::{ParquetMetaErrorKind, ParquetMetaResult};
use crate::parquet_meta_err;

#[cfg(not(target_endian = "little"))]
compile_error!("index meta format requires a little-endian target");

/// Fixed portion of the `_im` header.
pub const IM_HEADER_SIZE: usize = 48;
/// Current `_im` format version.
pub const IM_FORMAT_VERSION: u32 = 1;
/// Size of the CRC trailer at the end of the file.
pub const IM_TRAILER_SIZE: usize = 4;
/// First byte covered by the CRC; `IM_FILE_SIZE` at offset 0 is excluded
/// because the writer patches it last as the commit signal.
pub const IM_CRC_AREA_OFF: usize = 8;

const OFF_IM_FILE_SIZE: usize = 0;
const OFF_FEATURE_FLAGS: usize = 8;
const OFF_FORMAT_VERSION: usize = 16;
const OFF_PAYLOAD_KIND: usize = 20;
const OFF_INDEX_RG_COUNT: usize = 24;
const OFF_DATA_RG_COUNT: usize = 28;
const OFF_INDEX_COLUMN_COUNT: usize = 32;
const OFF_KEY_COUNT: usize = 36;
const OFF_RESERVED: usize = 40;

struct RowGroupMeta {
    first_key: u32,
    row_id_min: i64,
    row_id_max: i64,
    col_ranges: Vec<(u64, u64)>,
}

/// Builds a complete `_im` file in memory.
pub struct IndexMetaWriter {
    payload_kind: u32,
    key_count: u32,
    row_groups: Vec<RowGroupMeta>,
    data_boundaries: Vec<i64>,
}

impl IndexMetaWriter {
    pub fn new(payload_kind: u32, key_count: u32) -> Self {
        Self {
            payload_kind,
            key_count,
            row_groups: Vec::new(),
            data_boundaries: Vec::new(),
        }
    }

    /// Appends one index row group. `col_ranges` is one `(offset, length)`
    /// pair per index column, in schema order; every row group must supply
    /// the same number of pairs.
    pub fn add_row_group(
        &mut self,
        first_key: u32,
        row_id_min: i64,
        row_id_max: i64,
        col_ranges: &[(u64, u64)],
    ) {
        self.row_groups.push(RowGroupMeta {
            first_key,
            row_id_min,
            row_id_max,
            col_ranges: col_ranges.to_vec(),
        });
    }

    pub fn set_data_row_group_boundaries(&mut self, boundaries: &[i64]) {
        self.data_boundaries = boundaries.to_vec();
    }

    /// Overwrites the payload kind and key count set at construction. The JNI
    /// layer creates the writer before Java knows either value, so it calls
    /// this once the index build has determined them.
    pub fn set_payload(&mut self, payload_kind: u32, key_count: u32) {
        self.payload_kind = payload_kind;
        self.key_count = key_count;
    }

    /// Serialises the complete `_im` file. Takes `&self` — matching
    /// `ParquetMetaWriter::finish` — so the JNI layer can build the buffer
    /// without consuming the boxed writer that Java still owns and will
    /// later hand to `destroyWriter`.
    pub fn finish(&self) -> ParquetMetaResult<Vec<u8>> {
        if self.data_boundaries.is_empty() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "data row group boundaries not set"
            ));
        }
        let rg_count = self.row_groups.len();
        let col_count = self.row_groups.first().map_or(0, |rg| rg.col_ranges.len());
        for (i, rg) in self.row_groups.iter().enumerate() {
            if rg.col_ranges.len() != col_count {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::SchemaMismatch,
                    "row group {i} has {} column ranges, expected {col_count}",
                    rg.col_ranges.len()
                ));
            }
            if i > 0 && rg.first_key < self.row_groups[i - 1].first_key {
                return Err(parquet_meta_err!(
                    ParquetMetaErrorKind::InvalidValue,
                    "row group first keys must be non-decreasing at index {i}"
                ));
            }
        }

        let mut buf = vec![0u8; IM_HEADER_SIZE];
        buf[OFF_FORMAT_VERSION..OFF_FORMAT_VERSION + 4]
            .copy_from_slice(&IM_FORMAT_VERSION.to_le_bytes());
        buf[OFF_PAYLOAD_KIND..OFF_PAYLOAD_KIND + 4]
            .copy_from_slice(&self.payload_kind.to_le_bytes());
        buf[OFF_INDEX_RG_COUNT..OFF_INDEX_RG_COUNT + 4]
            .copy_from_slice(&(rg_count as u32).to_le_bytes());
        buf[OFF_DATA_RG_COUNT..OFF_DATA_RG_COUNT + 4]
            .copy_from_slice(&((self.data_boundaries.len() - 1) as u32).to_le_bytes());
        buf[OFF_INDEX_COLUMN_COUNT..OFF_INDEX_COLUMN_COUNT + 4]
            .copy_from_slice(&(col_count as u32).to_le_bytes());
        buf[OFF_KEY_COUNT..OFF_KEY_COUNT + 4].copy_from_slice(&self.key_count.to_le_bytes());
        buf[OFF_FEATURE_FLAGS..OFF_FEATURE_FLAGS + 8].copy_from_slice(&0u64.to_le_bytes());
        buf[OFF_RESERVED..OFF_RESERVED + 8].copy_from_slice(&0u64.to_le_bytes());

        for rg in &self.row_groups {
            buf.extend_from_slice(&rg.first_key.to_le_bytes());
        }
        buf.extend_from_slice(&self.key_count.to_le_bytes());
        align_to_8(&mut buf);

        for rg in &self.row_groups {
            buf.extend_from_slice(&rg.row_id_min.to_le_bytes());
        }
        for rg in &self.row_groups {
            buf.extend_from_slice(&rg.row_id_max.to_le_bytes());
        }
        for b in &self.data_boundaries {
            buf.extend_from_slice(&b.to_le_bytes());
        }
        for rg in &self.row_groups {
            for (offset, length) in &rg.col_ranges {
                buf.extend_from_slice(&offset.to_le_bytes());
                buf.extend_from_slice(&length.to_le_bytes());
            }
        }

        let crc_end = buf.len();
        buf.extend_from_slice(&0u32.to_le_bytes());
        let total = buf.len() as u64;
        buf[OFF_IM_FILE_SIZE..OFF_IM_FILE_SIZE + 8].copy_from_slice(&total.to_le_bytes());
        let crc = crc32fast::hash(&buf[IM_CRC_AREA_OFF..crc_end]);
        buf[crc_end..crc_end + 4].copy_from_slice(&crc.to_le_bytes());
        Ok(buf)
    }
}

fn align_to_8(buf: &mut Vec<u8>) {
    while buf.len() % 8 != 0 {
        buf.push(0);
    }
}
```

- [ ] **Step 4: Implement the reader**

Append to the same file, still above the test module:

```rust
/// Zero-copy reader over a complete, committed `_im` buffer.
pub struct IndexMetaReader<'a> {
    data: &'a [u8],
    rg_first_key_off: usize,
    row_id_min_off: usize,
    row_id_max_off: usize,
    data_boundary_off: usize,
    col_range_off: usize,
    index_rg_count: usize,
    data_rg_count: usize,
    index_column_count: usize,
    key_count: u32,
    payload_kind: u32,
    im_file_size: u64,
}

impl<'a> IndexMetaReader<'a> {
    pub fn new(data: &'a [u8]) -> ParquetMetaResult<Self> {
        if data.len() < IM_HEADER_SIZE + IM_TRAILER_SIZE {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::Truncated));
        }
        let im_file_size = read_u64(data, OFF_IM_FILE_SIZE);
        if im_file_size as usize > data.len() || (im_file_size as usize) < IM_HEADER_SIZE + IM_TRAILER_SIZE
        {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::Truncated));
        }
        let version = read_u32(data, OFF_FORMAT_VERSION);
        if version != IM_FORMAT_VERSION {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::VersionMismatch {
                found: version,
                expected: IM_FORMAT_VERSION,
            }));
        }
        let required = read_u64(data, OFF_FEATURE_FLAGS) & 0xFFFF_FFFF_0000_0000;
        if required != 0 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::UnsupportedFeature { flags: required }
            ));
        }
        let end = im_file_size as usize;
        let crc_end = end - IM_TRAILER_SIZE;
        let stored = read_u32(data, crc_end);
        let computed = crc32fast::hash(&data[IM_CRC_AREA_OFF..crc_end]);
        if stored != computed {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::ChecksumMismatch {
                stored,
                computed
            }));
        }

        let index_rg_count = read_u32(data, OFF_INDEX_RG_COUNT) as usize;
        let data_rg_count = read_u32(data, OFF_DATA_RG_COUNT) as usize;
        let index_column_count = read_u32(data, OFF_INDEX_COLUMN_COUNT) as usize;

        let rg_first_key_off = IM_HEADER_SIZE;
        let after_keys = rg_first_key_off + (index_rg_count + 1) * 4;
        let row_id_min_off = after_keys.next_multiple_of(8);
        let row_id_max_off = row_id_min_off + index_rg_count * 8;
        let data_boundary_off = row_id_max_off + index_rg_count * 8;
        let col_range_off = data_boundary_off + (data_rg_count + 1) * 8;
        let needed = col_range_off + index_rg_count * index_column_count * 16 + IM_TRAILER_SIZE;
        if needed > end {
            return Err(parquet_meta_err!(ParquetMetaErrorKind::Truncated));
        }

        Ok(Self {
            data,
            rg_first_key_off,
            row_id_min_off,
            row_id_max_off,
            data_boundary_off,
            col_range_off,
            index_rg_count,
            data_rg_count,
            index_column_count,
            key_count: read_u32(data, OFF_KEY_COUNT),
            payload_kind: read_u32(data, OFF_PAYLOAD_KIND),
            im_file_size,
        })
    }

    pub fn column_byte_range(&self, row_group: usize, column: usize) -> (u64, u64) {
        let base = self.col_range_off + (row_group * self.index_column_count + column) * 16;
        (read_u64(self.data, base), read_u64(self.data, base + 8))
    }

    pub fn data_row_group_boundary(&self, i: usize) -> i64 {
        read_u64(self.data, self.data_boundary_off + i * 8) as i64
    }

    pub fn data_row_group_count(&self) -> usize {
        self.data_rg_count
    }

    pub fn im_file_size(&self) -> u64 {
        self.im_file_size
    }

    pub fn index_column_count(&self) -> usize {
        self.index_column_count
    }

    pub fn index_row_group_count(&self) -> usize {
        self.index_rg_count
    }

    pub fn key_count(&self) -> u32 {
        self.key_count
    }

    pub fn payload_kind(&self) -> u32 {
        self.payload_kind
    }

    /// Inclusive `(rg_lo, rg_hi)` range of index row groups holding `key`,
    /// or `None` when `key` is outside the covered key space.
    pub fn row_group_range_for_key(&self, key: u32) -> Option<(usize, usize)> {
        if key >= self.key_count || self.index_rg_count == 0 {
            return None;
        }
        let first_key = |i: usize| read_u32(self.data, self.rg_first_key_off + i * 4);
        let n = self.index_rg_count;

        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if first_key(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let rg_lo = if lo < n && first_key(lo) == key {
            lo
        } else if lo == 0 {
            return None;
        } else {
            lo - 1
        };

        let mut ulo = 0usize;
        let mut uhi = n;
        while ulo < uhi {
            let mid = (ulo + uhi) / 2;
            if first_key(mid) <= key {
                ulo = mid + 1;
            } else {
                uhi = mid;
            }
        }
        Some((rg_lo, ulo - 1))
    }

    pub fn row_id_max(&self, row_group: usize) -> i64 {
        read_u64(self.data, self.row_id_max_off + row_group * 8) as i64
    }

    pub fn row_id_min(&self, row_group: usize) -> i64 {
        read_u64(self.data, self.row_id_min_off + row_group * 8) as i64
    }
}

fn read_u32(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
}

fn read_u64(data: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(data[off..off + 8].try_into().unwrap())
}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo test --lib -p qdb-parquet-meta index_meta
```

Expected: all 8 tests PASS.

- [ ] **Step 6: Run the four mandatory Rust checks**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt
cargo check --all-targets
cargo clippy --all-targets
cargo test --lib
```

Expected: zero errors and zero warnings from all four.

- [ ] **Step 7: Commit**

```bash
cd ~/claude/wt/pidx-parquet
git add core/rust/qdb-parquet-meta/src/index_meta.rs core/rust/qdb-parquet-meta/src/lib.rs
git commit -m "feat(idx): add _im covering index metadata format"
```

---

## Task 3: `_im` JNI bindings and Java reader

Exposes the Rust writer to Java and adds the memory-mapped Java reader that Phase 2's `ParquetPostingIndexReader` will sit on. Tests build a real `_im` buffer with the Rust writer and read it back through the Java reader, so the two implementations are pinned against each other rather than against a hand-rolled fixture.

**Files:**
- Create: `core/rust/qdbr/src/parquet_metadata/jni/index_writer.rs`
- Create: `core/src/main/java/io/questdb/cairo/IndexMetaFileWriter.java`
- Create: `core/src/main/java/io/questdb/cairo/IndexMetaFileReader.java`
- Create: `core/src/test/java/io/questdb/test/cairo/IndexMetaFileReaderTest.java`
- Modify: `core/rust/qdbr/src/parquet_metadata/jni/mod.rs`

**Interfaces:**
- Consumes: `IndexMetaWriter` / `IndexMetaReader` from Task 2.
- Produces: `IndexMetaFileWriter` natives `create()`, `setPayload(long writerPtr, int payloadKind, int keyCount)`, `addRowGroup(long writerPtr, int firstKey, long rowIdMin, long rowIdMax, long colRangesPtr, int colCount)`, `setDataRowGroupBoundaries(long writerPtr, long boundariesPtr, int count)`, `finish(long writerPtr)`, `resultDataPtr(long resultPtr)`, `resultDataLen(long resultPtr)`, `destroyResult(long resultPtr)`, `destroyWriter(long writerPtr)`; and `IndexMetaFileReader` with `openAndMapRO(FilesFacade ff, LPSZ path, IndexMetaFileReader reader)`, `ofAddress(long addr, long size)`, `getIndexRowGroupCount()`, `getDataRowGroupCount()`, `getIndexColumnCount()`, `getKeyCount()`, `getPayloadKind()`, `getRowGroupLoForKey(int key)`, `getRowGroupHiForKey(int key)`, `getRowIdMin(int rowGroup)`, `getRowIdMax(int rowGroup)`, `getDataRowGroupBoundary(int i)`, `getColumnByteRangeOffset(int rowGroup, int column)`, `getColumnByteRangeLength(int rowGroup, int column)`, `clear()`, `close()`. `getRowGroupLoForKey` returns `-1` when the key is not covered.

Model the JNI file on `core/rust/qdbr/src/parquet_metadata/jni/writer.rs` and the Java wrapper on `core/src/main/java/io/questdb/cairo/ParquetMetaFileWriter.java`; both already implement the create/populate/finish/destroy pointer lifecycle this task repeats, including the `check_not_null!` guard and the `Os.init()` static block.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/IndexMetaFileReaderTest.java` (Apache-2.0 header copied from a neighbouring file in the same package):

```java
package io.questdb.test.cairo;

import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.IndexMetaFileWriter;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class IndexMetaFileReaderTest extends AbstractCairoTest {

    @Test
    public void testKeyPackedIntoSharedRowGroup() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(0, reader.getRowGroupLoForKey(5));
            Assert.assertEquals(0, reader.getRowGroupHiForKey(5));
            Assert.assertEquals(3, reader.getRowGroupLoForKey(11_404));
            Assert.assertEquals(3, reader.getRowGroupHiForKey(11_404));
        }));
    }

    @Test
    public void testKeyOutOfRangeReturnsMinusOne() throws Exception {
        assertMemoryLeak(() -> withSample(reader ->
                Assert.assertEquals(-1, reader.getRowGroupLoForKey(11_405))));
    }

    @Test
    public void testKeySpanningMultipleRowGroups() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(1, reader.getRowGroupLoForKey(11_403));
            Assert.assertEquals(2, reader.getRowGroupHiForKey(11_403));
        }));
    }

    @Test
    public void testRoundTripHeaderFields() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(0, reader.getPayloadKind());
            Assert.assertEquals(11_405, reader.getKeyCount());
            Assert.assertEquals(4, reader.getIndexRowGroupCount());
            Assert.assertEquals(2, reader.getDataRowGroupCount());
            Assert.assertEquals(2, reader.getIndexColumnCount());
        }));
    }

    @Test
    public void testZoneMapsAndByteRanges() throws Exception {
        assertMemoryLeak(() -> withSample(reader -> {
            Assert.assertEquals(100_000, reader.getRowIdMin(1));
            Assert.assertEquals(157_999, reader.getRowIdMax(1));
            Assert.assertEquals(484, reader.getColumnByteRangeOffset(2, 1));
            Assert.assertEquals(80, reader.getColumnByteRangeLength(2, 1));
            Assert.assertEquals(0, reader.getDataRowGroupBoundary(0));
            Assert.assertEquals(1_000_000, reader.getDataRowGroupBoundary(2));
        }));
    }

    private static void addRowGroup(long writerPtr, int firstKey, long lo, long hi, long o0, long l0, long o1, long l1) {
        long ranges = Unsafe.malloc(4 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putLong(ranges, o0);
            Unsafe.getUnsafe().putLong(ranges + 8, l0);
            Unsafe.getUnsafe().putLong(ranges + 16, o1);
            Unsafe.getUnsafe().putLong(ranges + 24, l1);
            IndexMetaFileWriter.addRowGroup(writerPtr, firstKey, lo, hi, ranges, 2);
        } finally {
            Unsafe.free(ranges, 4 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void withSample(SampleAssertion assertion) {
        long writerPtr = IndexMetaFileWriter.create();
        long resultPtr = 0;
        try {
            IndexMetaFileWriter.setPayload(writerPtr, 0, 11_405);
            addRowGroup(writerPtr, 0, 0, 99_999, 4, 100, 104, 200);
            addRowGroup(writerPtr, 11_403, 100_000, 157_999, 304, 50, 354, 60);
            addRowGroup(writerPtr, 11_403, 158_000, 240_000, 414, 70, 484, 80);
            addRowGroup(writerPtr, 11_404, 240_001, 999_999, 564, 90, 654, 10);
            long boundaries = Unsafe.malloc(3 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putLong(boundaries, 0L);
                Unsafe.getUnsafe().putLong(boundaries + 8, 500_000L);
                Unsafe.getUnsafe().putLong(boundaries + 16, 1_000_000L);
                IndexMetaFileWriter.setDataRowGroupBoundaries(writerPtr, boundaries, 3);
            } finally {
                Unsafe.free(boundaries, 3 * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            }
            resultPtr = IndexMetaFileWriter.finish(writerPtr);
            try (IndexMetaFileReader reader = new IndexMetaFileReader()) {
                reader.ofAddress(
                        IndexMetaFileWriter.resultDataPtr(resultPtr),
                        IndexMetaFileWriter.resultDataLen(resultPtr)
                );
                assertion.run(reader);
            }
        } finally {
            if (resultPtr != 0) {
                IndexMetaFileWriter.destroyResult(resultPtr);
            }
            IndexMetaFileWriter.destroyWriter(writerPtr);
        }
    }

    @FunctionalInterface
    private interface SampleAssertion {
        void run(IndexMetaFileReader reader);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd ~/claude/wt/pidx-parquet
mvn -q -pl core -Dtest=IndexMetaFileReaderTest test
```

Expected: compilation failure — `package io.questdb.cairo does not contain IndexMetaFileWriter`.

- [ ] **Step 3: Write the JNI bindings**

Create `core/rust/qdbr/src/parquet_metadata/jni/index_writer.rs` with the Apache-2.0 header copied from `mod.rs` in the same directory, then:

```rust
//! JNI bindings for `IndexMetaFileWriter` (Java class `io.questdb.cairo.IndexMetaFileWriter`).
//!
//! These `extern "system"` functions are called from Java via JNI. Raw pointer
//! parameters are null-checked via the `check_not_null!` macro before
//! dereferencing, so the functions are safe in practice but cannot be marked
//! `unsafe` because they must match the JNI calling convention.
#![allow(clippy::not_unsafe_ptr_arg_deref)]

use crate::parquet::error::fmt_err;
use crate::parquet_metadata::index_meta::IndexMetaWriter;
use jni::objects::JClass;
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use std::slice;

/// Holds the finished _im file bytes.
pub struct IndexMetaBuiltFile {
    data: Vec<u8>,
}

macro_rules! check_not_null {
    ($env:expr, $ptr:expr, $name:expr) => {
        if $ptr.is_null() {
            let err = fmt_err!(InvalidType, concat!($name, " pointer is null"));
            return err.into_cairo_exception().throw($env);
        }
    };
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_addRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    first_key: jint,
    row_id_min: jlong,
    row_id_max: jlong,
    col_ranges_ptr: *const u64,
    col_count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, col_ranges_ptr, "IndexMetaFileWriter col ranges");
    let writer = unsafe { &mut *ptr };
    let raw = unsafe { slice::from_raw_parts(col_ranges_ptr, (col_count as usize) * 2) };
    let ranges: Vec<(u64, u64)> = raw.chunks_exact(2).map(|c| (c[0], c[1])).collect();
    writer.add_row_group(first_key as u32, row_id_min, row_id_max, &ranges);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_create(
    _env: JNIEnv,
    _class: JClass,
) -> *mut IndexMetaWriter {
    Box::into_raw(Box::new(IndexMetaWriter::new(0, 0)))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_destroyResult(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaBuiltFile,
) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_destroyWriter(
    _env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
) {
    if !ptr.is_null() {
        drop(unsafe { Box::from_raw(ptr) });
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_finish(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
) -> *mut IndexMetaBuiltFile {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    let writer = unsafe { &*ptr };
    match writer.finish() {
        Ok(data) => Box::into_raw(Box::new(IndexMetaBuiltFile { data })),
        Err(err) => {
            let mut err: crate::parquet::error::ParquetError = err.into();
            err.into_cairo_exception().throw(env);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_resultDataLen(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const IndexMetaBuiltFile,
) -> jlong {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaBuiltFile");
    let result = unsafe { &*ptr };
    result.data.len() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_resultDataPtr(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *const IndexMetaBuiltFile,
) -> *const u8 {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaBuiltFile");
    let result = unsafe { &*ptr };
    result.data.as_ptr()
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_setDataRowGroupBoundaries(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    boundaries_ptr: *const i64,
    count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    check_not_null!(env, boundaries_ptr, "IndexMetaFileWriter boundaries");
    let writer = unsafe { &mut *ptr };
    let boundaries = unsafe { slice::from_raw_parts(boundaries_ptr, count as usize) };
    writer.set_data_row_group_boundaries(boundaries);
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_cairo_IndexMetaFileWriter_setPayload(
    mut env: JNIEnv,
    _class: JClass,
    ptr: *mut IndexMetaWriter,
    payload_kind: jint,
    key_count: jint,
) {
    let env = &mut env;
    check_not_null!(env, ptr, "IndexMetaFileWriter");
    let writer = unsafe { &mut *ptr };
    writer.set_payload(payload_kind as u32, key_count as u32);
}
```

The `check_not_null!` macro is declared locally here exactly as it is in `writer.rs` — `macro_rules!` in a sibling module is not in scope, so it must be repeated rather than imported.

Note that the `_env`-unused variants (`create`, `destroyWriter`, `destroyResult`) take `_env: JNIEnv` by value with no `let env = &mut env;` line, matching `writer.rs`.

Declare the module in `core/rust/qdbr/src/parquet_metadata/jni/mod.rs`, after `pub mod converter;` so the list stays alphabetical:

```rust
pub mod index_writer;
```

- [ ] **Step 4: Write the Java JNI wrapper**

Create `core/src/main/java/io/questdb/cairo/IndexMetaFileWriter.java`, with members in alphabetical order:

```java
package io.questdb.cairo;

import io.questdb.std.Os;

/**
 * JNI wrapper for the Rust _im index metadata file writer.
 * Builds an _im file in memory using the Rust writer implementation.
 * <p>
 * The result is a native memory buffer holding the complete _im file bytes,
 * with IM_FILE_SIZE already patched into the header at offset 0. The caller
 * accesses the data via {@link #resultDataPtr} and {@link #resultDataLen},
 * and must call {@link #destroyResult} when done.
 */
public class IndexMetaFileWriter {

    public static native void addRowGroup(long writerPtr, int firstKey, long rowIdMin, long rowIdMax, long colRangesPtr, int colCount) throws CairoException;

    public static native long create();

    public static native void destroyResult(long resultPtr);

    public static native void destroyWriter(long writerPtr);

    public static native long finish(long writerPtr) throws CairoException;

    public static native long resultDataLen(long resultPtr);

    public static native long resultDataPtr(long resultPtr);

    public static native void setDataRowGroupBoundaries(long writerPtr, long boundariesPtr, int count);

    public static native void setPayload(long writerPtr, int payloadKind, int keyCount);

    static {
        Os.init();
    }
}
```

- [ ] **Step 5: Write the Java reader**

Create `core/src/main/java/io/questdb/cairo/IndexMetaFileReader.java` implementing `QuietCloseable`, with members in alphabetical order. It must:

- hold `addr`, `size`, and the five section offsets as instance fields, plus an `fd` and a mapped-length field for the `openAndMapRO` path so `close()` can unmap;
- `openAndMapRO(FilesFacade ff, LPSZ path, IndexMetaFileReader reader)` maps `IM_HEADER_SIZE` bytes, reads `IM_FILE_SIZE` from offset 0, and remaps to exactly that size — it must never call `ff.length()` to bound the mapping, per the spec's commit-boundary rule;
- `ofAddress(long addr, long size)` binds to an already-mapped buffer without owning it, for the test path above;
- validate `FORMAT_VERSION == 1`, reject any set bit in the required half of `FEATURE_FLAGS` (`flags & 0xFFFF_FFFF_0000_0000L`), and verify the CRC32 over `[8, IM_FILE_SIZE - 4)` before exposing any accessor, throwing `CairoException.critical(0)` with an ASCII message naming the field on failure;
- compute section offsets exactly as the Rust reader does. This is the part most likely to drift between the two implementations, so write it verbatim:

```java
        this.indexRowGroupCount = Unsafe.getUnsafe().getInt(addr + OFF_INDEX_RG_COUNT);
        this.dataRowGroupCount = Unsafe.getUnsafe().getInt(addr + OFF_DATA_RG_COUNT);
        this.indexColumnCount = Unsafe.getUnsafe().getInt(addr + OFF_INDEX_COLUMN_COUNT);
        this.rgFirstKeyOffset = IM_HEADER_SIZE;
        final long afterKeys = rgFirstKeyOffset + (indexRowGroupCount + 1L) * Integer.BYTES;
        this.rowIdMinOffset = (afterKeys + 7) & ~7L;
        this.rowIdMaxOffset = rowIdMinOffset + indexRowGroupCount * (long) Long.BYTES;
        this.dataBoundaryOffset = rowIdMaxOffset + indexRowGroupCount * (long) Long.BYTES;
        this.colRangeOffset = dataBoundaryOffset + (dataRowGroupCount + 1L) * Long.BYTES;
        final long needed = colRangeOffset
                + indexRowGroupCount * (long) indexColumnCount * 16L
                + IM_TRAILER_SIZE;
        if (needed > size) {
            throw CairoException.critical(0)
                    .put("_im file truncated [needed=").put(needed)
                    .put(", size=").put(size).put(']');
        }
```

  `(afterKeys + 7) & ~7L` is the Java spelling of the Rust reader's `next_multiple_of(8)`; both must agree or every offset after `RG_FIRST_KEY` shifts;
- implement `getRowGroupLoForKey` / `getRowGroupHiForKey` with the same lower-bound / upper-bound rule as the Rust reader, returning `-1` from both when `key >= keyCount`, when the row group count is zero, or when the lower bound falls before the first row group.

Mirror `ParquetMetaFileReader` for the mmap and lifecycle idioms — it already does the map-header-then-remap dance this reader repeats.

- [ ] **Step 6: Build the native library and run the tests**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt && cargo check --all-targets && cargo clippy --all-targets && cargo test --lib
cd ~/claude/wt/pidx-parquet
mvn -q -pl core -Dtest=IndexMetaFileReaderTest test
```

Expected: all four Rust checks clean; all 5 Java tests PASS with no memory-leak failures.

- [ ] **Step 7: Commit**

```bash
cd ~/claude/wt/pidx-parquet
git add core/rust/qdbr/src/parquet_metadata/jni/index_writer.rs \
        core/rust/qdbr/src/parquet_metadata/jni/mod.rs \
        core/rust/qdb-parquet-meta/src/index_meta.rs \
        core/src/main/java/io/questdb/cairo/IndexMetaFileWriter.java \
        core/src/main/java/io/questdb/cairo/IndexMetaFileReader.java \
        core/src/test/java/io/questdb/test/cairo/IndexMetaFileReaderTest.java
git commit -m "feat(idx): add _im JNI writer and Java reader"
```

---

## Task 4: Caller-driven Parquet row-group flush

`PartitionEncoder.createStreamingParquetWriter` takes a fixed `rowGroupSize` and the Rust side closes a row group on that row count. Key-aligned boundaries need the caller to close a row group at a chosen point instead. This task adds that primitive; nothing consumes it until Phase 2.

**Files:**
- Modify: `core/rust/qdbr/src/parquet_write/file.rs`
- Modify: `core/rust/qdbr/src/parquet_write/jni.rs`
- Modify: `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java`
- Test: `core/src/test/java/io/questdb/test/griffin/engine/table/parquet/ParquetRowGroupFlushTest.java`

**Interfaces:**
- Consumes: the existing streaming writer handle returned by `PartitionEncoder.createStreamingParquetWriter`.
- Produces: `PartitionEncoder.flushRowGroup(long writerPtr)`, which closes the current row group immediately if it holds at least one row and is a no-op otherwise. Phase 2's write path calls it at each key boundary once the accumulated row count reaches the configured target.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/griffin/engine/table/parquet/ParquetRowGroupFlushTest.java` (Apache-2.0 header copied from a neighbouring file in the same package). The test writes a table, encodes it through the streaming writer while calling `flushRowGroup` after chunks of 3 and 5 rows, then reads the resulting file back with `ParquetMetaFileReader` and asserts the row group sizes are exactly `3, 5` rather than a single group of 8. Model the streaming-writer setup and the read-back on whichever existing test in `core/src/test/java/io/questdb/test/griffin/engine/table/parquet/` already drives `createStreamingParquetWriter` and `writeStreamingParquetChunk`; reuse its fixture verbatim so this test differs only in the added `flushRowGroup` calls and the row-group-size assertions:

```java
        Assert.assertEquals(2, meta.getRowGroupCount());
        Assert.assertEquals(3, meta.getRowGroupSize(0));
        Assert.assertEquals(5, meta.getRowGroupSize(1));
```

If no such test exists, build the fixture from `PartitionEncoder.populateFromTableReader` plus `createStreamingParquetWriter`, passing a `rowGroupSize` of `1_000_000` so the fixed-size path cannot itself split the 8 rows — otherwise the assertion would pass without `flushRowGroup` doing anything, and the test would prove nothing.

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd ~/claude/wt/pidx-parquet
mvn -q -pl core -Dtest=ParquetRowGroupFlushTest test
```

Expected: compilation failure — `cannot find symbol: method flushRowGroup(long)`.

- [ ] **Step 3: Implement the Rust flush**

In `core/rust/qdbr/src/parquet_write/file.rs`, add a public method to the streaming writer type that closes the current row group. It must return `Ok(())` without writing anything when the pending row count is zero, so a caller that flushes twice in a row cannot emit an empty row group — the Parquet spec permits empty row groups but `ParquetMetaFileReader` treats a zero-row group as corruption. Reuse the same internal path the fixed-`rowGroupSize` threshold already takes when it closes a group, rather than duplicating the close logic.

- [ ] **Step 4: Add the JNI entry point**

In `core/rust/qdbr/src/parquet_write/jni.rs`, add `Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_flushRowGroup`, following the surrounding entry points' conventions for pointer validation and error propagation into `CairoException`.

- [ ] **Step 5: Declare the Java binding**

In `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java`, add the declaration in the correct alphabetical position among the existing `public static native` members — it sorts between `finishStreamingParquetWrite` and `populateEmptyPartition`:

```java
    public static native void flushRowGroup(long writerPtr) throws CairoException;
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt && cargo check --all-targets && cargo clippy --all-targets && cargo test --lib
cd ~/claude/wt/pidx-parquet
mvn -q -pl core -Dtest=ParquetRowGroupFlushTest test
```

Expected: all four Rust checks clean; the Java test PASSES with row group sizes `3, 5`.

- [ ] **Step 7: Verify the test can fail**

Temporarily comment out the two `flushRowGroup` calls in the test and re-run it. Expected: FAIL with one row group of 8 rows. Restore the calls and re-run to confirm PASS. A boundary test that passes without the primitive under test would be worthless; this step proves it does not.

- [ ] **Step 8: Commit**

```bash
cd ~/claude/wt/pidx-parquet
git add core/rust/qdbr/src/parquet_write/file.rs \
        core/rust/qdbr/src/parquet_write/jni.rs \
        core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionEncoder.java \
        core/src/test/java/io/questdb/test/griffin/engine/table/parquet/ParquetRowGroupFlushTest.java
git commit -m "feat(parquet): add caller-driven row group flush to streaming writer"
```

---

## Phase 1 Completion Check

Run once all four tasks are committed:

```bash
cd ~/claude/wt/pidx-parquet/core/rust/qdbr
cargo fmt && cargo check --all-targets && cargo clippy --all-targets && cargo test --lib
cd ~/claude/wt/pidx-parquet
mvn -q -pl core -Dtest='PropServerConfigurationTest,IndexMetaFileReaderTest,ParquetRowGroupFlushTest' test
mvn -q -pl core -Dtest='PostingIndex*Test,Covering*Test' test
```

The second Java command is the regression gate: Phase 1 changes no existing behaviour, so the whole posting and covering suite must stay green. If it does not, the cause is in Phase 1's diff, not a pre-existing flake — confirm by re-running the same command on `origin/master` before concluding otherwise.
