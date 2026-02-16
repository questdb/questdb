# O3 Commit with Parquet Partitions - Analysis

## Overview

This document describes how QuestDB handles Out-of-Order (O3) commits when a table partition is in Parquet format, and
identifies the "snowballing" problem where row groups grow unboundedly.

## Code Flow

### Entry Point

The flow starts in `O3PartitionJob.java:356` where the code checks if the partition is in Parquet format:

```java
if(isParquet){

processParquetPartition(...);
    return;
            }
```

### The Core Algorithm (`processParquetPartition`)

Location: `O3PartitionJob.java:72-321`

The algorithm at lines 147-266 works like this:

**1. Split O3 data by row group boundaries**

The O3 data range `[srcOooLo, srcOooHi]` is split into intervals based on the **minimum timestamp** of each existing row
group. For each row group starting from index 1, it
reads the row group's min timestamp and uses binary search to find the boundary:

```java
partitionDecoder.readRowGroupStats(rowGroupStatBuffers, parquetColumns, rowGroup);

final long min = rowGroupStatBuffers.getMinValueLong(0);
final long mergeRangeHi = Vect.boundedBinarySearchIndexT(
        sortedTimestampsAddr, min, mergeRangeLo, srcOooHi, BIN_SEARCH_SCAN_DOWN);
```

**2. Merge O3 data into the PREVIOUS row group**

Here's the key logic (lines 224-242):

```java
duplicateCount +=

mergeRowGroup(
    ...
    rowGroup -1,  // <-- merges into the PREVIOUS row group!
    ...
    mergeRangeLo,
    mergeRangeHi,
    ...
);

mergeRangeLo =mergeRangeHi +1;
```

**3. Handle the tail case (lines 245-266)**

Any remaining O3 data after processing all row group boundaries is merged into the **last row group**.

### Visual Representation

From the code comments:

```
+------+          <- rg0.min
| rg0  |  +-----+ <- srcOooLo
|      |  | OOO |
+------+  |     |
          |     |
+------+  |     | <- rg1.min
| rg1  |  |     |
|      |  |     |
+------+  |     |
          |     |
+------+  |     | <- rg2.min
| rg2  |  |     |
|      |  |     |
+------+  |     |
          |     |
          +-----+ <- srcOooHi
```

- On the first iteration, O3 range `[srcOooLo, rg1.min]` is merged into row group 0
- On the second iteration, O3 range `[rg1.min, rg2.min]` is merged into row group 1
- As a tail case, O3 range `[rg2.min, srcOooHi]` is merged into row group 2

## The Merging Process

Location: `O3PartitionJob.java:1533-1813` (`mergeRowGroup` method)

This is where the actual "snowballing" happens:

### Step 1: Decode the entire row group

```java
// Line 1571
decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, rowGroupIndex, 0,rowGroupSize);
```

The entire row group is decoded from Parquet into memory buffers.

### Step 2: Create a merge index

```java
// Lines 1583-1591
timestampMergeIndexAddr =

createMergeIndex(
        timestampDataPtr,        // existing row group timestamps
        sortedTimestampsAddr,    // O3 timestamps
    0,rowGroupSize -1,     // row group range
        mergeRangeLo, mergeRangeHi,  // O3 range
        timestampMergeIndexSize
        );
```

This creates an index that describes how to interleave the existing row group data with the O3 data in timestamp order.

### Step 3: Calculate merged row count

```java
// Lines 1576-1577
long mergeBatchRowCount = mergeRangeHi - mergeRangeLo + 1;
long mergeRowCount = mergeBatchRowCount + rowGroupSize;  // <-- keeps growing!
```

### Step 4: Merge all columns

For each column (lines 1682-1805):

- Allocate new memory for the merged data
- Call `O3CopyJob.mergeCopy()` to interleave old and new data according to the merge index
- Build a `PartitionDescriptor` with the merged data

### Step 5: Replace the row group in the Parquet file

```java
// Line 1807
partitionUpdater.updateRowGroup((short) rowGroupIndex,partitionDescriptor);
```

## Native Layer (Rust)

### PartitionUpdater

Location: `core/rust/qdbr/src/parquet_write/update.rs`

```rust
pub fn replace_row_group(&mut self, partition: &Partition, row_group_id: i16) -> ParquetResult<()> {
    let options = self.row_group_options();
    let row_group = create_row_group(
        partition,
        0,
        partition.columns[0].row_count,
        self.parquet_file.schema().fields(),
        &to_encodings(partition),
        options,
        false,
    )?;

    self.parquet_file.replace(row_group, Some(row_group_id))
}
```

This rewrites the row group in place in the Parquet file.

### JNI Interface

Location: `core/rust/qdbr/src/parquet_write/jni.rs:124-182`

The `updateRowGroup` JNI function calls `parquet_updater.replace_row_group()` when a row group ID is provided.

## The Snowball Problem

### The Issue

**Every O3 commit rewrites the affected row groups, which grow unboundedly.**

Consider this scenario:

1. Initial state: Parquet file with 1 row group containing 100K rows
2. O3 commit #1: Insert 1K rows → row group now has 101K rows (decoded + merged + rewritten)
3. O3 commit #2: Insert 1K rows → row group now has 102K rows (decoded + merged + rewritten)
4. O3 commit #N: ...

### Impact

**Each O3 commit**:

- Decodes the entire row group (which grows each time)
- Merges the new data
- Rewrites the entire row group

The row group size is **not bounded** by `rowGroupSize` configuration during merges - the config is only used during
initial encoding. The `mergeRowGroup` function simply concatenates:

```java
long mergeRowCount = mergeBatchRowCount + rowGroupSize;  // keeps growing!
```

### Consequences

1. **Memory usage**: Each O3 commit needs to hold the entire decoded row group + O3 data in memory
2. **I/O amplification**: The entire row group is re-encoded and rewritten for each O3 commit
3. **Growing row groups**: Row groups can become arbitrarily large, far exceeding the configured `rowGroupSize`
4. **Performance degradation**: As row groups grow, each subsequent O3 commit takes longer

### Why This is Different from Native Partitions

For native (non-Parquet) partitions, O3 commits can:

- Append to existing column files
- Split partitions when they get too large
- Write only the affected data

But for Parquet partitions, the entire row group must be rewritten because Parquet row groups are immutable units - you
can't append to them.

## Key Files

| File                                                                                  | Description                             |
|---------------------------------------------------------------------------------------|-----------------------------------------|
| `core/src/main/java/io/questdb/cairo/O3PartitionJob.java`                             | Main O3 partition processing logic      |
| `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionUpdater.java`    | Java wrapper for native Parquet updater |
| `core/src/main/java/io/questdb/griffin/engine/table/parquet/PartitionDecoder.java`    | Parquet row group decoder               |
| `core/src/main/java/io/questdb/griffin/engine/table/parquet/RowGroupBuffers.java`     | Decoded row group data buffers          |
| `core/src/main/java/io/questdb/griffin/engine/table/parquet/RowGroupStatBuffers.java` | Row group statistics (min/max values)   |
| `core/rust/qdbr/src/parquet_write/update.rs`                                          | Rust implementation of ParquetUpdater   |
| `core/rust/qdbr/src/parquet_write/jni.rs`                                             | JNI bindings for Parquet operations     |

## Potential Solutions

To address the snowballing problem, possible approaches include:

1. **Row group splitting**: When a merged row group exceeds `rowGroupSize`, split it into multiple row groups
2. **Compaction strategy**: Defer merging and accumulate O3 data separately, then compact periodically
3. **Append-only with metadata**: Append new row groups instead of merging, track logical ordering in metadata
4. **Hybrid approach**: Use native format for active partitions, convert to Parquet only after they become cold

Each approach has trade-offs in terms of read performance, write amplification, and implementation complexity.

---

## Implementation Progress

### Phase 1: Extract Merge Strategy Logic (Completed)

Created `O3ParquetMergeStrategy.java` to separate the merge decision logic from execution.

#### New Class: `O3ParquetMergeStrategy`

Location: `core/src/main/java/io/questdb/cairo/O3ParquetMergeStrategy.java`

**Data Structures:**

```java
public enum ActionType {
    MERGE,              // Merge row group slice with O3 data
    COPY_ROW_GROUP_SLICE, // Copy row group slice as-is (no overlap)
    COPY_O3             // Copy O3 data as new row group (no overlap)
}

public static class MergeAction {
    public ActionType type;
    public int rowGroupIndex;  // -1 if COPY_O3
    public long rgLo, rgHi;    // Row range within row group (supports partial slices)
    public long o3Lo, o3Hi;    // O3 data range
}
```

**Row Group Bounds Storage:**

- Stored as triples `(min, max, rowCount)` in a `LongList`
- Helper methods: `addRowGroupBounds()`, `getRowGroupMin/Max/RowCount()`

**Algorithm (`computeMergeActions`):**

1. **True overlap detection** using both min AND max timestamps:
    - O3 data overlaps with row group if `o3Ts >= rgMin AND o3Ts <= rgMax`
    - O3 data in gaps between row groups is tracked separately

2. **Small row group threshold** (default 4096 rows):
    - If O3 data falls in a gap and an adjacent row group is "small", merge into that row group
    - Prefers merging into previous small row group, then next
    - If both adjacent row groups are large, emit `COPY_O3` (create new row group)

3. **Output actions in timestamp order:**
    - `COPY_O3` for gap data before each row group
    - `MERGE` or `COPY_ROW_GROUP_SLICE` for each row group
    - `COPY_O3` for gap data after the last row group

#### Modified: `O3PartitionJob.processParquetPartition()`

The merge loop now:

1. Builds row group bounds by reading stats for all row groups upfront
2. Calls `O3ParquetMergeStrategy.computeMergeActions()` to compute the plan
3. Iterates over actions and executes them

```java
// Build row group bounds
for(int rg = 0;
rg<rowGroupCount;rg++){
        partitionDecoder.

readRowGroupStats(rowGroupStatBuffers, parquetColumns, rg);
    O3ParquetMergeStrategy.

addRowGroupBounds(rowGroupBounds,
                  rowGroupStatBuffers.getMinValueLong(0),
        rowGroupStatBuffers.

getMaxValueLong(0),
        partitionDecoder.

metadata().

getRowGroupSize(rg));
        }

// Compute and execute merge actions
        O3ParquetMergeStrategy.

computeMergeActions(rowGroupBounds, sortedTimestampsAddr, srcOooLo, srcOooHi, mergeActions);
for(
MergeAction action :mergeActions){
        switch(action.type){
        case MERGE:

mergeRowGroup(...); break;
        case COPY_ROW_GROUP_SLICE: /* no-op for now */ break;
        case COPY_O3:throw new

UnsupportedOperationException("Not yet implemented");
    }
            }
```

#### New Test: `O3ParquetMergeStrategyTest`

Location: `core/src/test/java/io/questdb/test/cairo/O3ParquetMergeStrategyTest.java`

Test coverage:

- No row groups → `COPY_O3`
- Single row group with overlap → `MERGE`
- O3 in gap between large row groups → `COPY_O3`
- O3 in gap merged into small previous row group
- O3 in gap merged into small next row group
- O3 before/after all row groups
- Mixed overlap and gap scenarios
- Custom threshold testing

### Phase 2: Track Dead Space with `unused_bytes` (Completed)

When a row group is replaced in-place (update mode), the old data stays in the file as dead space. Phase 2
tracks this wasted space so the rewrite decision in Phase 3 has the data it needs.

#### Rust: `QdbMeta` with `unused_bytes`

Location: `core/rust/qdbr/src/parquet/qdb_metadata.rs`

- Added `unused_bytes: u64` field to `QdbMeta` (serialized as JSON in a parquet KV metadata entry under key `questdb`)
- `ParquetUpdater::replace_row_group()` now accumulates the old row group's compressed size + column/offset index sizes
  into `accumulated_unused_bytes`
- `ParquetUpdater::end()` adds the old footer size to `accumulated_unused_bytes`, serializes into the new footer's
  QDB metadata

#### Java: Expose `unused_bytes` via `PartitionDecoder`

- Added `unusedBytesOffset()` JNI function in `parquet_read/jni.rs`
- Added `UNUSED_BYTES_OFFSET` + `Metadata.getUnusedBytes()` to `PartitionDecoder.java`

### Phase 3: Parquet File Rewrite to Prevent File Growth (Completed)

This is the main change. When dead space in a parquet file exceeds a threshold, the O3 commit writes all data to a
**fresh file** instead of appending. Untouched row groups are raw-copied (memcpy of byte ranges, no decode/re-encode).

#### Rewrite Trigger

Evaluated in `O3PartitionJob.processParquetPartition()`:

```java
isRewrite =
rowGroupCount ==1
        ||(double)unusedBytes /parquetSize >rewriteUnusedRatio  // default 0.5
    ||unusedBytes >rewriteUnusedMaxBytes;                     // default 1 GB
```

A single-row-group file always triggers rewrite because the entire row group is re-encoded anyway and writing to a
fresh file avoids accumulating dead space.

#### Configuration Properties

| Property                                                      | Default | Description                                          |
|---------------------------------------------------------------|---------|------------------------------------------------------|
| `cairo.partition.encoder.parquet.o3.rewrite.unused.ratio`     | `0.5`   | Rewrite when `unused_bytes / file_size` exceeds this |
| `cairo.partition.encoder.parquet.o3.rewrite.unused.max.bytes` | `1 GB`  | Rewrite when absolute unused bytes exceeds this      |

Files: `PropertyKey.java`, `CairoConfiguration.java`, `DefaultCairoConfiguration.java`,
`CairoConfigurationWrapper.java`, `PropServerConfiguration.java`

#### Split Reader/Writer File Descriptors

The `PartitionUpdater` now takes **separate** reader and writer file descriptors. This is required because in rewrite
mode the reader points to the old file while the writer points to a new empty file. Even in update mode, separate
fds are needed so reader and writer maintain independent cursor positions.

**`PartitionUpdater.of()` new signature (Java):**

```java
void of(LPSZ srcPath, int fileOpenOpts,
        int readerFd, long readFileSize,     // old file for reading metadata/data
        int writerFd, long writeFileSize,    // new file (0 = rewrite) or same file
        int timestampIndex, long compressionCodec,
        boolean statisticsEnabled, boolean rawArrayEncoding,
        long rowGroupSize, long dataPageSize)
```

**`ParquetUpdater::new()` (Rust):**

- `write_file_size == 0` → rewrite mode: creates `ParquetFile` in `Mode::Write` (fresh file)
- `write_file_size > 0` → update mode: creates `ParquetFile` in `Mode::Update` (append), seeks writer to end

#### New Rust Operations

**`copy_row_group(rg_index)`** — raw-copies an entire row group from the old file to the new file:

1. Reads byte range covering all column chunks + indexes from the reader
2. Calls `ensure_started()` to write the PAR1 header before computing offsets (fixes an off-by-4 bug)
3. Computes offset delta between old and new file positions
4. Adjusts `data_page_offset`, `dictionary_page_offset`, `column_index_offset`, `offset_index_offset` in the thrift
   metadata
5. Writes raw bytes + adjusted metadata via `ParquetFile::write_raw_row_group()`

**`slice_row_group(rg_index, row_lo, row_hi)`** — copies a sub-range of rows from a row group:

1. Reads the entire old file into memory from the reader fd
2. Decodes the row range using `ParquetDecoder`
3. Extracts symbol tables from dictionary pages
4. Re-encodes via `replace_row_group()`

**Mode-aware dispatch in existing operations:**

- `replace_row_group()`: rewrite → `parquet_file.write()`, update → `parquet_file.replace()` + dead space tracking
- `insert_row_group()`: rewrite → `parquet_file.write()`, update → `parquet_file.insert()`
- `append_row_group()`: rewrite → `parquet_file.write()`, update → `parquet_file.append()`
- `end()`: rewrite → `unused_bytes = 0`, update → accumulates dead space

#### ParquetFile Extensions

Location: `core/rust/qdbr/parquet2/src/write/file.rs`

- **`insert()`**: new method for inserting a row group at a specific position (for `COPY_O3` actions)
- **`write_raw_row_group(raw_bytes, row_group)`**: writes pre-encoded bytes + metadata (for `copy_row_group`)
- **`ensure_started()`**: writes PAR1 header if not yet written, so `current_offset()` returns the correct position
- **`current_offset()`**: returns the current write offset
- **`end()` Mode::Update** reworked: two-phase processing (replacements first, then insertions in ascending position
  order), KV metadata merge-by-key instead of append

#### Java: File Lifecycle in `O3PartitionJob`

```
if (isRewrite) {
    readerFd → old file (srcNameTxn directory)
    writerFd → new file (txn directory), writeFileSize = 0
} else {
    readerFd → old file
    writerFd → same old file, writeFileSize = parquetSize
}

Execute merge actions:
  MERGE           → mergeRowGroup() → partitionUpdater.updateRowGroup()
  COPY_ROW_GROUP_SLICE:
    rewrite + full range  → partitionUpdater.copyRowGroup()
    rewrite + partial     → partitionUpdater.sliceRowGroup()
    update  + partial     → partitionUpdater.sliceRowGroup()
    update  + full range  → no-op (stays in place in metadata)
  COPY_O3         → copyO3ToRowGroup() → partitionUpdater.addRowGroup()

After updateFileMetadata():
  rewrite → updateParquetIndexes() on the new txn directory
  update  → updateParquetIndexes() on the original directory
```

**Error handling:**

- Rewrite failure: original file intact, new txn directory removed by inner catch block
- Update failure: original file truncated to `parquetSize` (restoring last known good state)

#### Java: `TableWriter.o3ConsumePartitionUpdateSink`

The sink consumer now receives a `parquetRewrite` flag (high int of the flags long):

```java
if(parquetRewrite){
        txWriter.

updatePartitionSizeAndTxnByRawIndex(...)  // bumps nameTxn
    partitionRemoveCandidates.

add(partitionTimestamp, srcNameTxn)  // old dir queued for removal
}else{
        txWriter.

updatePartitionSizeByRawIndex(...)  // keeps same nameTxn
}
```

#### Bug Fix: PAR1 Header Offset

When `copy_row_group()` is the first operation on a fresh rewrite file, `current_offset()` returned 0 but
`write_raw_row_group()` writes the 4-byte PAR1 magic header first, so data actually starts at offset 4. The metadata
offsets were computed relative to 0, causing "Invalid thrift: protocol error" when reading back.

Fix: call `ensure_started()` before `current_offset()` in `copy_row_group()` so the PAR1 header is written first and
offsets are computed correctly.

### Files Changed

| File                                         | Change                                                                                                                  |
|----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| `O3ParquetMergeStrategy.java`                | New: merge strategy with `MERGE`/`COPY_ROW_GROUP_SLICE`/`COPY_O3` actions                                               |
| `O3PartitionJob.java`                        | Rewrite decision, split fds, action dispatch, txn-based directory                                                       |
| `TableWriter.java`                           | Sink consumer: `parquetRewrite` flag, nameTxn bump, old dir removal                                                     |
| `PartitionUpdater.java`                      | Split reader/writer fds, `copyRowGroup()`, `sliceRowGroup()`                                                            |
| `PartitionDecoder.java`                      | `getUnusedBytes()` accessor                                                                                             |
| `PropertyKey.java`                           | 2 new rewrite threshold properties                                                                                      |
| `CairoConfiguration.java`                    | 2 new interface methods                                                                                                 |
| `DefaultCairoConfiguration.java`             | Default values (0.5 ratio, 1 GB absolute)                                                                               |
| `CairoConfigurationWrapper.java`             | Delegation                                                                                                              |
| `PropServerConfiguration.java`               | Parsing + getters                                                                                                       |
| `parquet_write/update.rs`                    | Split reader/writer, rewrite/update modes, `copy_row_group`, `slice_row_group`, `insert_row_group`, dead space tracking |
| `parquet_write/jni.rs`                       | Split fds in `create`, new `copyRowGroup`/`sliceRowGroup`/`insertRowGroup` handlers                                     |
| `parquet2/write/file.rs`                     | `insert()`, `write_raw_row_group()`, `ensure_started()`, `current_offset()`, two-phase `end()`                          |
| `parquet2/metadata/column_chunk_metadata.rs` | `column_index_offset()`, `offset_index_offset()` accessors                                                              |
| `parquet2/metadata/row_metadata.rs`          | Made `into_thrift()` public                                                                                             |
| `parquet/qdb_metadata.rs`                    | `QdbMeta` with `unused_bytes` serialization                                                                             |
| `parquet_read/jni.rs`                        | `unusedBytesOffset()` JNI function                                                                                      |
| `parquet_read/mod.rs`                        | `unused_bytes` field in `ParquetDecoder`                                                                                |
| `O3ParquetMergeStrategyTest.java`            | Unit tests for merge strategy                                                                                           |
| `O3ParquetMergeStrategyFuzzTest.java`        | Fuzz test: multi-round O3 on parquet partition                                                                          |
| `PartitionUpdaterTest.java`                  | Updated for split fd API                                                                                                |
