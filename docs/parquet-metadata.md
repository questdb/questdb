# Parquet partition metadata file

## Goals

- Per-partition metadata file stored alongside `data.parquet` in the partition directory.
- Give optional access to statistics about each row group so that they can be pruned by the query planner.
- Give access (mandatory) to the parquet partition file's byte ranges, encodings, and compression for each column chunk of each row group to be able to decode data pulled from cold storage without the parquet footer.
- Store the QuestDB-specific metadata currently written as JSON in the parquet footer, avoiding the need to parse the parquet footer for this information.
- Usable with external parquet files.

## Background: current QuestDB parquet metadata

Currently stored under the `"questdb"` key in the parquet file's key-value metadata as JSON. This will be replaced by the binary format below.

```json
{
  "version": 1,
  "schema": [
    { "column_type": 12, "column_top": 0, "format": 1 },
    { "column_type": 5, "column_top": 256 },
    { "column_type": 26, "column_top": 0, "ascii": true }
  ],
  "unused_bytes": 4096
}
```

## File format

Binary file encoded in little-endian. One file per partition, stored in the partition directory alongside `data.parquet`.

The file has a header with column descriptors, row group blocks in the middle, and a footer at the end. The footer ends with a 4-byte trailer that stores the footer length, allowing readers to locate the footer given only the file size. The `_pm` file size is stored in `_txn` field 3 for each partition. Row group blocks are referenced by offset from the footer. On update, only new/changed row group blocks are appended; the footer reuses offsets to unchanged blocks. The file is small (typically tens of KB), memory-mapped and cached in `TableReader`. Bloom filter bitsets are stored in the out-of-line region of each row group block, referenced by `BLOOM_FILTER_OFF` in the column chunk.

### Overview

```
                  _pm metadata file                         data.parquet
                 +========================+                  +==========================+
                 | HEADER                 |                  |                          |
                 |  version               |                  |  ...column chunks...     |
                 |  feature_flags         |                  |                          |
                 |  designated_timestamp  |         +------->|  dict page  | data pages |
                 |  sorting_column_count  |         |        |                          |
                 |  column_count          |         |        +==========================+
                 |                        |         |
                 | COLUMN DESCRIPTORS     |         |
                 |  col 0: name, type, .. |         |
                 |  col 1: name, type, .. |         |
                 |  ...                   |         |
                 |                        |         |
                 | SORTING COLUMNS        |         |
                 |  col indices           |         |
                 |                        |         |
                 | NAME STRINGS           |         |
                 |                        |         |
                 | HEADER FEATURE SECTIONS|         |
                 |  (if any flags set)    |         |
                 +------------------------+         |
                 | ROW GROUP BLOCK 0      |         |
                 |  num_rows              |         |
                 |  chunk col 0:          |         |
                 |    codec, encodings    |         |
                 |    byte_range_start  --+---------+
                 |    total_compressed    |
                 |    null_count          |
                 |    bloom_filter_off  --+--.
                 |    min_stat, max_stat  |  |
                 |  chunk col 1: ...      |  |
                 |  ...                   |  |
                 | (out-of-line stats)    |  |
                 | (bloom filters)      <----'
                 +------------------------+
                 | ROW GROUP BLOCK 1      |
                 |  ...                   |
                 +------------------------+
                 |  ...                   |
                 +------------------------+
                 | FOOTER                 |
                 |  parquet_footer_offset |
                 |  parquet_footer_length |
                 |  row_group_count       |
                 |  footer_feature_flags  |
                 |  entry 0: offset ------+--> ROW GROUP BLOCK 0
                 |  entry 1: offset ------+--> ROW GROUP BLOCK 1
                 |  ...                   |
                 |  FOOTER FEAT SECTIONS  |
                 |  CRC32                 |
                 |  FOOTER_LENGTH (4B)  --+--> footer start = file_size - 4 - FOOTER_LENGTH
  _txn field 3:  +========================+
  pm file size = total file size

```

**Update mode** - only changed blocks are appended; unchanged blocks are reused:

```
                 +========================+
                 | HEADER                 |
                 +------------------------+
                 | ROW GROUP BLOCK 0      |  <-- unchanged, kept in place
                 +------------------------+
                 | ROW GROUP BLOCK 1      |  <-- was merged, old data now dead
                 +------------------------+
                 | (old footer)           |  <-- dead, superseded
                 +------------------------+
                 | ROW GROUP BLOCK 1'     |  <-- new version of block 1
                 +------------------------+
                 | ROW GROUP BLOCK 2      |  <-- newly appended
                 +------------------------+
                 | FOOTER (new)           |
                 |  entry 0: offset ------+--> BLOCK 0  (old, reused)
                 |  entry 1: offset ------+--> BLOCK 1' (new)
                 |  entry 2: offset ------+--> BLOCK 2  (new)
                 |  CRC32                 |
                 |  FOOTER_LENGTH (4B)    |
  _txn field 3:  +========================+
  pm file size = total file size (new)

```

### Feature flags

Both the header and footer contain a `feature_flags` field (`u64`) that gates optional sections:

- **Bits 0-31**: optional - reader ignores unknown bits silently.
- **Bits 32-63**: required - reader rejects the file if unknown bits are set.

Feature sections are written in ascending bit order after their respective fixed-format region (name strings for the header, row group entries for the footer). Each feature's section size is resolvable from the feature spec and file metadata (e.g., `column_count`), so readers navigate sections sequentially without an offset table.

If a reader encounters an unknown optional bit that is set, it stops parsing further sections (it cannot compute the size to skip). The CRC is always locatable via `footer_length` from the trailer regardless of unknown content.

#### Header feature flags

| bit | name        | level | section size             | description                                   |
| --- | ----------- | ----- | ------------------------ | --------------------------------------------- |
| 0   | COLUMN_TOPS | file  | `column_count * 8` bytes | `[u64; column_count]` - column top per column |

When `COLUMN_TOPS` is set, the section contains one `u64` per column giving the row number where valid data begins (rows before that are null). When unset, all tops are 0. The section is only written when at least one column has a non-zero top.

#### Footer feature flags

None defined yet. The `footer_feature_flags` field is reserved for future use (always 0).

### File header

| offset | size | field                | type | description                                                             |
| ------ | ---- | -------------------- | ---- | ----------------------------------------------------------------------- |
| 0      | 4    | FILE_FORMAT_VERSION  | u32  |                                                                         |
| 4      | 8    | FEATURE_FLAGS        | u64  | header feature flags                                                    |
| 12     | 4    | DESIGNATED_TIMESTAMP | i32  | index of the designated timestamp in descriptors (or -1)                |
| 16     | 4    | SORTING_COLUMN_COUNT | u32  |                                                                         |
| 20     | 4    | COLUMN_COUNT         | u32  |                                                                         |
| 24     | ..   | COLUMN_DESCRIPTORS   |      | COLUMN_COUNT * Column descriptor (32B each)                             |
| ..     | ..   | SORTING_COLUMNS      |      | SORTING_COLUMN_COUNT * Sorting column (4B each)                         |
| ..     | ..   | NAME_STRINGS         |      | Column names, each `[utf8 bytes]`; length from descriptor's NAME_LENGTH |
| ..     | ..   | HEADER_FEAT_SECTIONS |      | Feature sections in ascending bit order                                 |

For a column to be the designated timestamp it must comply to these rules:
- It must be the first column in sorting columns, sorted in `ascending` order
- The column type must be `timestamp`
- The column repetition must be `required` (no nulls allowed)

### Column descriptor (32 bytes)

Per-column metadata. Written once in the header, applies across all row groups.

| offset | size | field          | type | description                                                                                                   |
| ------ | ---- | -------------- | ---- | ------------------------------------------------------------------------------------------------------------- |
| 0      | 8    | NAME_OFFSET    | u64  | offset from the file start to column name (utf-8 encoded, not null-terminated)                                |
| 8      | 4    | ID             | i32  | index of the column related to QuestDB schema (or -1)                                                         |
| 12     | 4    | TYPE           | i32  | QuestDB column type code                                                                                      |
| 16     | 4    | FLAGS          | i32  | Column flags                                                                                                  |
| 20     | 4    | FIXED_BYTE_LEN | i32  | For FIXED_LEN_BYTE_ARRAY physical type: the fixed length in bytes (matches parquet type_length). 0 otherwise. |
| 24     | 4    | NAME_LENGTH    | u32  | length of the column name in bytes                                                                            |
| 28     | 1    | PHYSICAL_TYPE  | u8   | Parquet physical type: 0=BOOLEAN, 1=INT32, 2=INT64, 3=INT96, 4=FLOAT, 5=DOUBLE, 6=BYTE_ARRAY, 7=FIXED_LEN_BA  |
| 29     | 1    | MAX_REP_LEVEL  | u8   | Maximum repetition level (0 for non-nested columns)                                                           |
| 30     | 1    | MAX_DEF_LEVEL  | u8   | Maximum definition level (0 for required, 1 for optional)                                                     |
| 31     | 1    | RESERVED       | u8   | Reserved, must be 0                                                                                           |

#### Column flags

| bit offset | bit size | field               | type | description                              |
| ---------- | -------- | ------------------- | ---- | ---------------------------------------- |
| 0          | 1        | LOCAL_KEY_IS_GLOBAL | i1   | Symbol                                   |
| 1          | 1        | IS_ASCII            | i1   | Varchar                                  |
| 2          | 2        | FIELD_REPETITION    | u2   | 0 = Required, 1 = Optional, 2 = Repeated |
| 4          | 1        | DESCENDING          | i1   | For sorted column, 1 = Descending        |
| 5          | 27       | RESERVED            |      | Reserved, must be 0                      |

### Sorting column (4 bytes)

Alignment: 4 bytes.

| offset | size | field | type | description                                          |
| ------ | ---- | ----- | ---- | ---------------------------------------------------- |
| 0      | 4    | INDEX | u32  | Ordinal position of the column in column descriptors |

### Row group blocks

Written sequentially after the header. Each block holds the column chunk metadata for one row group. Column chunks are fixed-size (64 bytes), so block size = 8 + COLUMN_COUNT * 64. On update, new/changed blocks are appended after the old footer; unchanged blocks are left in place and referenced by the new footer.

Blocks must be aligned to 8 bytes so that the offset in the footer can be stored as a u32 (actual offset = value << 3).

For types > 8 bytes (LONG128, UUID, LONG256), min/max stat values are stored out-of-line immediately after the column chunks. Bloom filter bitsets follow the out-of-line stats, each padded to 8-byte alignment so `BLOOM_FILTER_OFF` can use the shifted u32 encoding. All out-of-line data is part of the row group block and written together with it.

#### Row group block

| offset | size | field         | type | description                            |
| ------ | ---- | ------------- | ---- | -------------------------------------- |
| 0      | 8    | NUM_ROWS      | u64  |                                        |
| 8      | ..   | COLUMN_CHUNKS |      | COLUMN_COUNT * Column chunk (64B each) |

### Column chunk (64 bytes)

Per-column-chunk metadata needed to locate and decode data from the parquet file.

| offset | size | field            | type | description                                                                                                                       |
| ------ | ---- | ---------------- | ---- | --------------------------------------------------------------------------------------------------------------------------------- |
| 0      | 1    | CODEC            | u8   | parquet CompressionCodec enum: 0=UNCOMPRESSED, 1=SNAPPY, 2=GZIP, 3=LZO, 4=BROTLI, 5=LZ4, 6=ZSTD, 7=LZ4_RAW                        |
| 1      | 1    | ENCODINGS        | u8   | bitmask: bit 0=PLAIN, 1=RLE_DICTIONARY, 2=DELTA_BINARY_PACKED, 3=DELTA_LENGTH_BYTE_ARRAY, 4=DELTA_BYTE_ARRAY, 5=BYTE_STREAM_SPLIT |
| 2      | 1    | STAT_FLAGS       | u8   |                                                                                                                                   |
| 3      | 1    | STAT_SIZES       | u8   | low nibble = MIN_STAT byte size (inline only), high nibble = MAX_STAT byte size (inline only)                                     |
| 4      | 4    | BLOOM_FILTER_OFF | u32  | byte offset from _pm file start >> 3 (actual = value << 3); points to bloom filter bitset in the OOL region; 0 = absent           |
| 8      | 8    | NUM_VALUES       | u64  | total values (may differ from row count for arrays)                                                                               |
| 16     | 8    | BYTE_RANGE_START | u64  | byte offset in parquet file to chunk start (dictionary page offset if present, else data page offset)                             |
| 24     | 8    | TOTAL_COMPRESSED | u64  | total compressed bytes of all pages                                                                                               |
| 32     | 8    | NULL_COUNT       | u64  | number of nulls                                                                                                                   |
| 40     | 8    | DISTINCT_COUNT   | u64  | number of distinct values                                                                                                         |
| 48     | 8    | MIN_STAT         | u64  | min value (see STAT_KIND)                                                                                                         |
| 56     | 8    | MAX_STAT         | u64  | max value (see STAT_KIND)                                                                                                         |

#### STAT_FLAGS interpretation

| bit offset | bit size | field                  | type | description                            |
| ---------- | -------- | ---------------------- | ---- | -------------------------------------- |
| 0          | 1        | MIN_STAT_PRESENT       | i1   | Indicates if MIN_STAT is present       |
| 1          | 1        | MIN_STAT_INLINED       | i1   | Indicates if MIN_STAT is inlined       |
| 2          | 1        | MIN_STAT_VALUE_EXACT   | i1   | Indicates if MIN_STAT value is exact   |
| 3          | 1        | MAX_STAT_PRESENT       | i1   | Indicates if MAX_STAT is present       |
| 4          | 1        | MAX_STAT_INLINED       | i1   | Indicates if MAX_STAT is inlined       |
| 5          | 1        | MAX_STAT_VALUE_EXACT   | i1   | Indicates if MAX_STAT value is exact   |
| 6          | 1        | DISTINCT_COUNT_PRESENT | i1   | Indicates if DISTINCT_COUNT is present |
| 7          | 1        | NULL_COUNT_PRESENT     | i1   | Indicates if NULL_COUNT is present     |
| 8          | 24       | RESERVED               |      | Reserved, must be 0                    |

Column types with fixed size that are <= 8 bytes (BOOLEAN, BYTE, SHORT, CHAR, INT/FLOAT/IPv4, LONG/DOUBLE/DATE/TIMESTAMP) MUST have their min/max stats inlined in the column chunk. For variable-length types (VARCHAR/STRING) and fixed-size types > 8 bytes (LONG128, UUID, LONG256), min/max stats MAY be stored out-of-line immediately after the row group blocks that references them.

### Bloom filter bitset

Bloom filter bitsets are stored in the out-of-line region of each row group block, after out-of-line stats. Each bloom filter entry is padded to 8-byte alignment so `BLOOM_FILTER_OFF` can use the shifted u32 encoding. The column chunk's `BLOOM_FILTER_OFF` field gives the absolute offset in the `_pm` file (right-shifted by 3). If `BLOOM_FILTER_OFF` is 0, there is no bloom filter for that column chunk.

At the offset, a 4-byte `LENGTH` field gives the size of the bitset, followed by the bitset bytes.

| offset | size | field  | type | description                                           |
| ------ | ---- | ------ | ---- | ----------------------------------------------------- |
| 0      | 4    | LENGTH | i32  | length of the bloom filter bitset in bytes (not bits) |
| 4      | ..   | BITSET |      | bloom filter bitset                                   |

### Footer

The `_pm` file size is stored in `_txn` field 3. The reader locates the footer by reading the 4-byte FOOTER_LENGTH trailer at the end of the file: `footer_offset = file_size - 4 - FOOTER_LENGTH`. This mirrors how parquet files store `footer_length + PAR1` at the end.

The CRC is located via `FOOTER_LENGTH`: `CRC offset = footer_start + FOOTER_LENGTH - 4`. This handles unknown footer feature sections between the row group entries and CRC.

| offset | size | field                 | type | description                                                                         |
| ------ | ---- | --------------------- | ---- | ----------------------------------------------------------------------------------- |
| 0      | 8    | PARQUET_FOOTER_OFFSET | u64  | byte offset in the parquet file where the parquet footer starts                     |
| 8      | 4    | PARQUET_FOOTER_LENGTH | u32  | length of the parquet footer in bytes                                               |
| 12     | 4    | ROW_GROUP_COUNT       | u32  |                                                                                     |
| 16     | 8    | FOOTER_FEATURE_FLAGS  | u64  | footer feature flags                                                                |
| 24     | 8    | UNUSED_BYTES          | u64  | accumulated dead bytes in the parquet file (old footers + replaced row group data)  |
| 32     | ..   | ROW_GROUP_ENTRIES     |      | ROW_GROUP_COUNT * Row group entry (4B each)                                         |
| ..     | ..   | FOOTER_FEAT_SECTIONS  |      | Feature sections in ascending bit order (if any footer flags set)                   |
| ..     | 4    | CHECKSUM              | u32  | CRC32 from the start of the file to this field (exclusive)                          |
| ..     | 4    | FOOTER_LENGTH         | u32  | total bytes from footer start through CHECKSUM (inclusive); NOT covered by CHECKSUM |

The parquet file size is derived from the footer metadata: `parquet_file_size = PARQUET_FOOTER_OFFSET + PARQUET_FOOTER_LENGTH + 8` (4B parquet footer length field + 4B PAR1 magic). This eliminates the need to store the parquet file size separately.

### Row group entry (4 bytes)

| offset | size | field        | type | description                                            |
| ------ | ---- | ------------ | ---- | ------------------------------------------------------ |
| 0      | 4    | BLOCK_OFFSET | u32  | byte offset from file start >> 3 (actual = value << 3) |

## Concurrent Writing/Reading

Atomicity is provided by the `_txn` file. The `_pm` file size is stored in `_txn` field 3 for each partition, enabling readers to memory-map the correct range and locate the footer via the trailer.

**Writer flow:**
1. Write/update `data.parquet`.
2. Write the `_pm` metadata file. On update, append new/changed row group blocks after the old footer, then write a new footer (with CRC + trailer) at the end.
3. Commit `_txn` (A/B buffered), updating the partition name txn and `_pm` file size.

**Reader flow:**
1. Read `_txn` via `safeReadTxn()` (spin-lock with version check).
2. Memory-map the `_pm` file using the file size from `_txn` field 3.
3. Read the 4-byte FOOTER_LENGTH trailer at the end of the file to locate the footer.
4. Read the footer: PARQUET_FOOTER_OFFSET, PARQUET_FOOTER_LENGTH, ROW_GROUP_COUNT, FOOTER_FEATURE_FLAGS, and row group entries.
5. For metadata-only operations (timestamp stats), read directly from the `_pm` file.
6. For data operations, derive the parquet file size from the footer (`PARQUET_FOOTER_OFFSET + PARQUET_FOOTER_LENGTH + 8`) and memory-map `data.parquet`.

**Rewrite mode** (new partition directory with new name txn): new metadata file created. No concurrent access until `_txn` flips.

**Update mode** (same partition directory): new row group blocks appended after old footer, new footer written at the end. Unchanged row groups keep their old offsets. Readers use the file size from the previous `_txn` commit to see only committed data.

## Access patterns

### Decoding a specific column chunk from cold storage

- Look up the column chunk from cached metadata.
- Fetch `[BYTE_RANGE_START, BYTE_RANGE_START + TOTAL_COMPRESSED)` from cold storage.
- Use CODEC to decompress; page headers carry per-page encoding.

### Finding a specific row group by timestamp

- Read each cached row group's timestamp column chunk MIN/MAX STAT.
- Binary search by timestamp range.

### Pruning row groups with bloom filter

- Look up the column chunk from cached metadata.
- Read `LENGTH` (i32) at `BLOOM_FILTER_OFF` in the `_pm` file, then read `LENGTH` bytes of bitset.
- Check if the value is in the bitset; skip row group if not present.

### Retrieving byte ranges for specific columns

- For each row group and column, look up the cached column chunk.
- Byte range: `[BYTE_RANGE_START, BYTE_RANGE_START + TOTAL_COMPRESSED)`.

## Migration strategy

We rely on QuestDB's existing migration system to run the migration passes.

### Initial setup

We pass over every partitions for each table and rely on the existing parquet files to generate the new metadata files at their last version.
As cold-storage depends on this feature, no object-storage access is required for this migration.

### Version change

The `FILE_FORMAT_VERSION` field in the header allows for future evolution. Incompatible changes (e.g. changing the header structure) would increment the version number.

Feature flags provide additive extensibility without version bumps: new optional metadata is gated by a flag bit. Old files simply don't have the flag set - the reader uses defaults. Required features (bits 32-63) allow the reader to reject files that it cannot correctly interpret.

Migration from an older to a newer version mustn't require having access to the parquet file (in order to avoid cold-storage access). This is easily feasible as we control the partitions files, thus we can safely fill in the new metadata fields with default values that indicate the absence of the new feature (e.g. no encryption, already used bitmap filter algorithm).

External parquet files needs to see their metadata file invalidated.

## Integration in a Multi-version Concurrency Controlled database

In QuestDB, the `_txn` file is responsible to tell the reader which partitions exists and where they are stored. Concurrent access between the TxWriter and TxReader relies on A/B double-buffering to remove needs for locks.

When adding a new row-group to a parquet partition, instead of rewriting the whole file, the row-group is appended to the file (after the existing footer) and a new footer is written afterwards.
Once the new footer is written, the new `_pm` file size is written to the partition in `_txn` field 3 so that new readers will memory-map the correct range and see the new footer/row-groups.
This lets existing readers continue to process the file without disruption from the writer, since they use the file size from their own `_txn` snapshot.

As this file's purpose is to reflect the underlying parquet file, the same behavior is used to update the file. Whenever a new row-group is added to the original parquet file, it's also added to this file. A new footer (with CRC + trailer) is appended, and the `_pm` file size is updated in `_txn`.

## Compaction

As new row-groups and footers are written into parquet files, more and more space is wasted. When a certain threshold is exceeded, the file is compacted in a new partition directory and the `_txn` file is updated to point to this new file.
Following this strategy, the metadata file is also written again from scratch when this compaction occurs, keeping it as small as possible.
