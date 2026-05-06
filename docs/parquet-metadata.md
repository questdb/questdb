# Parquet partition metadata file

## Goals

- Per-partition metadata file stored alongside `data.parquet` in the partition directory.
- Give optional access to statistics about each row group so that they can be pruned by the query planner.
- Give access (mandatory) to the parquet partition file's byte ranges, encodings, and compression for each column chunk
  of each row group to be able to decode data pulled from cold storage without the parquet footer.
- Store the QuestDB-specific metadata currently written as JSON in the parquet footer, avoiding the need to parse the
  parquet footer for this information.
- Usable with external parquet files.

## Background: current QuestDB parquet metadata

Currently stored under the `"questdb"` key in the parquet file's key-value metadata as JSON. This will be replaced by
the binary format below.

```json
{
  "version": 1,
  "schema": [
    {
      "column_type": 12,
      "column_top": 0,
      "format": 1
    },
    {
      "column_type": 5,
      "column_top": 256
    },
    {
      "column_type": 26,
      "column_top": 0,
      "ascii": true
    }
  ],
  "unused_bytes": 4096
}
```

## File format

Binary file encoded in little-endian. One file per partition, stored in the partition directory alongside`data.parquet`.

The file has a header with column descriptors, row group blocks in the middle, and a footer at the end. The committed
`_pm` file size is stored in the header's first 8 bytes (`PARQUET_META_FILE_SIZE`) and is patched last by the writer — readers
treat it as the MVCC commit signal. The footer ends with a 4-byte trailer that stores the footer length, so the latest
footer is at `PARQUET_META_FILE_SIZE - 4 - footer_length`. The parquet file size (separate concept) is stored in `_txn` field 3
for each partition. Row group blocks are referenced by offset from the footer. On update, only new/changed row group
blocks are appended; the footer reuses offsets to unchanged blocks. The file is small (typically tens of KB),
memory-mapped and cached in `TableReader`. Bloom filter bitsets are stored in the out-of-line region of each row group
block (inlined mode) or referenced from the parquet file (external mode), with offsets in the footer feature section.

**Callers never use the filesystem's reported file size to bound an `_pm` read or mapping.** The on-disk length may
include bytes from an in-progress, unpublished append and is not a valid commit boundary; only `PARQUET_META_FILE_SIZE` in the
header is. A reader mmaps the 32-byte header prefix, reads `PARQUET_META_FILE_SIZE`, then remaps to that size.

### Overview

```
                  _pm metadata file                                 data.parquet
                 +================================+                  +==========================+
                 | HEADER                         |                  |                          |
                 |  parquet_meta_file_size -------+--> end of file   |  ...column chunks...     |
                 |  feature_flags                 |                  |                          |
                 |  designated_timestamp          |         +------->|  dict page  | data pages |
                 |  sorting_column_count          |         |        |                          |
                 |  column_count                  |         |        +==========================+
                 |                                |         |
                 | COLUMN DESCRIPTORS             |         |
                 |  col 0: name, type, ..         |         |
                 |  col 1: name, type, ..         |         |
                 |  ...                           |         |
                 |                                |         |
                 | SORTING COLUMNS                |         |
                 |  col indices                   |         |
                 |                                |         |
                 | NAME STRINGS                   |         |
                 |                                |         |
                 | HEADER FEATURE SECTIONS        |         |
                 |  (if any flags set)            |         |
                 +--------------------------------+         |
                 | ROW GROUP BLOCK 0              |         |
                 |  num_rows                      |         |
                 |  chunk col 0:                  |         |
                 |    codec, encodings            |         |
                 |    byte_range_start  ----------+---------+
                 |    total_compressed            |
                 |    null_count                  |
                 |    _reserved (0)               |
                 |    min_stat, max_stat          |
                 |  chunk col 1: ...              |
                 |  ...                           |
                 | (out-of-line stats)            |
                 | (bloom filter bitsets)         |
                 +--------------------------------+
                 | ROW GROUP BLOCK 1              |
                 |  ...                           |
                 +--------------------------------+
                 |  ...                           |
                 +--------------------------------+
                 | FOOTER                         |
                 |  parquet_footer_offset         |
                 |  parquet_footer_length         |
                 |  row_group_count               |
                 |  unused_bytes                  |
                 |  prev_parquet_meta_file_size   |  (0 if first version; trailer at prev - 4)
                 |  footer_feature_flags          |  (per-footer flags)
                 |  entry 0: offset --------------+--> ROW GROUP BLOCK 0
                 |  entry 1: offset --------------+--> ROW GROUP BLOCK 1
                 |  ...                           |
                 | FOOTER FEATURE SECTIONS        |
                 |  bloom filter offsets          |
                 |  (if BLOOM_FILTERS set)        |
                 |  CRC32                         |
                 |  FOOTER_LENGTH (4B)            |  <-- trailer at parquet_meta_file_size - 4
  _txn field 3:  +================================+
  parquet file size

```

The latest footer is located by `footer_offset = parquet_meta_file_size - 4 - FOOTER_LENGTH`, where
`FOOTER_LENGTH` is read from the 4-byte trailer at `parquet_meta_file_size - 4`.

**Update mode** - only changed blocks are appended; unchanged blocks are reused:

```
                 +================================+
                 | HEADER                         |
                 |  parquet_meta_file_size -------+--> end of new file (patched last)
                 +--------------------------------+
                 | ROW GROUP BLOCK 0              |  <-- unchanged, kept in place
                 +--------------------------------+
                 | ROW GROUP BLOCK 1              |  <-- was merged, old data now dead
                 +--------------------------------+
                 | (old footer)                   |  <-- prev version, still readable via chain
                 | (old trailer)                  |  <-- still valid for older parquet_meta_file_size
                 +--------------------------------+
                 | ROW GROUP BLOCK 1'             |  <-- new version of block 1
                 +--------------------------------+
                 | ROW GROUP BLOCK 2              |  <-- newly appended
                 +--------------------------------+
                 | FOOTER (new)                   |
                 |  prev_parquet_meta_file_size --+-->|old trailer| (trailer at prev - 4 gives old footer)
                 |  entry 0: offset --------------+--> BLOCK 0  (old, reused)
                 |  entry 1: offset --------------+--> BLOCK 1' (new)
                 |  entry 2: offset --------------+--> BLOCK 2  (new)
                 |  CRC32                         |
                 |  FOOTER_LENGTH (4B)            |  <-- new trailer at end
  _txn field 3:  +================================+
  parquet file size (new)

```

Readers pinned to the previous snapshot's `parquet_meta_file_size` still see the old trailer at their
snapshot's end-of-file and walk the `prev_parquet_meta_file_size` chain to the footer matching their
`_txn` parquet-size token.

### Feature flags

`_pm` has two independent `u64` feature-flag fields:

- `FEATURE_FLAGS` in the header applies file-wide. It covers every footer in
  the MVCC chain and is the right place for capabilities that are the same for
  every snapshot of the file.
- `FOOTER_FEATURE_FLAGS` in each footer applies only to that footer. Two
  footers reachable via `PREV_PARQUET_META_FILE_SIZE` can carry different sets of
  footer flags, enabling per-snapshot feature sections.

Both fields share the same bit policy:

- **Bits 0-31**: optional - unknown bits may be ignored.
- **Bits 32-63**: required - unknown bits must cause the reader to reject the file (or, for footer flags, the specific footer the reader is about to use).

Feature sections appear in bit order. Header-gated sections live at the end of
the header (after name strings); footer-gated sections live at the end of the
footer (after row group entries, before the CRC). The footer-trailer's
`FOOTER_LENGTH` bounds all footer sections so readers can locate the CRC
without recognizing every bit.

#### Defined feature flags

No footer flag bits are defined yet. Header flag bits:

| bit | name                   | dependency | header section                                                                                                           | footer section                                                                                                                                 |
| --- | ---------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| 0   | BLOOM_FILTERS          | none       | `4 + bloom_col_count * 4` bytes: `[u32 bloom_col_count][u32; bloom_col_count]` column indices (sorted ascending, unique) | `row_group_count * bloom_col_count * 4` bytes: inlined offsets (`>>3`) into `_pm`; `0` = absent                                                |
| 1   | BLOOM_FILTERS_EXTERNAL | bit 0      | none (shares bit 0 header section)                                                                                       | entry width grows from 4 to 16 bytes: `[(u64 offset, u64 length); row_group_count * bloom_col_count]` into the parquet file; `(0, 0)` = absent |
| 2   | SORTING_IS_DTS_ASC     | none       | none                                                                                                                     | none                                                                                                                                           |

Bit 0 is only set when at least one column has a bloom filter. Bit 1 cannot be set without bit 0; the reader rejects the
file otherwise. Feature sections are ordered by bit position.

Bit 2 indicates that the partition's sorting order is implicitly the designated timestamp column in ascending order. The
on-disk `SORTING_COLUMN_COUNT` is 0 and the SORTING_COLUMNS section is absent, but readers treat the partition as sorted
by `[DESIGNATED_TIMESTAMP]` ascending. The designated timestamp column's DESCENDING flag must not be set. This flag is
only valid when `DESIGNATED_TIMESTAMP >= 0`; writers must not set it when `DESIGNATED_TIMESTAMP` is -1.

QuestDB-managed parquet snapshots represented by `_pm` always normalize `column_top` to `0`. Null prefixes are
materialized directly into parquet chunks/pages, and `_pm` pruning relies on per-chunk `NULL_COUNT` rather than
file-level `column_top` metadata.

### File header

| offset | size | field                   | type | description                                                                                                         |
| ------ | ---- | ----------------------- | ---- | ------------------------------------------------------------------------------------------------------------------- |
| 0      | 8    | PARQUET_META_FILE_SIZE  | u64  | total committed `_pm` file size; patched last by the writer and acts as the MVCC commit signal (not covered by CRC) |
| 8      | 8    | FEATURE_FLAGS           | u64  | reserved for future format extensions; currently always 0                                                           |
| 16     | 4    | DESIGNATED_TIMESTAMP    | i32  | index of the designated timestamp in descriptors (or -1)                                                            |
| 20     | 4    | SORTING_COLUMN_COUNT    | u32  |                                                                                                                     |
| 24     | 4    | COLUMN_COUNT            | u32  |                                                                                                                     |
| 28     | 4    | RESERVED                | u32  | must be 0 (alignment padding)                                                                                       |
| 32     | ..   | COLUMN_DESCRIPTORS      |      | COLUMN_COUNT * Column descriptor (32B each)                                                                         |
| ..     | ..   | SORTING_COLUMNS         |      | SORTING_COLUMN_COUNT * Sorting column (4B each)                                                                     |
| ..     | ..   | NAME_STRINGS            |      | Column names, each `[utf8 bytes]`; length from descriptor's NAME_LENGTH                                             |
| ..     | ..   | HEADER FEATURE SECTIONS |      | Feature-flag-gated sections, in bit order. See "Bloom filters" below.                                               |

The latest footer lives at `PARQUET_META_FILE_SIZE - 4 - FOOTER_LENGTH`, where `FOOTER_LENGTH` is read from the 4-byte trailer at
`PARQUET_META_FILE_SIZE - 4`. Readers do not consult `ff.length()` / `stat()` — the filesystem size is not a commit boundary.

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

Written sequentially after the header. Each block holds the column chunk metadata for one row group. Column chunks are
fixed-size (64 bytes), so block size = 8 + COLUMN_COUNT * 64. On update, new/changed blocks are appended after the old
footer; unchanged blocks are left in place and referenced by the new footer.

Blocks must be aligned to 8 bytes so that the offset in the footer can be stored as a u32 (actual offset = value << 3).

For types > 8 bytes (LONG128, UUID, LONG256), min/max stat values are stored out-of-line immediately after the column
chunks. When inlined bloom filters are present (feature flag bit 0 set, bit 1 clear), bloom filter bitsets follow the
out-of-line stats, each padded to 8-byte alignment. All out-of-line data is part of the row group block and written
together with it. References to the bitsets are in the footer feature section, not in the column chunk struct.

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
| 4      | 4    | RESERVED         | u32  | must be 0. Previously held bloom filter offsets, now moved to footer feature sections. Exists for layout preservation.            |
| 8      | 8    | NUM_VALUES       | u64  | total values (may differ from row count for arrays)                                                                               |
| 16     | 8    | BYTE_RANGE_START | u64  | byte offset in parquet file to chunk start (dictionary page offset if present, else data page offset)                             |
| 24     | 8    | TOTAL_COMPRESSED | u64  | total compressed bytes of all pages                                                                                               |
| 32     | 8    | NULL_COUNT       | u64  | number of nulls; mandatory for QuestDB-managed `_pm` files                                                                        |
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

Column types with fixed size that are <= 8 bytes (BOOLEAN, BYTE, SHORT, CHAR, INT/FLOAT/IPv4,
LONG/DOUBLE/DATE/TIMESTAMP) MUST have their min/max stats inlined in the column chunk. For variable-length types (
VARCHAR/STRING) and fixed-size types > 8 bytes (LONG128, UUID, LONG256), min/max stats MAY be stored out-of-line
immediately after the row group blocks that references them.

QuestDB-managed `_pm` files always set `NULL_COUNT_PRESENT`. Readers use `NULL_COUNT == NUM_VALUES` as the all-null fast
path when deciding whether a parquet column chunk needs to be fetched and decoded.

### Bloom filters

Bloom filter metadata is gated by feature flag bits 0 and 1 in the header.

**Header section** (bit 0, after name strings): declares which columns have bloom filters.

| offset | size                | field                | type  | description                                |
| ------ | ------------------- | -------------------- | ----- | ------------------------------------------ |
| 0      | 4                   | BLOOM_COL_COUNT      | u32   | number of columns with bloom filters (> 0) |
| 4      | BLOOM_COL_COUNT * 4 | BLOOM_FILTER_COLUMNS | u32[] | column indices, sorted ascending, unique   |

All indices must satisfy `index < COLUMN_COUNT`. The reader rejects the file otherwise.

**Footer section** (bit 0, after row group entries, before CRC): dense `ROW_GROUP_COUNT * BLOOM_COL_COUNT` matrix,
row-major. Entry `[rg_idx * BLOOM_COL_COUNT + pos]` where `pos` is the column's position in `BLOOM_FILTER_COLUMNS`.

- **Inlined (bit 1 = 0)**: each entry is a `u32` — the absolute `_pm` offset right-shifted by 3. `0` = absent for that
  `(row_group, bloom_col)` pair.
- **External (bit 1 = 1)**: each entry is `(u64 offset, u64 length)` into the parquet file. `(0, 0)` = absent.

**Bitset storage**:

- **Inlined**: bitsets live in the row group block's out-of-line region, padded to 8-byte alignment. At the offset:
  `[i32 LENGTH][bitset bytes]`.
- **External**: bitsets live in the parquet file. The footer section entry gives offset and length.

**Invariants**:

- Bit 0 is only set when at least one column has a bloom filter. If no columns have bloom filters, neither bit is set
  and no sections are written.
- Bit 1 requires bit 0; setting bit 1 alone is invalid.
- `BLOOM_FILTER_COLUMNS` is fixed at file creation time. Update mode does not rewrite the header; compaction (full
  rewrite) changes which columns have bloom filters.
- A column can be in `BLOOM_FILTER_COLUMNS` but absent in some row groups (sentinel value `0` or `(0, 0)`).

#### Bloom filter bitset (inlined)

| offset | size | field  | type | description                                           |
| ------ | ---- | ------ | ---- | ----------------------------------------------------- |
| 0      | 4    | LENGTH | i32  | length of the bloom filter bitset in bytes (not bits) |
| 4      | ..   | BITSET |      | bloom filter bitset                                   |

### Footer

The parquet file size is stored in `_txn` field 3. The reader locates the latest footer via the trailer at
`PARQUET_META_FILE_SIZE - 4`: `footer_offset = PARQUET_META_FILE_SIZE - 4 - FOOTER_LENGTH`. For MVCC, the reader walks
the chain via `PREV_PARQUET_META_FILE_SIZE` on each footer — the same size-then-trailer indirection that the header
uses for the latest footer, so each walk-back step re-validates the previous footer's location through its own
trailer.

The CRC covers all bytes after `PARQUET_META_FILE_SIZE`: `[8, CRC_field)`. This protects feature flags, column
descriptors, row group blocks, and footer content, while excluding the mutable `PARQUET_META_FILE_SIZE` field at
offset 0. It is located via `FOOTER_LENGTH`: `CRC offset = footer_start + FOOTER_LENGTH - 4`.

| offset | size | field                       | type | description                                                                                          |
| ------ | ---- | --------------------------- | ---- | ---------------------------------------------------------------------------------------------------- |
| 0      | 8    | PARQUET_FOOTER_OFFSET       | u64  | byte offset in the parquet file where the parquet footer starts                                      |
| 8      | 4    | PARQUET_FOOTER_LENGTH       | u32  | length of the parquet footer in bytes                                                                |
| 12     | 4    | ROW_GROUP_COUNT             | u32  |                                                                                                      |
| 16     | 8    | UNUSED_BYTES                | u64  | accumulated dead bytes in the parquet file (old footers + replaced row group data)                   |
| 24     | 8    | PREV_PARQUET_META_FILE_SIZE | u64  | committed `_pm` file size at the previous snapshot (0 if first); walk back via trailer at `prev - 4` |
| 32     | 8    | FOOTER_FEATURE_FLAGS        | u64  | per-footer feature flags; independent of the header's FEATURE_FLAGS                                  |
| 40     | ..   | ROW_GROUP_ENTRIES           |      | ROW_GROUP_COUNT * Row group entry (4B each)                                                          |
| ..     | ..   | FOOTER_FEATURE_SECTIONS     |      | Feature-flag-gated sections, in bit order (may be empty)                                             |
| ..     | 4    | CHECKSUM                    | u32  | CRC32 over bytes `[8, this field)` — all content after `PARQUET_META_FILE_SIZE`                      |
| ..     | 4    | FOOTER_LENGTH               | u32  | total bytes from footer start through CHECKSUM (inclusive); NOT covered by CHECKSUM                  |

The parquet file size is derived from the footer metadata:
`parquet_file_size = PARQUET_FOOTER_OFFSET + PARQUET_FOOTER_LENGTH + 8` (4B parquet footer length field + 4B PAR1
magic). This eliminates the need to store the parquet file size separately.

### Row group entry (4 bytes)

| offset | size | field        | type | description                                            |
| ------ | ---- | ------------ | ---- | ------------------------------------------------------ |
| 0      | 4    | BLOCK_OFFSET | u32  | byte offset from file start >> 3 (actual = value << 3) |

## Concurrent Writing/Reading

Atomicity is provided by the `_txn` file. The parquet file size is stored in `_txn` field 3 for each partition, serving
as the MVCC version token.

**Writer flow:**

1. Write/update `data.parquet`.
2. Write the `_pm` metadata file. On update, append new/changed row group blocks after the old trailer, then write a
   new footer (with CRC) and a new 4-byte `FOOTER_LENGTH` trailer at the end. Patch `PARQUET_META_FILE_SIZE` in the header as the
   last write — this is the MVCC commit signal. Readers see either the old committed size (with the old trailer at
   `old_parquet_meta_file_size - 4`) or the new one.
3. Commit `_txn` (A/B buffered), updating the partition name txn and parquet file size.

**Reader flow:**

1. Read `_txn` via `safeReadTxn()` (spin-lock with version check). Obtain parquet file size from field 3.
2. Memory-map the first 32 bytes of the `_pm` file. Read `PARQUET_META_FILE_SIZE` from offset 0.
3. Remap (or open-and-map) `parquet_meta_file_size` bytes of the `_pm` file. Do not consult `stat()` / `ff.length()` — the
   filesystem size may include unpublished bytes.
4. Read the 4-byte trailer at `PARQUET_META_FILE_SIZE - 4` to get `FOOTER_LENGTH`; derive `footer_offset = PARQUET_META_FILE_SIZE - 4 -
   FOOTER_LENGTH`.
5. Walk the MVCC chain: derive parquet file size from the current footer
   (`PARQUET_FOOTER_OFFSET + PARQUET_FOOTER_LENGTH + 8`). If it matches the `_txn` snapshot, stop. Otherwise, read
   `PREV_PARQUET_META_FILE_SIZE` from the current footer and repeat from step 4 with the new size. Each step
   re-validates the previous footer location via its own trailer.
6. Read the footer: PARQUET_FOOTER_OFFSET, PARQUET_FOOTER_LENGTH, ROW_GROUP_COUNT, UNUSED_BYTES, and row group entries.
7. For metadata-only operations (timestamp stats), read directly from the `_pm` file.
8. For data operations, memory-map `data.parquet` using the parquet file size from `_txn`.

**Rewrite mode** (new partition directory with new name txn): new metadata file created. No concurrent access until
`_txn` flips.

**Update mode** (same partition directory): new row group blocks appended after old trailer, new footer and trailer
written at the end. Unchanged row groups keep their old offsets. The header's `PARQUET_META_FILE_SIZE` is patched last
for atomicity. Readers pinned to an older `_txn` snapshot read the committed-at-that-time `PARQUET_META_FILE_SIZE`
(from their own earlier mapping) and walk the `PREV_PARQUET_META_FILE_SIZE` chain from the matching trailer to find
their footer.

## Access patterns

### Decoding a specific column chunk from cold storage

- Look up the column chunk from cached metadata.
- If `NULL_COUNT == NUM_VALUES`, treat the chunk as all-null and skip the parquet fetch.
- Fetch `[BYTE_RANGE_START, BYTE_RANGE_START + TOTAL_COMPRESSED)` from cold storage.
- Use CODEC to decompress; page headers carry per-page encoding.

### Finding a specific row group by timestamp

- Read each cached row group's timestamp column chunk MIN/MAX STAT.
- Binary search by timestamp range.

### Pruning row groups with bloom filter

- Check if the `BLOOM_FILTERS` feature flag (bit 0) is set in the header.
- Look up the column's position in `BLOOM_FILTER_COLUMNS` via binary search.
- Read the bloom filter offset from the footer feature section at `[rg_idx * bloom_col_count + pos]`.
- For inlined mode: read `LENGTH` (i32) at the offset in the `_pm` file, then read `LENGTH` bytes of bitset.
- For external mode: read the bitset from the parquet file at `(offset, length)`.
- Check if the value is in the bitset; skip row group if not present.

### Retrieving byte ranges for specific columns

- For each row group and column, look up the cached column chunk.
- Byte range: `[BYTE_RANGE_START, BYTE_RANGE_START + TOTAL_COMPRESSED)`.

## Migration strategy

We rely on QuestDB's existing migration system to run the migration passes.

### Initial setup

We pass over every partitions for each table and rely on the existing parquet files to generate the new metadata files
at their last version.
As cold-storage depends on this feature, no object-storage access is required for this migration.

### Evolution

Feature flags are reserved for future extensions. Required features (bits 32-63) allow the reader to reject files that
it cannot correctly interpret.

Migration from an older to a newer version mustn't require having access to the parquet file (in order to avoid
cold-storage access). This is easily feasible as we control the partitions files, thus we can safely fill in the new
metadata fields with default values that indicate the absence of the new feature (e.g. no encryption, already used
bitmap filter algorithm).

External parquet files needs to see their metadata file invalidated.

## Integration in a Multi-version Concurrency Controlled database

In QuestDB, the `_txn` file is responsible to tell the reader which partitions exists and where they are stored.
Concurrent access between the TxWriter and TxReader relies on A/B double-buffering to remove needs for locks.

When adding a new row-group to a parquet partition, instead of rewriting the whole file, the row-group is appended to
the file (after the existing footer) and a new footer is written afterwards. The header's `FOOTER_OFFSET` is patched
last to point to the new footer. The new parquet file size is written to `_txn` field 3 so that readers can identify
which footer matches their snapshot.

Existing readers continue to see their committed data by walking the `PREV_PARQUET_META_FILE_SIZE` chain: each footer links to
the previous one, and the reader selects the footer whose derived parquet file size matches the parquet file size from
its `_txn` snapshot.

As this file's purpose is to reflect the underlying parquet file, the same behavior is used to update the file. Whenever
a new row-group is added to the original parquet file, it's also added to this file. A new footer (with CRC + trailer)is
appended, the header's `FOOTER_OFFSET` is patched, and the parquet file size is updated in `_txn`.

## Compaction

As new row-groups and footers are written into parquet files, more and more space is wasted. When a certain threshold is
exceeded, the file is compacted in a new partition directory and the `_txn` file is updated to point to this new file.
Following this strategy, the metadata file is also written again from scratch when this compaction occurs, keeping it as
small as possible.
