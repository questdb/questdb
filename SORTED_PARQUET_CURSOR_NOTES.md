# Sorted Parquet Cursor — Implementation Notes

## What Was Done

### Radix Sort for ORDER BY read_parquet (WORKING)

`SqlCodeGenerator.java` (line ~5748) detects when ORDER BY is on a single
int/long/timestamp/date/ipv4 column over a `ReadParquetRecordCursorFactory`
(optionally wrapped in `VirtualRecordCursorFactory`). It enables random access
on the parquet cursor and uses `LongSortedLightRecordCursorFactory` (radix sort)
instead of `SortedRecordCursorFactory` (red-black tree that copies entire rows).

Helper methods:
- `unwrapParquetFactory()` — checks `instanceof` directly or through one level
  of `VirtualRecordCursorFactory`
- `resolveParquetSortColumn()` — maps virtual column index to parquet base
  column index via `ColumnFunction.getColumnIndex()`. Returns -1 for computed
  expressions (can't do two-pass optimization).

### ReadParquetRecordCursor (WORKING)

Supports random access via `enableRandomAccess()`:
- Per-row-group `RowGroupBuffers` caching (not reusing a single buffer)
- `ObjList<LongList>` for data/aux pointers per row group
- `ParquetRecord` holds its own `activeDataPtrs`/`activeAuxPtrs`/`activeRowInGroup`
- `getRowId()` = `(rowGroupIndex << 32) | rowInGroup`
- `recordAt(record, rowId)` decodes rowId and calls `positionAt()`
- `getRecordB()` returns a second `ParquetRecord`

### SortedReadParquetCursor (SEPARATE CLASS, TWO-PASS BLOCKED)

A standalone class in `SortedReadParquetCursor.java`. Has all the infrastructure
for two-pass decode:

**Pass 1 (hasNext):** Should decode only the sort column per row group via
`sortOnlyColumns` (a `DirectIntList` with one `[parquetIndex, columnType]` pair).

**Pass 2 (recordAt):** On first call, `decodeAllRowGroupsFully()` re-decodes
every cached row group with all columns. New `RowGroupBuffers` are appended to
the cache list (old sort-only ones stay and are freed at close()).

**Currently:** Both passes decode all columns due to SIGABRT (see below).

### ReadParquetRecordCursorFactory

Dispatches based on `sortColumnIndex`:
- `sortColumnIndex >= 0` → creates `SortedReadParquetCursor`
- `sortColumnIndex == -1` → creates `ReadParquetRecordCursor` with
  `enableRandomAccess()`

### VirtualRecordCursorFactory / VirtualFunctionRecordCursor

Both gained `setRandomAccessEnabled(boolean)` to flip `supportsRandomAccess`
at compile time (after construction). `VirtualFunctionRecordCursor` lazily
creates `recordB` when enabled.

### Tests (58 Java + 146 Python)

In `ReadParquetFunctionTest.java`:
- Plan checks for "Radix sort light" (serial mode only)
- All supported types: long, int, timestamp, date, IPv4
- DESC, LIMIT, nulls, expressions (VirtualRecord), multi-row-group
- Fallback checks: varchar (→ Sort), multi-column (→ Sort)
- All-column-types round-trip through sort

## The SIGABRT Bug (BLOCKING TWO-PASS)

### Symptom
When `SortedReadParquetCursor.switchToNextRowGroup()` calls
`decoder.decodeRowGroup(rgBuffers, sortOnlyColumns, ...)` with a 1-column
`DirectIntList`, the decode itself SUCCEEDS (confirmed via logging). But the
JVM crashes with SIGABRT (exit 134) during test teardown — specifically when
the `RowGroupBuffers` are freed (either via `Misc.free()` in
`decodeAllRowGroupsFully()` or in `freeCachedRowGroups()`).

### What Was Confirmed
- The decode call completes and returns the correct row count
- All 58 tests pass (results are correct) before the SIGABRT
- The crash is during cleanup, not during decode
- Using `columns` (all columns) instead of `sortOnlyColumns` (1 column) in the
  exact same code path → no crash
- The crash is NOT from `decodeAllRowGroupsFully()` — it happens even when we
  never free old buffers (append-only cache)
- The crash is NOT from `resolveParquetSortColumn` or the VirtualRecord path

### Likely Root Cause
The Rust `QdbAllocator` (in `qdbr/src/allocator.rs`) has assertions:
- Line 308: `assert!(new_layout.size() > old_layout.size())` in `grow()`
- Line 342: `assert!(new_layout.size() < old_layout.size())` in `shrink()`

When a `RowGroupBuffers` is decoded with 1 column, `ensure_n_columns(1)` resizes
the `column_bufs` `AcVec` from 0 to 1 element. Each `ColumnChunkBuffers` has
`data_vec: AcVec<u8>` and `aux_vec: AcVec<u8>`. When this RowGroupBuffers is
dropped, the AcVec destructor may call `shrink` with equal sizes (0 → 0) on an
empty-but-allocated buffer, triggering `assert!(new < old)`.

### Key Files for Fixing
- `questdb/core/rust/qdbr/src/allocator.rs` — The assertions on lines 308, 325, 342
- `questdb/core/rust/qdbr/src/parquet_read/row_groups.rs` — `ensure_n_columns()`,
  `RowGroupBuffers::new()`, `decode_row_group()`
- `questdb/core/rust/qdbr/src/parquet_read/mod.rs` — `ColumnChunkBuffers::new()`,
  `reset()`
- `questdb/core/rust/qdbr/src/parquet_read/decode.rs` — `ColumnChunkBuffers::new()`
- `questdb/core/rust/qdbr/src/parquet_read/jni.rs` — JNI bridge, `destroy()` at
  line 528

### How to Test the Fix
1. In `SortedReadParquetCursor.switchToNextRowGroup()`, change `columns` back to
   `sortOnlyColumns` (remove the TODO block)
2. `cd questdb && mvn package -DskipTests -pl core -am`
3. `mvn -Dtest=ReadParquetFunctionTest test -pl core`
4. Should get 58 tests pass + BUILD SUCCESS (no SIGABRT)

### Native Decoder Column Indexing (Critical Knowledge)
- `decodeRowGroup(buffers, columns, ...)` — `columns` is `[parquetIndex, type]` pairs
- Decoded chunks stored SEQUENTIALLY at indices 0..N-1 in the buffer, NOT at
  parquet column indices
- `getChunkDataPtr(i)` — `i` is sequential buffer index
- `decodeRowGroupWithRowFilter` has `columnOffset` param — chunks go at
  `columnOffset + i`
- `ensure_n_columns(n)` resizes `column_bufs` AcVec to `n` elements
- `ColumnChunkBuffers::new()` creates empty AcVecs (0 capacity)

## Performance Context

For 100M rows × 105 columns (ClickBench hits.parquet):
- Sort-only decode: ~800 MB (1 LONG column × 100M rows × 8 bytes)
- Full decode: ~67 GB (all 105 columns)
- Radix sort pair array: ~1.6 GB (100M × 16 bytes for value+rowId)

The two-pass optimization would reduce pass 1 from ~67 GB to ~800 MB, making
the sequential scan ~80× faster. The full decode still happens in pass 2, but
it's done per-row-group on demand rather than blocking the sort.
