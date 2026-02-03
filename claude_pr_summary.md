# Parquet Export Performance Optimization

## Problem

Parquet export of large tables (~140M rows, ~17 partitions) showed progressive slowdown during export due to page cache exhaustion. The system has limited RAM and as more data is read via mmap, the OS struggles to manage the page cache.

## Root Cause

Page cache exhaustion during sequential read of large tables. As partitions are read via memory-mapped files, the kernel keeps pages in cache, eventually exhausting available memory and causing slowdown.

## Solution: Explicit Partition Release via Cursor

Instead of using `madvise()` hints (which have race condition issues with shared mappings in MmapCache), we implemented a cleaner partition lifecycle management approach with explicit caller control.

### Why Not Madvise?

The initial approach used `madvise(MADV_SEQUENTIAL)` before reading and `madvise(MADV_DONTNEED)` after processing each page frame. However, this has problems:

1. **Race conditions with shared mappings**: MmapCache reuses memory mappings across concurrent readers. Even with `isSingleUse()` checks, another reader can acquire the mapping after the check but before the `madvise()` call.

2. **VMA splitting concerns**: Calling `madvise()` on partial ranges of a mapping can cause the kernel to split the VMA (Virtual Memory Area), increasing kernel overhead.

### Why Not Automatic Release?

An earlier iteration used `setReleasePartitionAfterScan(boolean)` to automatically close partitions when transitioning to new ones. However, this caused crashes with the Parquet streaming writer because:

1. **Rust holds borrowed slices**: The Rust Parquet writer creates `&'static [u8]` slices pointing directly at Java heap memory (memory-mapped partition files) - NO COPY occurs.

2. **Delayed reading**: Multiple partitions are accumulated in `pending_partitions`, and actual reading happens later during `write_pending_row_group()` when the row count threshold is reached.

3. **Multi-partition symbol processing**: `symbol_column_to_pages_multi_partition()` reads symbol data from multiple partitions at once.

Automatic release closed partitions before the Rust code finished reading from them, causing segfaults.

### Chosen Approach: Explicit Caller-Controlled Release

The solution adds an explicit `releaseOpenPartitions()` method that the caller invokes after processing each frame:

#### API Changes

```java
// PageFrameCursor interface - new method
default void releaseOpenPartitions() {
    // no-op by default
}

// TableReader - new public method (existing)
public void closePartitionByIndex(int partitionIndex)
```

#### Implementation

**FwdTableReaderPageFrameCursor / BwdTableReaderPageFrameCursor:**
- Track `lowestOpenPartitionIndex` (forward) or `highestOpenPartitionIndex` (backward)
- When `releaseOpenPartitions()` is called, close all partitions except the current frame's partition
- Caller has full control over when to release

**HTTPSerialParquetExporter:**
```java
PageFrame frame;
while ((frame = pageFrameCursor.next()) != null) {
    exporter.writePageFrame(pageFrameCursor, frame);

    // Release partitions after Parquet writer has finished processing.
    // This frees page cache memory for large sequential exports.
    pageFrameCursor.releaseOpenPartitions();
}
```

### Benefits

1. **No race conditions**: Partition closure is scoped to the specific TableReader instance
2. **Caller control**: Caller decides exactly when to release, after processing is complete
3. **Safe with native code**: Rust Parquet writer finishes with partition data before release
4. **No VMA splitting**: Entire partition mappings are released at once
5. **Backward compatible**: Default behavior keeps partitions open as before

### Files Changed

| File | Change |
|------|--------|
| `PageFrameCursor.java` | Added `releaseOpenPartitions()` default method |
| `TableReader.java` | Added `closePartitionByIndex()` public method |
| `FwdTableReaderPageFrameCursor.java` | Implements explicit partition release |
| `BwdTableReaderPageFrameCursor.java` | Implements explicit partition release |
| `HTTPSerialParquetExporter.java` | Calls `releaseOpenPartitions()` after each frame |
| `QueryProgress.java` | Forwards `releaseOpenPartitions()` |
| `SelectedRecordCursorFactory.java` | Forwards `releaseOpenPartitions()` |
| `ExtraNullColumnCursorFactory.java` | Forwards `releaseOpenPartitions()` |

### Testing

New test class `PageFrameCursorReleasePartitionTest.java` with 6 tests:
1. Forward cursor releases partitions when `releaseOpenPartitions()` called
2. Backward cursor releases partitions when `releaseOpenPartitions()` called
3. Default behavior keeps partitions open (no regression)
4. Forward cursor with multiple frames per partition
5. Backward cursor with multiple frames per partition
6. `toTop()` resets tracking correctly

Tests verify behavior using `TableReader.getOpenPartitionCount()`.

---

## Additional Optimization: Streaming Mode with MmapCache Bypass

### Problem

The `releaseOpenPartitions()` approach above handles partition lifecycle, but memory-mapped regions still go through `MmapCache`, which caches mappings for reuse across readers. For streaming exports, we want independent mappings that can release page cache immediately without affecting other readers.

### Solution: Streaming Mode in TableReader

Added a streaming mode that:
1. Bypasses `MmapCache` when mapping partition files (each mapping is independent)
2. Calls `madvise(MADV_DONTNEED)` on close to release page cache

### Key Design Decision: Madvise Timing

**MADV_DONTNEED must be called AFTER reading, not before.**

Per the [Linux man page](https://man7.org/linux/man-pages/man2/madvise.2.html), `MADV_DONTNEED` means "I'm done with these pages, free them." Calling it before reading has no effect since pages will just be re-faulted when accessed.

**Important**: `POSIX_MADV_DONTNEED` is treated as a no-op in glibc since 2.6 due to destructive semantics. The native `MADV_DONTNEED` should be used directly.

### Implementation

**Automatic MmapCache bypass based on madvise flags:**

Instead of a separate `bypassMmapCache` flag, the code automatically bypasses `MmapCache` when `madviseOpts != -1`. This is cleaner - if you're setting madvise options, you want independent mappings.

```java
// MemoryCMRImpl.java - map() and setSize0()
if (madviseOpts != -1) {
    this.pageAddress = TableUtils.mapRONoCache(ff, fd, size, memoryTag);
} else {
    this.pageAddress = TableUtils.mapRO(ff, fd, size, memoryTag);
}
```

**Madvise called on close (after reading):**

```java
// MemoryCMRImpl.java - close()
if (pageAddress != 0) {
    if (madviseOpts != -1) {
        ff.madvise(pageAddress, size, Files.POSIX_MADV_DONTNEED);
    }
    ff.munmap(pageAddress, size, memoryTag);
}
```

**TableReader streaming mode:**

```java
// TableReader.java
private boolean streamingMode = false;

public void setStreamingMode(boolean enabled) {
    this.streamingMode = enabled;
}

// In openOrCreateColumnMemory():
final int madviseOpts = streamingMode ? Files.POSIX_MADV_DONTNEED : -1;
```

**PageFrameCursor propagation:**

```java
// PageFrameCursor.java
default void setStreamingMode(boolean enabled) {
    // no-op by default
}

// TablePageFrameCursor.java
default void setStreamingMode(boolean enabled) {
    TableReader reader = getTableReader();
    if (reader != null) {
        reader.setStreamingMode(enabled);
    }
}
```

### Files Changed

| File | Change |
|------|--------|
| `MemoryCMRImpl.java` | Auto-bypass MmapCache when madviseOpts set; call madvise on close |
| `MemoryCMRDetachedImpl.java` | Simplified constructors (bypass controlled by madviseOpts) |
| `TableReader.java` | Added `streamingMode` field and setter |
| `PageFrameCursor.java` | Added `setStreamingMode()` default method |
| `TablePageFrameCursor.java` | Propagates streaming mode to TableReader |
| `Files.java` | Added `mmapNoCache()` and `mremapNoCache()` methods |
| `FilesFacade.java` | Added interface methods for no-cache mapping |
| `FilesFacadeImpl.java` | Implemented no-cache mapping methods |
| `TableUtils.java` | Added `mapRONoCache()` and `mremapNoCache()` helpers |

### Why This Works

1. **No shared mapping conflicts**: Each streaming reader gets its own independent mapping
2. **Madvise after reading**: Page cache is released when partition is closed, after all reads complete
3. **No VMA splitting**: Entire mapping gets the madvise call at once
4. **Backward compatible**: Default behavior (madviseOpts = -1) uses MmapCache as before

---

## Partition Conversion Optimization (ALTER TABLE CONVERT PARTITION)

### Problem

`TableWriter.convertPartitionNativeToParquet()` maps entire partitions into memory for Parquet encoding. For large partitions, this can exhaust page cache similar to the export case.

### Solution

Added madvise hints to the partition conversion code path:

1. **Use `mapRONoCache()`** instead of `mapRO()` for independent mappings
2. **Apply `MADV_SEQUENTIAL`** after mapping (hint for sequential read pattern)
3. **Apply `MADV_DONTNEED`** before unmapping (release page cache)

### Implementation

**TableUtils.java** - Added path-based `mapRONoCache` helper:
```java
public static long mapRONoCache(FilesFacade ff, LPSZ path, Log log, long size, int memoryTag) {
    final long fd = openRO(ff, path, log);
    try {
        return mapRONoCache(ff, fd, size, memoryTag);
    } finally {
        ff.close(fd);
    }
}
```

**TableWriter.java** - Changed all `mapRO` → `mapRONoCache` with madvise:
```java
final long columnAddr = mapRONoCache(ff, dFile(...), LOG, columnSize, memoryTag);
ff.madvise(columnAddr, columnSize, Files.POSIX_MADV_SEQUENTIAL);
partitionDescriptor.setColumnAddr(columnAddr, columnSize);
```

**MappedMemoryPartitionDescriptor.java** - Added `MADV_DONTNEED` before unmapping:
```java
@Override
public void clear() {
    // ... for each column ...
    if (columnAddr != 0) {
        ff.madvise(columnAddr, columnSize, Files.POSIX_MADV_DONTNEED);
        ff.munmap(columnAddr, columnSize, MemoryTag.MMAP_PARQUET_PARTITION_CONVERTER);
    }
    // ... same for secondary and symbol columns ...
}
```

### Files Changed

| File | Change |
|------|--------|
| `TableUtils.java` | Added `mapRONoCache(FilesFacade, LPSZ, Log, long, int)` helper |
| `TableWriter.java` | Use `mapRONoCache` + `MADV_SEQUENTIAL` for all partition file mappings |
| `MappedMemoryPartitionDescriptor.java` | Added `Files` import, `MADV_DONTNEED` before each `munmap()` |

---

## SQL Serial Parquet Exporter Optimization

### Problem

`SQLSerialParquetExporter` reads partitions via `TableReader` but doesn't release them after processing, causing page cache accumulation during large exports.

### Solution

Enable streaming mode and close partitions after encoding:

```java
try (TableReader reader = cairoEngine.getReader(tableToken)) {
    // Enable streaming mode for madvise hints
    reader.setStreamingMode(true);

    for (int partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++) {
        // ... process partition ...

        PartitionEncoder.encodeWithOptions(partitionDescriptor, ...);

        // Release page cache for processed partition
        reader.closePartitionByIndex(partitionIndex);
    }
}
```

### Files Changed

| File | Change |
|------|--------|
| `SQLSerialParquetExporter.java` | Added `setStreamingMode(true)` and `closePartitionByIndex()` after each partition |

---

## Analysis: Auto-Release Safety with Random Access

### Problem Statement

Can we automatically release partitions when moving to the next one, without explicit caller control?

### Frame Pointer Usage Analysis

Analyzed how `PageFrame` addresses are used across the codebase:

**Safe Patterns (addresses processed immediately):**
1. **Immediate Processing** - Frame addresses used and discarded before `next()`
2. **PageFrameAddressCache** - Two-pass: iterate ALL frames first, cache addresses, THEN process
3. **Parquet Streaming** - Per-frame: addresses copied to `DirectLongList`, passed to native, processed synchronously

**No Problematic Patterns Found:**
- No frames stored across `cursor.next()` calls without proper caching
- Async operations use frame INDEX, not pointers
- Native code (Rust Parquet) processes synchronously before returning

### Random Access Requirement Analysis

**Operations that REQUIRE random access (unsafe to auto-release):**

| Operation | Why Random Access Needed |
|-----------|-------------------------|
| ORDER BY (sorting) | Sort cursor uses `recordAt()` on base cursor |
| ASOF/LT/SPLICE Joins | Jump back to restore state |
| LatestBy | Navigate to specific rows by rowId |
| Window Functions | Access rows in window range |

**Key Finding:** `recordCursorSupportsRandomAccess()` indicates CAPABILITY, not REQUIREMENT.

A simple `SELECT * FROM table`:
- `recordCursorSupportsRandomAccess()` = **true** (CAN do random access)
- Actually REQUIRES random access = **false** (just iterates sequentially)

### Proposed Solution: New Factory Method

Add method to distinguish capability from requirement:

```java
/**
 * Returns true if this factory's internal implementation requires
 * random access to underlying data (e.g., sorting, certain joins).
 * Different from recordCursorSupportsRandomAccess() which indicates capability.
 */
default boolean requiresRandomAccessToUnderlyingData() {
    return false;  // Safe default - most simple scans don't need it
}
```

**Implementation by factory type:**

| Factory | `recordCursorSupportsRandomAccess()` | `requiresRandomAccessToUnderlyingData()` |
|---------|--------------------------------------|------------------------------------------|
| TableReaderRecordCursorFactory | true | **false** |
| SortedRecordCursorFactory | true | **true** |
| AsofJoinRecordCursorFactory | true | **true** |
| GroupByRecordCursorFactory | false | false |

### Usage for Auto-Release

```java
PageFrameCursor cursor = factory.getPageFrameCursor(ctx, ORDER_ASC);
cursor.setStreamingMode(true);

// Only enable auto-release if query doesn't need random access internally
if (!factory.requiresRandomAccessToUnderlyingData()) {
    cursor.setAutoReleasePartitions(true);
}
```

### Future Work

1. Implement `requiresRandomAccessToUnderlyingData()` across all `RecordCursorFactory` implementations
2. Add `setAutoReleasePartitions(boolean)` to `PageFrameCursor`
3. Auto-release partitions on transition when safe
4. Consider compile-time annotation in `SqlCodeGenerator` when building factory tree
