● Session Summary: Parquet Export Performance Optimization

Problem

Parquet export of large tables (~140M rows, ~17 partitions) showed progressive slowdown during export due to page cache exhaustion. The system has limited RAM and as more data is read via mmap, the OS   
struggles to manage the page cache.

Findings

1. Root Cause: Page cache exhaustion during sequential read of large tables. As partitions are read via memory-mapped files, the kernel keeps pages in cache, eventually exhausting available memory and   
   causing slowdown.
2. Cursor Wrapping: The PageFrameCursor used in HTTP parquet export is wrapped in a RegisteredPageFrameCursor (from QueryProgress.java), not a direct TablePageFrameCursor. This wrapper is used for query
   lifecycle management and logging.
3. TableReader Partition State: Partitions in TableReader track their open state via openPartitionInfo with partitionSize > -1 indicating an open partition. The closePartition() method exists but is     
   private and logs "closed partition [path=..." when called.

Attempts

1. Madvise Hints (Successful)

Added madvise() calls in CopyExportRequestTask.java:
- MADV_SEQUENTIAL before reading each page frame - hints kernel to use aggressive readahead
- MADV_DONTNEED after processing each page frame - marks pages as reclaimable

This helped with the original progressive slowdown issue.

2. Partition Release During Export (Failed - Caused Segfault)

Attempted to release partitions as they were processed to free page cache:

- Added releasePartition(int partitionIndex) public method to TableReader.java
- Added getTableReader() default method to PageFrameCursor.java interface
- Added getTableReader() delegation in RegisteredPageFrameCursor
- Added partition tracking in HTTPSerialParquetExporter.processStreamExport() to call releasePartition() when moving to a new partition

Result: Caused segmentation fault. The issue is likely that closing a partition while the cursor is still active invalidates memory addresses that are still being used by the parquet encoder. The mmap   
addresses obtained from PageFrame.getPageAddress() become invalid after closePartition() unmaps the files.

Current State

- Madvise hints remain in place - these help the kernel manage page cache more efficiently
- Partition release reverted - this approach doesn't work because the cursor/encoder may still hold references to memory addresses from closed partitions

Potential Future Approaches

1. Implement partition release at a higher level, after the entire partition data has been fully written to the output stream (not just read)
2. Copy partition data to a temporary buffer before releasing
3. Investigate if the Rust parquet encoder holds onto addresses longer than expected 