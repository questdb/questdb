# WAL io_uring Performance Ideas

This note collects potential performance optimizations for the io_uring-backed WAL writer path, with a focus on ASYNC mode (QuestDB default).

## Current Costs Observed

- `syncAsync()` uses `Vect.memcpy(...)` to snapshot the dirty range so the writer can keep appending while a pwrite is in flight.
- In ASYNC commits, `WalWriter.syncIfRequired()` already ends with `ringManager.waitForAll()`, so snapshot copies are not strictly necessary there.
- Additional `sync(true)` calls outside commit barriers (e.g., segment roll, initial aux flush) still trigger snapshot copies.

## Low‑Risk / High ROI Ideas

- Use no‑snapshot sync at all barrier points:
  - Replace `sync(true)` with `syncAsyncNoSnapshot()` in:
    - `WalWriter.flushColumnsForSegmentRoll()`
    - `WalWriter.flushIoUringInitIfNeeded()`
  - Then call `ringManager.waitForAll()` and `resumeWriteAfterSync()`.

- Skip columns that did not advance:
  - Track `lastSyncedAppendOffset` per `MemoryPURImpl`.
  - If `getAppendOffset()` is unchanged, skip flush for that column.

## Medium‑Risk / Medium ROI Ideas

- Page‑swap instead of snapshot copy:
  - Submit a pwrite for the current page, then allocate a new page for further appends.
  - Avoids memcpy, trades for extra page allocations/memory.

- Batch dirty ranges per column:
  - If multiple pages are dirty, issue a single writev rather than many pwrite SQEs.
  - Reduces SQE pressure and kernel overhead.

## Higher‑Risk / Larger Changes

- Use `IORING_OP_WRITE_FIXED` with registered buffers:
  - Register page buffers once and reuse.
  - Can reduce kernel pinning overhead, but requires careful buffer lifecycle management.

- Increase ring capacity to reduce mid‑batch drains:
  - Avoid `submitAndDrainAll()` in hot paths.
  - Tune ring size based on typical dirty page counts.

## Suggested Next Step

- Apply no‑snapshot sync to all barrier points.
- Add `lastSyncedAppendOffset` to skip clean columns.
- Measure with profiler; then consider page‑swap or writev batching if still CPU‑bound.

