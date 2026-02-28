# WAL io_uring Performance Ideas

This note collects potential performance optimizations for the io_uring-backed WAL writer path, with a focus on ASYNC mode (QuestDB default).

## Current Costs Observed

- `syncAsync()` uses `Vect.memcpy(...)` to snapshot the dirty range so the writer can keep appending while a pwrite is in flight.
- In ASYNC commits, `WalWriter.syncIfRequired()` already ends with `ringManager.waitForAll()`, so snapshot copies are not strictly necessary there.
- Additional `sync(true)` calls outside commit barriers (e.g., segment roll, initial aux flush) still trigger snapshot copies.

## Low‑Risk / High ROI Ideas

- ~~Use no‑snapshot sync at all barrier points:~~
  - ~~Replace `sync(true)` with `syncAsyncNoSnapshot()` in:~~
    - ~~`WalWriter.flushColumnsForSegmentRoll()`~~
    - ~~`WalWriter.flushIoUringInitIfNeeded()`~~
  - ~~Then call `ringManager.waitForAll()` and `resumeWriteAfterSync()`.~~
  - **Done.**

- ~~Skip columns that did not advance:~~
  - ~~Track `lastSyncedAppendOffset` per `MemoryPURImpl`.~~
  - ~~If `getAppendOffset()` is unchanged, skip flush for that column.~~
  - **Done.**

## Medium‑Risk / High ROI Ideas

### Pre‑registered buffer pool with page‑swap sync

Replace all `Unsafe.malloc/free` churn and snapshot `Vect.memcpy` with a fixed pool of
io_uring‑registered buffers. On sync, hand the current buffer to io_uring and grab a
fresh one from the pool — zero‑copy, zero‑alloc on the data path.

**Pool design:**

- All buffers are the same size (`extendSegmentSize`, typically the WAL page size).
- Allocated once at `WalWriter` init, registered via `io_uring_register_buffers()`.
- Shared across all columns of one writer. Free list is a simple stack of buffer indices.
- Pool size: `columnCount * 2 + headroom` (1 writing + 1 in‑flight per column).
- Use `IORING_OP_WRITE_FIXED` with buffer index instead of regular `IORING_OP_WRITEV`
  to skip kernel page‑pinning overhead per pwrite.

**Page lifecycle:**

```
POOL ──acquire──▶ WRITING ──sync/swap──▶ IN_FLIGHT ──CQE──▶ POOL
                     ▲                                        │
                     └─────────────acquire─────────────────────┘
```

On sync:
1. Submit current buffer for `IORING_OP_WRITE_FIXED` (no memcpy).
2. `pool.acquire()` a fresh buffer for continued appends.
3. CQE callback: `pool.release()` the old buffer.

**What gets eliminated:**

- `snapshotBufAddr` / `snapshotBufCapacity` / `snapshotBufSize` / `snapshotInFlight`
  state machine and the entire `syncAsync()` memcpy path.
- All 8 `dbg*` leak counters (pool owns all buffers; leak tracking is trivial).
- `Unsafe.malloc/free` on the data path (`mapWritePage`, `release`, `evictConfirmedPages`,
  `preadPage`).

**What gets simpler:**

- `mapWritePage()` → `pool.acquire()` + optional pread.
- `release()` / `evictConfirmedPages()` → `pool.release()`.
- `syncAsyncNoSnapshot()` and `syncAsync()` merge into one "submit + swap" path.

**Correctness notes — variable‑size columns and rollback:**

Variable‑size columns (VARCHAR, STRING, BINARY, ARRAY) read the last aux entry during
rollback / row cancellation (`setAppendAuxMemAppendPosition` calls
`Unsafe.getLong(auxMem.getAppendAddress())`). After a page‑swap the old buffer is
in‑flight or back in the pool, so that read would hit invalid memory.

Resolution: rollbacks are rare and can afford to be slow. On rollback, wait for any
in‑flight pwrite on the aux column to complete (`waitForPage`), then pread the page
from disk into a fresh pool buffer. The data is guaranteed on disk because the CQE
confirmed. No special caching or tail‑entry copying needed.

During normal forward appends (including all `putStr`, `putVarchar`, `putBin`),
columns are strictly append‑only — no read‑back occurs. Verified by tracing through
`VarcharTypeDriver.appendValue`, `StringTypeDriver.appendValue`, and all `put*`
methods in `MemoryPARWImpl`.

**Investigated and rejected — pipelined submit‑without‑wait:**

The idea of deferring `waitForAll()` to the next commit was investigated and rejected:
- With `syncAsyncNoSnapshot`, the live buffer is SUBMITTED; any `putLong()` between
  commits writes into a buffer the kernel may still be reading (data race).
- Deferring `getSequencerTxn()` requires snapshotting commit metadata and splitting
  the sequencer API into reserve + publish.
- Last‑commit‑before‑idle leaves seqTxn unpublished indefinitely.
- The fundamental bottleneck is the per‑commit `io_uring_submit_and_wait()` syscall,
  which cannot be eliminated without decoupling seqTxn publication from the commit call.

### Batch dirty ranges per column

- If multiple pages are dirty, issue a single writev rather than many pwrite SQEs.
- Reduces SQE pressure and kernel overhead.

## Higher‑Risk / Larger Changes

- Increase ring capacity to reduce mid‑batch drains:
  - Avoid `submitAndDrainAll()` in hot paths.
  - Tune ring size based on typical dirty page counts.

## Suggested Next Step

- ~~Apply no‑snapshot sync to all barrier points.~~ **Done.**
- ~~Add `lastSyncedAppendOffset` to skip clean columns.~~ **Done.**
- Implement the pre‑registered buffer pool with page‑swap sync.
- Measure with profiler; then consider writev batching if still CPU‑bound.

