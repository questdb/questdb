# Cheaper SYNC — Implementation Plan (PR-1)

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Make `commit.mode=sync` cheaper via (A) fdatasync-for-fsync and (C/D) append-scoped msync skip/range-narrowing, with zero crash-safety regression. See spec `2026-06-24-cheaper-sync-design.md`.

**Branch:** `nw_sync_cheaper` (stacked on the SP1/SP2/SP3 work). Worktree `~/claude/wt/oss/sync-cheaper`.

**Cross-cutting rules:** EXPLICIT test class names only (`-Dtest="pkg.*"` runs ZERO here). Default mode is NOSYNC — durability paths are exercised by the SYNC-forcing crash probes (`Phase2DurabilityProbeTest`, the crash package). Native changes require a rebuild of the C lib before Java tests see them — confirm the build picks up `linux/files.c` edits. Match existing JNI/style idioms exactly.

---

### Task 1: Expose `fdatasync` and use it at the B1 extend gate (Lever A)

**Files:**
- `core/src/main/c/linux/files.c` (add `Java_io_questdb_std_Files_fdatasync0`)
- `core/src/main/c/share/files.c` (non-Linux fallback: call `fsync`)
- `core/src/main/java/io/questdb/std/Files.java` (`fdatasync(long fd)` + `native fdatasync0(int)`)
- `core/src/main/java/io/questdb/std/FilesFacade.java` + `FilesFacadeImpl.java`
- `core/src/main/java/io/questdb/cairo/vm/MemoryCMARWImpl.java` (line ~325) + `MemoryPMARImpl.java` (line ~152)
- `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java` (override `fdatasync` → `recordDurable` + bump op count, mirroring `fsync`)
- Test: `core/src/test/java/io/questdb/test/std/` (Files fdatasync smoke) + the crash package

- [ ] **Step 1 — JNI + Java binding.** Add `fdatasync0` to `linux/files.c` mirroring the `fsync` JNI (`share/files.c:274`), returning `fdatasync((int)fd)`. In `share/files.c` add the same symbol calling `fsync((int)fd)` (non-Linux fallback — strictly safe). Add `Files.fdatasync(long)` + `private static native int fdatasync0(int)` mirroring `Files.fsync`. Add `void fdatasync(long fd)` to `FilesFacade` and implement in `FilesFacadeImpl` mirroring `fsync` (same errno/throw handling). Rebuild the native lib; confirm a smoke test (`Files.fdatasync(fd)` on a written file returns 0).

- [ ] **Step 2 — Use it at the extend gate.** In `MemoryCMARWImpl.sync` and `MemoryPMARImpl.sync`, replace `ff.fsync(fd)` with `ff.fdatasync(fd)` (the size-gate stays identical — fdatasync persists data + i_size). No other logic change.

- [ ] **Step 3 — Crash harness wiring.** In `CrashFaultFilesFacade`, override `fdatasync(long fd)` to do exactly what `fsync` does (`recordDurable(fd)` + bump the durability-op counter). Without this the model treats the new call as non-durable and `assertSyncDurable` would falsely fail.

- [ ] **Step 4 — Verify durability unchanged.** Run `Phase2DurabilityProbeTest` + the full crash package by explicit class name; all green (the Bar-2 proof must still hold with fdatasync). Run `TableWriterTest` subset + `O3Test#testStringColumnPageBoundaries` for hot-path sanity. Commit: `feat(core): expose fdatasync and use it for extend durability in SYNC mode (cheaper than fsync, same crash-safety)`.

---

### Task 2: Append-scoped msync narrowing + skip (Levers D + C)

**Files:**
- `core/src/main/java/io/questdb/cairo/vm/MemoryCMARWImpl.java` (add `appendOnly` + `lastSyncedAppendOffset`; update `sync`, `swapState`, `map`/`of`, `truncate`)
- `core/src/main/java/io/questdb/cairo/vm/MemoryPMARImpl.java` (add `appendOnly` + `lastSyncedPage`; update `sync`, `of`/`switchTo`, `truncate`)
- The factory / open sites that create column data/aux, symbol char/offset, and index .k/.v memories — set `appendOnly = true` there (find via `getPrimaryColumn`/`getSecondaryColumn`/symbol+index mem construction). `_txn`/`_cv`/`_meta`/`_todo` left default `false`.
- Test: crash package + new in-place-durability + append-skip tests

- [ ] **Step 1 — Add the flag + watermark (default OFF = safe).** `MemoryCMARWImpl`: `private boolean appendOnly = false; private long lastSyncedAppendOffset = 0;`. A setter or constructor/of param to enable it. Reset `lastSyncedAppendOffset = 0` in `map`/`of`/`truncate`; swap it in `swapState` (alongside `lastSyncedSize`). `MemoryPMARImpl`: `appendOnly` + `lastSyncedPage = -1`, reset in `of`/`switchTo`/`truncate`.

- [ ] **Step 2 — Narrow + skip in `sync` (only when appendOnly).** CMARW `sync(async)`:
```java
if (appendOnly) {
    long appendOffset = getAppendOffset();
    if (appendOffset == lastSyncedAppendOffset && size == lastSyncedSize) {
        return; // nothing new since last sync (C)
    }
    if (appendOffset > 0) ff.msync(pageAddress, appendOffset, async); // narrowed range (D)
    lastSyncedAppendOffset = appendOffset;
} else {
    ff.msync(pageAddress, size, async); // unchanged full-extent behavior for in-place memories
}
if (!async && size > lastSyncedSize) { ff.fdatasync(fd); lastSyncedSize = size; }
```
PMAR analog: skip `msync` if `mappedPage == lastSyncedPage` (no new page written) AND append pointer unchanged — be careful PMAR re-maps pages; the safe skip signal is "append position unchanged since last sync." If PMAR's skip is too subtle to make provably safe, implement only the narrowing/skip in CMARW and leave PMAR's msync as-is (still correct) — note the decision.

- [ ] **Step 3 — Enable appendOnly on append memories only.** Set `appendOnly=true` for column data/aux vectors, symbol char/offset mems, index .k/.v mems. DO NOT enable for `_txn`/`_cv`/`_meta`/`_todo`. Double-check each enabled site is truly append-only (no random-access `putLong(offset,...)` writes).

- [ ] **Step 4 — Durability + correctness tests.**
  - NEW `testInPlaceTxnCvStaySyncedAcrossManyCommits`: many commits where `_txn`/`_cv` change in place (e.g. lag updates, partition growth) with a crash after a late commit; `assertSyncDurable` proves they're still fully synced (guards against C/D leaking onto them / accidental appendOnly=true).
  - NEW `testAppendOnlySkipAndNarrowing`: an append memory synced twice with no write between → second sync issues no msync (assert via a recording FilesFacade msync-count); a memory that IS written → msync issued over the narrowed range.
  - Run the FULL crash package + `Phase2DurabilityProbeTest` + `SymbolMapTest` + `WalWriterTest` (symbol/index mem paths) + `TxnTest` by explicit class name; all green.

- [ ] **Step 5 — Commit.** `feat(core): append-scoped msync narrowing and skip in SYNC mode (gated by appendOnly; in-place _txn/_cv keep full-extent sync)`.

---

### Final review (after both tasks)
- [ ] Adversarial durability review (opus): independently confirm NO append-only flag was set on any in-place memory; the crash harness wiring for fdatasync is correct; and walk the crash timeline to confirm fdatasync + narrowed/skipped msync still makes every committed byte durable. Then the full crash + txn + wal regression by explicit class name. Update memory.

### Deferred to PR-2
sync_file_range for O3CopyJob bulk vectors (Lever B; Linux-only + fallback); a SYNC-cost microbenchmark to quantify the win.
