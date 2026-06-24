# Goal 2 — Cheaper SYNC commit mode (design)

**Branch:** `nw_sync_cheaper` (stacked on `nw_varchar_power_loss`, which has SP1/SP2/SP3). Separate follow-up PR.

**Goal:** Reduce the per-commit overhead of `cairo.commit.mode=sync` WITHOUT regressing the crash-safety that SP1/SP2 established. Every change must keep the Bar-2 durability probe (`Phase2DurabilityProbeTest.assertSyncDurable`) and the whole crash package green.

## Current SYNC cost (post-B1)
Per in-order commit (`TableWriter.commit00` → `syncColumns0` + `columnVersionWriter.commit` + `txWriter.commit`):
- For each column: `primary.sync(false)` + `secondary.sync(false)` → each = `msync(MS_SYNC)` of the FULL mmap extent (CMARW: `size`; PMAR: one `extendSegmentSize` page) + `fsync(fd)` iff the file extended (B1 gate `size > lastSyncedSize`).
- Per symbol column: ~4 more msync (char/offset/index .k/.v).
- `_cv`: 1 msync (+fsync on extend). `_txn`: 1 msync (+fsync on extend).
So a 20-col table that extended ≈ 40+ `msync` + up to 20+ `fsync` per commit. The irreducible cost is flushing the bytes actually written; the *waste* is (a) `fsync` flushing inode metadata that didn't need to be durable, (b) `msync` scanning the whole over-allocated extent (default 2 MB append page) to find a few dirty pages, (c) syncing memories that didn't change this commit.

## The four levers (from the terrain map) + scope decision
- **A. `fdatasync` instead of `fsync`** at the B1 extend gate (`MemoryCMARWImpl.sync:325`, `MemoryPMARImpl.sync:152`). Skips the inode-metadata sync; still persists data + `i_size` on Linux journaling FSes (ext4 data=ordered, XFS, ZFS) — the standard DB durability primitive. **Universally safe. IN (PR-1).**
- **D. Tighter `msync` range** — flush `[base, base+appendOffset)` instead of the full extent, so the syscall only walks the written page range, not the over-allocated tail. **Safe ONLY for append memories. IN (PR-1), gated by `appendOnly`.**
- **C. Skip `msync` when nothing changed since last sync** — `appendOffset == lastSyncedAppendOffset && size == lastSyncedSize`. Avoids the syscall entirely for unchanged memories (notably symbol/index mems when no new symbols). **Safe ONLY for append memories. IN (PR-1), gated by `appendOnly`.**
- **B. `sync_file_range`** for bulk O3 data vectors (`O3CopyJob.syncColumns`). Linux-only, needs platform fallback + correctness pairing with `fdatasync`, and O3 is a less-hot path. **DEFER to PR-2.**

## CRITICAL safety constraint (governs C and D)
`appendOffset` is the true dirty high-water mark ONLY for append-pattern memories. `_txn` (`txMemBase`) and `_cv` are written in place at fixed A/B offsets every commit and do NOT advance `appendOffset`; applying C/D to them would skip/under-flush a real update → silent durability loss that the crash-harness MODEL may not catch (msync doesn't advance `durableSize`, so a "green" run can hide a real regression). Therefore:
- Add an explicit `boolean appendOnly` to `MemoryCMARWImpl`/`MemoryPMARImpl`, **default `false`** (full-extent msync every sync = today's behavior, safe for in-place memories).
- Set `appendOnly = true` ONLY on memories proven append-only: the column data/aux vectors, symbol char/offset mems, and index .k/.v mems. Leave `_txn`/`_cv`/`_meta`/`_todo` default `false`.
- C and D apply ONLY when `appendOnly`. fdatasync (A) applies regardless.

## Correctness verification (every task)
- `Phase2DurabilityProbeTest` (Bar-2) + the full crash package stay green — run by EXPLICIT class names (wildcard runs zero).
- Wire `CrashFaultFilesFacade.fdatasync` → `recordDurable` (else the model treats fdatasync as non-durable and over-reports loss).
- NEW test: a MULTI-COMMIT in-place `_txn`/`_cv` durability case proving the `appendOnly=false` path still fully syncs after the first commit (guards against accidentally flipping them to append-only or letting C/D leak onto them).
- NEW test: an append-only memory that gets NO write in a commit is correctly skipped (C) AND one that does get a write is synced (no false skip).
- fdatasync platform fallback: Linux = `fdatasync(2)`; non-Linux (`share/files.c`) = alias to `fsync` (no durability regression on macOS/Windows; macOS true-durability via `F_FULLFSYNC` is a separate concern, out of scope — falling back to `fsync` is strictly safe).

## Out of scope (later)
sync_file_range (B / PR-2); macOS `F_FULLFSYNC`; per-column "changed?" propagation for selective-insert skipping beyond the append-offset signal; the SYNC-cost microbenchmark harness (nice-to-have).
