# `_cv` body checksum — Plan (additive, no migration)

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Detect a torn `_cv` (column-version) record and fall back to the other A/B area, mirroring the shipped `_txn` checksum (`49ac330c83`). Branch `nw_cv_checksum` (worktree `~/claude/wt/oss/cv-checksum`, stacked on the hardening branch which has the `_txn` checksum + crash harness).

**Key facts (from extraction):**
- `_cv` header = 40 bytes, no gap. Body = array of 32-byte blocks. A/B selected by `OFFSET_VERSION_64` parity; `OFFSET_OFFSET_{A,B}_64` / `OFFSET_SIZE_{A,B}_64` point to each area.
- `_cv` is ALWAYS fully rewritten per commit via `doCommit()` (whole area to a fresh `writeOffset`, then `storeFence` + `++version`). **No in-place stable-version mutation exists** → the checksum covers the ENTIRE area `[offset, offset+size)`, zero exclusions.
- Reuse the `_txn` hash (`TableUtils.calculateTxnBodyChecksum` internals — `hashRange`/`xxh3Avalanche64`; add a `calculateCvAreaChecksum(addr, size)` over the whole contiguous range, sentinel 0→1).

**Design — trailing per-area checksum long (additive, no migration, no `_meta` bump):**
- Writer: after `store()` writes the area at `writeOffset`, compute the checksum over `[writeOffset, writeOffset+areaSize)` and write it at `writeOffset+areaSize`. `OFFSET_SIZE_{A,B}` STAYS `areaSize` (data only; must remain a multiple of 32 so old readers still parse). Then `storeFence` + version bump.
- Reader (`readSafe`): after `readUnsafe(offset,size)`, if the file actually extends to `offset+size+8`, read+verify the checksum; mismatch under a stable version → A/B fallback to the other area → both fail → throw. Absent (file too short, or stored 0) → skip. Back-compat = identical to `_txn`'s sentinel.

## CRITICAL correctness trap — area placement vs the trailing checksum
The checksum at `offset+size` lives in the byte range where `calculateWriteOffset` may place the OTHER area (it appends a new area right after the current one, or reuses freed space before it). If the new area is written at `currentOffset+currentSize`, it CLOBBERS the current area's checksum → after the flip, the now-"other" area fails its checksum → A/B fallback/rollback breaks (false corruption).
**FIX:** every area's on-disk footprint is `size + Long.BYTES` (data + checksum). `calculateWriteOffset` (and any overlap/fits-before check) MUST reserve `Long.BYTES` so a new area is never placed over another area's data OR checksum. The `OFFSET_SIZE` field still stores `size` (data only); only the placement math uses `size + 8`. This is back-compat-safe (old readers use offset/size and ignore the spacing).

## Don't-map-past-EOF (back-compat safety)
Old `_cv` files end at `offset+size` (no trailing 8 bytes). The reader MUST check the real file/mapping length before reading `offset+size+8` — never `mem.resize`/read past EOF (→ SIGBUS). Determine presence from the actual file size (`ff.length(fd)` or the mapping size), absent → skip.

---

### Task 1: checksum helper + write side
- Add `calculateCvAreaChecksum(long addr, long size)` (reuse the `_txn` polynomial; sentinel 0→1).
- `ColumnVersionWriter.doCommit`: write the checksum at `writeOffset+areaSize`; bump file size to `writeOffset+areaSize+Long.BYTES`.
- `calculateWriteOffset` + overlap checks: reserve `Long.BYTES` per area (footprint = size+8) so areas never clobber each other's checksum. READ this method carefully; this is the crux.
- `dumpTo`: also write the checksum for the checkpoint area (so checkpoint `_cv` is protected); absent is also safe.
- Unit-test the helper (deterministic, changes on any covered byte, never 0).

### Task 2: read/verify side + A/B fallback
- `ColumnVersionReader.readSafe`: after `readUnsafe(offset,size)`, verify (file-size-guarded). Mismatch under stable version → load+verify the OTHER area (opposite parity, its offset/size from the header) → adopt if OK → else throw `_cv checksum mismatch in both A and B areas`. Add a `@TestOnly` fallback counter (mirror `_txn`).
- `readUnsafe` (writer self-read): verify-or-skip (do not break the writer path; a log-warn or skip is fine).

### Task 3: tests
- `testCvChecksumDetectsCorruption`: corrupt a byte in the current area → fallback or throw.
- `testCvChecksumFallbackAfterManyCommits`: after N commits (exercising area placement/append), corrupt the current area → fallback returns the correct prior state. THIS is the test that catches a wrong `calculateWriteOffset` +8 reservation.
- `testCvChecksumAbsentOldFormat`: craft/Use an old 40-byte-header `_cv` with no trailing bytes → `readSafe` succeeds, NO SIGBUS.
- `testCvRollbackVerifies`: `rollback()` re-exposes the prior area and its checksum still verifies.
- `testCvNoFalsePositiveUnderConcurrentCommits`: concurrent writer + `readSafe` loop → `fallbackCount==0` (mirror `_txn`).
- Keep green (explicit names): `ColumnVersionWriterTest` (incl. `testFuzzConcurrent`), `ColumnVersionTest`, `EngineMigrationTest#test426` (old `_cv` binary resource — must skip-verify cleanly), and the crash package.

### Final: adversarial review (opus) — independently confirm the placement reservation can never clobber a live/other area's checksum, the reader never maps past EOF on old files, the fallback can't false-positive on healthy concurrent commits, and the whole-area coverage is correct (no in-place mutation missed). Then full regression by explicit names; update memory.
