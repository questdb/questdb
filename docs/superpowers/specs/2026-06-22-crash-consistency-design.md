# Crash-consistency hardening — design spec

Date: 2026-06-22. Branch: `nw_varchar_power_loss` (worktree `~/claude/wt/oss/varchar-corruption`).
Companion: `CORRUPTION_AUDIT.md` (engine-wide finding catalogue, committed `f50020d7ac`).

## 1. Background & goal

The shipped VARCHAR torn-aux fix (commits `f987c28`..`5bdeaee`) is one instance of six systemic crash/power-loss patterns the audit found across var-length types, O3 merge, indexes, symbol maps, `_txn`/`_cv`, WAL, parquet, DDL and checkpoint recovery:

- **P1** inverted sync order (pointer synced before its data)
- **P2** `msync` without `fsync` (an extended file's size isn't journaled, so SYNC still tears)
- **P3** map-to-pointer-derived-size with no length check → SIGBUS / silent zeros
- **P4** missing directory fsync
- **P5** no record-body checksum (a torn body under a valid pointer reads as valid)
- **P6** transform data not fsync'd before the commit that exposes it

**Goal of this effort (Goal 1 in the user's framing): reduce corruption likelihood in all modes, especially SYNC.** Making SYNC *cheaper* (Goal 2) is an explicit **non-goal here** and ships as a separate follow-up PR (design seeds in §8 of the audit).

### Success bars (definition of "fixed")

- **Bar 1 — containment (all modes incl. NOSYNC):** after any crash, every committed row reads back correct **or** a clean `CairoException` is thrown. Never silent wrong data, never SIGBUS, never silent overwrite of older committed rows.
- **Bar 2 — durability (SYNC mode):** a crash after a SYNC commit loses **zero** committed transactions.
- **Bar 3 — NOSYNC hardening:** NOSYNC may lose the un-flushed tail, but the visible prefix is always correct, and multi-entry/multi-page torn tails are *detected*, not silently trusted.

### Engine-wide invariants to enforce

1. **Data before pointer** — write, and (in SYNC/ASYNC) sync, the referenced bytes before the pointer/offset/length/`_txn` that exposes them.
2. **Validate on recovery** — never map a file to a pointer-derived size, or trust a last entry, without checking it against the real file length / neighbour entries. On violation, throw (pool recreates the writer / query fails / table suspends) — never SIGBUS or silent.
3. **NOSYNC perf is sacred** — every durability addition is gated on `commitMode != NOSYNC` (and ideally on a "file grew this commit" flag). Recovery-side validation (invariant 2) is mode-independent and cheap, so it always runs.

## 2. Decomposition

Too large for one implementation plan; split into four independently-shippable, harness-validated sub-projects, built in order:

| SP | Title | Satisfies | Depends on |
|----|-------|-----------|------------|
| **SP0** | Crash-consistency test harness | (enables all) | — |
| **SP1** | Phase A — detect-don't-corrupt | Bar 1 | SP0 |
| **SP2** | Phase B — durability ordering | Bar 2 | SP0 |
| **SP3** | Phase C — integrity | Bar 3 (+ hardens Bar 1) | SP0 |

Each fix lands with a red-before / green-after harness test. The effort can stop cleanly after any SP.

---

## 3. SP0 — Crash-consistency harness (full design)

In-process, deterministic, CI-gating fault injection. No root, no real power-cut. A separate `dm-flakey`/`dm-log-writes` real-device suite is an **optional, manual, non-CI** cross-check (out of scope for SP0 CI gate; recommended before merging SP2's fsync changes to validate the FS-dependent P2 claim).

### 3.1 Components (test-only, `core/src/test/java/io/questdb/test/cairo/crash/`)

- **`CrashFaultFilesFacade extends TestFilesFacadeImpl`** — intercepts durability-relevant calls:
  - the `open*` family (`openRW`/`openRO`/etc.) → maintain fd→absolute-path map; register the file.
  - `close`/`closeRemove` → drop fd from map (path registration persists).
  - `fsync(fd)`/`fsyncAndClose(fd)` → advance that file's `durableSize` to its current real length.
  - `allocate`/`truncate`/`write`/`append` → update tracked current size (or just read real `length` lazily).
  - optional durability-op counter → throw `CrashSimulationError` after the *k*-th `fsync`/`msync`/`write` (for the exhaustive driver).
  - Delegates everything else to `TestFilesFacadeImpl`/real FS.
- **`CrashSimulationError extends Error`** — unwind signal; an `Error` so `catch (CairoException)`/`catch (Throwable)` business handlers don't absorb it. Caught only by the harness driver.
- **`AbstractCrashConsistencyTest extends AbstractCairoTest`** — wires the FF in via `getFilesFacade()` override; provides the model + crash-point + assertion helpers below.
- **Validation tests:** `VarcharCrashConsistencyTest` (reproduce the shipped bug via the harness) and a `Phase2DurabilityProbeTest` (quantify P2 on current SYNC code — expected red until SP2).

### 3.2 Durability model

Per registered file, track `durableSize` (long).

- **`markDurableBaseline()`** — set every file's `durableSize := current length`. Semantics: "all state committed before this call is long-since metadata-journaled and safe." Tests call it after setup/seed commits, before the transaction under test.
- **`fsync(fd)` / `fsyncAndClose(fd)`** — `durableSize := current length` for that file.
- **`msync(...)` does NOT change `durableSize`** — it flushes data pages, but a file *extend* is durable only after `fsync` or the FS metadata-journal commit, which the model treats as not-yet-occurred (the worst-case-but-real sub-journal-interval window SYNC must survive).
- **`crash()`** — for each registered file: physically `truncate` the real file to its `durableSize`; bytes below are intact. Then optional corruption knobs:
  - `tornTail(path, offset, len)` — zero a sub-range within `durableSize` (deterministic Bar-1 torn-body injection, exactly what the current varchar tests do by hand).
  - `dirLost(dirPath)` — delete files created under a directory that was never `fsync`'d (models P4: a committed `_txn` referencing a partition whose dirent wasn't flushed).

Properties: prior committed data is always safe (fair); a non-growing in-place update (e.g. `_txn` fast path) survives `crash()` untouched — its torn-body risk is exercised only via `tornTail`; a growing file's new extent is lost unless `fsync`'d since baseline (exposes P2 on current code, passes once SP2 `fdatasync`s the extend).

### 3.3 Crash-point strategies

- **Fixed-point** — `Engine crashAndReopen(Runnable workload)`: run `workload`; snapshot `durableSize`s; `releaseAllWriters()`/`releaseAllReaders()` (clean close performs no `fsync`, so it cannot cheat the model); apply `crash()` (+ any `tornTail`/`dirLost`); return a fresh engine on the same root for assertions.
- **Exhaustive** — `forEachCrashPoint(Runnable workload, Consumer<Engine> assertion)`: repeat the workload; on iteration *k* the FF throws `CrashSimulationError` after the *k*-th durability op; the driver catches it at the engine boundary, **releases the now-distressed writer's handles (munmap/close) before truncating** so no mapping is held over the `truncate`, then truncates each file to the `durableSize` captured at that point, reopens, runs `assertion`. Increment *k* until the workload completes without tripping. Caps iterations (configurable, default 200) and `log()`s coverage + any cap hit. This is the proof tool for ordering fixes (P1): it finds the window where the pointer is durable but its data isn't.

### 3.4 Assertion helpers (the three bars)

- `assertNoSilentCorruption(Engine, ExpectedTable)` — **Bar 1**: reopen + full scan; each committed row equals expected, or the reopen/read raised `CairoException`. No SIGBUS (run under the JVM's normal fault handling; a SIGBUS would crash the test = failure). Plus `assertCommittedBytesUnchanged(path, len, snapshot)` — raw data-file prefix compare (carried over from the current varchar tests).
- `assertSyncDurable(Engine, ExpectedTable)` — **Bar 2**: every committed txn present and equal after crash.
- `assertNosyncContainment(Engine, ExpectedPrefix)` — **Bar 3**: the visible prefix is a correct prefix of expected; any shortfall is a clean truncation, never garbage; torn tails throw rather than read wrong.

### 3.5 SP0 done-criteria

1. `VarcharCrashConsistencyTest` reproduces the shipped bug via `tornTail` — **red** on pre-fix `7e861e7239`, **green** on HEAD — proving the harness detects the exact class.
2. `Phase2DurabilityProbeTest` shows **current SYNC** loses a just-committed txn under `crash()` (P2 quantified). To keep CI green meanwhile it asserts the *current* (buggy) behaviour — committed txn is lost — with a comment that SP2 inverts it to `assertSyncDurable`. This documents the gap as an executable, always-running probe rather than an ignored test.
3. Deterministic, `assertMemoryLeak`-clean, no root, runs in normal CI; exhaustive driver bounded.

---

## 4. SP1 — Phase A: detect-don't-corrupt (Bar 1)

Recovery-side validation only; no change to the write/sync protocol, so safe in all modes (helps NOSYNC too). Each item ships with a harness test.

- **A1 — ARRAY torn-tail guard.** `ArrayTypeDriver.setAppendPosition` (`arr/ArrayTypeDriver.java:682`): add the monotonicity guard mirroring the shipped VARCHAR/STRING one (last row data-offset ≥ previous row data-end → else throw). Add an ARRAY power-loss test. (Audit finding #1.)
- **A2 — fix the two inverted sync orders (P1).** `BitmapIndexWriter.sync` → value (`.v`) before key (`.k`) (`idx/BitmapIndexWriter.java:424`); `O3CopyJob.syncColumns` → data (`dstVar`) before aux (`dstFix`) (`O3CopyJob.java:736`), and rename the misleading `dstFix*` params to `dstAux*`. (Findings #11, #13.) Proven by the exhaustive crash-point driver.
- **A3 — length-validation at map-to-pointer-size sites (P3).** Throw `CairoException` when the pointer-derived map size exceeds `ff.length(fd)`, instead of `assert`/SIGBUS/zeros: bitmap value file (`AbstractBitmapIndexReader.java:153`, `BitmapIndexWriter.java:265/336`), posting value file (`AbstractPostingIndexReader.java:797`, `PostingIndexWriter.java:1122`), parquet data mmap (promote the `VM_PARANOIA_MODE` assert in `MemoryCMRImpl.of:131` to a production check for parquet partitions), and WAL segment columns before apply (`TableWriterSegmentFileCache.mmapSegments` / `configure*MemOM`). (Findings #3, #4-read, #6.)
- **A4 — fd-accessor & tail validation (P3).** `VarcharTypeDriver`/`ArrayTypeDriver.getDataVectorSizeAtFromFd`: reject zeroed/out-of-range offset (mirror STRING `:167`). BINARY: cross-check the data-file length prefix on recovery. Symbol map: validate the tail `.c` length-prefix against `.o[count-1]/.o[count]` in `jumpCharMemToSymbolCount`. (Findings #14, #16, #17.)

## 5. SP2 — Phase B: durability ordering (Bar 2)

Adds the missing `fsync`s, gated on `commitMode != NOSYNC` and (where applicable) a "file grew this commit" flag so the NOSYNC default and non-growing fast paths are unaffected. Each item ships with a Bar-2 harness test; the P2 probe from SP0 flips green here.

- **B1 — `fdatasync` data-before-pointer on extend (P2).** On the commit path, after the existing `msync`, `fdatasync` files that grew, in data→aux→…→`_cv`→`_txn` order: `TxWriter`/`ColumnVersionWriter` (small; always when grown), column data/aux (`syncColumns0`), symbol `.c/.o/.k`, index `.v/.k`. (Finding #2.) Use `fdatasync` (size+data, skip atime/mtime).
- **B2 — directory fsync (P4).** `fsync(dirFd)` for new native partition dirs + their column files before `txWriter.commit` (`openPartition`/`openColumnFiles`); for the `_meta` swap-rename (fsync `_meta.swp` + parent dir after each rename) and `_todo` before relying on it. Reuse the existing `ff.fsyncAndClose(dirFd)` pattern. (Findings #7, #12.)
- **B3 — transform-path & checkpoint durability (P6).** fsync `data.parquet` before `_pm`/`_txn` in native→parquet and O3→parquet (return the fd or `sync_data` in Rust); fsync new native columns in parquet→native and ALTER COLUMN TYPE before their metadata/`_txn` commit; in checkpoint **recovery**, a global `ff.sync()` after restore and **before** `rmdir(checkpointRoot)`. (Findings #4-write, #5, #15.)

**Note (Goal 2 boundary):** B1–B3 add syncs that, done naively, raise SYNC cost. The follow-up Goal-2 PR replaces the per-mapping `msync` storm with `sync_file_range` for bulk append data + dirty-flag gating + batched dir fsyncs, keeping the fail-stop safety SP1/SP3 establish. SP2 must therefore keep its additions cleanly gated and flagged so Goal 2 can refactor them.

## 6. SP3 — Phase C: integrity (Bar 3, + hardens Bar 1)

- **C1 — `_txn`/`_cv` full-record body checksum (P5).** Write a checksum over the whole record body before the version-word bump; verify after the version matches; on mismatch fall back to the other A/B buffer (or throw). Subsumes the lag-only `TX_OFFSET_CHECKSUM_32`. (Finding #9.)
- **C2 — WAL integrity (P5).** Per-record CRC footer in WAL-e (`WalEventReader`/`WalEventCursor`) and in txnlog records; V2 sequencer: `fsync` the part file before publishing `maxTxn` to the header (`TableTransactionLogV2.addEntry`). (Findings #8, #10.)
- **C3 — NOSYNC multi-page torn-tail detection (Bar 3).** Strengthen the var-driver recovery beyond the O(1) last-entry guard to detect a whole unflushed aux page (e.g. an aux checksum, or a bounded backward scan to the first inconsistent entry), closing the documented NOSYNC residual window. (Finding #18.)

## 7. Non-goals

- Goal 2 (cheaper SYNC: `sync_file_range`, `fdatasync`-vs-`fsync`, dirty-flag gating, batched dir fsync, deferred segment-rotation syncs) — separate follow-up PR.
- Real-device (`dm-flakey`) CI gating — optional manual cross-check only.
- Re-designing the (already-correct) `_pm` sidecar, A/B selector flip, conversion/attach/detach commit-point ordering, or WAL write-order/suspend.
- New storage formats or on-disk layout changes beyond adding checksum fields in SP3.

## 8. Risks & mitigations

- **Model fidelity (P2 is FS-dependent).** The baseline model represents the sub-metadata-journal-interval crash window — real and reproducible on ext4/xfs, but the harness can't *prove* a given FS behaves so. Mitigation: optional `dm-flakey` cross-check before merging SP2.
- **Phase B SYNC perf regression.** Mitigated by commit-mode + grew-flag gating; fully addressed by the Goal-2 follow-up.
- **Breadth / review load.** Each SP is independently shippable and individually harness-gated; reviewers can take them one at a time.
- **C1/C2 add on-disk fields.** Must stay backward-compatible (version-gated readers) so existing tables open. Flagged for the SP3 plan.

## 9. Sequencing

SP0 → SP1 → SP2 → SP3. Within each SP, per-finding: write the harness test (red) → fix → green → next. SP0's `VarcharCrashConsistencyTest` doubles as the harness's own regression proof against the already-fixed bug.
