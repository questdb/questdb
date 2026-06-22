# QuestDB corruption-path audit

Audit findings (working document; may be dropped before the final fix PR). Scope: power-loss / crash corruption across the storage engine. Branch `nw_varchar_power_loss` @ `5bdeaee68e`. Produced from a 7-way parallel source audit + spot verification.

Goals: **(1)** reduce corruption likelihood in any mode, especially SYNC; **(2)** make SYNC overhead much cheaper (separate follow-up PR — design notes at the end).

---

## 0. Durability model (verified)

- `cairo.commit.mode` default = **`nosync`** (`PropServerConfiguration` → `"nosync"`; `DefaultCairoConfiguration.getCommitMode` → `CommitMode.NOSYNC`). `NOSYNC=2, SYNC=1, ASYNC=0`.
- `.sync()` everywhere bottoms out at `msync(addr,len, MS_SYNC|MS_ASYNC)` (`core/src/main/c/share/files.c:271`). **SYNC = msync only; there is no `fsync`/`fdatasync` on the normal insert commit path** (confirmed: zero `fsync` in `cairo/vm/`, `TxWriter`, `ColumnVersionWriter`, `SymbolMapWriter`).
- Exceptions that *do* fsync: O3 merge file sync (`O3CopyJob.syncColumns`), new-table create, ADD COLUMN (best-effort, swallowed), parquet `_pm`, checkpoint CREATE (global `ff.sync()`).
- The VARCHAR fix already landed on this branch is **one instance** of the systemic patterns below.

---

## 1. Cross-cutting patterns (systemic root causes)

| # | Pattern | Where it recurs | Net effect |
|---|---------|-----------------|------------|
| **P1** | **Inverted sync order** — the pointer/index file is synced *before* the data it references | `BitmapIndexWriter.sync` (.k before .v); `O3CopyJob.syncColumns` (aux before data) | durable pointer → not-yet-durable data after crash |
| **P2** | **msync without fsync** — appended/extended file's inode size isn't journaled, so msync'd data pages can end up beyond a reverted EOF | whole normal commit path: `_txn`, `_cv`, column data/aux, symbol `.c/.o`, index `.k/.v` | torn/short files even in SYNC mode (FS-dependent) |
| **P3** | **Missing recovery validation** — map a file to a size taken from a *pointer* without checking it against the real file length; or trust a torn last entry | ARRAY `setAppendPosition` (no guard at all); VARCHAR/ARRAY `getDataVectorSizeAtFromFd`; bitmap/posting value-file map; parquet `data.parquet` map; symbol `.c` tail | SIGBUS (map past EOF) or silent zeros/garbage instead of a clean error |
| **P4** | **Missing directory fsync** — new file/dir entries not durably linked before the txn that references them | `openPartition`/`openColumnFiles` (new native partitions); `_meta` swap-rename; `_todo` | committed `_txn` references a partition/file that doesn't exist by name after crash |
| **P5** | **No body integrity check** — double-buffer/record schemes detect a lost *pointer word* but not a torn *body* | `_txn` & `_cv` record bodies (checksum covers only lag fields); WAL-e records; V2 `_txnlog` records | a durable version/pointer over a torn body is read as valid silently |
| **P6** | **Transform data not fsync'd before its commit** | native→parquet & O3→parquet (`data.parquet`), parquet→native, ALTER COLUMN TYPE, **checkpoint recovery** | committed/"restored" state references non-durable bytes |

The data-before-pointer rule the VARCHAR fix introduced (`appendValue` writes data then the aux entry; `syncColumns0` syncs data/primary then aux/secondary) is correct and should be the **engine-wide invariant**. P1 are the two places that currently violate it; P3/P5 are the recovery-side counterpart (validate the pointer before trusting it).

---

## 2. Findings, severity-ranked

Severity = (likelihood of silent committed-data corruption) × (blast radius). "Silent" = no throw in production (`assert` is off in embedded use; on in the Docker image only).

### CRITICAL / HIGH

1. **[HIGH] ARRAY `setAppendPosition` has no torn-tail guard** — `arr/ArrayTypeDriver.java:682-703` (end calc `:903`). The exact bug just fixed for VARCHAR/STRING, **unfixed for ARRAY**, and *worse*: no `assert raw!=0` either, and `size` is a standalone field legitimately 0 for empty arrays, so a torn `offset=0,size=0` last entry is indistinguishable from valid → cursor lands inside committed data → next append overwrites live rows. Silent in all builds. Both modes. **Fix:** mirror the VARCHAR monotonicity guard (`lastOffset >= prevEnd`). CONFIRMED.

2. **[HIGH] msync-without-fsync on the commit path (P2)** — `TxWriter.java:209/707`, `ColumnVersionWriter.java:382`, `TableWriter.syncColumns0:13544/13547`, `SymbolMapWriter.java:394-398`. In SYNC the engine `msync`s but never `fsync`s; after a file *extend* (`allocateDiskSpace`→`ff.allocate` = ftruncate/fallocate, a metadata op) the new size may not be journaled before a power cut, so msync'd tail pages are lost/unreferenced — i.e. `_txn`/`_cv`/data/aux can be torn or short **even in SYNC mode**. FS-dependent (ext4/xfs default journaling makes it real, esp. post-extend). SYNC/ASYNC. **Fix:** in SYNC, `fdatasync` the file after msync when it grew (track a `grew` flag; small atomic files `_txn`/`_cv` always). This is the linchpin for Goal 1-in-SYNC and is co-designed with Goal 2. CONFIRMED (no fsync); impact SUSPECTED-high (FS-dependent).

3. **[HIGH] Index value-file mapped to header size with no length check (P3)** — `AbstractBitmapIndexReader.java:153`, `BitmapIndexWriter.java:265/336`, `AbstractPostingIndexReader.java:797`, `PostingIndexWriter.java:1122`. The `.v`/`.pv` mapping size comes from the durable `.k`/`.pk` header; `MemoryCMRImpl.of:131` only `assert`s `size<=ff.length` (off in prod). Torn/short value file → map past EOF → SIGBUS or silent zero reads. **Fix:** validate `size<=ff.length(fd)`→throw at these map sites (the sibling `ofWithSizeFromHeader` already does). CONFIRMED.

4. **[HIGH] `data.parquet` never fsync'd in parquet write paths (P6)** — native→parquet `TableUtils.java:2007-2024` + Rust `parquet_write/jni.rs` (only `_pm` is `sync_data`'d); O3→parquet `O3PartitionJob.java:575-582`. `_pm` (CRC'd, fsync'd) becomes durable describing a `data.parquet` whose tail isn't durable → on reopen the data file is mmap'd at `_txn`/`_pm` size with no length check → SIGBUS / mis-parse. SYNC/ASYNC (these paths bypass `syncColumns` entirely). **Fix:** `sync_data()` the parquet file before `_pm`, before `_txn`; promote the parquet mmap length check to production. CONFIRMED.

5. **[HIGH] Checkpoint recovery deletes the checkpoint dir before fsyncing restored files (P6)** — `DatabaseCheckpointAgent.java:900-955` (`rmdir` at `:950`), `TableSnapshotRestore.java` copies via `ff.copy` (no dst fsync). The "recovery done" marker (rmdir) can persist before the restored file contents → live DB partially overwritten **and** the checkpoint that would re-drive recovery is gone → silent, unrecoverable. Mode-independent (it's the DR tool). **Fix:** one global `ff.sync()` after restore, before the rmdir. CONFIRMED.

6. **[HIGH] WAL apply trusts WAL-e row range; segment `.d/.i` sizes never validated before mapping (P3)** — `WalTxnDetails.java:711`, `TableWriterSegmentFileCache.java:239-291`, `VarcharTypeDriver.configureDataMemOM`. Committed `[rowLo,rowHi)` comes only from the WAL-e events file; segment columns are mapped to that range with no `hi<=ff.length` check. If WAL-e/sequencer for txn K persisted but the segment data tail didn't (asymmetric flush; NOSYNC default), apply commits zeros/garbage rows silently. **Fix:** validate segment file lengths against the declared range before mapping → throw → table suspends (the existing suspend path is correct, just under-triggered). CONFIRMED.

7. **[HIGH] `_meta` swap-rename + `_todo` have no fsync of file or directory (P4)** — `TableWriter.java:12324-12427`, `renameOrFail` `TableUtils.java:2160`, `_todo` msync-only `TableWriter.java:13966`. Any DDL + power loss can leave `_meta` truncated/stale and the `_todo` repair record itself non-durable. `validateMeta`/`validateSwapMeta` catch a structurally-invalid `_meta` (loud) but not a durable-stale one. **Fix:** fsync `_meta.swp` + parent dir after each rename, fsync `_todo`+dir before relying on it (the `ff.fsyncAndClose(dirFd)` primitive is already used for parquet — just missing here). CONFIRMED.

8. **[HIGH] V2 sequencer publishes a durable maxTxn pointer to an unsynced record (P1/P5)** — `seq/TableTransactionLogV2.java:128-148/324-329`: `addEntry` syncs only `txnMem` (header maxTxn), never `txnPartMem` (the record). Durable pointer → torn record → apply reads wrong walId/segment/rowHi (or `walId==0` → table suspended). **Only affects V2** (`cairo.default.sequencer.part.txn.count>0`; common in Enterprise/replication). Default V1 keeps record+pointer in one msync'd file (safe). **Fix:** sync the part file before publishing maxTxn; add a per-record CRC. CONFIRMED.

### MEDIUM

9. **[MEDIUM] `_txn` / `_cv` have no body integrity check (P5)** — `TxReader.java:615-651`, `TableUtils.calculateTxnLagChecksum:299-308`, `ColumnVersionWriter.java:365-407`. The A/B selector flip is provably atomic (single aligned fenced 8-byte version word) and the seqlock is correct for *concurrent readers*, but **nothing detects a torn record body**: `TX_OFFSET_CHECKSUM_32` covers only `{txn,seqTxn,lag*}` — **not** transientRowCount, columnVersion, symbol-count array, or the partition table. A durable version word over a torn/partially-flushed body (esp. on the grow/`finishABHeader` slow path or the in-place fast path) is read as a valid transaction. `_cv`↔`_txn` *version* mismatch IS reconciled at open (rollback/throw); torn *bodies* slip through. **Fix:** full-record checksum written before the version bump, verified after the version matches (enables A/B fallback on torn body). CONFIRMED.

10. **[MEDIUM] No checksum on WAL-e / txnlog records; torn tail detected only by zero/bounds heuristics (P5)** — `WalEventReader.java:65-205`, `WalEventCursor.java:137-156`. A torn record whose length-prefix+`txn` survive but whose payload is partial parses as valid → wrong `endRowID`/timestamps feed finding #6. **Fix:** per-record CRC footer in WAL-e (and txnlog). CPU-only, no extra sync; converts the dominant silent cases into clean suspends. CONFIRMED.

11. **[MEDIUM] O3 merge syncs aux before data (P1)** — `O3CopyJob.java:736-748` (`dstFix*`=aux synced+fsync'd before `dstVar*`=data). Inverse of the established rule. Today masked by txn-versioning (merged files live in a `txn`-named dir invisible until `_txn` commits) + missing dir fsync (#12), but violates the invariant and bites under FS metadata reordering / future in-place mutation. **Fix:** swap the two blocks (one-liner); rename the misleading `dstFix*`→`dstAux*`. CONFIRMED.

12. **[MEDIUM] New native O3 partition dirs/files never directory-fsync'd before `_txn` (P4)** — dir create `O3PartitionJob.java:1515/827` via `TableUtils.createDirsOrFail` (mkdirs only); the parquet path explicitly *does* fsync the dir (`TableUtils.java:2020`) — native partitions never got it. `_txn` (fsync target only if #2 fixed) can reference a partition dir whose dirent wasn't flushed. **Fix:** one `fsync(dirFd)` per new partition dir before commit. CONFIRMED.

13. **[MEDIUM] BitmapIndex syncs .k before .v (P1)** — `idx/BitmapIndexWriter.java:424-427`. In-memory writes are correctly value-then-key, but `sync()` flushes key (pointer) before value (target). Bounded today: `_txn` synced last, and the last partition's bitmap is rebuilt from column data on reopen; historic/O3 partitions are *not* reindexed, so a torn `.v` there isn't repaired. Exposed under ASYNC. **Fix:** swap to `valueMem.sync(); keyMem.sync();` (one-liner). CONFIRMED.

14. **[MEDIUM] VARCHAR/ARRAY `getDataVectorSizeAtFromFd` accept a torn (zeroed) offset (P3)** — `VarcharTypeDriver.java:570-587`, `arr/ArrayTypeDriver.java:543-551`. The fd-based recovery accessor (used by frame copy/squash + VARCHAR→X conversion) only rejects negatives; STRING's equivalent already rejects `row>-1 && offset==0` (`StringTypeDriver.java:167`). **Fix:** add the same zero/range rejection. CONFIRMED.

15. **[MEDIUM] parquet→native CONVERT & ALTER COLUMN TYPE don't fsync new column files before commit (P6)** — `TableWriter.java:10844-10938`/`1718`, `ConvertOperatorImpl.java:328-372`. These bypass `syncColumns`; torn new columns are trusted on reopen (the thorough torn-tail checks exist only on ATTACH). **Fix:** fsync new column files (data before aux) + dir before the metadata/`_txn` commit, gated on commit mode. CONFIRMED.

### LOW (hardening / defense-in-depth)

16. **[LOW] BINARY ignores its own data-file length prefix on recovery** — inherits STRING's monotonicity guard (catches backward tears) but never cross-checks the 8-byte length prefix it stores, so a forward/inflated tear is accepted. `BinaryTypeDriver`/`StringTypeDriver:328`. CONFIRMED.

17. **[LOW] Symbol `.o`-durable / `.c`-torn not detected** — `SymbolMapWriter.jumpCharMemToSymbolCount:445-463` only catches an undersize `.o`; a durable `.o` pointing at non-durable `.c` bytes yields silent null/garbage. **Fix:** validate the tail `.c` length-prefix against `.o[count-1]/.o[count]` (bytes already mapped, no extra IO). CONFIRMED.

18. **[LOW] NOSYNC residual window in the new `setAppendPosition` guard** — O(1) guard misses a whole unflushed aux page (consecutive zeroed entries → `0<0`). Documented & accepted on this branch. CONFIRMED.

19. **[LOW] `setAppendAuxMemAppendPosition` (WAL rollback) unguarded in all drivers**; **[LOW] posting `pendingTxnAtSeal -1→0` fallback can mint an undroppable `txnAtSeal=0` orphan** (silent, test/legacy path); **[INFO] assert-only readers** (`getDataVectorOffset`, `_cv` alignment, `TxReader` bounds) are silent in embedded builds. CONFIRMED.

---

## 3. What is already correct (do not "fix")

- `_pm` parquet sidecar: CRC32 + MVCC commit-signal (size patched last) + verified every open + rejects `size>fileLength`. Exemplary — use as the template.
- Conversion/attach/detach **commit-point ordering**: all write to a new `txn`-named dir and delete the old one only *after* `_txn` commits; half-written targets are orphaned and purged. Atomic-exposure is right — only the *data durability before that commit* is missing (P6).
- Partition ATTACH validation (`attachPartitionCheckFilesMatchVarSizeColumn:4180-4199`): full offset bounds + monotonicity walk. This is the validation rigor normal partition *open* lacks.
- `_txn`/`_cv` A/B selector flip + seqlock: provably atomic & correct for concurrency (the gap is body integrity under crash, #9).
- WAL write-side ordering (`WalWriter.commit0`: segment → events → sequencer) and the apply no-skip invariant (`seqTxn==applied+1`, suspend-on-throw). Correct; the gaps are read/apply-side validation (#6, #10).
- V1 sequencer: record+pointer in one msync'd file. Safe (V2 regressed it, #8).

---

## 4. Recommendations

### Goal 1 — reduce corruption likelihood (this PR / near-term), phased by risk

**Phase A — cheap, local, low-risk, high-value (recommended first PR):**
- A1. ARRAY torn-tail guard in `setAppendPosition` (#1) — mirrors the shipped VARCHAR/STRING fix. + add an array power-loss test.
- A2. Swap the two inverted sync orders: `BitmapIndexWriter.sync` (#13) and `O3CopyJob.syncColumns` (#11). One line each; rename `dstFix*`→`dstAux*`.
- A3. Production length-validation at every "map to a pointer-derived size" site → throw instead of SIGBUS/silent-zeros: index value files (#3), parquet data file (#4 read side), WAL segment columns (#6 read side). Turns the worst outcomes into clean, pool-recoverable `CairoException`s.
- A4. `getDataVectorSizeAtFromFd` zero/range rejection for VARCHAR/ARRAY (#14); BINARY length-prefix + symbol `.c`-tail checks (#16,#17).

Phase A is almost entirely "detect-don't-corrupt" — it converts silent corruption into loud, recoverable errors without changing the write/sync protocol, so it's safe to ship broadly (helps NOSYNC too, since it's recovery-time validation).

**Phase B — fsync ordering (correctness in SYNC), co-designed with Goal 2:**
- B1. Add `fdatasync` (data → pointer order) on the commit path where files grew: `_txn`/`_cv` always (tiny), data/aux/symbol/index on extend (#2). This is the actual SYNC crash-safety fix.
- B2. Directory fsync for new native partitions (#12) and the `_meta`/`_todo` swap (#7).
- B3. Parquet `data.parquet` + parquet→native + ALTER TYPE + checkpoint-recovery fsync-before-marker (#4 write side, #5, #15).

**Phase C — integrity (catches torn bodies the ordering can't):**
- C1. Full-record checksum on `_txn` and `_cv` (#9) — highest-value NOSYNC hardening (makes the double-buffer self-validating; cheap, CPU-only).
- C2. Per-record CRC on WAL-e and txnlog (#10), + V2 sequencer part-file sync (#8).

### Goal 2 — make SYNC cheaper (follow-up PR) — design seeds

Per-SYNC-commit syscall count today ≈ `2·plainCols + 4·symbolCols + 2·indexedCols + 2 (cv+txn) + segmentRotations` (a 100-col table → 200+ msyncs). Plus per-segment msync on page rotation (`MemoryPMARImpl:172`). Phase B naively adds fsyncs on top — the follow-up must restructure so SYNC becomes *both safer and cheaper*:

- **`sync_file_range(WRITE|WAIT_BEFORE|WAIT_AFTER)`** over the dirtied byte range for the bulk append files (`.d/.i/.c/.o/.k/.v`) instead of per-mapping `msync`; reserve real `fdatasync` for the small atomic files (`_txn`,`_cv`) and directories.
- **Dirty-flag gating:** only sync columns/symbols whose append offset advanced this commit (today `syncColumns0`/`syncColumns` sync *all* columns and *all* symbol maps every commit).
- **`fdatasync` not `fsync`** (skip atime/mtime); **skip `_txn` fsync on the in-place fast path** (no extend → no metadata to flush).
- **Batch directory fsyncs:** one per new directory per commit, not per file.
- **Defer/coalesce segment-rotation msyncs** into one range flush at commit.
- Net: once data is reliably *detected-if-torn* (Phase A/C checksums + length checks), the expensive per-column data fsync can be downgraded to `sync_file_range` while keeping fail-stop safety — the integrity work is what *unlocks* the cheaper sync.

---

## 5. Confidence & caveats

- All file:line locations and inverted-order / no-fsync / no-validation facts are CONFIRMED by reading source (+ spot-verified: bitmap order, O3 order, absent fsync).
- The *exploitability* of P2 (msync-without-fsync) into real data loss is FS-dependent (ext4 data=ordered, xfs); CONFIRMED that no fsync occurs, SUSPECTED-high that it loses extended-file metadata on power cut. Worth an actual power-cut test (e.g. dm-flakey / qemu kill) to quantify before committing to the heavier Phase B/C changes.
- Several findings are bounded *today* by txn-versioning or last-partition reindex but violate the engine-wide data-before-pointer invariant; they are cheap to fix and remove latent footguns for future changes.
