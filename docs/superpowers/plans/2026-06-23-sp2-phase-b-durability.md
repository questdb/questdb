# SP2 — Phase B: durability ordering (Bar 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** In SYNC mode, make a file's EXTEND durable (not just its data pages), and make new files/dirs/transforms durable before the commit that exposes them — so a crash after a SYNC commit loses ZERO committed transactions (Bar 2). `fdatasync` isn't exposed, so use `ff.fsync(fd)`.

**Architecture:** B1 (the core) adds one hook to the two `Memory.sync(boolean)` impls: after the existing `msync`, when `!async` (SYNC) and the file grew since the last sync, `fsync(fd)` and record the new size. Because every committed file (columns, `_txn`, `_cv`, symbols, indexes) flows through these two impls, one change covers them all. B2/B3 add directory fsyncs and transform/checkpoint fsyncs at the specific sites. Everything is gated on `commitMode != NOSYNC` (NOSYNC default unaffected) and SYNC-only for the per-commit fsync (`!async`; ASYNC stays non-blocking).

**Tech Stack:** Java, the SP0/SP1 `crash/` harness, Maven. The P2 probe (`Phase2DurabilityProbeTest`) flips from "asserts loss" to "asserts zero loss" as B1's teeth.

**Spec:** `docs/superpowers/specs/2026-06-22-crash-consistency-design.md` §5. Exact current code per site was extracted and is inlined.

**Risk note:** This touches the hot commit path. Each task ends with a broad regression run. Recommended pre-merge suites (whole phase): `O3Test`, squash/attach/dedup/convert/WAL, plus SYNC-mode runs.

---

## Task order (risk-ascending after the core)
B1 (core, flips P2 probe) → B2-1 (partition dir) → B2-3 (`_todo`) → B3-8 (checkpoint) → B3-4 (native→parquet) → B3-5 (O3→parquet) → B3-7 (ALTER TYPE) → B2-2 (`_meta` swap, MED) → B3-6 (parquet→native, MED).

---

## Task B1: fsync-on-extend in Memory.sync (the core durability fix)

**Files:** Modify `core/src/main/java/io/questdb/cairo/vm/MemoryCMARWImpl.java`, `core/src/main/java/io/questdb/cairo/vm/MemoryPMARImpl.java`. Test: flip `core/src/test/java/io/questdb/test/cairo/crash/Phase2DurabilityProbeTest.java` + a focused durability test.

- [ ] **Step 1: flip the P2 probe to assert ZERO loss (failing test).** In `Phase2DurabilityProbeTest`, replace the `lostOrThrew`/`Assert.assertTrue("...must NOT durably keep...")` block with: after `crashAndReopen()`, `assertSyncDurable("p", "s", all);` (all committed rows present and correct). Update the class Javadoc: it now asserts SYNC durability (B1 makes the extend durable). Keep the `setProperty(CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, ...)` small-page override (still needed to force an extend) and the SYNC mode. Run it BEFORE the fix to confirm it FAILS (rows lost):
`cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=Phase2DurabilityProbeTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD|row count"`
Expected: FAIL (fewer rows than `all` after crash) — proves the bug exists and the test now demands the fix.

- [ ] **Step 2: implement the hook in `MemoryCMARWImpl`.**
Add field (near `minMappedMemorySize`): `private long lastSyncedSize = 0;`
Replace `sync` (~314):
```java
    public void sync(boolean async) {
        ff.msync(pageAddress, size, async);
        // In SYNC mode, also make a file EXTEND durable: msync flushes data pages but not the
        // inode size after a posix_fallocate/ftruncate grow. fsync the fd when the file grew
        // since the last sync, so a crash cannot lose the just-committed extent (P2).
        if (!async && size > lastSyncedSize) {
            ff.fsync(fd);
            lastSyncedSize = size;
        }
    }
```
In `map(...)` (~428), after `this.size`/`this.appendAddress` are set and before the LOG block, add `this.lastSyncedSize = this.size;`.
In `truncate()` (~351), after `this.size = sz;`, add `lastSyncedSize = sz;`.
In `swapState(...)` (~259), swap `lastSyncedSize` alongside the other fields:
```java
        long tLastSynced = this.lastSyncedSize;
        this.lastSyncedSize = other.lastSyncedSize;
        other.lastSyncedSize = tLastSynced;
```
(`size` only advances after `TableUtils.allocateDiskSpace` succeeds, so `size > lastSyncedSize` is exactly "the file was extended since last fsync". Confirm `fd` is valid where `pageAddress != 0`.)

- [ ] **Step 3: implement the hook in `MemoryPMARImpl`.**
Add field (near `madviseOpts`): `private long lastSyncedSize = 0;`
Replace `sync` (~141):
```java
    public void sync(boolean async) {
        if (pageAddress != 0) {
            ff.msync(pageAddress, getPageSize(), async);
            if (!async) {
                // File size after mapping page `mappedPage` is (mappedPage+1)*extendSegmentSize
                // (each mapPage posix_fallocates that length). fsync the extend in SYNC mode.
                long currentFileSize = (long) (mappedPage + 1) * getExtendSegmentSize();
                if (currentFileSize > lastSyncedSize) {
                    ff.fsync(fd);
                    lastSyncedSize = currentFileSize;
                }
            }
        }
    }
```
In `of(...)` (~121), after `fd = TableUtils.openFileRWOrFail(...)`, add `this.lastSyncedSize = 0;`.
In `truncate()` (~157), after the remap of page 0, add `lastSyncedSize = getExtendSegmentSize();`.
In `switchTo(...)` (~133), after `this.fd = fd;`, add `this.lastSyncedSize = 0;`.

- [ ] **Step 4: run.** P2 probe now GREEN (zero loss). Then broad SYNC regression — the existing SYNC commit tests must still pass (now doing extra fsyncs):
`cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=Phase2DurabilityProbeTest,VarcharPowerLossCorruptionTest,VarcharPowerLossFuzzTest -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|BUILD"`
Also run the whole crash package + a commit-mode suite (find one: `grep -rln "CommitMode.SYNC" core/src/test | head`). Expect green.

- [ ] **Step 5: commit** `fix(core): fsync file extends in SYNC mode (Memory.sync) so committed data survives crash`

**Out of scope (documented gaps, not the main commit path):** WAL writer raw msyncs (`WalWriter` 1875/1892), O3 frame-copy raw msyncs in `ContiguousFile*FrameColumn` (the `copyData` path already fsyncs). Note in the commit body; carry to backlog.

---

## Task B2-1: fsync new partition directory (openPartition)

**File:** `core/src/main/java/io/questdb/cairo/TableWriter.java` (`openPartition` ~8778). Test: a crash test that a new-partition SYNC commit survives (covered partly by B1's probe; add a partition-boundary variant if cheap).

- [ ] Apply after the `ff.mkdirs(path.slash(), mkDirMode)` success check, before the column loop (reuse the existing dir-fsync idiom from TableUtils ~2014):
```java
        if (!Os.isWindows() && configuration.getCommitMode() != CommitMode.NOSYNC) {
            final long partDirFd = TableUtils.openRONoCache(ff, path.slash$(), LOG);
            if (partDirFd != -1) {
                ff.fsyncAndClose(partDirFd);
            }
            final long rootDirFd = TableUtils.openRONoCache(ff, path.trimTo(pathSize).$(), LOG);
            if (rootDirFd != -1) {
                ff.fsyncAndClose(rootDirFd);
            }
        }
```
(Verify `path` is the partition dir at that point and `path.trimTo(pathSize)` is the table root; restore `path` after. Always-fsync (even if dir pre-existed) is safe and simpler than detecting new dirs.) Gating: `!Os.isWindows() && commitMode != NOSYNC`. Risk LOW.
- [ ] Test (existing O3/partition suites for regression) + commit `fix(core): fsync new partition directory before commit`.

## Task B2-3: fsync `_todo` recovery log

**File:** `TableWriter.java` (`writeRestoreMetaTodo` ~13966). After `todoMem.sync(false)`:
```java
        if (configuration.getCommitMode() != CommitMode.NOSYNC) {
            path.concat(TODO_FILE_NAME);
            final long todoFd = TableUtils.openRONoCache(ff, path.$(), LOG);
            if (todoFd != -1) {
                ff.fsyncAndClose(todoFd);
            }
            path.trimTo(pathSize);
        }
```
(Confirm `TODO_FILE_NAME` constant + `path` base. Runs between the two `_meta` renames — correct ordering.) Risk LOW. Commit `fix(core): fsync _todo before relying on it for meta recovery`.

## Task B3-8: checkpoint recovery — global sync before rmdir

**File:** `core/src/main/java/io/questdb/cairo/DatabaseCheckpointAgent.java` (`recover()` ~933, before `ff.rmdir(srcPath)` ~950). Insert after `finalizeParallelTasks()`/restore-complete and before the rmdir:
```java
        if (ff.sync() != 0) {
            LOG.error().$("sync() failed during checkpoint recovery [errno=").$(ff.errno()).I$();
        }
```
(Mirrors the existing `ff.sync()` at checkpoint CREATE ~543. Log-not-throw so recovery still completes.) Gating: always (recovery path). Risk LOW. Commit `fix(core): sync restored files before deleting checkpoint dir on recovery`.

## Task B3-4: native→parquet — fsync data.parquet before _pm

**File:** `core/src/main/java/io/questdb/cairo/TableUtils.java` (`produceParquetFromNative` ~2009, inside the `commitMode != NOSYNC` block, BEFORE `ff.fsync(parquetMetaFd)`):
```java
            setPathForParquetPartition(other.trimTo(pathSize), timestampType, partitionBy, partitionTimestamp, parquetNameTxn);
            final long parquetDataFd = TableUtils.openRONoCache(ff, other.$(), LOG);
            if (parquetDataFd != -1) {
                ff.fsyncAndClose(parquetDataFd);
            }
            // then existing: ff.fsync(parquetMetaFd); ...
```
(Confirm the `setPathForParquetPartition` arg names + `other`/`parquetNameTxn` in scope; the same helper is already called a few lines later.) Risk LOW. Commit `fix(core): fsync data.parquet before _pm in native->parquet convert`.

## Task B3-5: O3→parquet — fsync data.parquet before _pm

**File:** `core/src/main/java/io/questdb/cairo/O3PartitionJob.java` (~578, before `partitionUpdater.syncParquetMeta()`). Move the `txnName` definition (`isRewrite ? txn : srcNameTxn`) above the sync block, then:
```java
        if (cairoConfiguration.getCommitMode() != CommitMode.NOSYNC) {
            path.of(pathToTable);
            setPathForParquetPartition(path, timestampType, partitionBy, partitionTimestamp, txnName);
            final FilesFacade ff = cairoConfiguration.getFilesFacade();
            final long parquetDataFd = TableUtils.openRONoCache(ff, path.$(), LOG);
            if (parquetDataFd != -1) {
                ff.fsyncAndClose(parquetDataFd);
            }
            partitionUpdater.syncParquetMeta();
        }
```
(Verify scope of `pathToTable`/`path`/`timestampType`/`partitionBy`/`partitionTimestamp`.) Risk LOW-MED. Commit `fix(core): fsync data.parquet before _pm in O3->parquet update`.

## Task B3-7: ALTER COLUMN TYPE — fsync converted files before close

**File:** `core/src/main/java/io/questdb/griffin/ConvertOperatorImpl.java` (`cthConvertPartitionHandler`, on the success path, BEFORE `closeFds(...)`):
```java
            if (configuration.getCommitMode() != CommitMode.NOSYNC) {
                if (dstDataFd != -1) {
                    ff.fsync(dstDataFd);
                }
                if (dstFixFd != -1) {
                    ff.fsync(dstFixFd);
                }
            }
```
(Data before aux; on success only; runs in the worker thread where fds are still open. `configuration` is a field.) Risk LOW-MED. Commit `fix(core): fsync converted column files before commit in ALTER COLUMN TYPE`.

## Task B2-2: _meta swap-rename fsync (MED — care)

**File:** `TableWriter.java`. (a) In `rewriteMetadata` after `ddlMem.sync(false)` (~12410), fsync `_meta.swp` by path (`path.trimTo(pathSize).concat(META_SWAP_FILE_NAME)` + optional `.index`, openRONoCache + fsyncAndClose, restore path). (b) In `rewriteAndSwapMetadata`, after `renameMetaToMetaPrev()` and after `renameSwapMetaToMeta()`, fsync the table root dir (`openRONoCache(ff, path.trimTo(pathSize).$(), LOG)` + fsyncAndClose), gated `!Os.isWindows() && commitMode != NOSYNC`. CAREFULLY preserve `path` shared-state trim ordering. Risk MED — run the DDL/alter suites. Commit `fix(core): fsync _meta.swp + table dir across meta swap-rename`.

## Task B3-6: parquet→native — fsync columns + dir (MED — care)

**File:** `TableWriter.java` (`produceNativeFromParquet` finally block ~10930). BEFORE `other.trimTo(pathSize)`: for each column fd pair, in `commitMode != NOSYNC`, `ff.fsync(dstDataFd)` then `ff.fsync(dstAuxFd)` before `ff.close`; then fsync the new native partition dir (`other.trimTo(newPartitionDirLen).slash$()`, openRONoCache + fsyncAndClose). Preserve the error-path rmdir. Risk MED — run convert/parquet suites. Commit `fix(core): fsync native columns + dir before commit in parquet->native convert`.

---

## Self-review notes
- **Spec §5 coverage:** B1→Task B1 (the core, P2-probe teeth); B2→B2-1/B2-2/B2-3; B3→B3-4..B3-8. All map-to-extraction exact code.
- **Gating discipline:** per-commit fsync is SYNC-only (`!async`); all dir/transform fsyncs gated `commitMode != NOSYNC` (+ `!Os.isWindows()` for dir fsyncs). NOSYNC default and ASYNC non-blocking semantics preserved.
- **Deferred:** WAL writer + O3 frame-copy raw-msync fsync gaps (out of the main commit path; note for SP3/WAL work). A native `fdatasync` (cheaper than `fsync`) + dirty-flag-gated/`sync_file_range` batching is the **Goal-2** follow-up — SP2 keeps the additions cleanly gated/flagged so Goal 2 can optimize them.
- **Risk:** B1 is architecturally central but mechanically simple and self-covering; the `lastSyncedSize` reset points (map/of/truncate/swapState/switchTo) are the correctness-critical detail (pool reuse). B2-2/B3-6 are MED (shared `path` state / `finally` ordering) — sequence last, each with a regression run.
