# SP1 — Phase A: detect-don't-corrupt (Bar 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (or executing-plans). Steps use checkbox (`- [ ]`).

**Goal:** Make every torn/short-file recovery path fail loudly (clean `CairoException`) instead of silently corrupting or SIGBUS-ing — Bar 1, in all commit modes.

**Architecture:** Recovery-side validation only (no write/sync-protocol change), so it is mode-independent and safe broadly. Each product-code fix lands with a harness crash test (red-before/green-after) using the SP0 harness, which is first extended with O3-path coverage + the exhaustive crash-point driver.

**Tech Stack:** Java, JUnit4, QuestDB test framework + the SP0 `crash/` harness, Maven.

**Spec:** `docs/superpowers/specs/2026-06-22-crash-consistency-design.md` §4 (SP1). Exact current code for every site was extracted and is inlined below.

---

## File structure

Harness (test): extend `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java` and `AbstractCrashConsistencyTest.java`.
Product fixes (src/main): `arr/ArrayTypeDriver.java`, `VarcharTypeDriver.java`, `idx/BitmapIndexWriter.java`, `O3CopyJob.java`, `AbstractBitmapIndexReader.java`, `AbstractPostingIndexReader.java`, `TableReader.java`.
New tests: `ArrayCrashConsistencyTest`, `SyncOrderCrashConsistencyTest`, `MapLengthGuardCrashConsistencyTest`, `VarcharFdAccessorTest` under `crash/` (or `cairo/`).

---

## Task 0: Harness prep — open-family coverage + forEachCrashPoint

**Files:** Modify `core/src/test/java/io/questdb/test/cairo/crash/CrashFaultFilesFacade.java`, `AbstractCrashConsistencyTest.java`. Test: `CrashFaultFilesFacadeTest.java`.

- [ ] **Step 1: failing test** — append to `CrashFaultFilesFacadeTest`:

```java
    @Test
    public void testOpenAppendIsTrackedForDurability() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot4").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("d.d")) {
            long fd = ff.openAppend(path.$()); // append-opened file must be tracked
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 8, (byte) 9);
                Assert.assertEquals(8, ff.append(fd, buf, 8));
                ff.fsync(fd); // durable = 8
                Assert.assertEquals(8, ff.append(fd, buf, 8)); // grow to 16, not fsynced
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            ff.crash(dir);
            Assert.assertEquals(8, ff.length(path.$())); // rolled back to fsync'd size
        }
    }
```

- [ ] **Step 2: run, expect FAIL** (openAppend not overridden → fd untracked → crash truncates to 0, asserts 8≠0):
`cd ~/claude/wt/oss/varchar-corruption && mvn test -pl core -Dtest=CrashFaultFilesFacadeTest#testOpenAppendIsTrackedForDurability -Dsurefire.failIfNoSpecifiedTests=false 2>&1 | grep -E "Tests run|cannot find symbol"`

- [ ] **Step 3: implement** — in `CrashFaultFilesFacade`, override the remaining open-family methods to register fds, mirroring `openRW`/`openRO` (use the existing `toAbsPath(LPSZ)`/`key(LPSZ)` helper for the path key):

```java
    @Override
    public long openAppend(LPSZ name) {
        long fd = super.openAppend(name);
        if (fd > -1) {
            fdToPath.put(fd, key(name));
        }
        return fd;
    }

    @Override
    public long openCleanRW(LPSZ name, long size) {
        long fd = super.openCleanRW(name, size);
        if (fd > -1) {
            fdToPath.put(fd, key(name));
        }
        return fd;
    }
```
(If the facade inlined the key normalization rather than exposing a `key(LPSZ)` helper, factor a `private String key(LPSZ name)` used by openRW/openRO/openAppend/openCleanRW/tornTail so ALL sites normalize identically. Confirm `openAppend`/`openCleanRW` signatures via `grep -n "openAppend\|openCleanRW" core/src/main/java/io/questdb/std/FilesFacade.java`.)

Then add `forEachCrashPoint` to `AbstractCrashConsistencyTest`:

```java
    /**
     * Re-run {@code workload} crashing after each successive durability op (1..maxPoints), reopening
     * and running {@code assertion} each time, until the workload completes untripped. The proof tool
     * for ordering fixes: it finds the window where a pointer is durable but its data is not.
     * Releases the distressed writer's mappings (releaseAllWriters) before truncating, so no mapping
     * is held over crash().
     */
    protected void forEachCrashPoint(Runnable seed, Runnable workload, Runnable assertion, int maxPoints) {
        for (int k = 1; k <= maxPoints; k++) {
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.clear();
            crashFf.reset();
            seed.run();
            markDurableBaseline();
            boolean tripped = false;
            crashFf.armCrashAt(k);
            try {
                workload.run();
            } catch (CrashSimulationError e) {
                tripped = true;
            } finally {
                crashFf.armCrashAt(-1);
            }
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            crashFf.crash(engine.getConfiguration().getDbRoot());
            assertion.run();
            if (!tripped) {
                LOG.info().$("forEachCrashPoint completed untripped at k=").$(k).$();
                return;
            }
        }
        Assert.fail("forEachCrashPoint exhausted " + maxPoints + " points without completing");
    }
```
Add a `@Test forEachCrashPoint` smoke test in a concrete runner (e.g. extend `CrashConsistencySelfCheckTest`) under SYNC mode: seed a table, workload inserts a few rows, assertion = `assertNoSilentCorruption`. (Under NOSYNC there are no sync ops, so it would complete at k=1 trivially — use SYNC so the counter advances.)

- [ ] **Step 4: run** facade test + smoke test → green.
- [ ] **Step 5: commit** `test(core): harness open-family coverage + forEachCrashPoint driver`

---

## Task A1: ARRAY torn-tail guard (setAppendPosition + getDataVectorSizeAtFromFd)

**Files:** Modify `core/src/main/java/io/questdb/cairo/arr/ArrayTypeDriver.java`. Test: new `core/src/test/java/io/questdb/test/cairo/crash/ArrayCrashConsistencyTest.java`.

- [ ] **Step 1: failing test** — `ArrayCrashConsistencyTest` (mirror `VarcharCrashConsistencyTest`): create `create table a (ts timestamp, arr double[]) timestamp(ts) partition by none`, insert 10 rows of a small double[] (e.g. `ARRAY[1.0,2.0,3.0]`), `markDurableBaseline()`, `tornTail` the last aux entry's data-offset word (bytes 0-7 of entry `(rows-1)*16`, i.e. `crashFf.tornTail(auxPath, (rows-1)*16L, 8L)` — ARRAY stores the offset in the FIRST 8 bytes of the 16-byte entry), `crashAndReopen()`, attempt an append, assert `detected` (CairoException|CairoError, "array aux vector is damaged") and row 0 not silently wrong. Build the `.i` path with `TableUtils.iFile(path, "arr", TableUtils.COLUMN_NAME_TXN_NONE)`. (Confirm the array aux offset is in bytes 0-7 by reading `ArrayTypeDriver.readDataOffset`: `Unsafe.getLong(auxEntryAddress) & OFFSET_MAX` — yes, first 8 bytes.)

- [ ] **Step 2: run, expect FAIL** (no guard yet → silent overwrite → `detected` false).

- [ ] **Step 3: implement** — in `ArrayTypeDriver.setAppendPosition`, insert AFTER `long dataVectorSize = calcDataOffsetEnd(auxEntryPtr);`:

```java
        // Crash-consistency guard (mirrors VarcharTypeDriver). Data offsets are monotonic; the last
        // row's data start must be >= the previous row's data end. A lower value means the aux tail
        // was torn/partially flushed - fail loudly instead of placing the cursor inside committed data.
        if (pos > 1) {
            long lastDataOffset = readDataOffset(auxEntryPtr);
            long prevDataVectorSize = calcDataOffsetEnd(auxEntryPtr - ARRAY_AUX_WIDTH_BYTES);
            if (lastDataOffset < prevDataVectorSize) {
                throw CairoException.critical(0)
                        .put("array aux vector is damaged, possible torn write on the last entry [pos=").put(pos)
                        .put(", lastDataOffset=").put(lastDataOffset)
                        .put(", prevDataVectorSize=").put(prevDataVectorSize)
                        .put(']');
            }
        }
```
And replace the body of `ArrayTypeDriver.getDataVectorSizeAtFromFd` to reject a torn zero offset (non-null, non-first row):

```java
        final long offset = readLong(ff, auxFd, auxFileOffset) & OFFSET_MAX;
        final int size = readInt(ff, auxFd, auxFileOffset + Long.BYTES);
        if (row > 0 && size > 0 && offset == 0) {
            throw CairoException.critical(ff.errno())
                    .put("Invalid data offset read from array aux file, possible torn write [auxFd=").put(auxFd)
                    .put(", row=").put(row).put(", offset=").put(offset).put(", size=").put(size)
                    .put(", fileSize=").put(ff.length(auxFd)).put(']');
        }
        return offset + size;
```
(`CairoException` already imported. `readDataOffset(long)` and `calcDataOffsetEnd(long)` are private members usable here.)

- [ ] **Step 4: run** ArrayCrashConsistencyTest → green; also run existing `mvn test -pl core -Dtest=ArrayTypeDriverTest -Dsurefire.failIfNoSpecifiedTests=false` to confirm no regression.
- [ ] **Step 5: commit** `fix(core): guard ARRAY setAppendPosition / fd accessor against torn-aux power loss`

---

## Task A2: fix the two inverted sync orders (data before pointer)

**Files:** Modify `core/src/main/java/io/questdb/cairo/idx/BitmapIndexWriter.java`, `core/src/main/java/io/questdb/cairo/O3CopyJob.java`. Test: `SyncOrderCrashConsistencyTest` (forEachCrashPoint over an O3 commit of an indexed/var column under SYNC, assert no silent corruption at any crash point) — plus rely on existing O3/index suites for regression.

- [ ] **Step 1: failing/cover test** — `SyncOrderCrashConsistencyTest`: build a table with an indexed SYMBOL column and a VARCHAR column, insert in-order rows, then insert an OUT-OF-ORDER batch (older timestamps) to force the O3 path, wrapped so `forEachCrashPoint(seed, o3workload, assertNoSilentCorruption, N)` runs under `setProperty(CAIRO_COMMIT_MODE,"async")` (ASYNC so msync ordering matters and durability ops are counted). Assert no crash point yields silently wrong committed rows. (This test mainly guards against regressions and exercises the path; the swaps themselves are provably correct.)

- [ ] **Step 2: run** (pre-fix may pass or surface a window; the test's job is to stay green after the swap and exercise the path).

- [ ] **Step 3: implement** — pure swaps (no logic change).
`BitmapIndexWriter.sync` (lines ~424-427):
```java
    public void sync(boolean async) {
        // valueMem (.v, data) before keyMem (.k, pointer): a crash must never leave the key
        // file referencing value-block offsets that are not yet durable. Matches PostingIndexWriter.
        valueMem.sync(async);
        keyMem.sync(async);
    }
```
`O3CopyJob.syncColumns` (the two msync/fsync blocks ~734-748): swap so `dstVar` (data) is synced before `dstFix` (aux), keeping the parameter names (they are shared across the O3 copy chain — do NOT rename) and adding the clarifying comment:
```java
            boolean async = commitMode == CommitMode.ASYNC;
            // Sync data (dstVar = .d, primary) before aux (dstFix = .i, secondary): the aux vector
            // holds offsets into the data vector; a durable aux entry must never point past durable data.
            if (dstVarAddr != 0 && dstVarSize > 0) {
                ff.msync(dstVarAddr, dstVarSize, async);
                if (dstVarFd != -1 && dstVarFd != 0) {
                    ff.fsync(Math.abs(dstVarFd));
                }
            }
            if (dstFixAddr != 0 && dstFixSize > 0) {
                ff.msync(dstFixAddr, dstFixSize, async);
                if (dstFixFd != -1 && dstFixFd != 0) {
                    ff.fsync(Math.abs(dstFixFd));
                }
            }
```

- [ ] **Step 4: run** SyncOrderCrashConsistencyTest + a representative existing suite (`mvn test -pl core -Dtest=O3Test,BitmapIndexTest -Dsurefire.failIfNoSpecifiedTests=false` or similar) → green.
- [ ] **Step 5: commit** `fix(core): sync data before pointer in BitmapIndexWriter / O3CopyJob (data-before-aux)`

---

## Task A3: length-validation at map-to-pointer-size sites (P3: SIGBUS → clean throw)

**Files:** Modify `core/src/main/java/io/questdb/cairo/AbstractBitmapIndexReader.java`, `core/src/main/java/io/questdb/cairo/AbstractPostingIndexReader.java`, `core/src/main/java/io/questdb/cairo/TableReader.java`. Test: `MapLengthGuardCrashConsistencyTest`.

The pattern (from `MemoryCMRImpl.ofWithSizeFromHeader`): before mapping a file to a pointer-derived `size`, compare to `ff.length` and throw if `size > actualLength`. Direction is safe (healthy files are over-allocated ≥ committed). Do the 3 prioritized sites; DEFER WAL site 4 (needs WAL-e validation investigation) and the global `VM_PARANOIA_MODE` promotion (hot-path syscall cost).

- [ ] **Step 1: failing test** — `MapLengthGuardCrashConsistencyTest` (one method per site): build a table with a bitmap-indexed SYMBOL column (site 1a), commit, release, truncate the `.v` value file shorter than its `.k` header claims (use `crashFf` or direct `ff.truncate`), reopen + query → expect a clean `CairoException` ("value file too short"), NOT a SIGBUS/`InternalError`. Analogous methods for the posting `.pv` (2a) and the parquet `data.parquet` (3) if feasible to set up; if parquet setup is heavy, cover 1a+2a now and mark 3 with a `// TODO(SP1): parquet length-guard test` and still apply the product fix.

- [ ] **Step 2: run, expect FAIL** (SIGBUS/InternalError or silent zeros, not a CairoException).

- [ ] **Step 3: implement** the three checks (exact code from extraction):

Site 1a — `AbstractBitmapIndexReader.of`, before the `valueMem.of(...)` at ~line 153 (construct the filename before the trim, mirroring the existing call):
```java
if (valueMemSize > 0) {
    LPSZ vName = BitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn);
    long vActual = configuration.getFilesFacade().length(vName);
    if (valueMemSize > vActual) {
        throw CairoException.critical(0)
                .put("bitmap index value file too short [expected=").put(valueMemSize)
                .put(", actual=").put(vActual).put(", path=").put(vName).put(']');
    }
}
```
Site 2a — `AbstractPostingIndexReader.mapValueMem`, validate before `valueMem.of(...)`:
```java
LPSZ pvName = PostingIndexUtils.valueFileName(basePath, columnName, columnNameTxn, valueFileTxn);
if (valueMemSize > 0) {
    long pvActual = ff.length(pvName);
    if (valueMemSize > pvActual) {
        throw CairoException.critical(0)
                .put("posting index value file too short [expected=").put(valueMemSize)
                .put(", actual=").put(pvActual).put(", path=").put(pvName).put(']');
    }
}
valueMem.of(ff, pvName, valueMemSize, valueMemSize, MemoryTag.MMAP_INDEX_READER);
```
Site 3 — `TableReader` parquet open (after `parquetFileSize` is known, before mapping):
```java
final long parquetActualLength = ff.length(path.$());
if (parquetFileSize > parquetActualLength) {
    throw CairoException.critical(0)
            .put("parquet partition file too short [expected=").put(parquetFileSize)
            .put(", actual=").put(parquetActualLength).put(", path=").put(path).put(']');
}
```
Also apply the same guard to the writer-reopen variants in the same PR: `BitmapIndexWriter.of` (~line 336 and the fd-variant ~265 using `ff.length(valueFd)`) and `PostingIndexWriter.of` reopen (~1122, guarded by `!isInit && valueMemSize > 0`).

- [ ] **Step 4: run** the new test (clean CairoException, no SIGBUS) + existing index/parquet suites for regression.
- [ ] **Step 5: commit** `fix(core): validate index/parquet map size against file length (P3: clean error not SIGBUS)`

---

## Task A4: VARCHAR fd-accessor zero-offset rejection

**Files:** Modify `core/src/main/java/io/questdb/cairo/VarcharTypeDriver.java`. Test: extend `VarcharCrashConsistencyTest` or a small `VarcharFdAccessorTest`.

- [ ] **Step 1: failing test** — drive `getDataVectorSizeAtFromFd` via the frame/conversion path with a torn last aux entry (zeroed offset) and assert it throws "Invalid data offset read from varchar aux file" instead of returning a too-small size. (If wiring the frame path is heavy, a focused unit test that writes a varchar aux file via `VarcharTypeDriver.appendValue`, zeroes the last entry's bytes 8-15, and calls `getDataVectorSizeAtFromFd(ff, fd, lastRow)` asserting the throw is sufficient and deterministic.)

- [ ] **Step 2: run, expect FAIL** (currently returns `0 + size`, no throw).

- [ ] **Step 3: implement** — in `VarcharTypeDriver.getDataVectorSizeAtFromFd`, move the `size` declaration up and add the guard after the `hasNullOrInlinedFlag` early-return:
```java
    if (hasNullOrInlinedFlag(raw)) {
        return dataOffset;
    }
    final int size = (raw >> HEADER_FLAGS_WIDTH) & DATA_LENGTH_MASK;
    // split (non-null, non-inlined) entry at row > 0 must have a positive data offset; 0 means torn
    if (row > 0 && dataOffset == 0) {
        throw CairoException.critical(ff.errno())
                .put("Invalid data offset read from varchar aux file, possible torn write [auxFd=").put(auxFd)
                .put(", row=").put(row).put(", dataOffset=").put(dataOffset).put(", size=").put(size)
                .put(", fileSize=").put(ff.length(auxFd)).put(']');
    }
    return dataOffset + size;
```
BINARY needs nothing (inherits STRING's already-guarded accessors). Symbol `.c` tail cross-check is DEFERRED (false-positive risk in rollback/reopen; needs a dedicated read-only audit fn) — note in the commit body.

- [ ] **Step 4: run** the test + `VarcharTypeDriverTest`, `VarcharConversionTest` regression → green.
- [ ] **Step 5: commit** `fix(core): reject torn zero data-offset in VarcharTypeDriver fd accessor`

---

## Self-review notes
- **Spec §4 coverage:** A1→Task A1; A2→Task A2; A3→Task A3 (sites 1a/2a/3 + writer reopens; WAL site 4 + VM_PARANOIA promotion explicitly DEFERRED with rationale); A4→Task A4 (VARCHAR; BINARY no-op; symbol DEFERRED). Harness prereq (open-family + forEachCrashPoint) → Task 0.
- **Deferred (tracked):** WAL segment-length validation (needs WAL-e investigation), symbol `.c` tail cross-check, global paranoia-assert promotion. Carry to SP2/SP3 backlog.
- **Risk:** A1/A4 mirror the shipped, reviewed VARCHAR/STRING guards (lowest risk). A2 are pure swaps matching existing reference patterns. A3 checks are `requested > actual → throw` (safe direction; parquet site MEDIUM risk noted). Every product change is gated by an existing-suite regression run in Step 4.
- **Order:** Task 0 → A1 → A4 → A2 → A3 (A1/A4 highest-confidence first; A2 needs Task 0's driver; A3 largest/most sites last).
