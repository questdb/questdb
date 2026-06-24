# SP3 (Phase C — Integrity) Implementation Plan — no-format-change subset

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Close the highest-value crash-integrity gaps that require NO on-disk format version bump: a full-record body checksum on `_txn` (with A/B fallback), the missing V2 sequencer part-file sync, and an O(1) data-file cross-check that catches a fully-zeroed multi-page torn aux tail.

**Architecture:** Additive, back-compatible. `_txn` gets a body checksum written into its existing 12-byte record gap (`[+116,+128)`); `0` = "absent" so old files open unverified. On read, a checksum mismatch falls back to the other A/B buffer before throwing. The V2 txnlog gains the missing `txnPartMem.sync()`. The three var-length drivers gain a one-syscall `ff.length(dataFd)` cross-check — but ONLY if proven free of false positives on null / column-top / empty columns.

**Tech Stack:** QuestDB cairo, Java, existing CrashFaultFilesFacade harness.

**EXPLICITLY DEFERRED to a dedicated format-version PR (do NOT implement here):** `_cv` body checksum (needs header 40→56 + migration + `META_OFFSET_VERSION` bump), WAL-e per-record CRC (needs version-gated footer), txnlog per-record CRC in RESERVED (needs version gate). These touch migration machinery and cross-version compat; they must not be rushed.

**Cross-cutting rules (every task):**
- Branch `nw_varchar_power_loss`. Use EXPLICIT comma-separated test class names — `-Dtest="pkg.*"` runs ZERO tests under this surefire config (false-green).
- Default commit mode is NOSYNC; SYNC-gated code is invisible to default tests — test in SYNC where relevant.
- Any "pre-existing failure" claim must be proven by bisecting against `10e445498f` (B1). Three such claims this session were all real regressions.

---

### Task 1: V2 sequencer part-file sync (correctness, no format change)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/seq/TableTransactionLogV2.java` (`addEntry` ~129-148, `sync0`, and the metadata-change path ~151-163)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/` (new or existing sequencer test)

**Context:** V2 `addEntry` writes the 60-byte record to `txnPartMem` (the part file), then `storeFence`, then updates `maxTxn` in `txnMem` (the header file) and calls `sync0()`. `sync0()` syncs only `txnMem` — NOT `txnPartMem`. A crash after the header sync but before the part-file page is written back leaves a valid-looking `maxTxn` pointing at a zeroed/partial record (bogus walId/segmentId/segmentTxn). V1 is immune (single file). V2 is opt-in (`getDefaultSeqPartTxnCount()==0` ⇒ V1 default), so blast radius is small, but the fix is cheap and removes a real durability hole.

- [ ] **Step 1 — Inspect.** Read `TableTransactionLogV2.addEntry`, `sync0`, `beginMetadataChangeEntry`/`endMetadataChangeEntry`, and `TableTransactionLog.endMetadataChangeEntry`/`fullSync`. Confirm exactly which memories `sync0`/`fullSync` flush and that `txnPartMem` is omitted. Confirm the commit-mode plumbing (is there a `commitMode` field? what does V1's `sync0` do?).

- [ ] **Step 2 — Fix ordering.** Make the part file durable BEFORE the header that points to it, so a reader seeing `maxTxn=N` is guaranteed part-file record N is durable. In `sync0()` (and `fullSync`), sync `txnPartMem` first, then `txnMem`. Use the same async/sync flag V1 uses (`commitMode == CommitMode.ASYNC`). Apply to BOTH the data-entry path and the metadata-change path. Do not change record layout.

- [ ] **Step 3 — Test (crash-style).** Add a test that, in SYNC mode with V2 enabled (`cairo.default.sequencer.part.txn.count` > 0), appends several txns and asserts the part-file record for the latest `maxTxn` is fully durable (all fields non-zero / correct) — i.e. the part-file sync happened. If the existing crash harness can model the two-file durability split, prove the OLD code drops the record and the NEW code keeps it; otherwise assert `txnPartMem.sync` is invoked in the correct order (e.g. via the FilesFacade msync/fsync recorder). Run it explicitly; confirm green.

- [ ] **Step 4 — Regression + commit.** Run the WAL sequencer suites by explicit class name (e.g. `TableTransactionLogTest` and any `Sequencer*`/`WalWriter*` that exercise V2). Confirm green. Commit: `fix(wal): sync V2 sequencer part-file before header so maxTxn never points at an undurable txn record`.

---

### Task 2: `_txn` full-record body checksum with A/B fallback (high value, no format change)

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/TableUtils.java` (add `TX_OFFSET_BODY_CHECKSUM_64 = 116`; add `calculateTxnBodyChecksum(...)`; ensure `resetTxn` writes 0 to the gap)
- Modify: `core/src/main/java/io/questdb/cairo/TxWriter.java` (`commit()` fast path ~180-213; `commitFullRecord`/`finishABHeader` ~644-709; `resetLagAppliedRows`/`resetLagValuesUnsafe` ~361-372)
- Modify: `core/src/main/java/io/questdb/cairo/TxReader.java` (`unsafeLoadAll` ~615-651; `unsafeLoadBaseOffset` ~653-673)
- Test: `core/src/test/java/io/questdb/test/cairo/TxnTest.java` (extend; `testLoadTxn` with the binary resource must still pass) + a crash test under `core/src/test/java/io/questdb/test/cairo/crash/`

**Context — exact format (from extraction):**
- File header 64 bytes. A/B selected by `TX_BASE_OFFSET_VERSION_64`(=0) parity (even→A via `TX_BASE_OFFSET_A_32`=8, odd→B via `TX_BASE_OFFSET_B_32`=32); each area has its own base offset + symbols-size + partitions-size in the header.
- Record body from `baseOffset`: fixed fields 0..115, then **12-byte UNUSED gap `[116,128)`** (proven unused: `resetTxn` skips it, both commit paths skip it, the binary test resource is zero there), then `TX_OFFSET_MAP_WRITER_COUNT_32`=128, the symbol-count pairs, an int partition-table byte length, then the partition records.
- Existing `TX_OFFSET_CHECKSUM_32`=88 covers ONLY lag fields. The whole body (transient/fixed row counts, min/max ts, struct/data/partition/column/truncate versions, symbol counts, partition table) is unprotected.

- [ ] **Step 1 — Helper + constant.** In `TableUtils`: add `public static final int TX_OFFSET_BODY_CHECKSUM_64 = 116;` (8 of the 12 gap bytes; leave `[124,128)` reserved/zero). Add `calculateTxnBodyChecksum(long baseAddr, long recordSize)` computing a 64-bit checksum over the active record area `[0, recordSize)` EXCLUDING the 8 checksum bytes themselves at `[116,124)` (treat them as 0 during compute). `recordSize` = `TX_RECORD_HEADER_SIZE + symbolBytes + 4 + partitionBytes` (the full committed record). Define the sentinel: stored value `0` ⇒ "absent, skip verify"; if a real checksum computes to `0`, store `1` instead (document this).

- [ ] **Step 2 — Test the helper (TDD).** Unit-test `calculateTxnBodyChecksum`: deterministic; changes when any covered byte changes; ignores the 8 checksum bytes; never returns 0 (maps 0→1). Run explicitly, watch it fail, then it passes after Step 1.

- [ ] **Step 3 — Write side.** In `TxWriter`, AFTER the body (lag values, symbol counts, partition table) is fully written and BEFORE `storeFence` + version bump, compute `calculateTxnBodyChecksum(baseAddr, recordSize)` and store it at `baseOffset + TX_OFFSET_BODY_CHECKSUM_64`. Do this in BOTH the fast `commit()` path and `commitFullRecord`. CRITICAL: `resetLagAppliedRows`/`resetLagValuesUnsafe` mutate the current record in place (and update the lag checksum) — they MUST recompute and rewrite the body checksum too, else it goes stale. `resetTxn` must write 0 to `[116,124)` (absent) for freshly-created files.

- [ ] **Step 4 — Read/verify side with A/B fallback.** In `TxReader.unsafeLoadAll`, after loading the record for the version-selected area and before the final version re-check: read the stored checksum at `baseOffset + TX_OFFSET_BODY_CHECKSUM_64`; if `== 0` skip (old/absent). Else recompute over the loaded area; on MATCH proceed; on MISMATCH attempt the OTHER area (`otherVersion = version ^ 1`; read its base offset/size from the header A/B pointer; load+verify it); if the other area verifies, use it; if BOTH fail (or the other is also absent+inconsistent), throw `CairoException.critical(...).put("_txn body checksum mismatch in both A and B areas ...")`. Do NOT spin/retry on a stable-version mismatch (that's a permanent tear, not a writer race — the version-change check already handles the race).

- [ ] **Step 5 — Back-compat test.** `testOpenOldFormatTxn_noBodyChecksum`: build/open a `_txn` with `[116,124)`=0 (the existing binary resource files already are) and assert it loads correctly with no throw. Confirm `testLoadTxn` still passes.

- [ ] **Step 6 — Torn-body crash tests.** Using the crash harness: (a) `testTornTxnBodyDetectedAndRecovered` — two committed areas, then corrupt the version-selected area's body (e.g. zero `fixedRowCount` or a partition record) WITHOUT fixing its checksum; assert the reader falls back to the other area and reads the prior correct state. (b) `testTornTxnBodyBothAreasCorrupt` — corrupt both areas' bodies; assert a `CairoException` (never a silent wrong row count). (c) `testBodyChecksumValidAfterFastPathAndResetLag` — two slow commits + many fast commits + a `resetLagAppliedRows`; assert every resulting `_txn` verifies. Run all explicitly; green.

- [ ] **Step 7 — Hot-path cost note + regression.** The checksum now runs over the full record (incl. partition table) on every commit. Note the cost in the commit comment; if the partition table is large this is O(partitions) per commit — acceptable (commit already touches it), but call it out. Run `TxnTest`, `TableWriterTest` (a subset), and a WAL apply suite explicitly; green. Commit: `feat(core): add _txn body checksum with A/B fallback to detect torn transaction records (back-compatible, no format bump)`.

---

### Task 3: data-file cross-check for fully-zeroed multi-page torn aux — PROVE-SAFE-OR-DROP (low value, no format change)

**Files:**
- Modify (only if proven safe): `StringTypeDriver.setAppendPosition`, `VarcharTypeDriver.setAppendPosition`, `arr/ArrayTypeDriver.setAppendPosition`
- Test: the crash package + a null/column-top safety test

**Context:** The O(1) monotonicity guards (just fixed for the page-boundary SIGSEGV) compare only the last entry vs the previous. A whole zeroed aux page makes both read 0 → `0 < 0` is false → the tear passes undetected. Proposed cheap check: when `pos > 0` and the last entry's data-end reads 0 while the DATA file has bytes, the aux tail is torn. This is O(1) and paging-safe (operates on the data fd, no `jumpTo`).

- [ ] **Step 1 — PROVE NO FALSE POSITIVE FIRST (gate).** Before touching product code, write tests that build HEALTHY columns whose last entry legitimately has data-end 0 or whose data file is non-empty with a zero last offset: (a) an all-NULL string/varchar/array column; (b) a column-top column (added mid-table, early rows absent); (c) an empty column; (d) a single-row column. Determine empirically: for a healthy column with `pos > 0` and `dataFileLength > 0`, can the last entry's data-end ever be 0? Recall the ARRAY null-prefix false-positive earlier this session — nulls write 0 data bytes. If ANY healthy case has `lastDataEnd == 0 && dataFileLength > 0`, the check is UNSAFE → **DROP Task 3**, document why in the plan/memory, and stop. Only proceed if the check is provably false-positive-free (likely requires also gating on data-file length vs the previous entry, not just `==0`).

- [ ] **Step 2 — Implement (only if Step 1 proves safe).** Add the minimal cross-check to all three `setAppendPosition` guards, reusing each driver's fd/data-mem length accessor; throw the same "aux vector is damaged" critical exception with a distinct message (`possible multi-page torn aux tail`). Keep it O(1); no `jumpTo`.

- [ ] **Step 3 — Crash test.** Add a crash test that zeroes a whole aux page (multi-entry tail) and asserts the new check throws; confirm the existing single-entry torn-tail and all healthy-column tests (Step 1) stay green. Run the whole crash package + driver tests explicitly.

- [ ] **Step 4 — Commit (or record the drop).** If implemented: `feat(core): detect fully-zeroed multi-page torn aux tail via O(1) data-file cross-check`. If dropped: record the false-positive finding in memory and the plan, no code change.

---

### Final review (after all tasks)

- [ ] Dispatch a whole-SP3 reviewer (opus) over the diff since the SP2 tip: correctness of the A/B fallback (no false-positive throw on healthy multi-commit tables), the resetLag checksum-refresh, the V2 sync ordering, and the Task-3 safety proof. Bisect any failing test against B1. Then run the full guard+crash+txn regression by explicit class name and update memory.
