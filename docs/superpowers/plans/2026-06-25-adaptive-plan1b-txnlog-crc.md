# Adaptive Plan 1b — Sequencer txnlog record CRC (audit #8 integrity)

> Execute via superpowers:subagent-driven-development. Plan 2 of the integrity foundation.

**Goal:** Detect a torn/partially-written V2 sequencer txnlog record on read (→ suspend the table) instead of applying a garbage `walId/segment/rowHi`. Closes the integrity half of audit #8 (the part-before-header sync ordering is already done in `TableTransactionLogV2.sync0()`).

**Design:** Mirror Plan 1's `_event` CRC. Each V2 record already ends with a **reserved trailing `long` at `RESERVED_OFFSET` (currently written as `0L` in `addEntry`/`beginMetadataChangeEntry`)** — use it as the CRC slot. Writer computes `TableUtils.calculateCvAreaChecksum(recordAddr, RESERVED_OFFSET)` over the record body `[0, RESERVED_OFFSET)` and stores it there. Reader (the `TransactionLogCursorImpl`) verifies on each record advance. **Back-compat / magic-gate:** `calculateCvAreaChecksum` never returns 0, so a stored `0` means "legacy record without CRC" → skip verification; non-zero → verify, mismatch → throw `CairoException.critical(METADATA_VALIDATION)`. `RECORD_SIZE` is unchanged (additive, no format bump).

**Files:**
- `core/src/main/java/io/questdb/cairo/wal/seq/TableTransactionLogV2.java` — `addEntry()` and `beginMetadataChangeEntry()`: replace the trailing `txnPartMem.putLong(0L)` with the computed CRC (capture the record start offset before the field writes; compute over `[start, start+RESERVED_OFFSET)`); `TransactionLogCursorImpl`: verify the CRC where it advances to a record (in `hasNext()`/`next` before the getters are used).
- (Constants `RESERVED_OFFSET`, `RECORD_SIZE`, field offsets are in `TableTransactionLogFile`.)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/seq/` (or `.../wal/`) — `TableTransactionLogV2CrcTest` (new) or extend an existing seq test.

**Tasks (TDD):**
1. Writer: store CRC in the reserved slot (both `addEntry` + `beginMetadataChangeEntry`). Test: after commits, the reserved long per record is non-zero and equals the recomputed body checksum.
2. Reader: verify in `TransactionLogCursorImpl` advance; torn record (corrupt a body byte in a `_txnlog` part file) → cursor throws / table suspends on apply; legacy (reserved slot = 0) → reads unverified. Confirm the failure mode end-to-end (a thrown sequencer-cursor error during `ApplyWal2TableJob` suspends the table — verify, like Plan 1 did).
3. Regression: run the sequencer/WAL apply suites green.

**Tests/oracle:** torn txnlog record is loud (suspend), never a silent wrong-`walId`/`rowHi` apply; legacy records still read; `calculateCvAreaChecksum` determinism between write and read sides.

**Notes:** Scope to **V2** (the Enterprise/replication path; audit #8 is V2-specific). V1 keeps record+pointer in one msync'd file and is out of scope here. Confirm whether `endMetadataChangeEntry` / structural-change records also pass through the same reserved-slot write (they use `beginMetadataChangeEntry`).
