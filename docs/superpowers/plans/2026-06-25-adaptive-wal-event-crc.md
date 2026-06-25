# Adaptive Plan 1 — WAL-e Event-Record CRC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-record checksum trailer to WAL-e (`_event`) records so a torn/partially-written event record is detected loudly on read instead of being silently mis-applied. This is the keystone that lets later plans compute "the last intact WAL txn" (the durable frontier) for the `adaptive` commit mode.

**Architecture:** Mirror the already-shipped `_cv` body-checksum pattern (`TableUtils.calculateCvAreaChecksum` + a magic-gated `[MAGIC | xxh3]` trailer) onto each `_event` record. The writer appends the trailer when finalizing a record; the reader verifies it in `WalEventCursor.hasNext()` before parsing fields. Magic-gated ⇒ additive, no format bump, back-compatible (records written before this change lack the magic and are read unverified).

**Tech Stack:** Java 17 (QuestDB `core` module, Maven), `io.questdb.cairo.wal` package, xxh3 checksum helper in `TableUtils`.

**Context for this plan:** This is **Plan 1 of 4** for the adaptive commit mode (spec: `docs/superpowers/specs/2026-06-25-adaptive-commit-mode-design.md`, §13/§5.2). It depends only on primitives already present on the base branch (`nw_sync_batch`): the `_cv` checksum helpers and the crash-test harness. Sibling follow-on plans (write when reached): **1b** txnlog per-record CRC (`#8`, `TableTransactionLogV2` reserved trailing slot), **1c** WAL segment-length validation before mapping (`#6`). Run all commands from the repo root `~/claude/wt/oss/adaptive-commit`.

---

## File Structure

- **Modify** `core/src/main/java/io/questdb/cairo/wal/WalUtils.java` — add two constants: `WALE_CHECKSUM_MAGIC`, `WALE_CHECKSUM_TRAILER_SIZE`.
- **Modify** `core/src/main/java/io/questdb/cairo/wal/WalEventWriter.java` — extract a `finishRecord()` helper that writes the checksum trailer, and call it from the 5 record-emit methods (`appendData`, `appendMatViewInvalidate`, `appendSql`, `appendViewDefinition`, `truncate`).
- **Modify** `core/src/main/java/io/questdb/cairo/wal/WalEventCursor.java` — add `verifyRecordChecksum(...)` and call it from `hasNext()` after framing, before `readRecord()`.
- **Create** `core/src/test/java/io/questdb/test/cairo/wal/WalEventChecksumTest.java` — trailer-present, torn→suspended, legacy→reads.

Responsibilities stay where they already live (write framing in the writer, read framing in the cursor, shared constants in `WalUtils`, the hash in `TableUtils`). No new classes.

---

## Task 1: Add the checksum-trailer constants

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/WalUtils.java` (alongside the other `WALE_*` constants, ~line 77-93)

- [ ] **Step 1: Add the constants**

Add near the existing `WALE_HEADER_SIZE` / `WALE_FORMAT_VERSION` declarations:

```java
// Per-record checksum trailer for _event records, mirroring the _cv body-checksum trailer
// (see TableUtils.CV_CHECKSUM_MAGIC). 8-byte MAGIC followed by an 8-byte xxh3 checksum of the
// record body. Magic-gated for back-compat: records written before this change lack the trailer
// and are read unverified.
public static final long WALE_CHECKSUM_MAGIC = 0x57414C45434B5331L; // 'WALECKS1' (LE on disk)
public static final int WALE_CHECKSUM_TRAILER_SIZE = 2 * Long.BYTES;
```

- [ ] **Step 2: Compile to verify it resolves**

Run: `mvn -q -pl core -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 3: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/wal/WalUtils.java
git commit -m "feat(wal): add WALE checksum-trailer constants (magic + size)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Writer — append the checksum trailer per record

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/WalEventWriter.java` (the 5 record-emit methods + a new private `finishRecord()`)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/WalEventChecksumTest.java`

Each emit method currently ends with this 4-line "finalize" block (e.g. `appendData` lines 341-345):

```java
eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
eventMem.putInt(-1);
appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
```

We replace that block with a single `finishRecord()` call that additionally writes the trailer. Note `appendData`/`appendMatViewInvalidate` write a `WAL_FORMAT_OFFSET_32` format-version int *after* this block — that stays in the caller, untouched.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cairo/wal/WalEventChecksumTest.java`:

```java
package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class WalEventChecksumTest extends AbstractCairoTest {

    @Test
    public void testEventRecordsCarryChecksumTrailer() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            execute("insert into x values ('2024-01-01T00:01:00.000000Z', 2)");
            drainWalQueue();
            TableToken tt = engine.verifyTableName("x");
            byte[] bytes = Files.readAllBytes(findEventFile(tt.getDirName()));
            int magic = countMagic(bytes, WalUtils.WALE_CHECKSUM_MAGIC);
            Assert.assertTrue("expected a checksum trailer per record, got " + magic, magic >= 2);
        });
    }

    // --- helpers ---

    static Path findEventFile(CharSequence tableDirName) throws Exception {
        Path tableDir = Paths.get(engine.getConfiguration().getDbRoot().toString(), tableDirName.toString());
        try (Stream<Path> s = Files.walk(tableDir)) {
            return s.filter(p -> p.getFileName().toString().equals(WalUtils.EVENT_FILE_NAME))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no _event file under " + tableDir));
        }
    }

    static int countMagic(byte[] bytes, long magic) {
        int count = 0;
        for (int i = 0; i + 8 <= bytes.length; i++) {
            long v = 0;
            for (int b = 0; b < 8; b++) {
                v |= (bytes[i + b] & 0xFFL) << (8 * b);
            }
            if (v == magic) {
                count++;
            }
        }
        return count;
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl core test -Dtest=WalEventChecksumTest#testEventRecordsCarryChecksumTrailer`
Expected: FAIL — assertion "expected a checksum trailer per record, got 0" (no trailer written yet).

- [ ] **Step 3: Add the `finishRecord()` helper**

In `WalEventWriter.java`, add the import `import io.questdb.cairo.TableUtils;` and this private method:

```java
// Finalize the current record: append the [MAGIC | xxh3] checksum trailer, back-patch the
// length prefix (length is patched BEFORE hashing so it is covered), write the EOF sentinel,
// index entry, and header max-txn. Body hashed = [startOffset, bodyEnd) (excludes the trailer).
private void finishRecord() {
    final long bodyEnd = eventMem.getAppendOffset();
    final int length = (int) (bodyEnd - startOffset + WALE_CHECKSUM_TRAILER_SIZE);
    eventMem.putInt(startOffset, length);
    final long checksum = TableUtils.calculateCvAreaChecksum(eventMem.addressOf(startOffset), bodyEnd - startOffset);
    eventMem.putLong(WALE_CHECKSUM_MAGIC);
    eventMem.putLong(checksum);
    eventMem.putInt(-1);
    appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
    eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
}
```

- [ ] **Step 4: Replace the 4-line finalize block with `finishRecord()` in all 5 emit methods**

In `appendData` (lines 341-345), `appendMatViewInvalidate` (380-384), `appendSql` (401-405), `appendViewDefinition` (437-441), and `truncate` (543-547), replace:

```java
eventMem.putInt(startOffset, (int) (eventMem.getAppendOffset() - startOffset));
eventMem.putInt(-1);
appendIndex(eventMem.getAppendOffset() - Integer.BYTES);
eventMem.putInt(WALE_MAX_TXN_OFFSET_32, txn);
```

with:

```java
finishRecord();
```

Leave the subsequent `eventMem.putInt(WAL_FORMAT_OFFSET_32, ...)` lines (in `appendData`/`appendMatViewInvalidate`) exactly as they are.

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q -pl core test -Dtest=WalEventChecksumTest#testEventRecordsCarryChecksumTrailer`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/wal/WalEventWriter.java core/src/test/java/io/questdb/test/cairo/wal/WalEventChecksumTest.java
git commit -m "feat(wal): write per-record checksum trailer in WalEventWriter

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Reader — verify the trailer and fail loudly on a torn record

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/wal/WalEventCursor.java` (`hasNext()` ~lines 137-156, new `verifyRecordChecksum`)
- Test: `core/src/test/java/io/questdb/test/cairo/wal/WalEventChecksumTest.java`

- [ ] **Step 1: Write the failing tests**

Add to `WalEventChecksumTest`:

```java
    @Test
    public void testTornEventRecordSuspendsTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            java.nio.file.Path event = findEventFile(tt.getDirName());
            byte[] bytes = Files.readAllBytes(event);
            bytes[WalUtils.WALE_HEADER_SIZE + 12] ^= 0xFF; // corrupt record 0 body (within the checksummed range)
            Files.write(event, bytes);
            drainWalQueue();
            // torn record must be detected on apply and suspend the table, not silently mis-apply.
            assertSql("suspended\ntrue\n", "select suspended from wal_tables() where name = 'x'");
        });
    }

    @Test
    public void testLegacyRecordWithoutTrailerStillReads() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("insert into x values ('2024-01-01T00:00:00.000000Z', 1)");
            TableToken tt = engine.verifyTableName("x");
            java.nio.file.Path event = findEventFile(tt.getDirName());
            byte[] bytes = Files.readAllBytes(event);
            zeroFirstMagic(bytes, WalUtils.WALE_CHECKSUM_MAGIC); // simulate a pre-checksum (legacy) record
            Files.write(event, bytes);
            drainWalQueue();
            assertSql("suspended\nfalse\n", "select suspended from wal_tables() where name = 'x'");
            assertSql("count\n1\n", "select count() from x");
        });
    }

    static void zeroFirstMagic(byte[] bytes, long magic) {
        for (int i = 0; i + 8 <= bytes.length; i++) {
            long v = 0;
            for (int b = 0; b < 8; b++) {
                v |= (bytes[i + b] & 0xFFL) << (8 * b);
            }
            if (v == magic) {
                for (int b = 0; b < 8; b++) {
                    bytes[i + b] = 0;
                }
                return;
            }
        }
        throw new AssertionError("magic not found");
    }
```

- [ ] **Step 2: Run to verify the torn test fails**

Run: `mvn -q -pl core test -Dtest=WalEventChecksumTest#testTornEventRecordSuspendsTable`
Expected: FAIL — table is NOT suspended (`suspended` is `false`); the torn record is currently applied silently.

- [ ] **Step 3: Add `verifyRecordChecksum` and call it from `hasNext()`**

In `WalEventCursor.java`, add the import `import io.questdb.cairo.TableUtils;`. Capture the record start and verify in `hasNext()` (the method currently at lines 137-156):

```java
    public boolean hasNext() {
        offset = nextOffset;
        final long recordStart = offset;
        int length = readInt();
        if (length < 1) {
            // EOF
            return false;
        }
        nextOffset = length + nextOffset;

        if (memSize < nextOffset + Integer.BYTES) {
            eventMem.extend(nextOffset + Integer.BYTES);
            memSize = eventMem.size();
        }
        verifyRecordChecksum(recordStart, length);
        txn = readLong();
        if (txn == END_OF_EVENTS) {
            return false;
        }
        readRecord();
        return true;
    }

    // Verify the per-record checksum trailer if present. Magic-gated: a record without the trailer
    // (written by an older QuestDB) is read unverified. A present-but-mismatched trailer means the
    // record body was torn/partially written -> throw loudly so apply suspends the table.
    private void verifyRecordChecksum(long recordStart, int length) {
        final long bodyLen = (long) length - WalUtils.WALE_CHECKSUM_TRAILER_SIZE;
        if (bodyLen <= 0 || memSize < recordStart + length) {
            return; // too short to carry a trailer
        }
        final long trailerOffset = recordStart + bodyLen;
        if (eventMem.getLong(trailerOffset) != WalUtils.WALE_CHECKSUM_MAGIC) {
            return; // legacy record without a checksum trailer
        }
        final long stored = eventMem.getLong(trailerOffset + Long.BYTES);
        final long actual = TableUtils.calculateCvAreaChecksum(eventMem.addressOf(recordStart), bodyLen);
        if (actual != stored) {
            throw CairoException.critical(0)
                    .put("torn WAL event record [offset=").put(recordStart)
                    .put(", len=").put(length)
                    .put(", expected=").put(stored)
                    .put(", actual=").put(actual)
                    .put(']');
        }
    }
```

Note: `WalUtils.WALE_HEADER_SIZE` is already referenced here (line ~160), so `WalUtils` is in scope; reference the new constants as `WalUtils.WALE_CHECKSUM_MAGIC` / `WalUtils.WALE_CHECKSUM_TRAILER_SIZE` (or via the existing static import if the file uses one).

- [ ] **Step 4: Run the torn + legacy + happy tests to verify they pass**

Run: `mvn -q -pl core test -Dtest=WalEventChecksumTest`
Expected: PASS (all three) — torn record suspends the table; legacy record reads cleanly with `count = 1`; trailer-present still holds.

- [ ] **Step 5: Run the existing WAL suites to confirm no regression**

Run: `mvn -q -pl core test -Dtest=WalWriterTest,WalTableSqlTest,WalColumnarRowAppenderTest`
Expected: PASS — the additive trailer does not disturb existing read/write/round-trip behavior.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/wal/WalEventCursor.java core/src/test/java/io/questdb/test/cairo/wal/WalEventChecksumTest.java
git commit -m "feat(wal): verify per-record _event checksum; suspend on torn record

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review (run after the tasks)

- [ ] **Spec coverage:** This plan implements the `_event` side of spec §5.2 `WalIntegrity` (`#10`, events). Confirm the sibling gaps are tracked as follow-on plans: **1b** txnlog CRC (`#8`), **1c** segment-length validation (`#6`). The durable-frontier *computation* that consumes these lands in Plan 3 (`RecoveryCoordinator`).
- [ ] **Determinism:** `calculateCvAreaChecksum` hashes `[startOffset, bodyEnd)` on write and `[recordStart, recordStart + length - 16)` on read — confirm these are the same byte range (they are: `length = bodyEnd - startOffset + 16`, and on read `recordStart == startOffset`). The length prefix is back-patched *before* hashing on the write side so the covered value matches the read side.
- [ ] **Back-compat:** legacy `_event` files (no magic) are read unverified (`testLegacyRecordWithoutTrailerStillReads`); the length prefix already accounts for the 16 extra bytes so old + new readers both frame correctly via `length`.
- [ ] **No placeholders:** every step has concrete code/commands; no "add error handling"/"similar to" left in.

---

## Execution Handoff

See the parent skill's handoff: choose subagent-driven (fresh agent per task, review between) or inline execution. This plan is small (3 tasks) and a good first candidate either way.
