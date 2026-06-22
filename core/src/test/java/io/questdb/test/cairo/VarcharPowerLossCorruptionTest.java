/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;

/**
 * Demonstrates and reproduces the VARCHAR power-loss corruption vulnerability.
 *
 * <h2>Root Cause</h2>
 *
 * VARCHAR aux entries are 16 bytes wide. For a split value (string &gt; 9 bytes), the write
 * sequence within a single aux entry is:
 * <ol>
 *   <li>bytes 0-3:  header int {@code (size << 4 | flags)}   — written BEFORE data file</li>
 *   <li>bytes 4-9:  6-byte inline prefix                    — written BEFORE data file</li>
 *   <li>[data file write happens here]</li>
 *   <li>bytes 10-11: {@code (short) dataOffset}              — written AFTER data file</li>
 *   <li>bytes 12-15: {@code (int)(dataOffset >> 16)}         — written AFTER data file</li>
 * </ol>
 *
 * {@code setAppendPosition(N)} reads the last committed aux entry (index N-1) and computes
 * the data-vector size via {@code getDataOffset = Unsafe.getLong(entry + 8) >>> 16}.
 *
 * If the OS flushes bytes 0-9 to disk before power loss but bytes 8-15 are not yet on disk
 * (or are only partially on disk), recovery reads {@code getLong(entry+8)} with zeroed bytes,
 * producing {@code offset = 0}.  The header ({@code raw}, bytes 0-3) is still non-zero, so
 * the {@code assert raw != 0} guard silently passes and the function returns {@code 0 + size}
 * (the length of just that one string) instead of the true end of the data vector.  The data
 * pointer is set to this small wrong offset and the next write <em>overwrites already-committed
 * data without any error</em>.
 *
 * <h2>Why STRING is more resilient</h2>
 *
 * STRING uses an N+1 aux model: {@code setAppendPosition(N)} reads {@code aux[N]} — a single
 * 8-byte {@code putLong} call.  Either that entry is on disk (correct) or it is zero (the
 * data pointer goes to 0, which immediately corrupts row 0 and makes the problem obvious).
 * STRING has no half-corrupt intermediate state where the header looks valid but the offset
 * is silently wrong, producing a plausible-looking but incorrect data-vector end.
 */
public class VarcharPowerLossCorruptionTest extends AbstractCairoTest {

    // 20-byte ASCII string: exceeds VARCHAR_MAX_BYTES_FULLY_INLINED (9), forcing split mode
    private static final int SPLIT_STRING_LEN = 20;
    private static final String SPLIT_STRING = "AAAABBBBCCCCDDDDEEEE"; // exactly 20 ASCII chars

    // -----------------------------------------------------------------------------------------
    // Unit tests: directly verify the mechanism with in-memory aux data
    // -----------------------------------------------------------------------------------------

    /**
     * Core unit test: zeroing bytes 8-15 of the last committed VARCHAR aux entry causes
     * {@code setAppendPosition} to compute a wrong (too small) data-vector end.
     * <p>
     * The corruption is silent because the header bytes (0-3) remain non-zero and valid:
     * the {@code assert raw != 0} guard passes and the caller simply gets a wrong offset.
     */
    @Test
    public void testVarcharSetAppendPositionWithCorruptOffsetBytes() throws Exception {
        final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            try (
                    Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
                    Path dataPath = new Path().of(temp.newFile().getAbsolutePath())
            ) {
                try (
                        MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, auxPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                        MemoryCMARW dataMem = Vm.getSmallCMARWInstance(ff, dataPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
                ) {
                    final int rowCount = 10;
                    for (int i = 0; i < rowCount; i++) {
                        VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(SPLIT_STRING));
                    }

                    long correctDataEnd = dataMem.getAppendOffset(); // 10 * SPLIT_STRING_LEN = 200
                    Assert.assertEquals("data must hold 10 strings of 20 bytes",
                            (long) rowCount * SPLIT_STRING_LEN, correctDataEnd);

                    // Verify clean recovery gives the correct position
                    VarcharTypeDriver.INSTANCE.setAppendPosition(rowCount, auxMem, dataMem);
                    Assert.assertEquals("clean setAppendPosition must restore correct data end",
                            correctDataEnd, dataMem.getAppendOffset());

                    // Simulate power loss: zero bytes 8-15 of the last aux entry.
                    // These bytes encode the 48-bit data offset (getLong(entry+8) >>> 16).
                    // The header (bytes 0-3) is left intact so assert raw != 0 passes silently.
                    long lastEntryBase = (long) (rowCount - 1) * VARCHAR_AUX_WIDTH_BYTES;
                    auxMem.putLong(lastEntryBase + 8L, 0L);

                    // Confirm the header (raw) is still non-zero — corruption is silent
                    int rawAfterCorrupt = Unsafe.getInt(auxMem.addressOf(lastEntryBase));
                    Assert.assertNotEquals("header (raw) must still be non-zero after offset corruption",
                            0, rawAfterCorrupt);

                    // setAppendPosition now returns a wrong (too small) position
                    VarcharTypeDriver.INSTANCE.setAppendPosition(rowCount, auxMem, dataMem);
                    long corruptDataPos = dataMem.getAppendOffset();

                    Assert.assertNotEquals(
                            "corrupt aux must cause setAppendPosition to use wrong data position",
                            correctDataEnd, corruptDataPos);
                    Assert.assertTrue(
                            "corrupt position must point inside committed data, not at the end",
                            corruptDataPos < correctDataEnd);

                    // The corrupt position is exactly SPLIT_STRING_LEN because:
                    //   getDataOffset = getLong(entry+8) >>> 16 = 0
                    //   getDataVectorSize = 0 + size = SPLIT_STRING_LEN
                    Assert.assertEquals("corrupt recovery position = 0 + size = SPLIT_STRING_LEN",
                            (long) SPLIT_STRING_LEN, corruptDataPos);
                }
            }
        });
    }

    /**
     * Contrasting test for STRING: zeroing the N+1 aux entry produces an obviously wrong
     * data pointer (0), not a plausible-looking wrong value like VARCHAR partial corruption.
     */
    @Test
    public void testStringN1EntryCorruptProducesZero() throws Exception {
        final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            try (
                    Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
                    Path dataPath = new Path().of(temp.newFile().getAbsolutePath())
            ) {
                try (
                        MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, auxPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                        MemoryCMARW dataMem = Vm.getSmallCMARWInstance(ff, dataPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
                ) {
                    final int rowCount = 10;
                    StringTypeDriver.INSTANCE.configureAuxMemMA(auxMem); // writes aux[0] = 0
                    for (int i = 0; i < rowCount; i++) {
                        StringTypeDriver.appendValue(auxMem, dataMem, "row" + i + SPLIT_STRING);
                    }

                    // STRING setAppendPosition(N) reads aux[N] — the N+1 entry = end of data
                    long n1EntryOffset = (long) rowCount * Long.BYTES;
                    long correctN1Value = Unsafe.getLong(auxMem.addressOf(n1EntryOffset));
                    Assert.assertTrue("N+1 entry must be positive before corruption", correctN1Value > 0);

                    // Simulate partial flush: zero the N+1 entry
                    auxMem.putLong(n1EntryOffset, 0L);

                    // setAppendPosition reads 0 → dataMem.jumpTo(0) → total overwrite (obvious)
                    StringTypeDriver.INSTANCE.setAppendPosition(rowCount, auxMem, dataMem);
                    Assert.assertEquals(
                            "STRING corrupt N+1 → data pointer at 0 (obvious, total overwrite)",
                            0L, dataMem.getAppendOffset());
                }
            }
        });
    }

    /**
     * Verifies the exact mechanism: for a split VARCHAR value, the data offset is stored in
     * bytes 10-15 of the aux entry (as a {@code putShort} + {@code putInt}).  Zeroing bytes
     * 8-15 (including the prefix tail at bytes 8-9) still leaves the header non-zero, so
     * {@code getDataVectorSize} silently returns {@code 0 + size} = the length of that one
     * string, rather than the true total data-vector end.
     */
    @Test
    public void testVarcharPartialOffsetFlushProducesWrongDataVectorSize() throws Exception {
        final FilesFacade ff = TestFilesFacadeImpl.INSTANCE;
        assertMemoryLeak(() -> {
            try (
                    Path auxPath = new Path().of(temp.newFile().getAbsolutePath());
                    Path dataPath = new Path().of(temp.newFile().getAbsolutePath())
            ) {
                try (
                        MemoryCMARW auxMem = Vm.getSmallCMARWInstance(ff, auxPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE);
                        MemoryCMARW dataMem = Vm.getSmallCMARWInstance(ff, dataPath.$(), MemoryTag.MMAP_DEFAULT, CairoConfiguration.O_NONE)
                ) {
                    // Write 2 split strings so entry[1] has a non-zero data offset
                    VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(SPLIT_STRING));
                    VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(SPLIT_STRING));
                    // Entry[1]: offset = SPLIT_STRING_LEN; total data end = 2 * SPLIT_STRING_LEN

                    long correctEnd = VarcharTypeDriver.INSTANCE.getDataVectorSizeAt(auxMem.addressOf(0), 1);
                    Assert.assertEquals("correct data vector end = 2 * SPLIT_STRING_LEN",
                            (long) 2 * SPLIT_STRING_LEN, correctEnd);

                    // Header of entry[1] must be non-zero (size=20, flag=0 → int = 320)
                    int header = Unsafe.getInt(auxMem.addressOf(VARCHAR_AUX_WIDTH_BYTES));
                    Assert.assertNotEquals("header must be non-zero", 0, header);
                    Assert.assertEquals("header must encode size = SPLIT_STRING_LEN",
                            SPLIT_STRING_LEN, header >> 4);

                    // Zero bytes 8-15 of entry[1] (offset field zeroed)
                    auxMem.putLong(VARCHAR_AUX_WIDTH_BYTES + 8L, 0L);

                    // Header is still valid — no assert or exception, silent corruption
                    Assert.assertNotEquals("header still valid after offset zeroing",
                            0, Unsafe.getInt(auxMem.addressOf(VARCHAR_AUX_WIDTH_BYTES)));

                    // getDataVectorSize now returns 0 + SPLIT_STRING_LEN instead of 2 * SPLIT_STRING_LEN
                    long corruptEnd = VarcharTypeDriver.INSTANCE.getDataVectorSizeAt(auxMem.addressOf(0), 1);
                    Assert.assertNotEquals("corrupt entry must give wrong data vector end",
                            correctEnd, corruptEnd);
                    Assert.assertEquals("corrupt getDataVectorSize = 0 + size = SPLIT_STRING_LEN",
                            (long) SPLIT_STRING_LEN, corruptEnd);

                    // setAppendPosition would use corruptEnd = SPLIT_STRING_LEN as the data cursor,
                    // pointing INTO committed data (row 1's slot at bytes 20-39 gets overwritten).
                }
            }
        });
    }

    // -----------------------------------------------------------------------------------------
    // End-to-end tests: real table → corrupt aux on disk → reopen → verify silent overwrite
    // -----------------------------------------------------------------------------------------

    /**
     * End-to-end reproduction of the VARCHAR power-loss corruption.
     * <p>
     * After inserting 10 rows of 25-byte split strings, we zero bytes 8-15 of the last
     * committed aux entry (simulating the OS page-cache scenario where those bytes were not
     * flushed before the power loss but the txn file was).  On reopen, the writer's
     * {@code setAppendPosition} reads the corrupt entry and places the data cursor 225 bytes
     * too early, causing the next insert to silently overwrite rows 1..9.
     * <p>
     * Row 0 is unaffected (its data is at bytes 0-24, before the wrong cursor position).
     * Row 1 and beyond are silently corrupted: their committed data is overwritten by the
     * new row without any exception, error log, or visible indication.
     */
    @Test
    public void testVarcharEndToEndSilentCorruption() throws Exception {
        assertMemoryLeak(() -> {
            final int ROW_COUNT = 10;
            final String tableName = "pwrloss_varchar";
            // Each row: "rowNN" (5 chars) + SPLIT_STRING (20 chars) = 25 chars total (split mode)
            final String rowPrefix = "row";

            execute("create table " + tableName
                    + " (ts timestamp, v varchar) timestamp(ts) partition by none");

            for (int i = 0; i < ROW_COUNT; i++) {
                String val = rowPrefix + String.format("%02d", i) + SPLIT_STRING;
                execute("insert into " + tableName + " values (" + (i * 1_000_000L) + ", '" + val + "')");
            }

            // All rows readable before corruption
            StringBuilder expectedBefore = new StringBuilder("v\n");
            for (int i = 0; i < ROW_COUNT; i++) {
                expectedBefore.append(rowPrefix).append(String.format("%02d", i)).append(SPLIT_STRING).append('\n');
            }
            assertQuery("select v from " + tableName)
                    .expectSize()
                    .returns(expectedBefore.toString());

            engine.releaseAllWriters();

            // ---- Corrupt bytes 8-15 of the LAST committed aux entry ----
            TableToken tableToken = engine.verifyTableName(tableName);
            FilesFacade ff = configuration.getFilesFacade();
            long zeroPage = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(zeroPage, 8, (byte) 0);

                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot())
                            .concat(tableToken)
                            .concat(TableUtils.DEFAULT_PARTITION_NAME)
                            .slash();
                    TableUtils.iFile(path, "v", TableUtils.COLUMN_NAME_TXN_NONE);

                    long auxFd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue("cannot open varchar aux file: " + path, auxFd > 0);
                    try {
                        long auxFileLen = ff.length(auxFd);
                        Assert.assertTrue("aux file must have at least ROW_COUNT entries",
                                auxFileLen >= (long) ROW_COUNT * VARCHAR_AUX_WIDTH_BYTES);

                        // Bytes 8-15 of the last entry encode the data offset.
                        // For 10 rows of 25-byte strings the offset of row 9 = 9*25 = 225, non-zero.
                        long lastEntryBase = (long) (ROW_COUNT - 1) * VARCHAR_AUX_WIDTH_BYTES;
                        long offsetBytes = ff.readNonNegativeLong(auxFd, lastEntryBase + 8L);
                        Assert.assertNotEquals(
                                "bytes 8-15 of last aux entry must encode non-zero offset (225 bytes in)",
                                0L, offsetBytes);

                        // Zero bytes 8-15 — simulates partial page flush
                        long written = ff.write(auxFd, zeroPage, 8, lastEntryBase + 8L);
                        Assert.assertEquals("must overwrite 8 bytes with zeros", 8L, written);
                    } finally {
                        ff.close(auxFd);
                    }
                }
            } finally {
                Unsafe.free(zeroPage, 8, MemoryTag.NATIVE_DEFAULT);
            }

            // ---- Simulate restart: clear all readers/writers ----
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // ---- Insert one more row — triggers setAppendPosition on the corrupt aux ----
            // setAppendPosition reads aux[9] bytes 8-15 = 0 → getDataVectorSize = 0 + 25 = 25.
            // dataMem is positioned at byte 25 (start of row 1's data) instead of 250 (end of all data).
            // The new insert writes 29 bytes starting at offset 25, overwriting rows 1..N data.
            final String newVal = "newrow00" + SPLIT_STRING;
            execute("insert into " + tableName + " values (" + (ROW_COUNT * 1_000_000L) + ", '" + newVal + "')");

            // Row 0 is safe: its data occupies bytes 0-24 (before the wrong cursor position of 25)
            assertQuery("select v from " + tableName + " where ts = 0::timestamp")
                    .returns("v\n" + rowPrefix + "00" + SPLIT_STRING + "\n");

            // Row 1 is CORRUPT: its data slot (bytes 25-49) was overwritten by newVal.
            // aux[1] still says offset=25, size=25 (those aux bytes were NOT corrupted),
            // so reading row 1 returns the first 25 bytes now at that offset = newVal[0..24].
            // Expected if NOT corrupt: "row01AAAABBBBCCCCDDDDEEEE" (25 chars)
            // Actual (corrupt):        first 25 chars of "newrow00AAAABBBBCCCCDDDDEEEE"
            //                        = "newrow00AAAABBBBCCCCDDDDE" (8+16+1 = 25 chars)
            String corruptExpected = newVal.substring(0, 25);
            assertQuery("select v from " + tableName + " where ts = 1000000::timestamp")
                    .returns("v\n" + corruptExpected + "\n");
        });
    }

    /**
     * Same scenario for STRING: zeroing the N+1 entry forces {@code dataMem.jumpTo(0)},
     * corrupting ALL rows (from byte 0).  Row 0 is immediately wrong — the corruption is
     * total and immediately detectable, unlike VARCHAR's partial silent overwrite.
     */
    @Test
    public void testStringEndToEndObviousCorruption() throws Exception {
        assertMemoryLeak(() -> {
            final int ROW_COUNT = 10;
            final String tableName = "pwrloss_string";
            final String rowPrefix = "row";

            execute("create table " + tableName
                    + " (ts timestamp, s string) timestamp(ts) partition by none");

            for (int i = 0; i < ROW_COUNT; i++) {
                String val = rowPrefix + String.format("%02d", i) + SPLIT_STRING;
                execute("insert into " + tableName + " values (" + (i * 1_000_000L) + ", '" + val + "')");
            }

            engine.releaseAllWriters();

            TableToken tableToken = engine.verifyTableName(tableName);
            FilesFacade ff = configuration.getFilesFacade();
            long zeroPage = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(zeroPage, 8, (byte) 0);

                try (Path path = new Path()) {
                    path.of(configuration.getDbRoot())
                            .concat(tableToken)
                            .concat(TableUtils.DEFAULT_PARTITION_NAME)
                            .slash();
                    TableUtils.iFile(path, "s", TableUtils.COLUMN_NAME_TXN_NONE);

                    long auxFd = ff.openRW(path.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue("cannot open string aux file: " + path, auxFd > 0);
                    try {
                        // STRING aux has N+1 entries; N+1 th entry is at byte offset ROW_COUNT * 8
                        long n1EntryOffset = (long) ROW_COUNT * Long.BYTES;
                        long originalN1 = ff.readNonNegativeLong(auxFd, n1EntryOffset);
                        Assert.assertTrue("N+1 entry must be non-zero before corruption", originalN1 > 0);

                        // Zero the N+1 entry — the single 8-byte putLong written after each row's data
                        long written = ff.write(auxFd, zeroPage, 8, n1EntryOffset);
                        Assert.assertEquals("must write 8 zero bytes", 8L, written);
                    } finally {
                        ff.close(auxFd);
                    }
                }
            } finally {
                Unsafe.free(zeroPage, 8, MemoryTag.NATIVE_DEFAULT);
            }

            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // setAppendPosition reads aux[ROW_COUNT] = 0 → dataMem.jumpTo(0)
            // The new row's UTF-16 bytes overwrite from byte 0 → row 0 is immediately wrong.
            final String newVal = "newrow00" + SPLIT_STRING;
            execute("insert into " + tableName + " values (" + (ROW_COUNT * 1_000_000L) + ", '" + newVal + "')");

            // STRING: row 0 immediately shows the NEW value (total corruption from byte 0)
            assertQuery("select s from " + tableName + " where ts = 0::timestamp")
                    .returns("s\n" + newVal + "\n");
        });
    }

}
