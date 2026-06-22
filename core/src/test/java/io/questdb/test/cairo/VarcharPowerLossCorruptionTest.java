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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;

/**
 * Reproduces the VARCHAR power-loss corruption vulnerability and verifies the fix.
 *
 * <h2>The bug</h2>
 *
 * VARCHAR split-value aux entries (16 bytes) store the data offset in bytes 8-15, written
 * <em>after</em> the row's data. On reopen, {@link VarcharTypeDriver#setAppendPosition} reads
 * that offset to place the append cursor at the end of the data vector. A torn / partially
 * flushed last entry can leave the offset bytes zeroed while the 4-byte header (bytes 0-3) is
 * still valid, so the {@code assert raw != 0} guard passes and the recovery silently computes
 * {@code 0 + size} instead of the true data-vector end. The cursor then lands inside committed
 * data and the next append overwrites live rows - silently, with no error.
 *
 * <h2>The fix</h2>
 *
 * <ol>
 *   <li>{@code VarcharTypeDriver.appendValue} writes the data vector before the aux entry that
 *       points into it (target-before-pointer), matching {@code StringTypeDriver}.</li>
 *   <li>{@code setAppendPosition} validates that the last row's data offset is not below the
 *       previous row's data end (offsets are monotonic), throwing a {@link CairoException}
 *       instead of silently positioning the cursor inside committed data. STRING gets the
 *       same guard.</li>
 *   <li>{@code TableWriter.syncColumns0} documents and preserves the data-before-aux fsync
 *       ordering, so a durable aux entry never points past not-yet-durable data.</li>
 * </ol>
 */
public class VarcharPowerLossCorruptionTest extends AbstractCairoTest {

    private static final int SPLIT_STRING_LEN = 20;
    private static final String SPLIT_STRING = "AAAABBBBCCCCDDDDEEEE"; // 20 ASCII chars, forces split mode

    // -----------------------------------------------------------------------------------------
    // Mechanism: why the recovery guard is necessary
    // -----------------------------------------------------------------------------------------

    /**
     * Documents the low-level reason the recovery guard exists: with the offset bytes zeroed but
     * the header intact, {@code getDataVectorSizeAt} silently returns {@code 0 + size} (the length
     * of one string) rather than the true data-vector end. The header staying non-zero is exactly
     * why {@code assert raw != 0} does not catch it. The fix lives in {@code setAppendPosition},
     * which cross-checks neighbouring entries; this raw accessor remains a hot path and keeps no
     * validation, so it still returns the wrong value here - that is what the guard defends.
     */
    @Test
    public void testRawGetDataVectorSizeStaysWrongOnTornOffset() throws Exception {
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
                    VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(SPLIT_STRING));
                    VarcharTypeDriver.appendValue(auxMem, dataMem, new Utf8String(SPLIT_STRING));

                    long correctEnd = VarcharTypeDriver.INSTANCE.getDataVectorSizeAt(auxMem.addressOf(0), 1);
                    Assert.assertEquals(2L * SPLIT_STRING_LEN, correctEnd);

                    int header = Unsafe.getInt(auxMem.addressOf(VARCHAR_AUX_WIDTH_BYTES));
                    Assert.assertNotEquals("header non-zero", 0, header);

                    // Zero bytes 8-15 (offset) of entry[1]; header (0-3) stays valid.
                    auxMem.putLong(VARCHAR_AUX_WIDTH_BYTES + 8L, 0L);
                    Assert.assertNotEquals("header still valid", 0, Unsafe.getInt(auxMem.addressOf(VARCHAR_AUX_WIDTH_BYTES)));

                    long corruptEnd = VarcharTypeDriver.INSTANCE.getDataVectorSizeAt(auxMem.addressOf(0), 1);
                    Assert.assertEquals("raw accessor silently returns 0 + size", (long) SPLIT_STRING_LEN, corruptEnd);
                }
            }
        });
    }

    // -----------------------------------------------------------------------------------------
    // Recovery guard: setAppendPosition now detects the torn last entry
    // -----------------------------------------------------------------------------------------

    @Test
    public void testVarcharSetAppendPositionDetectsTornOffset() throws Exception {
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

                    // Healthy recovery works.
                    VarcharTypeDriver.INSTANCE.setAppendPosition(rowCount, auxMem, dataMem);
                    Assert.assertEquals((long) rowCount * SPLIT_STRING_LEN, dataMem.getAppendOffset());

                    // Torn last entry: zero its offset bytes, header stays valid.
                    long lastEntryBase = (long) (rowCount - 1) * VARCHAR_AUX_WIDTH_BYTES;
                    auxMem.putLong(lastEntryBase + 8L, 0L);

                    try {
                        VarcharTypeDriver.INSTANCE.setAppendPosition(rowCount, auxMem, dataMem);
                        Assert.fail("setAppendPosition must throw on a torn last aux entry");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "varchar aux vector is damaged");
                    }
                }
            }
        });
    }

    @Test
    public void testStringSetAppendPositionDetectsTornOffset() throws Exception {
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
                    StringTypeDriver.INSTANCE.configureAuxMemMA(auxMem);
                    for (int i = 0; i < rowCount; i++) {
                        StringTypeDriver.appendValue(auxMem, dataMem, "row" + i + SPLIT_STRING);
                    }

                    // Healthy recovery works.
                    StringTypeDriver.INSTANCE.setAppendPosition(rowCount, auxMem, dataMem);

                    // Torn N+1 entry: zero aux[rowCount].
                    auxMem.putLong((long) rowCount * Long.BYTES, 0L);

                    try {
                        StringTypeDriver.INSTANCE.setAppendPosition(rowCount, auxMem, dataMem);
                        Assert.fail("setAppendPosition must throw on a torn N+1 aux entry");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "string aux vector is damaged");
                    }
                }
            }
        });
    }

    // -----------------------------------------------------------------------------------------
    // End-to-end: torn aux on disk + reopen must not overwrite committed data
    // -----------------------------------------------------------------------------------------

    @Test
    public void testVarcharReopenAfterTornAuxDoesNotOverwriteCommittedData() throws Exception {
        assertReopenAfterTornAuxIsSafe("v", "varchar", true);
    }

    @Test
    public void testStringReopenAfterTornAuxDoesNotOverwriteCommittedData() throws Exception {
        assertReopenAfterTornAuxIsSafe("s", "string", false);
    }

    /**
     * Inserts 10 split rows, snapshots the data file, then zeroes the offset bytes of the last
     * committed aux entry (data file left intact - the realistic "aux tail did not reach disk"
     * shape). After reopen + insert, asserts (a) the torn aux is detected (the insert throws) and
     * (b) no committed data byte was overwritten.
     */
    private void assertReopenAfterTornAuxIsSafe(String colName, String colType, boolean varchar) throws Exception {
        assertMemoryLeak(() -> {
            final int ROW_COUNT = 10;
            final String tableName = "pwrloss_" + colType;

            execute("create table " + tableName + " (ts timestamp, " + colName + " " + colType
                    + ") timestamp(ts) partition by none");
            for (int i = 0; i < ROW_COUNT; i++) {
                execute("insert into " + tableName + " values (" + (i * 1_000_000L)
                        + ", '" + "row" + String.format("%02d", i) + SPLIT_STRING + "')");
            }

            engine.releaseAllWriters();

            final TableToken tableToken = engine.verifyTableName(tableName);
            final FilesFacade ff = configuration.getFilesFacade();
            long beforeBuf = 0;
            long afterBuf = 0;
            long logicalDataLen = 0;
            try (Path auxPath = new Path(); Path dataPath = new Path()) {
                auxPath.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.DEFAULT_PARTITION_NAME).slash();
                dataPath.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.DEFAULT_PARTITION_NAME).slash();
                TableUtils.iFile(auxPath, colName, TableUtils.COLUMN_NAME_TXN_NONE);
                TableUtils.dFile(dataPath, colName, TableUtils.COLUMN_NAME_TXN_NONE);

                long auxFd = ff.openRW(auxPath.$(), configuration.getWriterFileOpenOpts());
                Assert.assertTrue(auxFd > 0);
                try {
                    logicalDataLen = varchar
                            ? VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFd, ROW_COUNT - 1)
                            : StringTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFd, ROW_COUNT - 1);
                    Assert.assertTrue("committed data must be non-empty", logicalDataLen > 0);

                    beforeBuf = readFileBytes(ff, dataPath, logicalDataLen);

                    // Corrupt the last committed aux entry's offset, data file untouched.
                    long zero = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.setMemory(zero, 8, (byte) 0);
                        long offsetPos = varchar
                                ? (long) (ROW_COUNT - 1) * VARCHAR_AUX_WIDTH_BYTES + 8L // bytes 8-15 of last entry
                                : (long) ROW_COUNT * Long.BYTES;                        // N+1 entry
                        Assert.assertEquals(8L, ff.write(auxFd, zero, 8, offsetPos));
                    } finally {
                        Unsafe.free(zero, 8, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(auxFd);
                }

                engine.releaseAllReaders();
                engine.releaseAllWriters();

                // Reopen + append: must be detected, not silently corrupting. The guard throws a
                // CairoException, which the writer reopen path may surface wrapped in a CairoError.
                boolean detected = false;
                try {
                    execute("insert into " + tableName + " values (" + (ROW_COUNT * 1_000_000L)
                            + ", '" + "newrow" + SPLIT_STRING + "')");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "aux vector is damaged");
                    detected = true;
                } catch (CairoError e) {
                    TestUtils.assertContains(e.getMessage(), "aux vector is damaged");
                    detected = true;
                }
                engine.releaseAllWriters();

                afterBuf = readFileBytes(ff, dataPath, logicalDataLen);
                Assert.assertTrue(
                        "committed data bytes must not be overwritten by the post-crash append",
                        Vect.memeq(beforeBuf, afterBuf, logicalDataLen));
                Assert.assertTrue(
                        "torn aux must be detected on writer reopen (insert must throw)",
                        detected);
            } finally {
                if (beforeBuf != 0) {
                    Unsafe.free(beforeBuf, logicalDataLen, MemoryTag.NATIVE_DEFAULT);
                }
                if (afterBuf != 0) {
                    Unsafe.free(afterBuf, logicalDataLen, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private static long readFileBytes(FilesFacade ff, Path path, long len) {
        long fd = ff.openRO(path.$());
        Assert.assertTrue("cannot open file for snapshot: " + path, fd > 0);
        long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
        try {
            long n = ff.read(fd, buf, len, 0);
            Assert.assertEquals("short read while snapshotting " + path, len, n);
            return buf;
        } finally {
            ff.close(fd);
        }
    }
}
