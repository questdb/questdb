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

package io.questdb.test.cairo.crash;

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
 * Proves that a torn last VARCHAR aux entry never lets the next append overwrite committed rows. A split-value
 * aux entry holds a 4-byte header and, in bytes 8-15, the data offset. A torn entry can zero the offset while the
 * header stays valid, so the raw size accessor returns {@code 0 + size} instead of the data vector end.
 * {@link VarcharTypeDriver#setAppendPosition} checks that the last row's data offset is not below the previous
 * row's data end and throws a {@link CairoException} rather than place the append cursor inside committed data.
 * <p>
 * Under nosync, the shipped default, nothing is synced and this check is the only crash protection. It catches a
 * single torn last entry, not a whole unflushed aux page of zeroed entries, where the comparison reduces to
 * {@code 0 < 0}; tail loss is acceptable there, a silent overwrite of older rows is not.
 */
public class VarcharPowerLossCorruptionTest extends AbstractCairoTest {

    private static final int SPLIT_STRING_LEN = 20;
    private static final String SPLIT_STRING = "AAAABBBBCCCCDDDDEEEE"; // 20 ASCII chars, forces split mode

    /**
     * With the offset bytes zeroed and the header intact, {@code getDataVectorSizeAt} returns
     * {@code 0 + size}, the length of one string, instead of the data vector end, and
     * {@code assert raw != 0} does not catch it because the header is non-zero. This raw accessor is a
     * hot path with no validation; {@code setAppendPosition} cross-checks neighbouring entries instead.
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
