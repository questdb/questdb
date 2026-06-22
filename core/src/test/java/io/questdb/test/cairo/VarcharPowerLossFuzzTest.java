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

import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.StringTypeDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.questdb.cairo.VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;

/**
 * Stress tests for the VARCHAR/STRING power-loss corruption fix
 * (see {@link VarcharPowerLossCorruptionTest} for the focused cases).
 *
 * <p>Two properties are hammered with randomized inputs:
 * <ol>
 *   <li><b>No silent overwrite under a torn aux tail.</b> For many random table shapes we commit
 *       rows, snapshot the data file, zero the offset bytes of the last committed aux entry
 *       (the realistic "aux tail did not reach disk, data did" shape), reopen and append. The
 *       fix must either detect the damage (the append throws) and/or leave every committed data
 *       byte untouched - never silently overwrite live rows.</li>
 *   <li><b>No regression / no false positives on healthy data.</b> Random mixes of inline, split,
 *       empty and null values must round-trip correctly across a clean close + reopen, proving the
 *       appendValue reorder preserves the on-disk format and the recovery guard never fires on
 *       valid data.</li>
 * </ol>
 */
public class VarcharPowerLossFuzzTest extends AbstractCairoTest {

    private static final int ITERATIONS = 60;

    @Test
    public void testFuzzTornAuxTailNeverOverwritesCommittedDataVarchar() throws Exception {
        runTornAuxFuzz(true);
    }

    @Test
    public void testFuzzTornAuxTailNeverOverwritesCommittedDataString() throws Exception {
        runTornAuxFuzz(false);
    }

    /**
     * Healthy-data regression: random inline/split/empty/null values must survive a clean
     * close + reopen unchanged, and the recovery guard must never fire.
     */
    @Test
    public void testFuzzHealthyRoundTripAfterReopen() throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            for (int iter = 0; iter < ITERATIONS; iter++) {
                final String tableName = "fuzz_rt_" + iter;
                final boolean varchar = rnd.nextBoolean();
                final String colType = varchar ? "varchar" : "string";
                final int rowCount = 1 + rnd.nextInt(40);

                execute("create table " + tableName + " (ts timestamp, v " + colType
                        + ") timestamp(ts) partition by none");

                final List<String> expected = new ArrayList<>(rowCount);
                for (int i = 0; i < rowCount; i++) {
                    String v = randomSqlValue(rnd);
                    expected.add(v);
                    if (v == null) {
                        execute("insert into " + tableName + " values (" + ((long) i * 1_000_000L) + ", null)");
                    } else {
                        execute("insert into " + tableName + " values (" + ((long) i * 1_000_000L) + ", '" + v + "')");
                    }
                }

                // Clean close + reopen: exercises setAppendPosition on healthy data (no guard trip).
                engine.releaseAllReaders();
                engine.releaseAllWriters();

                // Append one more row after reopen to force the writer to position the cursor.
                String tail = randomSqlValue(rnd);
                expected.add(tail);
                if (tail == null) {
                    execute("insert into " + tableName + " values (" + ((long) rowCount * 1_000_000L) + ", null)");
                } else {
                    execute("insert into " + tableName + " values (" + ((long) rowCount * 1_000_000L) + ", '" + tail + "')");
                }

                assertColumnEquals(tableName, expected, varchar);
                execute("drop table " + tableName);
            }
        });
    }

    private void assertColumnEquals(String tableName, List<String> expected, boolean varchar) throws SqlException {
        try (RecordCursorFactory factory = select("select v from " + tableName)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                int row = 0;
                while (cursor.hasNext()) {
                    Assert.assertTrue("more rows than expected at row " + row, row < expected.size());
                    CharSequence actual = varchar
                            ? (record.getVarcharA(0) == null ? null : record.getVarcharA(0).toString())
                            : record.getStrA(0);
                    String exp = expected.get(row);
                    if (exp == null) {
                        Assert.assertNull("row " + row + " must be null", actual);
                    } else {
                        Assert.assertNotNull("row " + row + " must not be null, expected '" + exp + "'", actual);
                        Assert.assertEquals("row " + row + " mismatch after reopen", exp, actual.toString());
                    }
                    row++;
                }
                Assert.assertEquals("row count mismatch", expected.size(), row);
            }
        }
    }

    /**
     * @param varchar true for a VARCHAR column, false for STRING
     */
    private void runTornAuxFuzz(boolean varchar) throws Exception {
        assertMemoryLeak(() -> {
            final Rnd rnd = TestUtils.generateRandom(LOG);
            final FilesFacade ff = configuration.getFilesFacade();
            final String colType = varchar ? "varchar" : "string";

            for (int iter = 0; iter < ITERATIONS; iter++) {
                final String tableName = "fuzz_torn_" + colType + "_" + iter;
                // At least 2 rows so there is a previous entry to validate the last one against.
                final int rowCount = 2 + rnd.nextInt(38);

                execute("create table " + tableName + " (ts timestamp, v " + colType
                        + ") timestamp(ts) partition by none");

                // Use split values (length > 9) so the last row has a non-zero data offset that,
                // once zeroed, is provably wrong. Random lengths still vary the data layout.
                for (int i = 0; i < rowCount; i++) {
                    int len = 10 + rnd.nextInt(50); // 10..59 -> always split
                    execute("insert into " + tableName + " values (" + ((long) i * 1_000_000L)
                            + ", '" + randomAsciiValue(rnd, len) + "')");
                }

                engine.releaseAllWriters();

                final TableToken tableToken = engine.verifyTableName(tableName);
                long beforeBuf = 0;
                long afterBuf = 0;
                long logicalDataLen = 0;
                try (Path auxPath = new Path(); Path dataPath = new Path()) {
                    auxPath.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.DEFAULT_PARTITION_NAME).slash();
                    dataPath.of(configuration.getDbRoot()).concat(tableToken).concat(TableUtils.DEFAULT_PARTITION_NAME).slash();
                    TableUtils.iFile(auxPath, "v", TableUtils.COLUMN_NAME_TXN_NONE);
                    TableUtils.dFile(dataPath, "v", TableUtils.COLUMN_NAME_TXN_NONE);

                    long auxFd = ff.openRW(auxPath.$(), configuration.getWriterFileOpenOpts());
                    Assert.assertTrue("cannot open aux file [iter=" + iter + "]", auxFd > 0);
                    try {
                        logicalDataLen = varchar
                                ? VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFd, rowCount - 1)
                                : StringTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, auxFd, rowCount - 1);
                        Assert.assertTrue("data must be non-empty [iter=" + iter + "]", logicalDataLen > 0);

                        beforeBuf = readFileBytes(ff, dataPath, logicalDataLen);

                        // Randomly zero either bytes [8,16) or [10,16) of the last entry (varchar),
                        // or the whole N+1 entry (string). Header is preserved for varchar so the
                        // header != 0 invariant holds and only the recovery guard can catch it.
                        long offsetPos;
                        if (varchar) {
                            long base = (long) (rowCount - 1) * VARCHAR_AUX_WIDTH_BYTES;
                            offsetPos = base + (rnd.nextBoolean() ? 8L : 10L);
                        } else {
                            offsetPos = (long) rowCount * Long.BYTES;
                        }
                        long zeroLen = (long) (rowCount - 1) * VARCHAR_AUX_WIDTH_BYTES + VARCHAR_AUX_WIDTH_BYTES - offsetPos;
                        if (!varchar) {
                            zeroLen = Long.BYTES;
                        }
                        long zeros = Unsafe.malloc(zeroLen, MemoryTag.NATIVE_DEFAULT);
                        try {
                            Unsafe.setMemory(zeros, zeroLen, (byte) 0);
                            Assert.assertEquals(zeroLen, ff.write(auxFd, zeros, zeroLen, offsetPos));
                        } finally {
                            Unsafe.free(zeros, zeroLen, MemoryTag.NATIVE_DEFAULT);
                        }
                    } finally {
                        ff.close(auxFd);
                    }

                    engine.releaseAllReaders();
                    engine.releaseAllWriters();

                    boolean detected = false;
                    try {
                        execute("insert into " + tableName + " values (" + ((long) rowCount * 1_000_000L)
                                + ", '" + randomAsciiValue(rnd, 10 + rnd.nextInt(50)) + "')");
                    } catch (CairoException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "aux vector is damaged");
                        detected = true;
                    } catch (CairoError e) {
                        TestUtils.assertContains(e.getMessage(), "aux vector is damaged");
                        detected = true;
                    }
                    engine.releaseAllWriters();

                    afterBuf = readFileBytes(ff, dataPath, logicalDataLen);

                    // The core safety property: committed data must never be silently overwritten.
                    Assert.assertTrue(
                            "committed data overwritten by post-crash append [iter=" + iter
                                    + ", type=" + colType + ", rows=" + rowCount + "]",
                            Vect.memeq(beforeBuf, afterBuf, logicalDataLen));
                    // And the torn tail must actually be detected by the recovery guard.
                    Assert.assertTrue(
                            "torn aux not detected [iter=" + iter + ", type=" + colType + ", rows=" + rowCount + "]",
                            detected);
                } finally {
                    if (beforeBuf != 0) {
                        Unsafe.free(beforeBuf, logicalDataLen, MemoryTag.NATIVE_DEFAULT);
                    }
                    if (afterBuf != 0) {
                        Unsafe.free(afterBuf, logicalDataLen, MemoryTag.NATIVE_DEFAULT);
                    }
                }

                execute("drop table " + tableName);
            }
        });
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    /**
     * Random SQL-safe value covering inline (&lt;=9), split (&gt;9), empty and null shapes.
     * Uses letters/digits only to avoid SQL quoting issues.
     */
    private static String randomSqlValue(Rnd rnd) {
        int kind = rnd.nextInt(10);
        if (kind == 0) {
            return null;           // null
        }
        if (kind == 1) {
            return "";             // empty string (fully inlined, size 0)
        }
        int len = rnd.nextInt(40); // 0..39, straddles the 9-byte inline boundary
        return randomAsciiValue(rnd, len);
    }

    private static String randomAsciiValue(Rnd rnd, int len) {
        final char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int c = rnd.nextInt(62);
            if (c < 26) {
                chars[i] = (char) ('a' + c);
            } else if (c < 52) {
                chars[i] = (char) ('A' + (c - 26));
            } else {
                chars[i] = (char) ('0' + (c - 52));
            }
        }
        return new String(chars);
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
