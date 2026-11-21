/*+*****************************************************************************
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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A non-WAL TRUNCATE publishes the empty {@code _txn} before it empties a file that the old one names. Whatever step
 * an I/O error or a process kill stops it at, the table is either as it was or empty, and a query reads it whole
 * before any writer opens it again: no crash, no wrong rows, and no TRUNCATE that a later writer open completes.
 */
public class TruncateFaultTest extends AbstractCrashConsistencyTest {
    // TRUNCATE cuts a column file to one append page, see setUpAppendPage(): the first column spans three
    private static final int ROWS = (int) (3 * Files.PAGE_SIZE / Long.BYTES);

    @Before
    public void setUpAppendPage() {
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, Files.PAGE_SIZE);
    }

    @Test
    public void testFailureAfterPublishLeavesEmptyTable() throws Exception {
        // the I/O error hits the file of the last column, after TRUNCATE has cut the files of the ones before it
        final FaultFacade ff = new FaultFacade("b.d");
        assertMemoryLeak(ff, () -> {
            for (String keepSymbolMaps : new String[]{"", " KEEP SYMBOL MAPS"}) {
                createTable();
                ff.isTruncateFailArmed = true;
                assertTruncateFailsAfterPublish(ff, "TRUNCATE TABLE t" + keepSymbolMaps, "Cannot truncate fd=");
                assertEmptyTable();
                assertTableTakesRows();
                execute("DROP TABLE t");
            }
        });
    }

    @Test
    public void testFailureAfterSymbolMapsEmptiedLeavesEmptyTable() throws Exception {
        // TRUNCATE empties the symbol maps, then fails on a column file. No column file spans more than one append
        // page here, so only the symbol maps differ from the table as it was.
        setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 4 * Numbers.SIZE_1MB);
        final FaultFacade ff = new FaultFacade("a.d");
        assertMemoryLeak(ff, () -> {
            for (int symbolCount : new int[]{3, 5_000}) {
                execute("CREATE TABLE t (s SYMBOL, a LONG)");
                execute("INSERT INTO t SELECT 'sym' || (x % " + symbolCount + "), x FROM long_sequence(10_000)");
                ff.isTruncateFailArmed = true;
                assertTruncateFailsAfterPublish(ff, "TRUNCATE TABLE t", "Cannot truncate fd=");

                assertQuery("SELECT count() n, count_distinct(s) d FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                n\td
                                0\t0
                                """);
                assertQuery("SELECT s, a FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .returns("s\ta\n");
                assertQuery("SELECT count() n FROM t WHERE s = 'sym1'")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                n
                                0
                                """);

                // the rows written after it take symbol keys of their own
                execute("INSERT INTO t VALUES ('sym1', 1), ('sym2', 2), ('new', 3), ('sym1', 4)");
                assertQuery("SELECT s, count() n FROM t ORDER BY s")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                s\tn
                                new\t1
                                sym1\t2
                                sym2\t1
                                """);
                assertQuery("SELECT a FROM t WHERE s = 'sym1'")
                        .noLeakCheck()
                        .returns("""
                                a
                                1
                                4
                                """);
                execute("DROP TABLE t");
            }
        });
    }

    @Test
    public void testFailureMappingFirstPageAfterCutLeavesEmptyTable() throws Exception {
        // TRUNCATE cuts the file of the only column, and fails to map its first page again
        final FaultFacade ff = new FaultFacade("a.d");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (a LONG)");
            execute("INSERT INTO t SELECT x FROM long_sequence(" + ROWS + ")");
            ff.isMapAfterTruncateFailArmed = true;
            assertTruncateFailsAfterPublish(ff, "TRUNCATE TABLE t", "could not mmap column");

            assertQuery("SELECT count() n, sum(a) sa FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            n\tsa
                            0\tnull
                            """);
            assertWithJitOnAndOff("SELECT count() n FROM t WHERE a > 0", "n\n0\n");

            execute("INSERT INTO t VALUES (7), (8)");
            assertQuery("SELECT count() n, sum(a) sa FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("""
                            n\tsa
                            2\t15
                            """);
        });
    }

    @Test
    public void testKillAfterIndexKeyCountDropsLeavesWritableTable() throws Exception {
        // BitmapIndexWriter.truncate() drops the key count of a symbol map's index before it zeroes the key entries,
        // and no kill point lies between the two. A process killed there leaves the old entries above a zero key
        // count. The table reads empty, and the rows written after it take symbol keys of their own: the writer open
        // zeroes those entries, which add() otherwise takes for empty ones.
        final SymbolMapTruncateCrashFacade ff = new SymbolMapTruncateCrashFacade();
        runWithCrashFacade(ff, () -> {
            // the old symbols fill the buckets of the map index, which hashes a capacity of 512 into 256 buckets
            final String createSql = "CREATE TABLE t (s SYMBOL CAPACITY 512 NOCACHE, x LONG)";
            final String insertSql = "INSERT INTO t SELECT 'old' || x, x FROM long_sequence(300)";
            execute(createSql);
            execute(insertSql);
            engine.releaseAllReaders();
            final int base = ff.killPointCount();
            ff.startRecording();
            execute("TRUNCATE TABLE t");
            ff.stopRecording();
            final int points = ff.killPointCount() - base;
            execute("DROP TABLE t");
            int firstSymbolMapPoint = -1;
            for (int k = points; k > 0; k--) {
                if (ff.isSymbolMapTruncatePoint(base + k)) {
                    firstSymbolMapPoint = k;
                }
            }
            Assert.assertTrue("TRUNCATE does not empty the symbol map", firstSymbolMapPoint > 0);

            // the kill at the first kill point inside SymbolMapWriter.truncate(): _txn names no symbols, and the index
            // of the map is as it was
            execute(createSql);
            execute(insertSql);
            engine.releaseAllReaders();
            ff.armKillAt(ff.killPointCount() + firstSymbolMapPoint, tableDir());
            execute("TRUNCATE TABLE t");
            Assert.assertTrue(ff.isKillImageTaken());
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            ff.crash(tableDir());
            engine.getTxnScoreboardPool().remove(engine.verifyTableName("t"));

            // and the first store of BitmapIndexWriter.truncate(), which the kill lands after
            try (Path keyFile = new Path().of(tableDir()).concat("s").put(".k")) {
                final long fd = TableUtils.openRW(ff, keyFile.$(), LOG, configuration.getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.readNonNegativeInt(fd, BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT) > 128);
                    final long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                    try {
                        Unsafe.putInt(buf, 0);
                        Assert.assertEquals(Integer.BYTES, ff.write(fd, buf, Integer.BYTES, BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT));
                    } finally {
                        Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(fd);
                }
            }

            assertQuery("SELECT count() n, count_distinct(s) d FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("n\td\n0\t0\n");
            assertQuery("SELECT count() n FROM t WHERE s = 'old7'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("n\n0\n");

            // every new symbol twice: the second insert looks each one up in the index the first one built
            execute("INSERT INTO t SELECT 'new' || x, x FROM long_sequence(300)");
            execute("INSERT INTO t SELECT 'new' || x, x + 1_000 FROM long_sequence(300)");
            assertQuery("SELECT count() n, count_distinct(s) d FROM t")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("n\td\n600\t300\n");
            assertQuery("SELECT s, count() n FROM t WHERE s IN ('new1', 'new150', 'new300') ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tn
                            new1\t2
                            new150\t2
                            new300\t2
                            """);
            assertQuery("SELECT x FROM t WHERE s = 'new7'")
                    .noLeakCheck()
                    .returns("""
                            x
                            7
                            1007
                            """);
            assertQuery("SELECT count() n FROM t WHERE s IN ('old7', 'old123')")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("n\n0\n");
        });
    }

    @Test
    public void testKillAtEveryPointLeavesTableAsItWasOrEmpty() throws Exception {
        assertEveryKillPoint(false);
    }

    @Test
    public void testKillAtEveryPointLeavesTableAsItWasOrEmptyKeepSymbolMaps() throws Exception {
        assertEveryKillPoint(true);
    }

    private static void assertTruncateFailsAfterPublish(FaultFacade ff, String truncateSql, String cause) throws Exception {
        // TRUNCATE needs every query of the table closed
        engine.releaseAllReaders();
        try {
            execute(truncateSql);
            Assert.fail("TRUNCATE must fail");
        } catch (CairoException e) {
            // TRUNCATE published the empty table before the failure, and says so
            TestUtils.assertContains(e.getFlyweightMessage(), "Data has been persisted, but we could not perform housekeeping");
            TestUtils.assertContains(e.getFlyweightMessage(), cause);
        }
        Assert.assertTrue("the failure was not injected", ff.isFired);
        ff.isFired = false;
    }

    private static int countMod(int modulo, int remainder) {
        int count = 0;
        for (int x = 1; x <= ROWS; x++) {
            if (x % modulo == remainder) {
                count++;
            }
        }
        return count;
    }

    // x is 1 to ROWS: a = x, s cycles through 7 symbols, si through 5 indexed ones, b = x * 10
    private static void createTable() throws Exception {
        execute("CREATE TABLE t (a LONG, s SYMBOL, si SYMBOL INDEX, v VARCHAR, b LONG)");
        execute("INSERT INTO t SELECT x, 's' || (x % 7), 'k' || (x % 5), 'v' || x, x * 10 FROM long_sequence(" + ROWS + ")");
    }

    private static String tableDir() {
        return root + Files.SEPARATOR + engine.verifyTableName("t").getDirName();
    }

    // The rows createTable() writes: x is 1 to ROWS. A query reads them back from the ends of the column files
    // (LIMIT -2), through vectorized aggregates, through a filter with and without JIT, and through both symbols.
    private void assertCommittedTable() throws Exception {
        assertQuery("SELECT a, v, b FROM t LIMIT -2")
                .noLeakCheck()
                .expectSize()
                .returns("a\tv\tb\n"
                        + (ROWS - 1) + "\tv" + (ROWS - 1) + '\t' + (ROWS - 1) * 10L + '\n'
                        + ROWS + "\tv" + ROWS + '\t' + ROWS * 10L + '\n');
        assertQuery("SELECT count() n, sum(a) sa, min(a) lo, max(b) hi FROM t")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("n\tsa\tlo\thi\n" + ROWS + '\t' + (long) ROWS * (ROWS + 1) / 2 + "\t1\t" + ROWS * 10L + '\n');
        assertWithJitOnAndOff("SELECT count() n FROM t WHERE a > " + ROWS / 2, "n\n" + (ROWS - ROWS / 2) + '\n');
        assertQuery("SELECT count() n FROM t WHERE s = 's3'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("n\n" + countMod(7, 3) + '\n');
        assertQuery("SELECT count() n FROM t WHERE si = 'k2'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("n\n" + countMod(5, 2) + '\n');
    }

    private void assertEmptyTable() throws Exception {
        assertQuery("SELECT * FROM t")
                .noLeakCheck()
                .expectSize()
                .returns("a\ts\tsi\tv\tb\n");
        assertQuery("SELECT count() n, sum(a) sa, min(a) lo, max(b) hi FROM t")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        n\tsa\tlo\thi
                        0\tnull\tnull\tnull
                        """);
        assertWithJitOnAndOff("SELECT count() n FROM t WHERE a > " + ROWS / 2, "n\n0\n");
        assertQuery("SELECT count() n FROM t WHERE s = 's3'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("n\n0\n");
        assertQuery("SELECT count() n FROM t WHERE si = 'k2'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("n\n0\n");
    }

    private void assertEveryKillPoint(boolean isKeepSymbolMaps) throws Exception {
        final SymbolMapTruncateCrashFacade ff = new SymbolMapTruncateCrashFacade();
        runWithCrashFacade(ff, () -> {
            final String truncateSql = "TRUNCATE TABLE t" + (isKeepSymbolMaps ? " KEEP SYMBOL MAPS" : "");
            // the kill points of one TRUNCATE, and the ones SymbolMapWriter.truncate() takes among them
            createTable();
            assertCommittedTable();
            engine.releaseAllReaders();
            final int base = ff.killPointCount();
            ff.startRecording();
            execute(truncateSql);
            ff.stopRecording();
            final int points = ff.killPointCount() - base;
            execute("DROP TABLE t");
            // the sweep reaches the emptying of the symbol maps, which TRUNCATE without KEEP SYMBOL MAPS does
            Assert.assertEquals("emptying the symbol maps", !isKeepSymbolMaps, ff.symbolMapTruncatePointCount() > 0);

            int emptied = 0;
            for (int k = 1; k <= points; k++) {
                createTable();
                engine.releaseAllReaders();
                ff.armKillAt(ff.killPointCount() + k, tableDir());
                execute(truncateSql);
                Assert.assertTrue("kill point " + k + " was not reached", ff.isKillImageTaken());
                // process restart: the engine lets go of the table, and its files go back to the kill image
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                ff.crash(tableDir());
                // a fresh process starts with an empty scoreboard, whose max txn may be past the restored _txn
                engine.getTxnScoreboardPool().remove(engine.verifyTableName("t"));

                // before any writer opens the table again, a query reads it whole: as it was, or empty
                final boolean isEmpty = "n\n0\n".equals(TestUtils.printSqlToString(engine, sqlExecutionContext, "SELECT count() n FROM t", sink));
                if (isEmpty) {
                    assertEmptyTable();
                    emptied++;
                } else {
                    assertCommittedTable();
                }

                // the writer open completes nothing: the old rows stay, or stay gone
                execute("INSERT INTO t VALUES (0, 's3', 'k2', 'new', 0)");
                assertQuery("SELECT count() n FROM t")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("n\n" + (isEmpty ? 1 : ROWS + 1) + '\n');
                assertQuery("SELECT count() n FROM t WHERE s = 's3'")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("n\n" + (isEmpty ? 1 : countMod(7, 3) + 1) + '\n');
                assertQuery("SELECT count() n FROM t WHERE si = 'k2'")
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("n\n" + (isEmpty ? 1 : countMod(5, 2) + 1) + '\n');
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                execute("DROP TABLE t");
            }
            Assert.assertTrue("no kill point left the table empty", emptied > 0);
        });
    }

    private void assertTableTakesRows() throws Exception {
        execute("INSERT INTO t VALUES (1, 's3', 'k2', 'new1', 10), (2, 's9', 'k2', 'new2', 20)");
        assertQuery("SELECT * FROM t")
                .noLeakCheck()
                .expectSize()
                .returns("""
                        a\ts\tsi\tv\tb
                        1\ts3\tk2\tnew1\t10
                        2\ts9\tk2\tnew2\t20
                        """);
        assertQuery("SELECT count() n, sum(a) sa, min(a) lo, max(b) hi FROM t")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        n\tsa\tlo\thi
                        2\t3\t1\t20
                        """);
        assertQuery("SELECT a FROM t WHERE s = 's3'")
                .noLeakCheck()
                .returns("""
                        a
                        1
                        """);
        assertQuery("SELECT count() n FROM t WHERE si = 'k2'")
                .noLeakCheck()
                .expectSize()
                .noRandomAccess()
                .returns("""
                        n
                        2
                        """);
        // and TRUNCATE runs again
        execute("TRUNCATE TABLE t");
        assertEmptyTable();
    }

    // a count() with a filter, which compiles to native code when JIT is on
    private void assertWithJitOnAndOff(String countSql, String expected) throws Exception {
        try {
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                assertQuery(countSql)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns(expected);
            }
        } finally {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
        }
    }

    /**
     * Fails, once armed, the truncate() of a file, or the mapping of its first page after its truncate().
     */
    private static class FaultFacade extends TestFilesFacadeImpl {
        private final String fileName;
        volatile boolean isFired;
        volatile boolean isMapAfterTruncateFailArmed;
        volatile boolean isTruncateFailArmed;
        private volatile long fd = -1;
        private volatile boolean isTruncated;

        FaultFacade(String fileName) {
            this.fileName = Files.SEPARATOR + fileName;
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            if (isTruncated && fd == this.fd) {
                isTruncated = false;
                isMapAfterTruncateFailArmed = false;
                isFired = true;
                return FilesFacade.MAP_FAILED;
            }
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (Utf8s.endsWithAscii(name, fileName)) {
                this.fd = fd;
            }
            return fd;
        }

        @Override
        public boolean truncate(long fd, long size) {
            if (fd == this.fd) {
                if (isTruncateFailArmed) {
                    isTruncateFailArmed = false;
                    isFired = true;
                    return false;
                }
                isTruncated = isMapAfterTruncateFailArmed;
            }
            return super.truncate(fd, size);
        }
    }
}
