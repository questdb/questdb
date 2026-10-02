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

package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.frm.file.CompositeFrameCache;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

/**
 * A merge-append plan grows every column file once, ahead of its first action, and the writer keeps the frames of the
 * last few partitions it wrote open across commits - so a steady stream of inserts into one partition opens, maps and
 * allocates each column file once per commit at most, and nothing at all when a cached frame serves the commit. Any
 * operation that is not a plain insert, and any failure, lets the frames go.
 */
public class CompositeFrameCacheTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";
    private static final String DDL = " (ts TIMESTAMP, v LONG, w VARCHAR, s SYMBOL) TIMESTAMP(ts) PARTITION BY DAY";

    @Test
    public void testCacheDisabledStillWritesCorrectly() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_FRAME_CACHE_SIZE, "0");
            createTables();
            insertBoth(batch("T12:00:30", 10_000, 10));
            insertBoth(batch("T06:00:30", 20_000, 10));
            insertBoth(batch("T23:00:30", 30_000, 10));

            try (TableWriter writer = getWriter("t")) {
                Assert.assertNull(writer.getCompositeFrameCache());
            }
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);
        });
    }

    @Test
    public void testCacheHoldsAtMostConfiguredPartitions() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_FRAME_CACHE_SIZE, "2");
            execute("CREATE TABLE t" + DDL + " WAL");
            execute("CREATE TABLE ref" + DDL + " BYPASS WAL");
            for (int day = 1; day <= 3; day++) {
                final String base = "SELECT timestamp_sequence('2024-01-0" + day + "', 60_000_000L) ts, x v, 'a' || x w, 's' || (x % 3) s FROM long_sequence(600)";
                insertBoth(base);
            }
            // Two commits, each O3 into all three partitions at once: every partition runs a plan in both.
            for (int round = 0; round < 2; round++) {
                final StringBuilder union = new StringBuilder("SELECT * FROM (");
                for (int day = 1; day <= 3; day++) {
                    if (day > 1) {
                        union.append(" UNION ALL ");
                    }
                    union.append("SELECT timestamp_sequence('2024-01-0").append(day).append("T0").append(round + 1)
                            .append(":00:30', 60_000_000L) ts, ").append(day * 1000 + round * 100)
                            .append(" + x v, 'b' || x w, 's' || (x % 3) s FROM long_sequence(5)");
                }
                insertBoth(union.append(')').toString());
            }
            assertReusable(2);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);
        });
    }

    @Test
    public void testColumnOpenedEmptyGainsDataUnderCachedFrame() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            createTables();
            // Added while every row of the partition lies below it: the first plan's read-only frame opens it as
            // EMPTY, with no file. That plan then writes rows for it, and the next plan - served by the cached
            // frame - merges a piece that has them.
            execute("ALTER TABLE t ADD COLUMN e INT");
            execute("ALTER TABLE ref ADD COLUMN e INT");
            drainWalQueue();
            for (int i = 0; i < 4; i++) {
                final String select = "SELECT timestamp_sequence('" + DAY + "T0" + (2 * i + 1) + ":00:30', 1_000_000L) ts, "
                        + (10_000 * (i + 1)) + " + x v, 'w' || x w, 's' || (x % 3) s, x::INT e FROM long_sequence(50)";
                execute("INSERT INTO t (ts, v, w, s, e) " + select);
                execute("INSERT INTO ref (ts, v, w, s, e) " + select);
                drainWalQueue();
                Assert.assertFalse("commit " + i + " suspended the table", engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            }
            assertReusable(1);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);
        });
    }

    @Test
    public void testIndexLookupsAfterReplaceRangeCommits() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            for (String indexType : new String[]{"POSTING", "BITMAP"}) {
                execute("CREATE TABLE t (ts TIMESTAMP, v LONG, s SYMBOL INDEX TYPE " + indexType + ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO t SELECT timestamp_sequence('" + DAY + "', 60_000_000L) ts, x v, 'k' || (x % 5) s FROM long_sequence(600)");
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("t");
                final long dayLo = MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z");
                // Replace-range commits into the LAST partition: each one mutates it, so the writer re-seals its index
                // after the plan - a new sealed version, and the old one purged.
                for (int i = 0; i < 8; i++) {
                    final long lo = dayLo + (long) (i * 37) * 60_000_000L;
                    final long hi = lo + 20 * 60_000_000L;
                    try (WalWriter ww = engine.getWalWriter(tt)) {
                        for (int r = 0; r < 10; r++) {
                            final TableWriter.Row row = ww.newRow(lo + r * 60_000_000L + 1);
                            row.putLong(1, 100_000L * (i + 1) + r);
                            row.putSym(2, "k" + ((r + i) % 5));
                            row.append();
                        }
                        ww.commitWithParams(lo, hi, WAL_DEDUP_MODE_REPLACE_RANGE);
                    }
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
                }
                for (int k = 0; k < 5; k++) {
                    // lower() keeps the second query off the index, so it reads the column itself.
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            "SELECT * FROM t WHERE lower(s) = 'k" + k + "'",
                            "SELECT * FROM t WHERE s = 'k" + k + "'",
                            LOG
                    );
                }
                execute("DROP TABLE t");
                drainWalQueue();
            }
        });
    }

    @Test
    public void testIndexedTableKeepsNothingAndStaysCorrect() throws Exception {
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            for (String indexType : new String[]{"POSTING", "BITMAP"}) {
                final String ddl = " (ts TIMESTAMP, v LONG, s SYMBOL INDEX TYPE " + indexType + ") TIMESTAMP(ts) PARTITION BY DAY";
                execute("CREATE TABLE t" + ddl + " WAL");
                execute("CREATE TABLE ref" + ddl + " BYPASS WAL");
                // The day is the LAST partition: the one the writer binds its own index writers to between commits.
                final String base = "SELECT timestamp_sequence('" + DAY + "', 60_000_000L) ts, x v, 's' || (x % 7) s FROM long_sequence(1440)";
                execute("INSERT INTO t " + base);
                execute("INSERT INTO ref " + base);
                drainWalQueue();
                // Consecutive O3 commits into the same partition: each re-indexes it, and a frame kept open across
                // them would write the next commit's index entries against the previous one's index state.
                for (int i = 0; i < 6; i++) {
                    final String select = "SELECT timestamp_sequence('" + DAY + "T" + (10 + i) + ":00:30', 1_000_000L) ts, "
                            + (10_000 * (i + 1)) + " + x v, 's' || ((x + " + i + ") % 7) s FROM long_sequence(40)";
                    execute("INSERT INTO t " + select);
                    execute("INSERT INTO ref " + select);
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
                    assertReusable(0);
                }
                TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);
                for (int k = 0; k < 7; k++) {
                    TestUtils.assertSqlCursors(
                            engine,
                            sqlExecutionContext,
                            "SELECT * FROM ref WHERE s = 's" + k + "'",
                            "SELECT * FROM t WHERE s = 's" + k + "'",
                            LOG
                    );
                }
                execute("DROP TABLE t");
                execute("DROP TABLE ref");
                drainWalQueue();
            }
        });
    }

    @Test
    public void testFailedPlanEvictsAndResumeRewrites() throws Exception {
        final AtomicBoolean failAllocate = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                if (failAllocate.get()) {
                    return false;
                }
                return super.allocate(fd, size);
            }
        };
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            createTables();
            insertBoth(batch("T12:00:30", 10_000, 10));
            insertBoth(batch("T06:00:30", 20_000, 10));
            assertReusable(1);

            final TableToken token = engine.verifyTableName("t");
            failAllocate.set(true);
            execute("INSERT INTO t (ts, v, w, s) " + batch("T18:00:30", 30_000, 10));
            drainWalQueue();
            failAllocate.set(false);
            Assert.assertTrue("a failed allocation did not suspend the table", engine.getTableSequencerAPI().isSuspended(token));
            // The plan that failed part-way let its frames go.
            assertReusable(0);

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
            execute("INSERT INTO ref (ts, v, w, s) " + batch("T18:00:30", 30_000, 10));
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);
            assertReusable(1);
        });
    }

    @Test
    public void testFramesStayOpenAcrossCommitsAndGoOnAnyOtherOperation() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final ConcurrentHashMap<Long, String> fdNames = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, AtomicInteger> opens = new ConcurrentHashMap<>();
        final ConcurrentHashMap<String, AtomicInteger> allocations = new ConcurrentHashMap<>();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                final String name = fdNames.get(fd);
                if (armed.get() && name != null) {
                    allocations.computeIfAbsent(name, k -> new AtomicInteger()).incrementAndGet();
                }
                return super.allocate(fd, size);
            }

            @Override
            public boolean close(long fd) {
                fdNames.remove(fd);
                return super.close(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                final String file = columnFile(name);
                if (file != null) {
                    fdNames.put(fd, file);
                    if (armed.get()) {
                        opens.computeIfAbsent(file, k -> new AtomicInteger()).incrementAndGet();
                    }
                }
                return fd;
            }

            private String columnFile(LPSZ name) {
                if (!Utf8s.containsAscii(name, Files.SEPARATOR + DAY)) {
                    return null;
                }
                final String path = Utf8s.stringFromUtf8Bytes(name);
                final String file = path.substring(path.lastIndexOf(Files.SEPARATOR) + 1);
                // Column files only: v.d, w.d, w.i, s.d.
                return file.length() == 3 && file.charAt(1) == '.' ? file : null;
            }
        };
        assertMemoryLeak(ff, () -> {
            // Pooled frame columns capture the FilesFacade they were built with; start from a fresh pool.
            engine.resetFrameFactory();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "8K");
            // Small pieces, so a batch cuts the partition and lands as several actions.
            node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 16);
            createTables();

            // Two O3 commits: the first may rebuild the partition as a fresh version (the start of its chain), the
            // second runs a plan against that version and leaves its frames in the cache.
            insertBoth(batch("T12:00:30", 10_000, 10));
            insertBoth(batch("T06:00:30", 20_000, 10));
            assertReusable(1);

            // Several clusters, so this is a plan of several MERGE and NEW_PIECE actions on the same files - and
            // enough rows that every file has to grow past the page the previous commit left it rounded up to.
            final String multi = "SELECT * FROM (" +
                    batch("T01:00:30", 30_000, 600) +
                    " UNION ALL " + batch("T06:03:15", 40_000, 600) +
                    " UNION ALL " + batch("T18:00:30", 50_000, 600) +
                    " UNION ALL " + batch("T23:40:00", 60_000, 600) +
                    ")";
            armed.set(true);
            execute("INSERT INTO t (ts, v, w, s) " + multi);
            drainWalQueue();
            armed.set(false);
            execute("INSERT INTO ref (ts, v, w, s) " + multi);

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));
            // The cached frames served the plan: no column file of the partition was opened for it...
            Assert.assertEquals("opens: " + opens, 0, opens.size());
            // ...and each file was grown exactly once, ahead of the plan, however many actions wrote to it.
            Assert.assertEquals("allocations: " + allocations, 1, count(allocations, "v.d"));
            Assert.assertEquals("allocations: " + allocations, 1, count(allocations, "w.d"));
            Assert.assertEquals("allocations: " + allocations, 1, count(allocations, "w.i"));
            // The 4-byte symbol column's new rows may fit inside the page the previous commit rounded its file up
            // to - a 16K page does - and then it needs no allocation at all.
            Assert.assertTrue("allocations: " + allocations, count(allocations, "s.d") <= 1);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);
            assertReusable(1);

            // A structural change lets the frames go; the next insert opens them anew, over the new shape.
            execute("ALTER TABLE t ADD COLUMN x INT");
            execute("ALTER TABLE ref ADD COLUMN x INT");
            drainWalQueue();
            assertReusable(0);
            final String withX = "SELECT timestamp_sequence('" + DAY + "T09:00:30', 60_000_000L) ts, 70_000 + x v, 'g' || x w, 's' || (x % 3) s, x::INT x FROM long_sequence(7)";
            execute("INSERT INTO t (ts, v, w, s, x) " + withX);
            drainWalQueue();
            execute("INSERT INTO ref (ts, v, w, s, x) " + withX);
            assertReusable(1);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);

            // An UPDATE rewrites column files under the frames' feet; the cache is emptied before it runs.
            execute("UPDATE t SET v = v + 1 WHERE v >= 70_000");
            execute("UPDATE ref SET v = v + 1 WHERE v >= 70_000");
            drainWalQueue();
            assertReusable(0);
            insertBoth(batch("T10:00:30", 80_000, 3));
            assertReusable(1);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);

            // Dropping the partition the frames are open on.
            execute("ALTER TABLE t DROP PARTITION LIST '" + DAY + "'");
            execute("ALTER TABLE ref DROP PARTITION LIST '" + DAY + "'");
            drainWalQueue();
            assertReusable(0);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext, "ref", "t", LOG);
        });
    }

    /**
     * How many partitions the next insert would find open in the cache.
     */
    private static void assertReusable(int expected) {
        try (TableWriter writer = getWriter("t")) {
            final CompositeFrameCache cache = writer.getCompositeFrameCache();
            Assert.assertNotNull(cache);
            Assert.assertEquals(expected, cache.getReusableCount(writer.getTxn()));
        }
    }

    /**
     * {@code rows} rows a second from {@code timeOfDay}. The varchar is longer than the aux entry holds inline, so it
     * lands in the data file.
     */
    private static String batch(String timeOfDay, int base, int rows) {
        return "SELECT timestamp_sequence('" + DAY + timeOfDay + "', 1_000_000L) ts, " + base + " + x v, '" + base + "-abcdefghijklmnop-' || x w, 's' || (x % 3) s FROM long_sequence(" + rows + ")";
    }

    private static int count(ConcurrentHashMap<String, AtomicInteger> counts, String file) {
        final AtomicInteger n = counts.get(file);
        return n != null ? n.get() : 0;
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE t" + DDL + " WAL");
        execute("CREATE TABLE ref" + DDL + " BYPASS WAL");
        // A row a minute through the day, plus a later day so this one is never the active partition.
        final String base = "SELECT timestamp_sequence('" + DAY + "', 60_000_000L) ts, x v, 'a' || x w, 's' || (x % 3) s FROM long_sequence(1440)" +
                " UNION ALL SELECT '2024-01-02T00:00:00'::TIMESTAMP ts, 0L v, 'z' w, 's0' s FROM long_sequence(1)";
        insertBoth(base);
    }

    private static void insertBoth(String select) throws Exception {
        execute("INSERT INTO t (ts, v, w, s) " + select);
        execute("INSERT INTO ref (ts, v, w, s) " + select);
        drainWalQueue();
    }
}
