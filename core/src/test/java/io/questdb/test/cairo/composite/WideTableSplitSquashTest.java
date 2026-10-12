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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.frm.file.CompositeFrameCache;
import io.questdb.std.FilesFacade;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Frame writes to a table wider than one batch of column tasks (64 columns): the commit's own squash on a non-WAL
 * table, {@code SQUASH PARTITIONS} and the compaction sweep on a WAL table, and a merge-append commit. Fixed and
 * var-size columns sit on both sides of the 64th column.
 * <p>
 * Every test runs under {@code cairo.commit.mode=sync}. A plan's frames keep every column file open whatever the
 * column count, so each file the plan writes opens for write once and is fsynced once, through the writable fd that
 * wrote it. The test facade fails an fsync of a column file opened read-only, the way Windows does: FlushFileBuffers
 * needs a handle with write access.
 */
public class WideTableSplitSquashTest extends AbstractCairoTest {
    private static final String DAY = "2020-01-01";
    private static final int EXTRA_COLUMNS = 70;
    // i, v and ts, then the extra ones.
    private static final int COLUMN_COUNT = EXTRA_COLUMNS + 3;
    private final AtomicBoolean armed = new AtomicBoolean();
    private final Map<Long, String> columnFdPaths = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> fsyncsByPath = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> rwOpensByPath = new ConcurrentHashMap<>();
    private final Map<Long, String> readOnlyFdPaths = new ConcurrentHashMap<>();
    private final FilesFacade ff = new TestFilesFacadeImpl() {
        @Override
        public boolean close(long fd) {
            columnFdPaths.remove(fd);
            readOnlyFdPaths.remove(fd);
            return super.close(fd);
        }

        @Override
        public void fsync(long fd) {
            final String readOnlyPath = readOnlyFdPaths.get(fd);
            if (readOnlyPath != null) {
                throw CairoException.critical(0).put("could not fsync read-only fd [path=").put(readOnlyPath).put(']');
            }
            final String path = columnFdPaths.get(fd);
            if (armed.get() && path != null) {
                fsyncsByPath.computeIfAbsent(path, k -> new AtomicInteger()).incrementAndGet();
            }
            super.fsync(fd);
        }

        @Override
        public long openRO(LPSZ name) {
            final long fd = super.openRO(name);
            if (fd > -1 && isColumnFile(name)) {
                readOnlyFdPaths.put(fd, Utf8s.stringFromUtf8Bytes(name));
            }
            return fd;
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            final long fd = super.openRW(name, opts);
            if (fd > -1 && isColumnFile(name) && Utf8s.containsAscii(name, DAY)) {
                final String path = Utf8s.stringFromUtf8Bytes(name);
                columnFdPaths.put(fd, path);
                if (armed.get()) {
                    rwOpensByPath.computeIfAbsent(path, k -> new AtomicInteger()).incrementAndGet();
                }
            }
            return fd;
        }

        private boolean isColumnFile(LPSZ name) {
            return Utf8s.endsWithAscii(name, ".d") || Utf8s.endsWithAscii(name, ".i");
        }
    };

    @Override
    @Before
    public void setUp() {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        // Stands in for a day bigger than the default 50MB split threshold.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        super.setUp();
    }

    @Test
    public void testCommitSquashNonWal() throws Exception {
        assertMemoryLeak(ff, () -> {
            // Pooled frame columns capture the FilesFacade they were built with; start from a fresh pool.
            engine.resetFrameFactory();
            createBaseDays("BYPASS WAL");
            // The O3 rows split the day and the same commit squashes it back.
            armed.set(true);
            insertO3();
            armed.set(false);
            Assert.assertEquals("the commit's squash must fold the split day", 2, partitionCount());
            // Opens are not counted: the same commit's O3 merge opens the day's files as well, to cut the split off
            // them. testSquashPartitionsWal counts the opens of the same squash.
            assertFilesFsyncedOnce();
            assertRows();
        });
    }

    @Test
    public void testMergeAppendWal() throws Exception {
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, true);
            createBaseDays("WAL");
            armed.set(true);
            insertO3();
            armed.set(false);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("w")));
            Assert.assertTrue("fixture: the O3 rows must land as a merge-append plan", isComposite());
            assertFilesOpenedOnce();
            assertFilesFsyncedOnce();
            // Too wide for the writer's frame cache: the plan's frames hold every column file open while it runs,
            // and close with the commit rather than stay open in an idle pooled writer.
            try (TableWriter writer = getWriter("w")) {
                final CompositeFrameCache cache = writer.getCompositeFrameCache();
                Assert.assertNotNull(cache);
                Assert.assertEquals(0, cache.getReusableCount(writer.getTxn()));
            }
            assertRows();
        });
    }

    @Test
    public void testSquashPartitionsWal() throws Exception {
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            createSplitDay();
            armed.set(true);
            execute("ALTER TABLE w SQUASH PARTITIONS");
            drainWalQueue();
            armed.set(false);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("w")));
            Assert.assertEquals(2, partitionCount());
            assertFilesOpenedOnce();
            assertFilesFsyncedOnce();
            assertRows();
        });
    }

    @Test
    public void testSweepSquashWal() throws Exception {
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            createSplitDay();
            final long idleTicks = MicrosecondClockImpl.INSTANCE.getTicks() + 2 * Micros.HOUR_MICROS;
            armed.set(true);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine, configuration.getFilesFacade(), () -> idleTicks)) {
                job.run();
            }
            drainWalQueue();
            armed.set(false);
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("w")));
            Assert.assertEquals(2, partitionCount());
            assertFilesOpenedOnce();
            assertFilesFsyncedOnce();
            assertRows();
        });
    }

    private static void createBaseDays(String walClause) throws Exception {
        // One full day, at 15s a row.
        execute("CREATE TABLE w AS (" + select(0, "timestamp_sequence('" + DAY + "', 15 * 1_000_000L)", 5760)
                + ") TIMESTAMP(ts) PARTITION BY DAY " + walClause);
        // A later day, so the first one is no longer the last partition.
        execute("INSERT INTO w " + select(90_000, "timestamp_sequence('2020-01-03', 60 * 1_000_000L)", 200));
        drainWalQueue();
    }

    private static void createSplitDay() throws Exception {
        createBaseDays("WAL");
        insertO3();
        Assert.assertEquals("fixture must leave the day split", 3, partitionCount());
    }

    private static void insertO3() throws Exception {
        // O3 into the first day's tail: splits it, or lands as a merge-append plan when that is on.
        execute("INSERT INTO w " + select(70_000, "timestamp_sequence('" + DAY + "T22:00:07', 5 * 1_000_000L)", 200));
        drainWalQueue();
    }

    private static boolean isComposite() {
        try (TableReader reader = engine.getReader(engine.verifyTableName("w"))) {
            final int partitionIndex = reader.getTxFile().getPartitionIndex(MicrosTimestampDriver.floor(DAY + "T00:00:00.000000Z"));
            return partitionIndex > -1 && reader.getTxFile().isPartitionComposite(partitionIndex);
        }
    }

    private static int partitionCount() {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        final TableToken token = engine.verifyTableName("w");
        try (TableReader reader = engine.getReader(token)) {
            return reader.getTxFile().getPartitionCount();
        }
    }

    private static String select(int iBase, String tsExpr, int rows) {
        final StringBuilder sb = new StringBuilder("SELECT x::INT + ").append(iBase).append(" i, x::VARCHAR v, ")
                .append(tsExpr).append(" ts");
        for (int c = 0; c < EXTRA_COLUMNS; c++) {
            // Var-size columns interleaved with fixed ones: a STRING and a VARCHAR every ten columns.
            final String type = switch (c % 10) {
                case 4 -> "STRING";
                case 9 -> "VARCHAR";
                default -> "LONG";
            };
            sb.append(", x::").append(type).append(" c").append(c);
        }
        return sb.append(" FROM long_sequence(").append(rows).append(')').toString();
    }

    /**
     * Every column file of the day the fold wrote was fsynced once - not once per action - and every column got its
     * files fsynced.
     */
    private void assertFilesFsyncedOnce() {
        for (Map.Entry<String, AtomicInteger> e : fsyncsByPath.entrySet()) {
            Assert.assertEquals("fsyncs of " + e.getKey(), 1, e.getValue().get());
        }
        Assert.assertTrue("fsynced column files: " + fsyncsByPath.keySet(), fsyncsByPath.size() >= COLUMN_COUNT);
    }

    /**
     * Every column file of the day the fold wrote was opened for write once, not once per action: what a frame that
     * closes its columns between actions does.
     */
    private void assertFilesOpenedOnce() {
        Assert.assertFalse("no column file was opened for write", rwOpensByPath.isEmpty());
        for (Map.Entry<String, AtomicInteger> e : rwOpensByPath.entrySet()) {
            Assert.assertEquals("opens for write of " + e.getKey(), 1, e.getValue().get());
        }
    }

    private void assertRows() throws Exception {
        final long base = 5760L * 5761 / 2;
        final long batch = 200L * 201 / 2;
        assertQuery("SELECT count() c, sum(i) si, sum(c0) s0, sum(c64::LONG) s64, sum(c68) s68, sum(c69::LONG) s69 FROM w")
                .noRandomAccess()
                .expectSize()
                .returns("c\tsi\ts0\ts64\ts68\ts69\n"
                        + "6160\t" + (base + 2 * batch + 200L * 90_000 + 200L * 70_000)
                        + '\t' + (base + 2 * batch)
                        + '\t' + (base + 2 * batch)
                        + '\t' + (base + 2 * batch)
                        + '\t' + (base + 2 * batch) + '\n');
        // Every column of a row still holds that row's value, the ones past the 64th included.
        assertQuery("SELECT count() c FROM w WHERE v::LONG <> c0 OR c63 <> c0 OR c64::LONG <> c0 OR c68 <> c0 OR c69::LONG <> c0")
                .noRandomAccess()
                .expectSize()
                .returns("c\n0\n");
    }
}
