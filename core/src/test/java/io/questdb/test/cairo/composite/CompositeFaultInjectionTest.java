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


package io.questdb.test.cairo.composite;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Fault-injection reproducers from the PR #7595 review follow-up.
 */
public class CompositeFaultInjectionTest extends AbstractCairoTest {

    @Test
    public void testBitmapIndexDuplicatesAfterFailedMergeAppend() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                if (armed.get() && stackHas("indexWrittenRows")) {
                    armed.set(false);
                    return false;
                }
                return super.allocate(fd, size);
            }
        };
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x FROM long_sequence(2880)");
            drainWalQueue();
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T03:00:00.25', 1_000_000L), 'q' || (x % 4), x + 300_000 FROM long_sequence(50)");
            armed.set(true);
            drainWalQueue();
            Assert.assertFalse("fault did not fire", armed.get());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();

            assertQuery("SELECT count() c FROM t").noRandomAccess().expectSize().returns("c\n2930\n");
            assertQuery("SELECT count() c, count_distinct(v) d FROM t WHERE s = 'k3'").noRandomAccess().expectSize()
                    .returns("c\td\n288\t288\n");
            assertQuery("SELECT count() c, count_distinct(v) d FROM t WHERE s = 'q1'").noRandomAccess().expectSize()
                    .returns("c\td\n13\t13\n");
        });
    }

    @Test
    public void testBitmapIndexDuplicatesAfterFailedMidPartitionAppend() throws Exception {
        // O3 append in place into a partition that is not the last one, through O3CopyJob.updateIndex rather than
        // merge-append: the same add-loop failure must not leave entries the retry indexes again
        final AtomicBoolean armed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean allocate(long fd, long size) {
                if (armed.get() && stackHas("updateIndex")) {
                    armed.set(false);
                    return false;
                }
                return super.allocate(fd, size);
            }
        };
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(ff, () -> {
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x FROM long_sequence(720)");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-02', 60_000_000L), 'k' || (x % 10), x + 100_000 FROM long_sequence(100)");
            drainWalQueue();
            // appends after 2024-01-01's last row; the 'k' rows fill the keys' open value blocks, so the first
            // allocation, a new 'q' block, fails with 1000 rows already indexed
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T13:00:00', 1_000_000L)," +
                    " CASE WHEN x <= 1000 THEN 'k' || (x % 10) ELSE 'q' || (x % 4) END, x + 300_000 FROM long_sequence(5000)");
            armed.set(true);
            drainWalQueue();
            Assert.assertFalse("fault did not fire", armed.get());
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();

            assertQuery("SELECT count() c FROM t").noRandomAccess().expectSize().returns("c\n5820\n");
            assertQuery("SELECT count() c, count_distinct(v) d FROM t WHERE s = 'k3'").noRandomAccess().expectSize()
                    .returns("c\td\n182\t182\n");
            assertQuery("SELECT count() c, count_distinct(v) d FROM t WHERE s = 'q1'").noRandomAccess().expectSize()
                    .returns("c\td\n1000\t1000\n");
        });
    }

    @Test
    public void testCompositeUpgradeMetaFsyncFailureLeavesMetaHigh() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        assertMemoryLeak(metaFsyncFailureFacade(armed), () -> {
            execute("CREATE TABLE cx AS (SELECT x::INT i, timestamp_sequence(0, 15 * 1_000_000L) ts FROM long_sequence(400))" +
                    " TIMESTAMP(ts) PARTITION BY DAY WAL");
            drainWalQueue();
            Assert.assertEquals(ColumnType.VERSION, metaStorageVersion());

            // the upgrade lands in _meta, then its fsync fails before _txn publishes the composite partition
            execute("INSERT INTO cx SELECT x::INT + 70_000 i, timestamp_sequence('1970-01-01T00:20:07', 1_000_000L) ts FROM long_sequence(3)");
            armed.set(true);
            drainWalQueue();
            Assert.assertFalse("fault did not fire", armed.get());
            final TableToken tableToken = engine.verifyTableName("cx");
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, writerStorageVersion());

            execute("ALTER TABLE cx RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            try (TableReader reader = engine.getReader(tableToken)) {
                Assert.assertTrue("fixture produced no composite partition", reader.getTxFile().isPartitionComposite(0));
            }
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, writerStorageVersion());
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n403\n");
        });
    }

    @Test
    public void testCoveredValuesNullAfterOpenFailure() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && stackHas("mapColumnFile")) {
                    armed.set(false);
                    return -1;
                }
                return super.openRO(name);
            }
        };
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            execute("CREATE TABLE t (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v), v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x FROM long_sequence(2880)");
            drainWalQueue();
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01T03:00:00.25', 1_000_000L), 'q' || (x % 4), x + 300_000 FROM long_sequence(50)");
            armed.set(true);
            drainWalQueue();
            Assert.assertFalse("fault did not fire", armed.get());
            // the failed open of the covered column must fail the commit, not publish NULL covered values
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));

            execute("ALTER TABLE t RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("t")));

            assertQuery("SELECT count() c, count(v) nn FROM t").noRandomAccess().expectSize().returns("c\tnn\n2930\t2930\n");
            assertQuery("SELECT count() c, count(v) nn, sum(v) sv FROM t WHERE s = 'q1'").noRandomAccess().expectSize()
                    .returns("c\tnn\tsv\n13\t13\t3900325\n");
        });
    }

    @Test
    public void testCoveredValuesSurviveEveryOpenFailure() throws Exception {
        // fails each covered column open of the merge-append commit in turn, fixed-size and var-size (aux) alike:
        // every one has to either fail the commit or leave the covered values intact, never publish NULLs
        final AtomicInteger openCountdown = new AtomicInteger();
        final FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (openCountdown.get() > 0 && stackHas("mapColumnFile") && openCountdown.decrementAndGet() == 0) {
                    return -1;
                }
                return super.openRO(name);
            }
        };
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        assertMemoryLeak(ff, () -> {
            engine.resetFrameFactory();
            int failedCommits = 0;
            for (int failAt = 1; ; failAt++) {
                final String table = "t" + failAt;
                execute("CREATE TABLE " + table + " (ts TIMESTAMP, s SYMBOL INDEX TYPE POSTING INCLUDE (v, vs), v LONG, vs VARCHAR)" +
                        " TIMESTAMP(ts) PARTITION BY DAY WAL");
                execute("INSERT INTO " + table + " SELECT timestamp_sequence('2024-01-01', 60_000_000L), 'k' || (x % 10), x, 'a' || x" +
                        " FROM long_sequence(2880)");
                drainWalQueue();
                execute("INSERT INTO " + table + " SELECT timestamp_sequence('2024-01-01T03:00:00.25', 1_000_000L), 'q' || (x % 4)," +
                        " x + 300_000, 'b' || x FROM long_sequence(50)");
                openCountdown.set(failAt);
                drainWalQueue();
                final boolean fired = openCountdown.get() == 0;
                openCountdown.set(0);
                final TableToken token = engine.verifyTableName(table);
                if (engine.getTableSequencerAPI().isSuspended(token)) {
                    failedCommits++;
                    execute("ALTER TABLE " + table + " RESUME WAL");
                    drainWalQueue();
                    Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(token));
                }

                assertQuery("SELECT count() c, count(v) nn, count(vs) nvs FROM " + table).noRandomAccess().expectSize()
                        .returns("c\tnn\tnvs\n2930\t2930\t2930\n");
                assertQuery("SELECT count() c, count(v) nn, sum(v) sv, count(vs) nvs, min(vs) mvs FROM " + table + " WHERE s = 'q1'")
                        .noRandomAccess().expectSize()
                        .returns("c\tnn\tsv\tnvs\tmvs\n13\t13\t3900325\t13\tb1\n");
                execute("DROP TABLE " + table);
                if (!fired) {
                    break;
                }
            }
            Assert.assertTrue("no covered column open failed the commit", failedCommits > 0);
        });
    }

    @Test
    public void testSwapMetaFsyncFailureKeepsCompositeUpgrade() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        assertMemoryLeak(metaFsyncFailureFacade(armed), () -> {
            createSwapFixture();
            armed.set(true);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }
            Assert.assertFalse("fault did not fire", armed.get());
            // the downgrade landed in _meta before its fsync failed
            Assert.assertEquals(ColumnType.VERSION, metaStorageVersion());

            // a composite partition again: _meta has to be raised before _txn publishes it
            execute("INSERT INTO cx SELECT x::INT + 90_000 i, timestamp_sequence('1970-01-01T00:20:08', 1_000_000L) ts FROM long_sequence(3)");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("cx");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            try (TableReader reader = engine.getReader(tableToken)) {
                Assert.assertTrue("fixture produced no composite partition", reader.getTxFile().isPartitionComposite(0));
            }
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, writerStorageVersion());
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n456\n");
        });
    }

    @Test
    public void testSwapMetaOpenFailureBreaksNextCommit() throws Exception {
        // ADAPTIVE defers the housekeeping this asserts (fold, MAKE-PLAIN, purge) until the next save point moves the
        // durable-epoch pin off the range, so it pins NOSYNC like O3SquashPartitionTest, and keeps running under every sweep.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean armed = new AtomicBoolean();
        assertMemoryLeak(metaOpenFailureFacade(armed), () -> {
            createSwapFixture();
            armed.set(true);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }
            Assert.assertFalse("fault did not fire", armed.get());
            assertSwapCommittedAndNextCommitSucceeds();
        });
    }

    @Test
    public void testSwapMetaOpenFailureBreaksNextCommitQueued() throws Exception {
        // ADAPTIVE defers the housekeeping this asserts (fold, MAKE-PLAIN, purge) until the next save point moves the
        // durable-epoch pin off the range, so it pins NOSYNC like O3SquashPartitionTest, and keeps running under every sweep.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final AtomicBoolean armed = new AtomicBoolean();
        assertMemoryLeak(metaOpenFailureFacade(armed), () -> {
            createSwapFixture();
            armed.set(true);
            try (TableWriter writer = engine.getWriter(engine.verifyTableName("cx"), "test")) {
                // the held writer makes the job queue the swap on it instead of applying it inline
                try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                    job.run();
                }
                Assert.assertTrue("swap ran inline, not queued", armed.get());
                writer.tick(true);
            }
            Assert.assertFalse("fault did not fire", armed.get());
            assertSwapCommittedAndNextCommitSucceeds();
        });
    }

    @Test
    public void testSwapMetaOpenFailureThenDdlKeepsCompositeUpgrade() throws Exception {
        final AtomicBoolean armed = new AtomicBoolean();
        assertMemoryLeak(metaOpenFailureFacade(armed), () -> {
            createSwapFixture();
            armed.set(true);
            try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
                job.run();
            }
            Assert.assertFalse("fault did not fire", armed.get());
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());

            // the full _meta rewrite stamps the plain version, the table holding no composite partition
            execute("ALTER TABLE cx ADD COLUMN extra INT");
            drainWalQueue();
            Assert.assertEquals(ColumnType.VERSION, metaStorageVersion());

            // a composite partition again: _meta has to be raised before _txn publishes it
            execute("INSERT INTO cx (i, ts) SELECT x::INT + 90_000 i, timestamp_sequence('1970-01-01T00:20:08', 1_000_000L) ts FROM long_sequence(3)");
            drainWalQueue();
            final TableToken tableToken = engine.verifyTableName("cx");
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
            try (TableReader reader = engine.getReader(tableToken)) {
                Assert.assertTrue("fixture produced no composite partition", reader.getTxFile().isPartitionComposite(0));
            }
            Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());
            assertQuery("SELECT count() c FROM cx").noRandomAccess().expectSize().returns("c\n456\n");
        });
    }

    /**
     * The swap's _txn commit is durable before the _meta downgrade fails, so the swap stands, its post-commit
     * cleanup runs, _meta stays conservatively high, and the next commit both succeeds and retries the downgrade.
     */
    private void assertSwapCommittedAndNextCommitSucceeds() throws Exception {
        final TableToken tableToken = engine.verifyTableName("cx");
        Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());
        try (TableReader reader = engine.getReader(tableToken)) {
            Assert.assertFalse(reader.getTxFile().isPartitionComposite(0));
        }
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken).concat("1970-01-01");
            Assert.assertFalse("swapped-out source directory left behind", configuration.getFilesFacade().exists(path.$()));
        }

        execute("INSERT INTO cx SELECT x::INT + 90_000 i, timestamp_sequence('1970-01-02T01:00:00', 1_000_000L) ts FROM long_sequence(5)");
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));
        Assert.assertEquals(ColumnType.VERSION, metaStorageVersion());
        assertQuery("SELECT count() c, sum(i) s FROM cx").noRandomAccess().expectSize().returns("c\ts\n458\t4741496\n");
    }

    private static void createSwapFixture() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, "1K");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 8);
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-01T00:00:00.000000Z"));
        execute("CREATE TABLE cx AS (SELECT x::INT i, timestamp_sequence(0, 15 * 1_000_000L) ts FROM long_sequence(400))" +
                " TIMESTAMP(ts) PARTITION BY DAY WAL");
        drainWalQueue();
        execute("INSERT INTO cx SELECT x::INT + 70_000 i, timestamp_sequence('1970-01-01T00:20:07', 1_000_000L) ts FROM long_sequence(3)");
        drainWalQueue();
        execute("INSERT INTO cx SELECT x::INT + 80_000 i, timestamp_sequence('1970-01-02T00:00:10', 1_000_000L) ts FROM long_sequence(50)");
        drainWalQueue();
        Assert.assertEquals(ColumnType.MAX_STORAGE_VERSION, metaStorageVersion());

        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_IDLE_TIMEOUT, "1h");
        setCurrentMicros(MicrosFormatUtils.parseTimestamp("1970-01-05T00:00:00.000000Z"));
    }

    /**
     * Fails, once, the fsync that follows the storage version write in {@code _meta}, after the write has landed.
     */
    private static FilesFacade metaFsyncFailureFacade(AtomicBoolean armed) {
        return new TestFilesFacadeImpl() {
            private long metaFd = -1;

            @Override
            public void fsync(long fd) {
                if (fd == metaFd && armed.compareAndSet(true, false)) {
                    metaFd = -1;
                    throw CairoException.critical(5).put("could not fsync [fd=").put(fd).put(']');
                }
                super.fsync(fd);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                final long fd = super.openRW(name, opts);
                if (armed.get() && Utf8s.endsWithAscii(name, "_meta") && stackHas("writeStorageVersionToMeta")) {
                    metaFd = fd;
                }
                return fd;
            }
        };
    }

    private static FilesFacade metaOpenFailureFacade(AtomicBoolean armed) {
        return new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get() && Utf8s.endsWithAscii(name, "_meta") && stackHas("writeStorageVersionToMeta")) {
                    armed.set(false);
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };
    }

    /**
     * Reads the storage version straight off the {@code _meta} file on disk, not the writer's cached copy.
     */
    private static int metaStorageVersion() {
        try (
                MemoryMR mem = Vm.getCMRInstance();
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(engine.verifyTableName("cx")).concat(TableUtils.META_FILE_NAME);
            mem.smallFile(configuration.getFilesFacade(), path.$(), MemoryTag.MMAP_DEFAULT);
            return mem.getInt(TableUtils.META_OFFSET_VERSION);
        }
    }

    private static boolean stackHas(String method) {
        return StackWalker.getInstance().walk(s -> s.anyMatch(f -> f.getMethodName().equals(method)));
    }

    /**
     * The storage version the pooled writer caches, which decides whether its next commit rewrites {@code _meta}.
     */
    private static int writerStorageVersion() {
        try (TableWriter writer = engine.getWriter(engine.verifyTableName("cx"), "test")) {
            return ((TableWriterMetadata) writer.getMetadata()).getStorageVersion();
        }
    }
}
