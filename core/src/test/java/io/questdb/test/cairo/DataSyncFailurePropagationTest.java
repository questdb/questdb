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

package io.questdb.test.cairo;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.std.IntList;
import io.questdb.std.Os;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A failed disk flush in {@code sync} and {@code adaptive} mode must stop the engine wherever the flush runs: on
 * the writer thread, in an O3 or column task, in the WAL apply job, behind a wrapper or an error counter. Each
 * test runs a workload once to record its flushes, then once per distinct flush path with that flush failing, and
 * checks the engine stopped every time. A flush whose failure is dropped on its way to the handler fails the test
 * with the stack of the flush that was failed.
 */
@RunWith(Parameterized.class)
public class DataSyncFailurePropagationTest extends AbstractCairoTest {
    private static final int EIO = 5;
    private final String commitMode;
    private FlushFailureFacade ff;

    public DataSyncFailurePropagationTest(String commitMode) {
        this.commitMode = commitMode;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{"sync"}, {"adaptive"}});
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // Windows takes no directory flushes and fails some of the others differently; the paths are the same.
        Assume.assumeFalse(Os.isWindows());
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        // A save point after every apply batch, so its flushes are among the ones failed.
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        ff = new FlushFailureFacade();
    }

    @Test
    public void testCheckpointCreateAndRelease() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    try {
                        execute("CHECKPOINT CREATE");
                    } finally {
                        execute("CHECKPOINT RELEASE");
                    }
                }
        );
    }

    @Test
    public void testCreateAndDropLiveView() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("CREATE LIVE VIEW lv_" + table + " FLUSH EVERY 100ms START FROM NOW AS SELECT ts, s, sum(i) OVER (PARTITION BY s ORDER BY ts RANGE BETWEEN '1' SECOND PRECEDING AND CURRENT ROW) AS c FROM " + table);
                    execute("DROP LIVE VIEW lv_" + table);
                }
        );
    }

    @Test
    public void testCreateAndDropView() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("CREATE VIEW v_" + table + " AS (SELECT ts, s FROM " + table + " WHERE i > 2)");
                    drainWalAndViewQueues();
                    execute("DROP VIEW v_" + table);
                    drainWalAndViewQueues();
                }
        );
    }

    @Test
    public void testCreateAndRefreshMatView() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("CREATE MATERIALIZED VIEW mv_" + table + " AS (SELECT ts, count() c FROM " + table + " SAMPLE BY 1h) PARTITION BY DAY");
                    drainWalAndMatViewQueues();
                    insertDays(table, "2024-01-04", 1);
                    drainWalAndMatViewQueues();
                }
        );
    }

    @Test
    public void testCreateNonWalTable() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                },
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL")
        );
    }

    @Test
    public void testCreateWalTable() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                },
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL")
        );
    }

    @Test
    public void testNonWalAddColumn() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " ADD COLUMN c LONG")
        );
    }

    @Test
    public void testNonWalAlterColumnType() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " ALTER COLUMN i TYPE LONG")
        );
    }

    @Test
    public void testNonWalColumnPurge() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> {
                    execute("UPDATE " + table + " SET i = i + 1");
                    engine.releaseAllReaders();
                    drainPurgeJob();
                }
        );
    }

    @Test
    public void testNonWalConvertPartitionToNative() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    createNonWalTable(table);
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                },
                table -> execute("ALTER TABLE " + table + " CONVERT PARTITION TO NATIVE WHERE ts < '2024-01-03'")
        );
    }

    @Test
    public void testNonWalConvertPartitionToParquet() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'")
        );
    }

    @Test
    public void testNonWalDetachAttachPartition() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " DETACH PARTITION LIST '2024-01-01'");
                    renameDetachedToAttachable(table, "2024-01-01");
                    execute("ALTER TABLE " + table + " ATTACH PARTITION LIST '2024-01-01'");
                }
        );
    }

    @Test
    public void testNonWalDropAndRenameColumnAndAddIndex() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " RENAME COLUMN v TO v2");
                    execute("ALTER TABLE " + table + " DROP COLUMN v2");
                    execute("ALTER TABLE " + table + " ALTER COLUMN s ADD INDEX");
                }
        );
    }

    @Test
    public void testNonWalDropPartition() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("ALTER TABLE " + table + " DROP PARTITION LIST '2024-01-01'")
        );
    }

    @Test
    public void testNonWalInsertAcrossPages() throws Exception {
        // Small pages, so the row API releases (and in sync flushes) pages as it appends.
        node1.setProperty(PropertyKey.CAIRO_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> insertDays(table, "2024-01-04", 200)
        );
    }

    @Test
    public void testNonWalO3MergeIntoNativePartitions() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    insertDays(table, "2024-01-01", 3);
                },
                table -> insertOutOfOrder(table)
        );
    }

    @Test
    public void testNonWalO3MergeIntoParquetPartition() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
                    insertDays(table, "2024-01-01", 3);
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                },
                table -> insertOutOfOrder(table)
        );
    }

    @Test
    public void testNonWalTtlDropsPartitions() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY TTL 1 DAY BYPASS WAL");
                    insertDays(table, "2024-01-01", 3);
                },
                table -> execute("INSERT INTO " + table + " VALUES ('2024-01-06T00:00:00.000000Z', 1, 'a', 'ttl')")
        );
    }

    @Test
    public void testNonWalTruncate() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("TRUNCATE TABLE " + table)
        );
    }

    @Test
    public void testNonWalUpdate() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createNonWalTable,
                table -> execute("UPDATE " + table + " SET i = i + 1 WHERE ts < '2024-01-02'")
        );
    }

    @Test
    public void testWalAddColumn() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " ADD COLUMN c LONG");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalAlterColumnType() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " ALTER COLUMN i TYPE LONG");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalApplyBlockOfTransactions() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                },
                table -> {
                    execute("INSERT INTO " + table + " VALUES ('2024-01-01T10:30:00.000000Z', 101, 'b', 'w1')");
                    execute("INSERT INTO " + table + " VALUES ('2024-01-02T10:30:00.000000Z', 102, 'c', 'w2')");
                    execute("INSERT INTO " + table + " VALUES ('2024-01-03T23:30:00.000000Z', 103, 'd', 'w3')");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalApplyO3MergeIntoNativePartitions() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                },
                table -> {
                    insertOutOfOrder(table);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalApplyO3MergeIntoParquetPartition() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                    drainWalQueue();
                },
                table -> {
                    insertOutOfOrder(table);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalConvertPartitionToParquet() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalDedupUpsert() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> {
                    execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts)");
                    insertDays(table, "2024-01-01", 3);
                    drainWalQueue();
                },
                table -> {
                    insertDays(table, "2024-01-01", 2);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalDropAndRenameTable() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    // The dropped table's writer is closed by the next apply, outside the workload: a failed flush
                    // inside a writer close leaves that writer's files open in a test, where nothing halts.
                    execute("RENAME TABLE " + table + " TO " + table + "_r");
                    drainWalQueue();
                    execute("DROP TABLE " + table + "_r");
                }
        );
    }

    @Test
    public void testWalDropPartition() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("ALTER TABLE " + table + " DROP PARTITION LIST '2024-01-01'");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalFirstInsert() throws Exception {
        assertEveryFlushFailureStopsEngine(
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL"),
                table -> {
                    insertDays(table, "2024-01-01", 1);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalInsertAcrossPages() throws Exception {
        // Small pages, so the WAL writer's row API flips (and in sync flushes) pages as it appends.
        // Fixed-size columns only: a rollback after a failed page flip of a VARCHAR column crashes the JVM in a test
        // (production halts at the failed flush first).
        node1.setProperty(PropertyKey.CAIRO_WAL_WRITER_DATA_APPEND_PAGE_SIZE, 4096);
        assertEveryFlushFailureStopsEngine(
                table -> execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, l LONG) TIMESTAMP(ts) PARTITION BY DAY WAL"),
                table -> {
                    execute("INSERT INTO " + table + " SELECT timestamp_sequence('2024-01-01', 60_000_000L), x::INT, x FROM long_sequence(3000)");
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalTruncate() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("TRUNCATE TABLE " + table);
                    drainWalQueue();
                }
        );
    }

    @Test
    public void testWalUpdate() throws Exception {
        assertEveryFlushFailureStopsEngine(
                DataSyncFailurePropagationTest::createWalTable,
                table -> {
                    execute("UPDATE " + table + " SET i = i + 1 WHERE ts < '2024-01-02'");
                    drainWalQueue();
                }
        );
    }

    private static void createNonWalTable(String table) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        insertDays(table, "2024-01-01", 3);
    }

    private static void createWalTable(String table) throws Exception {
        execute("CREATE TABLE " + table + " (ts TIMESTAMP, i INT, s SYMBOL, v VARCHAR) TIMESTAMP(ts) PARTITION BY DAY WAL");
        insertDays(table, "2024-01-01", 3);
        drainWalQueue();
    }

    private static void insertDays(String table, String firstDay, int days) throws Exception {
        execute("INSERT INTO " + table + " SELECT timestamp_sequence('" + firstDay + "', 7_200_000_000L), x::INT, " +
                "rnd_symbol('a', 'b', 'c'), rnd_varchar(1, 40, 0) FROM long_sequence(" + days * 12 + ")");
    }

    private static void insertOutOfOrder(String table) throws Exception {
        execute("INSERT INTO " + table + " SELECT timestamp_sequence('2024-01-01T01:00:00.000000Z', 10_800_000_000L), " +
                "(1_000 + x)::INT, rnd_symbol('a', 'b', 'z'), rnd_varchar(1, 40, 0) FROM long_sequence(12)");
    }

    private void assertEveryFlushFailureStopsEngine(Workload setup, Workload action) throws Exception {
        assertMemoryLeak(ff, () -> {
            setup.run("dry_run");
            ff.arm(-1);
            action.run("dry_run");
            ff.disarm();
            // A workload may take no flush in a mode (a non-WAL O3 commit in adaptive), which leaves nothing to fail.
            // Flushes reached through the same stack take the same path to a handler, so only the first is failed.
            final int flushCount = ff.getFlushCount();
            final IntList firstOfEachPath = new IntList();
            firstOfEachPath.addAll(ff.getFirstFlushOfEachPath());

            final StringBuilder failures = new StringBuilder();
            int injectedCount = 0;
            for (int i = 0, n = firstOfEachPath.size(); i < n; i++) {
                final int failAt = firstOfEachPath.getQuick(i);
                final String table = "t_" + failAt;
                setup.run(table);
                ff.arm(failAt);
                try {
                    action.run(table);
                } catch (Throwable ignore) {
                    // the statement or the apply fails; the check is on the engine
                } finally {
                    ff.disarm();
                }
                final Throwable injection = ff.getInjection();
                if (injection != null) {
                    injectedCount++;
                }
                if (injection != null && !engine.isDurabilityFailed()) {
                    failures.append("flush ").append(failAt).append(" of ").append(flushCount)
                            .append(" failed without stopping the engine, at:\n").append(stackOf(injection));
                }
                engine.resetDurabilityFailure();
                engine.releaseInactive();
            }
            if (!failures.isEmpty()) {
                Assert.fail("[mode=" + commitMode + "]\n" + failures);
            }
            Assert.assertTrue("most flushes of the dry run were not reached again", injectedCount * 2 >= firstOfEachPath.size());
        });
    }

    private static void renameDetachedToAttachable(String table, String partition) throws Exception {
        final java.nio.file.Path tableDir = Paths.get(root, engine.verifyTableName(table).getDirName());
        Files.move(tableDir.resolve(partition + ".detached"), tableDir.resolve(partition + ".attachable"));
    }

    private static String stackOf(Throwable injection) {
        final StringBuilder sink = new StringBuilder();
        final StackTraceElement[] frames = injection.getStackTrace();
        int printed = 0;
        for (int i = 0; i < frames.length && printed < 14; i++) {
            final String className = frames[i].getClassName();
            if (className.startsWith("io.questdb.") && !className.startsWith("io.questdb.test.")) {
                sink.append("    ").append(frames[i]).append('\n');
                printed++;
            }
        }
        return sink.toString();
    }

    @FunctionalInterface
    private interface Workload {
        void run(String table) throws Exception;
    }

    private static final class FlushFailureFacade extends TestFilesFacadeImpl {
        private final AtomicInteger flushCount = new AtomicInteger();
        private final IntList firstFlushOfEachPath = new IntList();
        private final HashSet<String> paths = new HashSet<>();
        private volatile int failAt = -1;
        private volatile Throwable injection;
        private volatile boolean isCounting;

        @Override
        public void fdatasync(long fd) {
            if (shouldFail()) {
                throw CairoException.dataSyncFailure(this, EIO, "fdatasync", "injected fdatasync failure");
            }
            super.fdatasync(fd);
        }

        @Override
        public void fsync(long fd) {
            if (shouldFail()) {
                throw CairoException.dataSyncFailure(this, EIO, "fsync", "injected fsync failure");
            }
            super.fsync(fd);
        }

        @Override
        public void fsyncAndClose(long fd) {
            if (shouldFail()) {
                super.close(fd);
                throw CairoException.dataSyncFailure(this, EIO, "fsyncAndClose", "injected fsync failure");
            }
            super.fsyncAndClose(fd);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!async && shouldFail()) {
                throw CairoException.dataSyncFailure(this, EIO, "msync", "injected msync failure");
            }
            super.msync(addr, len, async);
        }

        @Override
        public int sync() {
            if (shouldFail()) {
                return -1;
            }
            return super.sync();
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            if (shouldFail()) {
                return -1;
            }
            return super.syncFileRange(fd, offset, nbytes, flags);
        }

        @Override
        public void syncfs(long fd) {
            if (shouldFail()) {
                throw CairoException.dataSyncFailure(this, EIO, "syncfs", "injected syncfs failure");
            }
            super.syncfs(fd);
        }

        void arm(int failAt) {
            flushCount.set(0);
            firstFlushOfEachPath.clear();
            paths.clear();
            injection = null;
            this.failAt = failAt;
            isCounting = true;
        }

        void disarm() {
            isCounting = false;
        }

        IntList getFirstFlushOfEachPath() {
            return firstFlushOfEachPath;
        }

        int getFlushCount() {
            return flushCount.get();
        }

        Throwable getInjection() {
            return injection;
        }

        private boolean shouldFail() {
            if (!isCounting) {
                return false;
            }
            final int flush = flushCount.incrementAndGet();
            if (failAt < 0) {
                synchronized (paths) {
                    if (paths.add(stackOf(new Throwable()))) {
                        firstFlushOfEachPath.add(flush);
                    }
                }
                return false;
            }
            if (flush == failAt) {
                injection = new Throwable("injected flush failure " + failAt);
                return true;
            }
            return false;
        }
    }
}
