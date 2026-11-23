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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PostingSealPurgeJob;
import io.questdb.cairo.PostingSealPurgeOperator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The POSTING seal purge deletes a superseded generation only after the state that replaced it is on disk: the files
 * of the live generation, the {@code .pk} that names it, and the table's {@code _txn}. Under NOSYNC and ASYNC no commit
 * flushes them, and a rebuild of the index of an older partition by an out-of-order commit flushes nothing either. A
 * power loss after the purge then must not leave a {@code .pk} on disk that names the deleted generation, or a
 * {@code _txn} below the txn of the chain entry that superseded it. Under SYNC, and on a WAL table with a save point
 * pin, that state is durable already, and the purge flushes nothing.
 */
public class PostingSealPurgeDurabilityCrashTest extends AbstractCrashConsistencyTest {
    private static final String ALL_ROWS_REBUILD_TWO = "count\tsum\n7\t28\n";
    // Two keys in each of two older partitions and a third, last partition.
    private static final String INSERT_R0 = """
            INSERT INTO t VALUES
            ('2024-01-01T01:00:00.000000Z', 'a', 1), ('2024-01-01T03:00:00.000000Z', 'b', 2),
            ('2024-01-02T01:00:00.000000Z', 'a', 3), ('2024-01-02T03:00:00.000000Z', 'b', 4),
            ('2024-01-03T01:00:00.000000Z', 'a', 5)""";
    // A row between the rows of 2024-01-01.
    private static final String INSERT_REBUILD_ONE = "INSERT INTO t VALUES ('2024-01-01T02:00:00.000000Z', 'b', 6)";
    // A row between the rows of 2024-01-01 and one between the rows of 2024-01-02.
    private static final String INSERT_REBUILD_TWO = "INSERT INTO t VALUES ('2024-01-01T02:00:00.000000Z', 'b', 6), ('2024-01-02T02:00:00.000000Z', 'a', 7)";
    private static final Pattern INDEX_FILE = Pattern.compile("k\\.(pk|pv|pc\\d+|pci).*");
    private static final Pattern VALUE_FILE = Pattern.compile("k\\.pv\\..*");
    // The names of the files the purge operator flushed, in order.
    private final List<String> purgeFlushes = Collections.synchronizedList(new ArrayList<>());
    // The name of the file whose next flush by the purge operator fails, or null.
    private volatile String failingPurgeFlush;

    @Override
    public void setUp() {
        super.setUp();
        // The production default: an out-of-order commit between the rows of an older partition rewrites it and
        // rebuilds its index.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        // Retry a held purge on every job run.
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY, 1);
        setProperty(PropertyKey.CAIRO_SQL_COLUMN_PURGE_RETRY_DELAY_LIMIT, 1);
        purgeFlushes.clear();
        failingPurgeFlush = null;
    }

    @Test
    public void testPartitionSwitchSealPurgeTxnNotWrittenBackAsync() throws Exception {
        assertPartitionSwitchSealPurge("async");
    }

    @Test
    public void testPartitionSwitchSealPurgeTxnNotWrittenBackNosync() throws Exception {
        assertPartitionSwitchSealPurge("nosync");
    }

    @Test
    public void testPurgeKeepsGenerationWhenFlushFails() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        runWithCrashFacade(() -> {
            createPurgeLogTable();
            createTable(false, false);
            execute(INSERT_R0);
            execute(INSERT_REBUILD_ONE);
            final TableToken token = engine.verifyTableName("t");
            engine.releaseAllReaders();
            failingPurgeFlush = "k.pk";
            try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
                job.run();
                Assert.assertNull("the flush did not come", failingPurgeFlush);
                Assert.assertEquals("purges left after the failed flush", 1, job.getOutstandingPurgeTasks());
                Assert.assertEquals(List.of("k.pv.0", "k.pv.1"), valueFiles(token, "2024-01-01.1"));
                for (int i = 0; i < 16 && job.getOutstandingPurgeTasks() > 0; i++) {
                    Os.sleep(1);
                    job.run();
                }
                Assert.assertEquals("purges left after the retry", 0, job.getOutstandingPurgeTasks());
            }
            Assert.assertEquals(List.of("k.pv.1"), valueFiles(token, "2024-01-01.1"));
            assertIndexMatchesScan("count\tsum\n6\t21\n");
        });
    }

    @Test
    public void testPurgeFlushesNothingUnderSavePointPin() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        runWithCrashFacade(() -> {
            createPurgeLogTable();
            createTable(true, true);
            execute(INSERT_R0);
            drainWalQueue();
            execute(INSERT_REBUILD_TWO);
            drainWalQueue();
            final TableToken token = engine.verifyTableName("t");
            Assert.assertTrue("a save point pins the table",
                    engine.getTableSequencerAPI().getTxnTracker(token).getPinnedEpochTxn() >= 0);
            runPurgeJob();
            assertOneValueFilePerPartition(token);
            Assert.assertEquals("", String.join(" ", purgeFlushes));
        });
    }

    @Test
    public void testPurgeFlushesNothingUnderSync() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            Assert.assertEquals(CommitMode.SYNC, engine.getResolvedCommitMode());
            createPurgeLogTable();
            createTable(true, false);
            execute(INSERT_R0);
            execute(INSERT_REBUILD_TWO);
            runPurgeJob();
            assertOneValueFilePerPartition(engine.verifyTableName("t"));
            Assert.assertEquals("", String.join(" ", purgeFlushes));
        });
    }

    @Test
    public void testPurgeFlushesSupersedingStateOnce() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        runWithCrashFacade(() -> {
            createPurgeLogTable();
            createTable(true, false);
            execute(INSERT_R0);
            // One commit rebuilds the index of two partitions: purges with the same reader window.
            execute(INSERT_REBUILD_TWO);
            runPurgeJob();
            final TableToken token = engine.verifyTableName("t");
            assertOneValueFilePerPartition(token);
            // Each purge flushes the live value file, its two cover files (v and the designated timestamp), the .pci
            // and the .pk; only the first flushes _txn, which covers the reader window of all.
            final String generation = "k\\.pv\\.\\d+ k\\.pc\\d\\.\\S+ k\\.pc\\d\\.\\S+ k\\.pci k\\.pk ";
            final String flushes = String.join(" ", purgeFlushes) + " ";
            Assert.assertTrue(flushes, Pattern.matches(generation + "_txn (" + generation + ")+", flushes));
            assertIndexMatchesScan(ALL_ROWS_REBUILD_TWO);
        });
    }

    @Test
    public void testRebuildPurgeIndexNotWrittenBackAsync() throws Exception {
        assertRebuildPurge("async");
    }

    @Test
    public void testRebuildPurgeIndexNotWrittenBackNosync() throws Exception {
        assertRebuildPurge("nosync");
    }

    @Override
    protected CrashFaultFilesFacade newCrashFacade() {
        return new CrashFaultFilesFacade() {
            @Override
            public void fdatasync(long fd) {
                recordPurgeFlush(fd);
                super.fdatasync(fd);
            }

            @Override
            public void fsync(long fd) {
                recordPurgeFlush(fd);
                super.fsync(fd);
            }

            @Override
            public void fsyncAndClose(long fd) {
                recordPurgeFlush(fd);
                super.fsyncAndClose(fd);
            }

            @Override
            public void syncfs(long fd) {
                recordPurgeFlush(fd);
                super.syncfs(fd);
            }

            private void recordPurgeFlush(long fd) {
                if (isOnPurgeOperatorStack()) {
                    final String path = Files.findPathOfFd(fd);
                    final String name = path == null ? "fd" : Paths.get(path).getFileName().toString();
                    purgeFlushes.add(name);
                    if (name.equals(failingPurgeFlush)) {
                        failingPurgeFlush = null;
                        close(fd);
                        throw CairoException.critical(5).put("test flush failure [file=").put(name).put(']');
                    }
                }
            }
        };
    }

    // Makes every directory entry under the table durable, so that the unlinks of the purge survive the power loss.
    private static void fsyncTableDirs(String tableDir) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(Paths.get(tableDir), 1)) {
            paths.filter(java.nio.file.Files::isDirectory).forEach(dir -> {
                final long fd = ff.openRONoCache(Path.getThreadLocal(dir.toString()).$());
                Assert.assertTrue(fd > -1);
                ff.fsyncAndClose(fd);
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean isOnPurgeOperatorStack() {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(
                f -> f.getClassName().equals(PostingSealPurgeOperator.class.getName())));
    }

    private static void runPurgeJob() throws Exception {
        engine.releaseAllReaders();
        try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
            for (int i = 0; i < 16 && (i == 0 || job.getOutstandingPurgeTasks() > 0); i++) {
                Os.sleep(1);
                job.run();
            }
            Assert.assertEquals("purges left", 0, job.getOutstandingPurgeTasks());
        }
    }

    private static String tableDir(TableToken token) {
        return Paths.get(root, token.getDirName()).toString();
    }

    private static List<String> valueFiles(TableToken token, String partitionDir) {
        final String[] names = Paths.get(tableDir(token), partitionDir).toFile().list((dir, name) -> VALUE_FILE.matcher(name).matches());
        Assert.assertNotNull(partitionDir, names);
        Arrays.sort(names);
        return Arrays.asList(names);
    }

    private void assertIndexMatchesScan(String expectedCountSum) throws Exception {
        for (String key : new String[]{"a", "b"}) {
            assertSqlCursors(
                    "SELECT ts, k, v FROM t WHERE k::STRING = '" + key + "'",
                    "SELECT ts, k, v FROM t WHERE k = '" + key + "'"
            );
        }
        assertQuery("SELECT count(), sum(v) FROM t")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns(expectedCountSum);
    }

    // Every partition holds one value file of the index: the purge deleted every superseded generation.
    private void assertOneValueFilePerPartition(TableToken token) {
        final Map<String, List<String>> byPartition = new TreeMap<>();
        try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(Paths.get(tableDir(token)), 2)) {
            paths.filter(p -> VALUE_FILE.matcher(p.getFileName().toString()).matches()).forEach(p ->
                    byPartition.computeIfAbsent(p.getParent().getFileName().toString(), k -> new ArrayList<>())
                            .add(p.getFileName().toString()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Assert.assertFalse("no value files", byPartition.isEmpty());
        for (Map.Entry<String, List<String>> e : byPartition.entrySet()) {
            Assert.assertEquals("value files in " + e.getKey() + ": " + e.getValue(), 1, e.getValue().size());
        }
    }

    // A non-WAL commit into the next partition seals the index of the last one; the purge deletes the generation it
    // superseded. The kernel wrote back every file of the table but _txn.
    private void assertPartitionSwitchSealPurge(String commitMode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_POSTING_SEAL_GEN_THRESHOLD, 1);
        runWithCrashFacade(() -> {
            createPurgeLogTable();
            createTable(false, false);
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 'a', 1), ('2024-01-01T02:00:00.000000Z', 'b', 2)");
            execute("INSERT INTO t VALUES ('2024-01-01T03:00:00.000000Z', 'a', 3), ('2024-01-01T04:00:00.000000Z', 'b', 4)");
            powerLossAfterPurge(
                    "INSERT INTO t VALUES ('2024-01-01T05:00:00.000000Z', 'a', 5), ('2024-01-02T01:00:00.000000Z', 'b', 6)",
                    true,
                    "count\tsum\n6\t21\n"
            );
        });
    }

    // An out-of-order commit between the rows of an older partition rewrites it into a new partition version and
    // rebuilds its index there, which flushes nothing outside SYNC; the purge deletes the generation the rebuild
    // superseded. The kernel wrote back every file of the table but the index files of the rewritten partition.
    private void assertRebuildPurge(String commitMode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        runWithCrashFacade(() -> {
            createPurgeLogTable();
            createTable(false, false);
            execute(INSERT_R0);
            powerLossAfterPurge(INSERT_REBUILD_ONE, false, "count\tsum\n6\t21\n");
        });
    }

    // The purge log table exists before the durable baseline, as on a server that ran its purge job before.
    private void createPurgeLogTable() throws Exception {
        try (PostingSealPurgeJob job = new PostingSealPurgeJob(engine)) {
            Assert.assertEquals(0, job.getOutstandingPurgeTasks());
        }
    }

    private void createTable(boolean isCovering, boolean isWal) throws Exception {
        execute("CREATE TABLE t (ts TIMESTAMP, k SYMBOL INDEX TYPE POSTING" + (isCovering ? " INCLUDE (v)" : "")
                + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY " + (isWal ? "WAL" : "BYPASS WAL"));
    }

    // Marks the setup durable, runs the operation and the purge, cuts the power, then checks that the index finds the
    // rows a scan finds, before and after the writer opens.
    private void powerLossAfterPurge(String operation, boolean isTxnNotWrittenBack, String expectedCountSum) throws Exception {
        final TableToken token = engine.verifyTableName("t");
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        markDurableBaseline();
        execute(operation);
        runPurgeJob();
        assertOneValueFilePerPartition(token);
        final String tableDir = tableDir(token);
        fsyncTableDirs(tableDir);
        try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(Paths.get(tableDir))) {
            paths.filter(java.nio.file.Files::isRegularFile).forEach(p -> {
                final String name = p.getFileName().toString();
                final boolean isWrittenBack = isTxnNotWrittenBack
                        ? !name.equals("_txn")
                        : !INDEX_FILE.matcher(name).matches() || !p.getParent().getFileName().toString().startsWith("2024-01-01");
                if (isWrittenBack) {
                    crashFf.markFileDurable(p.toString());
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        crashAndReopen();
        // A restarted process has a fresh scoreboard; the pooled one keeps the max txn of before the power loss.
        engine.getTxnScoreboardPool().remove(token);
        assertIndexMatchesScan(expectedCountSum);
        try (TableWriter writer = getWriter(token)) {
            Assert.assertNotNull(writer);
        }
        assertIndexMatchesScan(expectedCountSum);
    }
}
