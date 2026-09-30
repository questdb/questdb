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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

/**
 * Every commit mode that promises durability keeps its promise with only the directory barriers it takes.
 * <p>
 * The crash model is strict POSIX ({@link CrashFaultFilesFacade}): a directory entry survives a power loss
 * only if its parent directory was fsynced after the entry appeared, or a {@code syncfs} ran. Nothing is
 * written back by the kernel unless a test says so.
 * <ul>
 *   <li>SYNC fsyncs a partition directory after its column files exist, and the table directory after it,
 *   before the commit that publishes the partition. A barrier issued before the files exist persists an
 *   empty directory.</li>
 *   <li>ADAPTIVE takes no directory barrier in {@code TableWriter.openPartition}: the durable epoch makes
 *   directory entries durable, and recovery re-creates the later ones when it replays the WAL. That holds
 *   also when {@code syncfs} is not filesystem-wide (macOS, Windows), where the epoch fsyncs the dirty
 *   partition directories itself.</li>
 *   <li>The WAL writer, under SYNC and ADAPTIVE, fsyncs each new segment's entry in {@code wal<N>} and, once
 *   per writer, the table directory that holds {@code wal<N>}.</li>
 * </ul>
 */
public class DirectoryBarrierCrashTest extends AbstractAdaptiveCrashTest {

    @Test
    public void testAdaptiveEpochCoversPartitionDirs() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(true, new CrashFaultFilesFacade());
    }

    @Test
    public void testAdaptiveEpochCoversPartitionDirsWithoutFsWideSyncfs() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(true, new SingleFileSyncfsFacade());
    }

    @Test
    public void testAdaptiveLazyPartitionsSurviveCrash() throws Exception {
        assertAdaptiveWalPartitionsSurviveCrash(false, new CrashFaultFilesFacade());
    }

    @Test
    public void testAdaptiveWalSegmentsSurviveCrash() throws Exception {
        assertWalSegmentsSurviveCrash("adaptive");
    }

    @Test
    public void testSyncMultiPartitionCommitSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            markDurableBaseline();
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-02T00:00:00.000000Z', 2),
                        ('2024-01-03T00:00:00.000000Z', 3),
                        ('2024-01-04T00:00:00.000000Z', 4)
                    """);
            crashAndReopen();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    2024-01-03T00:00:00.000000Z\t3
                    2024-01-04T00:00:00.000000Z\t4
                    """);
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncO3IntoLastPartitionSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO x VALUES
                        ('2024-01-01T00:00:00.000000Z', 1),
                        ('2024-01-02T00:00:00.000000Z', 2),
                        ('2024-01-02T02:00:00.000000Z', 3)
                    """);
            markDurableBaseline();
            // O3 into the last partition writes a new version of it, which the writer then reopens.
            execute("INSERT INTO x VALUES ('2024-01-02T01:00:00.000000Z', 4)");
            crashAndReopen();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    2024-01-02T01:00:00.000000Z\t4
                    2024-01-02T02:00:00.000000Z\t3
                    """);
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncPartitionSwitchSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            markDurableBaseline();
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            crashAndReopen();
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    """);
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncWalApplyNewPartitionSurvivesCrash() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();
            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    """);
            releaseEngineHandles();
        });
    }

    @Test
    public void testSyncWalSegmentsSurviveCrash() throws Exception {
        assertWalSegmentsSurviveCrash("sync");
    }

    private static void appendRow(WalWriter writer, long ts, long v) {
        final TableWriter.Row row = writer.newRow(ts);
        row.putLong(1, v);
        row.append();
    }

    /**
     * An ADAPTIVE WAL table gets two new partitions after the baseline. The kernel wrote {@code _txn} and
     * {@code _cv} back before the power loss, so they name both partitions, while no directory barrier made
     * either partition's entries durable. Recovery must rewind the pointers to the durable epoch and replay
     * the WAL.
     *
     * @param isEpochAfterFirstPartition when true, a durable epoch covers the first new partition; the second
     *                                   is applied lazily in both variants
     */
    private void assertAdaptiveWalPartitionsSurviveCrash(
            boolean isEpochAfterFirstPartition,
            CrashFaultFilesFacade facade
    ) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, isEpochAfterFirstPartition ? 0 : -1);
        runWithFacade(facade, () -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO x VALUES ('2024-01-01T00:00:00.000000Z', 1)");
            drainWalQueue();
            final TableToken token = engine.verifyTableName("x");
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            markDurableBaseline();

            execute("INSERT INTO x VALUES ('2024-01-02T00:00:00.000000Z', 2)");
            drainWalQueue();
            if (isEpochAfterFirstPartition) {
                Assert.assertEquals("an epoch must cover the first new partition", 2, tracker.getDurableEpochSeqTxn());
            }
            node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            execute("INSERT INTO x VALUES ('2024-01-03T00:00:00.000000Z', 3)");
            drainWalQueue();
            Assert.assertTrue("the second new partition must be applied lazily", tracker.getDurableEpochSeqTxn() < 3);

            final String tableDir = root + File.separator + token.getDirName();
            crashFf.markFileDurable(tableDir + File.separator + TableUtils.TXN_FILE_NAME);
            crashFf.markFileDurable(tableDir + File.separator + TableUtils.COLUMN_VERSION_FILE_NAME);

            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            assertRows("""
                    ts\tv
                    2024-01-01T00:00:00.000000Z\t1
                    2024-01-02T00:00:00.000000Z\t2
                    2024-01-03T00:00:00.000000Z\t3
                    """);
            releaseEngineHandles();
        });
    }

    private void assertNotSuspended(TableToken token) {
        Assert.assertFalse(
                "table must not be suspended: " + engine.getTableSequencerAPI().getTxnTracker(token).getErrorMessage(),
                engine.getTableSequencerAPI().isSuspended(token)
        );
    }

    private void assertRows(String expected) throws Exception {
        assertQuery("SELECT ts, v FROM x").timestamp("ts").expectSize().returns(expected);
    }

    /**
     * A WAL writer created after the baseline writes three commits into three segments, and the power fails
     * before any of them is applied. Every commit was acknowledged (ADAPTIVE) or returned (SYNC), so all of
     * them must survive: {@code wal1} in the table directory, and each segment in {@code wal1}, have to be
     * durable by the time a commit that names them is sequenced.
     */
    private void assertWalSegmentsSurviveCrash(String commitMode) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, commitMode);
        node1.setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, 0);
        node1.setProperty(PropertyKey.CAIRO_WAL_COMMIT_WRITEBACK_DRAIN, false);
        node1.setProperty(PropertyKey.CAIRO_WAL_SEGMENT_ROLLOVER_ROW_COUNT, 3);
        runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false;
            execute("CREATE TABLE x (ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            final TableToken token = engine.verifyTableName("x");
            markDurableBaseline();
            try (WalWriter writer = getWalWriter("x")) {
                long v = 0;
                for (int commit = 0; commit < 3; commit++) {
                    for (int row = 0; row < 3; row++) {
                        appendRow(writer, v * 1_000_000L, v);
                        v++;
                    }
                    writer.commit();
                }
                Assert.assertEquals("the commits must span three segments", 2, writer.getSegmentId());
            }
            Assert.assertEquals(3, engine.getTableSequencerAPI().getTxnTracker(token).getSeqTxn());

            recoverAfterCrash(new TableToken[]{token});
            assertNotSuspended(token);
            assertQuery("SELECT count(), sum(v) FROM x").noRandomAccess().expectSize().returns("""
                    count\tsum
                    9\t36
                    """);
            releaseEngineHandles();
        });
    }

    private void runWithFacade(CrashFaultFilesFacade facade, TestUtils.LeakProneCode body) throws Exception {
        assumeCrashHarnessSupported();
        crashFf = facade;
        crashFf.setDbRoot(root);
        assertMemoryLeak(crashFf, body);
    }

    /**
     * {@code syncfs(fd)} as macOS and Windows have it: a flush of that one file, not of the filesystem. The
     * durable epoch then takes its fallback path, which fsyncs every file and directory it has to publish.
     */
    private static class SingleFileSyncfsFacade extends CrashFaultFilesFacade {
        @Override
        public boolean isSyncfsFileSystemWide() {
            return false;
        }

        @Override
        public void syncfs(long fd) {
            fsync(fd);
        }
    }
}
