/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;
import io.questdb.test.cairo.crash.AbstractAdaptiveCrashTest;
import io.questdb.test.cairo.crash.FsyncFailureFacade;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 * Durability of the parquet partition reindex that repairs stale {@code _cv} column tops: the one an
 * {@code ADD INDEX} runs in its transaction, and online {@code REINDEX TABLE}, which commits outside the WAL so no
 * WAL replay redoes it.
 * <ul>
 *     <li>Under ADAPTIVE, online REINDEX must end with a save point: otherwise a power loss rewinds the table to
 *     the save point before it, and the stale tops come back.</li>
 *     <li>Under SYNC, the new partition directory must survive a power loss right after the statement returns, and
 *     a failed barrier around it poisons the engine, as every failed barrier does.</li>
 *     <li>A reindex that fails partway through several partitions keeps the partitions it committed, and leaves
 *     no directory version behind that the writer does not reclaim.</li>
 * </ul>
 */
public class StaleParquetReindexCrashTest extends AbstractAdaptiveCrashTest {
    private static final String INDEX_QUERY = "select ts, x, v, s from bitmap_top where s = 'a' and ts in '2024-01-01T05'";
    private static final String ROW_A = "ts\tx\tv\ts\n2024-01-01T05:05:00.000000Z\t-1\t42\ta\n";
    private static final String STALE_DIR = "2024-01-01.4";

    @Test
    public void testOnlineReindexSurvivesPowerLoss() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Only the save points the test asks for
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1_000_000_000L);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        // A crash takes no save point at writer close
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        runWithCrashFacade(() -> {
            StaleParquetColumnTopTest.loadFixture("bitmap_top~10");
            final TableToken token = engine.verifyTableName("bitmap_top");
            // The first writer open enrols the table and takes the save point a recovery could rewind to
            try (TableWriter writer = getWriter(token)) {
                writer.advanceDurableEpoch(engine.getConfiguration().getMicrosecondClock().getTicks() / 1000L);
            }
            markDurableBaseline();
            assertExceptionNoLeakCheck(INDEX_QUERY, -1, "index does not cover parquet partition data");

            execute("reindex table bitmap_top");
            assertQuery(INDEX_QUERY).timestamp("ts").returns(ROW_A);

            recoverAfterCrash(new TableToken[]{token});
            Assert.assertFalse("recovery suspended the table", anyTableSuspended(token));
            assertQuery(INDEX_QUERY).timestamp("ts").returns(ROW_A);
        });
    }

    /**
     * Two parquet partitions; the second one's commit fails on its {@code _txn} msync under SYNC. The writer is
     * distressed, so the first partition's replaced directory gets no housekeeping from the statement: the next
     * writer open must reclaim it, together with the second partition's version the failed commit still published
     * to the page cache.
     */
    @Test
    public void testPartialReindexFailedCommitLeavesNoOrphanDirs() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        final FsyncFailureFacade ff = new FsyncFailureFacade("commitTxWriter", 1, true);
        assertMemoryLeak(ff, () -> {
            createTwoParquetPartitions();
            final File tableDir = new File(root, engine.verifyTableName("x").getDirName());
            final String[] before = listPartitionDirs(tableDir);
            Assert.assertEquals(2, before.length);

            ff.isArmed = true;
            Throwable failure = null;
            try {
                execute("reindex table x");
            } catch (Throwable th) {
                failure = th;
            } finally {
                ff.isArmed = false;
            }
            Assert.assertEquals("the second partition's commit must fail", 1, ff.failureCount);
            Assert.assertTrue(
                    "a failed _txn msync must fail the statement with a data-sync CairoError, failure: " + failure,
                    failure instanceof CairoError && CairoException.isDataSyncFailure(failure)
            );
            Assert.assertTrue("a failed data sync must poison the engine", engine.isDurabilityFailed());
            // Both partitions have a new version next to the old one: the distressed writer removed neither
            Assert.assertEquals(4, listPartitionDirs(tableDir).length);

            engine.resetDurabilityFailure();
            releaseEngineHandles();
            // The next open reclaims the versions _txn does not name
            try (TableWriter ignore = getWriter("x")) {
                final String[] after = listPartitionDirs(tableDir);
                Assert.assertEquals("one version per partition: " + String.join(",", after), 2, after.length);
                Assert.assertNotEquals(before[0], after[0]);
                Assert.assertNotEquals(before[1], after[1]);
            }
            assertTwoPartitionRows();
            releaseEngineHandles();
        });
    }

    /**
     * Two parquet partitions; the second one's hard link fails with a plain error. The first partition stays
     * committed at its new version, with its save point and its replaced directory removed; the second keeps its
     * old version, and the directory the failed attempt created is gone. The writer stays usable.
     */
    @Test
    public void testPartialReindexFailureKeepsCommittedPartitions() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        final HardLinkFailureFacade ff = new HardLinkFailureFacade();
        assertMemoryLeak(ff, () -> {
            createTwoParquetPartitions();
            final TableToken token = engine.verifyTableName("x");
            final File tableDir = new File(root, token.getDirName());
            final String[] before = listPartitionDirs(tableDir);
            Assert.assertEquals(2, before.length);
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(token);
            final long pinnedTxnBefore = tracker.getPinnedEpochTxn();

            // the first partition takes two links, the third is the second partition's
            ff.failAtCall = 3;
            try {
                assertExceptionNoLeakCheck("reindex table x", -1, "injected hard link failure");
            } finally {
                ff.failAtCall = -1;
            }
            Assert.assertFalse(engine.isDurabilityFailed());
            Assert.assertEquals("the committed partition must get its save point", pinnedTxnBefore + 1, tracker.getPinnedEpochTxn());
            releaseSavePointHolds("x");

            final String[] after = listPartitionDirs(tableDir);
            Assert.assertEquals("one version per partition: " + String.join(",", after), 2, after.length);
            Assert.assertNotEquals("the first partition must be at its new version", before[0], after[0]);
            Assert.assertEquals("the second partition must keep its version", before[1], after[1]);
            assertTwoPartitionRows();

            // the writer is not distressed
            execute("insert into x values ('2024-01-03T00:00:00.000000Z', 'a', 3.0)");
            drainWalQueue();
            assertQuery("select count() from x where s = 'a'").noRandomAccess().expectSize().returns("count\n3\n");
        });
    }

    @Test
    public void testSyncAddIndexRepairFailedDirFsyncIsFailStop() throws Exception {
        assertSyncReindexFsyncFailureIsFailStop(
                () -> {
                    execute("alter table bitmap_top alter column s add index");
                    drainWalQueue();
                },
                new FsyncFailureFacade("fsyncConvertedPartitionDir", 0, false),
                false
        );
    }

    /**
     * ADD INDEX repairs the stale partition in its transaction. Under SYNC the repaired partition's directory must
     * survive a power loss right after the apply, and the durable {@code _txn} must name it.
     * <p>
     * The index files are not checked: ADD INDEX writes them into every partition directory after the repair's
     * directory barrier, and flushes neither them nor their directory entries under SYNC, the same as before the
     * repair existed. The strict crash model loses them, a real kernel's writeback usually keeps them.
     */
    @Test
    public void testSyncAddIndexRepairSurvivesPowerLoss() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            StaleParquetColumnTopTest.loadFixture("bitmap_top~10");
            final TableToken token = engine.verifyTableName("bitmap_top");
            final File tableDir = new File(root, token.getDirName());
            execute("alter table bitmap_top alter column s drop index");
            drainWalQueue();
            markDurableBaseline();

            execute("alter table bitmap_top alter column s add index");
            drainWalQueue();
            Assert.assertFalse("the apply suspended the table", anyTableSuspended(token));
            assertQuery(INDEX_QUERY).timestamp("ts").returns(ROW_A);

            recoverAfterCrash(new TableToken[]{token});
            Assert.assertFalse("recovery suspended the table", anyTableSuspended(token));
            final String repairedDir = assertRepairedDirSurvived(tableDir, false);
            try (TableReader reader = engine.getReader(token)) {
                final long nameTxn = reader.getPartitionNameTxnByIndex(0);
                Assert.assertEquals("_txn must name the repaired directory", "2024-01-01." + nameTxn, repairedDir);
            }
            assertQuery("select ts, x, v, s from bitmap_top where ts in '2024-01-01T05:05'").timestamp("ts").returns(ROW_A);
        });
    }

    @Test
    public void testSyncOnlineReindexFailedDirFsyncIsFailStop() throws Exception {
        assertSyncReindexFsyncFailureIsFailStop(
                () -> execute("reindex table bitmap_top"),
                new FsyncFailureFacade("fsyncConvertedPartitionDir", 0, false),
                true
        );
    }

    @Test
    public void testSyncOnlineReindexFailedTxnMsyncIsFailStop() throws Exception {
        assertSyncReindexFsyncFailureIsFailStop(
                () -> execute("reindex table bitmap_top"),
                new FsyncFailureFacade("commitTxWriter", 0, true),
                true
        );
    }

    /**
     * Online REINDEX returns under SYNC, then the power fails. The new partition directory and the index files
     * in it must survive: the durable {@code _txn} names them.
     */
    @Test
    public void testSyncOnlineReindexSurvivesPowerLoss() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        runWithCrashFacade(() -> {
            StaleParquetColumnTopTest.loadFixture("bitmap_top~10");
            final TableToken token = engine.verifyTableName("bitmap_top");
            final File tableDir = new File(root, token.getDirName());
            markDurableBaseline();
            assertExceptionNoLeakCheck(INDEX_QUERY, -1, "index does not cover parquet partition data");

            execute("reindex table bitmap_top");
            assertQuery(INDEX_QUERY).timestamp("ts").returns(ROW_A);

            recoverAfterCrash(new TableToken[]{token});
            Assert.assertFalse("recovery suspended the table", anyTableSuspended(token));
            assertRepairedDirSurvived(tableDir, true);
            assertQuery(INDEX_QUERY).timestamp("ts").returns(ROW_A);
        });
    }

    /**
     * The repaired version of the stale partition has survived the crash next to or instead of the stale one, with
     * its parquet files, and with the key and value files of the index when {@code withIndexFiles}.
     *
     * @return the name of the repaired directory
     */
    private static String assertRepairedDirSurvived(File tableDir, boolean withIndexFiles) {
        final String[] day1Dirs = tableDir.list((dir, name) -> name.startsWith("2024-01-01"));
        Assert.assertNotNull(day1Dirs);
        String repairedDir = null;
        for (String dir : day1Dirs) {
            if (!STALE_DIR.equals(dir)) {
                Assert.assertNull("a single repaired version is expected: " + String.join(",", day1Dirs), repairedDir);
                repairedDir = dir;
            }
        }
        Assert.assertNotNull("the repaired partition directory must survive: " + String.join(",", day1Dirs), repairedDir);
        final String[] files = new File(tableDir, repairedDir).list();
        Assert.assertNotNull(files);
        Arrays.sort(files);
        final String names = Arrays.toString(files);
        Assert.assertTrue(names, names.contains("_pm") && names.contains("data.parquet"));
        if (withIndexFiles) {
            int indexFileCount = 0;
            for (String file : files) {
                if (file.startsWith("s.")) {
                    indexFileCount++;
                }
            }
            Assert.assertEquals("the key and value files of the index must survive: " + names, 2, indexFileCount);
        }
        return repairedDir;
    }

    private static String[] listPartitionDirs(File tableDir) {
        final String[] dirs = tableDir.list((dir, name) -> name.startsWith("2024-01-0"));
        Assert.assertNotNull(dirs);
        Arrays.sort(dirs);
        return dirs;
    }

    /**
     * Under SYNC, one sync of the stale partition's repair fails: the engine must be poisoned and refuse the next
     * write. {@code isStatement} says the failing change is a statement that runs on the writer and so must fail
     * with a data-sync {@link CairoError}, as opposed to a WAL apply.
     */
    private void assertSyncReindexFsyncFailureIsFailStop(TestUtils.LeakProneCode change, FsyncFailureFacade ff, boolean isStatement) throws Exception {
        Assume.assumeFalse("a directory takes no fsync on Windows", Os.isWindows());
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        assertMemoryLeak(ff, () -> {
            StaleParquetColumnTopTest.loadFixture("bitmap_top~10");
            if (!isStatement) {
                execute("alter table bitmap_top alter column s drop index");
                drainWalQueue();
            }
            Throwable failure = null;
            ff.isArmed = true;
            try {
                change.run();
            } catch (Throwable th) {
                failure = th;
            } finally {
                ff.isArmed = false;
            }
            final boolean isPoisoned = engine.isDurabilityFailed();
            Throwable nextWriteFailure = null;
            try {
                execute("insert into bitmap_top (ts, x) values ('2024-01-03T00:00:00.000000Z', 1)");
            } catch (Throwable th) {
                nextWriteFailure = th;
            }
            engine.resetDurabilityFailure();
            releaseEngineHandles();
            Assert.assertEquals("the injected failure must have been reached", 1, ff.failureCount);
            Assert.assertTrue("a failed data sync must poison the engine, failure: " + failure, isPoisoned);
            Assert.assertTrue(
                    "a poisoned engine must refuse the next write, failure: " + nextWriteFailure,
                    nextWriteFailure instanceof CairoError
                            && nextWriteFailure.getMessage().contains("engine is poisoned by a failed durability barrier")
            );
            if (isStatement) {
                Assert.assertTrue(
                        "the statement must fail with a data-sync CairoError, failure: " + failure,
                        failure instanceof CairoError && CairoException.isDataSyncFailure(failure)
                );
            }
        });
    }

    private void assertTwoPartitionRows() throws Exception {
        assertQuery("select ts, s, v from x where s = 'a'").timestamp("ts").returns("""
                ts\ts\tv
                2024-01-01T00:00:00.000000Z\ta\t1.0
                2024-01-02T00:00:00.000000Z\ta\t2.0
                """);
    }

    private void createTwoParquetPartitions() throws Exception {
        execute("create table x (ts timestamp, s symbol index, v double) timestamp(ts) partition by day wal");
        execute("""
                insert into x values
                    ('2024-01-01T00:00:00.000000Z', 'a', 1.0),
                    ('2024-01-01T01:00:00.000000Z', 'b', 1.5),
                    ('2024-01-02T00:00:00.000000Z', 'a', 2.0),
                    ('2024-01-02T01:00:00.000000Z', 'b', 2.5)
                """);
        drainWalQueue();
        execute("alter table x convert partition to parquet list '2024-01-01', '2024-01-02'");
        drainWalQueue();
        releaseSavePointHolds("x");
    }

    /**
     * Fails the n-th hard link a parquet reindex takes with a plain, non-durability error.
     */
    private static final class HardLinkFailureFacade extends TestFilesFacadeImpl {
        int failAtCall = -1;
        private int callCount;

        @Override
        public int hardLink(LPSZ src, LPSZ hardLink) {
            if (failAtCall > 0 && FsyncFailureFacade.isCalledFrom("prepareParquetPartitionReindex") && ++callCount == failAtCall) {
                throw CairoException.critical(5).put("injected hard link failure");
            }
            return super.hardLink(src, hardLink);
        }
    }
}
