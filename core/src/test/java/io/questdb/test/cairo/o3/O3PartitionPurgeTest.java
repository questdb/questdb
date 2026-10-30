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

package io.questdb.test.cairo.o3;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.TxReader;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.mp.MPSequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cairo.Overrides;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

@RunWith(Parameterized.class)
public class O3PartitionPurgeTest extends AbstractCairoTest {
    private static O3PartitionPurgeJob purgeJob;
    private final TestTimestampType timestampType;

    public O3PartitionPurgeTest(TestTimestampType timestampType) {
        this.timestampType = timestampType;
    }

    @AfterClass
    public static void end() {
        purgeJob = Misc.free(purgeJob);
    }

    @BeforeClass
    public static void setUpStatic() throws Exception {
        AbstractCairoTest.setUpStatic();
        purgeJob = new O3PartitionPurgeJob(engine, 1);
    }

    @Parameterized.Parameters(name = "timestamp={0}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO},
                {TestTimestampType.NANO},
        });
    }

    @Test
    public void test2ReadersUsePartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            execute("insert into tbl select 4, '1970-01-10T09'");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader("tbl")) {

                try (TableReader rdr2 = getReader("tbl")) {
                    // in order insert
                    execute("insert into tbl select 2, '1970-01-10T11'");

                    // OOO insert
                    execute("insert into tbl select 4, '1970-01-10T09'");

                    runPartitionPurgeJobs();

                    rdr2.openPartition(0);
                }

                runPartitionPurgeJobs();

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getDbRoot()).concat("tbl").concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                }
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testAdaptiveDropLeavesPurgeToSavePoint() throws Exception {
        // While only the save points hold the dropped versions, no purge task can delete them before the next save
        // point, so the DROP queues none and the save points after it queue the purge. A task per dropped
        // partition would fill the purge queue (128 in production) and log an error for every partition past it.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // No save point inside the DROP's apply batch: the test takes them.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_O3_PURGE_DISCOVERY_QUEUE_CAPACITY, 128);
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            // The message bus sizes the purge queue when it is built, so the production capacity needs its own engine.
            engine.closeNameRegistry();
            final LogCapture capture = new LogCapture();
            try (
                    CairoEngine e = new CairoEngine(configuration);
                    SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(e);
                    O3PartitionPurgeJob job = new O3PartitionPurgeJob(e, 1)
            ) {
                capture.start();
                e.execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL", ctx);
                e.execute("INSERT INTO t SELECT timestamp_sequence('2020-01-01', 86_400_000_000L)::" + timestampType.getTypeName() + ", x FROM long_sequence(1_000)", ctx);
                TestUtils.drainWalQueue(e);
                final TableToken tt = e.verifyTableName("t");
                final long nowMs = configuration.getMicrosecondClock().getTicks() / 1000;
                // Two save points that both name the partitions the DROP removes.
                try (TableWriter w = e.getWriter(tt, "test")) {
                    w.advanceDurableEpoch(nowMs);
                    w.advanceDurableEpoch(nowMs + 1);
                }
                final ObjList<String> dropped = partitionDirsBefore(tt, "2021-05-15");
                Assert.assertEquals(500, dropped.size());

                final MPSequence purgePubSeq = e.getMessageBus().getO3PurgeDiscoveryPubSeq();
                final long queuedBefore = purgePubSeq.current();
                e.execute("ALTER TABLE t DROP PARTITION WHERE ts < '2021-05-15'", ctx);
                TestUtils.drainWalQueue(e);
                Assert.assertEquals("purge tasks the DROP queued", 0, purgePubSeq.current() - queuedBefore);
                capture.drain();
                capture.assertNotLogged("could not queue for purge, queue is full");
                final StringSink failures = new StringSink();
                checkOnDisk(failures, "after the DROP", tt, dropped, true);

                try (TableWriter w = e.getWriter(tt, "test")) {
                    w.advanceDurableEpoch(nowMs + 2);
                }
                job.drain(0);
                checkOnDisk(failures, "the older save point names them", tt, dropped, true);

                try (TableWriter w = e.getWriter(tt, "test")) {
                    w.advanceDurableEpoch(nowMs + 3);
                }
                job.drain(0);
                checkOnDisk(failures, "no save point names them", tt, dropped, false);
                Assert.assertEquals("", failures.toString());
                assertQuery("SELECT count() FROM t").withEngine(e).withContext(ctx).noLeakCheck().expectSize().noRandomAccess().returns("count\n500\n");
            } finally {
                capture.stop();
                engine.reloadTableNames();
            }
        });
    }

    @Test
    public void testAdaptiveDropReadsEachSavePointOncePerPurgeTask() throws Exception {
        // The save point after a DROP of K partitions queues a purge task, which checks all K dropped versions against
        // the save points. A task reads each save point's _txn copy at most once, so it opens at most two files per
        // generation (header, then section), whatever K is.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        final AtomicInteger manifestOpens = new AtomicInteger();
        final FilesFacade countingFf = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (Utf8s.containsAscii(name, DurableEpochManifest.FILE_NAME)) {
                    manifestOpens.incrementAndGet();
                }
                return super.openRO(name);
            }
        };
        assertMemoryLeak(countingFf, () -> {
            execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2020-01-01', 86_400_000_000L)::" + timestampType.getTypeName() + ", x FROM long_sequence(300)");
            drainWalQueue();
            // A second save point that names the same partitions.
            execute("INSERT INTO t VALUES ('2020-10-26T01:00:00.000000Z', 0)");
            drainWalQueue();
            purgeJob.drain(0);
            final TableToken tt = engine.verifyTableName("t");
            final ObjList<String> dropped = new ObjList<>();
            try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.list(java.nio.file.Path.of(configuration.getDbRoot(), tt.getDirName()))) {
                s.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("2020-") && n.compareTo("2020-04-10") < 0).forEach(dropped::add);
            }
            Assert.assertEquals(100, dropped.size());

            execute("ALTER TABLE t DROP PARTITION WHERE ts < '2020-04-10'");
            drainWalQueue();
            final StringSink failures = new StringSink();
            int tasks = 0;
            int maxOpens = 0;
            manifestOpens.set(0);
            while (purgeJob.run()) {
                tasks++;
                maxOpens = Math.max(maxOpens, manifestOpens.getAndSet(0));
            }
            // Only the save point of the DROP's apply batch queues one: the DROP leaves the purge to it.
            Assert.assertEquals("purge tasks after the DROP", 1, tasks);
            Assert.assertTrue("no task read a save point", maxOpens > 0);
            Assert.assertTrue("manifest opens in one task: " + maxOpens, maxOpens <= 4);
            checkOnDisk(failures, "the older save point names them", tt, dropped, true);

            // Neither save point on disk names the dropped versions now.
            execute("INSERT INTO t VALUES ('2020-10-26T02:00:00.000000Z', 0)");
            drainWalQueue();
            maxOpens = 0;
            manifestOpens.set(0);
            while (purgeJob.run()) {
                maxOpens = Math.max(maxOpens, manifestOpens.getAndSet(0));
            }
            Assert.assertTrue("no task read a save point", maxOpens > 0);
            Assert.assertTrue("manifest opens in one task: " + maxOpens, maxOpens <= 4);
            checkOnDisk(failures, "no save point names them", tt, dropped, false);
            Assert.assertEquals("", failures.toString());
            assertQuery("SELECT count() FROM t").noLeakCheck().expectSize().noRandomAccess().returns("count\n202\n");
        });
    }

    @Test
    public void testAdaptiveDropWithReaderQueuesPurgeTaskPerPartition() throws Exception {
        // A reader that holds an older txn can let go at any time, so a DROP under a reader queues a purge task per
        // partition even while a save point also holds the dropped versions.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // No save point inside the DROP's apply batch: the test takes it.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 86_400_000_000L)::" + timestampType.getTypeName() + ", x FROM long_sequence(5)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            try (TableWriter w = getWriter(tt)) {
                w.advanceDurableEpoch(configuration.getMicrosecondClock().getTicks() / 1000);
            }
            final MPSequence purgePubSeq = engine.getMessageBus().getO3PurgeDiscoveryPubSeq();
            try (TableReader ignore = getReader(tt)) {
                final long queuedBefore = purgePubSeq.current();
                execute("ALTER TABLE t DROP PARTITION WHERE ts < '2024-01-04'");
                drainWalQueue();
                Assert.assertEquals("purge tasks the DROP queued", 3, purgePubSeq.current() - queuedBefore);
            }
            engine.releaseAllReaders();
            purgeJob.drain(0);
            assertQuery("SELECT count() FROM t").expectSize().noRandomAccess().returns("count\n2\n");
        });
    }

    @Test
    public void testAdaptiveLeftVersionGoesAfterNextSavePoint() throws Exception {
        // A partition version that leaves an adaptive table, by DROP PARTITION, a TTL eviction or an O3 rewrite,
        // stays while the older save point names it, because recovery can fall back to that save point. It goes
        // by itself once the next save point no longer names it, with the writer kept open.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        // The O3 change below must leave a partition version behind, which only a whole-partition rewrite does;
        // merge-append (which the test harness turns on) keeps the version and appends a piece. Pin the production
        // default.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            final String[] changes = {
                    "ALTER TABLE %s DROP PARTITION LIST '2024-01-01'",
                    "ALTER TABLE %s DROP PARTITION WHERE ts < '2024-01-02'",
                    "INSERT INTO %s VALUES ('2024-01-04T12:00:00.000000Z', 4)",
                    "INSERT INTO %s VALUES ('2024-01-01T01:00:00.000000Z', 4)"
            };
            final StringSink failures = new StringSink();
            for (int i = 0; i < changes.length; i++) {
                final String change = String.format(changes[i], "t" + i);
                final TableToken tt = createAdaptiveDays("t" + i, i == 2);
                final ObjList<String> left = versionDirs(tt);
                execute(change);
                drainWalQueue();
                purgeJob.drain(0);
                checkOnDisk(failures, change + ", the older save point names it", tt, left, true);
                execute("INSERT INTO t" + i + " VALUES ('2024-01-04T13:00:00.000000Z', 5)");
                drainWalQueue();
                purgeJob.drain(0);
                checkOnDisk(failures, change + ", no save point names it", tt, left, false);
            }
            Assert.assertEquals("", failures.toString());
        });
    }

    @Test
    public void testAdaptiveLeftVersionGoesAfterRestart() throws Exception {
        // A kill right after the change leaves the version on disk while the older save point names it. Recovery
        // pins both save points, so it stays through the restart and VACUUM TABLE, and goes after the restarted
        // writer's next save point.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        // As in testAdaptiveLeftVersionGoesAfterNextSavePoint: the O3 change must leave a partition version behind.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            final String[] changes = {
                    "ALTER TABLE %s DROP PARTITION LIST '2024-01-01'",
                    "INSERT INTO %s VALUES ('2024-01-04T12:00:00.000000Z', 4)",
                    "INSERT INTO %s VALUES ('2024-01-01T01:00:00.000000Z', 4)"
            };
            final StringSink failures = new StringSink();
            for (int i = 0; i < changes.length; i++) {
                final String change = String.format(changes[i], "t" + i);
                final TableToken tt = createAdaptiveDays("t" + i, i == 1);
                final ObjList<String> left = versionDirs(tt);
                execute(change);
                drainWalQueue();
                engine.releaseAllWriters();
                engine.releaseAllReaders();
                engine.getTxnScoreboardPool().remove(tt);
                engine.getTableSequencerAPI().resetForReboot(tt);
                new RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();
                execute("VACUUM TABLE t" + i);
                purgeJob.drain(0);
                checkOnDisk(failures, change + ", the older save point names it", tt, left, true);
                execute("INSERT INTO t" + i + " VALUES ('2024-01-04T13:00:00.000000Z', 5)");
                drainWalQueue();
                purgeJob.drain(0);
                checkOnDisk(failures, change + ", no save point names it", tt, left, false);
            }
            Assert.assertEquals("", failures.toString());
        });
    }

    @Test
    public void testAdaptiveO3RewriteQueuesPurgeTaskUnderSavePoint() throws Exception {
        // Only a DROP leaves the purge to the next save point. An O3 rewrite still queues a purge task at its commit
        // while a save point holds the old version, so versions that repeated rewrites leave do not pile up on disk
        // until the next save point.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // No save point inside the rewrite's apply batch: the test takes it.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        // As in testAdaptiveLeftVersionGoesAfterNextSavePoint: the O3 insert must rewrite the partition.
        setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            final TableToken tt = createAdaptiveDays("t", false);
            try (TableWriter w = getWriter(tt)) {
                w.advanceDurableEpoch(configuration.getMicrosecondClock().getTicks() / 1000);
            }
            final ObjList<String> left = versionDirs(tt);
            final MPSequence purgePubSeq = engine.getMessageBus().getO3PurgeDiscoveryPubSeq();
            final long queuedBefore = purgePubSeq.current();
            execute("INSERT INTO t VALUES ('2024-01-01T01:00:00.000000Z', 4)");
            drainWalQueue();
            Assert.assertEquals("purge tasks the O3 rewrite queued", 1, purgePubSeq.current() - queuedBefore);
            final StringSink failures = new StringSink();
            checkOnDisk(failures, "the save point names it", tt, left, true);
            purgeJob.drain(0);
            checkOnDisk(failures, "the purge task ran, the save point names it", tt, left, true);
            Assert.assertEquals("", failures.toString());
            assertQuery("SELECT count() FROM t").expectSize().noRandomAccess().returns("count\n6\n");
        });
    }

    @Test
    public void testAdaptiveVacuumRemovesDroppedPartitionAfterSavePoints() throws Exception {
        // A reader keeps the dropped partition through the save points that stop naming it; once it is closed,
        // VACUUM TABLE removes the partition.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        assertMemoryLeak(() -> {
            final StringSink failures = new StringSink();
            final TableToken tt = createAdaptiveDays("t", false);
            final ObjList<String> left = versionDirs(tt);
            try (TableReader ignore = getReader("t")) {
                execute("ALTER TABLE t DROP PARTITION LIST '2024-01-01'");
                drainWalQueue();
                for (int i = 0; i < 2; i++) {
                    execute("INSERT INTO t VALUES ('2024-01-04T1" + i + ":00:00.000000Z', 5)");
                    drainWalQueue();
                    purgeJob.drain(0);
                }
                checkOnDisk(failures, "a reader holds it", tt, left, true);
            }
            engine.releaseAllReaders();
            execute("VACUUM TABLE t");
            purgeJob.drain(0);
            checkOnDisk(failures, "after VACUUM TABLE", tt, left, false);
            Assert.assertEquals("", failures.toString());
        });
    }

    @Test
    public void testAsyncPurgeOnBusyWriter() throws Exception {
        int tableCount = 3;
        assertMemoryLeak(() -> {
            for (int i = 0; i < tableCount; i++) {
                execute("create table tbl" + i + " as (select x, cast('1970-01-01T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            }

            final CyclicBarrier barrier = new CyclicBarrier(3);
            AtomicInteger done = new AtomicInteger();
            // Open a reader so that the writer will not delete partitions easily
            ObjList<TableReader> readers = new ObjList<>(tableCount);
            try {
                for (int i = 0; i < tableCount; i++) {
                    readers.add(getReader("tbl" + i));
                }

                Thread writeThread = new Thread(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < 32; i++) {
                            for (int j = 0; j < tableCount; j++) {
                                execute("insert into tbl" + j +
                                        " select 2, '1970-01-10T10' from long_sequence(1) " +
                                        "union all " +
                                        "select 1, '1970-01-09T09'  from long_sequence(1)");
                            }
                        }
                        Path.clearThreadLocals();
                        done.incrementAndGet();
                    } catch (Throwable ex) {
                        LOG.error().$(ex).$();
                        done.decrementAndGet();
                    }
                });

                Thread readThread = new Thread(() -> {
                    try {
                        barrier.await();
                        while (done.get() == 0) {
                            for (int i = 0; i < tableCount; i++) {
                                readers.get(i).openPartition(0);
                                readers.get(i).reload();
                            }
                            Os.pause();
                            Path.clearThreadLocals();
                        }
                    } catch (Throwable ex) {
                        LOG.error().$(ex).$();
                        done.addAndGet(-2);
                    }
                });

                writeThread.start();
                readThread.start();

                barrier.await();
                while (done.get() == 0) {
                    runPartitionPurgeJobs();
                    Os.pause();
                }
                runPartitionPurgeJobs();

                Assert.assertEquals(1, done.get());
                writeThread.join();
                readThread.join();
            } finally {
                Misc.freeObjList(readers);
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testCheckpointDoesNotBlockPurge() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        assertMemoryLeak(() -> {
            try (Path path = new Path()) {

                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

                path.concat(engine.getConfiguration().getDbRoot()).concat(engine.verifyTableName("tbl")).concat("1970-01-10");
                int len = path.size();

                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");

                // This should lock partition 1970-01-10.1 from being deleted from the disk
                engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), false);
                runPartitionPurgeJobs();
                testPartitionExist(path, len, true, false, false);

                // OOO insert
                execute("insert into tbl select 2, '1970-01-10T09'");
                runPartitionPurgeJobs();
                testPartitionExist(path, len, true, true, false);

                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T08'");
                runPartitionPurgeJobs();
                // .2 is also preserved: O3PartitionPurgeJob skips deletion during checkpoint
                testPartitionExist(path, len, true, true, true);

                engine.checkpointRelease();
                runPartitionPurgeJobs();
                testPartitionExist(path, len, false, false, true);
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testCheckpointInProgressDefersPartitionRemoval() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        // Regression test: when a checkpoint is in progress but the TxnScoreboard has NOT been
        // pinned for a specific table (e.g. the table was created after the checkpoint started,
        // or the checkpoint hasn't iterated to this table yet), the writer must still defer old
        // partition directory removal. Without this fix, a backup that reads partition data from
        // the live db directory can find a partition version referenced by the checkpoint's _txn
        // snapshot already deleted on disk.
        assertMemoryLeak(() -> {
            // Start a checkpoint on an empty database. This sets isInProgress() = true
            // but does NOT pin the TxnScoreboard for any table.
            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), false);

            try (Path path = new Path()) {
                // Create the table AFTER the checkpoint started. Its scoreboard is NOT pinned
                // by the checkpoint — this simulates the race window.
                execute("create table tbl as (select x, cast('1970-01-10T10' as "
                        + timestampType.getTypeName() + ") ts from long_sequence(1)) "
                        + "timestamp(ts) partition by DAY BYPASS WAL");

                path.concat(engine.getConfiguration().getDbRoot())
                        .concat(engine.verifyTableName("tbl"))
                        .concat("1970-01-10");
                int len = path.size();

                // O3 insert → creates partition version .1 (old unversioned dir goes to
                // partitionRemoveCandidates). processPartitionRemoveCandidates0 checks:
                //   anyReadersBeforeCommittedTxn = false (no scoreboard pin for tbl)
                //   checkpointInProgress = true  (checkpoint is active)
                // With the fix, the removal is deferred. Without it, .0 would be deleted.
                execute("insert into tbl select 4, '1970-01-10T09'");

                // Version .1 is the current partition
                testPartitionExist(path, len, true);

                // Another O3 insert → creates .2, old .1 goes to remove candidates.
                // Same logic: no scoreboard pin, but isInProgress() defers removal.
                execute("insert into tbl select 5, '1970-01-10T08'");

                // .1 must still exist (deferred by checkpoint), .2 is the current version
                testPartitionExist(path, len, true, true);

                // O3PartitionPurgeJob also skips deletion while checkpoint is in progress,
                // so .1 is protected from both synchronous and async removal.
            } finally {
                engine.checkpointRelease();
            }

            // After checkpoint release, old versions can be cleaned up
            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getDbRoot())
                        .concat(engine.verifyTableName("tbl"))
                        .concat("1970-01-10");
                int len = path.size();
                // .1 should be purged now, .2 is the current version
                testPartitionExist(path, len, false, true);
            }
            Assert.assertEquals("0 partition purge errors expected", 0,
                    engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testCheckpointInProgressDefersPartitionRemovalWal() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        // Same as testCheckpointInProgressDefersPartitionRemoval but for WAL tables.
        // WAL tables apply O3 inserts through the WAL apply job, which eventually calls
        // processPartitionRemoveCandidates0 on the TableWriter — the same code path.
        // Merge-append rewrites in place with no new directory version, defeating this test's premise.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
        assertMemoryLeak(() -> {
            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), false);

            try (Path path = new Path()) {
                execute("CREATE TABLE tbl AS (SELECT x, cast('1970-01-10T10' AS "
                        + timestampType.getTypeName() + ") ts FROM long_sequence(1)) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");

                drainWalQueue();

                path.concat(engine.getConfiguration().getDbRoot())
                        .concat(engine.verifyTableName("tbl"))
                        .concat("1970-01-10");
                int len = path.size();

                // O3 insert creates a new partition version; old version goes to remove candidates.
                // With checkpoint active, removal is deferred.
                execute("INSERT INTO tbl SELECT 4, '1970-01-10T09'");
                drainWalQueue();

                testPartitionExist(path, len, true);

                // Another O3 insert creates yet another version.
                execute("INSERT INTO tbl SELECT 5, '1970-01-10T08'");
                drainWalQueue();

                testPartitionExist(path, len, true, true);
            } finally {
                engine.checkpointRelease();
            }

            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getDbRoot())
                        .concat(engine.verifyTableName("tbl"))
                        .concat("1970-01-10");
                int len = path.size();
                // .1 should be purged now, .2 is the current version
                testPartitionExist(path, len, false, true);
            }
            Assert.assertEquals("0 partition purge errors expected", 0,
                    engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testCheckpointDoesNotBlockRollbackOrphanCleanup() throws Exception {
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        // Rollback orphan partitions (txn >= lastCommittedTxn) are not referenced by any
        // reader or checkpoint snapshot, so they must be cleaned up immediately even when
        // a checkpoint is in progress. This test creates a fake orphan directory (simulating
        // a partition left behind by a rolled-back transaction), then forces a writer re-open
        // which triggers purgeUnusedPartitions → removeNonAttachedPartitions →
        // processPartitionRemoveCandidates0, verifying the orphan is cleaned up.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tbl AS (SELECT x, cast('1970-01-10T10' AS "
                    + timestampType.getTypeName() + ") ts FROM long_sequence(1)) "
                    + "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // O3 insert → creates version .1 as the current partition
            execute("INSERT INTO tbl SELECT 4, '1970-01-10T09'");

            engine.checkpointCreate(sqlExecutionContext.getCircuitBreaker(), false);

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName("tbl");
                path.concat(engine.getConfiguration().getDbRoot())
                        .concat(tableToken)
                        .concat("1970-01-10");
                int len = path.size();

                // Create a fake orphan directory with a txn far exceeding the committed txn,
                // simulating a partition left behind by a rolled-back transaction.
                path.trimTo(len).put(".999").$();
                Assert.assertEquals("failed to create orphan dir", 0, Files.mkdir(path.$(), 509));
                Assert.assertTrue(Files.exists(path.$()));

                // Release and re-acquire the writer. The writer constructor calls
                // purgeUnusedPartitions → removeNonAttachedPartitions which scans directories.
                // It finds .999 as non-attached and adds it to partitionRemoveCandidates.
                // processPartitionRemoveCandidates0 sees txn=999 >= lastCommittedTxn → removes
                // it despite checkpoint being active (rollback orphans are safe to delete).
                engine.releaseAllWriters();
                execute("INSERT INTO tbl SELECT 5, '1970-01-10T08'");

                // .999 orphan should be cleaned up despite active checkpoint
                path.trimTo(len).put(".999").$();
                Assert.assertFalse("rollback orphan .999 should be removed during checkpoint",
                        Files.exists(path.$()));

                // .1 should still exist (deferred by checkpoint as a legitimate old version)
                testPartitionExist(path, len, true, true);
            } finally {
                engine.checkpointRelease();
            }
            Assert.assertEquals("0 partition purge errors expected", 0,
                    engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testDedupWithPartitionPurge() throws Exception {
        Assume.assumeTrue(timestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            // Create a table with initial data
            execute("create table tbl as (" +
                    "select x, timestamp_sequence('1970-01-10T10', 1000000) ts " +
                    "from long_sequence(10)" +
                    ") timestamp(ts) partition by DAY WAL dedup upsert keys (ts)");

            // OOO insert to create partition version
            execute("insert into tbl select 100 + x, '1970-01-10T09' from long_sequence(1)");
            drainWalQueue();

            // Insert data identical to last X rows (dedup-like scenario) plus new data at the end
            // This simulates the dedup case where we re-commit existing rows + add new ones
            execute("insert into tbl " +
                    "select x, timestamp_sequence('1970-01-10T10', 1000000) ts " +
                    "from long_sequence(10) " +  // identical to original last 10 rows
                    "union all " +
                    "select 200 + x, '1970-01-10T11' from long_sequence(1)");  // new data at the end

            drainWalQueue();


            // This should lock the partition from being deleted
            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);
                // Merge data to create another partition version
                execute("insert into tbl " +
                        "select 202, '1970-01-10T11'");  // new data at the end

                drainWalQueue();

                // Partition overwrite control should prevent purge of in-use partition
                // it would fail here if the 1970-01-01 is deleted
                runPartitionPurgeJobs();

                TestUtils.assertReader(
                        """
                                x	ts
                                101	1970-01-10T09:00:00.000000Z
                                1	1970-01-10T10:00:00.000000Z
                                2	1970-01-10T10:00:01.000000Z
                                3	1970-01-10T10:00:02.000000Z
                                4	1970-01-10T10:00:03.000000Z
                                5	1970-01-10T10:00:04.000000Z
                                6	1970-01-10T10:00:05.000000Z
                                7	1970-01-10T10:00:06.000000Z
                                8	1970-01-10T10:00:07.000000Z
                                9	1970-01-10T10:00:08.000000Z
                                10	1970-01-10T10:00:09.000000Z
                                201	1970-01-10T11:00:00.000000Z
                                """,
                        rdr,
                        sink
                );
            }

            assertQuery("tbl")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x	ts
                            101	1970-01-10T09:00:00.000000Z
                            1	1970-01-10T10:00:00.000000Z
                            2	1970-01-10T10:00:01.000000Z
                            3	1970-01-10T10:00:02.000000Z
                            4	1970-01-10T10:00:03.000000Z
                            5	1970-01-10T10:00:04.000000Z
                            6	1970-01-10T10:00:05.000000Z
                            7	1970-01-10T10:00:06.000000Z
                            8	1970-01-10T10:00:07.000000Z
                            9	1970-01-10T10:00:08.000000Z
                            10	1970-01-10T10:00:09.000000Z
                            202	1970-01-10T11:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testInvalidFolderNames() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            TableToken tableToken = engine.verifyTableName("tbl");
            try (Path path = new Path()) {
                Files.mkdir(path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("invalid_folder.123").$(), 509);
                Files.mkdir(path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-01.invalid").$(), 509);

                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testLastPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, timestamp_sequence('1970-01-10', 60*60*1000000L)::" + timestampType.getTypeName() + " ts from long_sequence(5)) timestamp(ts) partition by HOUR");

                TableToken tableToken = engine.verifyTableName("tbl");
                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition where ts >= '1970-01-10T03'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T03").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T03").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T04").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T05").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testManyReadersOpenClosedAscDense() throws Exception {
        testManyReadersOpenClosedDense(0, 1, 5);
    }

    @Test
    public void testManyReadersOpenClosedAscSparse() throws Exception {
        testManyReadersOpenClosedSparse(0, 1, 4);
    }

    @Test
    public void testManyReadersOpenClosedDescDense() throws Exception {
        testManyReadersOpenClosedDense(3, -1, 4);
    }

    @Test
    public void testManyReadersOpenClosedDescSparse() throws Exception {
        testManyReadersOpenClosedSparse(4, -1, 5);
    }

    @Test
    public void testManyTablesFuzzTest() throws Exception {
        Rnd rnd = TestUtils.generateRandom(LOG);
        int tableCount = 1;
        int testIterations = 100;

        assertMemoryLeak(() -> {
            for (int i = 0; i < tableCount; i++) {
                execute("create table tbl" + i + " as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            }

            ObjList<TableReader> readers = new ObjList<>();
            for (int i = 0; i < testIterations; i++) {
                String tableName = "tbl" + rnd.nextInt(tableCount);
                String partition = "1970-0" + (1 + rnd.nextInt(1)) + "-01";

                runPartitionPurgeJobs();

                if (rnd.nextBoolean()) {
                    // deffo OOO insert
                    execute("insert into " + tableName + " select 4, '" + partition + "T09'");
                } else {
                    // in order insert if last partition
                    execute("insert into " + tableName + " select 2, '" + partition + "T11'");
                }

                // lock reader on this transaction
                readers.add(getReader(tableName));
            }

            runPartitionPurgeJobs();

            for (int i = 0; i < testIterations; i++) {
                runPartitionPurgeJobs();
                TableReader reader = readers.get(i);
                reader.openPartition(0);
                reader.close();
            }

            try (
                    Path path = new Path();
                    TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())
            ) {
                for (int i = 0; i < tableCount; i++) {
                    String tableName = "tbl" + i;
                    TableToken tableToken = engine.verifyTableName(tableName);
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken);
                    int len = path.size();
                    int partitionBy = PartitionBy.DAY;
                    txReader.ofRO(path.concat(TXN_FILE_NAME).$(), ColumnType.TIMESTAMP, partitionBy);
                    txReader.unsafeLoadAll();

                    Assert.assertEquals(2, txReader.getPartitionCount());
                    for (int p = 0; p < 2; p++) {
                        long partitionTs = txReader.getPartitionTimestampByIndex(p);
                        long partitionNameVersion = txReader.getPartitionNameTxn(p);

                        for (int v = 0; v < partitionNameVersion + 5; v++) {
                            path.trimTo(len);
                            TableUtils.setPathForNativePartition(path, ColumnType.TIMESTAMP, partitionBy, partitionTs, v);
                            path.concat("x.d").$();
                            Assert.assertEquals(Utf8s.toString(path), v == partitionNameVersion, Files.exists(path.$()));
                        }
                    }
                    txReader.clear();
                }
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testNonAsciiTableName() throws Exception {
        String tableName = "таблица";

        assertMemoryLeak(() -> {
            execute("create table " + tableName + " as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            execute("insert into " + tableName + " select 4, '1970-01-10T09'");

            // in order insert
            execute("insert into " + tableName + " select 2, '1970-01-10T11'");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader(tableName)) {
                // OOO insert
                execute("insert into " + tableName + " select 4, '1970-01-10T09'");

                // This should not fail
                rdr.openPartition(0);
            }

            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.concat(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                }
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testNosyncDropRemovesPartitionsAtOnce() throws Exception {
        // Outside adaptive no save point holds a partition version, so a DROP without readers removes the dropped
        // versions at once and queues no purge task.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts " + timestampType.getTypeName() + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT timestamp_sequence('2024-01-01', 86_400_000_000L)::" + timestampType.getTypeName() + ", x FROM long_sequence(5)");
            drainWalQueue();
            final TableToken tt = engine.verifyTableName("t");
            final ObjList<String> dropped = partitionDirsBefore(tt, "2024-01-04");
            Assert.assertEquals(3, dropped.size());
            engine.releaseAllReaders();
            final MPSequence purgePubSeq = engine.getMessageBus().getO3PurgeDiscoveryPubSeq();
            final long queuedBefore = purgePubSeq.current();
            execute("ALTER TABLE t DROP PARTITION WHERE ts < '2024-01-04'");
            drainWalQueue();
            Assert.assertEquals("purge tasks the DROP queued", 0, purgePubSeq.current() - queuedBefore);
            final StringSink failures = new StringSink();
            checkOnDisk(failures, "after the DROP", tt, dropped, false);
            Assert.assertEquals("", failures.toString());
            assertQuery("SELECT count() FROM t").expectSize().noRandomAccess().returns("count\n2\n");
        });
    }

    @Test
    public void testPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

                // OOO inserts partition 1970-01-09
                execute("insert into tbl select 4, '1970-01-09T10'");

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.0").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                try (TableReader rdr = getReader("tbl")) {
                    // OOO inserts partition 1970-01-09
                    execute("insert into tbl select 4, '1970-01-09T09'");

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.2").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition list '1970-01-09'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
            }
        });
    }

    @Test
    public void testPartitionSplitWithReaders() throws Exception {
        assertMemoryLeak(() -> {
            Overrides overrides = node1.getConfigurationOverrides();
            overrides.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 100);

            TableToken token;
            TableModel tm = new TableModel(configuration, "tbl", PartitionBy.DAY)
                    .col("x", ColumnType.INT)
                    .timestamp("timestamp", timestampType.getTimestampType());
            token = createPopulateTable(1, tm, 2000, "2022-02-24T04", 2);

            Path path = Path.getThreadLocal("");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '2022-02-24T19'");

                try (TableReader rdr2 = getReader("tbl")) {
                    // in order insert
                    execute("insert into tbl select 2, '2022-02-26T19'");

                    path.of(engine.getConfiguration().getDbRoot()).concat(token)
                            .concat(timestampType == TestTimestampType.NANO ? "2022-02-24T185959-687500001.1" : "2022-02-24T185959-687501.1");
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // OOO insert
                    execute("insert into tbl select 4, '2022-02-24T19'");

                    runPartitionPurgeJobs();

                    rdr2.openPartition(0);
                }

                runPartitionPurgeJobs();

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TIMESTAMP_NS_TYPE_NAME.equals(timestampType.getTypeName()) ? "2022-02-24T185959-687500001.1" : "2022-02-24T185959-687501.1");
            Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

            path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(TIMESTAMP_NS_TYPE_NAME.equals(timestampType.getTypeName()) ? "2022-02-24T185959-687500001.3" : "2022-02-24T185959-687501.3");
            Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testPartitionsNotVacuumedBeforeCommit() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (" +
                    "select x, " +
                    "timestamp_sequence('1970-01-01', 10 * 60 * 60 * 1000000L)::" + timestampType.getTypeName() + " ts " +
                    "from long_sequence(1)" +
                    ") timestamp(ts) partition by HOUR");

            try (Path path = new Path()) {
                try (TableWriter writer = getWriter("tbl")) {
                    TimestampDriver driver = ColumnType.getTimestampDriver(writer.getTimestampType());
                    long startTimestamp = driver.fromHours(1) + driver.fromMicros(10);

                    for (int i = 0; i < 10; i++) {
                        TableWriter.Row row = writer.newRow(startTimestamp);
                        row.putLong(0, i + 1);
                        row.append();
                        startTimestamp += driver.fromHours(1);
                    }

                    TableToken tableToken = engine.verifyTableName("tbl");
                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-01T01.0").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    execute("vacuum table tbl");
                    runPartitionPurgeJobs();

                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    writer.commit();
                }
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testPhantomPartition() throws Exception {
        String phantomPartitionSuffix = "2022-02-24.2";
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean exists(LPSZ path) {
                // When the purge job checks if the phantom partition exists, return false
                // This simulates the partition being removed between directory scanning and the exists check
                if (Utf8s.endsWithAscii(path, phantomPartitionSuffix)) {
                    return false;
                }
                return super.exists(path);
            }
        };

        assertMemoryLeak(ff, () -> {
            TableToken token;
            TableModel tm = new TableModel(configuration, "tbl", PartitionBy.DAY)
                    .col("x", ColumnType.INT)
                    .timestamp("timestamp", timestampType.getTimestampType());
            token = createPopulateTable(1, tm, 2000, "2022-02-24T04", 2);

            Path path = Path.getThreadLocal("");

            // Open a reader to hold the initial transaction - this forces partition versioning
            // Without a reader, the writer can update partitions in place without creating versions
            // OOO insert - creates .1 version (can't delete .0 because reader holds it)
            execute("insert into tbl select 4, '2022-02-24T19'");

            // in order insert
            execute("insert into tbl select 2, '2022-02-26T19'");

            try (TableReader rdr = getReader("tbl")) {

                path.of(engine.getConfiguration().getDbRoot()).concat(token).concat("2022-02-24.1");
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                // OOO insert - creates .3 version (skipping .2, which would be the phantom)
                execute("insert into tbl select 4, '2022-02-24T19'");

                // Now create the phantom .2 partition directory
                // The phantom is created on disk so it's found during directory scanning,
                // but our exists() override returns false to simulate it disappearing
                path.of(engine.getConfiguration().getDbRoot()).concat(token).concat(phantomPartitionSuffix);
                Assert.assertEquals("Failed to create phantom partition", 0, Files.mkdir(path.$(), 509));

                // Run the purge job with NO readers open - rangeUnlocked will be true
                // The purge job will:
                // 1. Scan directories and find .1, .2 (phantom), .3
                // 2. When processing, check if partition .2 exists (via ff.exists())
                // 3. Our override returns false, triggering the phantom detection code path
                // 4. The purge job logs "partition dir removed after scanning" and aborts
                runPartitionPurgeJobs();

                for (int partitionIndex = 0; partitionIndex < rdr.getPartitionCount(); partitionIndex++) {
                    rdr.openPartition(partitionIndex);
                }
            }

            Assert.assertEquals("Partitions that purged that was in use", 0, engine.getPartitionOverwriteControl().getErrorCount());

        });
    }

    @Test
    public void testPurgeFailed() throws Exception {
        assertMemoryLeak(() -> {
            AtomicInteger deleteAttempts = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean rmdir(Path name, boolean lazy) {
                    if (Utf8s.endsWithAscii(name, "1970-01-10")) {
                        deleteAttempts.incrementAndGet();
                        return false;
                    }
                    return super.rmdir(name, lazy);
                }
            };

            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            try (Path path = new Path()) {
                runPartitionPurgeJobs();

                Assert.assertEquals(2, deleteAttempts.get()); // One message from Writer, one from Reader

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testPurgeFailedAndVacuumed() throws Exception {
        runPartitionPurgeJobs();
        assertMemoryLeak(() -> {
            AtomicInteger deleteAttempts = new AtomicInteger();
            ff = new TestFilesFacadeImpl() {
                @Override
                public boolean rmdir(Path name, boolean lazy) {
                    if (Utf8s.endsWithAscii(name, "1970-01-10")) {
                        if (deleteAttempts.incrementAndGet() < 3) {
                            return false;
                        }
                    }
                    return super.rmdir(name, lazy);
                }
            };

            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            try (Path path = new Path()) {
                runPartitionPurgeJobs();

                Assert.assertEquals(2, deleteAttempts.get()); // One message from Writer, one from Reader

                TableToken tableToken = engine.verifyTableName("tbl");
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                // VACUUM SQL should delete partition version 1970-01-10 on attempt 3
                execute("vacuum partitions tbl");
                runPartitionPurgeJobs();
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testReaderUsesPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // OOO insert
            execute("insert into tbl select 4, '1970-01-10T09'");

            // This should lock partition 1970-01-10.1 from being deleted from the disk
            try (TableReader rdr = getReader("tbl")) {

                // in order insert
                execute("insert into tbl select 2, '1970-01-10T11'");

                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");

                // This should not fail
                rdr.openPartition(0);
            }
            runPartitionPurgeJobs();

            try (Path path = new Path()) {
                path.concat(engine.getConfiguration().getDbRoot()).concat("tbl").concat("1970-01-10");
                int len = path.size();
                for (int i = 0; i < 3; i++) {
                    path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                    Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                }
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testRollbackWithActiveReaders() throws Exception {
        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (Utf8s.containsAscii(name, "1970-01-09.3")) {
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(ff, () -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
                TableToken tableToken = engine.verifyTableName("tbl");

                // Open a reader to not make partition remove trivial
                try (TableReader ignored = getReader("tbl")) {
                    // In order inserts partition 1970-01-09
                    execute("insert into tbl select 4, '1970-01-09T10'");
                    execute("insert into tbl select 4, '1970-01-09T09:59'");

                    // Simulate a rolled back commit, add a directory with name 1970-01-09.2
                    try {
                        execute("insert into tbl select 4, '1970-01-09T09'");
                        Assert.fail("expected file open error");
                    } catch (CairoException e) {
                        // If the message bus does not have empty slots, error message can vary
                        if (!Chars.contains(e.getFlyweightMessage(), "could not open read-write")) {
                            TestUtils.assertContains(e.getMessage(), "failed and will be rolled back");
                        }
                    }
                    execute("insert into tbl select 4, '1970-01-09T10'");
                    // Close this reader so that the purge job is potentially able to delete the partition version '.0'
                }

                try (TableReader rdr = getReader("tbl")) {

                    // Make in order insert to bump txn number
                    execute("insert into tbl select 4, '1970-01-09T10'");

                    // OOO inserts partition 1970-01-09
                    execute("insert into tbl select 5, '1970-01-09T09'");
                    execute("insert into tbl select 5, '1970-01-09T08'");

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-09.5").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    runPartitionPurgeJobs();

                    // This should not fail
                    rdr.openPartition(0);
                }
            }

            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testTableDropAfterPurgeScheduled() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 to not do delete in the writer
            try (TableReader ignored = getReader("tbl")) {
                // OOO insert
                execute("insert into tbl select 4, '1970-01-10T09'");
            }

            engine.releaseInactive();
            execute("drop table tbl");

            // The main assertion here is that job runs without exceptions
            runPartitionPurgeJobs();
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testTableRecreatedInBeforePartitionDirDelete() throws Exception {
        //Windows sometimes fails to drop the table on CI, and it's not easily reproducible locally
        Assume.assumeFalse(Os.isWindows());
        AtomicBoolean dropTable = new AtomicBoolean();
        Thread recreateTable = new Thread(() -> {
            try {
                execute("drop table tbl");
                execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            } catch (Throwable e) {
                LOG.info().$("Failed to recreate table: ").$(e).$();
            } finally {
                Path.clearThreadLocals();
            }
        });

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean unlinkOrRemove(Path path, Log LOG) {
                if (dropTable.get() && Utf8s.endsWithAscii(path, "1970-01-10")) {
                    dropTable.set(false);
                    recreateTable.start();
                    Os.sleep(50);
                }
                int checkedType = isSoftLink(path.$()) ? Files.DT_LNK : Files.DT_UNKNOWN;
                return unlinkOrRemove(path, checkedType, LOG);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 to not do delete in the writer
            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);

                // ooo insert
                execute("insert into tbl select 4, '1970-01-10T07'");
            }

            dropTable.set(true);
            purgeJob.drain(0);
            recreateTable.join();

            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testTableRecreatedViaRenameInBeforePartitionDirDelete() throws Exception {
        // Windows sometimes fails to drop the table on CI, and it's not easily reproducible locally
        Assume.assumeFalse(Os.isWindows());
        AtomicBoolean dropTable = new AtomicBoolean();
        Thread recreateTable = new Thread(() -> {
            try {
                execute("drop table tbl");
                execute("create table tbl1 as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
                execute("rename table tbl1 to tbl");
            } catch (Throwable e) {
                LOG.info().$("Failed to recreate table: ").$(e).$();
            } finally {
                Path.clearThreadLocals();
            }
        });

        FilesFacade ff = new TestFilesFacadeImpl() {
            @Override
            public boolean unlinkOrRemove(Path path, Log LOG) {
                if (dropTable.get() && Utf8s.endsWithAscii(path, "1970-01-10")) {
                    recreateTable.start();
                    Os.sleep(50);
                }
                int checkedType = isSoftLink(path.$()) ? Files.DT_LNK : Files.DT_UNKNOWN;
                return unlinkOrRemove(path, checkedType, LOG);
            }
        };

        assertMemoryLeak(ff, () -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            // This should lock partition 1970-01-10.1 to not do delete in the writer
            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);

                // ooo insert
                execute("insert into tbl select 4, '1970-01-10T07'");
            }

            dropTable.set(true);
            purgeJob.drain(0);
            dropTable.set(false);
            recreateTable.join();

            try (TableReader rdr = getReader("tbl")) {
                rdr.openPartition(0);
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testTableWriterDeletePartitionWhenNoReadersOpen() throws Exception {
        String tableName = "tbl";

        assertMemoryLeak(() -> {
            execute("create table " + tableName + " as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            execute("insert into " + tableName +
                    " select 2, '1970-01-11T09' from long_sequence(1) " +
                    "union all " +
                    " select 2, '1970-01-12T09' from long_sequence(1) " +
                    "union all " +
                    " select 2, '1970-01-11T08' from long_sequence(1) " +
                    "union all " +
                    " select 2, '1970-01-10T09' from long_sequence(1) " +
                    "union all " +
                    "select 1, '1970-01-09T09'  from long_sequence(1)");

            try (Path path = new Path()) {
                TableToken tableToken = engine.verifyTableName(tableName);
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-11").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-11.0").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-11.1").concat("x.d").$();
                Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    @Test
    public void testTheOnlyPartitionDeletedAsyncAfterDroppedBySql() throws Exception {
        assertMemoryLeak(() -> {
            try (Path path = new Path()) {
                execute("create table tbl as (select x, timestamp_sequence('1970-01-09T22', 60*60*1000000L)::" + timestampType.getTypeName() + " ts" +
                        " from long_sequence(10)) " +
                        " timestamp(ts) partition by HOUR");

                // Remove middle partition
                TableToken tableToken = engine.verifyTableName("tbl");
                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition list '1970-01-10T00'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T00").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                // Remove last partition
                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T00").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));

                try (TableReader rdr = getReader("tbl")) {
                    try (TableReader rdr2 = getReader("tbl")) {
                        execute("alter table tbl drop partition list '1970-01-10T07'", sqlExecutionContext);
                        runPartitionPurgeJobs();

                        // This should not fail
                        rdr2.openPartition(0);
                    }
                    runPartitionPurgeJobs();

                    path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T07").concat("x.d").$();
                    Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));

                    // This should not fail
                    rdr.openPartition(0);
                }
                runPartitionPurgeJobs();

                path.of(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10T07").concat("x.d").$();
                Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    private static void checkOnDisk(StringSink failures, String context, TableToken tt, ObjList<String> dirs, boolean expected) {
        for (int i = 0, n = dirs.size(); i < n; i++) {
            if (java.nio.file.Files.exists(java.nio.file.Path.of(configuration.getDbRoot(), tt.getDirName(), dirs.getQuick(i))) != expected) {
                failures.put(context).put(": ").put(dirs.getQuick(i)).put(expected ? " is gone" : " is still on disk").put('\n');
            }
        }
    }

    // The partition directories on disk now whose names sort before the given partition.
    private static ObjList<String> partitionDirsBefore(TableToken tt, String partitionName) throws Exception {
        final ObjList<String> dirs = new ObjList<>();
        try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.list(java.nio.file.Path.of(configuration.getDbRoot(), tt.getDirName()))) {
            s.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("20") && n.compareTo(partitionName) < 0).forEach(dirs::add);
        }
        return dirs;
    }

    private static void testPartitionExist(Path path, int len, boolean... existence) {
        for (int i = 0, n = existence.length; i < n; i++) {
            path.trimTo(len).put(".").put(Integer.toString(i + 1)).concat("x.d").$();
            Assert.assertEquals(Utf8s.toString(path), existence[i], Files.exists(path.$()));
        }
    }

    // The 2024-01-01 directories on disk now.
    private static ObjList<String> versionDirs(TableToken tt) throws Exception {
        final ObjList<String> dirs = new ObjList<>();
        try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.list(java.nio.file.Path.of(configuration.getDbRoot(), tt.getDirName()))) {
            s.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("2024-01-01")).forEach(dirs::add);
        }
        Assert.assertTrue(dirs.size() > 0);
        return dirs;
    }

    // Three days in two batches, so the table has two save points that both name 2024-01-01.
    private TableToken createAdaptiveDays(String table, boolean ttl) throws Exception {
        execute("CREATE TABLE " + table + " (ts " + timestampType.getTypeName() + ", v LONG) TIMESTAMP(ts) PARTITION BY DAY"
                + (ttl ? " TTL 2 DAYS" : "") + " WAL");
        execute("INSERT INTO " + table + " VALUES ('2024-01-01T00:00:00.000000Z', 1), ('2024-01-01T02:00:00.000000Z', 1), ('2024-01-02T00:00:00.000000Z', 2), ('2024-01-03T00:00:00.000000Z', 3)");
        drainWalQueue();
        execute("INSERT INTO " + table + " VALUES ('2024-01-03T01:00:00.000000Z', 3)");
        drainWalQueue();
        return engine.verifyTableName(table);
    }

    private void runPartitionPurgeJobs() {
        // when the reader is returned to pool, it remains in open state
        // holding files such that purge fails with access violation
        engine.releaseInactive();
        purgeJob.drain(0);
    }

    private void testManyReadersOpenClosedDense(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");

            TableReader[] readers = new TableReader[iterations];
            try {
                for (int i = 0; i < iterations; i++) {
                    TableReader rdr = getReader("tbl");
                    readers[i] = rdr;

                    // OOO insert
                    execute("insert into tbl select 4, '1970-01-10T09'");

                    runPartitionPurgeJobs();
                }

                // Unwind readers one by one old to new
                for (int i = start; i >= 0 && i < iterations; i += increment) {
                    TableReader reader = readers[i];

                    reader.openPartition(0);
                    reader.close();
                    readers[i] = null;

                    runPartitionPurgeJobs();
                }

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName("tbl");
                    path.concat(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10");
                    int len = path.size();

                    Assert.assertFalse(Utf8s.toString(path.concat("x.d")), Files.exists(path.$()));
                    for (int i = 0; i < iterations; i++) {
                        path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                        Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                    }

                    path.trimTo(len).put(".").put(Integer.toString(iterations)).concat("x.d").$();
                    Assert.assertTrue(Files.exists(path.$()));
                }
            } finally {
                Misc.free(readers);
            }
            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }

    private void testManyReadersOpenClosedSparse(int start, int increment, int iterations) throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tbl as (select x, cast('1970-01-10T10' as " + timestampType.getTypeName() + ") ts from long_sequence(1)) timestamp(ts) partition by DAY");
            TableReader[] readers = new TableReader[2 * iterations];

            try {
                for (int i = 0; i < iterations; i++) {
                    TableReader rdr = getReader("tbl");
                    readers[2 * i] = rdr;

                    // in order insert
                    execute("insert into tbl select 2, '1970-01-10T11'");

                    runPartitionPurgeJobs();

                    TableReader rdr2 = getReader("tbl");
                    readers[2 * i + 1] = rdr2;
                    // OOO insert
                    execute("insert into tbl select 4, '1970-01-10T09'");

                    runPartitionPurgeJobs();
                }

                // Unwind readers one by in set order
                for (int i = start; i >= 0 && i < iterations; i += increment) {
                    TableReader reader = readers[2 * i];
                    reader.openPartition(0);
                    reader.close();
                    readers[2 * i] = null;

                    runPartitionPurgeJobs();

                    reader = readers[2 * i + 1];
                    reader.openPartition(0);
                    reader.close();
                    readers[2 * i + 1] = null;

                    runPartitionPurgeJobs();
                }

                try (Path path = new Path()) {
                    TableToken tableToken = engine.verifyTableName("tbl");
                    path.concat(engine.getConfiguration().getDbRoot()).concat(tableToken).concat("1970-01-10");
                    int len = path.size();

                    Assert.assertFalse(Utf8s.toString(path.concat("x.d")), Files.exists(path.$()));
                    for (int i = 0; i < 2 * iterations; i++) {
                        path.trimTo(len).put(".").put(Integer.toString(i)).concat("x.d").$();
                        Assert.assertFalse(Utf8s.toString(path), Files.exists(path.$()));
                    }

                    path.trimTo(len).put(".").put(Integer.toString(2 * iterations)).concat("x.d").$();
                    Assert.assertTrue(Files.exists(path.$()));
                }
            } finally {
                Misc.free(readers);
            }

            Assert.assertEquals("0 partition purge errors expected", 0, engine.getPartitionOverwriteControl().getErrorCount());
        });
    }
}
