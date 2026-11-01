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

package io.questdb.test.cairo.crash;

import io.questdb.PropertyKey;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Under ADAPTIVE the epoch flush must be filesystem-wide on both the batched and the non-batched
 * column-sync paths, including when no column is open.
 *
 * <p>{@code TableWriter.fsyncMaterializedState()} makes the durable cut. Under ADAPTIVE the apply is lazy,
 * so the columns of closed and O3-merged partitions are not durable until the epoch flush. The non-batched
 * path ({@code isAdaptiveEpochColumnSyncBatched()} false) msyncs only the open
 * partition's columns in {@code syncColumns0()}, so it must also end with a filesystem-wide
 * {@code syncfs()}. Otherwise the epoch's {@code _txn} and {@code _cv} reference closed-partition rows that
 * never reached the device; recovery restores that epoch, the WAL replay does not rebuild rows at or below
 * the durable cut, and those rows are silently lost. The per-commit apply path stays lazy.
 *
 * <p>Each test forces one column-sync path and drives an epoch that covers rows outside the open partition's
 * columns: a closed partition, a partition just converted to parquet, or a tail folded in by a graceful close
 * with every column closed.
 * <ul>
 *   <li><b>Mechanism:</b> the epoch's {@code fsyncMaterializedState()} issues at least one
 *       {@code syncfs()}.</li>
 *   <li><b>Outcome</b> (native data only): after the crash and recovery, every row reads back complete and
 *       correct.</li>
 * </ul>
 * The tests run under per-inode journaling ({@code modelSharedJournal=false}), so the rows are durable only
 * if the epoch's own fs-wide flush made them so.
 */
public class AdaptiveEpochFsWideFlushCrashTest extends AbstractAdaptiveCrashTest {

    // Day-1 rows (closed once day 2 is applied), then day-2 rows (open). One epoch covers them all.
    private static final int DAY1_ROWS = 4;
    private static final int DAY2_ROWS = 3;
    private static final int TOTAL = DAY1_ROWS + DAY2_ROWS;

    /**
     * Every test in this class asserts that the epoch issued a filesystem-wide syncfs, which only exists on
     * Linux: {@code syncfs(2)} is a Linux-specific syscall (2.6.39 / glibc 2.14), absent from the macOS SDK
     * and libSystem, and Windows offers nothing equivalent that a non-elevated process can call. On those
     * platforms {@link io.questdb.std.FilesFacade#isSyncfsFileSystemWide()} is false and the epoch takes the
     * per-file fallback ({@code TableWriter.fsyncAttachedPartitionFiles}) instead, so the syncfs counter
     * never moves. The gate is on the capability rather than on the OS, so a future platform that gains a
     * scoped whole-filesystem barrier is covered automatically.
     */
    @Before
    public void assumeFsWideSyncfs() {
        Assume.assumeTrue(
                "syncfs(2) is Linux-only; this class asserts the fs-wide epoch flush path",
                TestFilesFacadeImpl.INSTANCE.isSyncfsFileSystemWide()
        );
    }

    @Test
    public void testNonBatchedEpochFlushIsFsWideClosedPartitionSurvives() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // The test drives the epoch explicitly to count syncfs calls around it; auto-epoch must not fire.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        // Force the non-batched path.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertFalse("the non-batched path must be active for this test",
                    engine.getConfiguration().isAdaptiveEpochColumnSyncBatched());

            runWithCrashFacade(() -> {
                // Per-inode journaling: nothing journals the closed partition's columns for free; only the
                // epoch's own fs-wide syncfs can make them durable.
                crashFf.modelSharedJournal = false;

                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");

                // Day-1 rows -> partition 2024-10-01. Apply.
                for (int i = 0; i < DAY1_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Day-2 rows -> partition 2024-10-02. Applying these closes the day-1 partition (it is no
                // longer the active partition), so syncColumns0(), which syncs the open partition only, does
                // not reach the day-1 columns during the epoch.
                for (int i = 0; i < DAY2_ROWS; i++) {
                    execute("insert into t values ('2024-10-02T0" + i + ":00:00.000000Z', " + (DAY1_ROWS + i) + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");

                // Pre-crash a reader sees all TOTAL rows.
                Assert.assertEquals("pre-crash must see all rows", TOTAL, readVs().size());

                // Take a real save point, whose flush is fsyncMaterializedState(), and check that it issued a
                // fs-wide syncfs although the batched path is disabled.
                final int syncfsBefore = crashFf.syncfsCount();
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
                final int syncfsAfter = crashFf.syncfsCount();
                Assert.assertTrue(
                        "I1: the non-batched EPOCH flush must issue a filesystem-wide syncfs (before=" + syncfsBefore
                                + ", after=" + syncfsAfter + ")",
                        syncfsAfter > syncfsBefore
                );

                // Crash: drop everything not made durable. The day-1 (closed) partition is durable only
                // because the epoch's fs-wide syncfs flushed it.
                crashAndReopen();

                // Restart: recovery restores the epoch cut, then the boot path opens the table. The epoch
                // covers all rows (seqTxn at the frontier), so there is no post-epoch tail to replay: the
                // rows must come straight from the durable column files.
                new io.questdb.cairo.RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                Assert.assertFalse("table must NOT be suspended", engine.getTableSequencerAPI().isSuspended(tt));

                final List<Long> post = readVs();
                Assert.assertEquals("ALL rows (incl. the closed day-1 partition) must survive the crash",
                        TOTAL, post.size());
                for (int i = 0; i < TOTAL; i++) {
                    Assert.assertEquals("row " + i + " value", Long.valueOf(i), post.get(i));
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, "true");
        }
    }

    /**
     * Convert-last-partition, batched epoch path: converting the last (and only) partition to parquet runs
     * {@code closeActivePartition(false)}, which closes every column fd, immediately before
     * {@code advanceDurableEpoch}. The epoch's fs-wide flush must still fire, or the save point names
     * just-converted rows that never reached the device.
     */
    @Test
    public void testConvertLastPartitionEpochFsWideBatched() throws Exception {
        runConvertLastPartitionEpochFsWideCase(true);
    }

    /**
     * Convert-last-partition, non-batched epoch path: the same case on the
     * {@code fsyncMaterializedStateSyncFs} branch.
     */
    @Test
    public void testConvertLastPartitionEpochFsWideNonBatched() throws Exception {
        runConvertLastPartitionEpochFsWideCase(false);
    }

    /**
     * Graceful-close epoch, batched path, all columns closed: the outcome check on native data. A clean
     * writer close folds the un-epoched tail into a final durable epoch ({@code doClose} ->
     * {@code advanceDurableEpoch}) with the active partition already closed, so {@code fsyncMaterializedState}
     * runs with no open column. Its syncfs must still fire, or the tail is lost on crash.
     */
    @Test
    public void testGracefulCloseAllColumnsClosedEpochFsWideBatched() throws Exception {
        runGracefulCloseAllColumnsClosedCase(true);
    }

    /**
     * Graceful-close epoch, non-batched path, all columns closed: the same outcome check on the
     * {@code fsyncMaterializedStateSyncFs} branch.
     */
    @Test
    public void testGracefulCloseAllColumnsClosedEpochFsWideNonBatched() throws Exception {
        runGracefulCloseAllColumnsClosedCase(false);
    }

    /**
     * The power loss comes before the harness releases the writer, so the close-time save point that release runs
     * (a filesystem-wide syncfs) must not make the un-epoched tail durable: right after the rollback the on-disk
     * {@code _txn} still sits at the first-batch save point, and recovery rebuilds the tail from the WAL.
     */
    @Test
    public void testReleaseAfterThePowerLossAddsNoDurabilityCrashAndReopen() throws Exception {
        runReleaseAfterPowerLossCase(false);
    }

    @Test
    public void testReleaseAfterThePowerLossAddsNoDurabilitySimulateRestart() throws Exception {
        runReleaseAfterPowerLossCase(true);
    }

    private void runGracefulCloseAllColumnsClosedCase(boolean batched) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Large interval: the first-batch epoch fires once, with the active partition open, then no cadence
        // epoch fires, so the second insert is an un-epoched tail that only the graceful-close epoch can make
        // durable. The interval is >= 0 because flush-on-close is gated on interval >= 0. The backlog cap
        // stays at the 5M default, which a handful of rows never trips.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, batched ? "true" : "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertTrue("flush-on-close must be enabled for this test",
                    engine.getConfiguration().isAdaptiveEpochFlushOnClose());
            Assert.assertEquals("the requested epoch column-sync path must be active",
                    batched, engine.getConfiguration().isAdaptiveEpochColumnSyncBatched());

            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;

                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");

                // First insert and drain: the first-batch epoch (lastEpochTs==0) fires here with the active
                // partition open, so it makes these rows durable and advances durableEpochSeqTxn.
                for (int i = 0; i < DAY1_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Second insert and drain: the interval is large, so no cadence epoch fires and these rows are
                // an un-epoched tail (durableEpochSeqTxn stays behind getSeqTxn) until the graceful-close epoch.
                for (int i = 0; i < DAY2_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T1" + i + ":00:00.000000Z', " + (DAY1_ROWS + i) + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");
                Assert.assertEquals("pre-crash must see all rows", TOTAL, readVs().size());

                // Explicitly close the active partition (closes every column fd), then return the writer to the
                // pool. The close on a pooled writer does not run doClose; releaseInactive() below does.
                try (TableWriter w = getWriter(tt)) {
                    w.closeActivePartition(false);
                }

                // Force the pooled writer to close: doClose folds the un-epoched tail into a final durable
                // epoch with all columns closed -> advanceDurableEpoch -> fsyncMaterializedState with no open
                // column. The syncfs fd comes from the stable _txn fd, so the fs-wide flush fires and the tail
                // becomes durable.
                final int syncfsBefore = crashFf.syncfsCount();
                engine.releaseInactive();
                final int syncfsAfter = crashFf.syncfsCount();
                Assert.assertTrue(
                        "CRIT-1: the graceful-close EPOCH (all columns closed, batched=" + batched + ") must "
                                + "still issue a filesystem-wide syncfs (before=" + syncfsBefore + ", after="
                                + syncfsAfter + ")",
                        syncfsAfter > syncfsBefore
                );

                // Crash: only what the epoch's fs-wide syncfs flushed is durable. A second-insert tail that
                // never reached the device is dropped here, which shows as row loss or a read fault.
                crashAndReopen();

                new io.questdb.cairo.RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                Assert.assertFalse("table must NOT be suspended", engine.getTableSequencerAPI().isSuspended(tt));
                final List<Long> post = readVs();
                Assert.assertEquals("ALL rows must survive the graceful-close crash", TOTAL, post.size());
                for (int i = 0; i < TOTAL; i++) {
                    Assert.assertEquals("row " + i + " value", Long.valueOf(i), post.get(i));
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, "true");
        }
    }

    private void runConvertLastPartitionEpochFsWideCase(boolean batched) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // interval=-1: no cadence epoch fires, so the only epoch (and the only syncfs) in the convert window
        // is the convert's own advanceDurableEpoch, and the syncfs delta isolates the convert epoch.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, batched ? "true" : "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertEquals("the requested epoch column-sync path must be active",
                    batched, engine.getConfiguration().isAdaptiveEpochColumnSyncBatched());

            runWithCrashFacade(() -> {
                // Per-inode journaling: nothing journals the converted partition's data for free; only the
                // epoch's own fs-wide syncfs can make it durable.
                crashFf.modelSharedJournal = false;

                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");

                // All rows in a single partition (2024-10-01), which is the last partition. Converting it
                // drives, on the apply worker, closeActivePartition(false) -> every column fd == -1, then
                // advanceDurableEpoch -> fsyncMaterializedState -> the fs-wide syncfs.
                for (int i = 0; i < TOTAL; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");
                Assert.assertEquals("pre-convert must see all rows", TOTAL, readVs().size());

                // Convert the last (and only) partition to parquet. On the apply worker this runs
                // closeActivePartition(false) -> every column fd == -1, then advanceDurableEpoch ->
                // fsyncMaterializedState. The syncfs fd comes from the stable _txn fd, so the fs-wide flush
                // fires with no column open and syncfsCount moves.
                //
                // This checks the mechanism only. The outcome (row survival) is checked on native data by the
                // graceful-close cases: the parquet writer is native (JNI) and its mmap page writes are opaque
                // to the Java crash facade (writtenDataEnd stays 0), so the facade cannot model parquet
                // durability and a parquet crash-survival assertion would mean nothing here.
                final int syncfsBefore = crashFf.syncfsCount();
                execute("alter table t convert partition to parquet list '2024-10-01'");
                drainWalQueue();
                final int syncfsAfter = crashFf.syncfsCount();
                Assert.assertTrue(
                        "CRIT-1: the convert-last-partition EPOCH (all columns closed, batched=" + batched
                                + ") must still issue a filesystem-wide syncfs (before=" + syncfsBefore
                                + ", after=" + syncfsAfter + ")",
                        syncfsAfter > syncfsBefore
                );

                // The convert itself is intact and readable (all rows still present post-convert).
                Assert.assertFalse("table must NOT be suspended", engine.getTableSequencerAPI().isSuspended(tt));
                final List<Long> post = readVs();
                Assert.assertEquals("post-convert must see all rows", TOTAL, post.size());
                for (int i = 0; i < TOTAL; i++) {
                    Assert.assertEquals("row " + i + " value", Long.valueOf(i), post.get(i));
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, "true");
        }
    }

    private void runReleaseAfterPowerLossCase(boolean simulateRestart) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // As in the graceful-close case: one first-batch save point, then an un-epoched tail that only the
        // close-time save point would make durable.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 3_600_000);
        try {
            Assert.assertTrue("flush-on-close must be enabled for this test",
                    engine.getConfiguration().isAdaptiveEpochFlushOnClose());
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                for (int i = 0; i < DAY1_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();
                for (int i = 0; i < DAY2_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T1" + i + ":00:00.000000Z', " + (DAY1_ROWS + i) + ")");
                }
                drainWalQueue();
                final TableToken tt = engine.verifyTableName("t");
                Assert.assertEquals("pre-crash must see all rows", TOTAL, readVs().size());

                final int syncfsBefore = crashFf.syncfsCount();
                if (simulateRestart) {
                    simulateRestart(new TableToken[]{tt});
                } else {
                    crashAndReopen();
                }
                Assert.assertTrue("the release must run the close-time save point", crashFf.syncfsCount() > syncfsBefore);
                Assert.assertTrue("the close-time save point after the power loss made the tail durable",
                        readOnDiskTxnSeqTxn(tt, PartitionBy.DAY) < TOTAL);

                new io.questdb.cairo.RecoveryCoordinator(engine).recover();
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();
                Assert.assertEquals("recovery rebuilds every row from the WAL", TOTAL, readVs().size());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private List<Long> readVs() {
        final List<Long> out = new ArrayList<>();
        try (io.questdb.cairo.sql.RecordCursorFactory f = select("select v from t order by ts")) {
            try (io.questdb.cairo.sql.RecordCursor c = f.getCursor(sqlExecutionContext)) {
                io.questdb.cairo.sql.Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (io.questdb.griffin.SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }
}
