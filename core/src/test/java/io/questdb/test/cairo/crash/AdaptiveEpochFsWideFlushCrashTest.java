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
import io.questdb.cairo.SnapshotMarker;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.std.str.Path;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * I1 crash oracle: under ADAPTIVE the EPOCH flush must be FILESYSTEM-WIDE even on the NON-BATCHED path.
 *
 * <p>{@code TableWriter.fsyncMaterializedState()} makes the durable cut. On the default Linux/batched
 * path it ends with a single fs-wide {@code syncfs()} (inside {@code syncColumnsBatchedSync}). But when
 * {@code isAdaptiveEpochColumnSyncBatched()} is false (ext4 fast_commit mounts) — or on non-Linux — it fell to
 * {@code syncColumns0()}, which msyncs ONLY the currently-OPEN partition's columns. Under ADAPTIVE the
 * apply is lazy, so CLOSED / O3-merged partition columns are non-durable; an epoch taken via that path
 * records {@code _txn}/{@code _cv} that reference rows in closed partitions whose data was never flushed.
 * Recovery then restores that epoch and the closed-partition tail (≤ epoch.seqTxn) is never re-derived
 * (it is at or below the durable cut, so the WAL replay does not rebuild it) → SILENT row loss.
 *
 * <p>The fix adds a whole-table {@code syncfs()} to the non-batched branch of
 * {@code fsyncMaterializedState()} (the EPOCH flush only — the per-commit apply path stays lazy).
 *
 * <p>This test forces the non-batched path, builds a table whose epoch'd rows live in a CLOSED partition,
 * drives the epoch, crashes, and recovers:
 * <ul>
 *   <li><b>Mechanism proof:</b> the epoch's {@code fsyncMaterializedState()} issues at least one
 *       {@code syncfs()} (the fs-wide flush) even though the batched path is disabled.</li>
 *   <li><b>Outcome proof:</b> after the crash + recovery, the rows in the CLOSED partition read back
 *       complete and correct (not lost).</li>
 * </ul>
 * Runs under per-inode journaling ({@code modelSharedJournal=false}) so the closed partition is durable
 * ONLY if the epoch's own fs-wide flush made it so — nothing else journals it for free.
 */
public class AdaptiveEpochFsWideFlushCrashTest extends AbstractCrashConsistencyTest {

    // Day-1 (CLOSED-after-day-2) rows, then day-2 (open) rows. All are epoch'd as one cut.
    private static final int DAY1_ROWS = 4;
    private static final int DAY2_ROWS = 3;
    private static final int TOTAL = DAY1_ROWS + DAY2_ROWS;

    /**
     * Every assertion in this class is that the epoch issued a FILESYSTEM-WIDE syncfs, which only exists on
     * Linux: {@code syncfs(2)} is a Linux-specific syscall (2.6.39 / glibc 2.14), absent from the macOS SDK
     * and libSystem, and Windows offers nothing equivalent that a non-elevated process can call. On those
     * platforms {@link io.questdb.std.FilesFacade#isSyncfsFileSystemWide()} is false and the epoch takes the
     * per-file fallback ({@code TableWriter.fsyncAttachedPartitionFiles}) instead, so the syncfs counter
     * correctly never moves and all five tests fail for a reason that has nothing to do with the behaviour
     * under test. Gate on the capability itself rather than on the OS, so a future platform that gains a
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
        // Drive the epoch explicitly so we can probe syncfs right around it; auto-epoch must not fire.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        // FORCE the non-batched path: this is the ext4-fast_commit / non-Linux case I1 targets.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertFalse("the non-batched path must be active for this test",
                    engine.getConfiguration().isAdaptiveEpochColumnSyncBatched());

            runWithCrashFacade(() -> {
                // Per-inode journaling: nothing journals the closed partition's columns for free — only the
                // epoch's own fs-wide syncfs can make them durable.
                crashFf.modelSharedJournal = false;

                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");

                // Day-1 rows -> partition 2024-10-01. Apply.
                for (int i = 0; i < DAY1_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Day-2 rows -> partition 2024-10-02. Applying these CLOSES the day-1 partition (it is no
                // longer the active/open partition), so syncColumns0() — open-partition only — will NOT
                // reach the day-1 columns during the epoch.
                for (int i = 0; i < DAY2_ROWS; i++) {
                    execute("insert into t values ('2024-10-02T0" + i + ":00:00.000000Z', " + (DAY1_ROWS + i) + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");

                // Pre-crash a reader sees all TOTAL rows.
                Assert.assertEquals("pre-crash must see all rows", TOTAL, readVs().size());

                // Drive the durable epoch via fsyncMaterializedState() and PROVE it issued a fs-wide syncfs
                // even though the batched path is disabled. Without the I1 fix this count does NOT move.
                final long epochSeqTxn;
                final long epochTxn;
                final int syncfsBefore = crashFf.syncfsCount();
                try (TableWriter w = getWriter(tt)) {
                    w.fsyncMaterializedState();
                    epochSeqTxn = w.getSeqTxn();
                    epochTxn = w.getTxn();
                }
                final int syncfsAfter = crashFf.syncfsCount();
                Assert.assertTrue(
                        "I1: the non-batched EPOCH flush must issue a filesystem-wide syncfs (before=" + syncfsBefore
                                + ", after=" + syncfsAfter + ")",
                        syncfsAfter > syncfsBefore
                );

                // Record the epoch marker exactly as the apply-worker advance() hook does.
                try (SnapshotMarker marker = new SnapshotMarker(engine.getConfiguration());
                     Path p = new Path()) {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.SNAPSHOT_FILE_NAME);
                    marker.of(p.$());
                    marker.write(epochSeqTxn, epochTxn, 1L);
                }

                // CRASH: drop everything not made durable. The day-1 (closed) partition is durable ONLY
                // because the epoch's fs-wide syncfs flushed it; without the fix it is dropped here.
                crashAndReopen();

                // RESTART: recovery restores the epoch cut, then the boot path opens the table. The epoch
                // covers ALL rows (seqTxn at the frontier), so there is no post-epoch tail to replay — the
                // rows must come straight from the now-durable column files.
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
     * CRIT-1 (convert-last-partition, BATCHED epoch path): converting the last (and only) partition to
     * parquet runs {@code closeActivePartition(false)} — which nulls EVERY column fd — immediately before
     * {@code advanceDurableEpoch}. The batched {@code syncColumnsBatchedSync} sourced its {@code syncfs} fd
     * from an open column and, finding none, no-op'd the fs-wide flush while still stamping
     * {@code _txn.epoch}/{@code _cv.epoch} + the marker past the just-converted rows -> silent loss.
     */
    @Test
    public void testConvertLastPartitionEpochFsWideBatched() throws Exception {
        runConvertLastPartitionEpochFsWideCase(true);
    }

    /**
     * CRIT-1 (convert-last-partition, NON-BATCHED epoch path): same defect on the
     * {@code fsyncMaterializedStateSyncFs} branch (ext4 fast_commit / non-Linux).
     */
    @Test
    public void testConvertLastPartitionEpochFsWideNonBatched() throws Exception {
        runConvertLastPartitionEpochFsWideCase(false);
    }

    /**
     * CRIT-1 (graceful-close epoch, BATCHED path, all columns closed) — the full outcome proof on NATIVE
     * data: a clean writer close folds the un-epoched tail into a final durable epoch ({@code doClose} ->
     * {@code advanceDurableEpoch}) with the active partition already closed, so {@code fsyncMaterializedState}
     * runs with NO open column. Without the fix the syncfs no-op'd and the tail was silently lost on crash.
     */
    @Test
    public void testGracefulCloseAllColumnsClosedEpochFsWideBatched() throws Exception {
        runGracefulCloseAllColumnsClosedCase(true);
    }

    /**
     * CRIT-1 (graceful-close epoch, NON-BATCHED path, all columns closed) — same outcome proof on the
     * {@code fsyncMaterializedStateSyncFs} branch (ext4 fast_commit / non-Linux).
     */
    @Test
    public void testGracefulCloseAllColumnsClosedEpochFsWideNonBatched() throws Exception {
        runGracefulCloseAllColumnsClosedCase(false);
    }

    private void runGracefulCloseAllColumnsClosedCase(boolean batched) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Large interval: the first-batch epoch fires once (active partition OPEN -> syncfs sources a valid
        // column fd, no bug), then NO cadence epoch re-fires, so the SECOND insert is an un-epoched tail that
        // only the graceful-close epoch can make durable. >= 0 so flush-on-close is allowed (it is gated on
        // interval >= 0). Backlog cap stays at the 5M default -> a handful of rows never trips it.
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

                // First insert + drain: the first-batch epoch (lastEpochTs==0) fires HERE with the active
                // partition OPEN, so it makes these rows durable and advances durableEpochSeqTxn.
                for (int i = 0; i < DAY1_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                // Second insert + drain: interval is huge -> no cadence epoch -> these rows are an UN-epoched
                // tail (durableEpochSeqTxn stays behind getSeqTxn), non-durable until the graceful-close epoch.
                for (int i = 0; i < DAY2_ROWS; i++) {
                    execute("insert into t values ('2024-10-01T1" + i + ":00:00.000000Z', " + (DAY1_ROWS + i) + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");
                Assert.assertEquals("pre-crash must see all rows", TOTAL, readVs().size());

                // Explicitly close the active partition (nulls EVERY column fd), then return the writer to the
                // pool. The close on a pooled writer does NOT run doClose; releaseInactive() below does.
                try (TableWriter w = getWriter(tt)) {
                    w.closeActivePartition(false);
                }

                // Force the pooled writer to actually close -> doClose folds the un-epoched tail into a final
                // durable epoch with ALL columns closed -> advanceDurableEpoch -> fsyncMaterializedState with
                // NO open column. Without the fix its syncfs no-ops (all column fds -1). The fix sources the
                // syncfs fd from the stable _txn fd, so the fs-wide flush fires and the tail becomes durable.
                final int syncfsBefore = crashFf.syncfsCount();
                engine.releaseInactive();
                final int syncfsAfter = crashFf.syncfsCount();
                Assert.assertTrue(
                        "CRIT-1: the graceful-close EPOCH (all columns closed, batched=" + batched + ") must "
                                + "still issue a filesystem-wide syncfs (before=" + syncfsBefore + ", after="
                                + syncfsAfter + ")",
                        syncfsAfter > syncfsBefore
                );

                // CRASH: only what the epoch's fs-wide syncfs flushed is durable. Without the fix the
                // second-insert tail was never device-flushed -> dropped -> silent loss / read fault.
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
        // interval=-1: no cadence epoch fires, so the ONLY epoch (and the ONLY syncfs) in the convert window
        // is the convert's own advanceDurableEpoch -> the syncfs delta isolates the convert epoch exactly.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_COLUMN_SYNC_BATCHED, batched ? "true" : "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertEquals("the requested epoch column-sync path must be active",
                    batched, engine.getConfiguration().isAdaptiveEpochColumnSyncBatched());

            runWithCrashFacade(() -> {
                // Per-inode journaling: nothing journals the converted partition's data for free — only the
                // epoch's own fs-wide syncfs can make it durable.
                crashFf.modelSharedJournal = false;

                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");

                // All rows in a SINGLE partition (2024-10-01) = the LAST partition. Converting it drives, on
                // the apply worker, closeActivePartition(false) -> EVERY column fd == -1, then advanceDurable-
                // Epoch -> fsyncMaterializedState -> the fs-wide syncfs.
                for (int i = 0; i < TOTAL; i++) {
                    execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
                }
                drainWalQueue();

                final TableToken tt = engine.verifyTableName("t");
                Assert.assertEquals("pre-convert must see all rows", TOTAL, readVs().size());

                // Convert the last (and only) partition to parquet. On the apply worker this runs
                // closeActivePartition(false) -> EVERY column fd == -1, then advanceDurableEpoch ->
                // fsyncMaterializedState. With the bug the epoch's syncfs sources its fd from an open column,
                // finds none, and NO-OPs -> syncfsCount does NOT move, yet the epoch still stamps
                // _txn.epoch/_cv.epoch + the marker past the just-converted rows (the silent-loss window). The
                // fix sources the syncfs fd from the stable _txn fd, so the fs-wide flush fires regardless.
                //
                // NB: this is the MECHANISM proof (the fs-wide flush fires with all columns closed). The
                // outcome (row survival) is proven on NATIVE data in testGracefulCloseAllColumnsClosed...,
                // because the parquet writer is native (JNI) and its mmap page writes are opaque to the Java
                // crash facade (writtenDataEnd stays 0), so the facade cannot model parquet durability either
                // way — before OR after the fix — making a parquet crash-survival assertion meaningless here.
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
