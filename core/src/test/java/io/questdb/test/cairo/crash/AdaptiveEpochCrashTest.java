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
import io.questdb.cairo.CairoError;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.cairo.wal.seq.TableTransactionLogFile;
import io.questdb.cairo.wal.seq.TableTransactionLogV1;
import io.questdb.std.ObjHashSet;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Crash oracle for the adaptive durable epoch. Under {@link CommitMode#ADAPTIVE} the WAL commit is
 * fdatasync-durable but the table apply is lazy: {@code TableWriter.syncColumns()} skips the per-column flush, so
 * after a power cut the partition columns can be non-durable while {@code _txn}/{@code _cv} were only msync'd. A
 * durable epoch ({@code TableWriter.fsyncMaterializedState()}, fired by the apply worker) makes those columns
 * durable and publishes a save point recovery can land on.
 *
 * <p>The first scenarios isolate the table's materialized-state durability; roll-forward of the post-epoch WAL is
 * out of scope. After {@code drainWalQueue} the table is caught up ({@code writerTxn == seqTxn}), so a post-crash
 * read re-applies no WAL and reads the durable on-disk column state.
 *
 * <h3>Crash model</h3>
 * {@link CrashFaultFilesFacade} models msync(MS_SYNC)/fsync/fdatasync/syncfs as durable and mmap stores as
 * non-durable until journaled. A single in-order insert is applied via the lag path, which does not fsync the
 * columns, and under adaptive {@code syncColumns()} is skipped, so right after the lazy apply the partition
 * column's {@code journaledDataEnd} is 0.
 *
 * <h3>Scenarios</h3>
 * <ol>
 *   <li>{@link #testEpochMakesLazilyAppliedRowsDurable}: adaptive, lazily apply a row, fire a durable epoch,
 *       crash, reopen: the row reads back correct and the save point recorded exactly the cut's seqTxn. Runs
 *       under per-inode journaling ({@code modelSharedJournal=false}).</li>
 *   <li>{@link #testNegativeControlWithoutEpochAnchorIsAbsent}: the same workload with epochs disabled records no
 *       save point beyond the creation baseline, before and after the crash.</li>
 *   <li>{@link #testCrashMidEpochFsyncNoSilentCorruption}: a crash partway through the epoch's fsync sequence
 *       (driven via {@code fsyncMaterializedState()}); reopening yields a correct prefix, a visible error, or
 *       fewer rows, never a wrong value.</li>
 * </ol>
 *
 * <h3>Save-point rows</h3>
 * The table-driven rows power off at every durability operation of a save point's publish, after every deletion of
 * its removal, after a data-sync fault and with a fenced durable frontier. They judge only through recovery, never
 * through the save point's file names or format: recovery accepts the state, every row the durable frontier covered
 * reads back with its value, a write after recovery lands, a second recovery changes no file of the table, and a
 * second power loss recovers the same rows.
 */
public class AdaptiveEpochCrashTest extends AbstractAdaptiveCrashTest {

    private static final long DAY = 86_400_000_000L;
    private static final long DAY0 = 1_704_067_200_000_000L; // 2024-01-01T00:00:00Z
    private static final int EIO = 5;
    // No periodic save point inside a row unless the row takes one.
    private static final long EPOCH_INTERVAL_MS = 1_000_000_000L;
    // Bounds a sweep whose crash point never stops firing.
    private static final int MAX_SWEEP = 200;
    private static final int N = 6;  // rows used by the mid-epoch-fsync corruption scenario
    private static final long TORN_TAIL_CLOCK_START = 1_000_000L;
    // The kept records end in the log's first page and the torn ones start in its second: a page boundary apart.
    private static final int TORN_TAIL_KEPT = 160;
    private static final long TORN_TAIL_WINDOW_US = 1_000_000L;
    private final List<String> rowFailures = new ArrayList<>();
    private String commitMode;
    private SavePointFaultFacade faultFf;
    private int modeSwitches;
    private long nextRowId;
    private int rowCount;

    /**
     * The epoch makes the lazily-applied prefix durable across a crash.
     */
    @Test
    public void testEpochMakesLazilyAppliedRowsDurable() throws Exception {
        // Adaptive with an epoch on every batch, so the prefix gets its epoch deterministically.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                // Per-inode journaling (ext4 fast_commit): a journal commit on _cv/_txn does not journal the column
                // files' extents, so only the epoch's own syncfs can make the lazily applied column durable. Under the
                // default shared-journal model the _cv msync would journal the column too and hide the epoch's
                // contribution.
                crashFf.modelSharedJournal = false;
                execute("create table e (ts timestamp, v varchar) timestamp(ts) partition by day wal");

                // One small in-order insert, applied via the lag path (no column fsync; syncColumns skipped under
                // adaptive), so the column is dirty and unsynced, as in the negative control. Here the apply worker
                // then fires a durable epoch (interval 0), whose fsyncMaterializedState() makes the column durable.
                execute("insert into e values (" + tsOf(0) + ", '" + vOf(0) + "')");
                drainWalQueue(); // lazy lag apply + epoch

                TableToken tt = engine.verifyTableName("e");
                // The save point exists and covers the applied row (seqTxn == 1).
                assertMarkerSeqTxn(tt, 1);

                crashAndReopen();

                // The epoch's cut reads back durable and correct after the crash: the row is present and the save point
                // (asserted above) recorded this cut's seqTxn, under strict per-inode journaling.
                List<String> actual = readVarcharColumn("e", "v");
                Assert.assertEquals("epoch'd row count must survive crash", 1, actual.size());
                Assert.assertEquals("epoch'd row value", vOf(0), actual.get(0));
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * Negative control: with epochs disabled, the lazily applied columns are never made durable, and no save point
     * beyond the creation baseline is recorded. Scenario 1's durability therefore comes from the epoch flush, not
     * from another sync on the apply path.
     */
    @Test
    public void testDisabledPeriodicEpochRetainsCreationBaseline() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1); // epochs disabled
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                execute("create table n (ts timestamp, v varchar) timestamp(ts) partition by day wal");

                // The same single-insert lag path as scenario 1 (the column is dirty and unsynced after the lazy apply:
                // journaledDataEnd of n/2023-11-14/v.d is 0 here), but no epoch fires.
                execute("insert into n values (" + tsOf(0) + ", '" + vOf(0) + "')");
                drainWalQueue(); // lazy lag apply, NO epoch

                TableToken tt = engine.verifyTableName("n");

                // Disabling periodic epochs must still retain the creation-time generation-0 replay floor.
                assertCreationBaseline(tt);

                crashAndReopen();

                // The proven baseline survives and recovery can replay the WAL from seqTxn zero.
                assertCreationBaseline(tt);
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * Arms a crash partway through the epoch's fsync sequence: reopening never silently corrupts. The epoch issues
     * many durability ops (column syncfs, _cv fsync, _txn fsync, save point file fsyncs); a crash on an early one
     * leaves a partially durable cut, which degrades safely (correct prefix, visible error, fewer rows), never to a
     * wrong value.
     */
    @Test
    public void testCrashMidEpochFsyncNoSilentCorruption() throws Exception {
        // Epochs disabled on the apply path so the apply drains cleanly; the test then drives the epoch via
        // fsyncMaterializedState() with a crash armed mid-way, isolating the mid-epoch crash from the apply's own WAL
        // durability ops.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                execute("create table m (ts timestamp, v varchar) timestamp(ts) partition by day wal");
                for (int i = 0; i < N; i++) {
                    execute("insert into m values (" + tsOf(i) + ", '" + vOf(i) + "')");
                }
                drainWalQueue(); // clean lazy apply, no epoch

                // Drive the epoch's durable cut directly, armed to crash a few fsyncs in.
                boolean crashed = false;
                try (io.questdb.cairo.TableWriter w = getWriter(engine.verifyTableName("m"))) {
                    crashFf.armCrashAt(crashFf.durabilityOpCount() + 4);
                    try {
                        w.fsyncMaterializedState();
                    } catch (CrashSimulationError expected) {
                        crashed = true;
                    }
                } catch (CrashSimulationError expected) {
                    // The crash can also fire as the writer is closed or flushed; still a mid-cut crash.
                    crashed = true;
                }
                Assert.assertTrue("the crash must have fired during the epoch's fsync sequence", crashed);

                crashAndReopen();

                // Whatever survives is not silently wrong: a correct prefix, a visible error, or fewer rows are
                // acceptable; a wrong value is not.
                assertNoSilentCorruptionVarchar("m", "v");
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    @Test
    public void testFailedHeaderSyncThenPowerLoss() throws Exception {
        assertFailedHeaderSyncKeepsData(false, false);
    }

    @Test
    public void testFailedHeaderSyncThenRestartNextSavePointThenPowerLoss() throws Exception {
        assertFailedHeaderSyncThenRestartKeepsData(false, true);
    }

    @Test
    public void testFailedHeaderSyncThenRestartVacuumThenPowerLoss() throws Exception {
        assertFailedHeaderSyncKeepsData(true, false);
    }

    @Test
    public void testFailedHeaderSyncThenRestartWalPurgeThenPowerLoss() throws Exception {
        assertFailedHeaderSyncThenRestartKeepsData(false, false);
    }

    @Test
    public void testFailedHeaderSyncThenRestartWriterOpenThenPowerLoss() throws Exception {
        assertFailedHeaderSyncKeepsData(true, true);
    }

    @Test
    public void testFailedHeaderSyncThenRestartWriterOpenWalPurgeThenPowerLoss() throws Exception {
        assertFailedHeaderSyncThenRestartKeepsData(true, false);
    }

    /**
     * A data-sync fault at every durability operation of an O3 write whose apply takes a save point; the operation
     * completes on disk and then reports the error. Later O3 rewrites, a partition purge and a WAL purge follow
     * before the power loss, so a save point the failed publish left valid on disk must still find its data.
     */
    @Test
    public void testFaultOnSavePointSyncThenO3RewriteAndPurge() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (int op = 1; op <= MAX_SWEEP; op++) {
                final Scenario s = newScenario("f" + op);
                insertRows(s, 0, 2);
                insertRows(s, 1, 2);
                drainWalQueue();
                engine.releaseAllWalWriters();
                publishSavePoint(s);
                markDurableBaseline();
                // The next apply rewrites the first day's partition and takes a save point.
                tracker(s).setLastEpochTs(0);
                faultFf.armFaultAt(faultFf.durabilityOpCount() + op);
                tryInsertRows(s, 0, 2, true);
                try {
                    drainWalQueue();
                } catch (CairoError stop) {
                    // A failed flush in the apply stops the engine (D61); the writes below are then refused, as in
                    // production, where the process halts at the failed flush.
                    Assert.assertTrue(engine.isDurabilityFailed());
                }
                final boolean faulted = !faultFf.isFaultArmed();
                s.firedAt = faulted ? faultFf.faultedOp : null;
                faultFf.disarm();
                tryInsertRows(s, 0, 2, true);
                drainWalQueue();
                engine.releaseAllReaders();
                purgePartitions();
                purgeWal();
                powerLossAndCheck(s, faulted ? "fault at durability operation " + op : "no fault");
                if (!faulted) {
                    break;
                }
            }
            assertNoRowFailures();
        });
    }

    /**
     * A failure that does not poison the engine (a failed open, write or truncate) at every such call of a table's
     * first periodic save point, then an O3 rewrite of the partitions it would have named, a partition purge, a WAL
     * purge and a kill: the failed save point must not be selectable once the versions it names are gone.
     */
    @Test
    public void testIoFailureInSavePointThenO3RewritePurgeAndKill() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (int n = 1; n <= MAX_SWEEP; n++) {
                final Scenario s = newScenario("io" + n);
                insertRows(s, 0, 2);
                insertRows(s, 1, 2);
                drainWalQueue();
                engine.releaseAllWalWriters();
                markDurableBaseline();
                faultFf.armIoFaultAt(n);
                try {
                    publishSavePoint(s);
                } catch (CairoException | CairoError ignore) {
                    // The save point the failure interrupted.
                }
                final boolean faulted = !faultFf.isIoFaultArmed();
                s.firedAt = faulted ? faultFf.faultedOp : null;
                faultFf.disarm();
                // Rows before every existing row of both days: an O3 merge writes new partition versions.
                insertRows(s, 0, 2, true, true, true);
                insertRows(s, 1, 2, true, true, true);
                drainWalQueue();
                engine.releaseAllReaders();
                purgePartitions();
                purgeWal();
                restartAndCheck(s, faulted ? "failure at I/O call " + n : "no failure", false);
                if (!faulted) {
                    break;
                }
            }
            assertNoRowFailures();
        });
    }

    /**
     * A power loss after every durability operation and after every deletion of a save-point removal on a table with
     * two save points, whose WAL below the newer one was purged under its floor: the exit a non-adaptive restart
     * performs during recovery, followed by a start in the exit's mode and one in adaptive, and the demote clear.
     */
    @Test
    public void testPowerLossAfterEveryDeletionOfSavePointRemoval() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (String removal : new String[]{"sync exit", "nosync exit", "demote clear"}) {
                final boolean demote = "demote clear".equals(removal);
                final String exitMode = demote ? "adaptive" : removal.substring(0, removal.indexOf(' '));
                for (String secondMode : demote ? new String[]{"adaptive"} : new String[]{exitMode, "adaptive"}) {
                    for (int deletions = 0; deletions < 2; deletions++) {
                        for (int n = 1; n <= MAX_SWEEP; n++) {
                            setCommitMode("adaptive");
                            final Scenario s = newScenario("x_" + removal.charAt(0) + secondMode.charAt(0) + "_" + deletions + "_" + n);
                            insertRows(s, 0, 2);
                            drainWalQueue();
                            engine.releaseAllWalWriters();
                            publishSavePoint(s);
                            purgeWal();
                            markDurableBaseline();
                            final Action action;
                            if (demote) {
                                // A replica's applied state has NOSYNC grade, so no rows are applied after the save point.
                                action = () -> reconcileSavePoint(s, LocalDurabilityPolicy.REPLICA_SKIP);
                            } else {
                                insertRows(s, 1, 2);
                                drainWalQueue();
                                // Released up front, so the deletions counted are the exit's and no writer stays locked.
                                engine.releaseAllReaders();
                                engine.releaseAllWriters();
                                engine.releaseAllWalWriters();
                                action = () -> cleanRestart(s, exitMode);
                            }
                            final boolean crashed = deletions == 0 ? crashAtOperation(s, n, action) : crashAfterRemoval(s, n, action);
                            final String label;
                            if (crashed) {
                                label = removal + ", power loss after " + (deletions == 0 ? "durability operation " : "deletion ") + n;
                            } else if (secondMode.equals(exitMode)) {
                                label = removal + " completed, then rows and a WAL purge";
                                insertRows(s, 2, 2);
                                drainWalQueue();
                                purgeWal();
                                if ("sync".equals(exitMode)) {
                                    // The purge leaves no WAL to replay a partition whose name SYNC did not make durable.
                                    fsyncPartitionDirectories(s);
                                }
                            } else {
                                // No WAL purge here: crash() does not re-seed the durable content of a WAL directory it
                                // brings back, so the second power loss of an adaptive start would empty it.
                                label = removal + " completed";
                            }
                            setCommitMode(secondMode);
                            powerLossAndCheck(s, label + (demote ? "" : ", second start " + secondMode));
                            if (!crashed) {
                                break;
                            }
                        }
                    }
                }
            }
            assertNoRowFailures();
        });
    }

    /**
     * A power loss at every durability operation of the baseline a table without a save point gets: at the start
     * and first write after a demote clear, at the writer's reconcile after a promote (also with rows a replica
     * tenure applied lazily, which the baseline must flush), and at the start and first write of a table created
     * outside adaptive. The last row of each sweep loses power after the baseline, more rows and a WAL purge.
     */
    @Test
    public void testPowerLossAtEveryDurabilityOperationOfBaselineWithoutSavePoint() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (String baseline : new String[]{"demote clear, then a start", "demote clear, then a promote",
                    "replica rows, then a promote", "nosync table, then a start"}) {
                final boolean nosync = baseline.startsWith("nosync");
                final boolean tenure = baseline.startsWith("replica rows");
                final boolean promote = baseline.endsWith("promote");
                for (int op = 1; op <= MAX_SWEEP; op++) {
                    setCommitMode(nosync ? "nosync" : "adaptive");
                    final Scenario s = newScenario("m" + (nosync ? 'r' : tenure ? 't' : promote ? 'p' : 's') + op);
                    insertRows(s, 0, 2);
                    drainWalQueue();
                    engine.releaseAllWalWriters();
                    markDurableBaseline();
                    s.markUnsyncedRowsDurable();
                    if (!nosync) {
                        reconcileSavePoint(s, LocalDurabilityPolicy.REPLICA_SKIP);
                    }
                    if (tenure) {
                        // The tenure applies rows lazily; only its WAL and sequencer files are durable.
                        engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
                        try {
                            insertRows(s, 1, 2);
                            drainWalQueue();
                        } finally {
                            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
                        }
                        engine.releaseAllWalWriters();
                        markWalAndSequencerDurable(s.token);
                    }
                    Assert.assertEquals(0, savePointCount(s));
                    final boolean crashed = crashAtOperation(s, op, promote
                            ? () -> reconcileSavePoint(s, LocalDurabilityPolicy.ALWAYS_ON)
                            : () -> {
                        cleanRestart(s, "adaptive");
                        // The first write enrols the table.
                        insertRows(s, 1, 1, false, true, false);
                        drainWalQueue();
                    });
                    if (!crashed) {
                        insertRows(s, 2, 2);
                        drainWalQueue();
                        purgeWal();
                    }
                    powerLossAndCheck(s, crashed
                            ? baseline + ", power loss after durability operation " + op
                            : baseline + " completed, then rows and a WAL purge");
                    if (!crashed) {
                        break;
                    }
                }
            }
            assertNoRowFailures();
        });
    }

    /**
     * A power loss at every durability operation of recovery's restore, from the creation baseline and from a
     * periodic save point, with {@code _meta}, {@code _txn} and {@code _cv} all ahead of the save point: a column
     * added, rows and an UPDATE applied lazily after it, then a kill. The next start must finish the restore and
     * read back every row, and a start after it must change nothing.
     */
    @Test
    public void testPowerLossAtEveryDurabilityOperationOfRestore() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (String savePoint : new String[]{"creation baseline", "periodic save point"}) {
                final boolean periodic = savePoint.startsWith("periodic");
                for (int op = 1; op <= MAX_SWEEP; op++) {
                    setCommitMode("adaptive");
                    final Scenario s = newScenario("r" + (periodic ? 'p' : 'c') + op);
                    if (periodic) {
                        insertRows(s, 0, 2);
                        drainWalQueue();
                        publishSavePoint(s);
                    }
                    execute("alter table " + s.table + " add column c int");
                    insertRows(s, 1, 2);
                    execute("update " + s.table + " set c = 7");
                    drainWalQueue();
                    engine.releaseAllReaders();
                    engine.releaseAllWriters();
                    engine.releaseAllWalWriters();
                    restart(s);
                    // A kill keeps the page cache: the live files ahead of the save point are on disk.
                    markDurableBaseline();
                    final boolean crashed = crashAtOperation(s, op, () -> new RecoveryCoordinator(engine).recover());
                    powerLossAndCheck(s, crashed
                            ? "restore from the " + savePoint + ", power loss after durability operation " + op
                            : "restore from the " + savePoint + " completed");
                    if (!crashed) {
                        break;
                    }
                }
            }
            assertNoRowFailures();
        });
    }

    /**
     * A power loss at every durability operation of a table's first save point (taken when a table created
     * without one enrols at writer open), of its first periodic save point (a new generation) and of a later one
     * (a generation rewritten in place), and of the idle save point the apply job takes when the WAL purge job asks
     * for one, at the newest save point's state and over rows applied after it. The last row of each sweep loses
     * power after the save point, more rows and a WAL purge under the new floor.
     */
    @Test
    public void testPowerLossAtEveryDurabilityOperationOfSavePoint() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (int op = 1; op <= MAX_SWEEP; op++) {
                setCommitMode("nosync");
                final Scenario s = newScenario("e" + op);
                insertRows(s, 0, 2);
                insertRows(s, 1, 2);
                drainWalQueue();
                engine.releaseAllWalWriters();
                markDurableBaseline();
                s.markUnsyncedRowsDurable();
                cleanRestart(s, "adaptive");
                final boolean crashed = crashAtOperation(s, op, () -> {
                    try (TableWriter ignore = getWriter(s.token)) {
                        Assert.assertNotNull(ignore);
                    }
                });
                if (!crashed) {
                    insertRows(s, 2, 2);
                    drainWalQueue();
                    purgeWal();
                }
                powerLossAndCheck(s, crashed
                        ? "first save point at enrolment, power loss after durability operation " + op
                        : "first save point at enrolment completed, then rows and a WAL purge");
                if (!crashed) {
                    break;
                }
            }
            for (int prior = 0; prior < 2; prior++) {
                final String which = prior == 0 ? "first periodic save point" : "later periodic save point";
                for (int op = 1; op <= MAX_SWEEP; op++) {
                    setCommitMode("adaptive");
                    final Scenario s = newScenario("p" + prior + "_" + op);
                    insertRows(s, 0, 2);
                    drainWalQueue();
                    engine.releaseAllWalWriters();
                    for (int i = 0; i < prior; i++) {
                        publishSavePoint(s);
                        insertRows(s, 1, 2);
                        drainWalQueue();
                        engine.releaseAllWalWriters();
                    }
                    // The rows this save point covers are applied lazily, after the baseline, under per-inode
                    // journaling: only the save point's own flush makes their columns durable.
                    faultFf.modelSharedJournal = false;
                    markDurableBaseline();
                    insertRows(s, 1, 2);
                    drainWalQueue();
                    engine.releaseAllWalWriters();
                    final boolean crashed = crashAtOperation(s, op, () -> publishSavePoint(s));
                    if (crashed) {
                        writeBackLiveState(s);
                    } else {
                        insertRows(s, 2, 2);
                        drainWalQueue();
                        purgeWal();
                    }
                    powerLossAndCheck(s, crashed
                            ? which + ", power loss after durability operation " + op
                            : which + " completed, then rows and a WAL purge under its floor");
                    faultFf.modelSharedJournal = true;
                    if (!crashed) {
                        break;
                    }
                }
            }
            for (int tail = 0; tail < 2; tail++) {
                final String which = tail == 0 ? "idle save point at the newest save point's state" : "idle save point over rows applied after the newest save point";
                for (int op = 1; op <= MAX_SWEEP; op++) {
                    setCommitMode("adaptive");
                    final Scenario s = newScenario("i" + tail + "_" + op);
                    insertRows(s, 0, 2);
                    drainWalQueue();
                    engine.releaseAllWalWriters();
                    publishSavePoint(s);
                    if (tail == 0) {
                        // A second save point at the applied state; the first one is older.
                        insertRows(s, 1, 2);
                        drainWalQueue();
                        engine.releaseAllWalWriters();
                        publishSavePoint(s);
                        markDurableBaseline();
                    } else {
                        // Applied lazily: only the WAL holds these rows until a save point flushes them. Per-inode
                        // journaling, so that only the save point's own flush makes their columns durable.
                        faultFf.modelSharedJournal = false;
                        markDurableBaseline();
                        tracker(s).setLastEpochTs(nowMs());
                        insertRows(s, 1, 2);
                        drainWalQueue();
                        engine.releaseAllWalWriters();
                    }
                    // The table has taken no save point for an interval: the WAL purge job asks for one.
                    tracker(s).setLastEpochTs(0);
                    purgeWal();
                    final boolean crashed = crashAtOperation(s, op, () -> drainWalQueue());
                    if (crashed && tail == 1) {
                        writeBackLiveState(s);
                    }
                    if (!crashed) {
                        // The newest save point stands on the applied state, and without a tail so does the older one.
                        if (tracker(s).getDurableEpochSeqTxn() != tracker(s).getWriterTxn()
                                || (tail == 0 && tracker(s).getOtherPinnedEpochTxn() != tracker(s).getPinnedEpochTxn())) {
                            rowFailures.add(s.table + " [" + which + "]: no idle save point at the applied state");
                        }
                        // The rest of the idle spell: after a tail, the next request puts the older save point on the
                        // applied state too, and the WAL purge then frees the WAL that held the tail.
                        tracker(s).setLastEpochTs(0);
                        purgeWal();
                        drainWalQueue();
                        if (tracker(s).getOtherPinnedEpochTxn() != tracker(s).getPinnedEpochTxn()) {
                            rowFailures.add(s.table + " [" + which + "]: the older save point is not on the applied state");
                        }
                        purgeWal();
                    }
                    powerLossAndCheck(s, crashed
                            ? which + ", power loss after durability operation " + op
                            : which + " completed, then the idle spell's last save point and a WAL purge under its floor");
                    faultFf.modelSharedJournal = true;
                    if (!crashed) {
                        break;
                    }
                }
            }
            assertNoRowFailures();
        });
    }

    /**
     * A replica keeps no save point and never rewinds, so its partition moves finish at once, as outside adaptive: a
     * DETACH applied on the replica takes its final name, an ATTACH applied there consumes its source, and a DETACH
     * left under its temporary name (by a primary whose save point did not follow) gets its final name at the
     * replica's next writer open.
     */
    @Test
    public void testReplicaFinishesPartitionMovesAtOnce() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            final Scenario s = newScenario("rdet");
            insertRows(s, 0, 2);
            insertRows(s, 1, 2);
            insertRows(s, 2, 2);
            drainWalQueue();
            engine.releaseAllWalWriters();
            publishSavePoint(s);
            reconcileSavePoint(s, LocalDurabilityPolicy.REPLICA_SKIP);
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.REPLICA_SKIP);
            try {
                execute("alter table rdet detach partition list '2024-01-02'");
                drainWalQueue();
                Assert.assertEquals("[2024-01-01, 2024-01-02.detached, 2024-01-03]", partitionDirs(s));
                Files.move(tableDir(s).resolve("2024-01-02.detached"), tableDir(s).resolve("2024-01-02.attachable"));
                execute("alter table rdet attach partition list '2024-01-02'");
                drainWalQueue();
                Assert.assertEquals("[2024-01-01, 2024-01-02.2, 2024-01-03]", partitionDirs(s));

                execute("alter table rdet detach partition list '2024-01-01'");
                drainWalQueue();
                final long detachTxn;
                try (TableWriter w = getWriter(s.token)) {
                    detachTxn = w.getTxn();
                }
                engine.releaseAllWriters();
                Files.move(tableDir(s).resolve("2024-01-01.detached"), tableDir(s).resolve("2024-01-01." + detachTxn + ".detaching"));
                try (TableWriter w = getWriter(s.token)) {
                    Assert.assertNotNull(w);
                }
                engine.releaseAllWriters();
                Assert.assertEquals("[2024-01-01.detached, 2024-01-02.2, 2024-01-03]", partitionDirs(s));
                assertQuery("select count() from rdet").noRandomAccess().expectSize().returns("count\n4\n");
            } finally {
                engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
            }
        });
    }

    /**
     * A save point attempted after a flush of the table's sequencer log failed and fenced its durable frontier.
     * The failed flush loses the log's unsynced pages for good, so no later sync makes them durable.
     */
    @Test
    public void testSavePointWhileDurableFrontierFenced() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            final Scenario s = newScenario("fenced");
            insertRows(s, 0, 2);
            drainWalQueue();
            engine.releaseAllWalWriters();
            publishSavePoint(s);
            markDurableBaseline();
            tracker(s).fenceDurableFrontier(CairoException.dataSyncFailureCarry(EIO, "fdatasync"));
            faultFf.freezeDurableContentUnder(tableDir(s).resolve(WalUtils.SEQ_DIR));
            tryInsertRows(s, 1, 2, false);
            drainWalQueue();
            try {
                publishSavePoint(s);
            } catch (CairoException | CairoError ignore) {
                // A fenced frontier may refuse the save point.
            }
            powerLossAndCheck(s, "save point attempted while the durable frontier is fenced");
            assertNoRowFailures();
        });
    }

    /**
     * The apply job stops on its time quota, saves the WAL lag in {@code _txn} and takes a save point on that
     * {@code _txn}; a clean restart follows while the lag is held, the next commit consumes the lag and
     * finishes, no newer save point follows, and the process is killed. Recovery restores the save point, whose
     * {@code _txn} counts the lag rows, and the consuming commit has rewritten their bytes in place. Every row
     * of both WAL transactions must read back with its value.
     */
    @Test
    public void testSavedLagSurvivesRestartAndKillAfterConsumingCommit() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        runWithCrashFacade(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final String rows = "insert into %s select timestamp_sequence('2022-02-25T01:00', 1_000_000L), x, 'k' || (x %% 7), 'r' || x from long_sequence(1_000)";
            execute("create table lag (ts timestamp, v long, s symbol, vc varchar) timestamp(ts) partition by day wal with maxUncommittedRows=50");
            final TableToken tt = engine.verifyTableName("lag");
            execute("insert into lag values ('2022-02-25', 1_000_000, 'c', 'c1')");
            drainWalQueue();
            // 1,000 rows, the lag limit (maxUncommittedRows times the lag multiplier, 20), then one row below
            // them through a second WAL: one apply pass applies the first alone and saves its rows as lag
            execute(String.format(rows, "lag"));
            try (WalWriter ignore = engine.getWalWriter(tt)) {
                execute("insert into lag values ('2022-02-25T00:30', -1, 'neg', 'neg')");
            }
            tickWalQueue(1);
            Assert.assertEquals("_txn does not hold the saved WAL lag", 1_000, savedLagRows(tt));
            try (Path p = new Path(); TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade())) {
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
                txReader.ofRO(p.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                txReader.unsafeLoadAll();
                p.of(engine.getConfiguration().getDbRoot()).concat(tt);
                final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(
                        engine.getConfiguration().getFilesFacade(), p, p.size(), tt.getTableId());
                Assert.assertTrue("a save point must exist", saved.length > 0);
                Assert.assertEquals("the save point is not cut on the _txn that holds the lag", txReader.getTxn(), saved[0].epochTxn);
            }
            purgeWal();

            // clean restart while the lag is held, then no newer save point
            engine.releaseAllWriters();
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, EPOCH_INTERVAL_MS);
            drainWalQueue();
            Assert.assertEquals("the consuming commit left lag in _txn", 0, savedLagRows(tt));

            crashFf.takeKillImage(engine.getConfiguration().getDbRoot());
            recoverAfterCrash(new TableToken[]{tt});
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            execute("create table lag_expected (ts timestamp, v long, s symbol, vc varchar) timestamp(ts) partition by day bypass wal");
            execute("insert into lag_expected values ('2022-02-25', 1_000_000, 'c', 'c1')");
            execute(String.format(rows, "lag_expected"));
            execute("insert into lag_expected values ('2022-02-25T00:30', -1, 'neg', 'neg')");
            assertSqlCursors("select count(), sum(v) from lag_expected", "select count(), sum(v) from lag");
            assertSqlCursors("lag_expected", "lag");
            execute("drop table lag");
            execute("drop table lag_expected");
            drainWalQueue();
            releaseEngineHandles();
        });
    }

    /**
     * A power loss leaves the V1 sequencer log's header on the disk ahead of its newest records: the kernel wrote the
     * header page back, not the page with the three newest records. Recovery restores the table from its save point
     * and does not open the sequencer, so the check job's scan is the first to read the log. The scan must seed the
     * tracker with the txn the log keeps, the apply job must go idle, and the group flush must not report a commit
     * durable before its own flush: the frontier QWP acknowledges from stops at the flushed commit.
     */
    @Test
    public void testTornSequencerTailAfterRestore() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, String.valueOf(TORN_TAIL_WINDOW_US));
        // Only the explicit save point below, so the restart restores the table and replays the WAL after it.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            runWithCrashFacade(() -> {
                // A frozen clock: no commit reaches its group window, so only the writers' closes flush.
                setCurrentMicros(TORN_TAIL_CLOCK_START);
                execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName("t");
                try (WalWriter w = engine.getWalWriter(tt)) {
                    for (int i = 1; i <= 3; i++) {
                        commitTornTailRow(w, i);
                    }
                }
                drainWalQueue();
                try (TableWriter w = getWriter(tt)) {
                    w.advanceDurableEpoch(1L);
                }
                try (WalWriter w = engine.getWalWriter(tt)) {
                    for (int i = 4; i <= TORN_TAIL_KEPT; i++) {
                        commitTornTailRow(w, i);
                    }
                }
                drainWalQueue();
                markDurableBaseline();
                final WalWriter pending = engine.getWalWriter(tt);
                try {
                    for (int i = TORN_TAIL_KEPT + 1; i <= TORN_TAIL_KEPT + 3; i++) {
                        commitTornTailRow(pending, i);
                    }
                    crashFf.markFileDurable(sequencerLogPath(tt));
                    tearSequencerTail(tt);
                    powerLossDroppingPending(pending);
                } finally {
                    pending.close();
                }
                // CairoEngine.completeInit order: recovery, then the worker pools run the check job before any apply.
                simulateRestart(new TableToken[]{tt});
                new RecoveryCoordinator(engine).recover();
                new CheckWalTransactionsJob(engine).run();
                final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.assertEquals("recovery must restore the table", 1, tracker.getRecoveryIncarnation());
                Assert.assertEquals("the scan must seed the txn the log keeps", TORN_TAIL_KEPT, tracker.getSeqTxn());
                assertApplyGoesIdle(tt);

                setCurrentMicros(TORN_TAIL_CLOCK_START + 10 * TORN_TAIL_WINDOW_US);
                try (WalWriter flushed = engine.getWalWriter(tt)) {
                    commitTornTailRow(flushed, 5001);
                }
                final long flushedSeqTxn = engine.getTableSequencerAPI().lastTxn(tt);
                Assert.assertEquals(TORN_TAIL_KEPT + 1, flushedSeqTxn);
                try (WalWriter notFlushed = engine.getWalWriter(tt)) {
                    commitTornTailRow(notFlushed, 5002);
                    Assert.assertEquals(
                            "the durable frontier must stop at the flushed commit",
                            flushedSeqTxn,
                            engine.getDurableAckRegistry().getLocalDurableSeqTxn(tt.getDirName())
                    );
                }
                drainWalQueue();
                Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
                releaseEngineHandles();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setCurrentMicros(-1);
        }
    }

    /**
     * The NOSYNC form of {@link #testTornSequencerTailAfterRestore}. It lives here, with the adaptive form, because
     * both cover the WAL table scan every mode shares: NOSYNC flushes nothing, the kernel wrote back every page but the
     * one with the three newest records, and nothing opens the sequencer before the check job. The scan must seed the
     * tracker with the txn the log keeps, the table must not be suspended, and the apply job must go idle. Only the
     * torn log's sequencer is opened, once; an intact log is checked with its own reads, and a later scan reads its
     * header alone.
     */
    @Test
    public void testTornSequencerTailUnderNosync() throws Exception {
        assumeCrashHarnessSupported();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
        final SequencerFileCountingFacade ff = new SequencerFileCountingFacade();
        crashFf = ff;
        ff.setDbRoot(root);
        assertMemoryLeak(ff, () -> {
            execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("create table whole (ts timestamp, v long) timestamp(ts) partition by day wal");
            final TableToken tt = engine.verifyTableName("t");
            final TableToken whole = engine.verifyTableName("whole");
            try (WalWriter w = engine.getWalWriter(tt)) {
                for (int i = 1; i <= TORN_TAIL_KEPT; i++) {
                    commitTornTailRow(w, i);
                }
            }
            execute("insert into whole values (0, 1)");
            drainWalQueue();
            markDurableBaseline();
            final WalWriter pending = engine.getWalWriter(tt);
            try {
                for (int i = TORN_TAIL_KEPT + 1; i <= TORN_TAIL_KEPT + 3; i++) {
                    commitTornTailRow(pending, i);
                }
                markDurableBaseline();
                tearSequencerTail(tt);
                crashFf.powerLoss();
            } finally {
                pending.close();
            }
            // CairoEngine.completeInit order: recovery, then the worker pools run the check job before any apply.
            simulateRestart(new TableToken[]{tt, whole});
            new RecoveryCoordinator(engine).recover();
            ff.reset(tt, whole);
            new CheckWalTransactionsJob(engine).run();
            final SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
            Assert.assertEquals("the scan must seed the txn the log keeps", TORN_TAIL_KEPT, tracker.getSeqTxn());
            Assert.assertEquals(1, engine.getTableSequencerAPI().getTxnTracker(whole).getSeqTxn());
            Assert.assertEquals("the scan must open the torn log's sequencer once", 1, ff.tornSequencerOpens);
            Assert.assertEquals("the scan must not open an intact log's sequencer", 0, ff.wholeSequencerOpens);
            Assert.assertEquals("an intact log: one open", 1, ff.wholeLogOpens);
            Assert.assertEquals("an intact log: the header and its unflushed records", 2, ff.wholeLogReads);
            Assert.assertEquals("an intact log: one close", 1, ff.wholeLogCloses);
            Assert.assertFalse("must not be suspended: " + tracker.getErrorMessage(), tracker.isSuspended());
            assertApplyGoesIdle(tt);

            // Later scans, with the sequencers released, open no sequencer and read each header alone.
            engine.releaseInactiveTableSequencers();
            ff.reset(tt, whole);
            new CheckWalTransactionsJob(engine).run();
            engine.getTableSequencerAPI().forAllWalTables(new ObjHashSet<>(), true, (tableId, token, lastTxn) -> {
            });
            Assert.assertEquals(0, ff.tornSequencerOpens);
            Assert.assertEquals(0, ff.wholeSequencerOpens);
            Assert.assertEquals(2, ff.wholeLogOpens);
            Assert.assertEquals(2, ff.wholeLogReads);
            Assert.assertEquals(2, ff.wholeLogCloses);

            try (WalWriter w = engine.getWalWriter(tt)) {
                commitTornTailRow(w, 1000);
            }
            drainWalQueue();
            Assert.assertEquals(TORN_TAIL_KEPT + 1, tracker.getWriterTxn());
            Assert.assertEquals(TORN_TAIL_KEPT + 1, tracker.getSeqTxn());
            final String expectedCount = "count\n" + (TORN_TAIL_KEPT + 1) + "\n";
            assertQuery("select count() from t").noRandomAccess().expectSize().returns(expectedCount);
            releaseEngineHandles();
        });
    }

    private static void commitTornTailRow(WalWriter w, long i) {
        final TableWriter.Row row = w.newRow(DAY0 + i * 1_000_000L);
        row.putLong(1, i);
        row.append();
        w.commit();
    }

    private static long nowMs() {
        return engine.getConfiguration().getMicrosecondClock().getTicks() / 1000L;
    }

    private static java.nio.file.Path tableDir(Scenario s) {
        return Paths.get(engine.getConfiguration().getDbRoot().toString(), s.token.getDirName());
    }

    private static Map<String, byte[]> tableFiles(Scenario s) {
        final java.nio.file.Path dir = tableDir(s);
        final Map<String, byte[]> files = new TreeMap<>();
        try (Stream<java.nio.file.Path> stream = Files.walk(dir)) {
            stream.filter(Files::isRegularFile).forEach(p -> {
                try {
                    files.put(dir.relativize(p).toString(), Files.readAllBytes(p));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return files;
    }

    private static SeqTxnTracker tracker(Scenario s) {
        return engine.getTableSequencerAPI().getTxnTracker(s.token);
    }

    private static long tsOf(int i) {
        return 1_700_000_000_000_000L + (long) i * 1_000_000L;
    }

    private static String vOf(int i) {
        // More than 9 bytes, so it takes the split (out-of-line) varchar layout (data and aux both exercised).
        return "epoch-varchar-payload-" + String.format("%06d", i);
    }

    // A bounded count, not a drain: a tracker above the log re-queues the table on every run.
    private void assertApplyGoesIdle(TableToken tt) {
        engine.notifyWalTxnRepublisher(tt);
        try (ApplyWal2TableJob apply = createWalApplyJob()) {
            int runs = 0;
            while (apply.run()) {
                Assert.assertTrue("the apply job must go idle", ++runs < 10);
            }
        }
    }

    /**
     * Varchar analogue of {@link AbstractCrashConsistencyTest#assertNoSilentCorruption}: the rows that read back
     * match the expected prefix, or a visible error is thrown. Fewer rows (rollback) is acceptable; a wrong value is
     * not.
     */
    private void assertNoSilentCorruptionVarchar(String table, String column) {
        try {
            List<String> actual = readVarcharColumn(table, column);
            int n = Math.min(actual.size(), N);
            for (int i = 0; i < n; i++) {
                Assert.assertEquals("row " + i + " silently wrong", vOf(i), actual.get(i));
            }
        } catch (io.questdb.cairo.CairoException | io.questdb.cairo.CairoError e) {
            // Acceptable: corruption detected visibly.
        } catch (InternalError e) {
            // SIGBUS (mmap past truncated file) -> InternalError; acceptable visible detection.
        } catch (RuntimeException e) {
            if (!(e.getCause() instanceof io.questdb.cairo.CairoException)
                    && !(e.getCause() instanceof io.questdb.cairo.CairoError)) {
                throw e;
            }
        }
    }

    private void assertCreationBaseline(TableToken tt) {
        assertMarkerSeqTxn(tt, 0);
        assertFilePresent(tt, io.questdb.cairo.DurableEpochManifest.FILE_NAME + ".0");
    }

    /**
     * An O3 merge writes a new version of the first day, then the header sync of the save point that names it fails
     * and leaves the header only in the page cache. A restart without a power loss selects that save point, and a
     * writer open, or VACUUM TABLE, runs the partition purge before the power loss. Whichever save point recovery then
     * selects, the partition versions it names must be there.
     *
     * @param isRestartedFirst the process restarts and purges before the power loss; otherwise the power fails at once
     * @param isWriterOpened   with a restart: the WAL apply and a writer open run before the purge; otherwise VACUUM
     */
    private void assertFailedHeaderSyncKeepsData(boolean isRestartedFirst, boolean isWriterOpened) throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            final Scenario s = newScenario("hdr");
            insertRows(s, 0, 2);
            insertRows(s, 1, 2);
            drainWalQueue();
            engine.releaseAllWalWriters();
            publishSavePoint(s);
            markDurableBaseline();
            insertRows(s, 0, 2, true, false, true);
            drainWalQueue();
            engine.releaseAllReaders();
            publishSavePointWithFailedHeaderSync(s);
            if (isRestartedFirst) {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                restart(s);
                new RecoveryCoordinator(engine).recover();
                if (isWriterOpened) {
                    engine.notifyWalTxnRepublisher(s.token);
                    drainWalQueue();
                    try (TableWriter w = getWriter(s.token)) {
                        Assert.assertNotNull(w);
                    }
                    engine.releaseAllWriters();
                } else {
                    execute("VACUUM TABLE " + s.table);
                }
                engine.releaseAllReaders();
                purgePartitions();
                // the filesystem journals the directory changes on its own, as ext4 does within seconds
                try (Path path = new Path()) {
                    path.of(root).concat(s.token.getDirName());
                    faultFf.fsyncAndClose(faultFf.openRO(path.$()));
                }
            }
            powerLossAndCheck(s, isRestartedFirst ? (isWriterOpened ? "restart, writer open, purge" : "restart, VACUUM, purge") : "power loss");
            assertNoRowFailures();
        });
    }

    /**
     * Rows appended in a second WAL, then the header sync of the save point that covers them fails and leaves the
     * header only in the page cache. A restart without a power loss selects that save point. Before the power loss,
     * either the WAL purge runs (no partition version is superseded, so a writer open has nothing to purge), or the
     * next save point dies at its own header sync, after it has overwritten the other generation. Whichever save
     * point recovery then selects must be valid on the device and still have its WAL.
     *
     * @param isWriterOpened      the WAL apply and a writer open run before the WAL purge
     * @param isNextSavePointTorn the next save point dies at its header sync instead of the WAL purge running
     */
    private void assertFailedHeaderSyncThenRestartKeepsData(boolean isWriterOpened, boolean isNextSavePointTorn) throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            final Scenario s = newScenario("hdrw");
            insertRows(s, 0, 2);
            insertRows(s, 1, 2);
            drainWalQueue();
            engine.releaseAllWalWriters();
            publishSavePoint(s);
            markDurableBaseline();
            insertRows(s, 1, 2);
            drainWalQueue();
            engine.releaseAllWalWriters();
            engine.releaseAllReaders();
            publishSavePointWithFailedHeaderSync(s);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            engine.releaseAllWalWriters();
            restart(s);
            new RecoveryCoordinator(engine).recover();
            if (isWriterOpened) {
                engine.notifyWalTxnRepublisher(s.token);
                drainWalQueue();
                try (TableWriter w = getWriter(s.token)) {
                    Assert.assertNotNull(w);
                }
                engine.releaseAllWriters();
            }
            if (isNextSavePointTorn) {
                engine.notifyWalTxnRepublisher(s.token);
                drainWalQueue();
                engine.releaseAllReaders();
                publishSavePointWithFailedHeaderSync(s);
                powerLoss(s);
                Assert.assertTrue("the power loss must leave a valid save point on the device", savePointCount(s) > 0);
                restartAndCheck(s, "restart, next save point torn", false);
            } else {
                purgeWal();
                // the filesystem journals the directory changes on its own, as ext4 does within seconds
                try (Path path = new Path()) {
                    path.of(root).concat(s.token.getDirName());
                    faultFf.fsyncAndClose(faultFf.openRO(path.$()));
                }
                powerLossAndCheck(s, isWriterOpened ? "restart, writer open, WAL purge" : "restart, WAL purge");
            }
            assertNoRowFailures();
        });
    }

    private void assertFilePresent(TableToken tt, String fileName) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName);
            Assert.assertTrue(fileName + " must exist",
                    engine.getConfiguration().getFilesFacade().exists(p.$()));
        }
    }

    private void assertMarkerSeqTxn(TableToken tt, long expectedSeqTxn) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt);
            final DurableEpochManifest.Candidate[] saved = DurableEpochManifest.loadCandidates(
                    engine.getConfiguration().getFilesFacade(), p, p.size(), tt.getTableId());
            Assert.assertTrue("a save point must exist", saved.length > 0);
            Assert.assertEquals("save point seqTxn", expectedSeqTxn, saved[0].epochSeqTxn);
        }
    }

    private void assertNoRowFailures() {
        if (!rowFailures.isEmpty()) {
            Assert.fail(rowFailures.size() + " of " + rowCount + " rows failed:\n" + String.join("\n", rowFailures));
        }
    }

    // Compares the table with the rows its scenario committed: every covered row reads back with its value, and any
    // other row is one whose commit was not covered.
    private String checkRows(Scenario s) {
        final Map<Long, Long> actual = new TreeMap<>();
        try (RecordCursorFactory factory = select("select ts, v from " + s.table)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    if (actual.put(record.getTimestamp(0), record.getLong(1)) != null) {
                        return "row at ts=" + record.getTimestamp(0) + " reads back twice";
                    }
                }
            }
        } catch (Throwable th) {
            return "read failed: " + th;
        }
        for (Map.Entry<Long, Long> e : s.expected.entrySet()) {
            final Long value = actual.get(e.getKey());
            if (!e.getValue().equals(value)) {
                return "row v=" + e.getValue() + " at ts=" + e.getKey() + " reads back as " + value
                        + " [expected=" + s.expected.size() + ", actual=" + actual.size() + ']';
            }
        }
        for (Map.Entry<Long, Long> e : actual.entrySet()) {
            if (!s.expected.containsKey(e.getKey())
                    && !e.getValue().equals(s.optional.get(e.getKey()))
                    && !e.getValue().equals(s.unsynced.get(e.getKey()))) {
                return "unexpected row v=" + e.getValue() + " at ts=" + e.getKey();
            }
        }
        return null;
    }

    // A stop and a start in the given mode: the sequencer, its WAL id generator and the writers reopen under it.
    private void cleanRestart(Scenario s, String mode) {
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseAllWalWriters();
        setCommitMode(mode);
        restart(s);
        new RecoveryCoordinator(engine).recover();
    }

    private void configureSavePoints() {
        // A writer close takes no save point, and only the steps of a row take one.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_MAX_ROWS, Long.MAX_VALUE);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setCommitMode("adaptive");
        Assert.assertEquals(0, engine.getConfiguration().getAdaptiveCommitGroupWindowUs());
        Assert.assertFalse(engine.getConfiguration().isAdaptiveEpochFlushOnClose());
    }

    private boolean crashAfterRemoval(Scenario s, int removal, Action action) throws Exception {
        faultFf.armCrashAfterRemoval(removal);
        try {
            action.run();
        } catch (Throwable th) {
            if (faultFf.isRemovalCrashArmed()) {
                throw th;
            }
        }
        final boolean crashed = !faultFf.isRemovalCrashArmed();
        s.firedAt = crashed ? faultFf.removedPath : null;
        faultFf.disarm();
        return crashed;
    }

    private boolean crashAtOperation(Scenario s, int op, Action action) throws Exception {
        faultFf.armCrashAt(faultFf.durabilityOpCount() + op);
        try {
            action.run();
        } catch (Throwable th) {
            if (faultFf.isCrashArmed()) {
                throw th;
            }
        }
        final boolean crashed = !faultFf.isCrashArmed();
        s.firedAt = crashed ? faultFf.lastDurabilityOp() : null;
        faultFf.disarm();
        return crashed;
    }

    private void dropScenario(Scenario s) {
        try {
            execute("drop table " + s.table);
            drainWalQueue();
            purgeWal();
        } catch (Throwable th) {
            LOG.error().$("could not drop scenario table [table=").$(s.table).$(", error=").$(th).I$();
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseAllWalWriters();
    }

    // Flushes the names of the partition directories and of the files in them, as a filesystem that journals
    // directory entries (ext4, XFS) does; SYNC itself, as in base, makes no directory entry durable.
    private void fsyncPartitionDirectories(Scenario s) throws IOException {
        try (Stream<java.nio.file.Path> stream = Files.list(tableDir(s)); Path dir = new Path()) {
            for (java.nio.file.Path partition : stream.filter(Files::isDirectory).filter(d -> d.getFileName().toString().startsWith("2024")).toList()) {
                TableUtils.fsyncDirDurable(faultFf, dir.of(partition.toString()).$());
            }
            TableUtils.fsyncDirDurable(faultFf, dir.of(tableDir(s).toString()).$());
        }
    }

    private void insertRows(Scenario s, int day, int count) {
        insertRows(s, day, count, true, false, false);
    }

    // Commits one WAL transaction. A row is expected back after a power loss only when its commit succeeded under
    // a durable mode and `covered` says the durable frontier covers it. Rows at the day's start precede every row
    // inserted otherwise.
    private void insertRows(Scenario s, int day, int count, boolean covered, boolean mayFail, boolean atDayStart) {
        final StringBuilder sql = new StringBuilder("insert into ").append(s.table).append("(ts, v) values ");
        final long[] ts = new long[count];
        final long[] v = new long[count];
        for (int i = 0; i < count; i++) {
            v[i] = ++nextRowId;
            ts[i] = DAY0 + day * DAY + v[i] * (atDayStart ? 1L : 1_000_000L);
            sql.append(i > 0 ? ", " : "").append('(').append(ts[i]).append(", ").append(v[i]).append(')');
        }
        boolean committed = false;
        try {
            execute(sql);
            committed = true;
        } catch (Throwable th) {
            if (!mayFail || th instanceof AssertionError) {
                throw new AssertionError(th);
            }
        }
        final Map<Long, Long> target = !committed || !covered
                ? s.optional
                : "nosync".equals(commitMode) ? s.unsynced : s.expected;
        for (int i = 0; i < count; i++) {
            target.put(ts[i], v[i]);
        }
    }

    private void markWalAndSequencerDurable(TableToken token) throws Exception {
        final java.nio.file.Path tableDir = Paths.get(configuration.getDbRoot().toString(), token.getDirName());
        try (Stream<java.nio.file.Path> files = Files.walk(tableDir)) {
            files.filter(Files::isRegularFile).forEach(p -> {
                final String rel = tableDir.relativize(p).toString();
                if (rel.startsWith("wal") || rel.startsWith("txn_seq")) {
                    faultFf.markFileDurable(p.toAbsolutePath().toString());
                }
            });
        }
    }

    private Scenario newScenario(String table) throws Exception {
        final Scenario s = new Scenario(table, new HashSet<>(faultFf.noCacheOpenFdsSnapshot()));
        execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        s.token = engine.verifyTableName(table);
        // No periodic save point unless a row's step takes one.
        tracker(s).setLastEpochTs(nowMs());
        return s;
    }

    private String partitionDirs(Scenario s) throws IOException {
        try (Stream<java.nio.file.Path> stream = Files.list(tableDir(s))) {
            return stream.map(p -> p.getFileName().toString()).filter(n -> n.startsWith("2024")).sorted().toList().toString();
        }
    }

    private void powerLoss(Scenario s) {
        faultFf.powerOff();
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        engine.releaseCrashOrphanedWriters();
        engine.releaseCrashOrphanedWalWriters();
        engine.releaseAllWalWriters();
        engine.releaseInactiveTableSequencers();
        engine.releaseAllWriters();
        engine.releaseCrashOrphanedWriters();
        reclaimLingeringNonCacheFds(s.fds);
        faultFf.crash(engine.getConfiguration().getDbRoot());
        restart(s);
    }

    private void powerLossAndCheck(Scenario s, String label) {
        restartAndCheck(s, label, true);
    }

    private void publishSavePoint(Scenario s) {
        try (TableWriter w = getWriter(s.token)) {
            w.advanceDurableEpoch(nowMs());
        }
    }

    // The header sync fails and the header stays only in the page cache; the engine stops, and the next process
    // starts without the failure.
    private void publishSavePointWithFailedHeaderSync(Scenario s) {
        faultFf.isHeaderSyncDropped = true;
        Throwable failure = null;
        try {
            publishSavePoint(s);
        } catch (CairoException | CairoError th) {
            failure = th;
        }
        Assert.assertFalse("the header sync must have been reached", faultFf.isHeaderSyncDropped);
        Assert.assertNotNull("the failed header sync must fail the save point", failure);
        engine.resetDurabilityFailure();
    }

    private void purgePartitions() {
        try (O3PartitionPurgeJob job = new O3PartitionPurgeJob(engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (job.run()) {
            }
        }
    }

    // A WAL purge with a clock that always passes the purge interval, so the broad sweep runs.
    private void purgeWal() {
        engine.releaseAllWalWriters();
        final long step = engine.getConfiguration().getWalPurgeInterval() * 1000L + 1_000_000L;
        final long[] tick = {1L};
        final MicrosecondClock clock = () -> (tick[0] += step);
        try (WalPurgeJob job = new WalPurgeJob(engine, engine.getConfiguration().getFilesFacade(), clock)) {
            job.run();
            job.run();
        }
    }

    // The writer brings its save point in line with the local durability policy: a replica makes the table
    // durable and removes it, a primary publishes the baseline of a table that has none.
    private void reconcileSavePoint(Scenario s, LocalDurabilityPolicy policy) {
        engine.setLocalDurabilityPolicy(policy);
        try (TableWriter w = getWriter(s.token)) {
            w.reconcileDurableEpochAnchor();
        } finally {
            engine.setLocalDurabilityPolicy(LocalDurabilityPolicy.ALWAYS_ON);
        }
    }

    // Recovery must accept the state, a second recovery after a restart must change no file of the table, and the
    // table must then apply its WAL and read back every covered row.
    private String recoverAndCheck(Scenario s) {
        try {
            new RecoveryCoordinator(engine).recover();
        } catch (Throwable th) {
            return "recovery refused: " + th.getMessage();
        }
        // The steps production takes after recovery: the check job scans the WAL tables, then the first apply opens
        // the sequencer, which certifies its unsynced tail. A restored table first opens it there, a table at its
        // save point already during recovery: the second recovery is compared with the state the first process
        // reaches.
        new CheckWalTransactionsJob(engine).run();
        engine.getTableSequencerAPI().lastTxn(s.token);
        final Map<String, byte[]> afterFirst = tableFiles(s);
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        restart(s);
        try {
            new RecoveryCoordinator(engine).recover();
        } catch (Throwable th) {
            return "second recovery refused: " + th.getMessage();
        }
        final Map<String, byte[]> afterSecond = tableFiles(s);
        final List<String> changed = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : afterSecond.entrySet()) {
            if (!Arrays.equals(e.getValue(), afterFirst.get(e.getKey()))) {
                changed.add(e.getKey());
            }
        }
        for (String name : afterFirst.keySet()) {
            if (!afterSecond.containsKey(name)) {
                changed.add(name);
            }
        }
        if (!changed.isEmpty()) {
            return "a second recovery changed " + changed;
        }
        engine.notifyWalTxnRepublisher(s.token);
        drainWalQueue();
        if (engine.getTableSequencerAPI().isSuspended(s.token)) {
            return "table suspended after recovery, " + checkRows(s);
        }
        return checkRows(s);
    }

    // What a fresh process starts with: no suspension, a new scoreboard and tracker, no data-sync failure.
    private void restart(Scenario s) {
        if (engine.getTableSequencerAPI().isSuspended(s.token)) {
            tracker(s).setUnsuspended();
        }
        engine.getTxnScoreboardPool().remove(s.token);
        engine.getTableSequencerAPI().resetForReboot(s.token);
        if (engine.getTableSequencerAPI().isSuspended(s.token)) {
            tracker(s).setUnsuspended();
        }
        engine.resetDurabilityFailure();
    }

    // Judges one row: recovery after a power loss (or a kill, which keeps every file as it stands), the rows, a
    // write after recovery, then a second power loss.
    private void restartAndCheck(Scenario s, String label, boolean powerLoss) {
        rowCount++;
        String failure;
        try {
            if (powerLoss) {
                powerLoss(s);
            } else {
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                restart(s);
            }
            failure = recoverAndCheck(s);
            if (failure == null) {
                insertRows(s, 3, 1);
                drainWalQueue();
                failure = checkRows(s);
                if (failure != null) {
                    failure = "after a write that followed recovery, " + failure;
                }
            }
            // A nosync instance promises nothing for the state it applied after recovery.
            if (failure == null && !"nosync".equals(commitMode)) {
                if ("sync".equals(commitMode)) {
                    // The write above may have created partitions. This row checks the recovered state, not
                    // whether SYNC makes their names durable, which it does not promise.
                    fsyncPartitionDirectories(s);
                }
                powerLoss(s);
                failure = recoverAndCheck(s);
                if (failure != null) {
                    failure = "after a second power loss, " + failure;
                }
            }
        } catch (Throwable th) {
            failure = "unexpected " + th;
        }
        final int trackedFiles = faultFf.trackedFileCount();
        LOG.info().$("save point crash row [table=").$(s.table).$(", row=").$(label).$(", firedAt=").$(s.firedAt)
                .$(", expected=").$(s.expected.size()).$(", optional=").$(s.optional.size())
                .$(", result=").$(failure == null ? "ok" : failure).$(", trackedFiles=").$(trackedFiles).I$();
        if (failure != null) {
            rowFailures.add(s.table + " [" + label + ", at " + s.firedAt + "]: " + failure);
        }
        dropScenario(s);
    }

    private void runWithFaultFacade(TestUtils.LeakProneCode body) throws Exception {
        assumeCrashHarnessSupported();
        faultFf = new SavePointFaultFacade();
        crashFf = faultFf;
        faultFf.setDbRoot(root);
        assertMemoryLeak(faultFf, body);
    }

    private int savePointCount(Scenario s) {
        try (Path p = new Path()) {
            p.of(configuration.getDbRoot()).concat(s.token);
            return DurableEpochManifest.loadCandidates(configuration.getFilesFacade(), p, p.size(), s.token.getTableId()).length;
        }
    }

    private int savedLagRows(TableToken tt) {
        try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade()); Path path = new Path()) {
            path.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
            txReader.unsafeLoadAll();
            return txReader.getLagRowCount();
        }
    }

    private String sequencerLogPath(TableToken tt) {
        final String dbRoot = engine.getConfiguration().getDbRoot().toString();
        return Paths.get(dbRoot, tt.getDirName(), WalUtils.SEQ_DIR, WalUtils.TXNLOG_FILE_NAME).toString();
    }

    // Switches the instance mode and checks the configuration reports it. The test harness drops a pending
    // configuration change when a property is set again to the value it already has, so the epoch interval, set
    // last, takes a value no earlier switch used.
    private void setCommitMode(String mode) {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, mode);
        final long epochIntervalMs = EPOCH_INTERVAL_MS + ++modeSwitches;
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, epochIntervalMs);
        Assert.assertEquals(mode, CommitMode.toString(engine.getConfiguration().getCommitMode()));
        Assert.assertEquals(epochIntervalMs, engine.getConfiguration().getAdaptiveEpochIntervalMs());
        commitMode = mode;
    }

    // The records after the kept ones are zero on the disk: the page that holds them was not written back.
    private void tearSequencerTail(TableToken tt) {
        try (Path p = new Path()) {
            p.of(sequencerLogPath(tt));
            final long recordSize = TableTransactionLogV1.RECORD_SIZE;
            crashFf.tornTail(p.$(), TableTransactionLogFile.HEADER_SIZE + TORN_TAIL_KEPT * recordSize, 3 * recordSize);
        }
    }

    private void tryInsertRows(Scenario s, int day, int count, boolean covered) {
        insertRows(s, day, count, covered, true, false);
    }

    // After a crash and before the power loss: the commits of the rows a save point covers were acknowledged, so
    // their WAL and sequencer records are durable, and the kernel wrote the live _txn and _cv back on its own. A
    // save point that names a state before its columns are durable then loses those rows.
    private void writeBackLiveState(Scenario s) throws Exception {
        markWalAndSequencerDurable(s.token);
        final String tableDir = tableDir(s).toString();
        faultFf.markFileDurable(tableDir + File.separator + TableUtils.TXN_FILE_NAME);
        faultFf.markFileDurable(tableDir + File.separator + TableUtils.COLUMN_VERSION_FILE_NAME);
    }

    @FunctionalInterface
    private interface Action {
        void run() throws Exception;
    }

    /**
     * Adds to the crash model: power off once a crash fires, so nothing after it becomes durable; a crash after the
     * n-th file deletion; a data-sync fault reported after a durability operation took effect; and files whose
     * durable content stays what it was, as after a failed flush whose pages the kernel dropped.
     */
    private static final class SavePointFaultFacade extends CrashFaultFilesFacade {
        private final Map<String, byte[]> frozenContent = new HashMap<>();
        String faultedOp;
        boolean isHeaderSyncDropped;
        String removedPath;
        private int faultAtOp = -1;
        private int ioCalls;
        private int ioFaultAt = -1;
        private boolean poweredOff;
        private int removalCrashAt = -1;
        private int removals;

        @Override
        public void crash(CharSequence dbRoot) {
            super.crash(dbRoot);
            for (Map.Entry<String, byte[]> e : frozenContent.entrySet()) {
                final java.nio.file.Path file = Paths.get(e.getKey());
                if (Files.exists(file)) {
                    try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
                        ch.write(ByteBuffer.wrap(e.getValue()), 0);
                        ch.truncate(e.getValue().length);
                        ch.force(true);
                    } catch (IOException ex) {
                        throw new UncheckedIOException(ex);
                    }
                }
            }
            frozenContent.clear();
            disarm();
            poweredOff = false;
        }

        @Override
        public void fdatasync(long fd) {
            if (!poweredOff) {
                try {
                    super.fdatasync(fd);
                } catch (CrashSimulationError e) {
                    poweredOff = true;
                    throw e;
                }
                faultAfter("fdatasync");
            }
        }

        @Override
        public void fsync(long fd) {
            if (!poweredOff) {
                try {
                    super.fsync(fd);
                } catch (CrashSimulationError e) {
                    poweredOff = true;
                    throw e;
                }
                faultAfter("fsync");
            }
        }

        @Override
        public void fsyncAndClose(long fd) {
            if (poweredOff) {
                close(fd);
                return;
            }
            try {
                super.fsyncAndClose(fd);
            } catch (CrashSimulationError e) {
                poweredOff = true;
                throw e;
            }
        }

        @Override
        public void fsyncDurable(long fd) {
            if (isHeaderSyncDropped && StackWalker.getInstance().walk(frames -> frames.anyMatch(
                    frame -> frame.getClassName().endsWith(".DurableEpochManifest") && frame.getMethodName().equals("write")))
            ) {
                // the header write stays in the page cache; the kernel marks the page clean after the failed
                // writeback, so the device keeps the old content until the file is written again
                isHeaderSyncDropped = false;
                final String path = pathOf(fd);
                final byte[] durable = durableContentOf(path);
                frozenContent.put(path, durable != null ? durable.clone() : new byte[0]);
                faultedOp = "fsyncDurable of the save point header";
                throw CairoException.dataSyncFailure(this, EIO, "fsyncDurable", "injected");
            }
            super.fsyncDurable(fd);
        }

        @Override
        public void msync(long addr, long len, boolean async) {
            if (!poweredOff) {
                try {
                    super.msync(addr, len, async);
                } catch (CrashSimulationError e) {
                    poweredOff = true;
                    throw e;
                }
                if (!async) {
                    faultAfter("msync");
                }
            }
        }

        @Override
        public long openRO(LPSZ name) {
            return ioFault("openRO", name) ? -1 : super.openRO(name);
        }

        @Override
        public long openRONoCache(LPSZ name) {
            return ioFault("openRONoCache", name) ? -1 : super.openRONoCache(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            return ioFault("openRW", name) ? -1 : super.openRW(name, opts);
        }

        @Override
        public long openRWExisting(LPSZ name) {
            return ioFault("openRWExisting", name) ? -1 : super.openRWExisting(name);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            final boolean removed = super.removeQuiet(name);
            if (removed && removalCrashAt > 0 && ++removals >= removalCrashAt) {
                removalCrashAt = -1;
                removedPath = Utf8String.newInstance(name).toString();
                poweredOff = true;
                powerLoss();
                throw new CrashSimulationError(durabilityOpCount());
            }
            return removed;
        }

        @Override
        public int syncFileRange(long fd, long offset, long nbytes, int flags) {
            if (poweredOff) {
                return 0;
            }
            try {
                return super.syncFileRange(fd, offset, nbytes, flags);
            } catch (CrashSimulationError e) {
                poweredOff = true;
                throw e;
            }
        }

        @Override
        public void syncfs(long fd) {
            if (!poweredOff) {
                try {
                    super.syncfs(fd);
                } catch (CrashSimulationError e) {
                    poweredOff = true;
                    throw e;
                }
                faultAfter("syncfs");
            }
        }

        @Override
        public boolean truncate(long fd, long size) {
            return !ioFault("truncate", null) && super.truncate(fd, size);
        }

        @Override
        public long write(long fd, long address, long len, long offset) {
            if (ioFault("write", null)) {
                return -1;
            }
            if (!frozenContent.isEmpty()) {
                frozenContent.remove(pathOf(fd));
            }
            return super.write(fd, address, len, offset);
        }

        void armCrashAfterRemoval(int removal) {
            removals = 0;
            removalCrashAt = removal;
        }

        void armFaultAt(int op) {
            faultAtOp = op;
        }

        // Fails the n-th open, write or truncate from now on, the way a full disk or a failed open does: no data-sync
        // failure, so the engine is not poisoned.
        void armIoFaultAt(int n) {
            ioCalls = 0;
            ioFaultAt = n;
        }

        void disarm() {
            armCrashAt(-1);
            faultAtOp = -1;
            ioFaultAt = -1;
            removalCrashAt = -1;
        }

        void freezeDurableContentUnder(java.nio.file.Path dir) {
            try (Stream<java.nio.file.Path> stream = Files.walk(dir)) {
                stream.filter(Files::isRegularFile).forEach(p -> {
                    final String key = p.toAbsolutePath().toString();
                    final byte[] durable = durableContentOf(key);
                    frozenContent.put(key, durable != null ? durable.clone() : new byte[0]);
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        String lastDurabilityOp() {
            final List<String> log = durabilityOpLog();
            return log.isEmpty() ? null : log.get(log.size() - 1);
        }

        boolean isFaultArmed() {
            return faultAtOp > 0;
        }

        boolean isIoFaultArmed() {
            return ioFaultAt > 0;
        }

        boolean isRemovalCrashArmed() {
            return removalCrashAt > 0;
        }

        void powerOff() {
            poweredOff = true;
            powerLoss();
        }

        private void faultAfter(String operation) {
            if (faultAtOp > 0 && durabilityOpCount() >= faultAtOp) {
                faultAtOp = -1;
                faultedOp = lastDurabilityOp();
                throw CairoException.dataSyncFailure(this, EIO, operation, "injected");
            }
        }

        private boolean ioFault(String operation, LPSZ name) {
            if (ioFaultAt > 0 && ++ioCalls >= ioFaultAt) {
                ioFaultAt = -1;
                faultedOp = operation + (name != null ? " " + Utf8String.newInstance(name) : "");
                return true;
            }
            return false;
        }
    }

    private static final class Scenario {
        final Map<Long, Long> expected = new TreeMap<>();
        final Set<Long> fds;
        final Map<Long, Long> optional = new TreeMap<>();
        final String table;
        final Map<Long, Long> unsynced = new TreeMap<>();
        String firedAt;
        TableToken token;

        Scenario(String table, Set<Long> fds) {
            this.table = table;
            this.fds = fds;
        }

        void markUnsyncedRowsDurable() {
            expected.putAll(unsynced);
            unsynced.clear();
        }
    }

    // Counts the sequencer files the WAL table scan touches: the opens of _txnlog.meta.i, one per sequencer
    // construction, for the torn table, and those plus the opens, reads and closes of _txnlog for the intact one.
    private static final class SequencerFileCountingFacade extends CrashFaultFilesFacade {
        int tornSequencerOpens;
        int wholeLogCloses;
        int wholeLogOpens;
        int wholeLogReads;
        int wholeSequencerOpens;
        private String tornMetaIndex;
        private String wholeLog;
        private String wholeMetaIndex;

        @Override
        public boolean close(long fd) {
            if (isWholeLog(fd)) {
                wholeLogCloses++;
            }
            return super.close(fd);
        }

        @Override
        public long openRO(LPSZ name) {
            countOpen(name);
            return super.openRO(name);
        }

        @Override
        public long openRONoCache(LPSZ name) {
            countOpen(name);
            return super.openRONoCache(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            countOpen(name);
            return super.openRW(name, opts);
        }

        @Override
        public long openRWNoCache(LPSZ name, int opts) {
            countOpen(name);
            return super.openRWNoCache(name, opts);
        }

        @Override
        public long read(long fd, long buf, long len, long offset) {
            if (isWholeLog(fd)) {
                wholeLogReads++;
            }
            return super.read(fd, buf, len, offset);
        }

        @Override
        public int readNonNegativeInt(long fd, long offset) {
            if (isWholeLog(fd)) {
                wholeLogReads++;
            }
            return super.readNonNegativeInt(fd, offset);
        }

        @Override
        public long readNonNegativeLong(long fd, long offset) {
            if (isWholeLog(fd)) {
                wholeLogReads++;
            }
            return super.readNonNegativeLong(fd, offset);
        }

        void reset(TableToken torn, TableToken whole) {
            final String seq = File.separator + WalUtils.SEQ_DIR + File.separator;
            tornMetaIndex = torn.getDirName() + seq + WalUtils.TXNLOG_FILE_NAME_META_INX;
            wholeMetaIndex = whole.getDirName() + seq + WalUtils.TXNLOG_FILE_NAME_META_INX;
            wholeLog = whole.getDirName() + seq + WalUtils.TXNLOG_FILE_NAME;
            tornSequencerOpens = 0;
            wholeSequencerOpens = 0;
            wholeLogOpens = 0;
            wholeLogReads = 0;
            wholeLogCloses = 0;
        }

        private void countOpen(LPSZ name) {
            if (wholeLog != null) {
                // The name may carry its terminating zero.
                final String path = Utf8String.newInstance(name).toString().replace("\0", "");
                if (path.endsWith(tornMetaIndex)) {
                    tornSequencerOpens++;
                } else if (path.endsWith(wholeMetaIndex)) {
                    wholeSequencerOpens++;
                } else if (path.endsWith(wholeLog)) {
                    wholeLogOpens++;
                }
            }
        }

        private boolean isWholeLog(long fd) {
            final String path = pathOf(fd);
            return wholeLog != null && path != null && path.endsWith(wholeLog);
        }
    }
}
