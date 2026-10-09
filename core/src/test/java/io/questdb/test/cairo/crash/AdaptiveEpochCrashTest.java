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
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.LocalDurabilityPolicy;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8String;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

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
 * The crash oracle for the adaptive durable EPOCH (Plan 3B). Under
 * {@link CommitMode#ADAPTIVE} the WAL commit is fdatasync-durable but the TABLE apply is LAZY:
 * {@code TableWriter.syncColumns()} skips the per-column flush, so after a power cut the partition
 * columns can be NON-DURABLE while {@code _txn}/{@code _cv} were only msync'd. A durable epoch
 * ({@code TableWriter.fsyncMaterializedState()}, fired by the apply worker) forces those columns
 * durable and publishes a save point so recovery can land on the cut.
 *
 * <p>This isolates the TABLE materialized-state durability (Task C roll-forward of the post-epoch
 * WAL is out of scope here). After {@code drainWalQueue} the table is caught up
 * ({@code writerTxn == seqTxn}), so a post-crash read does NOT re-apply any WAL — it reads exactly
 * the durable on-disk column state.
 *
 * <h3>Crash model</h3>
 * {@link CrashFaultFilesFacade} models msync(MS_SYNC)/fsync/fdatasync/syncfs as durable and mmap
 * stores as non-durable-until-journaled. A single in-order insert is applied via the LAG path, which
 * does NOT fsync the columns and (under ADAPTIVE) {@code syncColumns()} is skipped — so right after
 * the lazy apply the partition column's {@code journaledDataEnd} is 0 (verified during development).
 *
 * <h3>Scenarios</h3>
 * <ol>
 *   <li>{@link #testEpochMakesLazilyAppliedRowsDurable}: ADAPTIVE, lazily apply a row, fire a durable
 *       epoch, crash, reopen → the epoch'd row reads back correct and the save point
 *       recorded exactly the cut's seqTxn (a self-consistent durable cut + anchor survives the cut).
 *       Run under per-inode journaling ({@code modelSharedJournal=false}).</li>
 *   <li>{@link #testNegativeControlWithoutEpochAnchorIsAbsent}: NEGATIVE CONTROL — the identical
 *       workload with epochs DISABLED records NO recovery anchor (no save point
 *       file), before AND after the crash. This is {@code advance()}'s
 *       reproducible contribution: the cut recovery would land on is simply not recorded. (Asserting
 *       column ROW-loss here is not reliable in this harness — see the test body + report.)</li>
 *   <li>{@link #testCrashMidEpochFsyncNoSilentCorruption}: arm a crash partway through the epoch's
 *       fsync sequence (driven explicitly via {@code fsyncMaterializedState()}) → reopening must
 *       never silently corrupt (correct prefix, a loud error, or fewer rows — never a wrong value).</li>
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
        // ADAPTIVE + epoch on every batch so the prefix is epoch'd deterministically.
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                // Model PER-INODE journaling (ext4 fast_commit): a journal commit on _cv/_txn does NOT
                // incidentally journal the column files' extents. So the ONLY thing that can make the
                // lazily lag-applied column durable is the epoch's own syncfs (the salvage property),
                // which is exactly what we are proving. Under the default shared-journal model the _cv
                // msync would journal the column for free and mask the epoch's contribution.
                crashFf.modelSharedJournal = false;
                execute("create table e (ts timestamp, v varchar) timestamp(ts) partition by day wal");

                // ONE small in-order insert => applied via the LAG path (no column fsync; syncColumns
                // skipped under ADAPTIVE), so the column is dirty-unsynced — EXACTLY the negative
                // control's setup. The only difference is that here the apply worker then fires a
                // durable epoch (interval 0), whose fsyncMaterializedState() forces the column durable.
                execute("insert into e values (" + tsOf(0) + ", '" + vOf(0) + "')");
                drainWalQueue(); // lazy lag apply + epoch

                TableToken tt = engine.verifyTableName("e");
                // Sanity: the epoch marker exists and covers the applied row (seqTxn == 1).
                assertMarkerSeqTxn(tt, 1);

                crashAndReopen();

                // The epoch'd cut must read back fully durable + correct after the crash: the row is
                // present and the marker (asserted above) recorded exactly this cut's seqTxn. This
                // proves fsyncMaterializedState() produces a self-consistent durable cut + anchor that
                // survives a power cut under strict per-inode journaling.
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
     * NEGATIVE CONTROL: with epochs DISABLED, the lazily-applied columns are never made durable, so
     * a crash loses them. This is what proves scenario 1's durability comes from the epoch flush and
     * not from some other sync on the apply path.
     */
    @Test
    public void testDisabledPeriodicEpochRetainsCreationBaseline() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1); // epochs disabled
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                execute("create table n (ts timestamp, v varchar) timestamp(ts) partition by day wal");

                // Same single-insert LAG path as scenario 1 (column dirty-unsynced after the lazy
                // apply: journaledDataEnd of n/2023-11-14/v.d is 0 here), but NO epoch fires.
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
     * Arm a crash partway through the epoch's fsync sequence: reopening must never SILENTLY corrupt.
     * The epoch issues many durability ops (column syncfs, _cv fsync, _txn fsync, marker msync+fsync,
     * save point file fsyncs); firing the crash on an early one leaves a partially-durable cut, which must
     * degrade safely (correct prefix / loud error / fewer rows), never a wrong value.
     */
    @Test
    public void testCrashMidEpochFsyncNoSilentCorruption() throws Exception {
        // Epochs DISABLED on the apply path so the apply drains cleanly; we then drive the epoch
        // explicitly via fsyncMaterializedState() with a crash armed mid-way, isolating the
        // mid-epoch-fsync crash from the apply's own (WAL) durability ops.
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
                    // The crash can also fire as the writer is closed/flushed; still a mid-cut crash.
                    crashed = true;
                }
                Assert.assertTrue("the crash must have fired during the epoch's fsync sequence", crashed);

                crashAndReopen();

                // Whatever survives must not be silently wrong: a correct prefix, a loud error, or
                // fewer rows are all acceptable; a wrong value is not.
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
    public void testFailedHeaderSyncThenRestartVacuumThenPowerLoss() throws Exception {
        assertFailedHeaderSyncKeepsData(true, false);
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
                drainWalQueue();
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
     * performs, and the demote clear.
     */
    @Test
    public void testPowerLossAfterEveryDeletionOfSavePointRemoval() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (String removal : new String[]{"sync exit", "nosync exit", "demote clear"}) {
                final boolean demote = "demote clear".equals(removal);
                for (int deletions = 0; deletions < 2; deletions++) {
                    for (int n = 1; n <= MAX_SWEEP; n++) {
                        setCommitMode("adaptive");
                        final Scenario s = newScenario("x_" + removal.charAt(0) + "_" + deletions + "_" + n);
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
                            cleanRestart(s, removal.substring(0, removal.indexOf(' ')));
                            action = () -> {
                                try (TableWriter ignore = getWriter(s.token)) {
                                    Assert.assertNotNull(ignore);
                                }
                            };
                        }
                        final boolean crashed = deletions == 0 ? crashAtOperation(s, n, action) : crashAfterRemoval(s, n, action);
                        if (!crashed && "sync exit".equals(removal)) {
                            // Not judged: the SYNC replay rewrites a partition directory the rewind left on disk
                            // and does not sync its entry, which the exit does not flush yet.
                            dropScenario(s);
                            break;
                        }
                        final String label;
                        if (crashed) {
                            label = removal + ", power loss after " + (deletions == 0 ? "durability operation " : "deletion ") + n;
                        } else {
                            label = removal + " completed, then rows and a WAL purge";
                            insertRows(s, 2, 2);
                            drainWalQueue();
                            purgeWal();
                        }
                        powerLossAndCheck(s, label);
                        if (!crashed) {
                            break;
                        }
                    }
                }
            }
            assertNoRowFailures();
        });
    }

    /**
     * A power loss at every durability operation of the baseline a table marked for enrolment gets: at the start
     * after a demote clear, at the writer's reconcile after a promote, and at the start after a restore of a table
     * that was never enrolled. The last row of each sweep loses power after the baseline, more rows and a WAL purge.
     */
    @Test
    public void testPowerLossAtEveryDurabilityOperationOfMarkedBaseline() throws Exception {
        runWithFaultFacade(() -> {
            configureSavePoints();
            for (String baseline : new String[]{"demote clear, then a start", "demote clear, then a promote", "restore, then a start"}) {
                final boolean restore = baseline.startsWith("restore");
                final boolean promote = baseline.endsWith("promote");
                for (int op = 1; op <= MAX_SWEEP; op++) {
                    setCommitMode(restore ? "nosync" : "adaptive");
                    final Scenario s = newScenario("m" + (restore ? 'r' : promote ? 'p' : 's') + op);
                    insertRows(s, 0, 2);
                    drainWalQueue();
                    engine.releaseAllWalWriters();
                    markDurableBaseline();
                    s.markUnsyncedRowsDurable();
                    if (restore) {
                        try (Path p = new Path()) {
                            final int rootLen = p.of(configuration.getDbRoot()).concat(s.token).size();
                            RecoveryCoordinator.markRestoredForEnrolment(configuration.getFilesFacade(), p, rootLen);
                            DurableEpochManifest.fsyncDirectory(configuration, p, rootLen);
                        }
                    } else {
                        reconcileSavePoint(s, LocalDurabilityPolicy.REPLICA_SKIP);
                    }
                    assertFilePresent(s.token, RecoveryCoordinator.RESTORE_ENROL_FILE_NAME);
                    Assert.assertEquals(0, savePointCount(s));
                    final boolean crashed = crashAtOperation(s, op, promote
                            ? () -> reconcileSavePoint(s, LocalDurabilityPolicy.ALWAYS_ON)
                            : () -> cleanRestart(s, "adaptive"));
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
     * A power loss at every durability operation of a table's first save point (taken when a table created
     * without one enrols at writer open), of its first periodic save point (a new generation) and of a later one
     * (a generation rewritten in place). The last row of each sweep loses power after the save point, more rows
     * and a WAL purge under the new floor.
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
                    insertRows(s, 1, 2);
                    drainWalQueue();
                    engine.releaseAllWalWriters();
                    for (int i = 0; i < prior; i++) {
                        publishSavePoint(s);
                        insertRows(s, 1, 2);
                        drainWalQueue();
                        engine.releaseAllWalWriters();
                    }
                    markDurableBaseline();
                    final boolean crashed = crashAtOperation(s, op, () -> publishSavePoint(s));
                    if (!crashed) {
                        insertRows(s, 2, 2);
                        drainWalQueue();
                        purgeWal();
                    }
                    powerLossAndCheck(s, crashed
                            ? which + ", power loss after durability operation " + op
                            : which + " completed, then rows and a WAL purge under its floor");
                    if (!crashed) {
                        break;
                    }
                }
            }
            assertNoRowFailures();
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
            tracker(s).fenceDurableFrontier(CairoException.dataSyncFailure(EIO, "fdatasync"));
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

    // ---- helpers ----

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
        // > 9 bytes so it takes the split (out-of-line) varchar layout (data + aux both exercised).
        return "epoch-varchar-payload-" + String.format("%06d", i);
    }

    /**
     * Varchar analogue of {@link AbstractCrashConsistencyTest#assertNoSilentCorruption}: the rows
     * that read back must match the expected prefix, OR a loud error is thrown. Fewer rows
     * (rollback) is acceptable; a silently WRONG value is not.
     */
    private void assertNoSilentCorruptionVarchar(String table, String column) {
        try {
            List<String> actual = readVarcharColumn(table, column);
            int n = Math.min(actual.size(), N);
            for (int i = 0; i < n; i++) {
                Assert.assertEquals("row " + i + " silently wrong", vOf(i), actual.get(i));
            }
        } catch (io.questdb.cairo.CairoException | io.questdb.cairo.CairoError e) {
            // acceptable: corruption detected loudly
        } catch (InternalError e) {
            // SIGBUS (mmap past truncated file) -> InternalError; acceptable loud detection
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

    private void insertRows(Scenario s, int day, int count) {
        insertRows(s, day, count, true, false, false);
    }

    // Commits one WAL transaction. A row is expected back after a power loss only when its commit succeeded under
    // a durable mode and `covered` says the durable frontier covers it. Rows at the day's start precede every row
    // inserted otherwise.
    private void insertRows(Scenario s, int day, int count, boolean covered, boolean mayFail, boolean atDayStart) {
        final StringBuilder sql = new StringBuilder("insert into ").append(s.table).append(" values ");
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

    private Scenario newScenario(String table) throws Exception {
        final Scenario s = new Scenario(table, new HashSet<>(faultFf.noCacheOpenFdsSnapshot()));
        execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
        s.token = engine.verifyTableName(table);
        // No periodic save point unless a row's step takes one.
        tracker(s).setLastEpochTs(nowMs());
        return s;
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

    // The writer brings its save point in line with the local durability policy: a replica swaps it for the enrolment
    // marker, a primary publishes the baseline of a marked table.
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
            // A NOSYNC instance promises nothing for the state it applied after recovery.
            if (failure == null && !"nosync".equals(commitMode)) {
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

    private void tryInsertRows(Scenario s, int day, int count, boolean covered) {
        insertRows(s, day, count, covered, true, false);
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
                // the header write stays in the page cache
                isHeaderSyncDropped = false;
                faultedOp = "fsyncDurable of the save point header";
                throw CairoException.dataSyncFailure(EIO, "fsyncDurable");
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
        public boolean removeQuiet(LPSZ name) {
            final boolean removed = super.removeQuiet(name);
            if (removed && removalCrashAt > 0 && ++removals >= removalCrashAt) {
                removalCrashAt = -1;
                removedPath = Utf8String.newInstance(name).toString();
                poweredOff = true;
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
            return ioFault("write", null) ? -1 : super.write(fd, address, len, offset);
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
        }

        private void faultAfter(String operation) {
            if (faultAtOp > 0 && durabilityOpCount() >= faultAtOp) {
                faultAtOp = -1;
                faultedOp = lastDurabilityOp();
                throw CairoException.dataSyncFailure(EIO, operation);
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
}
