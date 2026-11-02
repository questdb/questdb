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
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.DurableEpochManifest;
import io.questdb.cairo.O3PartitionPurgeJob;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cairo.wal.WalPurgeJob;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Recovery must never restore a damaged save point over the live files, and a crash anywhere inside the
 * writing of a save point must leave a state recovery accepts.
 *
 * <p>A save point holds byte copies of the live {@code _txn}, {@code _cv} and {@code _meta}. The damage rows
 * take a save point at a known cut, apply more rows lazily so the live files sit past it, damage one part of
 * the newest generation (a cut or garbage-filled copy, a flipped byte in each of the three copies, a torn
 * header, a header naming another cut), and require recovery to reject that generation, restore the
 * creation baseline and replay every row from the WAL.
 *
 * <p>The power-loss row crashes at every durability call of two consecutive save points (the first creates
 * a generation, the second overwrites the creation baseline in place) and requires recovery to accept what
 * is left, to keep every row, and to recover nothing on a second start. The restore row grows the live
 * {@code _meta} and {@code _cv} past their saved copies and requires the restore to leave each live file
 * byte-equal to its copy, so a second start counts no second recovery.
 */
public class AdaptiveRecoveryTornEpochCopyCrashTest extends AbstractAdaptiveCrashTest {

    private static final int K = 4; // rows before the epoch
    private static final int M = 5; // rows applied lazily AFTER the epoch (live _txn/_cv advance past it)
    private static final String RECOVERY_EVENTS = "questdb_wal_adaptive_recovery_events_total";

    /**
     * Recovery falls back to the older generation when the newest one is damaged on media or rejected on its
     * content, also after the WAL purge and the partition purge ran past the newest one. Both purges must keep what
     * the older generation needs, the WAL after its cut and the partition versions it names, after appends, after
     * an O3 rewrite and after a DROP PARTITION between the two save points; the WAL before the older cut still goes.
     */
    @Test
    public void testFallbackAfterPurgesKeepsEveryRow() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(() -> {
                int iteration = 0;
                for (boolean rejectedOnContent : new boolean[]{false, true}) {
                    for (String change : new String[]{"appends", "an O3 rewrite", "a DROP PARTITION"}) {
                        final boolean o3Rewrite = change.equals("an O3 rewrite");
                        final boolean dropped = change.equals("a DROP PARTITION");
                        final String context = (rejectedOnContent ? "rejected on content" : "damaged on media") + " after " + change;
                        final String name = "p" + iteration++;
                        execute("create table " + name + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                        final TableToken tt = engine.verifyTableName(name);
                        for (int i = 0; i < 12; i++) {
                            // The O3 row lands inside the first day, which then gets a new partition version.
                            final String ts = i == 5 && o3Rewrite ? "2024-10-01T00:30" : i < 4 ? "2024-10-01T0" + i + ":00" : "2024-10-02T" + (10 + i) + ":00";
                            execute("insert into " + name + " values ('" + ts + ":00.000000Z', " + i + ")");
                            if (i == 5 && dropped) {
                                execute("alter table " + name + " drop partition list '2024-10-01'");
                            }
                            if (i == 3 || i == 7) {
                                drainWalQueue();
                                takeSavePoint(tt);
                                engine.releaseAllWalWriters();
                            }
                            if (i == 7) {
                                if (rejectedOnContent) {
                                    rewriteNewestHeaderWithWrongColumnVersion(tt);
                                }
                                runPurgeJobs();
                                Assert.assertFalse(context + ": the WAL before the older save point goes", walDirExists(tt, 1));
                            }
                        }
                        drainWalQueue();
                        // A kill: no close-time save point.
                        engine.releaseAllWriters();
                        engine.releaseAllReaders();
                        engine.releaseAllWalWriters();
                        if (!rejectedOnContent) {
                            corruptRange(tt, 0, TableUtils.TXN_FILE_NAME, true);
                        }
                        engine.getTxnScoreboardPool().remove(tt);
                        engine.getTableSequencerAPI().resetForReboot(tt);
                        new RecoveryCoordinator(engine).recover();
                        Assert.assertEquals(context + ": recovery falls back to the older generation", 4, readTxnSeqTxn(tt));
                        final List<Long> restored = readVs(name);
                        restored.sort(null);
                        Assert.assertEquals(context + ": the older generation reads back", List.of(0L, 1L, 2L, 3L), restored);
                        engine.releaseAllReaders();
                        engine.notifyWalTxnRepublisher(tt);
                        drainWalQueue();
                        Assert.assertFalse(context + ": table suspended", engine.getTableSequencerAPI().isSuspended(tt));
                        final List<Long> rows = readVs(name);
                        rows.sort(null);
                        Assert.assertEquals(context, dropped
                                ? List.of(4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L)
                                : List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L), rows);
                        dropSweepTable(tt);
                        engine.releaseAllWriters();
                        engine.releaseAllReaders();
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A newest generation that passes its checksums but fails a content check, for each kind of definite
     * mismatch, is removed once the older generation is restored and checked. A kill after the replay moved the
     * live files past the older cut, but not past the rejected one, then cannot read as a wrong-lineage state:
     * the next start lands on the older generation again and the replay brings back every row. A read fault
     * of a restored live file also falls back, but must leave the save point in place, also when it sits exactly
     * at the sequencer frontier. A read fault of the save point itself must fail startup without deleting either
     * generation; a later start without the fault can reject its content and fall back. When the older generation
     * fails too, on content or on its checksums, no generation is selected: every start refuses, and both files stay on disk.
     */
    @Test
    public void testFallbackRemovesOnlyGenerationRejectedOnContent() throws Exception {
        final ContentCheckReadFaultFacade faultFf = new ContentCheckReadFaultFacade();
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            assertMemoryLeak(faultFf, () -> {
                final String metaFault = TableUtils.META_FILE_NAME;
                final String cvFault = TableUtils.COLUMN_VERSION_FILE_NAME;
                final String manifestFault = DurableEpochManifest.FILE_NAME + ".0";
                final ContentRow[] rows = {
                        new ContentRow(Rejection.CUT, null, Older.VALID, false),
                        new ContentRow(Rejection.HEADER_VERSIONS, null, Older.VALID, false),
                        new ContentRow(Rejection.SYMBOL_COUNT, null, Older.VALID, false),
                        new ContentRow(Rejection.TABLE_ID, null, Older.VALID, false),
                        new ContentRow(Rejection.TXN_CV_VERSIONS, null, Older.VALID, false),
                        new ContentRow(Rejection.SYMBOL_COUNT, metaFault, Older.VALID, false),
                        new ContentRow(Rejection.HEADER_VERSIONS, cvFault, Older.VALID, false),
                        new ContentRow(Rejection.HEADER_VERSIONS, manifestFault, Older.VALID, false),
                        new ContentRow(Rejection.SYMBOL_COUNT, metaFault, Older.VALID, true),
                        new ContentRow(Rejection.CUT, null, Older.CONTENT_MISMATCH, false),
                        new ContentRow(Rejection.TXN_CV_VERSIONS, null, Older.CONTENT_MISMATCH, false),
                        new ContentRow(Rejection.HEADER_VERSIONS, null, Older.FLIPPED_BYTE, false)
                };
                for (int i = 0; i < rows.length; i++) {
                    final ContentRow row = rows[i];
                    final String context = row.rejection + (row.readFault != null ? " behind a read fault on " + row.readFault : "")
                            + (row.older != Older.VALID ? " with the older generation " + row.older : "")
                            + (row.atFrontier ? " at the sequencer frontier" : "");
                    final TableToken tt = buildTableWithRejectedNewestGeneration(i, row.rejection, row.older, row.atFrontier);
                    if (row.older != Older.VALID) {
                        for (int start = 0; start < 2; start++) {
                            try {
                                new RecoveryCoordinator(engine).recover();
                                Assert.fail(context + ": a start with no valid generation must refuse");
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "no trustworthy adaptive epoch generation");
                            }
                            Assert.assertTrue(context + ": no generation is removed when none is selected", savePointFileExists(tt, 0));
                            Assert.assertTrue(context + ": no generation is removed when none is selected", savePointFileExists(tt, 1));
                        }
                        engine.getTxnScoreboardPool().remove(tt);
                        dropSweepTable(tt);
                        engine.releaseAllWriters();
                        engine.releaseAllReaders();
                        continue;
                    }
                    final boolean isManifestReadFault = manifestFault.equals(row.readFault);
                    faultFf.arm(row.readFault);
                    try {
                        if (isManifestReadFault) {
                            try {
                                new RecoveryCoordinator(engine).recover();
                                Assert.fail(context + ": an unreadable save point must fail startup");
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), manifestFault);
                            }
                            Assert.assertTrue(context + ": a read fault must preserve the newest generation", savePointFileExists(tt, 0));
                            Assert.assertTrue(context + ": a read fault must preserve the older generation", savePointFileExists(tt, 1));
                        } else {
                            new RecoveryCoordinator(engine).recover();
                        }
                    } finally {
                        faultFf.disarm();
                    }
                    if (isManifestReadFault) {
                        new RecoveryCoordinator(engine).recover();
                    }
                    Assert.assertEquals(context + ": recovery must fall back to the older generation", K, readTxnSeqTxn(tt));
                    Assert.assertTrue(context + ": the read fault never fired", row.readFault == null || faultFf.fired);
                    final boolean rejectedFileKept = savePointFileExists(tt, 0);
                    if (row.readFault == null) {
                        // A kill once the replay has passed the older cut but not the rejected one.
                        for (String fileName : new String[]{TableUtils.TXN_FILE_NAME, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.META_FILE_NAME}) {
                            replaceLiveFile(tt, fileName + ".mid", fileName);
                        }
                    }
                    engine.getTxnScoreboardPool().remove(tt);
                    assertRestartAfterFallbackConverges(tt, context, row.atFrontier ? K + M : K + M + 1);
                    Assert.assertEquals(context + ": only a definite mismatch removes the rejected generation", row.readFault != null && !isManifestReadFault, rejectedFileKept);
                    dropSweepTable(tt);
                    engine.releaseAllWriters();
                    engine.releaseAllReaders();
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * NEGATIVE CONTROL: restoring a torn {@code _txn} copy without validating it destroys the live
     * {@code _txn}. This is the failure the fall-back rows prevent.
     */
    @Test
    public void testNegativeControlOldBehaviourBricksOnTornCopy() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final TableToken tt = buildTableWithEpochAndLazyTail();
            Assert.assertTrue("live _txn must be healthy + ahead of the epoch before the bad restore",
                    readTxnSeqTxn(tt) >= K + M);

            engine.releaseAllWriters();
            engine.releaseAllReaders();

            tornByMode(tt, TornMode.TRUNCATE_TXN_EPOCH_TO_ZERO);
            // Emulate a restore without validation: the live _txn takes the bytes the torn copy still holds.
            try (Path src = new Path(); Path dst = new Path()) {
                final long[] copy = savedCopy(src, tt, 1, TableUtils.TXN_FILE_NAME);
                final long available = Math.max(0, Math.min(copy[1], ff.length(src.$()) - copy[0]));
                dst.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
                final long srcFd = ff.openRO(src.$());
                final long dstFd = ff.openRW(dst.$(), engine.getConfiguration().getWriterFileOpenOpts());
                try {
                    Assert.assertTrue(ff.truncate(dstFd, 0));
                    if (available > 0) {
                        Assert.assertEquals(available, ff.copyData(srcFd, dstFd, copy[0], available));
                    }
                } finally {
                    ff.close(dstFd);
                    ff.close(srcFd);
                }
            }

            boolean bricked = false;
            try {
                readTxnSeqTxn(tt);
            } catch (Throwable expected) {
                bricked = true;
            }
            Assert.assertTrue("restoring a torn _txn copy without validation must brick the live _txn", bricked);
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * The newest generation passes its checksums but fails the content checks, so recovery restores it, rejects
     * it and restores the older generation over it. The rejected generation stays listed first, ahead of the
     * live {@code _txn} the fallback rewound. With no power loss, and with one at each durability call of the
     * fallback, the next start must not refuse: it lands on the older generation, the start after it recovers
     * nothing, and the replay brings back every row.
     */
    @Test
    public void testPowerLossAtEverySyncOfFallbackThenRestart() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                final TableToken countToken = buildTableWithRejectedNewestGeneration(0, Rejection.SYMBOL_COUNT, Older.VALID, false);
                markDurableBaseline();
                final int opsBefore = crashFf.durabilityOpCount();
                new RecoveryCoordinator(engine).recover();
                final int n = crashFf.durabilityOpCount() - opsBefore;
                Assert.assertEquals("the first start must fall back to the older generation", K, readTxnSeqTxn(countToken));
                simulateRestart(new TableToken[]{countToken});
                assertRestartAfterFallbackConverges(countToken, "no power loss", K + M + 1);
                dropSweepTable(countToken);
                releaseEngineHandles();
                reclaimLingeringNonCacheFds(fdBaseline);
                Assert.assertTrue("the fallback must make durability calls", n > 0);

                for (int k = 1; k <= n; k++) {
                    final TableToken tt = buildTableWithRejectedNewestGeneration(k, Rejection.SYMBOL_COUNT, Older.VALID, false);
                    markDurableBaseline();
                    crashFf.armCrashAt(crashFf.durabilityOpCount() + k);
                    boolean fired = false;
                    try {
                        new RecoveryCoordinator(engine).recover();
                    } catch (CrashSimulationError expected) {
                        fired = true;
                    }
                    Assert.assertTrue("crash point " + k + " of " + n + " never fired", fired);
                    simulateRestart(new TableToken[]{tt});
                    assertRestartAfterFallbackConverges(tt, "power loss at durability call " + k + " of " + n, K + M + 1);
                    dropSweepTable(tt);
                    releaseEngineHandles();
                    reclaimLingeringNonCacheFds(fdBaseline);
                }
                Files.getMmapCache().asyncMunmap();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * After a fallback from the newest generation, the first save point must write into the slot that does not
     * hold the only good generation: both when the fallback removed the newest one (rejected on content) and when
     * it kept it (its sections fail their checksums under a valid header). A power loss at every durability call
     * of that save point must leave a state recovery accepts, with every row, and a second start must recover
     * nothing.
     */
    @Test
    public void testPowerLossAtEverySyncOfFirstSavePointAfterFallback() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                for (int kind = 0; kind < 2; kind++) {
                    final boolean keptNewest = kind == 1;
                    final TableToken countToken = buildFallenBackTable(1000 * kind, keptNewest);
                    final int opsBefore = crashFf.durabilityOpCount();
                    takeSavePoint(countToken);
                    final int n = crashFf.durabilityOpCount() - opsBefore;
                    dropSweepTable(countToken);
                    releaseEngineHandles();
                    reclaimLingeringNonCacheFds(fdBaseline);
                    Assert.assertTrue("the save point must make durability calls", n > 0);

                    for (int k = 1; k <= n; k++) {
                        final String context = "power loss at durability call " + k + " of " + n + " of the first save point after a fallback"
                                + (keptNewest ? " that kept the newest generation" : " that removed the newest generation");
                        final TableToken tt = buildFallenBackTable(1000 * kind + k, keptNewest);
                        markDurableBaseline();
                        crashFf.armCrashAt(crashFf.durabilityOpCount() + k);
                        boolean fired = false;
                        try {
                            takeSavePoint(tt);
                        } catch (CrashSimulationError expected) {
                            fired = true;
                        }
                        Assert.assertTrue("crash point " + k + " of " + n + " never fired", fired);
                        simulateRestart(new TableToken[]{tt});
                        try {
                            new RecoveryCoordinator(engine).recover();
                        } catch (CairoException e) {
                            Assert.fail(context + ": recovery refused: " + e.getFlyweightMessage());
                        }
                        final long events = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
                        new RecoveryCoordinator(engine).recover();
                        Assert.assertEquals(context + ": a second start must recover nothing", events, TestUtils.getMetricValue(engine, RECOVERY_EVENTS));
                        engine.notifyWalTxnRepublisher(tt);
                        drainWalQueue();
                        Assert.assertFalse(context + ": table suspended", engine.getTableSequencerAPI().isSuspended(tt));
                        assertRows(tt.getTableName(), K + M + 1, context);
                        dropSweepTable(tt);
                        releaseEngineHandles();
                        reclaimLingeringNonCacheFds(fdBaseline);
                    }
                }
                Files.getMmapCache().asyncMunmap();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    /**
     * A crash at every durability call of two save points in a row: the first writes generation one, the
     * second overwrites generation zero, which still holds the creation baseline.
     * Whatever survives, recovery must start, keep every row, and find nothing to recover on a second start.
     */
    @Test
    public void testPowerLossAtEverySyncOfSavePoint() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        // A writer closed after the injected power loss must not publish the save point the crash interrupted.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "false");
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            Assert.assertFalse(engine.getConfiguration().isAdaptiveEpochFlushOnClose());
            runWithCrashFacade(() -> {
                final Set<Long> fdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
                final TableToken countToken = buildSweepTable(0);
                final int opsBefore = crashFf.durabilityOpCount();
                takeTwoSavePoints(countToken);
                final int n = crashFf.durabilityOpCount() - opsBefore;
                dropSweepTable(countToken);
                releaseEngineHandles();
                reclaimLingeringNonCacheFds(fdBaseline);
                Assert.assertTrue("the save points must make durability calls", n > 0);

                for (int k = 1; k <= n; k++) {
                    final TableToken tt = buildSweepTable(k);
                    markDurableBaseline();
                    crashFf.armCrashAt(crashFf.durabilityOpCount() + k);
                    boolean fired = false;
                    try {
                        takeTwoSavePoints(tt);
                    } catch (CrashSimulationError expected) {
                        fired = true;
                    }
                    Assert.assertTrue("crash point " + k + " of " + n + " never fired", fired);

                    final TableToken[] tokens = {tt};
                    simulateRestart(tokens);
                    try {
                        new RecoveryCoordinator(engine).recover();
                    } catch (CairoException e) {
                        Assert.fail("recovery refused after a power loss at durability call " + k + " of " + n
                                + ": " + e.getFlyweightMessage());
                    }
                    final long events = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
                    new RecoveryCoordinator(engine).recover();
                    Assert.assertEquals("a second start after a power loss at durability call " + k
                            + " must recover nothing", events, TestUtils.getMetricValue(engine, RECOVERY_EVENTS));
                    engine.notifyWalTxnRepublisher(tt);
                    drainWalQueue();
                    Assert.assertFalse("crash point " + k + ": table suspended",
                            engine.getTableSequencerAPI().isSuspended(tt));
                    assertRows(tt.getTableName(), K + M, "crash point " + k);
                    dropSweepTable(tt);
                    releaseEngineHandles();
                    reclaimLingeringNonCacheFds(fdBaseline);
                }
                Files.getMmapCache().asyncMunmap();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_FLUSH_ON_CLOSE, "true");
        }
    }

    @Test
    public void testRecoveryFallsBackFromCorruptBodyTxnEpochAndReplaysWal() throws Exception {
        assertTornCopyHandledSafely(TornMode.CORRUPT_TXN_EPOCH_BODY);
    }

    @Test
    public void testRecoveryFallsBackFromFlippedCvByte() throws Exception {
        assertTornCopyHandledSafely(TornMode.FLIP_CV_BYTE);
    }

    @Test
    public void testRecoveryFallsBackFromFlippedMetaByte() throws Exception {
        assertTornCopyHandledSafely(TornMode.FLIP_META_BYTE);
    }

    @Test
    public void testRecoveryFallsBackFromFlippedTxnByte() throws Exception {
        assertTornCopyHandledSafely(TornMode.FLIP_TXN_BYTE);
    }

    @Test
    public void testRecoveryFallsBackFromMismatchedManifestAndReplaysWal() throws Exception {
        assertTornCopyHandledSafely(TornMode.MANIFEST_SEQTXN_MISMATCH);
    }

    /**
     * A tampered table id in the saved {@code _meta} is rejected. The checksum over the copy rejects it before
     * the table-id guard runs, so this proves the rejection, not that guard.
     */
    @Test
    public void testRecoveryFallsBackFromTamperedMetaEpoch() throws Exception {
        assertTornCopyHandledSafely(TornMode.TAMPERED_META_EPOCH);
    }

    @Test
    public void testRecoveryFallsBackFromTornHeader() throws Exception {
        assertTornCopyHandledSafely(TornMode.TORN_HEADER);
    }

    @Test
    public void testRecoveryFallsBackFromZeroByteTxnEpochAndReplaysWal() throws Exception {
        assertTornCopyHandledSafely(TornMode.TRUNCATE_TXN_EPOCH_TO_ZERO);
    }

    /**
     * Columns added and rows updated after the save point grow the live {@code _meta} and {@code _cv} past
     * their saved copies. The restore must leave each live file byte-equal to its copy, tail included, so a
     * second start finds the table at the save point and counts no second recovery.
     */
    @Test
    public void testRestoreTruncatesLiveFilesLongerThanTheirCopies() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final TableToken tt = buildTableWithEpochAndLazyTail();
            execute("alter table t add column c1 int");
            execute("alter table t add column c2 symbol");
            // Enough columns to grow _meta past the page it was allocated with: 64 for a 4 KiB page, as many more
            // as the OS page is larger (64 KiB on Windows).
            for (int i = 0, n = (int) (64 * Files.PAGE_SIZE / 4096); i < n; i++) {
                execute("alter table t add column a_rather_long_column_name_" + i + " long");
            }
            execute("insert into t (ts, v, c1, c2) values ('2024-10-02T00:00:00.000000Z', " + (K + M) + ", 1, 'a')");
            execute("update t set c1 = 7 where v < 2");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));

            engine.releaseAllWriters();
            engine.releaseAllReaders();
            final long liveMeta = liveLength(tt, TableUtils.META_FILE_NAME);
            final long savedMeta = savedLength(tt, 1, TableUtils.META_FILE_NAME);
            Assert.assertTrue("live _meta must have grown past its copy [live=" + liveMeta + ", saved=" + savedMeta + ']', liveMeta > savedMeta);
            final long liveCv = liveLength(tt, TableUtils.COLUMN_VERSION_FILE_NAME);
            final long savedCv = savedLength(tt, 1, TableUtils.COLUMN_VERSION_FILE_NAME);
            Assert.assertTrue("live _cv must have grown past its copy [live=" + liveCv + ", saved=" + savedCv + ']', liveCv > savedCv);

            final long before = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
            new RecoveryCoordinator(engine).recover();
            final long afterFirst = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
            Assert.assertEquals("the first start restores the save point", before + 1, afterFirst);
            for (String fileName : new String[]{TableUtils.TXN_FILE_NAME, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.META_FILE_NAME}) {
                Assert.assertTrue("restored " + fileName + " must equal its saved copy", liveEqualsSavedCopy(tt, 1, fileName));
            }

            new RecoveryCoordinator(engine).recover();
            Assert.assertEquals("a second start must recover nothing", afterFirst, TestUtils.getMetricValue(engine, RECOVERY_EVENTS));

            // A fresh process starts with an empty scoreboard. The pooled one still holds the pre-restore txn
            // high-water mark, and the UPDATE's replay would wait for a reader below it.
            engine.getTxnScoreboardPool().remove(tt);
            engine.notifyWalTxnRepublisher(tt);
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
            assertQuery("select count(), sum(v), sum(c1) from t").noRandomAccess().expectSize().returns("count\tsum\tsum1\n" + (K + M + 1) + "\t" + ((K + M) * (K + M + 1) / 2) + "\t15\n");
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * One file per generation: a save point's manifest is the header followed by byte copies of the live
     * {@code _txn}, {@code _cv} and {@code _meta}, and nothing else.
     */
    @Test
    public void testSavePointIsOneFilePerGeneration() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final TableToken tt = buildTableWithEpochAndLazyTail();
            try (TableWriter w = getWriter(tt)) {
                w.advanceDurableEpoch(2L);
            }
            engine.releaseAllWriters();
            final FilesFacade ff = engine.getConfiguration().getFilesFacade();
            final String[] names = {TableUtils.TXN_FILE_NAME, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.META_FILE_NAME};
            long sections = 0;
            try (Path p = new Path()) {
                for (String name : names) {
                    sections += liveLength(tt, name);
                }
                p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(DurableEpochManifest.FILE_NAME).put(".0");
                Assert.assertEquals("the manifest is the header and the three sections",
                        DurableEpochManifest.FILE_SIZE + sections, ff.length(p.$()));
            }
            for (String name : names) {
                Assert.assertTrue("the section of " + name + " must equal the live file", liveEqualsSavedCopy(tt, 0, name));
            }
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private static long readLong(FilesFacade ff, Path p, long offset) {
        final long fd = ff.openRO(p.$());
        Assert.assertTrue("must open " + p, fd > -1);
        final long buf = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        try {
            Assert.assertEquals(Long.BYTES, ff.read(fd, buf, Long.BYTES, offset));
            return Unsafe.getUnsafe().getLong(buf);
        } finally {
            Unsafe.free(buf, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
            ff.close(fd);
        }
    }

    private void assertRestartAfterFallbackConverges(TableToken tt, String context, int expectedRows) {
        try {
            new RecoveryCoordinator(engine).recover();
        } catch (CairoException e) {
            Assert.fail(context + ": the next start refused: " + e.getFlyweightMessage());
        }
        Assert.assertEquals(context + ": the next start must land on the older generation", K, readTxnSeqTxn(tt));
        final long events = TestUtils.getMetricValue(engine, RECOVERY_EVENTS);
        new RecoveryCoordinator(engine).recover();
        Assert.assertEquals(context + ": the start after it must recover nothing", events, TestUtils.getMetricValue(engine, RECOVERY_EVENTS));
        engine.notifyWalTxnRepublisher(tt);
        drainWalQueue();
        Assert.assertFalse(context + ": table suspended", engine.getTableSequencerAPI().isSuspended(tt));
        assertRows(tt.getTableName(), expectedRows, context);
    }

    private void assertRows(String tableName, int expected, String context) {
        final List<Long> rows = readVs(tableName);
        Assert.assertEquals(context + ": rows", expected, rows.size());
        for (int i = 0; i < expected; i++) {
            Assert.assertEquals(context + ": row " + i, Long.valueOf(i), rows.get(i));
        }
    }

    private void assertTornCopyHandledSafely(TornMode mode) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        // Drive the epoch explicitly at a KNOWN cut; the auto-epoch must not fire.
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
        try {
            // A descriptor or mapping leaked on a rejected generation fails this row, not a later one that the fd
            // cache would serve the stale file.
            assertMemoryLeak(() -> {
                Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());

                final TableToken tt = buildTableWithEpochAndLazyTail();
                Assert.assertTrue("live _txn must hold the post-epoch frontier", readTxnSeqTxn(tt) >= K + M);

                engine.releaseAllWriters();
                engine.releaseAllReaders();

                tornByMode(tt, mode);

                // Recovery rejects the newest generation and restores the proven creation baseline.
                new RecoveryCoordinator(engine).recover();
                Assert.assertEquals("a damaged newest generation must fall back to the generation-0 replay floor",
                        0, readTxnSeqTxn(tt));
                Assert.assertTrue("a generation rejected on its checksums is never removed", savePointFileExists(tt, 1));

                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                Assert.assertFalse("table must NOT be suspended after a safe fallback",
                        engine.getTableSequencerAPI().isSuspended(tt));
                assertRows("t", K + M, mode.name());
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * Create an ADAPTIVE table, apply K rows, take a durable epoch at seqTxn=K (into generation one), then apply
     * M MORE rows LAZILY so the live {@code _txn}/{@code _cv} advance past the epoch to the frontier.
     */
    private TableToken buildTableWithEpochAndLazyTail() throws Exception {
        execute("create table t (ts timestamp, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < K; i++) {
            execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();

        final TableToken tt = engine.verifyTableName("t");

        final long epochSeqTxn;
        try (TableWriter w = getWriter(tt)) {
            w.advanceDurableEpoch(1L);
            epochSeqTxn = w.getSeqTxn();
        }
        Assert.assertEquals("epoch must be taken at seqTxn=K", K, epochSeqTxn);

        for (int i = K; i < K + M; i++) {
            execute("insert into t values ('2024-10-01T0" + i + ":00:00.000000Z', " + i + ")");
        }
        drainWalQueue();
        return tt;
    }

    // A table whose start fell back from the newest generation, with the WAL replayed past it. The newest one is
    // rejected on content (and removed), or, when kept, fails its section checksums under a valid header.
    private TableToken buildFallenBackTable(int iteration, boolean keptNewest) throws Exception {
        final TableToken tt = buildTableWithRejectedNewestGeneration(iteration, Rejection.SYMBOL_COUNT, Older.VALID, false);
        if (keptNewest) {
            corruptRange(tt, 0, TableUtils.TXN_FILE_NAME, true);
        }
        new RecoveryCoordinator(engine).recover();
        Assert.assertEquals("the newest generation is kept only when its checksums fail", keptNewest, savePointFileExists(tt, 0));
        Assert.assertEquals("the start must fall back to the older generation", K, readTxnSeqTxn(tt));
        engine.notifyWalTxnRepublisher(tt);
        drainWalQueue();
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
        assertRows(tt.getTableName(), K + M + 1, "after the fallback");
        engine.releaseAllReaders();
        // The writer the replay pooled stays open: the save point under test must not include a writer open.
        return tt;
    }

    /**
     * A table whose newest generation passes its checksums but fails one of recovery's content checks, as a
     * write-side bug leaves it (see {@link Rejection}). The older generation is at seqTxn K and is consistent
     * unless {@code older} damages it, and the live table is one transaction past the newest generation, or,
     * {@code atFrontier}, exactly at it. The live {@code _txn}, {@code _cv} and {@code _meta} of a cut between the
     * two generations are kept with the suffix {@code .mid}.
     */
    private TableToken buildTableWithRejectedNewestGeneration(int iteration, Rejection rejection, Older older, boolean atFrontier) throws Exception {
        final String name = "f" + iteration;
        execute("create table " + name + " (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < K; i++) {
            execute("insert into " + name + " values ('2024-10-01T0" + i + ":00:00.000000Z', 'a" + i + "', " + i + ")");
        }
        drainWalQueue();
        final TableToken tt = engine.verifyTableName(name);
        try (TableWriter w = getWriter(tt)) {
            w.advanceDurableEpoch(1L);
        }
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final String[] liveFiles = {TableUtils.TXN_FILE_NAME, TableUtils.COLUMN_VERSION_FILE_NAME, TableUtils.META_FILE_NAME};
        try (Path live = new Path(); Path forge = new Path(); Path forgeOlder = new Path(); Path dst = new Path()) {
            live.of(engine.getConfiguration().getDbRoot()).concat(tt);
            final int liveLen = live.size();
            forge.of(engine.getConfiguration().getDbRoot()).concat("forge_" + name);
            final int forgeLen = forge.size();
            Assert.assertEquals(0, ff.mkdirs(forge.slash(), engine.getConfiguration().getMkDirMode()));
            forgeOlder.of(engine.getConfiguration().getDbRoot()).concat("forge_older_" + name);
            final int forgeOlderLen = forgeOlder.size();
            if (older == Older.CONTENT_MISMATCH) {
                // The live files equal the older generation's sections here.
                Assert.assertEquals(0, ff.mkdirs(forgeOlder.slash(), engine.getConfiguration().getMkDirMode()));
                for (String fileName : liveFiles) {
                    Assert.assertTrue(ff.copy(live.trimTo(liveLen).concat(fileName).$(), dst.of(forgeOlder).trimTo(forgeOlderLen).concat(fileName).$()) >= 0);
                }
            }
            if (rejection == Rejection.SYMBOL_COUNT) {
                Assert.assertTrue(ff.copy(live.trimTo(liveLen).concat(TableUtils.META_FILE_NAME).$(), dst.of(forge).trimTo(forgeLen).concat(TableUtils.META_FILE_NAME).$()) >= 0);
            } else if (rejection == Rejection.TXN_CV_VERSIONS) {
                // Adding a column to a non-empty table moves the column version past this _cv.
                Assert.assertTrue(ff.copy(live.trimTo(liveLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$(), dst.of(forge).trimTo(forgeLen).concat(TableUtils.COLUMN_VERSION_FILE_NAME).$()) >= 0);
            }

            execute("alter table " + name + " add column sym2 symbol");
            for (int i = K; i < K + 2; i++) {
                execute("insert into " + name + " values ('2024-10-01T0" + i + ":00:00.000000Z', 'a" + i + "', " + i + ", 'b')");
            }
            drainWalQueue();
            engine.releaseAllWriters();
            for (String fileName : liveFiles) {
                Assert.assertTrue(ff.copy(live.trimTo(liveLen).concat(fileName).$(), dst.of(live).trimTo(liveLen).concat(fileName).put(".mid").$()) >= 0);
            }
            for (int i = K + 2; i < K + M; i++) {
                execute("insert into " + name + " values ('2024-10-01T0" + i + ":00:00.000000Z', 'a" + i + "', " + i + ", 'b')");
            }
            drainWalQueue();
            engine.releaseAllWriters();
            for (String fileName : liveFiles) {
                final boolean keptFromEarlier = rejection == Rejection.SYMBOL_COUNT && TableUtils.META_FILE_NAME.equals(fileName)
                        || rejection == Rejection.TXN_CV_VERSIONS && TableUtils.COLUMN_VERSION_FILE_NAME.equals(fileName);
                if (!keptFromEarlier) {
                    // The cut row saves the _txn and _cv of the earlier cut under a header that names the later one.
                    final String from = rejection == Rejection.CUT && !TableUtils.META_FILE_NAME.equals(fileName) ? fileName + ".mid" : fileName;
                    Assert.assertTrue(ff.copy(live.trimTo(liveLen).concat(from).$(), dst.of(forge).trimTo(forgeLen).concat(fileName).$()) >= 0);
                }
            }
            final long seqTxn;
            final long txn;
            final long columnVersion;
            final long metadataVersion;
            try (TxReader tx = new TxReader(ff)) {
                tx.ofRO(live.trimTo(liveLen).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                Assert.assertTrue(tx.unsafeLoadAll());
                seqTxn = tx.getSeqTxn();
                txn = tx.getTxn();
                columnVersion = tx.getColumnVersion();
                metadataVersion = tx.getMetadataVersion();
            }
            try (MemoryMARW mem = Vm.getCMARWInstance()) {
                mem.smallFile(ff, forge.trimTo(forgeLen).concat(TableUtils.META_FILE_NAME).$(), MemoryTag.MMAP_DEFAULT);
                if (rejection == Rejection.SYMBOL_COUNT) {
                    // Equal metadata versions: otherwise the version check rejects the generation before the symbol check.
                    TableUtils.resetMetadataVersion(mem, metadataVersion);
                } else if (rejection == Rejection.TABLE_ID) {
                    mem.putInt(TableUtils.META_OFFSET_TABLE_ID, mem.getInt(TableUtils.META_OFFSET_TABLE_ID) + 1000);
                }
            }
            writeRejectedGeneration(
                    tt,
                    forge.trimTo(forgeLen),
                    forgeLen,
                    0,
                    seqTxn,
                    txn,
                    rejection == Rejection.HEADER_VERSIONS ? columnVersion + 1 : columnVersion,
                    metadataVersion
            );
            // Remove first, as replaceLiveFile() does: Windows copies onto no existing file.
            ff.removeQuiet(live.trimTo(liveLen).concat(DurableEpochManifest.FILE_NAME).put(".0").$());
            Assert.assertTrue(ff.copy(
                    forge.trimTo(forgeLen).concat(DurableEpochManifest.FILE_NAME).put(".0").$(),
                    live.trimTo(liveLen).concat(DurableEpochManifest.FILE_NAME).put(".0").$()
            ) >= 0);
            if (older == Older.CONTENT_MISMATCH) {
                try (TxReader tx = new TxReader(ff)) {
                    tx.ofRO(forgeOlder.trimTo(forgeOlderLen).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
                    Assert.assertTrue(tx.unsafeLoadAll());
                    Assert.assertEquals(K, tx.getSeqTxn());
                    writeRejectedGeneration(
                            tt,
                            forgeOlder.trimTo(forgeOlderLen),
                            forgeOlderLen,
                            1,
                            tx.getSeqTxn(),
                            tx.getTxn(),
                            tx.getColumnVersion() + 1,
                            tx.getMetadataVersion()
                    );
                }
                ff.removeQuiet(live.trimTo(liveLen).concat(DurableEpochManifest.FILE_NAME).put(".1").$());
                Assert.assertTrue(ff.copy(
                        forgeOlder.trimTo(forgeOlderLen).concat(DurableEpochManifest.FILE_NAME).put(".1").$(),
                        live.trimTo(liveLen).concat(DurableEpochManifest.FILE_NAME).put(".1").$()
                ) >= 0);
            } else if (older == Older.FLIPPED_BYTE) {
                corruptRange(tt, TableUtils.TXN_FILE_NAME, true);
            }
        }
        if (!atFrontier) {
            execute("insert into " + name + " values ('2024-10-02T00:00:00.000000Z', 'c', " + (K + M) + ", 'b')");
            drainWalQueue();
        }
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        return tt;
    }

    private TableToken buildSweepTable(int iteration) throws Exception {
        final String name = "s" + iteration;
        execute("create table " + name + " (ts timestamp, sym symbol, v long) timestamp(ts) partition by day wal");
        for (int i = 0; i < K; i++) {
            execute("insert into " + name + " values ('2024-10-01T0" + i + ":00:00.000000Z', 'a" + i + "', " + i + ")");
        }
        execute("alter table " + name + " add column sym2 symbol");
        for (int i = K; i < K + M; i++) {
            execute("insert into " + name + " values ('2024-10-01T0" + i + ":00:00.000000Z', 'a" + i + "', " + i + ", 'b')");
        }
        drainWalQueue();
        final TableToken tt = engine.verifyTableName(name);
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tt));
        return tt;
    }

    private void corruptRange(TableToken tt, String fileName, boolean flipOneByte) {
        corruptRange(tt, 1, fileName, flipOneByte);
    }

    private void corruptRange(TableToken tt, int generation, String fileName, boolean flipOneByte) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            final long[] copy = savedCopy(p, tt, generation, fileName);
            Assert.assertTrue("the saved " + fileName + " must be non-empty", copy[1] > 0);
            final long offset = flipOneByte ? copy[0] + copy[1] / 2 : copy[0];
            final int len = flipOneByte ? 1 : (int) copy[1];
            final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
            Assert.assertTrue("must open " + p, fd > 0);
            final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(len, ff.read(fd, buf, len, offset));
                for (int i = 0; i < len; i++) {
                    final byte b = Unsafe.getUnsafe().getByte(buf + i);
                    Unsafe.getUnsafe().putByte(buf + i, flipOneByte ? (byte) ~b : (byte) 0xA5);
                }
                Assert.assertEquals(len, ff.write(fd, buf, len, offset));
            } finally {
                Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
        }
    }

    // Every iteration marks the whole database durable, so a table left behind slows every later iteration.
    private void dropSweepTable(TableToken tt) throws Exception {
        execute("drop table " + tt.getTableName());
        drainWalQueue();
    }

    private long liveLength(TableToken tt, String fileName) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName);
            return engine.getConfiguration().getFilesFacade().length(p.$());
        }
    }

    private boolean liveEqualsSavedCopy(TableToken tt, int generation, String fileName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path saved = new Path(); Path live = new Path()) {
            final long[] copy = savedCopy(saved, tt, generation, fileName);
            live.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(fileName);
            final long len = copy[1];
            if (ff.length(live.$()) != len) {
                return false;
            }
            final long savedBuf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            final long liveBuf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            final long savedFd = ff.openRO(saved.$());
            final long liveFd = ff.openRO(live.$());
            try {
                return ff.read(savedFd, savedBuf, len, copy[0]) == len
                        && ff.read(liveFd, liveBuf, len, 0) == len
                        && io.questdb.std.Vect.memeq(savedBuf, liveBuf, len);
            } finally {
                ff.close(liveFd);
                ff.close(savedFd);
                Unsafe.free(liveBuf, len, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(savedBuf, len, MemoryTag.NATIVE_DEFAULT);
            }
        }
    }

    private long readTxnSeqTxn(TableToken tt) {
        try (TxReader tx = new TxReader(engine.getConfiguration().getFilesFacade());
             Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
            tx.ofRO(p.$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            if (!tx.unsafeLoadAll()) {
                throw new IllegalStateException("live _txn did not load (bricked)");
            }
            return tx.getSeqTxn();
        }
    }

    private List<Long> readVs(String tableName) {
        final List<Long> out = new ArrayList<>();
        try (
                SqlExecutionContext ctx = TestUtils.createSqlExecutionCtx(engine);
                RecordCursorFactory f = engine.select("select v from " + tableName + " order by ts", ctx);
                RecordCursor c = f.getCursor(ctx)
        ) {
            final io.questdb.cairo.sql.Record r = c.getRecord();
            while (c.hasNext()) {
                out.add(r.getLong(0));
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    private void replaceLiveFile(TableToken tt, String from, String to) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path src = new Path(); Path dst = new Path()) {
            src.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(from);
            dst.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(to);
            Assert.assertTrue(ff.removeQuiet(dst.$()));
            Assert.assertTrue("copy " + from + " -> " + to, ff.copy(src.$(), dst.$()) >= 0);
        }
    }

    // The newest generation passes its checksums, but its header records a column version its _txn section does not.
    private void rewriteNewestHeaderWithWrongColumnVersion(TableToken tt) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (TxReader tx = new TxReader(ff); Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt);
            final int rootLen = p.size();
            tx.ofRO(p.concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            Assert.assertTrue(tx.unsafeLoadAll());
            writeRejectedGeneration(tt, p.trimTo(rootLen), rootLen, 0, tx.getSeqTxn(), tx.getTxn(), tx.getColumnVersion() + 1, tx.getMetadataVersion());
        }
    }

    private void runPurgeJobs() {
        final long step = engine.getConfiguration().getWalPurgeInterval() * 1000L + 1_000_000L;
        final long[] tick = {1L};
        try (WalPurgeJob job = new WalPurgeJob(engine, engine.getConfiguration().getFilesFacade(), () -> (tick[0] += step))) {
            job.run();
            job.run();
        }
        engine.releaseAllReaders();
        try (O3PartitionPurgeJob job = new O3PartitionPurgeJob(engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (job.run()) {
            }
        }
    }

    /**
     * Points {@code p} at the file that holds the save point's copy of {@code fileName} in {@code generation},
     * and returns the copy's offset and length in it: the file's section in the generation's manifest (sizes in
     * the header at 48, 64 and 80; sections in {@code _txn}, {@code _cv}, {@code _meta} order after the header).
     */
    private long[] savedCopy(Path p, TableToken tt, int generation, String fileName) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(DurableEpochManifest.FILE_NAME).put('.').put(generation);
        final long txnSize = readLong(ff, p, 48);
        final long cvSize = readLong(ff, p, 64);
        final long metaSize = readLong(ff, p, 80);
        final long header = DurableEpochManifest.FILE_SIZE;
        if (TableUtils.TXN_FILE_NAME.equals(fileName)) {
            return new long[]{header, txnSize};
        }
        if (TableUtils.COLUMN_VERSION_FILE_NAME.equals(fileName)) {
            return new long[]{header + txnSize, cvSize};
        }
        return new long[]{header + txnSize + cvSize, metaSize};
    }

    private long savedLength(TableToken tt, int generation, String fileName) {
        try (Path p = new Path()) {
            return savedCopy(p, tt, generation, fileName)[1];
        }
    }

    private boolean savePointFileExists(TableToken tt, int generation) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(DurableEpochManifest.FILE_NAME).put('.').put(generation);
            return engine.getConfiguration().getFilesFacade().exists(p.$());
        }
    }

    private void takeSavePoint(TableToken tt) {
        try (TableWriter w = getWriter(tt)) {
            w.advanceDurableEpoch(3L);
        }
    }

    private void takeTwoSavePoints(TableToken tt) {
        try (TableWriter w = getWriter(tt)) {
            w.advanceDurableEpoch(1L);
            w.advanceDurableEpoch(2L);
        }
    }

    private void tornByMode(TableToken tt, TornMode mode) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        try (Path p = new Path()) {
            switch (mode) {
                case TRUNCATE_TXN_EPOCH_TO_ZERO -> {
                    // Cut the file at the start of the _txn copy: nothing of it survives.
                    final long[] copy = savedCopy(p, tt, 1, TableUtils.TXN_FILE_NAME);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open " + p, fd > 0);
                    try {
                        Assert.assertTrue("cut the _txn copy", ff.truncate(fd, copy[0]));
                    } finally {
                        ff.close(fd);
                    }
                }
                case CORRUPT_TXN_EPOCH_BODY -> corruptRange(tt, TableUtils.TXN_FILE_NAME, false);
                case FLIP_TXN_BYTE -> corruptRange(tt, TableUtils.TXN_FILE_NAME, true);
                case FLIP_CV_BYTE -> corruptRange(tt, TableUtils.COLUMN_VERSION_FILE_NAME, true);
                case FLIP_META_BYTE -> corruptRange(tt, TableUtils.META_FILE_NAME, true);
                case TAMPERED_META_EPOCH -> {
                    final long[] copy = savedCopy(p, tt, 1, TableUtils.META_FILE_NAME);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open " + p, fd > 0);
                    final long buf = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                    try {
                        final long offset = copy[0] + TableUtils.META_OFFSET_TABLE_ID;
                        Assert.assertEquals(Integer.BYTES, ff.read(fd, buf, Integer.BYTES, offset));
                        Unsafe.getUnsafe().putInt(buf, Unsafe.getUnsafe().getInt(buf) + 1000);
                        Assert.assertEquals(Integer.BYTES, ff.write(fd, buf, Integer.BYTES, offset));
                    } finally {
                        Unsafe.free(buf, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                        ff.close(fd);
                    }
                }
                case MANIFEST_SEQTXN_MISMATCH, TORN_HEADER -> {
                    p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(DurableEpochManifest.FILE_NAME).put('.').put(1);
                    final long fd = ff.openRW(p.$(), engine.getConfiguration().getWriterFileOpenOpts());
                    Assert.assertTrue("must open generation manifest", fd > 0);
                    final int len = mode == TornMode.TORN_HEADER ? 16 : Long.BYTES;
                    final long buf = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
                    try {
                        if (mode == TornMode.TORN_HEADER) {
                            // The header's last 16 bytes (closing magic and checksum) did not land.
                            Unsafe.getUnsafe().setMemory(buf, len, (byte) 0);
                            Assert.assertEquals(len, ff.write(fd, buf, len, DurableEpochManifest.FILE_SIZE - len));
                        } else {
                            Unsafe.getUnsafe().putLong(buf, K + 1000L);
                            Assert.assertEquals(len, ff.write(fd, buf, len, 24));
                        }
                    } finally {
                        Unsafe.free(buf, len, MemoryTag.NATIVE_DEFAULT);
                        ff.close(fd);
                    }
                }
                default -> throw new AssertionError(mode);
            }
        }
    }

    private boolean walDirExists(TableToken tt, int walId) {
        try (Path p = new Path()) {
            p.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(WalUtils.WAL_NAME_BASE).put(walId);
            return engine.getConfiguration().getFilesFacade().exists(p.$());
        }
    }

    private void writeRejectedGeneration(TableToken token, Path path, int rootLen, int generation, long seqTxn, long txn, long columnVersion, long metadataVersion) {
        final FilesFacade ff = engine.getConfiguration().getFilesFacade();
        final long header = Unsafe.malloc(DurableEpochManifest.FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
        long fd = -1;
        try (TxReader tx = new TxReader(ff)) {
            tx.ofRO(path.trimTo(rootLen).concat(TableUtils.TXN_FILE_NAME).$(), ColumnType.TIMESTAMP_MICRO, PartitionBy.DAY);
            Assert.assertTrue(tx.unsafeLoadAll());
            DurableEpochManifest.write(engine.getConfiguration(), token, path.trimTo(rootLen), rootLen, generation,
                    tx.getSeqTxn(), tx.getTxn(), tx.getColumnVersion(), tx.getMetadataVersion());
            // Corrupt only the header, without a writable mapping's page-rounded file resize.
            fd = ff.openRW(path.trimTo(rootLen).concat(DurableEpochManifest.FILE_NAME).put('.').put(generation).$(), 0);
            Assert.assertTrue(fd >= 0);
            Assert.assertEquals(DurableEpochManifest.FILE_SIZE, ff.read(fd, header, DurableEpochManifest.FILE_SIZE, 0));
            Unsafe.getUnsafe().putLong(header + 24, seqTxn);
            Unsafe.getUnsafe().putLong(header + 32, txn);
            Unsafe.getUnsafe().putLong(header + 40, columnVersion);
            Unsafe.getUnsafe().putLong(header + 96, metadataVersion);
            Unsafe.getUnsafe().putLong(header + 112, TableUtils.calculateCvAreaChecksum(header, 104));
            Assert.assertEquals(DurableEpochManifest.FILE_SIZE, ff.write(fd, header, DurableEpochManifest.FILE_SIZE, 0));
            ff.fsyncDurable(fd);
        } finally {
            if (fd >= 0) {
                ff.close(fd);
            }
            Unsafe.free(header, DurableEpochManifest.FILE_SIZE, MemoryTag.NATIVE_DEFAULT);
            path.trimTo(rootLen);
        }
    }

    private enum Older {
        // Passes its checksums, but its header records a column version its _txn section does not.
        CONTENT_MISMATCH,
        // One byte of its _txn section flipped: fails its checksums.
        FLIPPED_BYTE,
        VALID
    }

    private enum Rejection {
        // The _txn and _cv sections are an earlier cut than the header names.
        CUT,
        // The header records a column version the _txn section does not.
        HEADER_VERSIONS,
        // The _meta section has one symbol column fewer than the _txn section, at the same metadata version.
        SYMBOL_COUNT,
        // The _meta section names another table.
        TABLE_ID,
        // The _cv section is at an earlier column version than the _txn section names.
        TXN_CV_VERSIONS
    }

    // One row of the content-rejection table: how the newest generation fails, an optional read fault inside its
    // content check, how the older generation fails, and whether no transaction follows the newest generation.
    private record ContentRow(Rejection rejection, String readFault, Older older, boolean atFrontier) {
    }

    // While armed, fails every open of the named file that the content check of the first generation makes.
    // Recovery opening the older generation's save point file disarms it, so that generation's check reads.
    private static final class ContentCheckReadFaultFacade extends TestFilesFacadeImpl {
        private boolean fired;
        private volatile String suffix;

        @Override
        public long openRO(LPSZ name) {
            final String armed = suffix;
            if (armed != null) {
                if (Utf8s.endsWithAscii(name, Files.SEPARATOR + DurableEpochManifest.FILE_NAME + ".1")) {
                    // The older generation's checks begin; reading only its header to order the two does not count.
                    if (!isCalledFrom("readHeader")) {
                        suffix = null;
                    }
                } else if (Utf8s.endsWithAscii(name, armed) && isCalledFrom("areEpochCopiesValid")) {
                    fired = true;
                    return -1;
                }
            }
            return super.openRO(name);
        }

        void arm(String fileName) {
            fired = false;
            suffix = fileName == null ? null : Files.SEPARATOR + fileName;
        }

        void disarm() {
            suffix = null;
        }

        private static boolean isCalledFrom(String method) {
            return StackWalker.getInstance().walk(frames -> frames.anyMatch(f -> method.equals(f.getMethodName())));
        }
    }

    private enum TornMode {
        CORRUPT_TXN_EPOCH_BODY,
        FLIP_CV_BYTE,
        FLIP_META_BYTE,
        FLIP_TXN_BYTE,
        MANIFEST_SEQTXN_MISMATCH,
        TAMPERED_META_EPOCH,
        TORN_HEADER,
        TRUNCATE_TXN_EPOCH_TO_ZERO
    }
}
