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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TxReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

/**
 * The ADAPTIVE out-of-order (O3) crash sweep: drives an adaptive WAL table through
 * {@link AbstractAdaptiveCrashSweepTest#forEachAdaptiveCrashPoint} with a commit phase whose row
 * timestamps are out of order, so every apply after the first goes through the O3 merge path rather than
 * a tail append, and checks that adaptive recovery is crash-safe under O3.
 *
 * <p>The workload matches {@link AdaptiveCrashSweepSelfCheckTest}, except:
 * <ul>
 *   <li>{@code v} (commit order, 0..ROWS-1) and {@code ts} (designated timestamp order) are decoupled:
 *       row {@code v=0} carries the latest timestamp of the batch, a ceiling within the single
 *       {@code partition by day} bucket, and {@code v=1..5} each land at a distinct timestamp below that
 *       ceiling in zig-zag order. Every commit from {@code v=1} onward therefore has a minimum timestamp
 *       below the table's maximum timestamp, which routes WAL apply through the O3 merge path (see
 *       {@code TableWriter}'s {@code o3TimestampMin}/{@code getMaxTimestamp} handling and {@code
 *       WalEventCursor#isOutOfOrder}).</li>
 *   <li>The oracle orders by {@code v}, not {@code ts}: the surviving rows, sorted by {@code v}, must be
 *       an exact identity prefix {@code {0..m-1}}, whatever their physical order on disk.</li>
 * </ul>
 *
 * <p>At every crash point k the oracle asserts: (1) no silent corruption (surviving rows ordered by v are
 * an exact prefix, loud torn reads tolerated); (2) the table is not suspended and a follow-up insert and
 * read work; (3) full restore at k=N; (4) the recovered count does not decrease as k grows.
 */
public class AdaptiveO3CrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    // The phase of the DEDUP SKIP sweep: the switch to SKIP, then the writes of DEDUP_SKIP_WRITES.
    private static final int DEDUP_SKIP_COMMITS = 3;
    // The write after recovery: it resends the first stored row of the native day and adds a row on a new day.
    private static final String[] DEDUP_SKIP_FOLLOW_UP = {
            "2024-10-03T00:00:00.000000Z\t-1\tfollow\t-1",
            "2024-10-09T00:00:00.000000Z\t999\tfollow\t999"
    };
    private static final String DEDUP_SKIP_TABLE = "sweep_dedup_skip";
    /**
     * The writes of the DEDUP SKIP sweep's phase, as {@code CursorPrinter} prints their rows. The first only resends
     * stored keys, with other values: SKIP drops every row and writes no partition. The second resends a stored key
     * on every day and adds new rows: on the Parquet day that the merge rewrites, after a row group whose merge
     * writes nothing; on the Parquet day that the merge updates in place, after two such row groups; on the native
     * day, among and past the stored rows.
     */
    private static final String[][] DEDUP_SKIP_WRITES = {
            {
                    "2024-10-01T01:00:00.000000Z\t1001\tdup\t1",
                    "2024-10-01T09:00:00.000000Z\t1009\tdup\t9",
                    "2024-10-02T05:00:00.000000Z\t1105\tdup\t5",
                    "2024-10-02T10:00:00.000000Z\t1110\tdup\t10",
                    "2024-10-03T04:00:00.000000Z\t1204\tdup\t4",
                    "2024-10-03T16:00:00.000000Z\t1216\tdup\t16"
            },
            {
                    "2024-10-01T02:00:00.000000Z\t2002\tdup\t2",
                    "2024-10-01T06:30:00.000000Z\t2006\tnew\t6",
                    "2024-10-02T03:00:00.000000Z\t2103\tdup\t3",
                    "2024-10-02T05:00:00.000000Z\t2105\tdup\t5",
                    "2024-10-02T10:30:00.000000Z\t2110\tnew\t10",
                    "2024-10-03T08:00:00.000000Z\t2208\tdup\t8",
                    "2024-10-03T10:00:00.000000Z\t2210\tnew\t10",
                    "2024-10-03T22:00:00.000000Z\t2222\tnew\t22"
            }
    };
    private static final String FOLD_DAY = "2024-01-01";
    private static final long FOLD_DAY_MICROS = SavedLagWorkload.micros(FOLD_DAY + "T00:00:00.000000Z");
    private static final String FOLD_TABLE = "sweep_fold";

    private static final int ROWS = 6; // v = 0,1,2,3,4,5 (commit order)

    /**
     * Hour of day (single partition day 2024-10-01), indexed by {@code v}. Index 0 (v=0) is the ceiling:
     * the latest timestamp in the batch, inserted first. Every later element is strictly below the ceiling
     * and moves through the day in zig-zag order (down, up, down, up, down relative to the previous
     * element), so each of v=1..5 forces an O3 merge at a different position in the existing column data
     * rather than always at the head.
     *
     * <pre>
     *   v:      0   1   2   3   4   5
     *   hour:  20   8  14   2  11   5
     * </pre>
     * In ascending timestamp order the v labels are {@code [3,5,1,4,2,0]}, unrelated to the commit order
     * {@code [0,1,2,3,4,5]}.
     */
    private static final int[] TS_HOUR = {20, 8, 14, 2, 11, 5};

    /**
     * O3 rows applied before the durable epoch in the lazy-gap scenario.
     */
    private static final int LAZY_K = 4;
    /**
     * O3 rows applied lazily after the epoch, with epochs disabled: the rows at risk without recovery.
     */
    private static final int LAZY_M = 5;
    // Hours for the K+M=9 lazy-gap rows; index 0 is the ceiling (23), all others below it, in a
    // non-monotonic order (independent scenario from TS_HOUR, same O3-forcing technique).
    private static final int[] LAZY_TS_HOUR = {23, 9, 14, 4, 11, 2, 17, 7, 20};
    /**
     * The crash point of the lazy-gap comparison, set by phase rather than by an op count: this many lazy
     * rows are applied, each as its own txn, and the crash fires on the first durability op of the next lazy
     * row's commit, before that row's WAL is durable. Recovery restores the {@code LAZY_K} epoch rows plus
     * the applied rows, {@code [0, 1, 2, 3, 4]}, a partial result whatever the number of ops per commit.
     */
    private static final int LAZY_GAP_APPLIED_ROWS = 1;
    // Set while the fold sweep runs: its check of the restored state goes between recovery and the WAL replay.
    private FoldUnderSavePointWorkload foldWorkload;

    /**
     * DEDUP SKIP under the power-loss sweep. The phase switches a DEDUP UPSERT table to SKIP on the same key, then
     * resends stored keys with other values, see {@link #DEDUP_SKIP_WRITES}: first only resent keys, which SKIP
     * drops without writing any partition, then resent keys together with new rows. Of the two Parquet days the
     * merge rewrites the first, which lacks a column added after its conversion, and updates the second in place;
     * the last day is native. The count pass proves that the first write leaves every partition as it is, and that
     * the second rewrites the first Parquet day and updates the second one in place.
     * <p>
     * At every crash point recovery must apply every durable commit and land on the rows of that prefix of the
     * phase, under the mode it set, and a write after recovery that resends a stored key must keep the row the mode
     * picks: the stored one once the switch to SKIP is committed. At k=N every commit is recovered.
     * <p>
     * The native day merges into pieces, as the suite's default merge-append makes it; see
     * {@link #testDedupSkipSweepWithoutMergeAppendRecoversCleanlyAtEveryCrashPoint} for the production default.
     */
    @Test
    public void testDedupSkipSweepRecoversCleanlyAtEveryCrashPoint() throws Exception {
        assertDedupSkipSweepRecoversCleanly(true);
    }

    /**
     * {@link #testDedupSkipSweepRecoversCleanlyAtEveryCrashPoint} with merge-append off, the production default: the
     * native day merges without pieces.
     */
    @Test
    public void testDedupSkipSweepWithoutMergeAppendRecoversCleanlyAtEveryCrashPoint() throws Exception {
        assertDedupSkipSweepRecoversCleanly(false);
    }

    /**
     * Folds a day's split partitions into a composite target that only the durable-epoch pin holds, and crashes
     * at every durability op of that fold. The writer appends past the extent E the save point's geometry record
     * describes, in place, and publishes a new record beside the old one; recovery restores the save point and
     * must read exactly the day's rows from before the fold, and the WAL replay must then fold the day.
     * <p>
     * Setup takes a save point on the seeded day and then stops periodic save points, so that pin stays inside
     * the front's range: from its name txn to the split. A split and two narrow backdated writes then make the
     * front composite, and a manual save point lands on that shape; the seed's pin is now the older of the two.
     * The commit phase is one insert into a later day; its housekeeping runs the fold, and the save point after
     * it flushes the fold. The count pass proves the fold went in place: the day ends as one partition, the front
     * still composite, under its old name txn.
     */
    @Test
    public void testFoldIntoEpochHeldCompositeTargetSurvivesEveryCrashPoint() throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
            foldWorkload = new FoldUnderSavePointWorkload();
            try {
                final SweepResult r = forEachAdaptiveCrashPoint(foldWorkload);
                LOG.info().$("[fold under save point sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                        .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$(", ops:\n").$(phaseDurabilityOps()).$();
                Assert.assertFalse("the sweep must cover every crash point of the fold", r.truncated);
                Assert.assertEquals(r.n, r.sweptPoints);
            } finally {
                foldWorkload = null;
            }
        }));
    }

    /**
     * The O3 sweep: every commit-phase durability op is crashed at least once; recovery must leave an
     * identity prefix (ordered by v) at every k, never suspended, and restore everything at k=N.
     */
    @Test
    public void testO3SweepRecoversCleanlyAtEveryCrashPoint() throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final SweepResult r = forEachAdaptiveCrashPoint(new O3Workload());

            LOG.info().$("[O3 sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("default cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);

            // Oracle clause 4: recovered counts never decrease as k grows.
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // A genuine rise, not a degenerate all-full sweep: the earliest crash point recovers
            // strictly fewer than the full set.
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );

            // Oracle clause 3: at the last crash point, k=N, recovery restores every committed row.
            Assert.assertEquals(
                    "k=N must recover ALL committed rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * Runs the lazy-gap scenario at the same crash point with {@code RecoveryCoordinator.recover()} enabled
     * and disabled: {@code LAZY_K} O3 rows, a durable epoch ({@code interval=0}), then {@code LAZY_M} more
     * O3 rows applied lazily with epochs disabled ({@code interval=-1}), then a crash after
     * {@link #LAZY_GAP_APPLIED_ROWS} lazy applies, which leaves a partial result. Both arms must recover a
     * valid identity prefix, never suspended, and the two recovered row sets are identical: for O3 data at
     * this crash point, the {@code _txn} and {@code _cv} rewind to the epoch does not change the outcome.
     * For in-order data the same scenario loses the lazily applied rows when recovery is disabled.
     */
    @Test
    public void testRecoveryVsNoRecoveryIdenticalForO3LazyGapCrash() throws Exception {
        final List<Long> withRecovery = runLazyGapCrashScenario(LAZY_GAP_APPLIED_ROWS, true);
        final List<Long> withoutRecovery = runLazyGapCrashScenario(LAZY_GAP_APPLIED_ROWS, false);

        LOG.info().$("[O3 lazy-gap finding] appliedLazyRows=").$(LAZY_GAP_APPLIED_ROWS)
                .$(" withRecovery=").$(withRecovery.toString())
                .$(" withoutRecovery=").$(withoutRecovery.toString()).$();

        // Base oracle (both arms independently): no silent corruption -- an exact identity prefix.
        Assert.assertTrue("withRecovery must be a valid identity prefix: " + withRecovery,
                isIdentityPrefix(withRecovery));
        Assert.assertTrue("withoutRecovery must be a valid identity prefix: " + withoutRecovery,
                isIdentityPrefix(withoutRecovery));
        // Non-degenerate: a genuine partial result (not 0, not the full LAZY_K+LAZY_M), so the
        // comparison below is meaningful rather than vacuously true regardless of recovery.
        Assert.assertTrue(
                "the crash must land on a non-degenerate PARTIAL result for this "
                        + "comparison to be meaningful (withRecovery=" + withRecovery + ")",
                withRecovery.size() > LAZY_K && withRecovery.size() < (LAZY_K + LAZY_M)
        );
        Assert.assertEquals(
                "the crash must land after " + LAZY_GAP_APPLIED_ROWS + " lazy applies and before the next "
                        + "lazy row's WAL is durable (withRecovery=" + withRecovery + ")",
                LAZY_K + LAZY_GAP_APPLIED_ROWS, withRecovery.size()
        );

        // Recovery does not change the outcome at this crash point. If this assertion fails, recovery
        // matters for O3 here: invert the test to assert that the recovery-disabled arm loses rows, and
        // update the javadoc, rather than restoring this assertion.
        Assert.assertEquals(
                "recovery-enabled and recovery-disabled currently produce IDENTICAL results after "
                        + LAZY_GAP_APPLIED_ROWS + " lazy applies for this O3 lazy-gap scenario (the SP-D1 W1 finding)",
                withRecovery, withoutRecovery
        );
    }

    /**
     * Save point taken at the saved lag, then the consuming commit finishes and no newer save point
     * follows; the process is killed after that. Recovery restores the save point, whose {@code _txn}
     * counts the lag rows, and the consuming commit has rewritten their bytes in place. NOSYNC has no save
     * point and is safe in this case; ADAPTIVE is pinned.
     */
    @Test
    public void testSavedLagSavePointSurvivesKillAfterConsumingCommit() throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        runWithCrashFacade(() -> {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            final SavedLagWorkload workload = SavedLagWorkload.inPlaceAppend();
            workload.savePointsInCommitPhase = false;
            final Set<Long> nonCacheFdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());
            final TableToken[] tokens = workload.setup(1);
            workload.commit();
            Assert.assertEquals("the consuming commit left lag in _txn", 0, workload.savedLagRows());
            crashFf.takeKillImage(engine.getConfiguration().getDbRoot());
            recoverAfterCrash(tokens);
            workload.oracle(1, 1);
            workload.teardown();
            releaseEngineHandles();
            reclaimLingeringNonCacheFds(nonCacheFdBaseline);
        });
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointAcrossPartitionBoundary() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.acrossPartitionBoundary());
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointDedup() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.inPlaceAppendDedup("upsert"));
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointDedupSkip() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.inPlaceAppendDedup("skip"));
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointEmptyTable() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.emptyTable());
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointInPlaceAppend() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.inPlaceAppend());
    }

    @Test
    public void testSavedLagSurvivesEveryKillPointSplitLastPartition() throws Exception {
        assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload.splitLastPartition());
    }

    @Override
    protected void recoverAfterCrash(TableToken[] tokens) {
        if (foldWorkload == null) {
            super.recoverAfterCrash(tokens);
            return;
        }
        simulateRestart(tokens);
        new RecoveryCoordinator(engine).recover();
        foldWorkload.assertRestoredStateBeforeReplay();
        for (TableToken tt : tokens) {
            engine.notifyWalTxnRepublisher(tt);
        }
        drainWalQueue();
    }

    private void assertDedupSkipSweepRecoversCleanly(boolean isMergeAppend) throws Exception {
        withAdaptiveW0(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
            final SweepResult r = forEachAdaptiveCrashPoint(new DedupSkipWorkload(isMergeAppend));
            LOG.info().$("[dedup skip sweep] mergeAppend=").$(isMergeAppend).$(", N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$(", ops:\n").$(phaseDurabilityOps()).$();
            Assert.assertFalse("the sweep must cover every crash point of the phase", r.truncated);
            Assert.assertEquals(r.n, r.sweptPoints);
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered commits must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            Assert.assertEquals("the first crash point must lose the switch to SKIP", 0, r.recoveredByK()[1]);
            Assert.assertEquals("k=N must recover every commit of the phase", DEDUP_SKIP_COMMITS, r.recoveredByK()[r.sweptPoints]);
        }));
    }

    /**
     * A WAL apply pass that stops early saves the WAL lag in {@code _txn}; the lag rows sit in the last
     * partition's files after its committed rows, and the next commit consumes them. Kill the process at
     * every kill point of that consuming commit, in the suite's commit mode: after recovery the table must
     * hold every row of both WAL transactions, by value, because a kill keeps the WAL.
     */
    private void assertSavedLagSurvivesEveryKillPoint(SavedLagWorkload workload) throws Exception {
        runWithCrashFacade(() -> {
            final SweepResult r = forEachKillPoint(workload);
            LOG.info().$("[saved lag kill sweep] ").$(workload.name).$(" N=").$(r.n)
                    .$(", recoveredByK=").$(Arrays.toString(r.recoveredByK())).$();
            Assert.assertEquals(r.n + 1, r.sweptPoints);
        });
    }

    /**
     * Runs the LAZY_K/LAZY_M lazy-gap scenario once: LAZY_K O3 rows -> durable epoch -> LAZY_M more O3
     * rows applied lazily (epoch disabled) -> crash on the first durability op of the commit of lazy row
     * {@code appliedLazyRows} (counted from 0), after that many lazy applies ->
     * the recovery triple with {@code RecoveryCoordinator.recover()} included or skipped per
     * {@code recoveryOn} -> returns the recovered rows ordered by v (tolerant of a loud torn read).
     */
    private List<Long> runLazyGapCrashScenario(int appliedLazyRows, boolean recoveryOn) throws Exception {
        final List<Long>[] resultBox = new List[1];
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        staticOverrides.setAdaptiveRecoveryRollForwardEnabled(recoveryOn);
        try {
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)
                final String table = "o3_lazy_gap";
                execute("drop table if exists " + table);
                execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
                final TableToken tt = engine.verifyTableName(table);

                // LAZY_K rows, out-of-order among themselves -> apply -> durable epoch at seqTxn=LAZY_K
                // (interval=0 fires an epoch on this first applied batch).
                for (int i = 0; i < LAZY_K; i++) {
                    insertO3Row(table, LAZY_TS_HOUR[i], i);
                }
                drainWalQueue();

                // Disable further epochs: the next LAZY_M rows are applied lazily (no new durable cut).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);

                // Baseline of non-cached engine fds after a clean release (as in the sweep driver): a
                // simulated crash unwinds an fsync mid-operation on the live JVM, so a fault-injection fd
                // left open by the interrupted operation lingers where a real power loss's process death
                // would have reclaimed it. Reclaim the per-cycle delta at the end.
                final Set<Long> nonCacheFdBaseline = new HashSet<>(crashFf.noCacheOpenFdsSnapshot());

                markDurableBaseline();
                final int base = crashFf.durabilityOpCount();
                int crashOp = -1;
                try {
                    for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                        if (i == LAZY_K + appliedLazyRows) {
                            // the first durability op of this row's commit
                            crashOp = crashFf.durabilityOpCount() + 1;
                            crashFf.armCrashAt(crashOp);
                        }
                        insertO3Row(table, LAZY_TS_HOUR[i], i);
                        drainWalQueue();
                        if (anyTableSuspended(tt)) {
                            break;
                        }
                    }
                } catch (CrashSimulationError propagated) {
                    // expected: the armed crash fired during the WAL-commit fsync path
                }
                Assert.assertTrue(
                        "the lazy batch must reach row " + (LAZY_K + appliedLazyRows) + " to arm the crash",
                        crashOp > 0
                );
                Assert.assertFalse("the armed crash must fire", crashFf.isCrashArmed());
                final String crashedOp = crashFf.durabilityOpLog().get(crashOp - 1);
                LOG.info().$("[O3 lazy-gap finding] recovery=").$(recoveryOn)
                        .$(" crash at k=").$(crashOp - base).$(" of the lazy batch: ").$(crashedOp).$();

                // The driver's fresh-restart model; recover() runs only when recoveryOn is set.
                simulateRestart(new TableToken[]{tt});
                if (recoveryOn) {
                    new RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(tt);
                drainWalQueue();

                resultBox[0] = readVOrderedByVAllowTorn(table);

                execute("drop table if exists " + table);
                drainWalQueue();
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                engine.releaseInactiveTableSequencers();
                for (long fd : crashFf.noCacheOpenFdsSnapshot()) {
                    if (!nonCacheFdBaseline.contains(fd)) {
                        crashFf.forceClose(fd);
                    }
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
            staticOverrides.setAdaptiveRecoveryRollForwardEnabled(true);
        }
        return resultBox[0];
    }

    private static void insertO3Row(String table, int hour, int v) throws Exception {
        execute("insert into " + table + " values ('" + String.format("2024-10-01T%02d:00:00.000000Z", hour)
                + "', " + v + ")");
    }

    private void withAdaptiveW0(RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0"); // W = 0 (synchronous)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);        // durable epoch every batch
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            body.run();
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    private interface RunnableEx {
        void run() throws Exception;
    }

    /**
     * count(*): the committed row count from table metadata, reliable even if a column read would tear.
     */
    private long rowCount(String table) {
        try (RecordCursorFactory f = select("select count() from " + table)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                return c.hasNext() ? r.getLong(0) : 0L;
            }
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads {@code select v ... order by v}, returning the identity values gathered so far. A loud torn
     * read (CairoException, CairoError, or a SIGBUS InternalError on a truncated column) is an acceptable
     * crash outcome: the possibly partial result read before the tear is returned rather than rethrown.
     */
    private List<Long> readVOrderedByVAllowTorn(String table) {
        final List<Long> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select v from " + table + " order by v")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // acceptable: corruption detected loudly; return the prefix read before the tear
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * True iff {@code rows}, in order, is exactly the identity sequence 0,1,2,...
     */
    private boolean isIdentityPrefix(List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i) == null || rows.get(i) != (long) i) {
                return false;
            }
        }
        return true;
    }

    /**
     * The DEDUP SKIP workload, see {@link #testDedupSkipSweepRecoversCleanlyAtEveryCrashPoint}. A Java model of the
     * table gives the rows every recovered state must read: the rows setup writes, then the committed writes of the
     * phase, under SKIP, which keeps the stored row of a resent key.
     */
    private final class DedupSkipWorkload implements AdaptiveCrashWorkload {
        private final boolean isMergeAppend;
        private long baseSeqTxn;
        private int iteration;
        private TableToken tt;

        private DedupSkipWorkload(boolean isMergeAppend) {
            this.isMergeAppend = isMergeAppend;
        }

        @Override
        public void commit() throws Exception {
            execute("alter table " + DEDUP_SKIP_TABLE + " dedup enable skip keys(ts)");
            if (isApplyCrashed()) {
                return;
            }
            final String partitionsBefore = iteration == 0 ? readTxnToString(tt, false, true, true, true) : null;
            execute(insertSql(DEDUP_SKIP_WRITES[0]));
            if (isApplyCrashed()) {
                return;
            }
            long[][] versionsBefore = null;
            if (iteration == 0) {
                // the count pass, no crash armed: prove the writes take the paths the sweep is for
                Assert.assertEquals("the write that only resends keys changed a partition", partitionsBefore,
                        readTxnToString(tt, false, true, true, true));
                versionsBefore = partitionVersions();
            }
            execute(insertSql(DEDUP_SKIP_WRITES[1]));
            if (isApplyCrashed()) {
                return;
            }
            if (iteration == 0) {
                final long[][] versions = partitionVersions();
                Assert.assertEquals("the write changed the partition count", versionsBefore.length, versions.length);
                Assert.assertNotEquals("the merge did not rewrite the Parquet day that lacks a column",
                        versionsBefore[0][0], versions[0][0]);
                Assert.assertTrue("the rewrite left no Parquet file", versions[0][1] > -1);
                Assert.assertEquals("the merge did not update the second Parquet day in place",
                        versionsBefore[1][0], versions[1][0]);
                Assert.assertTrue("the update in place did not grow the Parquet file", versions[1][1] > versionsBefore[1][1]);
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            final String at = "k=" + k + " of " + n + " [op=" + phaseDurabilityOp(k) + "]";
            Assert.assertNull(at + ": the table is suspended after recovery", firstSuspendReason(tt));
            final long seqTxn;
            final boolean isSkip;
            try (TableReader reader = engine.getReader(tt)) {
                seqTxn = reader.getSeqTxn();
                final TableReaderMetadata metadata = reader.getMetadata();
                Assert.assertTrue(at + ": the timestamp is no longer a dedup key", metadata.isDedupKey(metadata.getTimestampIndex()));
                isSkip = metadata.getDedupMode() == TableUtils.DEDUP_MODE_SKIP;
            }
            // under W=0 every commit that returned is durable, and recovery applies every durable commit
            Assert.assertEquals(at + ": recovery left a durable commit unapplied", engine.getTableSequencerAPI().lastTxn(tt), seqTxn);
            final int committed = (int) (seqTxn - baseSeqTxn);
            Assert.assertTrue(at + ": recovered " + committed + " commits of the phase", committed >= 0 && committed <= DEDUP_SKIP_COMMITS);
            Assert.assertEquals(at + ": wrong dedup mode after " + committed + " commits of the phase [isSkip=" + isSkip + ']',
                    committed > 0, isSkip);
            final TreeMap<String, String> rows = seedRows();
            for (int i = 1; i < committed; i++) {
                putRows(rows, DEDUP_SKIP_WRITES[i - 1], true);
            }
            assertRows(at + " after recovery", rows);
            // a resent key: the mode the recovered table runs decides which row stays
            execute(insertSql(DEDUP_SKIP_FOLLOW_UP));
            drainWalQueue();
            Assert.assertNull(at + ": the write after recovery suspended the table", firstSuspendReason(tt));
            putRows(rows, DEDUP_SKIP_FOLLOW_UP, committed > 0);
            assertRows(at + " after a write", rows);
            engine.releaseAllReaders();
            return committed;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            this.iteration = iteration;
            execute("drop table if exists " + DEDUP_SKIP_TABLE);
            drainWalQueue();
            // three row groups of 4 rows on each Parquet day
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, isMergeAppend ? "true" : "false");
            execute("create table " + DEDUP_SKIP_TABLE + " (ts timestamp, v long, vc varchar) timestamp(ts) partition by day wal"
                    + " dedup upsert keys(ts)");
            // the rows of seedRows()
            execute("insert into " + DEDUP_SKIP_TABLE + " (ts, v, vc) select '2024-10-01'::timestamp + (x - 1) * 3_600_000_000,"
                    + " x, 'a' || x from long_sequence(12)");
            execute("insert into " + DEDUP_SKIP_TABLE + " (ts, v, vc) select '2024-10-02'::timestamp + (x - 1) * 3_600_000_000,"
                    + " 100 + x, 'b' || x from long_sequence(12)");
            execute("insert into " + DEDUP_SKIP_TABLE + " (ts, v, vc) select '2024-10-03'::timestamp + (x - 1) * 14_400_000_000,"
                    + " 200 + x, 'c' || x from long_sequence(6)");
            execute("alter table " + DEDUP_SKIP_TABLE + " convert partition to parquet list '2024-10-01'");
            // the first Parquet day lacks the column, so a merge that writes rows there rewrites its file
            execute("alter table " + DEDUP_SKIP_TABLE + " add column w int");
            execute("alter table " + DEDUP_SKIP_TABLE + " convert partition to parquet list '2024-10-02'");
            drainWalQueue();
            tt = engine.verifyTableName(DEDUP_SKIP_TABLE);
            baseSeqTxn = engine.getTableSequencerAPI().lastTxn(tt);
            if (iteration == 0) {
                final long[][] versions = partitionVersions();
                Assert.assertTrue("the fixture must hold two Parquet days and a native one",
                        versions.length == 3 && versions[0][1] > -1 && versions[1][1] > -1 && versions[2][1] == -1);
            }
            return new TableToken[]{tt};
        }

        @Override
        public void teardown() throws Exception {
            try {
                execute("drop table if exists " + DEDUP_SKIP_TABLE);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[dedup skip sweep] teardown drop skipped: ").$(e.getMessage()).$();
            }
        }

        private static String insertSql(String[] rows) {
            final StringBuilder sql = new StringBuilder("insert into ").append(DEDUP_SKIP_TABLE).append(" (ts, v, vc, w) values ");
            for (int i = 0; i < rows.length; i++) {
                final String[] cells = rows[i].split("\t");
                sql.append(i > 0 ? ", ('" : "('").append(cells[0]).append("', ").append(cells[1]).append(", '")
                        .append(cells[2]).append("', ").append(cells[3]).append(')');
            }
            return sql.toString();
        }

        // Writes rows to the model: SKIP keeps the stored row of a resent key, UPSERT replaces it.
        private static void putRows(TreeMap<String, String> rows, String[] written, boolean isSkip) {
            for (String row : written) {
                final String ts = row.substring(0, row.indexOf('\t'));
                if (isSkip) {
                    rows.putIfAbsent(ts, row);
                } else {
                    rows.put(ts, row);
                }
            }
        }

        // The rows setup() writes, by timestamp. The column w comes after them, so it is NULL in all of them.
        private static TreeMap<String, String> seedRows() {
            final TreeMap<String, String> rows = new TreeMap<>();
            for (int x = 1; x <= 12; x++) {
                final String a = String.format("2024-10-01T%02d:00:00.000000Z", x - 1);
                rows.put(a, a + "\t" + x + "\ta" + x + "\tnull");
                final String b = String.format("2024-10-02T%02d:00:00.000000Z", x - 1);
                rows.put(b, b + "\t" + (100 + x) + "\tb" + x + "\tnull");
            }
            for (int x = 1; x <= 6; x++) {
                final String c = String.format("2024-10-03T%02d:00:00.000000Z", (x - 1) * 4);
                rows.put(c, c + "\t" + (200 + x) + "\tc" + x + "\tnull");
            }
            return rows;
        }

        // Checks the rows, and the recovered table's cursor reset, size and random access, through the assertQuery()
        // battery. The failure names the crash point.
        private void assertRows(String at, TreeMap<String, String> rows) throws Exception {
            final StringSink expected = new StringSink();
            expected.put("ts\tv\tvc\tw\n");
            for (String row : rows.values()) {
                expected.put(row).put('\n');
            }
            try {
                assertQuery("select ts, v, vc, w from " + DEDUP_SKIP_TABLE)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .returns(expected);
            } catch (AssertionError e) {
                throw new AssertionError(at + ": " + e.getMessage(), e);
            }
        }

        // Applies the WAL. True when an armed crash suspended the table, which ends the phase; the count pass has
        // no crash armed, so a suspend there fails it.
        private boolean isApplyCrashed() {
            drainWalQueue();
            if (iteration == 0) {
                Assert.assertNull("the uncrashed phase suspended the table", firstSuspendReason(tt));
            }
            return anyTableSuspended(tt);
        }

        // The name txn of every partition and the size of its Parquet file, -1 for a native partition, from _txn.
        private long[][] partitionVersions() {
            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade()); Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
                txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                txReader.unsafeLoadAll();
                final long[][] versions = new long[txReader.getPartitionCount()][];
                for (int i = 0; i < versions.length; i++) {
                    versions[i] = new long[]{
                            txReader.getPartitionNameTxn(i),
                            txReader.isPartitionParquet(i) ? txReader.getPartitionParquetFileSize(i) : -1
                    };
                }
                return versions;
            }
        }
    }

    /**
     * The fold of a day's split partitions into a composite target held only by the durable-epoch pin; see
     * {@link #testFoldIntoEpochHeldCompositeTargetSurvivesEveryCrashPoint}. Every iteration rebuilds the same
     * shape, so the day's row count, its sum and its cursor-order fingerprint from setup are the values every
     * recovered state must show.
     */
    private final class FoldUnderSavePointWorkload implements AdaptiveCrashWorkload {
        private String dayFingerprint;
        private long dayRows;
        private long daySum;
        private long frontExtent;
        private long frontNameTxn;
        private int iteration;
        private TableToken tt;

        @Override
        public void commit() throws Exception {
            // The fold is the opportunistic one: one split allowed, and the siblings count as cold at once.
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 0);
            // The apply job's save point follows the commit, so the fold still runs under the setup's pins, and
            // the save point then flushes it: a crash inside that save point leaves the fold's bytes past E on
            // disk while recovery restores the setup's save point.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("insert into " + FOLD_TABLE + " (i, ts) values (999_999, '2024-01-06T00:00:00.000000Z')");
            drainWalQueue();
            if (iteration == 0) {
                // The count pass, no crash armed: prove the fold ran, and ran in place.
                Assert.assertFalse("the uncrashed fold suspended the table", anyTableSuspended(tt));
                try (TableReader reader = engine.getReader(tt)) {
                    final TxReader txFile = reader.getTxFile();
                    Assert.assertEquals("the fold did not run", 1, dayPartitionCount(txFile));
                    final int front = txFile.getPartitionIndex(FOLD_DAY_MICROS);
                    Assert.assertTrue("the fold flattened the composite target", txFile.isPartitionComposite(front));
                    Assert.assertEquals("the fold copied the target instead of appending in place",
                            frontNameTxn, txFile.getPartitionNameTxn(front));
                    Assert.assertTrue("the fold appended nothing past the target's extent",
                            reader.getPartitionPhysicalRowCount(front) > frontExtent);
                }
                assertDayUnchanged("after the uncrashed fold");
                engine.releaseAllReaders();
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            Assert.assertFalse(
                    "k=" + k + ": table must NOT be suspended after recovery",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );
            assertDayUnchanged("k=" + k + " of " + n + " after the replay");
            final int recovered = (int) rowCount(FOLD_TABLE);
            // A crash on the insert's own WAL fsync loses the insert, and with it the commit that runs the fold:
            // a follow-up write in a later day drives the housekeeping, and must land on the recovered table.
            execute("insert into " + FOLD_TABLE + " (i, ts) values (888_888, '2024-01-09T00:00:00.000000Z')");
            drainWalQueue();
            Assert.assertFalse("k=" + k + ": the follow-up write suspended the table", anyTableSuspended(tt));
            Assert.assertEquals("k=" + k + ": the follow-up write did not land", recovered + 1, rowCount(FOLD_TABLE));
            assertDayUnchanged("k=" + k + " of " + n + " after the follow-up write");
            try (TableReader reader = engine.getReader(tt)) {
                Assert.assertEquals("k=" + k + ": the day was not folded", 1, dayPartitionCount(reader.getTxFile()));
            }
            engine.releaseAllReaders();
            return recovered;
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            this.iteration = iteration;
            execute("drop table if exists " + FOLD_TABLE);
            drainWalQueue();
            // Hold the siblings still until the commit phase; the previous iteration's commit lowered the limits.
            setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4 << 10);
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MAX_SPLITS, 1_000);
            setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1_000);
            setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_HOT_COMMITS, 10);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("create table " + FOLD_TABLE + " as (select x::int i, timestamp_sequence('" + FOLD_DAY
                    + "', 1_000_000L) ts from long_sequence(4_000)) timestamp(ts) partition by day wal");
            // The last periodic save point: its pin falls in the front's range, which ends at the split, and
            // stays there until two more save points.
            drainWalQueue();
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            // Cut a sibling off the day's tail with an O3 write inside it; merge-append would rewrite in place.
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            execute("insert into " + FOLD_TABLE + " (i, ts) select x::int + 200_000, timestamp_sequence('"
                    + FOLD_DAY + "T00:50:00', 1_000L) from long_sequence(200)");
            drainWalQueue();
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
            // Two narrow backdated writes over the same rows make the front composite: merge-append rewrites
            // the owning piece at the file tail and leaves dead space behind.
            for (int i = 0; i < 2; i++) {
                execute("insert into " + FOLD_TABLE + " (i, ts) select x::int + 400_000, timestamp_sequence('"
                        + FOLD_DAY + "T00:10:00', 1_000_000L) from long_sequence(200)");
                drainWalQueue();
            }
            // A later day, so neither sibling is the last partition: the fold leaves a composite last one alone.
            execute("insert into " + FOLD_TABLE + " (i, ts) select x::int + 900_000,"
                    + " timestamp_sequence('2024-01-05', 1_000_000L) from long_sequence(100)");
            drainWalQueue();
            tt = engine.verifyTableName(FOLD_TABLE);
            // The save point recovery restores: it names the front's composite geometry record and its extent E.
            try (TableWriter writer = getWriter(tt)) {
                writer.advanceDurableEpoch(engine.getConfiguration().getMicrosecondClock().getTicks() / 1000L);
            }

            final long fromTxn;
            final long toTxn;
            try (TableReader reader = engine.getReader(tt)) {
                final TxReader txFile = reader.getTxFile();
                Assert.assertEquals("fixture must leave the day split in two", 2, dayPartitionCount(txFile));
                final int front = txFile.getPartitionIndex(FOLD_DAY_MICROS);
                Assert.assertTrue("fixture left the fold target plain", txFile.isPartitionComposite(front));
                frontExtent = reader.getPartitionPhysicalRowCount(front);
                Assert.assertTrue("fixture left the composite target with no dead space [E=" + frontExtent
                                + ", liveRows=" + txFile.getPartitionSize(front) + ']',
                        frontExtent > txFile.getPartitionSize(front));
                frontNameTxn = txFile.getPartitionNameTxn(front);
                // The range TableWriter.scanPartitionTailHolders checks for a target followed by its own sibling.
                fromTxn = frontNameTxn + 1;
                toTxn = txFile.getPartitionNameTxn(front + 1) + 1;
            }
            dayRows = scalar("select count() from " + FOLD_TABLE + " where ts in '" + FOLD_DAY + "'");
            daySum = scalar("select sum(i) from " + FOLD_TABLE + " where ts in '" + FOLD_DAY + "'");
            dayFingerprint = fingerprintOfDay();
            engine.releaseAllReaders();
            Assert.assertEquals("a reader holds the fold target, the fold would wait for it", 0, engine.getBusyReaderCount());
            Assert.assertFalse("the durable-epoch pin does not hold the fold target's range [" + fromTxn + ", " + toTxn
                            + "), so the fold would not run under it",
                    engine.getTableSequencerAPI().getTxnTracker(tt).isRangeAvailableToEpoch(fromTxn, toTxn));
            return new TableToken[]{tt};
        }

        @Override
        public void teardown() throws Exception {
            try {
                execute("drop table if exists " + FOLD_TABLE);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[fold under save point sweep] teardown drop skipped: ").$(e.getMessage()).$();
            }
        }

        private static int dayPartitionCount(TxReader txFile) {
            int count = 0;
            for (int i = 0, n = txFile.getPartitionCount(); i < n; i++) {
                final long ts = txFile.getPartitionTimestampByIndex(i);
                if (ts >= FOLD_DAY_MICROS && ts < FOLD_DAY_MICROS + 86_400_000_000L) {
                    count++;
                }
            }
            return count;
        }

        private static String fingerprintOfDay() throws Exception {
            long count = 0;
            long hash = 0;
            try (RecordCursorFactory f = select("select ts, i from " + FOLD_TABLE + " where ts in '" + FOLD_DAY + "'")) {
                try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                    final Record r = c.getRecord();
                    while (c.hasNext()) {
                        count++;
                        hash = hash * 1_000_003L + r.getTimestamp(0);
                        hash = hash * 1_000_003L + r.getInt(1);
                    }
                }
            }
            return count + "/" + hash;
        }

        private static long scalar(String sql) throws Exception {
            try (RecordCursorFactory f = select(sql)) {
                try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue("query returned no row: " + sql, c.hasNext());
                    return c.getRecord().getLong(0);
                }
            }
        }

        private void assertDayUnchanged(String when) throws Exception {
            Assert.assertEquals(when + ": the day's row count changed", dayRows,
                    scalar("select count() from " + FOLD_TABLE + " where ts in '" + FOLD_DAY + "'"));
            Assert.assertEquals(when + ": the day's values changed", daySum,
                    scalar("select sum(i) from " + FOLD_TABLE + " where ts in '" + FOLD_DAY + "'"));
            Assert.assertEquals(when + ": the day's rows or their order changed", dayFingerprint, fingerprintOfDay());
        }

        /**
         * Runs between recovery and the WAL replay. Recovery restores the setup's save point, which names the
         * front's composite record: the day must read exactly as before the fold, and the restored {@code _txn}
         * must not name the fold's new record, whose pieces reach past that save point's extent E. A state in
         * which the fold survived whole, one partition for the day, is accepted too.
         */
        void assertRestoredStateBeforeReplay() {
            try {
                try (TableReader reader = engine.getReader(tt)) {
                    final TxReader txFile = reader.getTxFile();
                    final int dayPartitions = dayPartitionCount(txFile);
                    LOG.info().$("[fold under save point sweep] restored before replay [dayPartitions=").$(dayPartitions)
                            .$(", txn=").$(txFile.getTxn()).I$();
                    if (dayPartitions != 1) {
                        Assert.assertEquals("recovery left the day in neither the save point's shape nor the fold's",
                                2, dayPartitions);
                        final int front = txFile.getPartitionIndex(FOLD_DAY_MICROS);
                        Assert.assertTrue("recovery left the fold target plain", txFile.isPartitionComposite(front));
                        Assert.assertEquals("recovery moved the fold target to another directory",
                                frontNameTxn, txFile.getPartitionNameTxn(front));
                        Assert.assertEquals("the restored _txn names a geometry record past the save point's extent",
                                frontExtent, reader.getPartitionPhysicalRowCount(front));
                    }
                }
                assertDayUnchanged("before the replay");
                engine.releaseAllReaders();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * The O3 workload: an ADAPTIVE {@code (ts timestamp, v long)} WAL table, single {@code
     * partition by day} bucket, {@code v=0..ROWS-1} identity in commit order, timestamps out of order
     * (see {@link #TS_HOUR}) so every apply after v=0 goes through the O3 merge path.
     */
    private final class O3Workload implements AdaptiveCrashWorkload {
        private String table;
        private TableToken tt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            table = "sweep_o3";
            execute("drop table if exists " + table);
            drainWalQueue();
            execute("create table " + table + " (ts timestamp, v long) timestamp(ts) partition by day wal");
            tt = engine.verifyTableName(table);
            return new TableToken[]{tt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = 0; i < ROWS; i++) {
                // v=0 sets the ceiling (latest ts in the batch); v=1..5 each land below it at a
                // non-monotonic position -> every apply after the first is an O3 merge, not an append.
                // W=0: the WAL commit fdatasyncs synchronously here -> an armed crash on that op
                // propagates a CrashSimulationError out of execute().
                insertO3Row(table, TS_HOUR[i], i);
                // ADAPTIVE apply + durable epoch -> more durability ops. An armed crash here is
                // swallowed by ApplyWal2TableJob's catch(Throwable) into a table suspend (no throw).
                drainWalQueue();
                // Stop as soon as the crash has fired (as a real power loss would): if it manifested as
                // a suspend, further inserts would durably extend the WAL and mask this injection point.
                if (anyTableSuspended(tt)) {
                    return;
                }
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // Clean reopen: recovery must not leave the table suspended.
            Assert.assertFalse(
                    "k=" + k + ": table must NOT be suspended after recovery",
                    engine.getTableSequencerAPI().isSuspended(tt)
            );

            // No silent corruption: surviving rows, ordered by v (ts is scrambled on purpose), are an
            // exact identity prefix {0..m-1}. A loud torn read is tolerated (returns the prefix read so
            // far); a wrong or absent value inside the prefix fails the test.
            final List<Long> rows = readVOrderedByVAllowTorn(table);
            for (int i = 0; i < rows.size(); i++) {
                Assert.assertNotNull("k=" + k + " row " + i + " read back NULL (corruption)", rows.get(i));
                Assert.assertEquals(
                        "k=" + k + " row " + i + " silently WRONG (not an identity prefix ordered by v)",
                        (long) i, (long) rows.get(i)
                );
            }

            // Recovered committed-row count from the metadata (reliable even if a column read tore).
            final int recovered = (int) rowCount(table);
            Assert.assertTrue(
                    "k=" + k + ": a torn read cannot show MORE identity rows than were committed",
                    rows.size() <= recovered
            );

            if (k == n) {
                Assert.assertEquals("k=N: recovery must restore ALL committed rows", ROWS, recovered);
                Assert.assertEquals("k=N: the full identity set must read back clean", ROWS, rows.size());
            }

            // Clean reopen: a follow-up write and read must succeed on the recovered table. A later day
            // (a fresh partition, plain append) keeps the follow-up check independent of the O3 path.
            execute("insert into " + table + " values ('2024-10-09T00:00:00.000000Z', 999)");
            drainWalQueue();
            Assert.assertEquals(
                    "k=" + k + ": follow-up insert must land on the recovered table",
                    recovered + 1, rowCount(table)
            );
            return recovered;
        }

        @Override
        public void teardown() throws Exception {
            // The next setup drops the table first, so a single reused table name keeps exactly one table
            // alive at a time, bounding on-disk state and engine-registry churn across the whole sweep.
            // Best-effort final drop so the last iteration leaves nothing.
            try {
                execute("drop table if exists " + table);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[O3 sweep] teardown drop skipped for ").$(table).$(": ").$(e.getMessage()).$();
            }
        }
    }

    /**
     * The saved-lag workload. Setup: the table's committed rows, then two WAL transactions: the first
     * holds exactly the table's lag limit of rows ({@code maxUncommittedRows} times the lag multiplier),
     * the second one row out of order below them, written through a second WAL so that no later
     * transaction shares the first one's WAL segment. On a DEDUP table the second transaction also
     * resends the key of the first lag row with other values. One apply pass with a zero time quota applies the
     * first transaction on its own, keeps its rows in WAL lag (the second sorts below them), stops on the
     * quota and saves the lag in {@code _txn}; under ADAPTIVE a save point lands on it. Setup asserts
     * {@code _txn} holds that lag, so the test cannot pass on a workload that saved none, and
     * then runs the WAL purge, which must not take the lag's WAL segment. Commit phase: the drain that
     * consumes the lag. Oracle: every row of the table and of both transactions, by value; of the resent key,
     * UPSERT keeps the row of the second transaction and SKIP the lag row.
     */
    private static final class SavedLagWorkload implements AdaptiveCrashWorkload {
        // the mode of DEDUP ENABLE on the timestamp, "upsert" or "skip"; null when the table does not deduplicate
        private final String dedupMode;
        private final List<String> expectedRows = new ArrayList<>();
        private final String[] initialInserts;
        private final int lagRows;
        private final int maxUncommittedRows;
        private final String name;
        private final String outOfOrderTs;
        private final String table;
        private final String[] lagInsert;
        private final boolean splitLastPartition;
        boolean savePointsInCommitPhase = true;
        private TableToken tt;

        private SavedLagWorkload(
                String name,
                int maxUncommittedRows,
                String dedupMode,
                boolean splitLastPartition,
                String[] initialInserts,
                String[] lagInsert,
                String outOfOrderTs
        ) {
            this.name = name;
            this.maxUncommittedRows = maxUncommittedRows;
            this.dedupMode = dedupMode;
            this.splitLastPartition = splitLastPartition;
            this.initialInserts = initialInserts;
            this.lagInsert = lagInsert;
            this.lagRows = Integer.parseInt(lagInsert[1]);
            this.outOfOrderTs = outOfOrderTs;
            // a table per workload: a failing workload must not leave its table to the next one
            this.table = "sweep_lag_" + name;
            for (int i = 0; i < initialInserts.length; i += 3) {
                addExpectedRows(initialInserts[i], Integer.parseInt(initialInserts[i + 1]), Long.parseLong(initialInserts[i + 2]));
            }
            addExpectedRows(lagInsert[0], lagRows, Long.parseLong(lagInsert[2]));
            expectedRows.add(row(micros(outOfOrderTs), -1, "neg", "neg"));
            if ("upsert".equals(dedupMode)) {
                // the second transaction wrote the resent key last; SKIP keeps the lag row the first one wrote
                final long firstLagTs = micros(lagInsert[0]);
                Assert.assertTrue(expectedRows.remove(generatedRow(firstLagTs, 1, Long.parseLong(lagInsert[2]))));
                expectedRows.add(row(firstLagTs, -2, "dup", "dup"));
            }
            expectedRows.sort(null);
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            if (engine.getTableSequencerAPI().isSuspended(tt)) {
                final io.questdb.cairo.wal.seq.SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(tt);
                Assert.fail(name + " kill point k=" + k + " of " + n + ": the table is suspended after recovery [error="
                        + tracker.getErrorMessage() + "]");
            }
            final List<String> actual = new ArrayList<>();
            try (RecordCursorFactory f = select("select ts, v, s, vc from " + table)) {
                try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                    final Record r = c.getRecord();
                    while (c.hasNext()) {
                        final CharSequence sym = r.getSymA(2);
                        final io.questdb.std.str.Utf8Sequence vc = r.getVarcharA(3);
                        actual.add(row(r.getTimestamp(0), r.getLong(1), sym == null ? null : sym.toString(), vc == null ? null : vc.toString()));
                    }
                }
            }
            actual.sort(null);
            if (!expectedRows.equals(actual)) {
                int firstDiff = 0;
                while (firstDiff < Math.min(actual.size(), expectedRows.size()) && actual.get(firstDiff).equals(expectedRows.get(firstDiff))) {
                    firstDiff++;
                }
                Assert.fail(name + " kill point k=" + k + " of " + n + ": the table does not hold the rows of both WAL transactions"
                        + " [rows=" + actual.size() + ", sum(v)=" + sumV(actual)
                        + ", expected rows=" + expectedRows.size() + ", sum(v)=" + sumV(expectedRows)
                        + ", first difference at " + firstDiff + ": "
                        + (firstDiff < actual.size() ? actual.get(firstDiff) : "<none>") + " instead of "
                        + (firstDiff < expectedRows.size() ? expectedRows.get(firstDiff) : "<none>") + "]");
            }
            return actual.size();
        }

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            execute("drop table if exists " + table);
            drainWalQueue();
            // set inside the assertMemoryLeak body: a quota set before it does not reach the engine
            setProperty(PropertyKey.CAIRO_WAL_APPLY_TABLE_TIME_QUOTA, 0);
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            // A merge-append table keeps no WAL lag, and the suite turns merge-append on.
            setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "false");
            // Only a one-txn block keeps lag. With cairo.wal.apply.block.size on, the block row limit dwarfs the
            // lag txn, so the out-of-order txn would join it in one block and commit it in full.
            setProperty(PropertyKey.DEBUG_WAL_APPLY_MAX_TXN_BLOCK_SIZE, 1);
            if (splitLastPartition) {
                setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 1);
            }
            execute("create table " + table + " (ts timestamp, v long, s symbol, vc varchar) timestamp(ts) partition by day wal"
                    + " with maxUncommittedRows=" + maxUncommittedRows);
            if (dedupMode != null) {
                execute("alter table " + table + " dedup enable " + dedupMode + " keys(ts)");
            }
            tt = engine.verifyTableName(table);
            for (int i = 0; i < initialInserts.length; i += 3) {
                execute(insertSql(initialInserts[i], initialInserts[i + 1], initialInserts[i + 2]));
            }
            drainWalQueue();

            execute(insertSql(lagInsert[0], lagInsert[1], lagInsert[2]));
            // hold the first WAL so the out-of-order row goes to a second one
            try (WalWriter ignore = engine.getWalWriter(tt)) {
                execute("insert into " + table + " values ('" + outOfOrderTs + "', -1, 'neg', 'neg')"
                        + (dedupMode != null ? ", ('" + lagInsert[0] + "', -2, 'dup', 'dup')" : ""));
            }
            tickWalQueue(1);
            Assert.assertEquals(name + ": _txn does not hold the saved WAL lag", lagRows, savedLagRows());
            engine.releaseAllWalWriters();
            drainPurgeJob();
            if (!savePointsInCommitPhase) {
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            }
            return new TableToken[]{tt};
        }

        @Override
        public void commit() {
            drainWalQueue();
        }

        @Override
        public void teardown() throws Exception {
            try {
                execute("drop table if exists " + table);
                drainWalQueue();
            } catch (Exception e) {
                LOG.info().$("[saved lag sweep] teardown drop skipped: ").$(e.getMessage()).$();
            }
        }

        static SavedLagWorkload acrossPartitionBoundary() {
            // lag limit 2,000 rows: 600 lag rows on day 2, 1,400 on day 3; the out-of-order row lands on
            // day 1, the lag's day-2 rows append in place, and day 2 is trimmed when day 3 becomes last
            return new SavedLagWorkload(
                    "acrossPartitionBoundary", 100, null, false,
                    new String[]{"2022-02-24T00:00:00.000000Z", "1", "1000000", "2022-02-25T00:00:00.000000Z", "1", "2000000"},
                    new String[]{"2022-02-25T23:50:00.000000Z", "2000", "0"},
                    "2022-02-24T12:00:00.000000Z"
            );
        }

        static SavedLagWorkload emptyTable() {
            // no committed rows: the lag lives in an artificial partition the consuming commit removes
            return new SavedLagWorkload(
                    "emptyTable", 50, null, false,
                    new String[0],
                    new String[]{"2022-02-25T01:00:00.000000Z", "1000", "0"},
                    "2022-02-25T00:30:00.000000Z"
            );
        }

        static SavedLagWorkload inPlaceAppend() {
            return inPlaceAppend("inPlaceAppend", null);
        }

        static SavedLagWorkload inPlaceAppendDedup(String dedupMode) {
            return inPlaceAppend("skip".equals(dedupMode) ? "inPlaceAppendDedupSkip" : "inPlaceAppendDedup", dedupMode);
        }

        static SavedLagWorkload splitLastPartition() {
            // the out-of-order row lands inside the committed tail; with a 1-byte split threshold the
            // consuming commit splits the last partition and cuts the old directory to its committed rows
            return new SavedLagWorkload(
                    "splitLastPartition", 50, null, true,
                    new String[]{"2022-02-25T00:00:00.000000Z", "5000", "1000000"},
                    new String[]{"2022-02-25T02:00:00.000000Z", "1000", "0"},
                    "2022-02-25T01:23:00.500000Z"
            );
        }

        private static String generatedRow(long fromTs, int x, long vBase) {
            // row x of insertSql(), from 1
            final long v = vBase + x;
            return row(fromTs + (x - 1) * 1_000_000L, v, "k" + (x % 7), "r" + v);
        }

        private static SavedLagWorkload inPlaceAppend(String name, String dedupMode) {
            // one committed row; the out-of-order row sorts between it and the lag, so the consuming commit
            // appends the merged rows in place over the lag
            return new SavedLagWorkload(
                    name, 50, dedupMode, false,
                    new String[]{"2022-02-25T00:00:00.000000Z", "1", "1000000"},
                    new String[]{"2022-02-25T01:00:00.000000Z", "1000", "0"},
                    "2022-02-25T00:30:00.000000Z"
            );
        }

        private static long micros(String ts) {
            final Instant instant = Instant.parse(ts);
            return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1_000L;
        }

        private static String row(long ts, long v, String s, String vc) {
            // zero-padded, so the natural string order is the timestamp order
            return String.format("%020d|%d|%s|%s", ts, v, s, vc);
        }

        private static long sumV(List<String> rows) {
            long sum = 0;
            for (String r : rows) {
                final int from = r.indexOf('|') + 1;
                sum += Long.parseLong(r.substring(from, r.indexOf('|', from)));
            }
            return sum;
        }

        private void addExpectedRows(String fromTs, int rows, long vBase) {
            final long from = micros(fromTs);
            for (int x = 1; x <= rows; x++) {
                expectedRows.add(generatedRow(from, x, vBase));
            }
        }

        private String insertSql(String fromTs, String rows, String vBase) {
            return "insert into " + table + " select timestamp_sequence('" + fromTs + "', 1_000_000L), " + vBase + " + x,"
                    + " 'k' || (x % 7), 'r' || (" + vBase + " + x) from long_sequence(" + rows + ")";
        }

        int savedLagRows() {
            try (TxReader txReader = new TxReader(engine.getConfiguration().getFilesFacade()); Path path = new Path()) {
                path.of(engine.getConfiguration().getDbRoot()).concat(tt).concat(TableUtils.TXN_FILE_NAME);
                txReader.ofRO(path.$(), ColumnType.TIMESTAMP, PartitionBy.DAY);
                txReader.unsafeLoadAll();
                return txReader.getLagRowCount();
            }
        }
    }
}
