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
import io.questdb.cairo.CommitMode;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RecoveryCoordinator;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.mv.MatViewState;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Does a materialized view and its adaptive base table recover to a mutually consistent state after a crash at
 * every durability op of a sustained lazy gap?
 *
 * <p>{@link RecoveryCoordinator#recover()} recovers mat views: its loop skips regular views, but a mat view is
 * {@code isView()==false} with an on-disk {@code _meta}/{@code _txn}/{@code _cv} and, under global adaptive, an
 * effective adaptive commit mode, so it goes through {@code recoverTable} like a normal table. This sweep exercises
 * that path together with the {@code _mv.s} refresh watermark.
 *
 * <h3>The schema and the sustained lazy gap</h3>
 * A base adaptive WAL table {@code base(ts, v)} and a mat view {@code mv} =
 * {@code select ts, count() cnt from base sample by 1h}. Each base row lands in a distinct hour of a single day
 * (out of order among themselves, so every apply after the first takes the O3 merge path), so the mv is a
 * bijection: one mv bucket (hour, count=1) per surviving base row. {@code v} equals commit order 0..8, so the base
 * oracle can order by {@code v} and assert an identity prefix independent of ts order.
 *
 * <p>Setup drives K base rows with the durable epoch enabled (a durable cut at seqTxn=K for both base and mv,
 * since the epoch cadence is a global property both tables observe), then disables the epoch; the driver's swept
 * {@code commit()} phase applies the M remaining base rows lazily, each refreshing the mv lazily too: the
 * sustained gap between the durable cut and the frontier, for both tables at once.
 *
 * <h3>Synchronous refresh</h3>
 * The mat view refresh runs on the test thread: {@link #drainWalAndMatViewQueues()} runs
 * {@code drainWalQueue -> drainMatViewQueue (while(refreshJob.run());) -> drainWalQueue}. With fixed data and a
 * pinned clock the per-commit durability-op sequence is the same in the count pass and every sweep pass, as
 * {@code forEachAdaptiveCrashPoint} requires.
 *
 * <h3>A crash the refresh swallows</h3>
 * A {@link CrashSimulationError} fired during the mv refresh-commit fsync is caught by {@code MatViewRefreshJob}'s
 * {@code catch (Throwable)} and turned into a view invalidation ({@code refreshFailState}): neither a propagated
 * error nor a table suspend, so the driver's {@code fired = CrashSimulationError | anyTableSuspended} contract
 * cannot see it. The workload's {@code commit()} therefore polls {@link #viewInvalid} after each drain and rethrows
 * a {@code CrashSimulationError}. A crash in the mv WAL apply, or the base insert or apply, still shows up as a
 * suspend or a propagated error. In the commit phase only a fired crash invalidates the view, so this is exact.
 *
 * <h3>Modelling a fresh restart for the mat view state</h3>
 * The driver models a booted engine for the tables (release handles, {@code crash()}, clear the transient
 * suspend, evict the pooled {@code TxnScoreboard}, {@code recover()}, republish, apply). The in-memory
 * {@code MatViewStateStore} is anonymous memory {@code crash()} cannot roll back, so it keeps a stale pre-crash
 * {@code lastRefreshBaseTxn} where a real boot re-reads the recovered on-disk {@code _mv.s}. The oracle therefore
 * calls {@link io.questdb.cairo.CairoEngine#hydrateMatViewStateStore()}, the boot and promote rehydration entry
 * point, before it inspects the view, and then drives the refresh once more to model post-boot reconciliation.
 *
 * <h3>The base/view consistency oracle</h3>
 * At every crash point k, after recovery and rehydration:
 * <ol>
 *   <li>base is not suspended and reads back an identity prefix {@code {0..p-1}} ordered by v;</li>
 *   <li>mv is not suspended;</li>
 *   <li><b>no phantom</b>: while the view reports valid (the engine would serve it), every mv bucket aggregates
 *       only base rows that survived recovery: the mv contents are a subset of the aggregation recomputed from
 *       the recovered base;</li>
 *   <li><b>exact reconciliation</b>: after the post-boot refresh (and, only if the view came back invalid, a
 *       {@code REFRESH ... FULL}, the recovery of an invalidated view), the view is valid and its contents
 *       equal the aggregation recomputed from the recovered base (no missing rows, no phantom rows);</li>
 *   <li>at k=N every committed base row is restored and the mv has the full bucket set;</li>
 *   <li>a follow-up insert lands in the base and flows through to the mv;</li>
 *   <li>the recovered base count is non-decreasing in k (the durable-survival floor).</li>
 * </ol>
 * A failure here (a valid mv that references base rows recovery removed, a surviving base row the reconciled mv
 * is missing, or a stuck suspended or invalid view) is a mat view recovery consistency bug.
 */
public class AdaptiveMatViewLazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    /**
     * Crash-point cap for this sweep. The base and view commit phase runs past the shared
     * {@link #DEFAULT_ADAPTIVE_CRASH_POINT_CAP} of 200, and the assertions demand no truncation, so this test raises
     * its own ceiling with headroom for a small change in the durability-op tail.
     */
    private static final int MAT_VIEW_CRASH_POINT_CAP = 256;
    /**
     * Floor on the crash points of one part where the view came back valid, so the phantom check runs: five parts
     * of 5 keep the whole sweep's floor of 25. A 30-point part spans one whole insert, apply and refresh step (30
     * durability ops), whose run of valid-view points is 10 to 14 long, so a part sees about 14 wherever its bounds
     * fall. The floor is absolute rather than a fraction, so ordinary op-count drift does not trip it, while it still
     * fails if the view starts coming back invalid nearly everywhere.
     */
    private static final int MIN_VALID_VIEW_POINTS_PER_PART = 5;
    /**
     * Pre-epoch base rows: a durable epoch is taken after these (interval 0), for base and mv.
     */
    private static final int LAZY_K = 4;
    /**
     * Post-epoch base rows applied lazily (epoch disabled): the sustained lazy gap this sweep crashes into.
     */
    private static final int LAZY_M = 5;
    /**
     * Total committed base rows once the whole gap is applied.
     */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * Distinct hour of day within the single {@code partition by day} bucket 2024-10-01, in commit order v=0..8.
     * Index 0 (v=0) is the ceiling (hour 23, inserted first); every later element is below it and visits the
     * timeline non-monotonically, so every apply after v=0 takes the O3 merge path. Distinct hours make the mv
     * ({@code count() ... sample by 1h}) a bijection: one bucket per base row.
     */
    private static final int[] LAZY_TS_HOUR = {23, 9, 14, 4, 11, 2, 17, 7, 20};

    /**
     * The full sweep (N=150 crash points) runs in five parts of 30 points; see
     * {@link #assertSweepPartRecoversMutuallyConsistent}. If N grows well past 150, the last part needs a further split.
     */
    @Test
    public void testMatViewLazyGapSweepRecoversMutuallyConsistentAtCrashPoints121ToLast() throws Exception {
        assertSweepPartRecoversMutuallyConsistent(121, Integer.MAX_VALUE);
    }

    @Test
    public void testMatViewLazyGapSweepRecoversMutuallyConsistentAtCrashPoints1To30() throws Exception {
        assertSweepPartRecoversMutuallyConsistent(1, 30);
    }

    @Test
    public void testMatViewLazyGapSweepRecoversMutuallyConsistentAtCrashPoints31To60() throws Exception {
        assertSweepPartRecoversMutuallyConsistent(31, 60);
    }

    @Test
    public void testMatViewLazyGapSweepRecoversMutuallyConsistentAtCrashPoints61To90() throws Exception {
        assertSweepPartRecoversMutuallyConsistent(61, 90);
    }

    @Test
    public void testMatViewLazyGapSweepRecoversMutuallyConsistentAtCrashPoints91To120() throws Exception {
        assertSweepPartRecoversMutuallyConsistent(91, 120);
    }

    /**
     * The full lazy gap (all M rows applied lazily), crashed and reopened with recovery enabled: restores every
     * base row and leaves the mv exactly consistent with the restored base.
     */
    @Test
    public void testRollForwardRebuildsMatViewMutuallyConsistentAfterCrash() throws Exception {
        assertMatViewRollForward(true);
    }

    /**
     * Data before pointer, for a base table and the mat view reading through it.
     *
     * <p>The same crash with recovery disabled is held to the same bar as with recovery: {@code _txn}, {@code _cv}
     * and the index files make their per-commit flush decision on the table's effective mode, so the commit pointer
     * is as lazy as the data it describes and a crash never leaves the base torn past its epoch. What distinguishes a
     * healthy build from a regressed one is the invariant in {@link #assertMatViewRollForward}: after the crash and
     * before any recovery, the base's {@code _txn} does not claim a seqTxn beyond the durable epoch cut. See
     * {@link AdaptiveIndexedSymbolLazyGapCrashSweepTest} for the single-variable A/B, and
     * {@link AdaptiveRecoveryRollForwardCrashTest} for roll-forward itself.
     */
    @Test
    public void testWithoutRecoveryMatViewStillReconcilesFromTheDurableWal() throws Exception {
        assertMatViewRollForward(false);
    }

    /**
     * The mat view lazy-gap workload. {@code setup} builds the durable epoch prefix (K base rows and the mv refreshed
     * against them, epoch enabled, then disabled) and returns both the base token and the mv's table token, so
     * {@code recover()} and the driver's per-token scoreboard eviction cover both. The swept {@code commit} phase
     * applies the M lazily applied base rows, each driving a lazy mv refresh.
     */
    private final class MatViewLazyGapWorkload implements AdaptiveCrashWorkload {
        /**
         * Crash points whose view came back valid after recovery (the phantom check ran there).
         */
        int validViewPoints;
        /**
         * Crash points whose view came back invalid after recovery (reconciled via a full refresh).
         */
        int invalidViewPoints;
        private TableToken baseTt;
        private TableToken mvTt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            // Epoch enabled for the K-row prefix, so a durable cut is taken at seqTxn=K (base and mv).
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop materialized view if exists mv");
            execute("drop table if exists base");
            drainWalAndMatViewQueues();

            execute("create table base (ts timestamp, v long) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select ts, count() cnt from base sample by 1h) partition by day");
            baseTt = engine.verifyTableName("base");
            mvTt = engine.verifyTableName("mv");

            // The instance must be adaptive for the epoch and recovery machinery to engage; assert it, so a mode
            // regression (never an epoch, so the whole sweep would prove nothing) fails.
            Assert.assertEquals(
                    "instance must run ADAPTIVE (else the sweep is vacuous)",
                    CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode()
            );

            // K base rows, then apply and refresh the mv: durable epoch at seqTxn=K for both tables.
            for (int i = 0; i < LAZY_K; i++) {
                insertBaseRow(LAZY_TS_HOUR[i], i);
            }
            drainWalAndMatViewQueues();

            // Disable further epochs: the swept commit() phase (the M rows and their mv refreshes) is applied lazily,
            // building the sustained gap between the durable cut and the frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{baseTt, mvTt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                insertBaseRow(LAZY_TS_HOUR[i], i);
                // Drive base apply, mv refresh and mv apply synchronously. W=0: the base WAL commit fdatasyncs
                // synchronously, so an armed crash on that leg propagates as CrashSimulationError; a crash on a lazy
                // apply msync becomes a table suspend; a crash on the mv refresh-commit fsync becomes a view
                // invalidation.
                drainWalAndMatViewQueues();
                // Stop as a real power loss would: on a suspended base or mv (the apply path) or an invalidated view
                // (the refresh path). Rethrow the Error for the invalidation case, since the driver only watches
                // CrashSimulationError and anyTableSuspended. In this phase only a fired crash invalidates the view.
                if (anyTableSuspended(baseTt, mvTt)) {
                    return;
                }
                if (viewInvalid(mvTt)) {
                    throw new CrashSimulationError(-1);
                }
            }
        }

        @Override
        public int oracle(int k, int n) throws Exception {
            // Model a fresh restart's mat view rehydration: the live engine kept a stale in-memory MatViewState
            // across the simulated crash, where a real boot re-reads the recovered on-disk _mv.s. This is the mat view
            // analogue of the driver's scoreboard eviction.
            engine.hydrateMatViewStateStore();

            // (1) base: not suspended, clean identity prefix by v.
            Assert.assertFalse("k=" + k + ": base must NOT be suspended after recovery",
                    engine.getTableSequencerAPI().isSuspended(baseTt));
            final List<Long> baseVs = readVOrderedByVAllowTorn();
            for (int i = 0; i < baseVs.size(); i++) {
                Assert.assertNotNull("k=" + k + " base row " + i + " read back NULL (corruption)", baseVs.get(i));
                Assert.assertEquals(
                        "k=" + k + " base row " + i + " silently WRONG (not an identity prefix ordered by v)",
                        (long) i, (long) baseVs.get(i)
                );
            }
            final int p = (int) rowCount("base");
            Assert.assertTrue("k=" + k + ": a torn read cannot show MORE base rows than committed",
                    baseVs.size() <= p);
            Assert.assertTrue("k=" + k + ": recovery must never drop below the K durable epoch rows (p=" + p + ")",
                    p >= LAZY_K);

            // (2) mv: not suspended.
            Assert.assertFalse("k=" + k + ": mat-view must NOT be suspended after recovery",
                    engine.getTableSequencerAPI().isSuspended(mvTt));

            // (3) No phantom: while the view reports valid (would be served), every mv bucket aggregates only
            // surviving base rows; mv contents are a subset of the aggregation recomputed from the recovered base.
            if (!viewInvalid(mvTt)) {
                validViewPoints++;
                final List<String> mvNow = readAggAllowTorn("select cast(ts as long) t, cnt from mv order by 1");
                final Set<String> expectedFromBase = new HashSet<>(
                        readAggAllowTorn("select cast(ts as long) t, count() cnt from base sample by 1h order by 1"));
                for (String bucket : mvNow) {
                    Assert.assertTrue(
                            "k=" + k + ": VALID mat-view references a base row recovery rewound away (phantom "
                                    + "bucket " + bucket + "; recovered-base buckets=" + expectedFromBase + ") — "
                                    + "the flagged base/view mutual-consistency bug",
                            expectedFromBase.contains(bucket)
                    );
                }
            } else {
                invalidViewPoints++;
            }

            // (4) Reconcile (the post-boot refresh). A view left invalid by a mid-refresh crash is a safe state that a
            // full refresh recovers; this does not mask the phantom check above, which concerns valid views.
            drainWalAndMatViewQueues();
            if (viewInvalid(mvTt)) {
                execute("refresh materialized view mv full");
                drainWalAndMatViewQueues();
            }
            Assert.assertFalse("k=" + k + ": mat-view must reconcile to VALID after the post-boot refresh",
                    viewInvalid(mvTt));
            Assert.assertFalse("k=" + k + ": mat-view must NOT be suspended after reconciliation",
                    engine.getTableSequencerAPI().isSuspended(mvTt));
            assertMvEqualsBaseAggregation(k, "after reconciliation");

            // (5) k=N: full restore of both.
            if (k == n) {
                Assert.assertEquals("k=N: recovery must restore ALL committed base rows", ROWS, p);
                Assert.assertEquals("k=N: base must read back the full identity set clean", ROWS, baseVs.size());
                Assert.assertEquals("k=N: mv must have one bucket per base row (distinct hours)", ROWS,
                        readAggAllowTorn("select cast(ts as long) t, cnt from mv order by 1").size());
            }

            // (6) Both are writable: a follow-up base insert (fresh day, plain append) lands in base and flows through
            // to the mv. Last, so it does not perturb the consistency assertions.
            execute("insert into base values ('2024-10-09T00:00:00.000000Z', 999)");
            drainWalAndMatViewQueues();
            Assert.assertEquals("k=" + k + ": follow-up insert must land on the recovered base",
                    p + 1, rowCount("base"));
            assertMvEqualsBaseAggregation(k, "after follow-up insert");

            return p;
        }

        @Override
        public void teardown() throws Exception {
            try {
                execute("drop materialized view if exists mv");
                execute("drop table if exists base");
                drainWalAndMatViewQueues();
            } catch (Exception e) {
                LOG.info().$("[mat-view lazy-gap sweep] teardown drop skipped: ").$(e.getMessage()).$();
            }
        }
    }

    private void assertMatViewRollForward(boolean recoveryEnabled) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
        try {
            Assert.assertEquals(CommitMode.ADAPTIVE, engine.getConfiguration().getCommitMode());
            runWithCrashFacade(() -> {
                crashFf.modelSharedJournal = false;
                execute("drop materialized view if exists mv");
                execute("drop table if exists base");
                drainWalAndMatViewQueues();
                execute("create table base (ts timestamp, v long) timestamp(ts) partition by day wal");
                execute("create materialized view mv as (select ts, count() cnt from base sample by 1h) partition by day");
                final TableToken baseTt = engine.verifyTableName("base");
                final TableToken mvTt = engine.verifyTableName("mv");

                // K rows, then apply and refresh: durable epoch at seqTxn=K.
                for (int i = 0; i < LAZY_K; i++) {
                    insertBaseRow(LAZY_TS_HOUR[i], i);
                }
                drainWalAndMatViewQueues();

                // Baseline the durable state (table-name registry and the K-row epoch cut of base and mv) as the driver
                // does after setup: crash() must roll back only the lazily applied M rows, not the registry; otherwise
                // the live engine's stale MemoryCARW writes past the rolled-back registry file on the cleanup drop and
                // hits SIGBUS (a harness artifact; a real boot re-reads the registry from its durable content).
                markDurableBaseline();

                // Disable the epoch; apply the M rows lazily (columns non-durable past the epoch).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
                for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                    insertBaseRow(LAZY_TS_HOUR[i], i);
                }
                drainWalAndMatViewQueues();

                // Pre-crash: base has all rows and the mv is consistent with it.
                Assert.assertEquals("pre-crash base must have all rows", ROWS, rowCount("base"));
                assertMvEqualsBaseAggregation(-1, "pre-crash");

                // Crash: drop non-durable column data; keep the fsync/msync'd state and the durable WAL.
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                crashFf.crash(engine.getConfiguration().getDbRoot());

                // Model a fresh restart: clear the transient apply-crash suspend, evict the pooled scoreboards, and run
                // recovery or skip it, per the arm.
                if (engine.getTableSequencerAPI().isSuspended(baseTt)) {
                    engine.getTableSequencerAPI().getTxnTracker(baseTt).setUnsuspended();
                }
                if (engine.getTableSequencerAPI().isSuspended(mvTt)) {
                    engine.getTableSequencerAPI().getTxnTracker(mvTt).setUnsuspended();
                }
                engine.getTxnScoreboardPool().remove(baseTt);
                engine.getTxnScoreboardPool().remove(mvTt);
                // The base's commit pointer as the crash left it, before anything can repair it.
                final long preRecoveryBaseSeqTxn = readOnDiskTxnSeqTxn(baseTt, PartitionBy.DAY);
                if (recoveryEnabled) {
                    new RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(baseTt);
                engine.notifyWalTxnRepublisher(mvTt);
                drainWalQueue();

                // Model the restart's mat view rehydration and post-boot reconciliation.
                engine.hydrateMatViewStateStore();
                drainWalAndMatViewQueues();

                final String arm = recoveryEnabled ? "recovery-enabled" : "recovery-disabled";

                // The invariant (data before pointer), asserted first so a regression reports its cause. The crash
                // left the base's _txn at or below its durable epoch cut in both arms, so no reader, and no mv refresh
                // reading through it, can see a row whose column data was never flushed. Gating TxWriter and
                // ColumnVersionWriter on the global `!= NOSYNC` instead of the table's effective mode puts this seqTxn
                // past the cut, and the mv then aggregates rows the crash dropped.
                Assert.assertTrue(
                        arm + ": the crash must not leave the base _txn claiming past the durable epoch cut: "
                                + "the commit pointer is lazily durable like the data it exposes "
                                + "(preRecoveryBaseSeqTxn=" + preRecoveryBaseSeqTxn + ", durable cut=" + LAZY_K + ")",
                        preRecoveryBaseSeqTxn >= 0 && preRecoveryBaseSeqTxn <= LAZY_K
                );

                // The consequence, the same for both arms: replaying the durable WAL lands on the full, mutually
                // consistent result whether or not the epoch rewind ran first.
                if (viewInvalid(mvTt)) {
                    execute("refresh materialized view mv full");
                    drainWalAndMatViewQueues();
                }
                Assert.assertFalse(arm + ": base must NOT be suspended",
                        engine.getTableSequencerAPI().isSuspended(baseTt));
                Assert.assertFalse(arm + ": mv must NOT be suspended",
                        engine.getTableSequencerAPI().isSuspended(mvTt));
                Assert.assertFalse(arm + ": mv must reconcile to VALID", viewInvalid(mvTt));
                Assert.assertEquals(arm + ": ALL base rows must be rebuilt from the WAL", ROWS, rowCount("base"));
                Assert.assertTrue(arm + ": the rebuilt base must be a clean identity prefix",
                        isIdentityPrefix(readVOrderedByVAllowTorn()));
                assertMvEqualsBaseAggregation(-1, arm);

                // Cleanup.
                if (engine.getTableSequencerAPI().isSuspended(mvTt)) {
                    engine.getTableSequencerAPI().getTxnTracker(mvTt).setUnsuspended();
                }
                if (engine.getTableSequencerAPI().isSuspended(baseTt)) {
                    engine.getTableSequencerAPI().getTxnTracker(baseTt).setUnsuspended();
                }
                engine.getTxnScoreboardPool().remove(baseTt);
                engine.getTxnScoreboardPool().remove(mvTt);
                execute("drop materialized view if exists mv");
                execute("drop table if exists base");
                drainWalAndMatViewQueues();
            });
        } finally {
            setProperty(PropertyKey.CAIRO_COMMIT_MODE, "nosync");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0");
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 1000);
        }
    }

    /**
     * The mutual-consistency sweep over crash points {@code firstK..min(lastK, N)}. Crashes at each durability op of
     * the M lazily applied base rows (each also drives a lazy mv refresh) in that range, running the recovery triple,
     * restart rehydration and the base/view consistency oracle at each point. The whole sweep needs more than the
     * test timeout in one method (the crash facade's per-point cost grows with the points already run in it), so it
     * is split into parts; every point starts from a fresh identical baseline, so a part sweeps exactly the points
     * the whole sweep would in its range. The whole-sweep checks apply per part: the valid-view floor
     * ({@link #MIN_VALID_VIEW_POINTS_PER_PART}), recovered base counts non-decreasing in k within the part and at
     * least LAZY_K at its first point; the genuine rise only in the part holding k=1, the full set only in the part
     * holding k=N.
     */
    private void assertSweepPartRecoversMutuallyConsistent(int firstK, int lastK) throws Exception {
        withAdaptiveLazyGap(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final MatViewLazyGapWorkload workload = new MatViewLazyGapWorkload();
            // Sweeping every crash point is the point of this test (the assertions below demand no truncation), and
            // the base and view commit phase runs past the default cap of 200, so raise the cap for this workload.
            final SweepResult r = forEachAdaptiveCrashPoint(workload, MAT_VIEW_CRASH_POINT_CAP, firstK, lastK);
            final int first = r.firstSweptK;
            final int last = r.lastSweptK;

            LOG.info().$("[mat-view lazy-gap sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", part=").$(first).$("..").$(last)
                    .$(", validViewPoints=").$(workload.validViewPoints)
                    .$(", invalidViewPoints=").$(workload.invalidViewPoints)
                    .$(", recoveredBaseByK=").$(Arrays.toString(r.recoveredByK())).$();

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("the cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);
            Assert.assertEquals(
                    "every swept crash point must count as a valid or an invalid view",
                    last - first + 1, workload.validViewPoints + workload.invalidViewPoints
            );

            // The "no phantom in a valid view" clause is the detector for the consistency bug, so it must run: a
            // passing sweep where the view came back invalid at every point (phantom check always skipped) proves
            // nothing.
            Assert.assertTrue(
                    "the phantom check must run on a meaningful number of VALID post-recovery views (else the "
                            + "GREEN sweep is vacuous); validViewPoints=" + workload.validViewPoints
                            + ", invalidViewPoints=" + workload.invalidViewPoints
                            + ", part=" + first + ".." + last,
                    workload.validViewPoints >= MIN_VALID_VIEW_POINTS_PER_PART
            );

            // Oracle clause 7 (durable-survival floor): recovered base counts non-decreasing in k.
            for (int k = first + 1; k <= last; k++) {
                Assert.assertTrue(
                        "recovered base counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // The durable epoch floor: the part's first crash point keeps at least the K durable epoch rows.
            Assert.assertTrue(
                    "crash point k=" + first + " must recover at least the K durable epoch rows (floor)",
                    r.recoveredByK()[first] >= LAZY_K
            );
            if (first == 1) {
                // A real rise, not a degenerate all-full sweep.
                Assert.assertTrue(
                        "sweep must show a genuine rise (earliest crash point < full set)",
                        r.recoveredByK()[1] < ROWS
                );
            }
            if (last == r.sweptPoints) {
                // k=N: recovery restores all committed base rows.
                Assert.assertEquals(
                        "k=N must recover ALL committed base rows (W=0 => every returned commit's WAL is durable)",
                        ROWS, r.recoveredByK()[last]
                );
            }
        }));
    }

    private void withAdaptiveLazyGap(RunnableEx body) throws Exception {
        setProperty(PropertyKey.CAIRO_COMMIT_MODE, "adaptive");
        setProperty(PropertyKey.CAIRO_ADAPTIVE_COMMIT_GROUP_WINDOW, "0"); // W = 0 (synchronous)
        setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);        // setup re-affirms/flips per phase
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

    private static void insertBaseRow(int hour, int v) throws Exception {
        execute("insert into base values ('" + String.format("2024-10-01T%02d:00:00.000000Z", hour) + "', " + v + ")");
    }

    /**
     * True if the mat view's in-memory refresh state reports invalid (a null state counts as not invalid).
     */
    private boolean viewInvalid(TableToken mvTt) {
        final MatViewState st = engine.getMatViewStateStore().getViewState(mvTt);
        return st != null && st.isInvalid();
    }

    /**
     * count(*) on base: the committed row count from metadata, reliable even if a column read would tear.
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
     * Reads {@code select v from base order by v}, returning the identity values gathered so far. A visible torn
     * read (Cairo or JVM error on a truncated column) is an acceptable crash outcome: the prefix read before the
     * tear is returned rather than rethrown.
     */
    private List<Long> readVOrderedByVAllowTorn() {
        final List<Long> out = new ArrayList<>();
        try (RecordCursorFactory f = select("select v from base order by v")) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0));
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // Acceptable: corruption detected visibly; return the prefix read before the tear.
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    private boolean isIdentityPrefix(List<Long> rows) {
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i) == null || rows.get(i) != (long) i) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads a two-column {@code (timestampAsLong, count)} aggregation into canonical {@code "ts|cnt"} rows,
     * tolerating a visible torn read (returns the prefix read before the tear).
     */
    private List<String> readAggAllowTorn(String sql) {
        final List<String> out = new ArrayList<>();
        try (RecordCursorFactory f = select(sql)) {
            try (RecordCursor c = f.getCursor(sqlExecutionContext)) {
                final Record r = c.getRecord();
                while (c.hasNext()) {
                    out.add(r.getLong(0) + "|" + r.getLong(1));
                }
            }
        } catch (CairoException | CairoError | InternalError torn) {
            // Acceptable: corruption detected visibly; return the prefix.
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * The core consistency assertion: the mv's contents equal the {@code count() ... sample by 1h} aggregation
     * recomputed from the current (recovered) base, row for row. Catches both a phantom mv bucket (referencing a
     * base row that is gone) and a surviving base row the mv is missing.
     */
    private void assertMvEqualsBaseAggregation(int k, String phase) {
        final List<String> mvRows = readAggAllowTorn("select cast(ts as long) t, cnt from mv order by 1");
        final List<String> baseAgg = readAggAllowTorn("select cast(ts as long) t, count() cnt from base sample by 1h order by 1");
        Assert.assertEquals(
                "k=" + k + " (" + phase + "): mat-view must equal the aggregation recomputed from the recovered "
                        + "base (base/view mutual consistency)",
                baseAgg, mvRows
        );
    }
}
