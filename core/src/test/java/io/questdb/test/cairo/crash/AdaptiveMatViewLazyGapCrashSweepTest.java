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
 * SP-D Task W4 (the flagged-risk workload): does a MATERIALIZED VIEW and its ADAPTIVE base table recover to
 * a MUTUALLY CONSISTENT state after a crash swept across every durability op of a sustained lazy gap?
 *
 * <p>This is the one recovery path the adaptive design docs left explicitly open
 * (adaptive-commit-mode-design.md: "confirm epoch + durable-frontier semantics compose with mat-view
 * refresh state and the V2 split txnlog"). {@link RecoveryCoordinator#recover()} EXPLICITLY recovers
 * mat-views: its loop skips regular VIEWs but a mat-view is {@code isView()==false} with an on-disk
 * {@code _meta}/{@code _txn}/{@code _cv} and (under global ADAPTIVE) an effective ADAPTIVE commit mode, so
 * it goes through {@code recoverTable} exactly like a normal table. Nothing before this sweep exercised
 * that path together with the {@code _mv.s} refresh watermark.
 *
 * <h3>The schema and the sustained lazy gap</h3>
 * A base adaptive WAL table {@code base(ts, v)} and a mat-view {@code mv} =
 * {@code select ts, count() cnt from base sample by 1h}. Each base row lands in a DISTINCT hour of a single
 * day (out-of-order among themselves so every apply after the first engages the O3 merge path), so the mv
 * is a clean bijection: one mv bucket (hour, count=1) per surviving base row. {@code v} equals commit order
 * 0..8, so the base oracle can order by {@code v} and assert an identity prefix independent of ts order.
 *
 * <p>Setup drives K base rows with the durable epoch ENABLED (a durable cut is taken at seqTxn=K for BOTH
 * base and mv, since the epoch cadence is a global property both tables observe), then DISABLES the epoch;
 * the driver's swept {@code commit()} phase applies the M remaining base rows LAZILY, each one refreshing
 * the mv lazily too — the sustained gap between the durable cut and the frontier, for both tables at once.
 *
 * <h3>Determinism / how the refresh is driven synchronously</h3>
 * The mat-view refresh is NOT a background thread here: {@link #drainWalAndMatViewQueues()} runs
 * {@code drainWalQueue -> drainMatViewQueue (while(refreshJob.run());) -> drainWalQueue} entirely on the
 * test thread. With fixed data and a pinned clock the per-commit durability-op sequence is reproducible
 * across the count pass and every sweep pass — the property {@code forEachAdaptiveCrashPoint} needs.
 *
 * <h3>Detecting a crash that the refresh swallows (the one wrinkle)</h3>
 * A {@link CrashSimulationError} (an {@code Error}) fired during the mv REFRESH-commit fsync is caught by
 * {@code MatViewRefreshJob}'s {@code catch (Throwable)} and turned into a view INVALIDATION
 * ({@code refreshFailState}) — not a propagated error and not a table suspend, so the driver's
 * {@code fired = CrashSimulationError | anyTableSuspended} contract cannot see it. This workload's
 * {@code commit()} therefore also polls {@link #viewInvalid} after each drain and RE-THROWS a
 * {@code CrashSimulationError} so the crash is faithfully reported as fired. (A crash in the mv WAL APPLY,
 * or base insert/apply, still manifests as suspend/propagate and is detected normally.) In the controlled
 * commit phase the ONLY thing that invalidates the view is a fired crash, so this bridge is exact.
 *
 * <h3>Modelling a fresh restart for the mat-view state</h3>
 * The driver already models a booted engine for the tables (release handles, {@code crash()}, clear the
 * transient suspend, evict the pooled {@code TxnScoreboard}, {@code recover()}, republish, apply). The
 * in-memory {@code MatViewStateStore}, however, is anonymous memory the simulated {@code crash()} cannot
 * roll back, so it keeps a STALE pre-crash {@code lastRefreshBaseTxn} where a real boot would re-read the
 * (recovered) on-disk {@code _mv.s}. The oracle therefore calls {@link io.questdb.cairo.CairoEngine#hydrateMatViewStateStore()}
 * — the exact boot/promote rehydration entry point — before it inspects the view, the mat-view analog of
 * the scoreboard eviction. It then drives the refresh once more to model the post-boot reconciliation.
 *
 * <h3>The base/view mutual-consistency oracle (NOT weakened)</h3>
 * At every crash point k, after recovery + restart-model rehydration:
 * <ol>
 *   <li>base is NOT suspended and reads back a clean identity prefix {@code {0..p-1}} ordered by v;</li>
 *   <li>mv is NOT suspended;</li>
 *   <li><b>no phantom</b>: while the view still reports VALID (i.e. the engine would serve it), every mv
 *       bucket must aggregate ONLY base rows that survived recovery — the mv contents are a subset of the
 *       aggregation recomputed from the RECOVERED base. A single mv bucket referencing a base row that
 *       recovery rewound away is the flagged consistency bug;</li>
 *   <li><b>exact reconciliation</b>: after the post-boot refresh (and, only if the view came back INVALID,
 *       a {@code REFRESH ... FULL} — the safe operator/auto recovery of a flagged view), the view is VALID
 *       and its contents EQUAL the aggregation recomputed from the recovered base, exactly (no missing
 *       surviving rows, no phantom rows);</li>
 *   <li>at k=N every committed base row is restored and the mv has the full bucket set;</li>
 *   <li>a follow-up insert lands in the base AND flows through to the mv (both writable after recovery);</li>
 *   <li>the recovered base count is monotonic non-decreasing in k (the Bar-2 durable-survival floor).</li>
 * </ol>
 * GREEN at every crash point => mat-view + adaptive epoch/recovery compose safely. A RED here — a valid mv
 * that references rewound-away base rows, a surviving base row the reconciled mv is missing, or a stuck
 * suspended/invalid view — is a REAL mat-view recovery consistency bug and a GA-blocker candidate.
 */
public class AdaptiveMatViewLazyGapCrashSweepTest extends AbstractAdaptiveCrashSweepTest {

    /** Pre-epoch base rows: a durable epoch is taken after these (interval=0), for base AND mv. */
    private static final int LAZY_K = 4;
    /** Post-epoch base rows applied LAZILY (epoch disabled) — the sustained lazy gap this sweep crashes into. */
    private static final int LAZY_M = 5;
    /** Total committed base rows once the whole gap is applied. */
    private static final int ROWS = LAZY_K + LAZY_M; // 9

    /**
     * DISTINCT hour-of-day (single {@code partition by day} bucket 2024-10-01), in commit order v=0..8.
     * Index 0 (v=0) is the CEILING (hour 23, inserted FIRST); every later element is strictly below it and
     * visits the timeline non-monotonically, so every apply after v=0 engages the O3 merge path. Distinct
     * hours => the mv ({@code count() ... sample by 1h}) is a clean bijection: one bucket per base row.
     */
    private static final int[] LAZY_TS_HOUR = {23, 9, 14, 4, 11, 2, 17, 7, 20};

    /**
     * THE HEADLINE mutual-consistency sweep. Crashes across EVERY durability op of the M lazily-applied
     * base rows (each of which also drives a lazy mv refresh), running the full recovery triple + restart
     * rehydration + the base/view mutual-consistency oracle at each point.
     */
    @Test
    public void testMatViewLazyGapSweepRecoversMutuallyConsistentAtEveryCrashPoint() throws Exception {
        withAdaptiveLazyGap(() -> runWithCrashFacade(() -> {
            crashFf.modelSharedJournal = false; // per-inode strictness (ext4 fast_commit)

            final MatViewLazyGapWorkload workload = new MatViewLazyGapWorkload();
            final SweepResult r = forEachAdaptiveCrashPoint(workload);

            LOG.info().$("[mat-view lazy-gap sweep] N=").$(r.n).$(", sweptPoints=").$(r.sweptPoints)
                    .$(", validViewPoints=").$(workload.validViewPoints)
                    .$(", invalidViewPoints=").$(workload.invalidViewPoints)
                    .$(", recoveredBaseByK=").$(Arrays.toString(r.recoveredByK())).$();

            // NON-VACUITY self-check: the sharp "no phantom in a VALID view" clause is the load-bearing
            // detector for the flagged bug, so it must actually FIRE — a GREEN sweep where the view came
            // back INVALID at every point (phantom check always skipped) would be a hollow pass. Assert a
            // meaningful number of crash points left the view VALID post-recovery and thus ran the phantom
            // check against the recovered base.
            Assert.assertTrue(
                    "the phantom check must run on a meaningful number of VALID post-recovery views (else the "
                            + "GREEN sweep is vacuous); validViewPoints=" + workload.validViewPoints,
                    workload.validViewPoints >= r.sweptPoints / 2
            );

            Assert.assertTrue("N must be > 0", r.n > 0);
            Assert.assertEquals("default cap must not truncate this small workload", r.n, r.sweptPoints);
            Assert.assertFalse("small workload must not be truncated", r.truncated);

            // Oracle clause 7 (Bar-2 durable-survival FLOOR): recovered base counts non-decreasing in k.
            for (int k = 2; k <= r.sweptPoints; k++) {
                Assert.assertTrue(
                        "recovered base counts must be non-decreasing at k=" + k + " ("
                                + r.recoveredByK()[k - 1] + " -> " + r.recoveredByK()[k] + ")",
                        r.recoveredByK()[k] >= r.recoveredByK()[k - 1]
                );
            }
            // A genuine rise, not a degenerate all-full sweep.
            Assert.assertTrue(
                    "sweep must show a genuine rise (earliest crash point < full set)",
                    r.recoveredByK()[1] < ROWS
            );
            // The durable epoch floor: even the earliest crash point keeps at least the K durable epoch rows.
            Assert.assertTrue(
                    "every crash point must recover at least the K durable epoch rows (floor)",
                    r.recoveredByK()[1] >= LAZY_K
            );
            // k=N: recovery restores ALL committed base rows.
            Assert.assertEquals(
                    "k=N must recover ALL committed base rows (W=0 => every returned commit's WAL is durable)",
                    ROWS, r.recoveredByK()[r.sweptPoints]
            );
        }));
    }

    /**
     * NEGATIVE CONTROL — GREEN arm: the full lazy gap (all M rows applied lazily) crashed and reopened WITH
     * recovery enabled restores every base row AND leaves the mv exactly consistent with the restored base.
     */
    @Test
    public void testRollForwardRebuildsMatViewMutuallyConsistentAfterCrash() throws Exception {
        assertMatViewRollForward(true);
    }

    /**
     * NEGATIVE CONTROL — RED arm: the identical crash with recovery DISABLED
     * ({@code CAIRO_ADAPTIVE_RECOVERY_ROLL_FORWARD_ENABLED=false}). The post-epoch base columns were never
     * made durable, so the crash drops them; without the epoch rewind + WAL roll-forward the base is torn
     * past the epoch and the mv cannot be reconciled to the full, correct, mutually-consistent result —
     * proving adaptive recovery does real work for the mat-view schema, not just for plain tables.
     */
    @Test
    public void testNegativeControlWithoutRecoveryBreaksMatViewConsistency() throws Exception {
        assertMatViewRollForward(false);
    }

    // ------------------------------------------------------------------------------------------------
    // Workload
    // ------------------------------------------------------------------------------------------------

    /**
     * W4 mat-view lazy-gap workload. {@code setup} builds the durable epoch prefix (K base rows + the mv
     * refreshed against them, epoch ENABLED, then DISABLED), returning BOTH the base token and the mv's own
     * table token so {@code recover()} + the driver's per-token scoreboard eviction cover both. The swept
     * {@code commit} phase applies the M lazily-applied base rows, each driving a lazy mv refresh.
     */
    private final class MatViewLazyGapWorkload implements AdaptiveCrashWorkload {
        /** Crash points whose view came back VALID post-recovery (the phantom check ran there). */
        int validViewPoints;
        /** Crash points whose view came back INVALID post-recovery (reconciled via a full refresh). */
        int invalidViewPoints;
        private TableToken baseTt;
        private TableToken mvTt;

        @Override
        public TableToken[] setup(int iteration) throws Exception {
            // Epoch ENABLED for the K-row prefix so a durable cut is taken at seqTxn=K (base and mv).
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, 0);
            execute("drop materialized view if exists mv");
            execute("drop table if exists base");
            drainWalAndMatViewQueues();

            execute("create table base (ts timestamp, v long) timestamp(ts) partition by day wal "
                    + "with commit_mode='adaptive'");
            execute("create materialized view mv as (select ts, count() cnt from base sample by 1h) partition by day");
            baseTt = engine.verifyTableName("base");
            mvTt = engine.verifyTableName("mv");

            // The mv must be an ADAPTIVE table for the epoch/recovery machinery to engage — it inherits the
            // global adaptive mode (no explicit per-table override). Assert it so a silent mode regression
            // (mv not adaptive => never epoch'd => this whole sweep would be vacuous) fails loudly.
            Assert.assertEquals(
                    "mat-view must resolve to ADAPTIVE effective commit mode (else the sweep is vacuous)",
                    CommitMode.ADAPTIVE, engine.getTableSequencerAPI().resolveEffectiveCommitMode(mvTt)
            );

            // K base rows -> apply + refresh mv -> durable epoch at seqTxn=K for both tables.
            for (int i = 0; i < LAZY_K; i++) {
                insertBaseRow(LAZY_TS_HOUR[i], i);
            }
            drainWalAndMatViewQueues();

            // DISABLE further epochs: the driver's swept commit() phase (the M rows + their mv refreshes) is
            // applied LAZILY, building the sustained gap between the durable cut and the frontier.
            setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
            return new TableToken[]{baseTt, mvTt};
        }

        @Override
        public void commit() throws Exception {
            for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                insertBaseRow(LAZY_TS_HOUR[i], i);
                // Drive base apply + mv refresh + mv apply synchronously. W=0: base WAL commit fdatasyncs
                // synchronously so an armed crash on that leg propagates out as CrashSimulationError; a
                // crash on a lazy apply msync is swallowed into a table SUSPEND; a crash on the mv
                // refresh-commit fsync is swallowed into a view INVALIDATION.
                drainWalAndMatViewQueues();
                // Detect the crash as a real power loss would halt everything: a suspended base or mv (the
                // apply-swallow path) OR an invalidated view (the refresh-swallow path). Re-throw the
                // Error for the invalidation case so the driver's fired-detection sees it (the driver only
                // watches CrashSimulationError + anyTableSuspended). In this controlled phase nothing but a
                // fired crash invalidates the view, so this bridge is exact.
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
            // Model a fresh restart's mat-view rehydration: the live engine kept a STALE in-memory
            // MatViewState across the simulated crash; a real boot re-reads the recovered on-disk _mv.s.
            // This is the mat-view analog of the driver's scoreboard eviction — inspect the TRUE post-boot
            // state, not the survivor artifact.
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

            // (3) NO PHANTOM: while the view still reports VALID (would be served), every mv bucket must
            // aggregate ONLY surviving base rows — mv contents are a subset of the aggregation recomputed
            // from the RECOVERED base. A bucket referencing a rewound-away base row is the flagged bug.
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

            // (4) RECONCILE (model the post-boot refresh; a view left INVALID by a mid-refresh crash is a
            // SAFE flagged state that a full refresh recovers — this does not mask the phantom bug above,
            // which concerns VALID views).
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

            // (6) writability of BOTH: a follow-up base insert (fresh day, plain append) must land in base
            // AND flow through to the mv. Do this LAST so it does not perturb the consistency assertions.
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

    // ------------------------------------------------------------------------------------------------
    // Negative control
    // ------------------------------------------------------------------------------------------------

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
                execute("create table base (ts timestamp, v long) timestamp(ts) partition by day wal "
                        + "with commit_mode='adaptive'");
                execute("create materialized view mv as (select ts, count() cnt from base sample by 1h) partition by day");
                final TableToken baseTt = engine.verifyTableName("base");
                final TableToken mvTt = engine.verifyTableName("mv");

                // K rows -> apply + refresh -> durable epoch at seqTxn=K.
                for (int i = 0; i < LAZY_K; i++) {
                    insertBaseRow(LAZY_TS_HOUR[i], i);
                }
                drainWalAndMatViewQueues();

                // Baseline the durable state (table-name registry + the K-row epoch cut of base/mv) as the
                // driver does after setup: crash() must roll back ONLY the lazily-applied M rows, not the
                // registry — otherwise the live engine's stale MemoryCARW writes past the rolled-back
                // registry file on the cleanup drop and SIGBUSes (a harness artifact; on a real boot the
                // registry is re-read from its durable content).
                markDurableBaseline();

                // Disable the epoch; apply the M rows LAZILY (columns non-durable past the epoch).
                setProperty(PropertyKey.CAIRO_ADAPTIVE_EPOCH_INTERVAL, -1);
                for (int i = LAZY_K; i < LAZY_K + LAZY_M; i++) {
                    insertBaseRow(LAZY_TS_HOUR[i], i);
                }
                drainWalAndMatViewQueues();

                // Pre-crash: base has all rows and the mv is consistent with it.
                Assert.assertEquals("pre-crash base must have all rows", ROWS, rowCount("base"));
                assertMvEqualsBaseAggregation(-1, "pre-crash");

                // CRASH: drop non-durable column data; keep the fsync/msync'd state + the durable WAL.
                engine.releaseAllReaders();
                engine.releaseAllWriters();
                engine.releaseAllWalWriters();
                crashFf.crash(engine.getConfiguration().getDbRoot());

                // Model a fresh restart: clear the transient apply-crash suspend, evict the pooled
                // scoreboards, run recovery (GREEN) or skip it (the RED negative control kill-switch).
                if (engine.getTableSequencerAPI().isSuspended(baseTt)) {
                    engine.getTableSequencerAPI().getTxnTracker(baseTt).setUnsuspended();
                }
                if (engine.getTableSequencerAPI().isSuspended(mvTt)) {
                    engine.getTableSequencerAPI().getTxnTracker(mvTt).setUnsuspended();
                }
                engine.getTxnScoreboardPool().remove(baseTt);
                engine.getTxnScoreboardPool().remove(mvTt);
                if (recoveryEnabled) {
                    new RecoveryCoordinator(engine).recover();
                }
                engine.notifyWalTxnRepublisher(baseTt);
                engine.notifyWalTxnRepublisher(mvTt);
                drainWalQueue();

                // Model the restart's mat-view rehydration + post-boot reconciliation.
                engine.hydrateMatViewStateStore();
                drainWalAndMatViewQueues();

                if (recoveryEnabled) {
                    if (viewInvalid(mvTt)) {
                        execute("refresh materialized view mv full");
                        drainWalAndMatViewQueues();
                    }
                    Assert.assertFalse("base must NOT be suspended after recovery",
                            engine.getTableSequencerAPI().isSuspended(baseTt));
                    Assert.assertFalse("mv must NOT be suspended after recovery",
                            engine.getTableSequencerAPI().isSuspended(mvTt));
                    Assert.assertFalse("mv must reconcile to VALID after recovery", viewInvalid(mvTt));
                    Assert.assertEquals("recovery must rebuild ALL base rows from the WAL", ROWS, rowCount("base"));
                    assertMvEqualsBaseAggregation(-1, "GREEN control after recovery");
                } else {
                    // Without recovery: the post-epoch base columns the crash dropped are never re-applied,
                    // so the full, correct, mutually-consistent result must NOT come back. Acceptable
                    // broken outcomes: a torn base read, a base short of ROWS, a suspended base/mv, an mv
                    // stuck invalid, or an mv that cannot be made to equal the (torn) base aggregation.
                    boolean fullCorrect;
                    try {
                        final boolean baseOk = !engine.getTableSequencerAPI().isSuspended(baseTt)
                                && rowCount("base") == ROWS
                                && isIdentityPrefix(readVOrderedByVAllowTorn())
                                && readVOrderedByVAllowTorn().size() == ROWS;
                        final boolean mvOk = !engine.getTableSequencerAPI().isSuspended(mvTt) && !viewInvalid(mvTt);
                        final List<String> mvRows = readAggAllowTorn("select cast(ts as long) t, cnt from mv order by 1");
                        final List<String> baseAgg = readAggAllowTorn("select cast(ts as long) t, count() cnt from base sample by 1h order by 1");
                        fullCorrect = baseOk && mvOk && mvRows.equals(baseAgg) && mvRows.size() == ROWS;
                    } catch (CairoException | CairoError | InternalError torn) {
                        fullCorrect = false; // a loud torn read is a broken outcome — recovery did real work
                    }
                    Assert.assertFalse(
                            "NEGATIVE CONTROL: without recovery the full correct mutually-consistent result "
                                    + "must NOT come back (else recovery does no real work / columns not lazy)",
                            fullCorrect
                    );
                }

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

    // ------------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------------

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

    /** True iff the mat-view's in-memory refresh state reports invalid (null state treated as not-invalid). */
    private boolean viewInvalid(TableToken mvTt) {
        final MatViewState st = engine.getMatViewStateStore().getViewState(mvTt);
        return st != null && st.isInvalid();
    }

    /** count(*) on base — the committed row count from metadata (reliable even if a column read would tear). */
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
     * Read {@code select v from base order by v}, returning the identity values gathered so far. A loud torn
     * read (Cairo/JVM error on a truncated column) is an acceptable crash outcome — the prefix read before
     * the tear is returned rather than rethrown.
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
            // acceptable: corruption detected loudly; return the prefix read before the tear
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
     * Read a two-column {@code (timestampAsLong, count)} aggregation into canonical {@code "ts|cnt"} rows,
     * tolerating a loud torn read (returns the prefix read before the tear).
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
            // acceptable: corruption detected loudly; return the prefix
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * The core mutual-consistency assertion: the mv's contents EQUAL the {@code count() ... sample by 1h}
     * aggregation recomputed from the CURRENT (recovered) base — exactly, row for row. Catches both a
     * phantom mv bucket (referencing a base row that is gone) and a surviving base row the mv is missing.
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
