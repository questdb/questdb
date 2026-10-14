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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.std.LongHashSet;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Differential fuzz for the out-of-order repair routes of anchored live views.
 * <p>
 * The view is {@code PARTITION BY sym ORDER BY ts} under a random anchor, and the oracle is the
 * same window over {@code PARTITION BY sym, <bucket>} recomputed from the base, or, for a zoned
 * anchor, over each key's runs of equal anchor values, which also holds where a DST fall-back
 * winds the anchor back (see {@link LiveViewFuzzAnchor}). What this class adds over
 * {@code LiveViewFuzzTest} is the input the repair routes are sensitive to and the plain fuzz
 * never generates:
 * <ul>
 *     <li>an indexed SYMBOL key, which the keyed routes follow through the base's posting
 *     index;</li>
 *     <li>{@code ANCHOR DAILY}, with and without a zone, over data crossing DST changes, inside
 *     a repeated hour, and over 1970 data below the anchor's origin;</li>
 *     <li>timestamp ties across keys, with commits cut inside a tie group on purpose;</li>
 *     <li>several commits per refresh pass, un-flushed leads under the next late row, and
 *     unreferenced {@code ALTER TABLE ADD COLUMN} commits inside a pass;</li>
 *     <li>restarts after any pass, a parked repair or a resident lead included;</li>
 *     <li>the cost, budget and switch knobs of every route ({@link LiveViewFuzzKnobs});</li>
 *     <li>episodes that build the shape of a bug an earlier review found, on a key of their
 *     own: an {@code ALTER} between late rows in one pass, a tie row behind late rows within
 *     one flush interval, and a held view writer that leaves the view's WAL ahead of its
 *     table.</li>
 * </ul>
 * Each test method is a sub-arm whose draws favour some routes, and asserts that its batch
 * reached them ({@link LiveViewRouteCoverage}): a later change to the cost model or a route's
 * gate must not quietly move the fuzz off a route.
 * <p>
 * The oracle runs after a share of the passes as well as at the end, because some silent bugs
 * heal before a run ends. Reads through the in-memory tier are checked while a lead is
 * resident, together with the view's symbol labels and a keyed {@code WINDOW JOIN} from it.
 * <p>
 * Every refresh fault a run records must be one the run explains: a fault the fault arm
 * injected, or a {@link TableReferenceOutOfDateException} after an unreferenced
 * {@code ALTER}. Such an {@code ALTER} invalidates the compiled factory, and a pass that
 * carries it together with late rows recovers through the metadata drift path, which counts
 * a fault. Each {@code ALTER} may account for one.
 */
public class LiveViewRepairRouteFuzzTest extends AbstractLiveViewTest {
    // Runs per test method, about half a second each. Raise it locally to soak.
    private static final int RUNS_PER_TEST = 10;
    // A pass that skips its repair's inline apply rather than injecting a fault.
    private static final int APPLY_SKIP = 11;
    private static final long DAY_MICROS = 86_400_000_000L;
    private static final long HOUR_MICROS = 3_600_000_000L;
    // How a pass decides whether to hold its flush off.
    private static final int PIN_NO = 0;
    private static final int PIN_RANDOM = -1;
    private static final int PIN_YES = 1;
    private static final String STAGING_BUFFER_CLOSE_FAULT = "injected staging-buffer close fault";
    // The sub-arms. Each one is a test method; see the methods for what each steers toward.
    private static final int STEER_ANCHORS = 3;
    private static final int STEER_FAULTS = 5;
    private static final int STEER_KEYED_OPEN_SEGMENT = 1;
    private static final int STEER_LEAD = 4;
    private static final int STEER_MIXED = 0;
    private static final int STEER_SPARSE_FALLBACK = 2;
    private static final String[] SYMBOLS = {"AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH"};
    // The key of the rows the episodes add beside the generated dataset. No generated row
    // carries it, so the episodes keep (ts, key) unique by tracking only their own timestamps.
    private static final String SYNTHETIC_KEY = "ZZ";
    // sum, count, max, row_number, avg, rank, dense_rank
    private static final int VARIANT_COUNT = 7;
    private final LiveViewRouteCoverage coverage = new LiveViewRouteCoverage();
    private final FaultLedger ledger = new FaultLedger();
    private final ObjList<String> runKeys = new ObjList<>();
    private final StringSink syntheticSink = new StringSink();
    private final LongHashSet syntheticTs = new LongHashSet();
    private int consecutiveFaultedPasses;
    private LiveViewMidDrainFault fault;
    private int firedFaults;
    private int firedThisRun;
    private boolean isOpenSegmentKeyedReplayForced;
    private LiveViewRefreshJob job;
    // The highest timestamp inserted into the base so far this run.
    private long maxInsertedTs;
    private int tierReadRecomputeChecks;
    private int tierReadTableOnlyChecks;
    private int windowJoinChecks;

    @Before
    public void pinClock() {
        setCurrentMicros(0);
    }

    @Test
    public void testAnchorShapes() throws Exception {
        // Daily anchors only: zone-less at an offset over data from 2025-10 and 2026, zoned
        // over data crossing DST changes or inside Berlin's repeated hour, and zone-less over
        // 1970 data below the origin. Some passes carry an ALTER between late rows.
        runBatch(STEER_ANCHORS);
    }

    @Test
    public void testFaults() throws Exception {
        // One injected fault at most per pass, file faults and hook faults, with a restart
        // before the retry now and then. The view must come out ACTIVE and equal to the
        // recompute, and every refresh fault must be one the arm injected.
        runBatch(STEER_FAULTS);
    }

    @Test
    public void testKeyedOpenSegment() throws Exception {
        // Corrections inside the open anchor segment above a sealed root, on an unfiltered
        // view over an indexed key with the sparse identity: the two keyed open-segment
        // routes.
        runBatch(STEER_KEYED_OPEN_SEGMENT);
    }

    @Test
    public void testLeadResident() throws Exception {
        // IN MEMORY views with most passes pinned, so late rows land on an un-flushed lead, some
        // repair applies skipped and some passes under a held view writer, so a view WAL block
        // stays committed but unapplied, and tie rows behind late rows within one flush
        // interval. Reads through the tier are checked at those points.
        runBatch(STEER_LEAD);
    }

    @Test
    public void testMixed() throws Exception {
        // Every anchor shape and knob, with some passes carrying an ALTER between late rows.
        runBatch(STEER_MIXED);
    }

    @Test
    public void testSparseFallback() throws Exception {
        // Exact duplicate rows. A keyed repair over a repeated (timestamp, key) pair cannot
        // upsert it without collapsing it, so it abandons its sparse publication.
        runBatch(STEER_SPARSE_FALLBACK);
    }

    private static String oracleProjection(int variant, String partition) {
        final String part = "PARTITION BY sym, " + partition + " ORDER BY ts";
        // The explicit ROWS frame matters for exact duplicate rows only: those are peers, and
        // the view accumulates row by row.
        final String frame = part + " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW";
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER (" + frame + ") AS v";
            case 1 -> "ts, sym, count() OVER (" + frame + ") AS v";
            case 2 -> "ts, sym, i, max(i) OVER (" + frame + ") AS v";
            case 3 -> "ts, sym, row_number() OVER (" + part + ") AS v";
            case 4 -> "ts, sym, x, avg(x) OVER (" + frame + ") AS v";
            case 5 -> "ts, sym, rank() OVER (" + part + ") AS v";
            case 6 -> "ts, sym, dense_rank() OVER (" + part + ") AS v";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    // Every output column, so the comparison is total even over exact duplicate rows.
    private static String orderByAll(int variant) {
        return switch (variant) {
            case 0, 2, 4 -> "1, 2, 3, 4";
            default -> "1, 2, 3";
        };
    }

    private static int[] order(Rnd rnd, int lo, int hi, int maxLateness) {
        final int[] a = new int[hi - lo];
        for (int k = 0; k < a.length; k++) {
            a[k] = lo + k;
        }
        if (maxLateness <= 0) {
            for (int k = a.length - 1; k > 0; k--) {
                final int j = rnd.nextInt(k + 1);
                final int tmp = a[k];
                a[k] = a[j];
                a[j] = tmp;
            }
        } else {
            for (int k = 0; k < a.length - 1; k++) {
                if (rnd.nextInt(4) == 0) {
                    final int j = k + 1 + rnd.nextInt(Math.min(maxLateness, a.length - k - 1));
                    final int tmp = a[k];
                    a[k] = a[j];
                    a[j] = tmp;
                }
            }
        }
        return a;
    }

    private static String steerName(int steer) {
        return switch (steer) {
            case STEER_MIXED -> "mixed";
            case STEER_KEYED_OPEN_SEGMENT -> "keyed open segment";
            case STEER_SPARSE_FALLBACK -> "sparse fallback";
            case STEER_ANCHORS -> "anchors";
            case STEER_LEAD -> "lead resident";
            case STEER_FAULTS -> "faults";
            default -> throw new IllegalArgumentException("steer=" + steer);
        };
    }

    private static String viewProjection(int variant) {
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER w AS v";
            case 1 -> "ts, sym, count() OVER w AS v";
            case 2 -> "ts, sym, i, max(i) OVER w AS v";
            case 3 -> "ts, sym, row_number() OVER w AS v";
            case 4 -> "ts, sym, x, avg(x) OVER w AS v";
            case 5 -> "ts, sym, rank() OVER w AS v";
            case 6 -> "ts, sym, dense_rank() OVER w AS v";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    /**
     * Arms one fault for the next pass and returns which. File faults fail an open of a base
     * WAL column, of a base column outside the WAL, of the view's checkpoint timeline, of its
     * state file or of its symbol key file; hook faults throw inside a repair. A refresh memory
     * limit breach is not on the menu: it invalidates the view by design.
     */
    private int armFault(Rnd rnd) {
        final LiveViewTestRefreshHooks hooks = hooks(job);
        final int pick = rnd.nextInt(11);
        switch (pick) {
            case 0 -> fault.arm(rnd.nextInt(3));
            case 1 -> fault.armAppliedScan();
            case 2 -> fault.armTimelineOpen();
            case 3 -> fault.armViewStateOpen();
            case 4 -> fault.armViewSymbolKeyOpen(viewInstance().getLiveViewToken().getDirName());
            case 5 -> hooks.setSimulateRepairCleanupFault(rnd.nextInt(2));
            case 6 -> {
                // The unwind cleanup runs only for a head-miss turn that is already unwinding, so
                // the fault rides on a failed read of the applied base, which unwinds a replay.
                hooks.setSimulateRepairUnwindCleanupFault();
                fault.armAppliedScan();
            }
            case 7 -> hooks.setSimulateStagingBufferCloseFault();
            case 8 -> hooks.setSimulateStoredSuffixCountFault();
            case 9 -> hooks.setSimulateColdKeyedTimelineFault(true);
            case 10 -> hooks.setSimulateKeyedTransplantFault(true);
            default -> throw new IllegalStateException("fault=" + pick);
        }
        LOG.info().$("LV route fuzz: armed fault [pick=").$(pick).I$();
        return pick;
    }

    /**
     * Asserts the routes and counters the arm is steered toward. Each assertion covers a
     * counter that every measured batch of the arm reached, most of them many times over: a
     * later change to a route's gate or to the cost model must not move the fuzz off a route
     * quietly. The rest of the table is logged, not asserted. No arm reaches
     * {@code KEY_DOMAIN_SPLICE_DECLINE_COUNT}, which needs a root sealed under an older state
     * format, and the keyed arm reaches {@code OPEN_SEGMENT_KEYED_CHEAPER_COUNT} in about one
     * batch of ten, too rarely to assert.
     */
    private void assertCoverage(int steer) {
        coverage.log(LOG, steerName(steer));
        LOG.info().$("live view route fuzz checks [arm=").$(steerName(steer))
                .$(", tierReadRecompute=").$(tierReadRecomputeChecks)
                .$(", tierReadTableOnly=").$(tierReadTableOnlyChecks)
                .$(", windowJoin=").$(windowJoinChecks)
                .$(", firedFaults=").$(firedFaults)
                .$(", injectedRefreshFaults=").$(ledger.injected)
                .$(", driftFaults=").$(ledger.drift).I$();
        switch (steer) {
            case STEER_MIXED, STEER_ANCHORS -> {
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD);
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_SEGMENT_REBUILD);
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                coverage.assertReachedStat(LiveViewRefreshStats.SEGMENT_YIELD_COUNT);
                assertTimestampGroupsRepaired();
            }
            case STEER_KEYED_OPEN_SEGMENT -> {
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_KEYED_COLD_OPEN_SEGMENT);
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_KEYED_RESUME_FROM_ANCHOR);
                coverage.assertReachedStat(LiveViewRefreshStats.TRANSPLANTED_KEY_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.OPEN_SEGMENT_KEYED_PRICED_COUNT);
            }
            case STEER_SPARSE_FALLBACK -> {
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT);
                coverage.assertReachedStat(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.SPARSE_PUBLICATION_FALLBACK_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.KEYED_SCAN_PRICED_COUNT);
            }
            case STEER_LEAD -> {
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_SEGMENT_REBUILD);
                Assert.assertTrue("no read through the tier matched the recompute over a resident lead", tierReadRecomputeChecks > 0);
                Assert.assertTrue("no read through the tier ran over an unapplied view WAL block", tierReadTableOnlyChecks > 0);
                Assert.assertTrue("no WINDOW JOIN from the view ran", windowJoinChecks > 0);
            }
            case STEER_FAULTS -> {
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD);
                coverage.assertReachedStat(LiveViewRefreshStats.OPEN_SEGMENT_KEYED_PRICED_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT);
                Assert.assertTrue("no read through the tier matched the recompute between faults", tierReadRecomputeChecks > 0);
                Assert.assertTrue("no WINDOW JOIN from the view ran between faults", windowJoinChecks > 0);
            }
            default -> throw new IllegalArgumentException("steer=" + steer);
        }
    }

    // Some repair checked a timestamp group wider than one row: the ties reached a repair.
    private void assertTimestampGroupsRepaired() {
        Assert.assertTrue(
                "no repair saw a timestamp tie\n" + coverage.table(),
                coverage.get(LiveViewRefreshStats.OUTPUT_UNIQUENESS_MAX_GROUP_ROWS) > 1
        );
    }

    private void assertView(String oracleSql, String orderBy, String where) throws Exception {
        final LiveViewInstance instance = viewInstance();
        Assert.assertFalse("[" + where + "] view invalid: " + instance.getInvalidationReason(), instance.isInvalid());
        try {
            TestUtils.assertSqlCursors(
                    engine,
                    sqlExecutionContext,
                    "(" + oracleSql + ") ORDER BY " + orderBy,
                    "(lv) ORDER BY " + orderBy,
                    LOG,
                    true
            );
        } catch (AssertionError e) {
            throw new AssertionError("[" + where + "] " + e.getMessage(), e);
        }
        assertSymbolLabelsConsistent("lv", "sym", runKeys);
    }

    // The checks that hold with a lead resident, a repair parked or a view WAL block pending.
    private void checkReads(Rnd rnd, PassShape shape, boolean isWindowJoinDue) throws Exception {
        switch (assertTierReadConsistent("lv", "base", shape.oracleSql, shape.orderBy)) {
            case TIER_READ_RECOMPUTE -> tierReadRecomputeChecks++;
            case TIER_READ_TABLE_ONLY -> tierReadTableOnlyChecks++;
            default -> {
            }
        }
        assertSymbolLabelsConsistent("lv", "sym", runKeys);
        if (isWindowJoinDue || rnd.nextInt(8) == 0) {
            assertViewWindowJoinMatchesCopy("lv", "base", "sym", "i", "t.v > 0");
            windowJoinChecks++;
        }
    }

    private void disarmFaults() {
        hooks(job).clearFaults();
        if (fault != null) {
            fault.disarmAll();
        }
    }

    // A drain that tolerates the one fault that escapes the job: the staging buffer close
    // stands in for an -ea assert, which the job lets through once it released the latch.
    private boolean drainCatchingStagingFault(boolean isRetryDriven) {
        try {
            if (isRetryDriven) {
                drainJobThroughRetryBackoff(job);
            } else {
                drainJob(job);
            }
            return false;
        } catch (AssertionError e) {
            if (STAGING_BUFFER_CLOSE_FAULT.equals(e.getMessage())) {
                return true;
            }
            throw e;
        }
    }

    private boolean hasFired(int pick) {
        final LiveViewTestRefreshHooks hooks = hooks(job);
        return switch (pick) {
            case 0 -> fault.hasFired();
            case 1 -> fault.hasAppliedScanFired();
            case 2 -> !fault.isTimelineOpenArmed();
            case 3 -> fault.hasViewStateOpenFired();
            case 4 -> fault.hasViewSymbolKeyOpenFired();
            case 5 -> !hooks.isRepairCleanupFaultArmed();
            case 6 -> !hooks.isRepairUnwindCleanupFaultArmed() || fault.hasAppliedScanFired();
            case 7 -> !hooks.isStagingBufferCloseFaultArmed();
            case 8 -> !hooks.isStoredSuffixCountFaultArmed();
            case 9 -> !hooks.isColdKeyedTimelineFaultArmed();
            case 10 -> !hooks.isKeyedTransplantFaultArmed();
            default -> throw new IllegalStateException("fault=" + pick);
        };
    }

    private void insertCommit(StringSink sink, LiveViewFuzzRows rows, int[] order, int from, int to) throws Exception {
        if (from >= to) {
            return;
        }
        sink.clear();
        sink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        for (int r = from; r < to; r++) {
            final int k = order[r];
            if (r > from) {
                sink.put(',');
            }
            sink.put('(').put(rows.tsv[k]).put("::timestamp,");
            maxInsertedTs = Math.max(maxInsertedTs, rows.tsv[k]);
            if (rows.symIdx[k] == LiveViewFuzzRows.NULL_KEY) {
                sink.put("null,");
            } else {
                sink.put('\'').put(SYMBOLS[rows.symIdx[k]]).put("',");
            }
            if (rows.iv[k] == Numbers.LONG_NULL) {
                sink.put("null,");
            } else {
                sink.put(rows.iv[k]).put(',');
            }
            if (rows.xNull[k]) {
                sink.put("null");
            } else {
                sink.put(rows.xv[k]);
            }
            sink.put(')');
        }
        execute(sink);
    }

    private LiveViewRefreshJob newJob() {
        final LiveViewRefreshJob newJob = new LiveViewRefreshJob(0, engine, 1);
        final LiveViewTestRefreshHooks hooks = hooks(newJob);
        hooks.setRefreshFaultObserver(ledger::accept);
        hooks.setForceOpenSegmentKeyedReplay(isOpenSegmentKeyedReplayForced);
        return newJob;
    }

    /**
     * One refresh pass over whatever commits are waiting: a pinned flush now and then, at most
     * one injected fault or skipped repair apply, the read checks that hold mid-flight, and a
     * restart now and then. {@code forcedPick} is the fault the caller already armed, or -1 to
     * draw one.
     */
    private void pass(Rnd rnd, PassShape shape, int forcedPick) throws Exception {
        pass(rnd, shape, forcedPick, PIN_RANDOM, true);
    }

    /**
     * @param pinMode          {@link #PIN_RANDOM} draws whether the pass holds its flush off,
     *                         {@link #PIN_YES} and {@link #PIN_NO} decide it
     * @param isRestartAllowed false inside an episode, whose passes must run on one job
     */
    private void pass(Rnd rnd, PassShape shape, int forcedPick, int pinMode, boolean isRestartAllowed) throws Exception {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        final boolean isPinned = pinMode == PIN_RANDOM ? rnd.nextInt(100) < shape.pinPct : pinMode == PIN_YES;
        if (isPinned) {
            // Keeps this pass's rows as an un-flushed lead under the next late row.
            viewInstance().setLastFlushTimeUs(currentMicros);
        }
        final long yieldsBefore = job.getStatsForTest().get(LiveViewRefreshStats.SEGMENT_YIELD_COUNT);
        // A restarted view recovers on its first turn, and a recovery whose rebuild faults
        // invalidates the view by design, so no fault goes into that turn.
        final boolean isRecovered = viewInstance().isCheckpointRestoreAttempted();
        final int pick;
        if (forcedPick >= 0) {
            pick = forcedPick;
        } else if (shape.faultPct > 0 && isRecovered && consecutiveFaultedPasses < 2 && rnd.nextInt(100) < shape.faultPct) {
            pick = armFault(rnd);
        } else if (rnd.nextInt(100) < shape.applySkipPct) {
            pick = APPLY_SKIP;
            hooks(job).setSimulateRepairApplyFailure(true);
        } else {
            pick = -1;
        }
        if (pick == APPLY_SKIP) {
            drainJob(job);
            // A repair's own block is committed and not applied: reads must stay on the table
            // as it stands.
            checkReads(rnd, shape, false);
            hooks(job).clearFaults();
        } else if (pick >= 0) {
            injectedPass(rnd, shape, pick);
        } else {
            consecutiveFaultedPasses = 0;
            drainJob(job);
        }
        drainWalQueue();
        if (isPinned || pick >= 0) {
            checkReads(rnd, shape, false);
        }

        final LiveViewInstance instance = viewInstance();
        final boolean isParked = instance.getSuspendedRepair() != null
                || job.getStatsForTest().get(LiveViewRefreshStats.SEGMENT_YIELD_COUNT) > yieldsBefore;
        final boolean isLeadResident = instance.getLeadRowCount() > 0;
        if (isRestartAllowed
                && shape.restartPct > 0
                && instance.getStateReader().getSeedState() == LiveViewState.SEED_STATE_ACTIVE
                && (rnd.nextInt(100) < shape.restartPct
                || (isParked && rnd.nextInt(3) == 0)
                || (isLeadResident && rnd.nextInt(4) == 0))) {
            restart();
        }
    }

    // The pass carrying an armed fault, which counts as injected every refresh fault it
    // records until its retry is through.
    private void injectedPass(Rnd rnd, PassShape shape, int pick) throws Exception {
        ledger.isInjectionOpen = true;
        // A reader that pins the slot holding the lead keeps a recovery from taking back the
        // symbol ids the lead interned.
        final LiveViewInMemoryTier tier = viewInstance().getInMemoryTier();
        int pin = tier != null && rnd.nextInt(3) == 0 ? tier.acquireRead() : -1;
        try {
            boolean isFired = drainCatchingStagingFault(false);
            isFired |= hasFired(pick);
            if (isFired && rnd.nextInt(4) == 0) {
                // A crash right after the faulted turn, before its retry.
                disarmFaults();
                ledger.isInjectionOpen = false;
                if (pin >= 0) {
                    tier.releaseRead(pin);
                    pin = -1;
                }
                restart();
            } else {
                isFired |= drainCatchingStagingFault(true);
                isFired |= hasFired(pick);
                disarmFaults();
                ledger.isInjectionOpen = false;
                if (isFired) {
                    // The reads a recovery's discarded symbol ids would mislabel, before a
                    // later turn or a restart heals them, and while a pin holds them if one does.
                    drainWalQueue();
                    checkReads(rnd, shape, true);
                }
            }
            LOG.info().$("LV route fuzz: injected pass done [pick=").$(pick).$(", fired=").$(isFired).I$();
            if (isFired) {
                firedThisRun++;
                firedFaults++;
                consecutiveFaultedPasses++;
            } else {
                consecutiveFaultedPasses = 0;
            }
        } finally {
            ledger.isInjectionOpen = false;
            if (pin >= 0) {
                tier.releaseRead(pin);
            }
        }
    }

    /**
     * An unreferenced {@code ALTER} between an in-order row and two late rows, all applied
     * before one refresh pass reads them: the first late row in the segment the frontier stands
     * in, the second a day below it. The drain walks the in-order row and the {@code ALTER}, and
     * stops on the first late row with no change ceiling, since the {@code ALTER} could have
     * changed rows anywhere; the base has applied the second by then, which makes it the
     * apply-ahead range. A plan that folded that range's maximum into the missing ceiling once
     * ended the repair below the first late row, and the view lost it.
     */
    private void ddlBetweenLateRowsEpisode(Rnd rnd, PassShape shape, long dataStartMicros) throws Exception {
        // The commits already waiting go first, so the four statements below reach a pass of
        // their own.
        pass(rnd, shape, -1, PIN_NO, false);
        final long frontier = maxInsertedTs;
        insertSynthetic(frontier + 1);
        execute("ALTER TABLE base ADD COLUMN extra_ddl" + syntheticTs.size() + " INT");
        ledger.driftCredits++;
        insertSynthetic(Math.max(dataStartMicros, frontier - 1 - rnd.nextLong(HOUR_MICROS)));
        insertSynthetic(Math.max(dataStartMicros, frontier - DAY_MICROS - rnd.nextLong(HOUR_MICROS)));
        drainWalQueue();
        pass(rnd, shape, -1, PIN_RANDOM, false);
    }

    /**
     * Holds the view's table writer across a few passes, as a busy writer or a memory-pressure
     * back-off does: no apply of the view's own WAL lands, so a repair's or a flush's block
     * stays committed and unapplied while later drains publish leads. Reads must stay on the
     * table as it stands throughout ({@link #assertTierReadConsistent}).
     */
    private void heldWriterEpisode(Rnd rnd, PassShape shape) throws Exception {
        // Under the reason the WAL apply job takes the writer with, which is what a busy writer
        // looks like to the view's own apply.
        final TableWriter writer = engine.getWriterUnsafe(viewInstance().getLiveViewToken(), TableUtils.WAL_2_TABLE_WRITE_REASON);
        try {
            for (int k = 0, n = 1 + rnd.nextInt(3); k < n; k++) {
                final long frontier = maxInsertedTs;
                insertSynthetic(rnd.nextBoolean() ? frontier + 1 + rnd.nextInt(1_000_000) : frontier - 1 - rnd.nextLong(HOUR_MICROS));
                drainWalQueue();
                pass(rnd, shape, -1, PIN_RANDOM, false);
                checkReads(rnd, shape, false);
            }
        } finally {
            writer.close();
        }
        drainWalQueue();
    }

    // One base row on the synthetic key, at the first timestamp at or below {@code ts} the key
    // has not used.
    private void insertSynthetic(long ts) throws SqlException {
        long t = ts;
        while (!syntheticTs.add(t)) {
            t--;
        }
        maxInsertedTs = Math.max(maxInsertedTs, t);
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES (").put(t).put("::timestamp, '")
                .put(SYNTHETIC_KEY).put("', ").put(1 + (t & 511)).put(", 1.0)");
        execute(syntheticSink);
    }

    /**
     * A flushed pass, a late row, then a row tying the newest timestamp, then another late row,
     * the last two within one flush interval. The first late row's repair runs to the end of the
     * base and
     * re-versions the root at the newest timestamp; the tie row drains into the un-flushed lead;
     * the second late row discards that lead and resumes from the root. A resume that trusted
     * the root over the tie it no longer covers once skipped the tie row for good.
     */
    private void tieBehindLateRowsEpisode(Rnd rnd, PassShape shape, long dataStartMicros) throws Exception {
        // A flushed pass first, so the cadence can seal a root at the frontier.
        pass(rnd, shape, -1, PIN_NO, false);
        insertSynthetic(Math.max(dataStartMicros, maxInsertedTs - 1 - rnd.nextLong(HOUR_MICROS)));
        drainWalQueue();
        pass(rnd, shape, -1, PIN_NO, false);
        if (syntheticTs.contains(maxInsertedTs)) {
            return;
        }
        insertSynthetic(maxInsertedTs);
        drainWalQueue();
        pass(rnd, shape, -1, PIN_YES, false);
        insertSynthetic(Math.max(dataStartMicros, maxInsertedTs - 1 - rnd.nextLong(HOUR_MICROS)));
        drainWalQueue();
        pass(rnd, shape, -1, PIN_YES, false);
    }

    // A crash-like restart: the registry drops every instance, a lead and a parked repair
    // included, and rebuilds from disk.
    private void restart() {
        coverage.absorb(job);
        job = Misc.free(job);
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
        job = newJob();
    }

    private void runBatch(int steer) throws Exception {
        final Rnd rnd = TestUtils.generateRandom(LOG);
        if (steer == STEER_FAULTS) {
            fault = new LiveViewMidDrainFault("ts", "ts", "sym");
            assertMemoryLeak(fault.facade(), () -> {
                for (int run = 0; run < RUNS_PER_TEST; run++) {
                    runOne(rnd, run, steer);
                }
            });
        } else {
            assertMemoryLeak(() -> {
                for (int run = 0; run < RUNS_PER_TEST; run++) {
                    runOne(rnd, run, steer);
                }
            });
        }
        assertCoverage(steer);
    }

    private void runOne(Rnd rnd, int run, int steer) throws Exception {
        final LiveViewFuzzKnobs knobs = LiveViewFuzzKnobs.randomize(rnd);
        int anchorMode = rnd.nextInt(LiveViewFuzzAnchor.MODE_COUNT);
        final int variant = rnd.nextInt(VARIANT_COUNT);
        final int rowCount = 150 + rnd.nextInt(250);
        int symCount = 1 + rnd.nextInt(SYMBOLS.length);
        boolean isIndexed = rnd.nextInt(4) != 0;
        boolean isInMemory = rnd.nextBoolean();
        boolean isFiltered = rnd.nextInt(4) == 0;
        int stepMode = rnd.nextInt(3);
        final boolean isSeeded = rnd.nextInt(3) == 0;
        int tiePct = rnd.nextInt(3) == 0 ? 0 : 10 + rnd.nextInt(40);
        final int tieCutPct = tiePct > 0 ? rnd.nextInt(60) : 0;
        int duplicatePct = 0;
        final int deferPct = rnd.nextInt(60);
        int pinPct = isInMemory ? rnd.nextInt(60) : 0;
        int applySkipPct = 0;
        final int restartPct = rnd.nextInt(3) == 0 ? 0 : 5 + rnd.nextInt(15);
        final int checkPct = 10 + rnd.nextInt(20);
        int maxLateness = rnd.nextBoolean() ? 0 : 1 + rnd.nextInt(40);
        int faultPct = 0;
        // Shares of passes that run an episode instead: see ddlBetweenLateRowsEpisode,
        // tieBehindLateRowsEpisode and heldWriterEpisode.
        int ddlEpisodePct = 0;
        int leadEpisodePct = 0;
        isOpenSegmentKeyedReplayForced = false;
        // Half the fault runs take the keyed open-segment shape, so the faults placed in the keyed
        // routes - the cold keyed timeline, the stored row count, the transplant - have a route
        // to fire in.
        final boolean isKeyedShape = steer == STEER_KEYED_OPEN_SEGMENT || (steer == STEER_FAULTS && rnd.nextBoolean());
        if (isKeyedShape) {
            // The open segment is the day the frontier stands in. Small lateness keeps the
            // corrections inside it, tight steps put many rows and so many roots in it, and the
            // view carries the sparse identity on an unfiltered key.
            anchorMode = rnd.nextBoolean() ? LiveViewFuzzAnchor.DAILY_PLAIN : LiveViewFuzzAnchor.EXPRESSION;
            stepMode = 0;
            // Shallow lateness keeps a correction in the open segment above a root. One run in
            // four reaches deeper into the open day, where a keyed read of one key can price below the
            // rows above the root, and every run spreads the rows over more keys, so one key is
            // a small share of them.
            maxLateness = 1 + rnd.nextInt(rnd.nextInt(4) == 0 ? 40 : 8);
            symCount = 4 + rnd.nextInt(SYMBOLS.length - 3);
            isIndexed = true;
            isFiltered = false;
            knobs.checkpointRows = 1 + rnd.nextInt(2);
            knobs.indexOpenRows = 1;
            knobs.isIsolatedRuntime = true;
            knobs.isKeyedReplay = true;
            knobs.isOpenSegmentKeyedReplay = true;
            knobs.isSparsePublication = true;
            knobs.scanMaxKeys = LiveViewFuzzKnobs.DEFAULT;
            knobs.scanMaxRows = LiveViewFuzzKnobs.DEFAULT;
            // Forcing takes the route whatever the pricing says, which the keyed arm always does
            // so every batch reaches it; the fault arm leaves one run in four to the pricing.
            isOpenSegmentKeyedReplayForced = steer == STEER_KEYED_OPEN_SEGMENT || rnd.nextInt(4) != 0;
        }
        switch (steer) {
            case STEER_SPARSE_FALLBACK -> {
                duplicatePct = 5 + rnd.nextInt(11);
                isIndexed = true;
                knobs.indexOpenRows = 1;
                knobs.isKeyedReplay = true;
                knobs.isPerSegment = true;
                knobs.isSparsePublication = true;
                knobs.scanMaxKeys = LiveViewFuzzKnobs.DEFAULT;
                knobs.scanMaxRows = LiveViewFuzzKnobs.DEFAULT;
            }
            case STEER_ANCHORS -> {
                anchorMode = 1 + rnd.nextInt(LiveViewFuzzAnchor.MODE_COUNT - 1);
                ddlEpisodePct = 6;
            }
            case STEER_LEAD -> {
                isInMemory = true;
                pinPct = 30 + rnd.nextInt(50);
                applySkipPct = 10 + rnd.nextInt(30);
                leadEpisodePct = 15;
            }
            case STEER_MIXED -> ddlEpisodePct = 6;
            case STEER_FAULTS -> {
                isInMemory = rnd.nextInt(3) != 0;
                pinPct = isInMemory ? rnd.nextInt(60) : 0;
                faultPct = 15;
            }
            default -> {
            }
        }
        if ((steer == STEER_MIXED || steer == STEER_ANCHORS) && run % 3 == 0) {
            // A one-row replay budget with the yield on parks nearly every repair, so each batch
            // has runs that restart over a parked repair and resume one.
            knobs.isSegmentYield = true;
            knobs.replayMaxRows = 1;
        }
        if ((steer == STEER_MIXED || steer == STEER_ANCHORS) && run % 3 == 1 && tiePct == 0) {
            // And runs whose repairs meet ties.
            tiePct = 30 + rnd.nextInt(20);
        }
        if ((steer == STEER_MIXED || steer == STEER_ANCHORS) && run % 3 == 2) {
            // And runs where one turn walks an ALTER and the late rows behind it: a turn budget
            // of one commit would walk the ALTER on a turn of its own.
            knobs.turnMaxCommits = 64;
            ddlEpisodePct = 20;
        }
        // The keyed shape packs more rows into each day, so the open segment holds enough of
        // them for a keyed read to price below a whole one, and partitions by hour more often,
        // which is what lets the postings be counted against more than one partition.
        final boolean isHourPartitioned = stepMode < 2 && rnd.nextInt(isKeyedShape ? 2 : 4) == 0;
        int stepMax = stepMode == 0 ? 5_000_000 : stepMode == 1 ? 60_000_000 : 900_000_000;
        int dayJumpEvery = isKeyedShape ? 60 : stepMode == 0 ? 20 : 12;
        knobs.apply(AbstractLiveViewTest::setProperty);

        final LiveViewFuzzAnchor anchor = LiveViewFuzzAnchor.draw(rnd, anchorMode, true);
        if (anchor.denseStepMaxMicros > 0) {
            // The shape's own data: dense, with no day jumps, so it covers its night.
            stepMax = anchor.denseStepMaxMicros;
            dayJumpEvery = Integer.MAX_VALUE;
        }
        final LiveViewFuzzRows rows = LiveViewFuzzRows.generate(
                rnd, rowCount, symCount, anchor.dataStartMicros, stepMax, dayJumpEvery, tiePct);
        if (duplicatePct > 0) {
            rows.addExactDuplicates(rnd, duplicatePct);
        }
        runKeys.clear();
        for (int s = 0; s < symCount; s++) {
            runKeys.add(SYMBOLS[s]);
        }
        runKeys.add(SYNTHETIC_KEY);
        syntheticTs.clear();
        maxInsertedTs = Long.MIN_VALUE;
        setCurrentMicros(anchor.clockMicros);

        final String where = isFiltered ? " WHERE i > 0" : "";
        final String viewSql = "SELECT " + viewProjection(variant) + " FROM base" + where
                + " WINDOW w AS (PARTITION BY sym ORDER BY ts " + anchor.clause + ")";
        final String oracleSql = "SELECT " + oracleProjection(variant, anchor.oraclePartition()) + " FROM " + anchor.oracleFrom("base", where);
        final String orderBy = orderByAll(variant);
        final StringSink knobLog = new StringSink();
        knobs.toSink(knobLog);
        LOG.info().$("LV route fuzz: arm=").$(steerName(steer)).$(", run=").$(run).$(", variant=").$(variant)
                .$(", rows=").$(rowCount).$(", symCount=").$(symCount).$(", indexed=").$(isIndexed)
                .$(", anchor=").$(anchor.clause).$(", inMemory=").$(isInMemory).$(", filtered=").$(isFiltered)
                .$(", hourPartitioned=").$(isHourPartitioned).$(", seeded=").$(isSeeded)
                .$(", tiePct=").$(tiePct).$(", ties=").$(rows.getTieCount()).$(", tieCutPct=").$(tieCutPct)
                .$(", duplicates=").$(rows.getDuplicateCount()).$(", deferPct=").$(deferPct)
                .$(", pinPct=").$(pinPct).$(", applySkipPct=").$(applySkipPct).$(", restartPct=").$(restartPct)
                .$(", checkPct=").$(checkPct).$(", maxLateness=").$(maxLateness).$(", stepMode=").$(stepMode)
                .$(", faultPct=").$(faultPct).$(", forceOpenKeyed=").$(isOpenSegmentKeyedReplayForced)
                .$(", knobs=[").$(knobLog).$("]").I$();

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL" + (isIndexed ? " INDEX" : "")
                + ", i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY " + (isHourPartitioned ? "HOUR" : "DAY") + " WAL");
        if (fault != null) {
            fault.of(engine.verifyTableName("base").getDirName());
        }

        ledger.clear();
        firedThisRun = 0;
        consecutiveFaultedPasses = 0;
        final StringSink sink = new StringSink();
        final int preCount = isSeeded ? rnd.nextInt(rowCount / 2 + 1) : 0;
        final PassShape shape = new PassShape(oracleSql, orderBy, pinPct, applySkipPct, faultPct, restartPct);
        try {
            if (preCount > 0) {
                final int[] preOrder = order(rnd, 0, preCount, maxLateness);
                insertCommit(sink, rows, preOrder, 0, preCount);
                drainWalQueue();
            }
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms " + (isInMemory ? "IN MEMORY 1h " : "")
                    + "START FROM BEGINNING AS " + viewSql);
            job = newJob();
            if (preCount > 0) {
                driveSeedToCompletion(job, "lv");
            }

            final int[] order = order(rnd, preCount, rowCount, maxLateness);
            final int commits = Math.max(1, Math.min(order.length, 4 + rnd.nextInt(30)));
            final int[] bounds = new int[commits + 1];
            for (int c = 0; c <= commits; c++) {
                bounds[c] = (int) ((long) c * order.length / commits);
            }
            rows.cutInsideTieGroups(rnd, order, bounds, tieCutPct);
            int alters = 0;
            for (int c = 1; c <= commits; c++) {
                insertCommit(sink, rows, order, bounds[c - 1], bounds[c]);
                if (alters < 3 && rnd.nextInt(25) == 0) {
                    // A non-data commit the view does not read, inside the same refresh pass.
                    execute("ALTER TABLE base ADD COLUMN extra" + alters++ + " INT");
                    ledger.driftCredits++;
                }
                drainWalQueue();
                if (c < commits && rnd.nextInt(100) < deferPct) {
                    continue; // fold this commit into the next refresh pass
                }
                if (ddlEpisodePct > 0 && alters < 3 && rnd.nextInt(100) < ddlEpisodePct) {
                    alters++;
                    ddlBetweenLateRowsEpisode(rnd, shape, anchor.dataStartMicros);
                } else if (leadEpisodePct > 0 && rnd.nextInt(100) < leadEpisodePct) {
                    if (rnd.nextBoolean()) {
                        tieBehindLateRowsEpisode(rnd, shape, anchor.dataStartMicros);
                    } else {
                        heldWriterEpisode(rnd, shape);
                    }
                } else {
                    pass(rnd, shape, -1);
                }
                if (rnd.nextInt(100) < checkPct) {
                    driveRefreshToQuiescence(job);
                    assertView(oracleSql, orderBy, "intermediate, commit " + c + "/" + commits);
                }
            }
            if (faultPct > 0 && firedThisRun == 0) {
                // No drawn fault fired. One in-order row above every other one, whose forward
                // drain reads the commit's timestamp column from the WAL, takes the fault that
                // read always reaches, so no run is vacuous. The recompute reads it like any
                // other base row. The job keeps a WAL segment it has read open, so the pooled
                // WAL writers go first: the row then lands in a segment the drain has to open.
                driveRefreshToQuiescence(job);
                engine.releaseInactive();
                execute("INSERT INTO base (ts, sym, i, x) VALUES (" + (rows.tsv[rowCount - 1] + 1) + "::timestamp, 'AA', 1, 1.0)");
                drainWalQueue();
                fault.arm(0);
                pass(rnd, shape, 0);
            }
            driveRefreshToQuiescence(job);
        } finally {
            coverage.absorb(job);
            job = Misc.free(job);
        }
        assertView(oracleSql, orderBy, "final");
        if (viewInstance().getInMemoryTier() != null) {
            // A run that ended on a restart with nothing left to refresh has no tier yet.
            assertModeBMatchesDiskOnly("lv", "SELECT * FROM lv");
        }
        ledger.assertExplained("arm=" + steerName(steer) + ", run=" + run);
        if (faultPct > 0) {
            Assert.assertTrue("no injected fault fired in run " + run + ", so it tested no recovery", firedThisRun > 0);
        }
        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    /**
     * What every pass of one run draws from.
     */
    private record PassShape(String oracleSql, String orderBy, int pinPct, int applySkipPct, int faultPct, int restartPct) {
    }

    /**
     * The refresh faults one run recorded, sorted into the ones it explains and the rest.
     * Fed by the job's fault hook, inside its failure handler, so it only records.
     */
    private static final class FaultLedger {
        private final ObjList<Throwable> unexplained = new ObjList<>();
        // ALTERs whose metadata drift recovery has not been seen yet.
        private int driftCredits;
        private int drift;
        private int injected;
        private boolean isInjectionOpen;

        void accept(Throwable t) {
            if (isInjectionOpen) {
                injected++;
            } else if (t instanceof TableReferenceOutOfDateException && driftCredits > 0) {
                driftCredits--;
                drift++;
            } else {
                unexplained.add(t);
            }
        }

        void assertExplained(String where) {
            if (unexplained.size() == 0) {
                return;
            }
            final StringSink sink = new StringSink();
            sink.put("[").put(where).put("] refresh faults the run did not inject and no ALTER explains:");
            for (int i = 0, n = unexplained.size(); i < n; i++) {
                final Throwable t = unexplained.getQuick(i);
                sink.put("\n  ").put(t.toString());
                final StackTraceElement[] stack = t.getStackTrace();
                for (int j = 0; j < Math.min(stack.length, 12); j++) {
                    sink.put("\n      at ").put(stack[j].toString());
                }
            }
            Assert.fail(sink.toString());
        }

        void clear() {
            unexplained.clear();
            driftCredits = 0;
            isInjectionOpen = false;
        }
    }
}
