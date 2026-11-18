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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.lv.LiveViewCheckpointAnchorPlan;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRepairMarker;
import io.questdb.cairo.lv.LiveViewCheckpointRepairRoute;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewRefreshStats;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.concurrent.atomic.AtomicInteger;

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
    // The anchor dstFrontierEpisode runs under: daily at a wall time the Berlin fall-back repeats
    // and the spring-forward skips, so a segment around either transition has no start of its own.
    private static final String BERLIN_REPEATED_ANCHOR_CLAUSE = "ANCHOR DAILY '02:30' 'Europe/Berlin'";
    private static final long DAY_MICROS = 86_400_000_000L;
    // Where dstFrontierEpisode puts the view's frontier once its departure leaves it: in the half
    // hour before the fall-back, whose segment has no start of its own; in the repeated half hour
    // after it, whose rows carry the previous day's anchor value; or on the spring-forward day,
    // whose segment has no start of its own either.
    private static final int DST_FRONTIER_IN_OPEN_RUN = 0;
    private static final int DST_FRONTIER_IN_REPEATED_HALF_HOUR = 1;
    private static final int DST_FRONTIER_ON_SPRING_FORWARD_DAY = 2;
    // Salt the draws of dstFrontierEpisode apart from the run's sequence, so a run that reaches it
    // draws everything else as it did before the episode existed.
    private static final long DST_FRONTIER_SALT0 = 0x3C6EF372FE94F82BL;
    private static final long DST_FRONTIER_SALT1 = 0xA54FF53A5F1D36F1L;
    private static final int DST_FRONTIER_SHAPES = 3;
    private static final long MINUTE_MICROS = 60_000_000L;
    // The rows each batch of departureEpisode commits: above the largest row cadence the knobs
    // draw, so each batch seals a root of its own.
    private static final int DEPARTURE_BATCH_ROWS = 96;
    // The rows of SYNTHETIC_TIE_KEY each step of sparsePublicationEpisode commits around its late row, so
    // the keyed read of SYNTHETIC_KEY prices below the whole segment under every anchor and partitioning
    // the arm draws. An hourly anchor over DAY partitions estimates the segment at 1/24 of the partition
    // holding it, 16 rows here, against at most 8 for the keyed read: 3 or 4 postings, one index open and
    // a seek per piece the late row's merge leaves in the partition.
    private static final int SPARSE_EPISODE_FILL_ROWS = 384;
    // The variants before rank (see VARIANT_COUNT): their functions hold their state in the
    // fused anchor root. rank() and dense_rank() keep a partition map of their own, which the
    // keyed open-segment routes decline (LiveViewRefreshJob.isEveryFunctionDurablyGrouped).
    private static final int GROUPED_VARIANT_COUNT = 5;
    private static final long HOUR_MICROS = 3_600_000_000L;
    // Salt the draws of keptTimelineRetireEpisode apart from the run's sequence, so a run that
    // reaches it draws everything else as it did before the episode existed.
    private static final long KEPT_TIMELINE_SALT0 = 0x510E527FADE682D1L;
    private static final long KEPT_TIMELINE_SALT1 = 0x9B05688C2B3E6C1FL;
    // Salt the draw of keptTimelineRetireEpisode's fault on its check of _timeline apart from the
    // others, so the episode draws what it drew before the fault existed.
    private static final long KEPT_TIMELINE_CHECK_SALT0 = 0x6A09E667F3BCC908L;
    private static final long KEPT_TIMELINE_CHECK_SALT1 = 0xBB67AE8584CAA73BL;
    // What keptTimelineRetireEpisode's retire meets besides the lock on _timeline alone, drawn as 0:
    // the lock with the check of _timeline after the unlink failing, or that check failing alone,
    // after an unlink that removed the file.
    private static final int KEPT_TIMELINE_FAULT_CHECK = 2;
    private static final int KEPT_TIMELINE_FAULT_LOCK_AND_CHECK = 1;
    // Salt the draw of keptTimelineRetireEpisode's lock on the repair marker apart from both, so
    // the episode draws what it drew before the lock existed.
    private static final long KEPT_TIMELINE_MARKER_SALT0 = 0x1F83D9ABFB41BD6BL;
    private static final long KEPT_TIMELINE_MARKER_SALT1 = 0x5BE0CD19137E2179L;
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
    // carries it, so the episodes keep (ts, key) unique by tracking only their own timestamps,
    // but for the exact copy sparsePublicationEpisode commits on purpose, last in its run.
    private static final String SYNTHETIC_KEY = "ZZ";
    // The key of the second row of the tie tieInClosedSegmentEpisode builds, at a timestamp it
    // claims for SYNTHETIC_KEY, and of rows other episodes put where it has none, so its
    // (ts, key) stays unique too.
    private static final String SYNTHETIC_TIE_KEY = "YY";
    // sum, count, max, row_number, avg, rank, dense_rank
    private static final int VARIANT_COUNT = 7;
    // Fails a test during which a WAL apply suspended a table, which would leave the oracles comparing
    // a frozen base or view table.
    @Rule
    public final WalApplySuspensionRule walApplySuspensionRule = new WalApplySuspensionRule();
    private final LiveViewRouteCoverage coverage = new LiveViewRouteCoverage();
    private final FaultLedger ledger = new FaultLedger();
    private final ObjList<String> runKeys = new ObjList<>();
    private final StringSink syntheticSink = new StringSink();
    private final LongHashSet syntheticTs = new LongHashSet();
    // The lock on _timeline the mixed arm's episodes take: see keptTimelineRetireEpisode and
    // keptSpliceEpisode.
    private final TimelineLock timelineLock = new TimelineLock();
    private int consecutiveFaultedPasses;
    // Episodes of dstFrontierEpisode the batch ran, logged with the route coverage.
    private int dstFrontierEpisodes;
    private LiveViewMidDrainFault fault;
    private int firedFaults;
    private int firedThisRun;
    private boolean isOpenSegmentKeyedReplayForced;
    private LiveViewRefreshJob job;
    // Late rows keptTimelineRetireEpisode committed while the lock kept the timeline, and after
    // the lock went, and the episodes; and the episodes of keptSpliceEpisode.
    private int keptSpliceEpisodes;
    // Episodes of keptTimelineRetireEpisode whose retire under the lock could not check _timeline
    // after the unlink.
    private int keptTimelineCheckFaults;
    private int keptTimelineLateRowsLocked;
    private int keptTimelineLateRowsReleased;
    // Episodes of keptTimelineRetireEpisode in which a seal was refused a runtime history beside
    // the marker a lock kept once _timeline went.
    private int keptTimelineMarkerRefusals;
    private int keptTimelineRetireEpisodes;
    // Episodes of keptTimelineRetireEpisode whose retire removed _timeline and could not check it.
    private int removedTimelineCheckFaults;
    // Cycles without rows that sealed the root a keyed resume owed once its closing seal failed:
    // see keyedOwedRootEpisode.
    private int keyedOwedRootSeals;
    // Second keyed late rows inside the window a failed closing seal left open whose replacement
    // commit failed in process, and whose process stopped at it, and the episodes that reached
    // either: see secondKeyedRepairCommitFaultEpisode.
    private int secondKeyedRepairCommitFaultEpisodes;
    private int secondKeyedRepairCommitFaults;
    private int secondKeyedRepairCommitStops;
    // The highest timestamp inserted into the base so far this run.
    private long maxInsertedTs;
    // The keyed prices and keyed closed-segment repairs of sparsePublicationEpisode's steps this
    // batch, which assertCoverage takes off the batch's totals: the drawn runs must reach both too.
    private long sparseEpisodeKeyedPrices;
    private long sparseEpisodeKeyedRepairs;
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
    public void testAnchorShapesWhereEveryDrawnDepartureMissesTheSpliceTruncate() throws Exception {
        // A seed whose ten departures all missed the splice frontier truncate before every other
        // one was steered onto it: the key budget declined the chains of runs 1, 4 and 8, the
        // count guard those of runs 2 and 5, and a repair the main loop parked took the passes of
        // runs 0, 3, 6, 7 and 9. The steered runs 0, 2, 4, 6 and 8 truncate whatever they drew,
        // one each; the drawn ones still miss, run 1's on the key budget. Run 2 drew the chain
        // count 0 and the key budget 1, so its steered departure, whose repair runs under the
        // defaults, also checks that the rest of the run gets those draws back.
        runBatch(STEER_ANCHORS, TestUtils.generateRandom(LOG, 6340059912079L, 1794566112891L));
        Assert.assertEquals(5, coverage.get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT));
        Assert.assertEquals(1, coverage.get(LiveViewRefreshStats.RESUME_CHAIN_KEY_BUDGET_DECLINE_COUNT));
    }

    @Test
    public void testClosedSegmentStartAboveSkipsTheSegmentsBesideBerlinTransitions() throws Exception {
        // The segments a step of sparsePublicationEpisode must not take, under the anchor of the arm's
        // Berlin runs (LiveViewCheckpointAnchorPlan). Away from a transition the search takes the first
        // segment start at or above its floor, 02:30 CET. The 2026 spring-forward skips 02:30 on
        // 2026-03-29, so the segment from 2026-03-28 has no exact end and the next one starts half an
        // hour below its own anchor value: the search lands on 02:30 CEST on 2026-03-30. The 2026
        // fall-back gives the segment from 2026-10-24 a second part, 01:00Z to 01:30Z on 2026-10-25,
        // which reads 02:00 to 02:29 CET again, so that segment has no exact end, and those rows sit
        // above the segment starting at 02:30 CEST that day, which has no start of its own: the
        // search lands on the day's second 02:30, in CET.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT " + viewProjection(0)
                    + " FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts " + BERLIN_REPEATED_ANCHOR_CLAUSE + ")");
            job = newJob();
            try {
                // The view builds its anchor on its first refresh.
                insertKeyBatch(ts("2026-01-01T12:00:00.000000Z"), 0, 1);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                final LiveViewCheckpointAnchorPlan anchorPlan = viewInstance().getAnchorWindow().getCheckpointAnchorPlan();
                Assert.assertEquals(ts("2026-01-10T01:30:00.000000Z"), closedSegmentStartAbove(anchorPlan, ts("2026-01-10T00:00:00.000000Z")));
                Assert.assertEquals(ts("2026-03-30T00:30:00.000000Z"), closedSegmentStartAbove(anchorPlan, ts("2026-03-28T00:00:00.000000Z")));
                Assert.assertEquals(ts("2026-10-25T01:30:00.000000Z"), closedSegmentStartAbove(anchorPlan, ts("2026-10-24T00:30:00.000000Z")));
            } finally {
                job = Misc.free(job);
            }
        });
    }

    @Test
    public void testDepartureResumeWinsAPriceTieWithTheSegmentRebuild() throws Exception {
        // The departure a steered run commits, with its late row on the hour above the kept batch,
        // under an hourly expression anchor over an HOUR-partitioned base. The resume from the kept
        // batch's root and the rebuild of the late row's segment both price at the late row alone:
        // the kept batch's hour spreads its rows over the whole hour, so the half second above them
        // rounds down to none. The resume wins the tie and truncates its splice at the late row,
        // which keeps a steered departure on the route when its late row crosses a segment start.
        assertMemoryLeak(() -> {
            final long hourTs = ts("2026-01-01T02:00:00.000000Z");
            setCurrentMicros(hourTs - DAY_MICROS);
            // Below DEPARTURE_BATCH_ROWS, so each batch seals a root at its newest row.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 32);
            final String bucket = "timestamp_floor('1h', ts)";
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT " + viewProjection(0)
                    + " FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + bucket + ")");
            final String oracleSql = "SELECT " + oracleProjection(0, bucket) + " FROM base";
            final String orderBy = orderByAll(0);
            runKeys.add(SYNTHETIC_KEY);
            job = newJob();
            try {
                // A row two hours below, so the kept batch's hour is not the table's first
                // partition, whose estimate would span from the table's minimum instead.
                insertSynthetic(hourTs - 2 * HOUR_MICROS);
                insertSyntheticBatch(hourTs - 500_000);
                final long keptHighTs = maxInsertedTs;
                drainWalQueue();
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                Assert.assertEquals("a root seals the kept batch", keptHighTs, instance.getHeadCheckpointMaxTs());
                final long departedLowTs = insertSyntheticBatch(hourTs + 500_000);
                final long departedHighTs = maxInsertedTs;
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("a root seals the departed batch", departedHighTs, instance.getHeadCheckpointMaxTs());

                final LiveViewRefreshStats stats = job.getStatsForTest();
                final long resumesBefore = stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                final long truncatesBefore = stats.get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT);
                commitDeparture(departedLowTs, departedHighTs, hourTs);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the resume wins the tie",
                        resumesBefore + 1,
                        stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                );
                Assert.assertEquals(
                        "and truncates its splice at the late row",
                        truncatesBefore + 1,
                        stats.get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT)
                );
                Assert.assertEquals("where a root seals", hourTs, instance.getHeadCheckpointMaxTs());
                assertView(oracleSql, orderBy, "after the departure");

                restart();
                driveRefreshToQuiescence(job);
                assertRestoredFromTimeline("lv");
                assertView(oracleSql, orderBy, "after the restart");
            } finally {
                job = Misc.free(job);
            }
        });
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
        // Every anchor shape and knob, with some passes carrying an ALTER between late rows, and
        // every run retiring its timeline under a lock that keeps the file: see
        // keptTimelineRetireEpisode and keptSpliceEpisode.
        runBatch(STEER_MIXED);
    }

    @Test
    public void testRunsUnderTheWalApplySuspensionRule() {
        final LogCapture capture = walApplySuspensionRule.getCapture();
        Assert.assertNotNull("the rule must wrap every test of the class", capture);
        Assert.assertTrue("the rule's capture must hold the console log interceptor", capture.isInstalled());
    }

    @Test
    public void testSparseFallback() throws Exception {
        // Exact duplicate rows. A keyed repair over a repeated (timestamp, key) pair cannot
        // upsert it without collapsing it, so it abandons its sparse publication.
        runBatch(STEER_SPARSE_FALLBACK);
    }

    @Test
    public void testSparseFallbackWhereNoDrawnRunPublishesSparsely() throws Exception {
        // A seed whose ten runs published no keyed repair sparsely before every other run was steered
        // onto both publications: runs 0, 3 and 7 drew one key, runs 1, 2, 4 and 8 routed their late
        // rows elsewhere, and the seven keyed closed-segment repairs of runs 5, 6 and 9 all met a
        // duplicate and fell back. The steered runs 0, 2, 4, 6 and 8 publish sparsely once and fall
        // back once each, and the drawn runs repair as they did. The steered runs draw nothing, so the
        // batch ends on the random state it ended on before they were steered.
        final Rnd rnd = TestUtils.generateRandom(LOG, 27607860535165L, 1794587380691L);
        runBatch(STEER_SPARSE_FALLBACK, rnd);
        Assert.assertEquals(5, coverage.get(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT));
        Assert.assertEquals(12, coverage.get(LiveViewRefreshStats.SPARSE_PUBLICATION_FALLBACK_COUNT));
        // Each of the ten steered steps prices one keyed read and repairs once on the keyed
        // closed-segment route, and sparsePublicationEpisode counts them. assertCoverage takes them off
        // the batch's totals, which leaves the drawn runs the 7 repairs and 31 prices the batch made
        // before any run was steered.
        Assert.assertEquals("keyed closed-segment repairs of the steered steps", 10, sparseEpisodeKeyedRepairs);
        Assert.assertEquals("keyed prices of the steered steps", 10, sparseEpisodeKeyedPrices);
        Assert.assertEquals(
                "keyed closed-segment repairs of the drawn runs",
                7,
                coverage.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT) - sparseEpisodeKeyedRepairs
        );
        Assert.assertEquals(
                "keyed prices of the drawn runs",
                31,
                coverage.get(LiveViewRefreshStats.KEYED_SCAN_PRICED_COUNT) - sparseEpisodeKeyedPrices
        );
        Assert.assertEquals(4_959_227_367_749_357_659L, rnd.getSeed0());
        Assert.assertEquals(4_504_132_681_311_710_744L, rnd.getSeed1());
    }

    @Test
    public void testSparsePublicationEpisodeUnderAnHourlyAnchorOverDayPartitions() throws Exception {
        // The tightest price sparsePublicationEpisode meets in the sparse fallback arm: an hourly
        // anchor over DAY partitions, where the estimate counts the late row's segment as 1/24 of the
        // partition holding it, 16 of the 387 or 388 rows a step commits there, against at most 8 for
        // the keyed read of the late row's key. Both steps still take the keyed closed-segment route, one
        // publishing sparsely and one falling back. A row at 23:10 the day below puts the first step's
        // segment on the next day's first hour, so neither step's partition is the table's first or
        // last, whose span the table's extremes would narrow.
        assertMemoryLeak(() -> {
            final long firstTs = ts("2026-01-01T23:10:00.000000Z");
            setCurrentMicros(firstTs - DAY_MICROS);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "true");
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
            final String bucket = "timestamp_floor('1h', ts)";
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL INDEX, i LONG, x DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT " + viewProjection(0)
                    + " FROM base WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION " + bucket + ")");
            runKeys.add(SYMBOLS[0]);
            runKeys.add(SYNTHETIC_KEY);
            runKeys.add(SYNTHETIC_TIE_KEY);
            job = newJob();
            try {
                insertKeyBatch(firstTs, 0, 1);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                sparsePublicationEpisode(new PassShape("SELECT " + oracleProjection(0, bucket) + " FROM base", orderByAll(0), 0, 0, 0, 0));
                final LiveViewRefreshStats stats = job.getStatsForTest();
                Assert.assertEquals(2, stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT));
                Assert.assertEquals(1, stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT));
                Assert.assertEquals(1, stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_FALLBACK_COUNT));
            } finally {
                job = Misc.free(job);
            }
        });
    }

    // The start of the first anchor segment at or above fromTs whose start is its own and whose end
    // is exact, probing hour by hour: a segment beside a zone transition may have neither. A segment
    // with no start of its own reads Long.MIN_VALUE, below every fromTs. No anchor the arms draw
    // goes 40 days without one.
    private static long closedSegmentStartAbove(LiveViewCheckpointAnchorPlan anchorPlan, long fromTs) {
        for (int k = 0; k < 40 * 24; k++) {
            final long start = anchorPlan.getSegmentStart(fromTs + k * HOUR_MICROS);
            if (start >= fromTs && anchorPlan.getSegmentEndExclusive(start) != Numbers.LONG_NULL) {
                return start;
            }
        }
        throw new AssertionError("no closed anchor segment within 40 days above " + fromTs);
    }

    // The first Europe/Berlin transition above minTs that winds the clocks back, isFallBack, or
    // forward, in epoch micros.
    private static long nextBerlinTransition(long minTs, boolean isFallBack) {
        final ZoneRules rules = ZoneId.of("Europe/Berlin").getRules();
        Instant at = Instant.ofEpochSecond(Math.floorDiv(minTs, 1_000_000L));
        while (true) {
            final ZoneOffsetTransition transition = rules.nextTransition(at);
            if (transition.isOverlap() == isFallBack) {
                return transition.toEpochSecond() * 1_000_000L;
            }
            at = transition.getInstant();
        }
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
     * counter that every measured batch of the arm reached, or that an episode the batch runs
     * steers onto, most of them many times over: a later change to a route's gate or to the
     * cost model must not move the fuzz off a route quietly. The rest of the table is logged,
     * not asserted. No arm reaches {@code KEY_DOMAIN_SPLICE_DECLINE_COUNT}, which needs a root
     * sealed under an older state format, and the keyed arm reaches
     * {@code OPEN_SEGMENT_KEYED_CHEAPER_COUNT} in about one batch of ten, too rarely to assert.
     */
    private void assertCoverage(int steer) {
        coverage.log(LOG, steerName(steer));
        LOG.info().$("live view route fuzz checks [arm=").$(steerName(steer))
                .$(", tierReadRecompute=").$(tierReadRecomputeChecks)
                .$(", tierReadTableOnly=").$(tierReadTableOnlyChecks)
                .$(", windowJoin=").$(windowJoinChecks)
                .$(", dstFrontierEpisodes=").$(dstFrontierEpisodes)
                .$(", firedFaults=").$(firedFaults)
                .$(", injectedRefreshFaults=").$(ledger.injected)
                .$(", driftFaults=").$(ledger.drift).I$();
        switch (steer) {
            case STEER_MIXED, STEER_ANCHORS -> {
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_RANGE_REBUILD);
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_SEGMENT_REBUILD);
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR);
                coverage.assertReachedStat(LiveViewRefreshStats.SEGMENT_YIELD_COUNT);
                // Reached through departureEpisode's steered departures, every other run's, each
                // of which asserts it.
                coverage.assertReachedStat(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT);
                assertTimestampGroupsRepaired();
                if (steer == STEER_MIXED) {
                    // Every run of the arm runs one of the two episodes, and each episode asserts
                    // what its own retire under the lock left.
                    LOG.info().$("LV route fuzz kept timelines [retireEpisodes=").$(keptTimelineRetireEpisodes)
                            .$(", lateRowsLocked=").$(keptTimelineLateRowsLocked)
                            .$(", lateRowsReleased=").$(keptTimelineLateRowsReleased)
                            .$(", markerRefusals=").$(keptTimelineMarkerRefusals)
                            .$(", checkFaults=").$(keptTimelineCheckFaults)
                            .$(", removedTimelineCheckFaults=").$(removedTimelineCheckFaults)
                            .$(", spliceEpisodes=").$(keptSpliceEpisodes).I$();
                    Assert.assertTrue("no repair retired its timeline under a lock that kept the file", keptTimelineRetireEpisodes > 0);
                    Assert.assertTrue("no seal was refused a history beside a marker its retire left", keptTimelineMarkerRefusals > 0);
                    Assert.assertTrue("no retire under the lock failed its check of the timeline", keptTimelineCheckFaults > 0);
                    Assert.assertTrue("no retire that removed the timeline failed its check of it", removedTimelineCheckFaults > 0);
                    Assert.assertTrue("no failed head-miss splice left its timeline under a lock", keptSpliceEpisodes > 0);
                }
            }
            case STEER_KEYED_OPEN_SEGMENT -> {
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_KEYED_COLD_OPEN_SEGMENT);
                coverage.assertReached(LiveViewCheckpointRepairRoute.ROUTE_KEYED_RESUME_FROM_ANCHOR);
                coverage.assertReachedStat(LiveViewRefreshStats.TRANSPLANTED_KEY_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.OPEN_SEGMENT_KEYED_PRICED_COUNT);
                Assert.assertTrue("no cycle without rows sealed the root a keyed resume owed", keyedOwedRootSeals > 0);
                LOG.info().$("LV route fuzz second keyed repairs inside the owed window [commitFaults=")
                        .$(secondKeyedRepairCommitFaults)
                        .$(", stops=").$(secondKeyedRepairCommitStops).I$();
                // Both shapes of secondKeyedRepairCommitFaultEpisode, counted rather than drawn over
                // the two episodes the batch's first two runs give it.
                Assert.assertTrue(
                        "no second keyed repair inside the owed window failed its replacement commit in the process",
                        secondKeyedRepairCommitFaults > 0
                );
                Assert.assertTrue(
                        "no second keyed repair inside the owed window stopped the process at its replacement commit",
                        secondKeyedRepairCommitStops > 0
                );
            }
            case STEER_SPARSE_FALLBACK -> {
                // Reached through sparsePublicationEpisode's steps, every other run's, each of which
                // asserts its route and publication.
                coverage.assertReachedStat(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT);
                coverage.assertReachedStat(LiveViewRefreshStats.SPARSE_PUBLICATION_FALLBACK_COUNT);
                // The keyed price and the route are asserted on the drawn runs' share, apart from the
                // steps', which replay one key alone in its commit: a gate only the drawn shapes trip -
                // several keys, ties, a NULL key - must not hide behind the steps.
                Assert.assertTrue(
                        "no drawn run moved KEYED_SCAN_PRICED_COUNT\n" + coverage.table(),
                        coverage.get(LiveViewRefreshStats.KEYED_SCAN_PRICED_COUNT) > sparseEpisodeKeyedPrices
                );
                Assert.assertTrue(
                        "no drawn run published a repair on the keyed closed segment route\n" + coverage.table(),
                        coverage.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT) > sparseEpisodeKeyedRepairs
                );
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
     * Rows on the synthetic key above every other row, in two commits that each seal what they
     * carry on a pass of their own - {@link #DEPARTURE_BATCH_ROWS} rows clear every row cadence
     * the knobs draw - then a pure-delete REPLACE_RANGE over the newer commit's rows, which is
     * how a mat view's refresh removes its newest bucket: the repair's output ends below the
     * root those rows sealed. One run in three puts a late synthetic row below the deleted rows
     * in the same commit, and one in three in the next pass. No generated row sits in the band,
     * so it deletes the synthetic rows alone. Then two passes past any duration cadence the knobs
     * draw that put no row through the window: a synthetic row the filter drops each, or, over a
     * view with no filter, one column added to the base. The departure left the newest timestamp
     * the view has seen above the newest row it holds, and no root may sit there.
     * <p>
     * A departure reaches the splice frontier truncate only when its repair resumes from the kept
     * batch's root and chains to the departed batch's. The run's draws can decline the chain -
     * {@code max.chained.boundaries} 0, or a {@code scan.max.keys} the keys of a rank() or
     * dense_rank() view outgrow - and a repair the main loop parked can take the passes, so the
     * batches seal no root of their own and the REPLACE_RANGE folds into it. A steered departure,
     * {@code isTruncateSteered}, drives the refresh to quiescence after each pass, so each batch
     * seals a root on its own, and runs the REPLACE_RANGE's repair under the configured chain
     * count and key budget: the repair truncates its splice at the new frontier, the late row or
     * the kept batch's newest row, and leaves the head on a root there, and the episode asserts
     * both. It then gives the rest of the run its drawn chain count and key budget back, and
     * asserts that too.
     */
    private void departureEpisode(
            Rnd rnd,
            LiveViewFuzzKnobs knobs,
            PassShape shape,
            boolean isFiltered,
            boolean isTruncateSteered
    ) throws Exception {
        pass(rnd, shape, -1, PIN_NO, false);
        if (isTruncateSteered) {
            driveRefreshToQuiescence(job);
        }
        insertSyntheticBatch(maxInsertedTs + 1 + rnd.nextInt(1_000_000));
        drainWalQueue();
        pass(rnd, shape, -1, PIN_NO, false);
        final long keptHighTs = maxInsertedTs;
        if (isTruncateSteered) {
            driveRefreshToQuiescence(job);
            Assert.assertEquals("a root seals the kept batch", keptHighTs, viewInstance().getHeadCheckpointMaxTs());
        }
        final long departedLowTs = insertSyntheticBatch(keptHighTs + 2 + rnd.nextInt(1_000_000));
        drainWalQueue();
        pass(rnd, shape, -1, PIN_NO, false);
        final long departedHighTs = maxInsertedTs;
        if (isTruncateSteered) {
            driveRefreshToQuiescence(job);
            Assert.assertEquals("a root seals the departed batch", departedHighTs, viewInstance().getHeadCheckpointMaxTs());
        }
        final int lateMode = rnd.nextInt(3);
        final long lateTs = keptHighTs + 1 + rnd.nextLong(departedLowTs - keptHighTs - 1);
        LOG.info().$("LV route fuzz: departure [departedLowTs=").$ts(departedLowTs)
                .$(", departedHighTs=").$ts(departedHighTs)
                .$(", lateMode=").$(lateMode)
                .$(", lateTs=").$ts(lateTs)
                .$(", steered=").$(isTruncateSteered).I$();
        final LiveViewRefreshStats stats = job.getStatsForTest();
        final long truncatesBefore = stats.get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT);
        if (isTruncateSteered) {
            // The configured chain count and key budget admit the repair's one-boundary chain.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, (String) null);
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, (String) null);
        }
        try {
            commitDeparture(departedLowTs, departedHighTs, lateMode == 1 ? lateTs : Numbers.LONG_NULL);
            drainWalQueue();
            pass(rnd, shape, -1, PIN_NO, false);
            if (isTruncateSteered) {
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        "the steered departure's resume truncates its splice at the new frontier",
                        truncatesBefore + 1,
                        stats.get(LiveViewRefreshStats.SPLICE_FRONTIER_TRUNCATE_COUNT)
                );
                Assert.assertEquals(
                        "and leaves the head on a root at the new frontier",
                        lateMode == 1 ? lateTs : keptHighTs,
                        viewInstance().getHeadCheckpointMaxTs()
                );
            }
        } finally {
            if (isTruncateSteered) {
                knobs.apply(AbstractLiveViewTest::setProperty);
            }
        }
        if (isTruncateSteered) {
            // The rest of the run - lateMode 2's late row, the passes past the duration cadence and
            // a Berlin run's dstFrontierEpisode - repairs under the run's own draws again. A key
            // budget drawn at DEFAULT reads the server default, 100,000.
            Assert.assertEquals(
                    "the steered departure gives the run its drawn chain count back",
                    knobs.maxChainedBoundaries,
                    engine.getConfiguration().getLiveViewCheckpointRepairMaxChainedBoundaries()
            );
            Assert.assertEquals(
                    "and its drawn key budget",
                    knobs.scanMaxKeys == LiveViewFuzzKnobs.DEFAULT ? 100_000L : knobs.scanMaxKeys,
                    engine.getConfiguration().getLiveViewCheckpointRepairScanMaxKeys()
            );
        }
        if (lateMode == 2) {
            insertSynthetic(lateTs);
            drainWalQueue();
            pass(rnd, shape, -1, PIN_NO, false);
        }
        for (int k = 0, n = isFiltered ? 2 : 1; k < n; k++) {
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            if (isFiltered) {
                long t = maxInsertedTs + 1;
                while (!syntheticTs.add(t)) {
                    t++;
                }
                maxInsertedTs = t;
                execute("INSERT INTO base (ts, sym, i, x) VALUES (" + t + "::timestamp, '" + SYNTHETIC_KEY + "', -1, 1.0)");
            } else {
                execute("ALTER TABLE base ADD COLUMN departure_stretch INT");
                ledger.driftCredits++;
            }
            drainWalQueue();
            pass(rnd, shape, -1, PIN_NO, false);
        }
        driveRefreshToQuiescence(job);
        assertNoCheckpointRootAboveTheNewestRow(viewInstance());
    }

    // The REPLACE_RANGE that departs the synthetic rows from departedLowTs to departedHighTs: a
    // pure delete, or, for a lateTs other than LONG_NULL, one from lateTs up that holds a synthetic
    // row there.
    private void commitDeparture(long departedLowTs, long departedHighTs, long lateTs) {
        try (WalWriter walWriter = engine.getWalWriter(engine.verifyTableName("base"))) {
            if (lateTs != Numbers.LONG_NULL) {
                Assert.assertTrue(syntheticTs.add(lateTs));
                final TableWriter.Row row = walWriter.newRow(lateTs);
                row.putSym(1, SYNTHETIC_KEY);
                row.putLong(2, 1 + (lateTs & 511));
                row.putDouble(3, 1.0);
                row.append();
            }
            walWriter.commitWithParams(
                    lateTs != Numbers.LONG_NULL ? lateTs : departedLowTs,
                    departedHighTs + 1,
                    WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE
            );
        }
    }

    /**
     * A correction that empties the view's newest rows next to a Europe/Berlin transition, under
     * the run's {@code ANCHOR DAILY '02:30' 'Europe/Berlin'}, in one of three shapes a salted
     * draw picks, placed on the first transition at least three days above every row. One commit
     * puts the kept rows and the departed rows on salted keys, and a REPLACE_RANGE then deletes
     * the departed rows, so a localized end-of-table head-miss rebuild emits no row and derives
     * the state at the new frontier F instead of retiring the timeline:
     * <ul>
     *     <li>{@link #DST_FRONTIER_IN_OPEN_RUN}: kept rows at 00:05Z to 00:25Z, on the previous
     *     day's segment, and at 00:35Z to 00:55Z, in the half hour before the fall-back at 01:00Z,
     *     whose anchor value no other instant carries, so its segment has no start of its own;
     *     the departed rows at 01:35Z to 01:50Z. F's floor is the view's start: the rebuild
     *     restores the newest root below F when it counts every row of the view at or below it,
     *     and folds every base row up to F otherwise;</li>
     *     <li>{@link #DST_FRONTIER_IN_REPEATED_HALF_HOUR}: also kept rows at 01:05Z to 01:25Z, in
     *     the half hour after the fall-back, which reads 02:05 to 02:25 again and carries the
     *     previous day's anchor value. F's floor is that day's segment start, above every root
     *     of the run, so the rebuild folds the base rows from there;</li>
     *     <li>{@link #DST_FRONTIER_ON_SPRING_FORWARD_DAY}: kept rows on the day below, then from
     *     01:00Z, when 02:30 is skipped, to 12:00Z, whose segment starts half an hour below its
     *     own anchor value, so it has no start of its own; the departed rows on the next day's
     *     segment. F's floor is the view's start, as in the first shape.</li>
     * </ul>
     * Pins the cadence, so no root seals the episode's rows, and the routes that would take the
     * correction elsewhere, for the episode. The run's oracle, then a restart that restores from
     * the root at F, then rows committed in order at exactly F and above it - in the first shape,
     * one of a key whose newest kept row is on the previous day's segment, in the repeated half
     * hour, where its run continues - then the oracle and another restart that restores.
     */
    private void dstFrontierEpisode(Rnd rnd, LiveViewFuzzKnobs knobs, PassShape shape, int symCount) throws Exception {
        final Rnd episodeRnd = new Rnd(rnd.getSeed0() ^ DST_FRONTIER_SALT0, rnd.getSeed1() ^ DST_FRONTIER_SALT1);
        final int dstShape = episodeRnd.nextInt(DST_FRONTIER_SHAPES);
        driveRefreshToQuiescence(job);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, (String) null);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        try {
            final LiveViewInstance instance = viewInstance();
            final long transitionTs = nextBerlinTransition(
                    Math.max(maxInsertedTs, instance.getLatestSeenTs()) + 3 * DAY_MICROS,
                    dstShape != DST_FRONTIER_ON_SPRING_FORWARD_DAY
            );
            // The kept rows' offsets from the transition, in minutes, ascending, then the departed
            // rows'.
            final int[] keptMinutes = switch (dstShape) {
                case DST_FRONTIER_IN_OPEN_RUN -> new int[]{-55, -50, -45, -40, -35, -25, -20, -15, -10, -5};
                case DST_FRONTIER_IN_REPEATED_HALF_HOUR -> new int[]{-55, -50, -45, -40, -35, -25, -20, -15, -10, -5, 5, 10, 15, 20, 25};
                default -> new int[]{-12 * 60, -30, 0, 5, 10, 15, 20, 11 * 60};
            };
            final int[] departedMinutes = dstShape == DST_FRONTIER_ON_SPRING_FORWARD_DAY
                    ? new int[]{23 * 60 + 35, 23 * 60 + 40, 23 * 60 + 45, 23 * 60 + 50}
                    : new int[]{35, 40, 45, 50};
            // A key whose newest kept row is on the previous day's segment, 00:20Z, and which has
            // no row in the half hour before the fall-back, in the first shape.
            final int continuingKeyMinute = -40;
            final ObjList<String> keptKeys = new ObjList<>();
            syntheticSink.clear();
            syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
            for (int k = 0; k < keptMinutes.length; k++) {
                final String key = dstShape == DST_FRONTIER_IN_OPEN_RUN && keptMinutes[k] == continuingKeyMinute
                        ? SYNTHETIC_TIE_KEY
                        : SYMBOLS[episodeRnd.nextInt(symCount)];
                keptKeys.add(key);
                appendEpisodeRow(k > 0, transitionTs + keptMinutes[k] * MINUTE_MICROS, key);
            }
            for (int departedMinute : departedMinutes) {
                appendEpisodeRow(true, transitionTs + departedMinute * MINUTE_MICROS, SYMBOLS[episodeRnd.nextInt(symCount)]);
            }
            final long frontierTs = transitionTs + keptMinutes[keptMinutes.length - 1] * MINUTE_MICROS;
            final String frontierKey = keptKeys.getQuick(keptKeys.size() - 1);
            final long departedLowTs = transitionTs + departedMinutes[0] * MINUTE_MICROS;
            final long departedHighTs = transitionTs + departedMinutes[departedMinutes.length - 1] * MINUTE_MICROS;
            maxInsertedTs = Math.max(maxInsertedTs, departedHighTs);
            execute(syntheticSink);
            drainWalQueue();
            driveRefreshToQuiescence(job);

            // The rule's route: the frontier's floor in the repeated half hour is the previous
            // day's segment start, above every root of the run, so the rebuild folds from there.
            // In the other two shapes it is the view's start, and the newest root at or below the
            // frontier serves when it counts every row of the view at or below it.
            boolean isRestoreExpected = false;
            long rootTs = Numbers.LONG_NULL;
            if (dstShape != DST_FRONTIER_IN_REPEATED_HALF_HOUR) {
                final LongList ladder = snapshotCheckpointLadder(instance);
                for (int k = ladder.size() - 2; k >= 0; k -= 2) {
                    if (ladder.getQuick(k) <= frontierTs) {
                        rootTs = ladder.getQuick(k);
                        isRestoreExpected = ladder.getQuick(k + 1) == queryLong("SELECT count() FROM lv WHERE ts <= " + rootTs);
                        break;
                    }
                }
            }
            LOG.info().$("LV route fuzz: DST frontier [shape=").$(dstShape)
                    .$(", transition=").$ts(transitionTs)
                    .$(", frontier=").$ts(frontierTs)
                    .$(", departedLowTs=").$ts(departedLowTs)
                    .$(", rootTs=").$ts(rootTs)
                    .$(", restore=").$(isRestoreExpected).I$();
            final LiveViewRefreshStats stats = job.getStatsForTest();
            final long headMissRepairsBefore = countHeadMissRepairs(job);
            final long sealsBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT);
            final long restoresBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT);
            final long warmUpsBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT);
            final long retiresBefore = stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT);
            replaceBaseRange(departedLowTs, departedHighTs + 1, Double.NaN);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
            Assert.assertEquals("and seals the frontier", sealsBefore + 1, stats.get(LiveViewRefreshStats.HEAD_MISS_ZERO_ROW_FRONTIER_SEAL_COUNT));
            Assert.assertEquals(
                    "a restore derives the frontier's state",
                    restoresBefore + (isRestoreExpected ? 1 : 0),
                    stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_RESTORE_COUNT)
            );
            Assert.assertEquals(
                    "a warm-up derives the frontier's state",
                    warmUpsBefore + (isRestoreExpected ? 0 : 1),
                    stats.get(LiveViewRefreshStats.HEAD_MISS_FRONTIER_STATE_WARM_UP_COUNT)
            );
            Assert.assertEquals("nothing retires", retiresBefore, stats.get(LiveViewRefreshStats.HEAD_MISS_SPLICE_ZERO_ROW_RETIRE_COUNT));
            final LongList ladder = snapshotCheckpointLadder(instance);
            Assert.assertEquals("the newest root sits at the new frontier", frontierTs, ladder.getQuick(ladder.size() - 2));
            Assert.assertEquals("and counts every row of the view", queryLong("SELECT count() FROM lv"), ladder.getQuick(ladder.size() - 1));
            assertView(shape.oracleSql, shape.orderBy, "after the DST frontier departure");
            restart();
            driveRefreshToQuiescence(job);
            assertRestoredFromTimeline("lv");
            assertView(shape.oracleSql, shape.orderBy, "after the restart from the DST frontier");

            // In order after the restore: a row at exactly the frontier, which folds over the
            // root's state, and a row above it.
            syntheticSink.clear();
            syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
            appendEpisodeRow(false, frontierTs, frontierKey);
            final long aboveTs = switch (dstShape) {
                // The continuing key's row in the repeated half hour, which carries the anchor
                // value of its 00:20Z row again.
                case DST_FRONTIER_IN_OPEN_RUN -> transitionTs + 10 * MINUTE_MICROS;
                case DST_FRONTIER_IN_REPEATED_HALF_HOUR -> transitionTs + 28 * MINUTE_MICROS;
                default -> transitionTs + 12 * 60 * MINUTE_MICROS;
            };
            appendEpisodeRow(true, aboveTs, dstShape == DST_FRONTIER_IN_OPEN_RUN ? SYNTHETIC_TIE_KEY : SYMBOLS[episodeRnd.nextInt(symCount)]);
            execute(syntheticSink);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            assertView(shape.oracleSql, shape.orderBy, "after the rows at and above the DST frontier");
            restart();
            driveRefreshToQuiescence(job);
            assertRestoredFromTimeline("lv");
            assertView(shape.oracleSql, shape.orderBy, "after the restart past the DST frontier");
            dstFrontierEpisodes++;
        } finally {
            knobs.apply(AbstractLiveViewTest::setProperty);
        }
    }

    // One row of the DST frontier or sparse publication episode on key, a comma ahead of it when
    // isNext, appended to syntheticSink's INSERT. Its i passes the filtered view's predicate.
    private void appendEpisodeRow(boolean isNext, long ts, String key) {
        if (isNext) {
            syntheticSink.put(", ");
        }
        syntheticSink.put('(').put(ts).put("::timestamp, '").put(key).put("', ").put(1 + (ts & 511)).put(", 1.0)");
    }

    /**
     * Two late rows on {@link #SYNTHETIC_KEY}, each into a closed anchor segment of its own above
     * every other row, which the keyed closed-segment route repairs under the run's anchor,
     * variant and draws. The first segment's rows of the key are distinct, so its repair
     * publishes sparsely; the second's carry an exact copy of the row above the late row - inside
     * the repair's output, which starts at the late row - so its repair abandons the sparse
     * attempt and publishes its whole range. A run of the arm reaches neither unless its draws
     * line up: a late row in a closed segment, a keyed read priced below the segment, and a
     * duplicate in the output or none. The arm forces every switch the route reads, so the
     * episode keeps the run's knobs as drawn; it draws nothing, so the runs after it draw as they
     * did before it existed. It counts the keyed prices and repairs of its steps, which
     * {@link #assertCoverage} takes off the batch's totals.
     */
    private void sparsePublicationEpisode(PassShape shape) throws Exception {
        driveRefreshToQuiescence(job);
        final LiveViewRefreshStats stats = job.getStatsForTest();
        final long pricesBefore = stats.get(LiveViewRefreshStats.KEYED_SCAN_PRICED_COUNT);
        final long repairsBefore = stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT);
        closedSegmentPublicationStep(shape, false);
        closedSegmentPublicationStep(shape, true);
        sparseEpisodeKeyedPrices += stats.get(LiveViewRefreshStats.KEYED_SCAN_PRICED_COUNT) - pricesBefore;
        sparseEpisodeKeyedRepairs += stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT) - repairsBefore;
    }

    /**
     * One step of {@link #sparsePublicationEpisode}, in a fresh frame above every row: in the
     * first closed segment above them, one commit of {@link #SPARSE_EPISODE_FILL_ROWS} rows of
     * {@link #SYNTHETIC_TIE_KEY} from the segment's middle up, two microseconds apart, and two
     * rows of {@link #SYNTHETIC_KEY} among them, the upper one twice when
     * {@code isDuplicatePair}; then one row in the middle of the first closed segment at least a
     * day above it, which closes it and puts the head in a later partition; then the late row of
     * SYNTHETIC_KEY between the two, each commit refreshed to quiescence. Asserts that the late
     * row takes the keyed closed-segment route and publishes sparsely, or abandons its sparse
     * publication over the duplicate pair, then the oracle.
     */
    private void closedSegmentPublicationStep(PassShape shape, boolean isDuplicatePair) throws Exception {
        final LiveViewInstance instance = viewInstance();
        final LiveViewCheckpointAnchorPlan anchorPlan = instance.getAnchorWindow().getCheckpointAnchorPlan();
        Assert.assertNotNull("the view's anchor must plan segments", anchorPlan);
        final long segmentStart = closedSegmentStartAbove(anchorPlan, Math.max(maxInsertedTs, instance.getLatestSeenTs()) + 1);
        final long segmentEnd = anchorPlan.getSegmentEndExclusive(segmentStart);
        final long midTs = segmentStart + (segmentEnd - segmentStart) / 2;
        final long keyLowTs = midTs + 2L * (SPARSE_EPISODE_FILL_ROWS / 4) + 1;
        final long lateTs = midTs + 2L * (SPARSE_EPISODE_FILL_ROWS / 2) + 1;
        final long keyHighTs = midTs + 2L * (3 * SPARSE_EPISODE_FILL_ROWS / 4) + 1;
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        for (int k = 0; k < SPARSE_EPISODE_FILL_ROWS; k++) {
            appendEpisodeRow(k > 0, midTs + 2L * k, SYNTHETIC_TIE_KEY);
        }
        appendEpisodeRow(true, keyLowTs, SYNTHETIC_KEY);
        appendEpisodeRow(true, keyHighTs, SYNTHETIC_KEY);
        if (isDuplicatePair) {
            // Above the late row, so the repair's output, which starts at the late row, carries the pair.
            appendEpisodeRow(true, keyHighTs, SYNTHETIC_KEY);
        }
        execute(syntheticSink);
        syntheticTs.add(keyLowTs);
        syntheticTs.add(keyHighTs);
        maxInsertedTs = Math.max(maxInsertedTs, midTs + 2L * (SPARSE_EPISODE_FILL_ROWS - 1));
        drainWalQueue();
        driveRefreshToQuiescence(job);

        final long headSegmentStart = closedSegmentStartAbove(anchorPlan, segmentEnd + DAY_MICROS);
        final long headTs = headSegmentStart + (anchorPlan.getSegmentEndExclusive(headSegmentStart) - headSegmentStart) / 2;
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        appendEpisodeRow(false, headTs, SYNTHETIC_TIE_KEY);
        execute(syntheticSink);
        maxInsertedTs = Math.max(maxInsertedTs, headTs);
        drainWalQueue();
        driveRefreshToQuiescence(job);

        final LiveViewRefreshStats stats = job.getStatsForTest();
        final long repairsBefore = stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT);
        final long sparseBefore = stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT);
        final long fallbacksBefore = stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_FALLBACK_COUNT);
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        appendEpisodeRow(false, lateTs, SYNTHETIC_KEY);
        execute(syntheticSink);
        syntheticTs.add(lateTs);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals(
                "the late row takes the keyed closed-segment route",
                repairsBefore + 1,
                stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_CLOSED_SEGMENT)
        );
        if (isDuplicatePair) {
            Assert.assertEquals(
                    "and abandons its sparse publication over the duplicate pair",
                    fallbacksBefore + 1,
                    stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_FALLBACK_COUNT)
            );
            Assert.assertEquals("and publishes nothing sparsely", sparseBefore, stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT));
            assertView(shape.oracleSql, shape.orderBy, "after the closed-segment fallback");
        } else {
            Assert.assertEquals("and publishes sparsely", sparseBefore + 1, stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_COUNT));
            Assert.assertEquals("and falls back nowhere", fallbacksBefore, stats.get(LiveViewRefreshStats.SPARSE_PUBLICATION_FALLBACK_COUNT));
            assertView(shape.oracleSql, shape.orderBy, "after the closed-segment sparse publication");
        }
    }

    private long queryLong(String sql) throws SqlException {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    /**
     * A keyed resume whose closing seal fails over the root it owes, then cycles without rows past
     * the cadence, under a row cadence no pass reaches, the configured duration cadence and the
     * configured chain budget, which lets the resume splice. Batches A and B, a row of every run
     * key each, above every row, commit past the duration cadence and seal a root each, and batch
     * C, a row of every run key but the first, commits above them inside the cadence. A late row
     * of the first key between A and B takes the keyed resume from A's root: its splice
     * re-versions B's root and keeps it as the newest, below C, and its closing seal fails on the
     * writer stage the seal hook sets. The keyed replay ran beside the primary, and the freeze of
     * B's root, which no replayed row follows, left the primary's batch minimum empty, so the head
     * parked on B's root no longer sits at the newest row the view's table holds. A REPLACE_RANGE
     * above every row, which deletes nothing and puts no row through the window, then commits past
     * the cadence: its cycle reads the table, finds C above the head, and seals the root the
     * repair owed. The next one republishes that root at its own commit, and a restart restores
     * from it. A variant the keyed routes decline takes the row another way, and the episode ends
     * there.
     */
    private void keyedOwedRootEpisode(LiveViewFuzzKnobs knobs, String oracleSql, String orderBy, int symCount) throws Exception {
        driveRefreshToQuiescence(job);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, (String) null);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
        // A resume that declines the chain truncates instead of splicing, and owes no root.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, (String) null);
        try {
            final LiveViewInstance instance = viewInstance();
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            final long batchAHighTs = insertKeyBatch(maxInsertedTs + 1, 0, symCount);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("a root seals batch A", batchAHighTs, instance.getHeadCheckpointMaxTs());
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            final long batchBHighTs = insertKeyBatch(batchAHighTs + 2, 0, symCount);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("a root seals batch B", batchBHighTs, instance.getHeadCheckpointMaxTs());
            final long batchCHighTs = insertKeyBatch(batchBHighTs + 1, 1, symCount);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            Assert.assertEquals("no root seals batch C", batchBHighTs, instance.getHeadCheckpointMaxTs());

            final long keyedResumesBefore = instance.getO3OpenSegmentKeyedResumeCount();
            final long sealFailuresBefore = instance.getCheckpointSealFailures();
            final long markerBaseGeneration = publishedCheckpointGeneration(instance);
            hooks(job).setSimulateResumeHeadSealStart(
                    () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
            );
            try {
                execute("INSERT INTO base (ts, sym, i, x) VALUES (" + (batchAHighTs + 1) + "::timestamp, '" + SYMBOLS[0] + "', 1, 1.0)");
                drainWalQueue();
                driveRefreshToQuiescence(job);
            } finally {
                hooks(job).setSimulateResumeHeadSealStart(null);
                job.setCheckpointTimelineTestFailureStage(0);
            }
            if (instance.getO3OpenSegmentKeyedResumeCount() == keyedResumesBefore) {
                LOG.info().$("LV route fuzz: keyed owed root episode ends, no keyed resume took the late row").$();
                return;
            }
            Assert.assertEquals("the keyed resume's closing seal fails", sealFailuresBefore + 1, instance.getCheckpointSealFailures());
            Assert.assertEquals("and leaves the head on B's root", batchBHighTs, instance.getHeadCheckpointMaxTs());
            Assert.assertEquals("with the batch minimum empty", Long.MAX_VALUE, instance.getMinSeenTsSinceCheckpoint());
            assertRepairMarkerLive(instance, markerBaseGeneration);

            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            commitRowlessReplaceRange();
            Assert.assertEquals("the cycle seals the frontier", batchCHighTs, instance.getHeadCheckpointMaxTs());
            Assert.assertEquals("the splice and the owed root", markerBaseGeneration + 2, publishedCheckpointGeneration(instance));
            final LongList ladder = snapshotCheckpointLadder(instance);
            Assert.assertEquals(batchCHighTs, ladder.getQuick(ladder.size() - 2));
            keyedOwedRootSeals++;

            final long generation = publishedCheckpointGeneration(instance);
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            commitRowlessReplaceRange();
            Assert.assertEquals("the next cycle republishes the root", generation + 1, publishedCheckpointGeneration(instance));
            Assert.assertEquals(batchCHighTs, instance.getHeadCheckpointMaxTs());
            Assert.assertEquals("and seals none", ladder.toString(), snapshotCheckpointLadder(instance).toString());

            restart();
            driveRefreshToQuiescence(job);
            assertRestoredFromTimeline("lv");
            assertView(oracleSql, orderBy, "after the restore from the keyed owed root");
            secondKeyedRepairCommitFaultEpisode(oracleSql, orderBy, symCount);
        } finally {
            knobs.apply(AbstractLiveViewTest::setProperty);
        }
    }

    /**
     * Run by keyedOwedRootEpisode once its keyed resume took the late row, under its settings.
     * Batches A and B, a row of every run key each, above every row, commit past the duration
     * cadence and seal a root each, with room for two rows between them, and batch C, a row of
     * every run key but the first, commits above them inside the cadence. A late row of the first
     * key just above A's root takes the keyed resume, whose closing seal fails on the writer stage
     * the seal hook sets, so its marker stays live over B's re-versioned root, below the frontier.
     * A second late row of the first key above it takes the keyed resume again: its marker
     * replaces the live one, records no seqTxn, and its replacement commit fails from the
     * role-switch mint seam. Every other such episode of the batch, from its second, counted rather
     * than drawn, stops the process at that commit, and the restart, which finds the live marker,
     * rebuilds from the applied base. The others fail the commit in process, where the keyed replay
     * ran beside the primary runtime, so no recovery runs and the retry repairs the row under its
     * own live marker. The run's base lost nothing, so the view matches the oracle either way.
     */
    private void secondKeyedRepairCommitFaultEpisode(String oracleSql, String orderBy, int symCount) throws Exception {
        final LiveViewInstance instance = viewInstance();
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        final long batchAHighTs = insertKeyBatch(Math.max(maxInsertedTs, instance.getLatestSeenTs()) + 1, 0, symCount);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("a root seals batch A", batchAHighTs, instance.getHeadCheckpointMaxTs());
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        final long batchBHighTs = insertKeyBatch(batchAHighTs + 3, 0, symCount);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("a root seals batch B", batchBHighTs, instance.getHeadCheckpointMaxTs());
        insertKeyBatch(batchBHighTs + 1, 1, symCount);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("no root seals batch C", batchBHighTs, instance.getHeadCheckpointMaxTs());

        final long keyedResumesBefore = instance.getO3OpenSegmentKeyedResumeCount();
        final long markerBaseGeneration = publishedCheckpointGeneration(instance);
        hooks(job).setSimulateResumeHeadSealStart(
                () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
        );
        try {
            execute("INSERT INTO base (ts, sym, i, x) VALUES (" + (batchAHighTs + 1) + "::timestamp, '" + SYMBOLS[0] + "', 1, 1.0)");
            drainWalQueue();
            driveRefreshToQuiescence(job);
        } finally {
            hooks(job).setSimulateResumeHeadSealStart(null);
            job.setCheckpointTimelineTestFailureStage(0);
        }
        if (instance.getO3OpenSegmentKeyedResumeCount() == keyedResumesBefore) {
            LOG.info().$("LV route fuzz: second keyed repair episode ends, no keyed resume took the late row").$();
            return;
        }
        assertRepairMarkerLive(instance, markerBaseGeneration);

        final boolean isStop = secondKeyedRepairCommitFaultEpisodes++ % 2 == 1;
        final long lvSeqTxnBefore = engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken());
        final long runtimeRestoresBefore = instance.getCheckpointRuntimeRestores();
        final LiveViewReplacementCommitWitness witness = new LiveViewReplacementCommitWitness(
                engine,
                instance,
                markerBaseGeneration + 1,
                isStop ? LiveViewReplacementCommitWitness.FAIL_CANCELLATION : LiveViewReplacementCommitWitness.FAIL_CRITICAL
        );
        execute("INSERT INTO base (ts, sym, i, x) VALUES (" + (batchAHighTs + 2) + "::timestamp, '" + SYMBOLS[0] + "', 2, 1.0)");
        drainWalQueue();
        // The refresh fault the witness throws is the one this episode injects.
        ledger.isInjectionOpen = true;
        CairoEngine.setRoleSwitchMintObserver(witness::onMint);
        try {
            if (isStop) {
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !witness.hasFailed(); pass++) {
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    drainWalQueue();
                    job.processNotificationsForTest();
                }
            } else {
                driveRefreshToQuiescence(job);
            }
        } finally {
            CairoEngine.setRoleSwitchMintObserver(null);
            ledger.isInjectionOpen = false;
        }
        Assert.assertTrue("the second keyed resume must reach its replacement commit", witness.hasFailed());
        Assert.assertEquals("over the live marker, its marker records no seqTxn", Numbers.LONG_NULL, witness.getRecordedLvSeqTxn(0));
        if (isStop) {
            Assert.assertEquals("the replacement never commits", lvSeqTxnBefore, engine.getTableSequencerAPI().lastTxn(instance.getLiveViewToken()));
            restart();
            driveRefreshToQuiescence(job);
            assertRebuiltFromAppliedBase("lv");
            secondKeyedRepairCommitStops++;
            assertView(oracleSql, orderBy, "after the restart behind the second keyed repair's marker");
        } else {
            Assert.assertEquals("the faulted commit and its retry", 2, witness.size());
            Assert.assertEquals("over its own live marker, the retry's records none either", Numbers.LONG_NULL, witness.getRecordedLvSeqTxn(1));
            Assert.assertEquals("the keyed replay keeps the primary runtime", runtimeRestoresBefore, instance.getCheckpointRuntimeRestores());
            Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
            secondKeyedRepairCommitFaults++;
            assertView(oracleSql, orderBy, "after the second keyed repair's commit fault");
        }
    }

    // The repair marker on disk names markerBaseGeneration under the splice's generation one past
    // it, which a restore reads as live.
    private void assertRepairMarkerLive(LiveViewInstance instance, long markerBaseGeneration) {
        try (
                Path dir = new Path().of(configuration.getDbRoot())
                        .concat(instance.getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
        ) {
            Assert.assertTrue("the repair marker must stay on disk", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            Assert.assertEquals(markerBaseGeneration, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
        }
        Assert.assertEquals("no root has landed past the splice", markerBaseGeneration + 1, publishedCheckpointGeneration(instance));
    }

    /**
     * A late row below every row of the run under a lock on {@code _timeline}, the file a retire
     * unlinks, whose repair retires the timeline ahead of its commit, and the lock keeps the file.
     * An anchored view localizes a late row to its segment, so the episode first takes that away:
     * past the duration cadence, a root seals a row above every row, and a REPLACE_RANGE then
     * deletes a row above it, which leaves the newest timestamp the view has seen above the newest
     * row its table holds and so denies the anchor's bound (DENIAL_UNFLUSHED_OUTPUT, the term for
     * output the runtime holds that its table does not). No root sits below the late row, so the
     * head-miss rebuild re-derives the whole view and retires the timeline ahead of its commit,
     * writing no marker of its own: the retire writes one naming no generation beside the file it
     * kept. From then on the view refuses the kept generation: nothing publishes over it, no head
     * stands on it and no resume anchors in it. A drawn 0 to 2 late rows inside the run's rows
     * follow, the lock going ahead of a drawn one of them or after the last, and the view matches
     * the oracle after each; the first late row past the lock retires the kept file and its
     * marker. A forward row past the duration cadence then seals a root, which a fresh history
     * takes when no seal landed since the retire. Draws from a sequence salted apart from the
     * run's, so the run draws everything else as it did before the episode existed.
     * <p>
     * The batch's first episode, and one in two after it until a seal has been refused, also locks
     * the repair marker once the lock on {@code _timeline} goes, drawn from a sequence salted apart
     * from the episode's. The first late row past the lock on {@code _timeline} then retires the
     * kept file, cannot remove the marker, and its closing seal opens no runtime history beside the
     * marker, which every restart would read against it. With no late row past that lock, a forward
     * row's seal does the same. The marker's lock goes ahead of the forward row that seals the root.
     * <p>
     * The batch's first episode also fails the check of {@code _timeline} that follows the retire's
     * unlink under the lock, which the retire answers as it answers the file the lock kept. Its next
     * episode takes no lock and fails that check alone, after an unlink that removed the file: see
     * {@link #removeTheTimelineThatItsCheckCannotSee}. After both, each episode draws among the two
     * and the lock alone, from a sequence salted apart from the episode's and the marker's.
     */
    private void keptTimelineRetireEpisode(Rnd rnd, PassShape shape) throws Exception {
        final Rnd episodeRnd = new Rnd(rnd.getSeed0() ^ KEPT_TIMELINE_SALT0, rnd.getSeed1() ^ KEPT_TIMELINE_SALT1);
        final Rnd markerRnd = new Rnd(rnd.getSeed0() ^ KEPT_TIMELINE_MARKER_SALT0, rnd.getSeed1() ^ KEPT_TIMELINE_MARKER_SALT1);
        final Rnd checkRnd = new Rnd(rnd.getSeed0() ^ KEPT_TIMELINE_CHECK_SALT0, rnd.getSeed1() ^ KEPT_TIMELINE_CHECK_SALT1);
        final int retireFault = keptTimelineCheckFaults == 0
                ? KEPT_TIMELINE_FAULT_LOCK_AND_CHECK
                : removedTimelineCheckFaults == 0 ? KEPT_TIMELINE_FAULT_CHECK : checkRnd.nextInt(3);
        final boolean isMarkerLockDrawn = retireFault != KEPT_TIMELINE_FAULT_CHECK
                && (keptTimelineMarkerRefusals == 0 || markerRnd.nextInt(2) == 0);
        final LiveViewInstance instance = viewInstance();
        driveRefreshToQuiescence(job);
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        insertSynthetic(Math.max(maxInsertedTs, instance.getLatestSeenTs()) + 1);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        final long rootTs = maxInsertedTs;
        Assert.assertEquals("a root seals the row above every row", rootTs, instance.getHeadCheckpointMaxTs());
        final long seenTs = rootTs + 1;
        insertSynthetic(seenTs);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        commitDeparture(seenTs, seenTs, Numbers.LONG_NULL);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("the view has seen the departed row", seenTs, instance.getLatestSeenTs());
        Assert.assertEquals("which its table does not hold", rootTs, queryLong("SELECT max(ts)::long FROM lv"));

        final long keptGeneration = publishedCheckpointGeneration(instance);
        final long minTs = queryLong("SELECT min(ts)::long FROM base");
        final LiveViewRefreshStats stats = job.getStatsForTest();
        final long headMissRepairsBefore = countHeadMissRepairs(job);
        final long resetsBefore = instance.getCheckpointTimelineResets();
        final int lateRows = episodeRnd.nextInt(3);
        final int releasedAhead = episodeRnd.nextInt(lateRows + 1);
        LOG.info().$("LV route fuzz: kept timeline retire [rootTs=").$ts(rootTs)
                .$(", lateTs=").$ts(minTs - 1)
                .$(", keptGeneration=").$(keptGeneration)
                .$(", lateRows=").$(lateRows)
                .$(", releasedAhead=").$(releasedAhead)
                .$(", retireFault=").$(retireFault).I$();
        long sealFailuresAtTheMarkerLock = Numbers.LONG_NULL;
        if (retireFault == KEPT_TIMELINE_FAULT_CHECK) {
            removeTheTimelineThatItsCheckCannotSee(instance, shape, minTs, headMissRepairsBefore, resetsBefore);
        } else {
            timelineLock.lock();
            if (retireFault == KEPT_TIMELINE_FAULT_LOCK_AND_CHECK) {
                timelineLock.failTheCheckAfterTheNextTimelineUnlink();
            }
            try {
                insertSynthetic(minTs - 1);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals("and retires the timeline ahead of its commit", resetsBefore + 1, instance.getCheckpointTimelineResets());
                Assert.assertTrue("which the lock keeps", timelineLock.getRefusedTimelineUnlinks() > 0);
                if (retireFault == KEPT_TIMELINE_FAULT_LOCK_AND_CHECK) {
                    Assert.assertEquals("and whose check of _timeline after the unlink fails", 1, timelineLock.disarmTheCheck());
                    keptTimelineCheckFaults++;
                }
                assertTimelineKept(instance, keptGeneration, "the late row below every row");
                try (Path dir = checkpointsDir(instance)) {
                    Assert.assertEquals(
                            "the retire wrote the marker beside the file, naming no generation",
                            Numbers.LONG_NULL,
                            LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir)
                    );
                }
                assertView(shape.oracleSql, shape.orderBy, "after the late row below every row");
                for (int k = 0; k < lateRows; k++) {
                    if (k == releasedAhead) {
                        timelineLock.unlock();
                        if (isMarkerLockDrawn) {
                            sealFailuresAtTheMarkerLock = instance.getCheckpointSealFailures();
                            timelineLock.lockMarker();
                        }
                    }
                    final long resumesBefore = countResumes(stats);
                    insertSynthetic(minTs + episodeRnd.nextLong(rootTs - minTs));
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals("no resume anchors in the kept generation", resumesBefore, countResumes(stats));
                    if (k < releasedAhead) {
                        assertTimelineKept(instance, keptGeneration, "late row " + k + " under the lock");
                        keptTimelineLateRowsLocked++;
                    } else {
                        Assert.assertFalse("the late row's retire removed the kept file", instance.isCheckpointTimelineRetirePending());
                        if (isMarkerLockDrawn) {
                            assertNoHistoryBesideTheMarker(instance, "late row " + k + " past the lock on _timeline");
                        }
                        keptTimelineLateRowsReleased++;
                    }
                    assertView(shape.oracleSql, shape.orderBy, "after late row " + k + " over the kept timeline");
                }
            } finally {
                timelineLock.unlock();
            }
        }
        try {
            if (isMarkerLockDrawn && sealFailuresAtTheMarkerLock == Numbers.LONG_NULL) {
                // No late row ran past the lock on _timeline: a forward row's seal retries the
                // retire, which removes the kept file and cannot remove the marker, and is refused.
                sealFailuresAtTheMarkerLock = instance.getCheckpointSealFailures();
                timelineLock.lockMarker();
                setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                insertSynthetic(maxInsertedTs + 1);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertFalse("the seal's retry of the retire removed the kept file", instance.isCheckpointTimelineRetirePending());
                Assert.assertEquals("the seal is refused", sealFailuresAtTheMarkerLock + 1, instance.getCheckpointSealFailures());
                assertNoHistoryBesideTheMarker(instance, "the forward row under the lock on the marker");
                assertView(shape.oracleSql, shape.orderBy, "after the forward row under the lock on the marker");
            }
            if (isMarkerLockDrawn && instance.getCheckpointSealFailures() > sealFailuresAtTheMarkerLock) {
                Assert.assertTrue("the lock refused the marker's removal", timelineLock.getRefusedMarkerUnlinks() > 0);
                keptTimelineMarkerRefusals++;
            }
        } finally {
            timelineLock.unlockMarker();
        }
        final boolean isKeptBeforeTheForwardRow = instance.isCheckpointTimelineRetirePending();
        final boolean hasTimelineBeforeTheForwardRow = hasTimeline(instance);
        setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
        insertSynthetic(maxInsertedTs + 1);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertFalse("nothing keeps the timeline once the lock is gone", instance.isCheckpointTimelineRetirePending());
        Assert.assertEquals("a root seals the forward row", maxInsertedTs, instance.getHeadCheckpointMaxTs());
        if (isKeptBeforeTheForwardRow || !hasTimelineBeforeTheForwardRow || isMarkerLockDrawn) {
            Assert.assertEquals("in a fresh history", 1, publishedCheckpointGeneration(instance));
        }
        try (Path dir = checkpointsDir(instance)) {
            Assert.assertFalse("the marker went with the kept file", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
        assertView(shape.oracleSql, shape.orderBy, "after the forward row behind the kept timeline");
        if (retireFault != KEPT_TIMELINE_FAULT_CHECK) {
            keptTimelineRetireEpisodes++;
        }
    }

    /**
     * The step of {@link #keptTimelineRetireEpisode} that meets no lock: the late row below every row,
     * whose head-miss rebuild retires the timeline ahead of its commit, removes {@code _timeline}, and
     * cannot check it after the unlink. The view refuses the timeline it cannot tell is gone, and
     * writes the marker a kept file needs, beside no file. The first seal past the fault, the late
     * row's closing seal when its turn seals and the episode's forward row's otherwise, retries the
     * retire, finds the file gone, which takes the marker with it, and opens a fresh history.
     */
    private void removeTheTimelineThatItsCheckCannotSee(
            LiveViewInstance instance,
            PassShape shape,
            long minTs,
            long headMissRepairsBefore,
            long resetsBefore
    ) throws Exception {
        timelineLock.failTheCheckAfterTheNextTimelineUnlink();
        insertSynthetic(minTs - 1);
        drainWalQueue();
        driveRefreshToQuiescence(job);
        Assert.assertEquals("the head-miss rebuild repairs the late row", headMissRepairsBefore + 1, countHeadMissRepairs(job));
        Assert.assertEquals("and retires the timeline ahead of its commit", resetsBefore + 1, instance.getCheckpointTimelineResets());
        Assert.assertEquals("whose check of _timeline after the unlink fails", 1, timelineLock.disarmTheCheck());
        try (Path dir = checkpointsDir(instance)) {
            if (instance.isCheckpointTimelineRetirePending()) {
                Assert.assertFalse("no seal ran past the fault, and the file the retire removed stays gone", hasTimeline(instance));
                Assert.assertTrue("the marker a kept file needs stands beside no file", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                Assert.assertEquals("naming no generation", Numbers.LONG_NULL, LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir));
            } else {
                Assert.assertFalse("the closing seal's retry took the marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
                Assert.assertEquals("and opened a fresh history", 1, publishedCheckpointGeneration(instance));
            }
        }
        assertView(shape.oracleSql, shape.orderBy, "after the late row whose retire could not check _timeline");
        removedTimelineCheckFaults++;
    }

    /**
     * The head-miss rebuild's end-of-table splice fails to publish, and the retire that answers it
     * meets a lock on {@code _timeline}. Under a row cadence no commit reaches, the episode commits
     * a root at P, at the start of a day three days above every row, past the duration cadence,
     * with rows above it, then, three days up, rows from S, a root above them past the duration
     * cadence and rows above that root. A REPLACE_RANGE then deletes the row at S + 1: the resume
     * would read every row above P's root, the localized rebuild only the segment S sits in, so
     * the rebuild takes it, replays up
     * to the newest row and splices the timeline, which fails after the splice's metadata, before
     * the superblock names it. The closing seal lands on the generation the splice left, above
     * every root it keeps, and the lock goes on at that seal, so the retire that follows keeps the
     * file, the splice's marker beside it, and takes the head the seal set with it. A stretch
     * without rows past the cadence - rows the filter drops, or REPLACE_RANGEs above every row
     * that delete nothing - publishes nothing over the kept file. With the lock gone, a restart
     * finds the live marker and rebuilds from the applied base, and a late row then gives what the
     * oracle gives. Restores the run's knobs on the way out.
     */
    private void keptSpliceEpisode(LiveViewFuzzKnobs knobs, PassShape shape, boolean isFiltered) throws Exception {
        driveRefreshToQuiescence(job);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1_000_000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_MAX_DURATION_MICROS, (String) null);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ADAPTIVE_CADENCE_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_PER_SEGMENT_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_REPLAY_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_ISOLATED_RUNTIME_ENABLED, "false");
        // The configured chain count and key budget admit the splice's one-boundary chain.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, (String) null);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, (String) null);
        try {
            final LiveViewInstance instance = viewInstance();
            // At the start of a day, so the resume's price counts P's rows above the root: it
            // spreads the rows of a partition below the newest over the partition's whole span.
            final long prefixRootTs = (Math.max(maxInsertedTs, instance.getLatestSeenTs()) / DAY_MICROS + 3) * DAY_MICROS;
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            commitSyntheticRows(prefixRootTs, 1);
            Assert.assertEquals("a root seals P", prefixRootTs, instance.getHeadCheckpointMaxTs());
            commitSyntheticRows(prefixRootTs + 1, 8);
            final long segmentTs = prefixRootTs + 3 * DAY_MICROS;
            commitSyntheticRows(segmentTs, 4);
            setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
            commitSyntheticRows(segmentTs + 4, 1);
            Assert.assertEquals("a root seals the rows from S", segmentTs + 4, instance.getHeadCheckpointMaxTs());
            commitSyntheticRows(segmentTs + 5, 3);
            Assert.assertEquals("and none the rows above it", segmentTs + 4, instance.getHeadCheckpointMaxTs());

            final long spliceBaseGeneration = publishedCheckpointGeneration(instance);
            final long headMissRepairsBefore = countHeadMissRepairs(job);
            final long repairFailuresBefore = instance.getCheckpointRepairFailures();
            final int markerRenamesBefore = timelineLock.getMarkerRenames();
            timelineLock.afterTheNextMarker(
                    () -> job.setCheckpointTimelineTestFailureStage(LiveViewCheckpointTimelineStoreWriter.TEST_FAIL_AFTER_METADATA_PUBLISH)
            );
            hooks(job).setSimulateHeadMissHeadSealStart(() -> {
                job.setCheckpointTimelineTestFailureStage(0);
                timelineLock.lock();
            });
            try {
                commitDeparture(segmentTs + 1, segmentTs + 1, Numbers.LONG_NULL);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals("the head-miss rebuild repairs the departure", headMissRepairsBefore + 1, countHeadMissRepairs(job));
                Assert.assertEquals("the splice's marker alone", markerRenamesBefore + 1, timelineLock.getMarkerRenames());
                Assert.assertEquals("the splice fails to publish", repairFailuresBefore + 1, instance.getCheckpointRepairFailures());
                Assert.assertTrue("the retire behind it meets the lock", timelineLock.getRefusedTimelineUnlinks() > 0);
                // The closing seal landed on the generation the splice left.
                assertTimelineKept(instance, spliceBaseGeneration + 1, "the failed splice");
                assertRepairMarkerLive(instance, spliceBaseGeneration);
                for (int c = 0; c < 2; c++) {
                    setCurrentMicros(currentMicros + DURATION_CADENCE_ELAPSED_MICROS);
                    if (isFiltered) {
                        long t = maxInsertedTs + 1;
                        while (!syntheticTs.add(t)) {
                            t++;
                        }
                        maxInsertedTs = t;
                        execute("INSERT INTO base (ts, sym, i, x) VALUES (" + t + "::timestamp, '" + SYNTHETIC_KEY + "', -1, 1.0)");
                        drainWalQueue();
                        driveRefreshToQuiescence(job);
                    } else {
                        commitRowlessReplaceRange();
                    }
                }
                assertTimelineKept(instance, spliceBaseGeneration + 1, "the stretch without rows");
                assertRepairMarkerLive(instance, spliceBaseGeneration);
            } finally {
                hooks(job).setSimulateHeadMissHeadSealStart(null);
                timelineLock.afterTheNextMarker(null);
                timelineLock.unlock();
                job.setCheckpointTimelineTestFailureStage(0);
            }
            restart();
            driveRefreshToQuiescence(job);
            assertRebuiltFromAppliedBase("lv");
            assertView(shape.oracleSql, shape.orderBy, "after the restart over the kept splice");
            insertSynthetic(segmentTs + 2);
            drainWalQueue();
            driveRefreshToQuiescence(job);
            assertView(shape.oracleSql, shape.orderBy, "after the late row behind the kept splice");
            keptSpliceEpisodes++;
        } finally {
            knobs.apply(AbstractLiveViewTest::setProperty);
        }
    }

    // The view refuses keptGeneration, the generation the superblock names, which a retire could
    // not remove: nothing publishes over it, no head stands on it, and a marker beside it reads as
    // live - one naming no generation, or one a publication of the kept generation outlives.
    private void assertTimelineKept(LiveViewInstance instance, long keptGeneration, String where) {
        Assert.assertTrue("[" + where + "] the view refuses the generation the retire kept", instance.isCheckpointTimelineRetirePending());
        Assert.assertEquals("[" + where + "] nothing publishes over it", keptGeneration, publishedCheckpointGeneration(instance));
        Assert.assertEquals("[" + where + "] no head stands on it", Numbers.LONG_NULL, instance.getHeadCheckpointMaxTs());
        try (Path dir = checkpointsDir(instance)) {
            Assert.assertTrue("[" + where + "] a marker stands beside it", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
            final long markerBaseGeneration = LiveViewCheckpointRepairMarker.readBaseGeneration(configuration, dir);
            Assert.assertTrue(
                    "[" + where + "] and reads as live, markerBaseGeneration=" + markerBaseGeneration,
                    markerBaseGeneration == Numbers.LONG_NULL || keptGeneration <= markerBaseGeneration + 1
            );
        }
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    // Whether the view's checkpoint timeline, _timeline, is on disk.
    // The lock on the repair marker kept it beside no history: no slot of the view's _timeline
    // passes its checksum, so no generation stands that a restart would read the marker against.
    private static void assertNoHistoryBesideTheMarker(LiveViewInstance instance, String when) {
        try (Path dir = checkpointsDir(instance)) {
            Assert.assertTrue("[" + when + "] the lock keeps the marker", LiveViewCheckpointRepairMarker.exists(configuration.getFilesFacade(), dir));
        }
        Assert.assertEquals("[" + when + "] no history opens beside it", 0, publishedCheckpointGeneration(instance));
    }

    private static boolean hasTimeline(LiveViewInstance instance) {
        try (Path dir = checkpointsDir(instance); Path timeline = new Path()) {
            LiveViewCheckpointLayout.timelinePath(timeline, dir);
            return configuration.getFilesFacade().exists(timeline.$());
        }
    }

    // The repairs job ran on either resume from a sealed anchor.
    private static long countResumes(LiveViewRefreshStats stats) {
        return stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_RESUME_FROM_ANCHOR)
                + stats.getRouteCount(LiveViewCheckpointRepairRoute.ROUTE_KEYED_RESUME_FROM_ANCHOR);
    }

    // count rows on the synthetic key at fromTs and the microseconds above it, in one commit,
    // applied and refreshed.
    private void commitSyntheticRows(long fromTs, int count) throws SqlException {
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        for (int k = 0; k < count; k++) {
            final long t = fromTs + k;
            Assert.assertTrue(syntheticTs.add(t));
            maxInsertedTs = Math.max(maxInsertedTs, t);
            if (k > 0) {
                syntheticSink.put(", ");
            }
            syntheticSink.put('(').put(t).put("::timestamp, '").put(SYNTHETIC_KEY).put("', ").put(1 + (t & 511)).put(", 1.0)");
        }
        execute(syntheticSink);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // A REPLACE_RANGE above every row, which deletes nothing and puts no row through the window,
    // applied and refreshed.
    private void commitRowlessReplaceRange() {
        final long lo = Math.max(maxInsertedTs, viewInstance().getLatestSeenTs()) + 1;
        replaceBaseRange(lo, lo + 1, Double.NaN);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    // One row of each run key from SYMBOLS[fromSym] to SYMBOLS[symCount - 1], a microsecond apart
    // from fromTs up, in one commit. Returns the newest.
    private long insertKeyBatch(long fromTs, int fromSym, int symCount) throws SqlException {
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        long t = fromTs;
        for (int s = fromSym; s < symCount; s++, t++) {
            if (s > fromSym) {
                syntheticSink.put(", ");
            }
            syntheticSink.put('(').put(t).put("::timestamp, '").put(SYMBOLS[s]).put("', ").put(1 + (t & 511)).put(", 1.0)");
        }
        execute(syntheticSink);
        maxInsertedTs = Math.max(maxInsertedTs, t - 1);
        return t - 1;
    }

    /**
     * Holds the view's table writer across a few passes, as a busy writer or a memory-pressure
     * back-off does: no apply of the view's own WAL lands, so a repair's or a flush's block
     * stays committed and unapplied while later drains publish leads. Reads must stay on the
     * table as it stands throughout ({@link #assertTierReadConsistent}).
     */
    private void heldWriterEpisode(Rnd rnd, PassShape shape, long dataStartMicros) throws Exception {
        // Under the reason the WAL apply job takes the writer with, which is what a busy writer
        // looks like to the view's own apply.
        final TableWriter writer = engine.getWriterUnsafe(viewInstance().getLiveViewToken(), TableUtils.WAL_2_TABLE_WRITE_REASON);
        try {
            for (int k = 0, n = 1 + rnd.nextInt(3); k < n; k++) {
                final long frontier = maxInsertedTs;
                // Clamped like the other episodes' late rows: the 1970 anchor shape's data starts
                // half an hour above the epoch, and the base refuses a timestamp below it.
                insertSynthetic(rnd.nextBoolean()
                        ? frontier + 1 + rnd.nextInt(1_000_000)
                        : Math.max(dataStartMicros, frontier - 1 - rnd.nextLong(HOUR_MICROS)));
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
        insertSynthetic(ts, false);
    }

    // With isTie, a second row on SYNTHETIC_TIE_KEY at the same timestamp, in the same commit.
    private void insertSynthetic(long ts, boolean isTie) throws SqlException {
        long t = ts;
        while (!syntheticTs.add(t)) {
            t--;
        }
        maxInsertedTs = Math.max(maxInsertedTs, t);
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES (").put(t).put("::timestamp, '")
                .put(SYNTHETIC_KEY).put("', ").put(1 + (t & 511)).put(", 1.0)");
        if (isTie) {
            syntheticSink.put(", (").put(t).put("::timestamp, '")
                    .put(SYNTHETIC_TIE_KEY).put("', ").put(1 + (t & 511)).put(", 1.0)");
        }
        execute(syntheticSink);
    }

    // DEPARTURE_BATCH_ROWS rows on the synthetic key in one commit, at the first timestamps
    // from fromTs up the key has not used. Returns the lowest of them.
    private long insertSyntheticBatch(long fromTs) throws SqlException {
        syntheticSink.clear();
        syntheticSink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        long lowTs = Numbers.LONG_NULL;
        long t = fromTs;
        for (int k = 0; k < DEPARTURE_BATCH_ROWS; k++, t++) {
            while (!syntheticTs.add(t)) {
                t++;
            }
            if (k == 0) {
                lowTs = t;
            } else {
                syntheticSink.put(", ");
            }
            maxInsertedTs = Math.max(maxInsertedTs, t);
            syntheticSink.put('(').put(t).put("::timestamp, '").put(SYNTHETIC_KEY).put("', ").put(1 + (t & 511)).put(", 1.0)");
        }
        execute(syntheticSink);
        return lowTs;
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

    /**
     * A timestamp tie inside a closed anchor segment, after every generated commit. An in-order
     * row two days above the frontier closes the segment the newest generated rows sit in, and
     * one late commit then puts a row on each synthetic key at the newest generated row's
     * timestamp the view reads. That segment's repair is bounded, so the output uniqueness check
     * measures it, and it re-emits at least the two synthetic rows of the group: a keyed one
     * replays the keys its correction carries, and an unkeyed one every row from the correction
     * up. A run's own ties reach only the repairs its draws happen to route there.
     */
    private void tieInClosedSegmentEpisode(Rnd rnd, PassShape shape, LiveViewFuzzRows rows, boolean isFiltered) throws Exception {
        long tieTs = Numbers.LONG_NULL;
        for (int k = rows.tsv.length - 1; k >= 0; k--) {
            // The view's filter is i > 0, which a null i fails as well.
            if (!isFiltered || (rows.iv[k] != Numbers.LONG_NULL && rows.iv[k] > 0)) {
                tieTs = rows.tsv[k];
                break;
            }
        }
        if (tieTs == Numbers.LONG_NULL) {
            return;
        }
        LOG.info().$("LV route fuzz: tie in closed segment [tieTs=").$ts(tieTs).I$();
        insertSynthetic(maxInsertedTs + 2 * DAY_MICROS);
        drainWalQueue();
        pass(rnd, shape, -1, PIN_NO, false);
        insertSynthetic(tieTs, true);
        drainWalQueue();
        pass(rnd, shape, -1, PIN_NO, false);
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
        runBatch(steer, TestUtils.generateRandom(LOG));
    }

    private void runBatch(int steer, Rnd rnd) throws Exception {
        if (steer == STEER_FAULTS) {
            fault = new LiveViewMidDrainFault("ts", "ts", "sym");
            assertMemoryLeak(fault.facade(), () -> {
                for (int run = 0; run < RUNS_PER_TEST; run++) {
                    runOne(rnd, run, steer);
                }
            });
        } else if (steer == STEER_MIXED) {
            // The arm's episodes lock _timeline around a retire, which no other pass does.
            assertMemoryLeak(timelineLock, () -> {
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
        int variant = rnd.nextInt(VARIANT_COUNT);
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
        // to fire in. Every third fault run takes it whatever the draw, forced onto the route
        // and over a variant the route serves, so no batch goes without a keyed route by
        // chance.
        final boolean isKeyedRunGuaranteed = steer == STEER_FAULTS && run % 3 == 0;
        final boolean isKeyedShape = steer == STEER_KEYED_OPEN_SEGMENT
                || (steer == STEER_FAULTS && (isKeyedRunGuaranteed || rnd.nextBoolean()));
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
            // The per-segment decomposition is what collects the open segment's key domain,
            // so with it off neither keyed open-segment route is on offer.
            knobs.isPerSegment = true;
            knobs.isSparsePublication = true;
            knobs.scanMaxKeys = LiveViewFuzzKnobs.DEFAULT;
            knobs.scanMaxRows = LiveViewFuzzKnobs.DEFAULT;
            // Forcing takes the route whatever the pricing says, which the keyed arm always does
            // so every batch reaches it; the fault arm leaves one run in four to the pricing.
            isOpenSegmentKeyedReplayForced = steer == STEER_KEYED_OPEN_SEGMENT
                    || isKeyedRunGuaranteed
                    || rnd.nextInt(4) != 0;
            // A guaranteed fault run takes a variant the keyed routes serve, and so do the keyed
            // arm's first two runs, which gives every batch of it the two episodes of
            // secondKeyedRepairCommitFaultEpisode that take both of its shapes.
            if (isKeyedRunGuaranteed || (steer == STEER_KEYED_OPEN_SEGMENT && run < 2)) {
                variant = rnd.nextInt(GROUPED_VARIANT_COUNT);
            }
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
            // has runs that restart over a parked repair and resume one. Only the per-segment
            // loop yields, so it goes on too.
            knobs.isPerSegment = true;
            knobs.isSegmentYield = true;
            knobs.replayMaxRows = 1;
        }
        // And runs whose repairs meet ties: their own, and the one tieInClosedSegmentEpisode
        // builds in a closed segment, whose repair only the per-segment loop runs.
        final boolean isTieRun = (steer == STEER_MIXED || steer == STEER_ANCHORS) && run % 3 == 1;
        if (isTieRun) {
            knobs.isPerSegment = true;
            if (tiePct == 0) {
                tiePct = 30 + rnd.nextInt(20);
            }
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

        // A tie run's episode needs the closed-segment route, which declines the segments of
        // Berlin's repeated hour. Without the run oracle, the draw anchors where no hour repeats.
        final LiveViewFuzzAnchor anchor = LiveViewFuzzAnchor.draw(rnd, anchorMode, !isTieRun);
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
        runKeys.add(SYNTHETIC_TIE_KEY);
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
                        heldWriterEpisode(rnd, shape, anchor.dataStartMicros);
                    }
                } else {
                    pass(rnd, shape, -1);
                }
                if (rnd.nextInt(100) < checkPct) {
                    driveRefreshToQuiescence(job);
                    assertView(oracleSql, orderBy, "intermediate, commit " + c + "/" + commits);
                }
            }
            if (isTieRun) {
                tieInClosedSegmentEpisode(rnd, shape, rows, isFiltered);
            }
            if (steer == STEER_MIXED || steer == STEER_ANCHORS) {
                // Every other run steers its departure onto the splice frontier truncate, so the
                // batch reaches it whatever the draws: see departureEpisode.
                departureEpisode(rnd, knobs, shape, isFiltered, run % 2 == 0);
            }
            if (steer == STEER_MIXED) {
                // Every run retires its timeline under a lock on _timeline that keeps the file:
                // the even runs ahead of a repair's commit, the odd ones behind a head-miss
                // splice that failed to publish.
                if (run % 2 == 0) {
                    keptTimelineRetireEpisode(rnd, shape);
                } else {
                    keptSpliceEpisode(knobs, shape, isFiltered);
                }
            }
            if (steer == STEER_KEYED_OPEN_SEGMENT) {
                keyedOwedRootEpisode(knobs, oracleSql, orderBy, symCount);
            }
            if (steer != STEER_FAULTS && BERLIN_REPEATED_ANCHOR_CLAUSE.equals(anchor.clause)) {
                dstFrontierEpisode(rnd, knobs, shape, symCount);
            }
            if (steer == STEER_SPARSE_FALLBACK && run % 2 == 0) {
                // Every other run steers two late rows onto the keyed closed-segment route, one that
                // publishes sparsely and one that falls back, so the batch reaches both whatever the
                // draws: see sparsePublicationEpisode.
                sparsePublicationEpisode(shape);
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
    private record PassShape(String oracleSql, String orderBy, int pinPct, int applySkipPct, int faultPct,
                             int restartPct) {
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

    /**
     * While locked, refuses to unlink a live view's checkpoint timeline, {@code _timeline}, and
     * to remove its {@code _checkpoints} directory, the way a reader that holds the file open
     * refuses it on Windows. While the marker is locked, refuses every unlink of the repair
     * marker, {@code _repairing}, and of its staged sibling, counting them. Also runs an action
     * once, right after the next rename that publishes a repair marker, ahead of anything the
     * repair does next, and fails, once armed, the check of whether {@code _timeline} exists that
     * follows its next unlink, refused or not.
     */
    private static final class TimelineLock extends TestFilesFacadeImpl {
        private final AtomicInteger failedTimelineChecks = new AtomicInteger();
        private final AtomicInteger markerRenames = new AtomicInteger();
        private final AtomicInteger refusedMarkerUnlinks = new AtomicInteger();
        private final AtomicInteger refusedTimelineUnlinks = new AtomicInteger();
        private volatile Runnable afterTheNextMarker;
        private volatile boolean isFailingTheCheckAfterTheNextTimelineUnlink;
        private volatile boolean isFailingTheNextTimelineCheck;
        private volatile boolean isLocked;
        private volatile boolean isMarkerLocked;

        private static boolean isTimelineFile(LPSZ name) {
            return Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.TIMELINE_FILE_NAME);
        }

        @Override
        public boolean exists(LPSZ path) {
            if (isFailingTheNextTimelineCheck && isTimelineFile(path)) {
                isFailingTheNextTimelineCheck = false;
                failedTimelineChecks.incrementAndGet();
                throw CairoException.critical(0).put("injected fault on the check of _timeline");
            }
            return super.exists(path);
        }

        @Override
        public boolean removeQuiet(LPSZ name) {
            if (isMarkerLocked
                    && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)
                    && (Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)
                    || Utf8s.endsWithAscii(name, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME + LiveViewCheckpointLayout.TMP_SUFFIX))) {
                refusedMarkerUnlinks.incrementAndGet();
                return false;
            }
            if (isTimelineFile(name)) {
                if (isFailingTheCheckAfterTheNextTimelineUnlink) {
                    isFailingTheCheckAfterTheNextTimelineUnlink = false;
                    isFailingTheNextTimelineCheck = true;
                }
                if (isLocked) {
                    refusedTimelineUnlinks.incrementAndGet();
                    return false;
                }
            }
            return super.removeQuiet(name);
        }

        @Override
        public int rename(LPSZ from, LPSZ to) {
            final int result = super.rename(from, to);
            if (result == Files.FILES_RENAME_OK && Utf8s.endsWithAscii(to, LiveViewCheckpointLayout.REPAIRING_MARKER_FILE_NAME)) {
                markerRenames.incrementAndGet();
                final Runnable action = afterTheNextMarker;
                if (action != null) {
                    afterTheNextMarker = null;
                    action.run();
                }
            }
            return result;
        }

        @Override
        public boolean rmdir(Path name, boolean haltOnError) {
            if (isLocked && Utf8s.containsAscii(name, LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME)) {
                return false;
            }
            return super.rmdir(name, haltOnError);
        }

        void afterTheNextMarker(Runnable action) {
            afterTheNextMarker = action;
        }

        // Lifts the fault on the check of _timeline, armed or pending, and returns the checks it
        // failed since it was armed.
        int disarmTheCheck() {
            isFailingTheCheckAfterTheNextTimelineUnlink = false;
            isFailingTheNextTimelineCheck = false;
            return failedTimelineChecks.getAndSet(0);
        }

        // Fails, once, the check of whether _timeline exists that follows its next unlink.
        void failTheCheckAfterTheNextTimelineUnlink() {
            failedTimelineChecks.set(0);
            isFailingTheNextTimelineCheck = false;
            isFailingTheCheckAfterTheNextTimelineUnlink = true;
        }

        int getMarkerRenames() {
            return markerRenames.get();
        }

        int getRefusedMarkerUnlinks() {
            return refusedMarkerUnlinks.get();
        }

        int getRefusedTimelineUnlinks() {
            return refusedTimelineUnlinks.get();
        }

        // Counts the refused unlinks from 0.
        void lock() {
            refusedTimelineUnlinks.set(0);
            isLocked = true;
        }

        // Counts the refused unlinks of the marker from 0.
        void lockMarker() {
            refusedMarkerUnlinks.set(0);
            isMarkerLocked = true;
        }

        void unlock() {
            isLocked = false;
        }

        void unlockMarker() {
            isMarkerLocked = false;
        }
    }
}
