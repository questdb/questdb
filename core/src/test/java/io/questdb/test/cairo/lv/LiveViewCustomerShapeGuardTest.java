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

package io.questdb.test.cairo.lv;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointKeyProjector;
import io.questdb.cairo.lv.LiveViewCheckpointOutputUniqueness;
import io.questdb.cairo.lv.LiveViewCheckpointRepairPlan;
import io.questdb.cairo.lv.LiveViewCompiledPlan;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewSegmentRepairEnvelope;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The reported customer shape, replayed against the production configuration, guard by
 * guard.
 * <p>
 * The open-segment keyed resume arms only where a list of static conditions holds - an
 * indexed base SYMBOL projected as the window partition key, a base that does not
 * deduplicate, a view with no filter, a live-view table carrying the identity a sparse
 * publication upserts on, and a daily anchor with a closed-form segment end. Every one of
 * them is a property of the customer's DDL rather than of any repair, so they can be
 * settled before a soak is allocated rather than discovered inside one. These cases settle
 * them.
 * <p>
 * The DDL below carries the reported shape column for column - an ordered designated
 * timestamp, an indexed SYMBOL key, a DOUBLE measure, hourly WAL partitioning and a daily
 * anchor over a cumulative sum and count - under neutral table and column names. Only the
 * names differ from the ones the reported logs recorded, and no guard reads a name. The
 * fixture changes exactly one production value: {@code cairo.live.view.checkpoint.rows},
 * so a thousand-row fixture seals the roots a million-row cadence would not. That is a
 * cadence and not a price - every switch and every cost the route consults stays at its
 * shipped default, which is what makes the decisions these cases assert the production
 * decisions.
 */
public class LiveViewCustomerShapeGuardTest extends AbstractLiveViewTest {

    private static final int ACCOUNT_COUNT = 8;
    private static final String BASE = "payments";
    // The customer's second view anchors at 12:00, so the anchor segment and the calendar
    // day do not coincide: a segment runs 12:00 to 12:00.
    private static final String ANCHOR_TIME = "12:00";
    private static final int FIRST_HOUR = 12;
    private static final int HOURS_PER_SEGMENT = 10;
    // Rows of one account inside one hourly base partition. It has to clear the open
    // route's effective index-open price of four rows for a single-key resume to read less
    // than the whole range, which is the crossover
    // testTheOpenSegmentCapSelectsWhatTheConfiguredPriceDeclines measures.
    private static final int ROWS_PER_ACCOUNT_PER_HOUR = 4;
    // The minute the last row of an hour lands on, since one row lands per minute from 1.
    private static final int HEAD_MINUTE = ROWS_PER_ACCOUNT_PER_HOUR * ACCOUNT_COUNT;
    // The origin of the anchor segment the trickle case corrects into, spelled as its
    // view's START FROM boundary.
    private static final String SEGMENT_ORIGIN = "2026-01-04T12:00:00.000000Z";
    // The checkpoint cadence the fixture runs at, so a few-hundred-row view seals the
    // roots a million-row cadence would not. It is also what bounds how far below a
    // correction the newest usable anchor can sit.
    private static final int CHECKPOINT_ROWS = 16;
    // Corrections the trickle case makes, each one minute deeper under the batch head.
    private static final int TRICKLE_PASSES = 5;
    private static final String VIEW = "payments_view";
    // The view's START FROM boundary as the DDL spells it, or null when the view starts
    // from the beginning.
    private String startFrom;

    @Test
    public void testACustomerViewCreatedWithoutTheIdentityNeverArmsTheRoute() throws Exception {
        // The operational half of the identity guard, and the reason a running server's
        // configuration cannot answer it: the dedup keys a sparse publication upserts on go
        // into the view table's own _meta at CREATE. A view created by a build that had the
        // switch off - or by a build that predates it, which is every build the customer has
        // run so far - carries none, and turning the switch on afterwards does not add them.
        // Such a view has to be re-CREATEd before any of this route is available to it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        assertMemoryLeak(() -> {
            createCustomerShape(true, ANCHOR_TIME, null);
            // Back on, as an upgraded server would have it. The view already exists.
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
            Assert.assertTrue(engine.getConfiguration().isLiveViewCheckpointRepairSparsePublicationEnabled());

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                driveSegmentInOrder(job, 4);

                Assert.assertFalse(
                        "the view must carry no dedup identity, whatever the switch reads now",
                        viewInstance().isDedupKeyed()
                );

                correct(job, 4, FIRST_HOUR, 1);

                // The read the route would have taken is still the cheaper one, which is
                // what says the missing identity and not the price turned it down.
                Assert.assertEquals(1, job.openSegmentKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.openSegmentSparseResumeCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUnindexedCustomerBaseDeclinesTheRoute() throws Exception {
        // The base column the reported incident's own table carried, as far as the supplied
        // DDL records it: a SYMBOL with no index. The keyed read needs the posting index, so
        // the decomposition never widens to collect a key domain and nothing is priced.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        assertMemoryLeak(() -> {
            createCustomerShape(false, ANCHOR_TIME, null);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                Assert.assertEquals(
                        LiveViewSegmentRepairEnvelope.GATE_KEY_NOT_INDEXED,
                        viewInstance().getKeyedScanGate()
                );

                driveSegmentInOrder(job, 4);
                correct(job, 4, FIRST_HOUR, 1);

                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testATurnOwningItsWholeKeyDomainStaysOnWholeRangeReplay() throws Exception {
        // The other side of the production crossover, and the reason the reported-density
        // A/B leaves its shallow repairs on whole-range replay: Q is the whole refresh
        // turn's key domain, not just the corrected key. A turn holding its forward output
        // in the in-memory tier owns those keys too, and once Q covers the keys the range
        // holds, the keyed read reaches every row the whole-range read does and pays a
        // posting index and a merge heap on top.
        //
        // The customer's own FLUSH EVERY 5s is what produces such a turn here: the segment
        // below is driven to quiescence rather than to durability, so the correction lands
        // on a turn that still owns every account's forward rows.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        assertMemoryLeak(() -> {
            createCustomerShape(true, ANCHOR_TIME, null);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                driveSegmentInOrder(job, 4, false);

                correct(job, 4, FIRST_HOUR, 1);

                Assert.assertEquals(
                        "the resume must still be priced - the shape is eligible, the read is not cheaper",
                        1,
                        job.openSegmentKeyedPricedCountForTest()
                );
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.openSegmentSparseResumeCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheConfiguredPriceIsWhatTheClosedSegmentReads() throws Exception {
        // The other half of the cap's proof: lower the configured price to the value the
        // open segment caps at, and the closed-segment repair the case above declined is
        // selected. Nothing about the fixture changed, so the configured price is what that
        // route reads - and the open route reads it capped.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 4);
        assertMemoryLeak(() -> {
            createCustomerShape(true, ANCHOR_TIME, null);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                driveSegmentInOrder(job, 4);

                correct(job, 3, FIRST_HOUR + 1, 1);

                Assert.assertTrue(job.keyedScanPricedCountForTest() > 0);
                Assert.assertTrue(
                        "at four rows per index open the closed segment takes the keyed read too",
                        job.keyedScanCheaperCountForTest() > 0
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheCustomerShapeArmsEveryOpenSegmentGuard() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        assertMemoryLeak(() -> {
            createCustomerShape(true, ANCHOR_TIME, null);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = viewInstance();
                final LiveViewCompiledPlan plan = instance.getCompiledPlan();

                // Guard 1: the view's SELECT carries no filter, so every new base row
                // produces exactly one output row - the identity the arithmetic checkpoint
                // positions rest on.
                Assert.assertNull("the customer SELECT must carry no filter", plan.getFilter());

                // Guard 2: the base does not deduplicate, for the same identity.
                final TableToken baseToken = engine.getTableTokenIfExists(BASE);
                Assert.assertNotNull(baseToken);
                try (TableReader reader = engine.getReader(baseToken)) {
                    for (int i = 0, n = reader.getMetadata().getColumnCount(); i < n; i++) {
                        Assert.assertFalse(
                                "the customer base must not deduplicate: " + reader.getMetadata().getColumnName(i),
                                reader.getMetadata().isDedupKey(i)
                        );
                    }
                }

                // Guard 3: the window partition key traces back to an indexed base SYMBOL
                // the posting-index scan can follow.
                Assert.assertEquals(
                        "checkpoint_keyed_scan_gate must read available",
                        LiveViewSegmentRepairEnvelope.GATE_AVAILABLE,
                        instance.getKeyedScanGate()
                );
                final LiveViewCheckpointKeyProjector projector =
                        plan.getWindowFactory().getCheckpointKeyProjector();
                Assert.assertNotNull("the anchored window must project one partition key", projector);
                final int windowInputColumnIndex = projector.getIndexedSymbolColumnIndex();
                Assert.assertTrue("the partition key must be an indexed SYMBOL", windowInputColumnIndex >= 0);
                final int scanColumnIndex = plan.traceWindowInputColumnToBaseScan(windowInputColumnIndex);
                Assert.assertTrue("the partition key must trace to a base scan column", scanColumnIndex >= 0);
                Assert.assertTrue(
                        "the base scan must support an indexed forward timestamp range on it",
                        plan.getPageFrameFactory().isIndexedForwardTimestampRangeSupported(scanColumnIndex)
                );

                // Guard 4: the daily anchor has a closed-form segment end, so there is an
                // open segment to resume into at all.
                Assert.assertNotNull(
                        "ANCHOR DAILY '" + ANCHOR_TIME + "' must yield a checkpoint anchor plan",
                        instance.getAnchorWindow().getCheckpointAnchorPlan()
                );
                Assert.assertEquals(
                        "checkpoint_segment_repair_gate must read available",
                        LiveViewSegmentRepairEnvelope.GATE_AVAILABLE,
                        instance.getSegmentScopeGate()
                );

                // Guard 5: the view table carries the (designated timestamp, output key)
                // identity the sparse publication upserts on, and it is the same pair the
                // repair's uniqueness check proves.
                Assert.assertTrue("the view must carry the sparse-publication dedup identity", instance.isDedupKeyed());
                Assert.assertEquals(
                        LiveViewCheckpointOutputUniqueness.outputKeyColumnIndex(plan),
                        instance.getDedupKeyColumnIndex()
                );

                // Guard 6: the production configuration the customer would run. This fixture
                // overrides none of these.
                Assert.assertTrue(engine.getConfiguration().isLiveViewCheckpointRepairPerSegmentEnabled());
                Assert.assertTrue(engine.getConfiguration().isLiveViewCheckpointRepairOpenSegmentKeyedReplayEnabled());
                Assert.assertTrue(engine.getConfiguration().isLiveViewCheckpointRepairSparsePublicationEnabled());
                Assert.assertEquals(256L, engine.getConfiguration().getLiveViewCheckpointRepairKeyedScanIndexOpenRows());
                Assert.assertEquals(100_000L, engine.getConfiguration().getLiveViewCheckpointRepairScanMaxKeys());
            }

            // And the same two gates as an operator reads them off a running server, which
            // is the preflight the soak protocol asks for.
            assertQuery("SELECT checkpoint_segment_repair_gate, checkpoint_keyed_scan_gate"
                    + " FROM live_views() WHERE view_name = '" + VIEW + "'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            checkpoint_segment_repair_gate\tcheckpoint_keyed_scan_gate
                            available\tavailable
                            """);
        });
    }

    @Test
    public void testTheOpenSegmentCapSelectsWhatTheConfiguredPriceDeclines() throws Exception {
        // The pricing decision at the shipped configuration, proven without reading a log
        // line: the configured index-open price stays 256, and the open segment's own
        // effective cap of four rows is the only reason the same repair is selected. The
        // control is the same correction one anchor segment lower - a closed segment, which
        // keeps the configured price and therefore declines the read the open segment takes.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        assertMemoryLeak(() -> {
            createCustomerShape(true, ANCHOR_TIME, null);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                driveSegmentInOrder(job, 4);

                // A late row of one account, reaching back to the open segment's first hour
                // and therefore spanning every hourly base partition above it.
                correct(job, 4, FIRST_HOUR, 1);

                Assert.assertEquals(
                        "the open segment's resume must be priced once, at the production default",
                        1,
                        job.openSegmentKeyedPricedCountForTest()
                );
                Assert.assertEquals(1, job.openSegmentKeyedCheaperCountForTest());
                Assert.assertEquals(1, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(
                        "positions must come from the exact insert delta rather than a stored-row scan",
                        1,
                        job.openSegmentArithmeticRowPositionCountForTest()
                );
                Assert.assertEquals(1, job.openSegmentSparseResumeCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(0, job.outputUniquenessDuplicateRowsForTest());
                Assert.assertTrue(job.sparsePublicationRowsKeptForTest() > 0);

                final long postingRows = job.openSegmentKeyedPostingRowsForTest();
                final long wholeRangeRows = job.openSegmentKeyedWholeRangeRowsForTest();
                Assert.assertTrue(
                        "the keyed read must be the smaller one: posting=" + postingRows
                                + " whole=" + wholeRangeRows,
                        postingRows < wholeRangeRows
                );

                // The control. The same correction one segment lower is a closed-segment
                // repair, which prices at the configured 256 rows per index open. On a
                // fixture holding four rows of an account per hourly partition that is far
                // above the whole-range read, so it declines - which is what says the open
                // route's selection above came from the cap and not from the fixture.
                correct(job, 3, FIRST_HOUR + 1, 1);

                Assert.assertTrue(
                        "the closed-segment repair must be priced at the configured price",
                        job.keyedScanPricedCountForTest() > 0
                );
                Assert.assertEquals(
                        "and decline: 256 rows per index open outprices the whole-range read here",
                        0,
                        job.keyedScanCheaperCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheTimeZoneAnchoredCustomerViewDeclinesTheRoute() throws Exception {
        // The customer's FIRST view - the one the incident was reported against - anchors
        // DAILY '00:00' 'Asia/Kolkata'. A time-zone-aware daily anchor desugars to
        // timestamp_floor_utc, whose buckets change width at a DST transition and so have no
        // closed-form end, and LiveViewCheckpointFunctionCompiler.anchorPlan declines it
        // outright. There is then no segmentation to decompose against: no closed segment to
        // scope, no open segment to resume into, and none of this route.
        //
        // Asia/Kolkata observes no DST today, but the plan is withheld from the desugaring
        // rather than from the zone, so this is a verdict on the shape and not on the data. A
        // customer wanting the route on that view has to drop the zone from the anchor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 16);
        assertMemoryLeak(() -> {
            createCustomerShape(true, "00:00", "Asia/Kolkata");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                final LiveViewInstance instance = viewInstance();
                Assert.assertNull(
                        "a time-zone-aware daily anchor must yield no checkpoint anchor plan",
                        instance.getAnchorWindow().getCheckpointAnchorPlan()
                );
                // The gate an operator reads is "incomplete dependency" rather than "no
                // anchor plan": the anchored functions declare an anchored dependency, and
                // with no anchor plan to satisfy it the dependency check refuses first.
                Assert.assertEquals(
                        LiveViewSegmentRepairEnvelope.GATE_INCOMPLETE_DEPENDENCY,
                        instance.getSegmentScopeGate()
                );

                driveSegmentInOrder(job, 4);
                correct(job, 4, FIRST_HOUR, 1);

                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.segmentRepairCountForTest());
                // The view still refreshes and still holds one output row per base row: the
                // decline costs it the localized repair path and nothing else.
                assertNoRefreshFaults(VIEW);
                Assert.assertEquals(rowCount(BASE), rowCount(VIEW));
            }
        });
    }

    @Test
    public void testATrickleOfLateRowsRepairsTheCorrectionDepthRatherThanTheView() throws Exception {
        // The reported symptom, end to end: an anchored view under a steady trickle of
        // rows a minute or two late, with no sealed root below any of them.
        //
        // The anchor segment is a day wide, so the convergence boundary an anchored repair
        // proves sits at TOMORROW's segment end - which the runtime frontier cannot reach
        // until the day rolls over. Under a finite-bound requirement that denies
        // localization on every repair for a full day, and an unlocalized rebuild re-emits
        // the view from its START FROM boundary, retires the timeline and leaves one fresh
        // root at the batch head - which the next correction is again below. Each pass then
        // costs O(view size) and the view only grows: the reported logs ran that loop 803
        // times, at 289 to 294 rows scanned and 4 to 12 re-emitted per pass on this
        // fixture.
        //
        // Two things close it, and this case needs both. Localizing behind the EOF bound
        // cuts the emit half: the anchor expires by time, so the replacement floor rises
        // from START FROM to the correction itself and the replay's state is promoted
        // rather than the pre-repair runtime restored. Splicing the timeline instead of
        // truncating it cuts the scan half: the roots above the correction survive the
        // repair that moved their output, so the next correction finds an anchor below
        // itself, the cost comparison picks the resume over the rebuild, and the read
        // collapses from the whole segment to the tail above that anchor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, CHECKPOINT_ROWS);
        assertMemoryLeak(() -> {
            // START FROM the segment origin, which is where the reported view sits: its
            // first anchor segment, so the segment start and the view boundary coincide
            // and the scan floor has nowhere to rise to yet.
            createCustomerShape(true, ANCHOR_TIME, null, SEGMENT_ORIGIN);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // One hour short of the segment's start, so the bootstrap correction below
                // lands under every root the cadence seals.
                for (int hour = FIRST_HOUR + 1; hour < FIRST_HOUR + HOURS_PER_SEGMENT; hour++) {
                    driveHourInOrder(job, 4, hour);
                }

                final LiveViewInstance instance = viewInstance();

                // The bootstrap. A correction at the view's own floor has no root below it,
                // so the repair is the boundary rebuild - and it re-emits the whole view,
                // because the correction IS the view's floor and there is nothing above it
                // to keep. That cost is unavoidable and unchanged.
                //
                // What it leaves behind is not. The rebuild used to retire the whole
                // timeline and re-seal one root at the batch head, which is what left every
                // following correction with nothing below it. It splices now: every root
                // the cadence sealed during the drive keeps its logical key and receives a
                // new payload version, so the ladder comes out of the bootstrap the depth
                // it went in.
                final long viewRows = rowCount(VIEW);
                correct(job, 4, FIRST_HOUR, 1, 1);

                Assert.assertEquals(
                        LiveViewCheckpointRepairPlan.DISPOSITION_BOUNDARY_REBUILD,
                        instance.getCheckpointRepairLastDisposition()
                );
                Assert.assertEquals(
                        "a correction at the view floor re-emits the view whatever the plan proves",
                        viewRows + 1,
                        instance.getO3BoundaryReplayRows()
                );

                final long ladderDepth =
                        instance.getCheckpointTimeline()[LiveViewInstance.CHECKPOINT_TIMELINE_ENTRIES];
                Assert.assertTrue(
                        "the bootstrap must leave a ladder, not one head-sealed root",
                        ladderDepth > 1
                );
                Assert.assertEquals(
                        "every surviving root must be one the splice re-versioned",
                        ladderDepth,
                        instance.getCheckpointRepairRootsVersioned()
                );

                // The trickle. Each correction sits a few minutes under the head of the
                // last batch - and one minute deeper than the one before it, so no pass is
                // resting on a root its own predecessor sealed.
                final long rebuiltBefore = instance.getO3BoundaryReplayRows();
                long resumedBefore = instance.getO3ResumeReplayRows();
                long scannedBefore = instance.getO3ReplayScanRows();
                for (int pass = 1; pass <= TRICKLE_PASSES; pass++) {
                    final int minute = HEAD_MINUTE - pass;
                    correct(job, 4, FIRST_HOUR + HOURS_PER_SEGMENT - 1, minute, 1);

                    // The ladder the bootstrap kept holds a root below this correction, so
                    // the plan prices a resume from it against the localized rebuild and
                    // the resume wins. "resume cheaper" is that comparison's outcome, not a
                    // refusal: it names why the rebuild's bounds were dropped.
                    Assert.assertEquals(
                            LiveViewCheckpointRepairPlan.DENIAL_RESUME_CHEAPER,
                            instance.getCheckpointRepairLastDenialReason()
                    );
                    Assert.assertEquals(
                            LiveViewCheckpointRepairPlan.DISPOSITION_RESUME_FROM_ANCHOR,
                            instance.getCheckpointRepairLastDisposition()
                    );
                    // No rebuild ran at all, so nothing below re-reads the segment: the
                    // boundary-replay counter has not moved since the bootstrap.
                    Assert.assertEquals(
                            "pass " + pass + " must take the resume, not the rebuild",
                            rebuiltBefore,
                            instance.getO3BoundaryReplayRows()
                    );
                    Assert.assertEquals(0, job.segmentRepairCountForTest());
                    // And the resume itself takes its own cheapest route: the correction
                    // touched one account, so the replay follows that key through the base
                    // posting index rather than reading the interval whole.
                    Assert.assertEquals(pass, job.openSegmentKeyedResumeCountForTest());

                    // The scan collapsed from the whole segment to the tail above the
                    // anchor. The cadence seals a root every CHECKPOINT_ROWS view rows, so
                    // the newest root below the correction is within one cadence of it, and
                    // the pass reads that tail plus the rows the earlier passes inserted
                    // into it.
                    final long scanned = instance.getO3ReplayScanRows() - scannedBefore;
                    Assert.assertTrue(
                            "pass " + pass + " scanned " + scanned + " of a " + rowCount(VIEW)
                                    + "-row view, expected the tail above the anchor",
                            scanned <= CHECKPOINT_ROWS + pass
                    );
                    // The emit is bounded by the same tail. It is not the count of view
                    // rows above the correction, and deliberately so: the keyed route
                    // recomputes the corrected account's rows above the anchor and copies
                    // every other account's stored rows forward untouched, so this counts
                    // one key's rows rather than eight. What proves the other seven came
                    // through intact is the from-base recompute at the end of the case.
                    final long resumed = instance.getO3ResumeReplayRows() - resumedBefore;
                    Assert.assertTrue(
                            "pass " + pass + " must re-emit the anchor's tail, not the view",
                            resumed > 0 && resumed <= CHECKPOINT_ROWS + pass
                    );

                    // And the ladder comes out of each pass the depth it went in: the
                    // resume splices the roots above its anchor rather than dropping them,
                    // which is what keeps the next correction cheap too.
                    Assert.assertEquals(
                            "pass " + pass + " must leave the ladder intact",
                            ladderDepth,
                            instance.getCheckpointTimeline()[LiveViewInstance.CHECKPOINT_TIMELINE_ENTRIES]
                    );

                    resumedBefore = instance.getO3ResumeReplayRows();
                    scannedBefore = instance.getO3ReplayScanRows();
                }

                // The promotion is what the EOF bound licenses, and only rows arriving
                // after the repair can tell a correct one from a lost one: they are
                // evaluated incrementally against whatever state the repair left behind.
                driveHourInOrder(job, 4, FIRST_HOUR + HOURS_PER_SEGMENT);
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * The from-base recompute the view has to equal, bucketed on the same daily anchor the
     * view's own window carries.
     */
    private void assertViewMatchesRecompute() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T" + ANCHOR_TIME
                + ":00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum, "
                + "count(account_id) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_count "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from " + BASE
                + (startFrom != null ? " where created_at >= '" + startFrom + "'" : "") + ")";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + recompute + ") order by 2, 1, 3",
                "(" + VIEW + ") order by 2, 1, 3",
                LOG,
                true
        );
        assertNoRefreshFaults(VIEW);
    }

    /**
     * One late row of {@code account}, at {@code hour} of the anchor day beginning on
     * 2026-01-{@code day}, then a refresh. It arrives after the day it belongs to has been
     * driven forward, so it is the correction that triggers a checkpoint repair.
     */
    private void correct(LiveViewRefreshJob job, int day, int hour, int account) throws Exception {
        correct(job, day, hour, 55, account);
    }

    private void correct(LiveViewRefreshJob job, int day, int hour, int minute, int account) throws Exception {
        execute("insert into " + BASE + " values " + row(day, hour, minute, account));
        drainWalQueue();
        driveUntilDurable(job);
    }

    /**
     * The reported DDL, at the columns the view reads, under neutral names.
     *
     * @param isKeyIndexed whether {@code account_id} carries the posting index the keyed
     *                     read follows; the ordered base the customer built for their
     *                     second attempt does, and that is the shape the logs recorded
     * @param anchorTime   the daily anchor's wall time
     * @param anchorZone   the anchor's time zone, or null for the customer's second view,
     *                     which carries none
     */
    private void createCustomerShape(boolean isKeyIndexed, String anchorTime, String anchorZone) throws Exception {
        createCustomerShape(isKeyIndexed, anchorTime, anchorZone, null);
    }

    private void createCustomerShape(boolean isKeyIndexed, String anchorTime, String anchorZone, String startFrom) throws Exception {
        this.startFrom = startFrom;
        execute("create table " + BASE + " ("
                + "created_at timestamp, "
                + "account_id symbol nocache" + (isKeyIndexed ? " index capacity 4" : "") + ", "
                + "amount double"
                + ") timestamp(created_at) partition by hour wal");
        execute("insert into " + BASE + " values " + segmentRows(2) + ", " + segmentRows(3));
        drainWalQueue();
        execute("create live view " + VIEW + " flush every 5s start from "
                + (startFrom != null ? "'" + startFrom + "'" : "beginning") + " as "
                + "select created_at, account_id, "
                + "sum(amount) over w as cumulative_sum, "
                + "count(account_id) over w as cumulative_count "
                + "from " + BASE + " "
                + "window w as (partition by account_id order by created_at "
                + "anchor daily '" + anchorTime + "'" + (anchorZone != null ? " '" + anchorZone + "'" : "") + ")");
    }

    /**
     * Drives one hourly base partition forward in order - every account's rows for that
     * hour in one commit - and flushes the view's output through to disk.
     */
    private void driveHourInOrder(LiveViewRefreshJob job, int day, int hour) throws Exception {
        driveHourInOrder(job, day, hour, true);
    }

    private void driveHourInOrder(LiveViewRefreshJob job, int day, int hour, boolean isFlushed) throws Exception {
        final StringBuilder rows = new StringBuilder();
        for (int i = 0; i < ROWS_PER_ACCOUNT_PER_HOUR; i++) {
            for (int account = 1; account <= ACCOUNT_COUNT; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(day, hour, account + i * ACCOUNT_COUNT, account));
            }
        }
        execute("insert into " + BASE + " values " + rows);
        drainWalQueue();
        if (isFlushed) {
            driveUntilDurable(job);
        } else {
            driveRefreshToQuiescence(job);
        }
    }

    /**
     * Drives one anchor day forward in order, one commit per hour, so the checkpoint cadence
     * seals roots inside it and the runtime's frontier ends up standing in it. Without a root
     * strictly below the correction that follows, the repair rebuilds from the view's own
     * floor instead, which is a different executor with none of this route in it.
     */
    private void driveSegmentInOrder(LiveViewRefreshJob job, int day) throws Exception {
        driveSegmentInOrder(job, day, true);
    }

    private void driveSegmentInOrder(LiveViewRefreshJob job, int day, boolean isFlushed) throws Exception {
        for (int hour = FIRST_HOUR; hour < FIRST_HOUR + HOURS_PER_SEGMENT; hour++) {
            driveHourInOrder(job, day, hour, isFlushed);
        }
    }

    /**
     * Drives the refresh job until every output row the base implies is durable.
     * <p>
     * {@code FLUSH EVERY 5s} is the customer's own cadence, and
     * {@link #driveRefreshToQuiescence} stops at the first pass that makes no progress -
     * which, inside a flush interval, leaves the turn's output in the in-memory tier. Those
     * rows are not durable, so the next repair owns their keys as well as the corrected one,
     * and a fixture of eight accounts then hands every repair the whole account domain. Real
     * turns own thousands of keys out of millions; this drives the flush through so the
     * fixture's correction owns the one key it corrects.
     */
    private void driveUntilDurable(LiveViewRefreshJob job) {
        for (int i = 0; i < REFRESH_QUIESCENCE_PASSES; i++) {
            setCurrentMicros(currentMicros + Micros.SECOND_MICROS);
            drainWalQueue();
            drainJob(job);
            drainWalQueue();
            if (rowCount(VIEW) == expectedViewRows()) {
                return;
            }
        }
        Assert.fail("the live view never made its whole output durable: view=" + rowCount(VIEW)
                + " expected=" + expectedViewRows());
    }

    /**
     * Base rows at or above the view's own {@code START FROM} boundary - the whole base
     * for the cases that start from the beginning, and one anchor segment's worth for the
     * one that starts at a segment origin.
     */
    private long expectedViewRows() {
        return startFrom == null
                ? rowCount(BASE)
                : countRows(BASE, "created_at >= '" + startFrom + "'");
    }

    /**
     * One row of {@code account} at {@code hour}:{@code minute} on 2026-01-{@code day}, as
     * a timestamp literal - the same instant {@link #row} writes, without the tuple around
     * it.
     */
    private String segmentTs(int day, int hour, int minute) {
        return "2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z";
    }

    /**
     * View rows at or above {@code ts} - the replacement a repair whose output floor
     * landed there has to carry, and nothing else.
     */
    private long viewRowsAtOrAbove(String ts) {
        return countRows(VIEW, "created_at >= '" + ts + "'");
    }

    private long countRows(String tableName, String predicate) {
        try (RecordCursorFactory factory = select("select count() from " + tableName + " where " + predicate)) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                return cursor.getRecord().getLong(0);
            }
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
    }

    private long rowCount(String tableName) {
        final TableToken token = engine.getTableTokenIfExists(tableName);
        Assert.assertNotNull("table '" + tableName + "' must exist", token);
        try (TableReader reader = engine.getReader(token)) {
            return reader.size();
        }
    }

    /**
     * One row of {@code account} at {@code hour}:{@code minute} on 2026-01-{@code day}, as an
     * INSERT tuple.
     */
    private String row(int day, int hour, int minute, int account) {
        return "('" + segmentTs(day, hour, minute) + "', 'acct-" + account + "', 1.0)";
    }

    /**
     * One whole anchor day of every account, in order: {@link #HOURS_PER_SEGMENT} hourly base
     * partitions holding {@link #ROWS_PER_ACCOUNT_PER_HOUR} rows of each of
     * {@link #ACCOUNT_COUNT} accounts.
     */
    private String segmentRows(int day) {
        final StringBuilder rows = new StringBuilder();
        for (int hour = FIRST_HOUR; hour < FIRST_HOUR + HOURS_PER_SEGMENT; hour++) {
            for (int i = 0; i < ROWS_PER_ACCOUNT_PER_HOUR; i++) {
                for (int account = 1; account <= ACCOUNT_COUNT; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(day, hour, account + i * ACCOUNT_COUNT, account));
                }
            }
        }
        return rows.toString();
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(VIEW);
        Assert.assertNotNull("live view '" + VIEW + "' must be registered", instance);
        return instance;
    }
}
