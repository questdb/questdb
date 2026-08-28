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
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Coverage for the repair of a correction that lands in the <b>open</b> anchor segment -
 * the one the runtime is still standing in, which the resume repairs by replaying every
 * base row above its anchor.
 * <p>
 * That resume is where the reported workload's volume is: under a daily anchor almost every
 * late commit is shallower than a day, so it lands in the open segment. These cases pin both
 * an ordinary checkpoint resume and the cold keyed bootstrap, including sparse fallback.
 */
public class LiveViewOpenSegmentKeyedReplayTest extends AbstractLiveViewTest {

    @Test
    public void testAHeadMissReplaysTheOpenSegmentColdByKeyAndPublishesSparsely() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                final long scanRowsBefore = instance.getO3ReplayScanRows();

                // The seed leaves one head root. This correction sits below it, so there is
                // no predecessor to resume from and the replay starts cold at the day origin.
                commit(row(3, 2, 35, "acct-1"), job);

                Assert.assertEquals(1, job.openSegmentColdKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentColdKeyedUnpricedCountForTest());
                Assert.assertEquals(1, job.openSegmentColdKeyedCheaperCountForTest());
                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                assertQuery("SELECT o3_open_segment_keyed_resume_count, "
                        + "o3_open_segment_cold_keyed_replay_count FROM live_views()")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("o3_open_segment_keyed_resume_count\t"
                                + "o3_open_segment_cold_keyed_replay_count\n0\t1\n");
                Assert.assertEquals(0, job.keyedReplaySegmentCountForTest());
                Assert.assertTrue(
                        job.openSegmentColdKeyedPostingRowsForTest()
                                < job.openSegmentColdKeyedWholeRangeRowsForTest()
                );
                Assert.assertEquals(
                        job.openSegmentColdKeyedPostingRowsForTest(),
                        instance.getO3ReplayScanRows() - scanRowsBefore
                );
                Assert.assertEquals(
                        "checkpoint positions must come from the exact insert delta, not a stored-row scan",
                        1,
                        job.openSegmentArithmeticRowPositionCountForTest()
                );
                Assert.assertEquals(
                        "a sparse cold repair must leave unaffected stored rows in place",
                        0,
                        job.keyedReplayMergedRowsForTest()
                );
                Assert.assertTrue(job.transplantedKeyCountForTest() > 0);
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                Assert.assertEquals(0, job.sparsePublicationFallbackCountForTest());
                Assert.assertTrue(job.sparsePublicationRowsKeptForTest() > 0);
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * A cold keyed head miss opens the view's own stored-row cursor in the executor's
     * prologue, some three hundred lines above the {@code try} whose {@code finally}
     * releases it, and the prologue can throw: the row-position rebase reads the pinned
     * generation's checkpoint metadata and raises {@link io.questdb.cairo.CairoException}
     * over a missing or torn page, and the rebase itself throws outright on an overflow.
     * The cursor holds a pooled reader of the live view's table, so one faulted repair
     * strands it for the process's life and the pool drains a tenant per fault.
     * <p>
     * The fault is injected rather than driven off a {@code FilesFacade}: the two steps
     * ahead of the rebase answer every I/O failure by dropping the capture - which deletes
     * the rebase rather than faulting it - and the rebase reads no file at all while the
     * pinned generation carries no row-position delta, which is every cold head miss a
     * fresh view takes. See {@code setSimulateColdKeyedTimelineFaultForTest}.
     */
    @Test
    public void testAColdKeyedHeadMissFreesItsStoredRowCursorWhenTheTimelineFaults() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();

                // Same correction as the cold keyed route's own case: it sits below the
                // seed's single head root, so the replay starts cold at the day origin and
                // opens the stored-row merge in the prologue.
                job.setSimulateColdKeyedTimelineFaultForTest(true);
                commit(row(3, 2, 35, "acct-1"), job);

                Assert.assertEquals(
                        "the injected checkpoint fault must have unwound exactly one refresh",
                        1,
                        instance.getRefreshFaultCount()
                );
                Assert.assertEquals(
                        "a faulted cold keyed head miss must release the stored-row cursor's pooled reader",
                        0,
                        engine.getBusyReaderCount()
                );

                // The fault is one-shot, so the retry behind it repairs the view for real.
                // That it produces the correct output is what says the release above did not
                // take a cursor some later turn still meant to read.
                assertViewMatchesRecomputeIgnoringFaults();
                Assert.assertEquals(
                        "the retry must leave no reader behind either",
                        0,
                        engine.getBusyReaderCount()
                );
            }
        });
    }

    @Test
    public void testAColdKeyedSpliceSurvivesARestart() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                commit(row(3, 2, 35, "acct-1"), job);
                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(1, job.sparsePublicationCountForTest());
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARepeatedPairFallsBackWithoutDiscardingTheColdKeyedSplice() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final long rowsBefore = count("select count() from lv");

                // The seed already carries this pair. The keyed scan stays valid, but an
                // upsert on the view's identity would collapse the two output rows.
                commit(row(3, 2, 10, "acct-1"), job);

                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(0, job.sparsePublicationCountForTest());
                Assert.assertEquals(1, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(1, job.outputUniquenessDuplicateRowsForTest());
                Assert.assertEquals(rowsBefore + 1, count("select count() from lv"));
                Assert.assertEquals(2, rowsAt("2026-01-03T02:10:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();
            }

            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(2, rowsAt("2026-01-03T02:10:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAColdHeadMissDeclinesWhenEveryKeyIsAffected() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                commit(
                        row(3, 2, 35, "acct-1") + ", "
                                + row(3, 2, 36, "acct-2") + ", "
                                + row(3, 2, 37, "acct-3") + ", "
                                + row(3, 2, 38, "acct-4"),
                        job
                );

                Assert.assertEquals(1, job.openSegmentColdKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentColdKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(0, job.transplantedKeyCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAColdHeadMissDeclinesAnOverflowedKeyDomain() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                commit(
                        row(3, 2, 35, "acct-1") + ", "
                                + row(3, 2, 36, "acct-2"),
                        job
                );

                Assert.assertEquals(0, job.openSegmentColdKeyedPricedCountForTest());
                Assert.assertEquals(1, job.openSegmentColdKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(0, job.transplantedKeyCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAColdKeyedSpliceHandsTheNextCorrectionToTheOrdinaryResume() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);

                commit(row(3, 2, 35, "acct-1"), job);
                // The cold repair re-versioned the old 09:40 root. Move the frontier past
                // it, then correct above it: preserving that root is what gives this repair
                // a predecessor to resume from.
                commit(
                        row(3, 10, 10, "acct-1") + ", "
                                + row(3, 11, 20, "acct-2") + ", "
                                + row(3, 12, 30, "acct-3") + ", "
                                + row(3, 13, 40, "acct-4"),
                        job
                );
                commit(row(3, 9, 50, "acct-4"), job);

                Assert.assertEquals(
                        "only the bootstrap repair should have to start cold",
                        1,
                        job.openSegmentColdKeyedReplayCountForTest()
                );
                Assert.assertEquals(
                        "the next correction must price from the re-versioned root",
                        1,
                        job.openSegmentKeyedPricedCountForTest()
                );
                Assert.assertEquals(
                        "this small fixture deliberately leaves the whole resume cheaper",
                        0,
                        job.openSegmentKeyedResumeCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionInTheOpenSegmentCollectsItsKeysAndPricesThem() throws Exception {
        // The measurement the route rests on: one account corrected inside the open day,
        // against a resume that reads every account's rows from its anchor to the end of the
        // base table.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        // Even the arithmetic path's measured setup price is larger than this tiny
        // fixture's whole range. Price the setup at one row so the case can exercise the
        // route; reported-density coverage validates the production crossover.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);
                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());

                // Below the frontier and inside the open day, and above the root the two
                // in-order rows above sealed - which is the shape that takes the anchor
                // resume and denies every route built for a closed segment.
                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(
                        "the open segment's resume must be priced exactly once",
                        1,
                        job.openSegmentKeyedPricedCountForTest()
                );
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(
                        "one account of four is less to read than every row above the anchor",
                        1,
                        job.openSegmentKeyedCheaperCountForTest()
                );
                Assert.assertTrue(
                        "the keyed scan must read fewer rows than the whole range: posting="
                                + job.openSegmentKeyedPostingRowsForTest()
                                + " whole=" + job.openSegmentKeyedWholeRangeRowsForTest(),
                        job.openSegmentKeyedPostingRowsForTest() < job.openSegmentKeyedWholeRangeRowsForTest()
                );
                Assert.assertEquals(
                        "no closed segment was touched, so none may be repaired",
                        0,
                        job.segmentRepairCountForTest()
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testACorrectionInTheOpenSegmentIsResumedByKeyAndPublishedSparsely() throws Exception {
        // The route end to end: the resume follows the corrected account through the base's
        // posting index, leaves every other account's stored rows exactly where they stand,
        // and the view still matches a from-base recompute afterwards.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        // The identity the publication upserts on. It is a CREATE-time schema property, so
        // it has to be on before the view exists.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(
                        "the resume must follow the correction's own keys",
                        1,
                        job.openSegmentKeyedResumeCountForTest()
                );
                assertQuery("SELECT o3_open_segment_keyed_resume_count, "
                        + "o3_open_segment_cold_keyed_replay_count FROM live_views()")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("o3_open_segment_keyed_resume_count\t"
                                + "o3_open_segment_cold_keyed_replay_count\n1\t0\n");
                Assert.assertEquals(
                        "and publish only the rows it recomputed",
                        1,
                        job.openSegmentSparseResumeCountForTest()
                );
                Assert.assertEquals(
                        "checkpoint positions must come from the exact insert delta, not a stored-row scan",
                        1,
                        job.openSegmentArithmeticRowPositionCountForTest()
                );
                Assert.assertEquals(
                        "nothing may abandon its attempt on output that names each pair once",
                        0,
                        job.sparsePublicationFallbackCountForTest()
                );
                Assert.assertTrue(
                        "the publication must have left the other accounts' rows alone",
                        job.sparsePublicationRowsKeptForTest() > 0
                );
                Assert.assertTrue(
                        "the corrected keys must be handed back to the primary runtime",
                        job.transplantedKeyCountForTest() > 0
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAKeyedResumeSurvivesARestartAndAFurtherCorrection() throws Exception {
        // The ladder a keyed resume leaves has to be restorable: its roots hold the
        // corrected keys' state and every other key's entry exactly as the old root wrote
        // it, and the row positions count the rows the publication left alone as well as
        // the ones it wrote. A restart is what reads all of that back.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);
                commit(row(4, 2, 35, "acct-1"), job);
                Assert.assertEquals(1, job.openSegmentSparseResumeCountForTest());
                assertViewMatchesRecompute();
            }
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                // A second correction, now against the ladder the first one spliced.
                commit(row(4, 3, 15, "acct-4"), job);
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testARepeatedPairAbandonsTheKeyedResumesSparsePublication() throws Exception {
        // The dynamic condition, and the only one this route cannot decide before it
        // replays: the pair the publication upserts on has to name each recomputed row
        // once. Two base rows of one account at one instant produce two output rows
        // carrying different cumulative sums, and an upsert keyed on (created_at,
        // account_id) would keep one of them.
        //
        // What makes the fallback cheap here is that the arithmetic resume walked nothing
        // to reach this point: its boundary positions came from the durable ones plus the
        // exact insert count, so the stored interval is still unread when the verdict
        // arrives and the merge writes it in the one pass that would otherwise only have
        // counted it. The rows the resume left alone go out with the replacement, which
        // collapses nothing.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);
                final long rowsBefore = count("select count() from lv");
                final String untouchedBefore = dumpRowsOf("acct-3");

                // A second acct-1 row at the exact instant its own 02:10 row already
                // holds - late, inside the open day, and above a root of it.
                commit(row(4, 2, 10, "acct-1"), job);

                Assert.assertEquals(
                        "the resume must still follow the correction's own keys",
                        1,
                        job.openSegmentKeyedResumeCountForTest()
                );
                Assert.assertEquals(
                        "and still derive its checkpoint positions from the insert delta",
                        1,
                        job.openSegmentArithmeticRowPositionCountForTest()
                );
                Assert.assertEquals(
                        "the repeated pair denies the upsert",
                        0,
                        job.openSegmentSparseResumeCountForTest()
                );
                Assert.assertEquals(1, job.sparsePublicationFallbackCountForTest());
                Assert.assertEquals(1, job.outputUniquenessDuplicateRowsForTest());
                Assert.assertEquals(
                        "the replacement carries the rows the resume had left alone",
                        rowsBefore + 1,
                        count("select count() from lv")
                );
                Assert.assertEquals(
                        "both rows of the pair survive; an upsert would have kept one",
                        2,
                        rowsAt("2026-01-04T02:10:00.000000Z", "acct-1")
                );
                TestUtils.assertEquals(untouchedBefore, dumpRowsOf("acct-3"));
                assertViewMatchesRecompute();
            }

            // The ladder the fallback published carries the arithmetic positions, so a
            // restart is what proves they describe the rows the replacement wrote.
            restartCycle();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
                driveRefreshToQuiescence(job);
                Assert.assertTrue(viewInstance().isCheckpointRestoreSucceeded());
                Assert.assertEquals(2, rowsAt("2026-01-04T02:10:00.000000Z", "acct-1"));
                assertViewMatchesRecompute();

                commit(row(4, 3, 15, "acct-4"), job);

                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnOlderSelectedRootCanMakeKeyedFasterThanTheRowVerdict() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);
                seedRestoreDominantRates();

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(
                        "posting-row pricing must still prefer the short whole interval",
                        0,
                        job.openSegmentKeyedCheaperCountForTest()
                );
                Assert.assertEquals(
                        "the selected root is older than the runtime head, so restore-aware pricing must override",
                        1,
                        job.openSegmentRestoreAwareCheaperCountForTest()
                );
                Assert.assertEquals(1, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.runtimeAnchorReuseCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAReusableHeadKeepsTheWholeRangeWhenItsScanIsFaster() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(row(4, 0, 10, "acct-1"), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                seedRestoreDominantRates();

                // Intra-commit O3 wholly above the sealed head: no row enters the window
                // pipeline before detection, so the selected anchor is the reusable head.
                commit(
                        row(4, 0, 30, "acct-1") + ", " + row(4, 0, 20, "acct-1"),
                        job
                );

                Assert.assertEquals(1, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentRestoreAwareCheaperCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(1, job.runtimeAnchorReuseCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAViewWithoutTheDedupKeysNeverResumesByKey() throws Exception {
        // The publication is an upsert on the view's own identity, so a view CREATEd
        // without it has nothing to upsert onto - and the block would otherwise have to
        // carry every stored row above the anchor, which is the whole range and no saving.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(0, job.openSegmentSparseResumeCountForTest());
                // The pricing still runs and still says the keyed read is smaller, which is
                // what says the identity is what turned the route down.
                Assert.assertEquals(1, job.openSegmentKeyedCheaperCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testTheOpenSegmentIsNotPricedWithTheRouteDeclined() throws Exception {
        // The switch is what decides whether the decomposition walks every commit's rows at
        // all, so a declined route must leave the resume reading exactly what it always did.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_OPEN_SEGMENT_KEYED_REPLAY_ENABLED, "false");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAnUnindexedKeyCollectsNoDomainAndPricesNothing() throws Exception {
        // The route turns an unindexed view down at the decomposition rather than at the
        // pricing: the keyed scan it would take needs the posting index, so there is nothing
        // to price and no reason to pay for the wider walk that collects a domain.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), false);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(0, job.openSegmentKeyedPricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
                Assert.assertEquals(0, job.openSegmentKeyedCheaperCountForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    private void seedRestoreDominantRates() {
        viewInstance().getOpenSegmentRepairCost().setRatesForTest(
                1_000_000_000L,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1
        );
    }

    /**
     * Drops the in-memory view registry and rebuilds it, which is what makes the next
     * refresh restore its runtime from the checkpoint timeline rather than continue from
     * the state this process happens to be holding.
     */
    private void restartCycle() {
        engine.getLiveViewRegistry().clear();
        engine.buildViewGraphs();
    }

    private long count(String sql) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            Assert.assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    /**
     * One account's stored rows, in an order a repeated pair cannot make ambiguous. It is
     * what says a publication left a key it never touched exactly where it stood.
     */
    private String dumpRowsOf(String account) throws Exception {
        return TestUtils.printSqlToString(
                engine,
                sqlExecutionContext,
                "select * from lv where account_id = '" + account + "' order by 1, 3",
                new StringSink()
        );
    }

    private long rowsAt(String timestamp, String account) throws Exception {
        return count("select count() from lv where account_id = '" + account + "'"
                + " and created_at = '" + timestamp + "'::timestamp");
    }

    private LiveViewInstance viewInstance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private void assertViewMatchesRecompute() throws Exception {
        assertViewMatchesRecomputeIgnoringFaults();
        assertNoRefreshFaults("lv");
    }

    private void assertViewMatchesRecomputeIgnoringFaults() throws Exception {
        final String bucket = "timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp)";
        final String recompute = "select created_at, account_id, "
                + "sum(amount) over (partition by account_id, bucket order by created_at "
                + "rows between unbounded preceding and current row) as cumulative_sum "
                + "from (select created_at, account_id, amount, " + bucket + " as bucket from tx)";
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                // The cumulative sum breaks the tie a repeated (timestamp, key) pair
                // otherwise leaves in this ordering.
                "(" + recompute + ") order by 2, 1, 3",
                "(lv) order by 2, 1, 3",
                LOG,
                true
        );
    }

    /**
     * Drives the open day forward in order, one commit per hour, so the cadence seals a
     * checkpoint root inside it. Without one the plan finds no boundary strictly below a
     * correction there, denies the resume and rebuilds from the view's own floor instead -
     * which is a different repair with a different executor and none of this route in it.
     * <p>
     * The hours matter as much as the roots: the base is partitioned by hour, so a resume
     * spanning several of them is what lets a key's postings be counted against a range
     * wider than the one partition the floor sits in.
     */
    private void openTheDayAboveARoot(LiveViewRefreshJob job) throws Exception {
        for (int hour = 0; hour < 10; hour++) {
            final StringBuilder rows = new StringBuilder();
            for (int account = 1; account <= 4; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(4, hour, account * 10, "acct-" + account));
            }
            commit(rows.toString(), job);
        }
    }

    private void commit(String values, LiveViewRefreshJob job) throws Exception {
        execute("insert into tx values " + values);
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    private void createView(String seedRows, boolean isKeyIndexed) throws Exception {
        execute("create table tx (created_at timestamp, account_id symbol nocache"
                + (isKeyIndexed ? " index capacity 4" : "") + ", "
                + "amount double) timestamp(created_at) partition by hour wal");
        execute("insert into tx values " + seedRows);
        drainWalQueue();
        execute("create live view lv flush every 100ms start from beginning as "
                + "select created_at, account_id, sum(amount) over w as cumulative_sum "
                + "from tx window w as (partition by account_id order by created_at anchor daily '00:00')");
    }

    /**
     * One row of {@code account} at {@code hour}:{@code minute} on 2026-01-{@code day}, as
     * an INSERT tuple. With a daily anchor the day is also the segment.
     */
    private String row(int day, int hour, int minute, String account) {
        return "('2026-01-" + String.format("%02d", day) + "T" + String.format("%02d", hour)
                + ":" + String.format("%02d", minute) + ":00.000000Z', '" + account + "', 1.0)";
    }

    /**
     * Ten rows of each of four accounts on each of 2026-01-02 and 2026-01-03, one per hour.
     * 2026-01-04 is left to {@link #openTheDayAboveARoot}, which drives it in order: it is
     * the open segment once the view has caught up, so a correction inside it is the shape
     * every route built for a closed segment declines.
     */
    private String seedFourAccountsOverTwoDays() {
        final StringBuilder rows = new StringBuilder();
        for (int day = 2; day <= 3; day++) {
            for (int hour = 0; hour < 10; hour++) {
                for (int account = 1; account <= 4; account++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(row(day, hour, account * 10, "acct-" + account));
                }
            }
        }
        return rows.toString();
    }
}
