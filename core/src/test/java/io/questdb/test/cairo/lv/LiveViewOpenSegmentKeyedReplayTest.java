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
import io.questdb.cairo.lv.LiveViewCheckpointOpenSegmentCost;
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
    // account_id's position in the view's base page-frame scan, which is the index both
    // index checks of a cold keyed repair are asked about.
    private static final int BASE_KEY_COLUMN_INDEX = 1;
    // How many extra rows of one account a hot hour of openTheDayAboveARoot carries.
    // Enough that the account's postings over the partition the replay starts inside
    // outrun what the whole-range estimate counts of that partition, which is the slice
    // above the anchor and not the partition.
    private static final int HOT_ACCOUNT_ROWS_PER_HOUR = 60;

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

    @Test
    public void testAnAnchorResumeWhoseReplayCleanupFaultsReleasesItsRepairSession() throws Exception {
        // The anchor resume runs the same shape as the head-miss executor: a cleanup chain that
        // frees native memory and closes files, with the repair session released on its last
        // statement. A throw from any statement ahead of that release used to skip it - and this
        // executor NEVER parks, so no instance is holding the session and handleRefreshFailure's
        // discardSuspendedRepair has nothing to find. The session, its descriptor mapping and its
        // Paths were then lost for the life of the process, whatever the view did afterwards.
        //
        // The checkpoint chain is declined, which leaves the resume holding a session and no staged
        // capture. Without that the capture would leak on this path too - the publication tail that
        // owns it is exactly what the throw skips - and the oracle could not tell the two apart.
        //
        // The throw is injected; there is no reproducible natural producer for it. What the case
        // pins is the ordering, which holds for any throwable the cleanup raises.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                // The resume's own replay cleanup is the next chain this worker runs.
                job.setSimulateRepairCleanupFaultForTest(0);
                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertFalse(
                        "the injected cleanup fault never fired, so the resume's cleanup never ran"
                                + " and the case pinned nothing",
                        job.isRepairCleanupFaultArmedForTest()
                );
                Assert.assertEquals(
                        "the injected cleanup fault must cost exactly one refresh fault",
                        1L,
                        viewInstance().getRefreshFaultCount()
                );
                // The fault is recoverable and the recompute behind it is what recovers it.
                assertViewMatchesRecomputeIgnoringFaults();
            }
            // assertMemoryLeak is the oracle for the session itself: one nothing released leaves its
            // descriptor's Paths and its scratch overlay allocated, which no assertion above sees.
        });
    }

    @Test
    public void testAnAnchorResumeWhoseTailCleanupFaultsReleasesItsRepairSession() throws Exception {
        // The second of the anchor resume's two cleanup chains: the publication tail frees the
        // staged capture and then ends the repair, and a throw from the free used to take the
        // release with it. Same executor, same unrecoverable session, one statement further on.
        //
        // Same fixture as the replay-cleanup case, with the fault armed one chain later so it lands
        // on the tail rather than on the replay cleanup ahead of it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                // Let the resume's replay cleanup through, and fault the tail behind it.
                job.setSimulateRepairCleanupFaultForTest(1);
                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertFalse(
                        "the injected cleanup fault never fired, so the resume's publication tail"
                                + " never ran and the case pinned nothing",
                        job.isRepairCleanupFaultArmedForTest()
                );
                Assert.assertEquals(
                        "the injected cleanup fault must cost exactly one refresh fault",
                        1L,
                        viewInstance().getRefreshFaultCount()
                );
                assertViewMatchesRecomputeIgnoringFaults();
            }
        });
    }

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
    public void testAColdKeyedHeadMissWhosePrologueCleanupFaultsReleasesItsRepairSession() throws Exception {
        // The head-miss executor's third cleanup chain, and the only one that runs when the replay
        // never started: the prologue's own finally, gated on replayEntered. It closes the stored-row
        // cursor's pooled reader, drops the keyed merge state and frees the staged capture before it
        // ends the repair, and a throw from any of those used to take the release with it.
        //
        // Nothing else would then free the session. The park that attaches a session to its instance
        // lives inside the replay this arm proves never started, so handleRefreshFailure's
        // discardSuspendedRepair finds nothing, and the descriptor mapping and the three Paths the
        // session holds are lost for the life of the process.
        //
        // The prologue fault is the natural one this suite already drives - the row-position rebase
        // raising over the pinned generation's checkpoint metadata - so the arm is reached for real.
        // The second fault, on the cleanup chain itself, is injected: no reproducible natural
        // producer was found for it. What the case pins is the ordering, which holds for any
        // throwable the three statements ahead of the release raise.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();

                // The prologue throws, and the chain its unwind runs is the next one this worker
                // reaches - so the cleanup fault lands on that chain rather than on a replay's.
                job.setSimulateColdKeyedTimelineFaultForTest(true);
                job.setSimulateRepairCleanupFaultForTest(0);
                commit(row(3, 2, 35, "acct-1"), job);

                Assert.assertFalse(
                        "the injected cleanup fault never fired, so the prologue's unwind never"
                                + " reached the cleanup and the case pinned nothing",
                        job.isRepairCleanupFaultArmedForTest()
                );
                Assert.assertNull(
                        "a repair whose prologue faulted must leave nothing parked on the view",
                        instance.getSuspendedRepair()
                );
                Assert.assertEquals(
                        "the injected faults must cost exactly one refresh fault between them",
                        1,
                        instance.getRefreshFaultCount()
                );
                Assert.assertEquals(
                        "the cleanup ahead of the release must still return the stored-row cursor's"
                                + " pooled reader",
                        0,
                        engine.getBusyReaderCount()
                );

                // Both faults are one-shot, so the retry behind them repairs the view for real.
                assertViewMatchesRecomputeIgnoringFaults();
                Assert.assertEquals(
                        "the retry must leave no reader behind either",
                        0,
                        engine.getBusyReaderCount()
                );
            }
            // assertMemoryLeak is the oracle for the session itself: one nothing released leaves its
            // descriptor's Paths allocated, which no assertion above can see.
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
    public void testAColdKeyedHeadMissParksOnItsReplayBudget() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();
                final long resumesBefore = instance.getCheckpointRepairResumes();

                commit(row(3, 2, 35, "acct-1"), job);

                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertTrue(
                        "a cold keyed repair must honor the configured replay budget",
                        instance.getCheckpointRepairResumes() > resumesBefore
                );
                Assert.assertTrue(
                        "the repair must have handed its keys back",
                        job.transplantedKeyCountForTest() > 0
                );
                assertViewMatchesRecompute();

                // In order, above everything the view holds, so this is the plain forward
                // drain folding onto whatever accumulators the repair left the primary
                // standing on. A key the correction touched carries a wrong cumulative sum
                // from here on if those are the stale ones.
                commit(row(3, 10, 0, "acct-1") + ", " + row(3, 10, 30, "acct-2"), job);

                Assert.assertTrue(
                        "a forward commit repairs nothing, so the state it folds onto is the"
                                + " state the cold keyed repair left behind",
                        instance.getCheckpointRepairResumes() > resumesBefore
                );
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testAColdKeyedHeadMissParksAfterTheBaseIndexIsDropped() throws Exception {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_REPLAY_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();

                execute("ALTER TABLE tx ALTER COLUMN account_id DROP INDEX");
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertTrue(
                        "the primary plan must still carry the pre-drop compile, or the two"
                                + " compiles cannot diverge and this test proves nothing",
                        instance.getCompiledPlan().getPageFrameFactory()
                                .isIndexedForwardTimestampRangeSupported(BASE_KEY_COLUMN_INDEX)
                );

                job.setForceOpenSegmentKeyedReplayForTest(true);
                final long resumesBefore = instance.getCheckpointRepairResumes();

                commit(row(3, 2, 35, "acct-1"), job);

                Assert.assertTrue(
                        "a degraded cold keyed repair must honor the configured replay budget",
                        instance.getCheckpointRepairResumes() > resumesBefore
                );
                Assert.assertTrue(
                        "the cold keyed route must have been taken, or this drives nothing:"
                                + " the transplant is gated on it",
                        job.transplantedKeyCountForTest() > 0
                );
                Assert.assertEquals(
                        "the replay must have declined the indexed substitution, which is what"
                                + " leaves the cold route standing with the keyed one cleared",
                        0,
                        job.openSegmentColdKeyedReplayCountForTest()
                );
                assertViewMatchesRecompute();

                // In order, above everything the view holds, so this is the plain forward
                // drain folding onto whatever accumulators the repair left the primary
                // standing on.
                commit(row(3, 10, 0, "acct-1") + ", " + row(3, 10, 30, "acct-2"), job);

                Assert.assertTrue(
                        "a forward commit repairs nothing, so the state it folds onto is the"
                                + " state the cold keyed repair left behind",
                        instance.getCheckpointRepairResumes() > resumesBefore
                );
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

    @Test
    public void testASaturatedKeyedEstimateDeniesTheRestoreAwareOverride() throws Exception {
        // The row verdict survives a budgeted estimate that stopped early - the merge charges
        // at least one row per posting row, so a count that reached wholeRangeRows answers
        // "not cheaper" whatever the uncounted keys and partitions hold. The restore-aware
        // override does not: it prices the keyed side against an elapsed model whose other
        // term is a state restore, and wholeRangeRows bounds only the whole side. Its whole
        // population is !rowCheaper, which is exactly where a stopped count lands, so without
        // a guard the route reads a floor as a total precisely when the real posting count is
        // furthest above it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job, HOT_ACCOUNT_ROWS_PER_HOUR);
                seedRestoreDominantRates();

                commit(row(4, 2, 35, "acct-1"), job);

                Assert.assertEquals(1, job.openSegmentKeyedPricedCountForTest());
                Assert.assertTrue(
                        "the fixture must saturate: the estimate has to reach the whole-range"
                                + " count it was budgeted at",
                        job.openSegmentKeyedPostingRowsForTest()
                                >= job.openSegmentKeyedWholeRangeRowsForTest()
                );
                Assert.assertEquals(
                        "a saturated count can never read as the cheaper row verdict",
                        0,
                        job.openSegmentKeyedCheaperCountForTest()
                );
                // A restore-aware count of 0 is what the override reports for ANY of its
                // reasons, so on its own it does not say the saturation guard is the one
                // that declined. The elapsed estimates the same pricing pass recorded are
                // what pin the rest of that predicate. They are its own figures: the pass
                // runs once here - one non-cold priced count - and a cold pass never calls
                // the elapsed model at all. Under the seeded rates the two can only sit
                // this far apart with a positive selected-root byte count priced cold,
                // because the restore term is the only one carrying the whole side: both
                // scan terms are one nanosecond per row, so a warm or byte-free whole side
                // would price at the row count the saturated keyed side has already passed.
                // A factor of two is stricter than the margin the override itself needs -
                // a 150% keyed upper bound under an 85% hysteresis floor is a factor of
                // about 1.77 - so clearing it means the model preferred the keyed side and
                // the saturation guard is what is left to deny the route.
                final LiveViewCheckpointOpenSegmentCost elapsedCost =
                        viewInstance().getOpenSegmentRepairCost();
                Assert.assertTrue(
                        "the elapsed model must prefer the keyed side, or the case cannot"
                                + " tell the guard from the model declining: keyed="
                                + elapsedCost.getLastKeyedEstimateNanos() + "ns whole="
                                + elapsedCost.getLastWholeEstimateNanos() + "ns",
                        elapsedCost.getLastKeyedEstimateNanos()
                                < elapsedCost.getLastWholeEstimateNanos() / 2
                );
                Assert.assertEquals(
                        "and restore-dominant rates must not turn that floor into a route",
                        0,
                        job.openSegmentRestoreAwareCheaperCountForTest()
                );
                Assert.assertEquals(0, job.openSegmentKeyedResumeCountForTest());
                // The resume the declined override leaves reads the whole range off a
                // restored root rather than off the live window state, which is the same
                // reading the sibling case that lets the override through takes of its own
                // drive - the one this fixture repeats with two hot hours added.
                Assert.assertEquals(0, job.runtimeAnchorReuseCountForTest());
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
        openTheDayAboveARoot(job, 0);
    }

    /**
     * The same forward drive, with two of its hours carrying {@code hotRowsPerHour} extra
     * rows of one account. A key holding almost every row of the partition the replay
     * interval starts inside is what stops a budgeted estimate early: the keyed side counts
     * that partition's postings whole while the whole-range side counts only the slice above
     * the anchor, so the keyed count passes the budget inside the first partition or two and
     * every partition above them goes uncounted. Zero leaves the plain drive above.
     * <p>
     * The hot rows sit in the hour's first minute, below the four hourly rows, so every
     * commit is still in timestamp order and none of them is itself a correction.
     */
    private void openTheDayAboveARoot(LiveViewRefreshJob job, int hotRowsPerHour) throws Exception {
        for (int hour = 0; hour < 10; hour++) {
            final StringBuilder rows = new StringBuilder();
            if (hour == 1 || hour == 2) {
                for (int second = 0; second < hotRowsPerHour; second++) {
                    if (rows.length() > 0) {
                        rows.append(", ");
                    }
                    rows.append(hotRow(hour, second));
                }
            }
            for (int account = 1; account <= 4; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(4, hour, account * 10, "acct-" + account));
            }
            commit(rows.toString(), job);
        }
    }

    /**
     * One acct-1 row at 2026-01-04 {@code hour}:00:{@code second}, as an INSERT tuple.
     */
    private String hotRow(int hour, int second) {
        return "('2026-01-04T" + String.format("%02d", hour) + ":00:"
                + String.format("%02d", second) + ".000000Z', 'acct-1', 1.0)";
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
