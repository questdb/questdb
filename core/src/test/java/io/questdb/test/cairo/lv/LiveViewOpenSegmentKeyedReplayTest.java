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
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointOpenSegmentCost;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

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
    // The account column's symbol capacity in the cases that correct from a few thousand to
    // tens of thousands of accounts at once: above every account count they insert, so no
    // commit grows it.
    private static final int WIDE_CORRECTION_SYMBOL_CAPACITY = 65_536;
    // Accumulator components of the wide fused view: with the anchor value beside them they
    // fill a leaf entry's whole inline budget, so its fused payload is as wide as any gets.
    private static final int WIDE_PAYLOAD_COMPONENTS =
            (LiveViewCheckpointContracts.MAX_INLINE_LEAF_STATE_BYTES - Long.BYTES) / (Double.BYTES + Long.BYTES);
    private static final int WIDE_PAYLOAD_BYTES = Long.BYTES + WIDE_PAYLOAD_COMPONENTS * (Double.BYTES + Long.BYTES);
    /**
     * Heap bytes the window around a worker's second transplant may allocate. It spans the
     * transplant and the tails of the operations on either side of it, none of which scale
     * with the keys the transplant hands back; one payload imaged into a heap array per key
     * costs this by the sixteenth key.
     */
    private static final long TRANSPLANT_WINDOW_HEAP_LIMIT_BYTES = 4_096;

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
    public void testAColdKeyedHeadMissWhoseTransplantFaultsRebuildsBeforeSealingAndSurvivesARestart() throws Exception {
        // The cold keyed route replays the corrected keys in an isolated runtime and hands
        // their accumulators back to the primary just before the head seal images it. A
        // hand-back that throws - a refresh memory limit reached over the primary's map -
        // leaves the durable output correct and the primary holding stale accumulators for
        // exactly the corrected keys. Marking the runtime dirty and carrying on used to seal
        // the head over that runtime: a restart then restored it as clean, the dirty mark
        // gone with the process, and every later row on those keys extended the stale total.
        //
        // The failure now unwinds the turn before the seal, and the refresh's own failure
        // path rebuilds the window state from the applied base. What the case pins is the
        // restart: no base commit runs between the fault and the restore, so the only thing
        // standing between the restored runtime and the stale one is what the turn sealed.
        // The splice is switched off for the same reason: with it, the restart could restore
        // off the re-versioned roots the replay froze and never touch the head seal at all.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, 0);
        try {
            assertMemoryLeak(() -> {
                createView(seedFourAccountsOverTwoDays(), true);
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    final LiveViewInstance instance = viewInstance();

                    // Same correction as the cold keyed route's own case, so the replay runs
                    // beside the primary and owes it a hand-back. One refresh turn only, the way
                    // a worker would run it: driving to quiescence would let a later tick read
                    // the dirty mark and rebuild, which is the recovery a restart never sees.
                    job.setSimulateKeyedTransplantFaultForTest(true);
                    execute("insert into tx values " + row(3, 2, 35, "acct-1"));
                    drainWalQueue();
                    setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                    Assert.assertTrue("the turn must find the correction", job.run());
                    Assert.assertFalse(
                            "the injected transplant fault never fired, so the case pinned nothing",
                            job.isKeyedTransplantFaultArmedForTest()
                    );
                    // The fault fires after the freeze, so the transplant unwound holding the
                    // payloads it froze. They stay for the next transplant to clear, and the
                    // worker's close frees them: the leak check below is the witness.
                    Assert.assertTrue(
                            "the faulted transplant must have frozen its payloads before it threw",
                            job.getTransplantPayloadCountForTest() > 0
                    );
                    Assert.assertEquals(
                            "the failed hand-back must cost exactly one refresh fault",
                            1,
                            instance.getRefreshFaultCount()
                    );
                    Assert.assertFalse(
                            "the rebuild the fault forces must have paid the debt within the turn",
                            instance.isWindowStateDirty()
                    );
                }

                // The restart, with nothing committed in between: whatever the faulted turn
                // sealed is what the restore comes back on.
                restartCycle();
                try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                    driveRefreshToQuiescence(job);
                    Assert.assertFalse("the restored view must not come back dirty", viewInstance().isWindowStateDirty());
                    // Forward rows on both the corrected key and an untouched one: each extends
                    // the running total the restored runtime holds for its account.
                    commit(row(3, 12, 30, "acct-1"), job);
                    commit(row(3, 12, 31, "acct-2"), job);
                    assertViewMatchesRecomputeIgnoringFaults();
                }
            });
        } finally {
            setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_MAX_CHAINED_BOUNDARIES, (String) null);
        }
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
    public void testAColdKeyedHeadMissWhosePrologueCannotCloseItsStoredRowsFreesItsCapture() throws Exception {
        // The prologue's own unwind closes the stored-row cursor, drops the keyed merge state and
        // frees the staged capture. Closing the cursor hands a pooled reader of the view's own
        // table back, and a reader the table outgrew while it was held reloads the txn file as it
        // goes passive, which can fail to remap. That throw used to skip the capture free. On a
        // first turn nothing else owns the capture: the session takes it only when a park hands it
        // over, and the park sits inside the replay this unwind proves never started. The process
        // then lost the capture's Paths, its copy of Q and its staged data segment for good.
        //
        // The prologue fault is the one this suite already drives. The cursor's close fails on the
        // real remap of a reader the view's table outgrew; StoredRowCloseFault says how the case
        // gets such a reader into the cursor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        final StoredRowCloseFault fault = new StoredRowCloseFault();
        assertMemoryLeak(fault, () -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();

                fault.holdReaderOf(instance);
                try {
                    commit(laterHoursOfTheSeedsOpenDay(), job);
                    fault.installOn(instance);
                    // Same correction as the cold keyed route's own case: the prologue opens the
                    // stored-row merge, and then throws.
                    job.setSimulateColdKeyedTimelineFaultForTest(true);
                    commit(row(3, 2, 35, "acct-1"), job);
                } finally {
                    fault.returnHeldReader();
                }

                fault.assertCloseFailedOnTheRemap();
                Assert.assertNull(
                        "a repair whose prologue faulted must leave nothing parked on the view",
                        instance.getSuspendedRepair()
                );
                Assert.assertEquals(
                        "the two faults must cost exactly one refresh fault between them",
                        1,
                        instance.getRefreshFaultCount()
                );
                Assert.assertEquals("no reader may stay borrowed", 0, engine.getBusyReaderCount());

                // Both faults are one-shot, so the retry behind them repairs the view for real.
                assertViewMatchesRecomputeIgnoringFaults();
                Assert.assertEquals("the retry must leave no reader behind either", 0, engine.getBusyReaderCount());
            }
            // assertMemoryLeak is the oracle for the capture: one nothing freed leaves its Paths and
            // its copy of Q allocated, which no assertion above can see.
        });
    }

    @Test
    public void testAColdKeyedHeadMissWhoseStoredRowsFailToCloseFreesItsCapture() throws Exception {
        // The replay's own unwind closes the stored-row cursor first and frees the staged capture
        // further down. A replay that ran to the end frees the capture later still, in the
        // publication tail. Closing the cursor hands a pooled reader of the view's table back, and
        // a reader close can fail for any remap or I/O reason; this case injects one. That throw
        // used to leave the unwind before the capture free and the pinned base reader's return,
        // and the tail behind it never ran: the process lost the capture and the base reader.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        final StoredRowCloseFault fault = new StoredRowCloseFault();
        assertMemoryLeak(fault, () -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();

                fault.holdReaderOf(instance);
                try {
                    commit(laterHoursOfTheSeedsOpenDay(), job);
                    fault.installOn(instance);
                    commit(row(3, 2, 35, "acct-1"), job);
                } finally {
                    fault.returnHeldReader();
                }

                fault.assertCloseFailedOnTheRemap();
                Assert.assertTrue(
                        "the correction must have taken the cold keyed route",
                        job.openSegmentColdKeyedReplayCountForTest() > 0
                );
                Assert.assertNull(
                        "a repair whose cleanup faulted must leave nothing parked on the view",
                        instance.getSuspendedRepair()
                );
                Assert.assertEquals(
                        "the failed close must cost exactly one refresh fault",
                        1,
                        instance.getRefreshFaultCount()
                );
                Assert.assertEquals(
                        "the pinned base reader must be back in the pool",
                        0,
                        engine.getBusyReaderCount()
                );

                assertViewMatchesRecomputeIgnoringFaults();
                Assert.assertEquals("the retry must leave no reader behind either", 0, engine.getBusyReaderCount());
            }
            // assertMemoryLeak is the oracle for the capture.
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
    public void testAKeyedResumeWhoseStoredRowsFailToCloseFreesItsCapture() throws Exception {
        // The anchor resume's cleanup closes the stored-row cursor ahead of re-attaching the pinned
        // base reader and, on an unwinding turn, freeing the staged capture. A resume that ran to
        // the end frees the capture in the publication tail instead. Closing the cursor hands a
        // pooled reader of the view's table back, and a reader close can fail for any remap or I/O
        // reason; this case injects one. That throw used to skip the rest of the cleanup and the
        // whole tail: this executor never parks, so nothing else in the process freed the capture,
        // and the base reader stayed detached from its pool.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        final StoredRowCloseFault fault = new StoredRowCloseFault();
        assertMemoryLeak(fault, () -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                final LiveViewInstance instance = viewInstance();

                fault.holdReaderOf(instance);
                try {
                    // Ten commits, each an hour partition of the view's table the held reader
                    // never saw.
                    openTheDayAboveARoot(job);
                    fault.installOn(instance);
                    commit(row(4, 2, 35, "acct-1"), job);
                } finally {
                    fault.returnHeldReader();
                }

                fault.assertCloseFailedOnTheRemap();
                Assert.assertTrue(
                        "the correction must have been resumed by key",
                        job.openSegmentKeyedResumeCountForTest() > 0
                );
                Assert.assertEquals(
                        "the failed close must cost exactly one refresh fault",
                        1,
                        instance.getRefreshFaultCount()
                );
                Assert.assertEquals(
                        "the pinned base reader must be back in the pool",
                        0,
                        engine.getBusyReaderCount()
                );

                assertViewMatchesRecomputeIgnoringFaults();
                Assert.assertEquals("the retry must leave no reader behind either", 0, engine.getBusyReaderCount());
            }
            // assertMemoryLeak is the oracle for the capture.
        });
    }

    @Test
    public void testASecondKeyedResumeTransplantsThroughTheFirstOnesArenas() throws Exception {
        // The transplant freezes the isolated runtime's keys through the contract a seal
        // freezes the primary's with: each key goes into the worker's native key arena and each
        // payload into its native payload arena, whose memory it clears rather than frees
        // between repairs. A worker's second resume over the same key therefore freezes its
        // keys and payloads into the same memory its first one left behind, so it allocates
        // nothing per key, on the heap or off it.
        //
        // The allocator may hand a freed block straight back, so an unchanged key address
        // cannot tell a kept arena from one freed and allocated again. The witness counts the
        // native allocations around the second transplant instead, and there must be none.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        final TransplantAllocationWitness witness = new TransplantAllocationWitness();
        assertMemoryLeak(witness, () -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);
                Assert.assertEquals(1, job.openSegmentKeyedResumeCountForTest());
                final long firstTransplantedKeys = job.transplantedKeyCountForTest();
                Assert.assertTrue(firstTransplantedKeys > 0);
                Assert.assertEquals(
                        "every key the transplant handed back must be frozen in its arena",
                        firstTransplantedKeys,
                        job.getTransplantKeyArenaKeyCountForTest()
                );
                final long firstKeyAddress = job.getTransplantKeyArenaAddressForTest();
                Assert.assertNotEquals(0, firstKeyAddress);
                Assert.assertEquals(
                        "every key the transplant handed back must have its payload in the payload arena",
                        firstTransplantedKeys,
                        job.getTransplantPayloadCountForTest()
                );
                final long firstPayloadAddress = job.getTransplantPayloadArenaAddressForTest();
                Assert.assertNotEquals(0, firstPayloadAddress);
                final long firstPayloadCapacity = job.getTransplantPayloadArenaCapacityForTest();
                assertViewMatchesRecompute();

                witness.watchNextTransplant(job);
                commit(row(4, 2, 36, "acct-1"), job);
                Assert.assertEquals(2, job.openSegmentKeyedResumeCountForTest());
                Assert.assertEquals(
                        "the second resume must hand back as many keys as the first",
                        2 * firstTransplantedKeys,
                        job.transplantedKeyCountForTest()
                );
                Assert.assertEquals(firstTransplantedKeys, job.getTransplantKeyArenaKeyCountForTest());
                Assert.assertEquals(
                        "the second transplant must reuse the native memory the first one kept, "
                                + "so nothing around it may allocate or grow any",
                        0,
                        witness.getAllocationsAroundTransplant()
                );
                Assert.assertEquals(
                        "the second transplant must freeze its keys into the memory the first one kept",
                        firstKeyAddress,
                        job.getTransplantKeyArenaAddressForTest()
                );
                Assert.assertEquals(firstTransplantedKeys, job.getTransplantPayloadCountForTest());
                Assert.assertEquals(
                        "the second transplant must freeze its payloads into the memory the first one kept",
                        firstPayloadAddress,
                        job.getTransplantPayloadArenaAddressForTest()
                );
                Assert.assertEquals(firstPayloadCapacity, job.getTransplantPayloadArenaCapacityForTest());
                assertViewMatchesRecompute();
            }
        });
    }

    @Test
    public void testATransplantPastTheKeyLimitFreesItsArenaAndShrinksItsHandleLists() throws Exception {
        // A worker keeps the transplant's key arena and handle lists for its lifetime and only
        // clears them between repairs, so a correction wider than the retention limit must
        // free the arena and shrink the lists back rather than park its key domain there.
        assertKeyedRepairFreesItsKeyArena(LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEYS + 1_024, 0);
    }

    @Test
    public void testATransplantOfWideKeysPastTheByteLimitFreesItsArena() throws Exception {
        // Far fewer keys than the key limit, each wider than a kilobyte, so only the byte
        // limit can tell that the arena grew past what a worker may keep.
        final int keyCount = 4_096;
        final int accountChars = 1_536;
        Assert.assertTrue(keyCount < LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEYS);
        Assert.assertTrue((long) keyCount * accountChars > LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEY_BYTES);
        assertKeyedRepairFreesItsKeyArena(keyCount, accountChars);
    }

    @Test
    public void testATransplantOfWidePayloadsPastTheByteLimitFreesItsPayloadArena() throws Exception {
        // A worker keeps the transplant's payload arena for its lifetime and only clears it
        // between repairs. Payloads as wide as the leaf budget allows pass the arena's byte
        // limit far inside the key limit, so only that limit can tell the arena grew past what
        // a worker may keep, and past it the arena must be freed rather than parked there.
        final int keyCount = (int) (LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_PAYLOAD_BYTES / WIDE_PAYLOAD_BYTES) + 1_024;
        Assert.assertTrue(keyCount < LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEYS);
        // The identity the sparse publication upserts on, which the keyed route requires.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            final String everyAccount = createWidePayloadView(keyCount);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                // Each correction below touches every key, see assertKeyedRepairFreesItsKeyArena.
                job.setForceOpenSegmentKeyedReplayForTest(true);
                for (int correction = 0; correction < 2; correction++) {
                    // Each below the one before it, so every repair replays cold.
                    execute("INSERT INTO tx SELECT '2026-01-02T00:" + (30 - 10 * correction)
                            + ":00.000000Z'::timestamp + x * 1_000, " + everyAccount);
                    drainWalQueue();
                    driveRefreshToQuiescence(job);
                    Assert.assertEquals(correction + 1, job.openSegmentColdKeyedReplayCountForTest());
                    Assert.assertEquals((correction + 1L) * keyCount, job.transplantedKeyCountForTest());
                    Assert.assertEquals(
                            "the view must fuse every component into one payload",
                            WIDE_PAYLOAD_BYTES,
                            viewInstance().getAnchorWindow().getCheckpointWindowStatePlan().getTotalInlineStateBytes()
                    );
                    Assert.assertEquals(
                            "a transplant past the payload byte limit must free its payload arena",
                            0,
                            job.getTransplantPayloadArenaCapacityForTest()
                    );
                    Assert.assertEquals("the handles must go with the arena they name", 0, job.getTransplantPayloadCountForTest());
                    Assert.assertEquals(0, transplantHandles(job, "transplantPayloads").size());
                    Assert.assertEquals(
                            "narrow keys within the key limits keep the key arena",
                            keyCount,
                            job.getTransplantKeyArenaKeyCountForTest()
                    );
                }
                assertNoRefreshFaults("lv");
            }
        });
    }

    @Test
    public void testASecondTransplantOfANarrowKeySetAllocatesNoHeap() throws Exception {
        assertSecondWideTransplantAllocatesNoHeap(1_024);
    }

    @Test
    public void testASecondTransplantOfWidePayloadsAllocatesNoHeapPerKey() throws Exception {
        // These payloads hold more bytes than a worker once kept pooled on the heap for the
        // transplant, so a transplant that imaged them into heap arrays handed its arrays back
        // after the first repair and allocated every one of them again at the second.
        assertSecondWideTransplantAllocatesNoHeap(
                (int) (LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_PAYLOAD_BYTES / WIDE_PAYLOAD_BYTES) + 1_024
        );
    }

    @Test
    public void testAKeyBudgetOfZeroStillResumesTheOpenSegmentByKey() throws Exception {
        // server.conf documents a key budget at or below zero as unlimited. The open
        // segment's domain is collected under the same budget as a closed segment's, so a
        // budget of zero must still collect it rather than leave the resume reading every
        // row above its anchor.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_KEYED_SCAN_INDEX_OPEN_ROWS, 1);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SCAN_MAX_KEYS, 0);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(seedFourAccountsOverTwoDays(), true);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                openTheDayAboveARoot(job);

                commit(row(4, 2, 35, "acct-1"), job);

                assertQuery("""
                        SELECT o3_open_segment_keyed_resume_count, o3_open_segment_cold_keyed_replay_count
                        FROM live_views()""")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                o3_open_segment_keyed_resume_count\to3_open_segment_cold_keyed_replay_count
                                1\t0
                                """);
                Assert.assertEquals(0, job.openSegmentKeyedUnpricedCountForTest());
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

    private static LongList transplantHandles(LiveViewRefreshJob job, String name) throws Exception {
        final Field field = LiveViewRefreshJob.class.getDeclaredField(name);
        field.setAccessible(true);
        return (LongList) field.get(job);
    }

    /**
     * Drives one keyed repair wide enough to pass a transplant retention limit, and checks
     * what the transplant left on the worker. The view holds one row of acct-0 and one of
     * each of {@code keyCount} other accounts in the open day, and a single correction below
     * every other account's row adds a row to each of those accounts at once. A correction
     * that touches every account but one is more to read by key than whole, so the pricing
     * declines the keyed route, and the test switch takes it anyway: the repair replays every
     * corrected account cold from the day's origin in the isolated runtime, and the transplant
     * hands each one back to the primary.
     *
     * @param accountChars how wide each account name is padded, or 0 to keep it narrow
     */
    private void assertKeyedRepairFreesItsKeyArena(int keyCount, int accountChars) throws Exception {
        // The identity the sparse publication upserts on, which the keyed route requires.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            createView(row(3, 0, 10, "acct-0"), true, WIDE_CORRECTION_SYMBOL_CAPACITY);
            final String everyAccount = (accountChars > 0
                    ? "rpad(concat('acct-', x), " + accountChars + ", 'k')"
                    : "concat('acct-', x)") + ", 1.0 FROM long_sequence(" + keyCount + ")";
            execute("INSERT INTO tx SELECT '2026-01-03T01:00:00.000000Z'::timestamp + x * 1_000_000, "
                    + everyAccount);
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
                job.setForceOpenSegmentKeyedReplayForTest(true);

                execute("INSERT INTO tx SELECT '2026-01-03T00:30:00.000000Z'::timestamp + x * 1_000, "
                        + everyAccount);
                drainWalQueue();
                driveRefreshToQuiescence(job);

                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(
                        "the repair must hand back every key the correction touched",
                        keyCount,
                        job.transplantedKeyCountForTest()
                );
                Assert.assertEquals(
                        "a transplant past the retention limits must free its key arena",
                        0,
                        job.getTransplantKeyArenaKeyCountForTest()
                );
                final LongList keys = transplantHandles(job, "transplantKeys");
                Assert.assertEquals("the handles must go with the arena they name", 0, keys.size());
                Assert.assertTrue(
                        "the handle list must shrink back, capacity=" + keys.capacity(),
                        keys.capacity() < keyCount
                );
                Assert.assertEquals(0, transplantHandles(job, "transplantRemovedKeys").size());
                final LongList values = transplantHandles(job, "transplantValues");
                Assert.assertTrue(
                        "the anchor value list must shrink back, capacity=" + values.capacity(),
                        values.capacity() < keyCount
                );
                if (keyCount > LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEYS) {
                    // The payloads here are narrow - an anchor value and one sum - so their
                    // bytes stay far inside the payload byte limit, and only the count limit
                    // can free the arena that holds them.
                    Assert.assertTrue(
                            (long) keyCount * 32 < LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_PAYLOAD_BYTES
                    );
                    Assert.assertEquals(
                            "a transplant past the key limit must free its payload arena",
                            0,
                            job.getTransplantPayloadArenaCapacityForTest()
                    );
                    final LongList payloads = transplantHandles(job, "transplantPayloads");
                    Assert.assertEquals("the payload handles must go with the arena they name", 0, payloads.size());
                    Assert.assertTrue(
                            "the payload handle list must shrink back, capacity=" + payloads.capacity(),
                            payloads.capacity() < keyCount
                    );
                } else {
                    Assert.assertEquals(
                            "narrow payloads within the payload limits keep the payload arena",
                            keyCount,
                            job.getTransplantPayloadCountForTest()
                    );
                }
                assertViewMatchesRecompute();
            }
        });
    }

    /**
     * Drives two cold keyed repairs of a view whose fused payloads are as wide as a leaf
     * allows, each correcting every one of {@code keyCount} accounts, and checks the heap the
     * window around the second transplant allocated. The first repair warms whatever the
     * worker keeps for its transplants; the second one measures what a warm one costs.
     */
    private void assertSecondWideTransplantAllocatesNoHeap(int keyCount) throws Exception {
        Assert.assertTrue(keyCount < LiveViewCheckpointTimelineStoreWriter.MAX_RETAINED_FROZEN_KEYS);
        // The identity the sparse publication upserts on, which the keyed route requires.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_REPAIR_SPARSE_PUBLICATION_ENABLED, "true");
        final TransplantAllocationWitness witness = new TransplantAllocationWitness();
        assertMemoryLeak(witness, () -> {
            final String everyAccount = createWidePayloadView(keyCount);
            try (
                    LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1);
                    TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()
            ) {
                driveRefreshToQuiescence(job);
                job.setForceOpenSegmentKeyedReplayForTest(true);

                execute("INSERT INTO tx SELECT '2026-01-02T00:30:00.000000Z'::timestamp + x * 1_000, "
                        + everyAccount);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(1, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(keyCount, job.transplantedKeyCountForTest());

                // Below the first correction, so the second repair replays cold as well.
                witness.watchNextTransplant(job, scope.getBean());
                execute("INSERT INTO tx SELECT '2026-01-02T00:20:00.000000Z'::timestamp + x * 1_000, "
                        + everyAccount);
                drainWalQueue();
                driveRefreshToQuiescence(job);
                Assert.assertEquals(2, job.openSegmentColdKeyedReplayCountForTest());
                Assert.assertEquals(2L * keyCount, job.transplantedKeyCountForTest());
                final long heap = witness.getHeapAroundTransplant();
                Assert.assertTrue(
                        "a warm transplant of " + keyCount + " keys with " + WIDE_PAYLOAD_BYTES
                                + "-byte payloads allocated " + heap + " bytes on the Java heap;"
                                + " a payload must not be imaged into a heap array",
                        heap < TRANSPLANT_WINDOW_HEAP_LIMIT_BYTES
                );
                assertNoRefreshFaults("lv");
            }
        });
    }

    /**
     * Creates {@code tx} and {@code lv} with as many sums as fill a leaf entry's inline
     * budget, fused into one payload per key, and seeds one row of each of
     * {@code keyCount} accounts in the open day.
     *
     * @return the tail of an INSERT ... SELECT that adds one row to every account
     */
    private String createWidePayloadView(int keyCount) throws Exception {
        final StringBuilder columns = new StringBuilder();
        final StringBuilder projections = new StringBuilder();
        final StringBuilder values = new StringBuilder();
        for (int i = 1; i <= WIDE_PAYLOAD_COMPONENTS; i++) {
            columns.append(", q").append(i).append(" DOUBLE");
            projections.append(", sum(q").append(i).append(") OVER w AS s").append(i);
            values.append(", x::double");
        }
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL CAPACITY "
                + WIDE_CORRECTION_SYMBOL_CAPACITY + " INDEX" + columns + ") "
                + "TIMESTAMP(created_at) PARTITION BY HOUR WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS "
                + "SELECT created_at, account_id" + projections + " FROM tx "
                + "WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')");
        final String everyAccount = "concat('acct-', x)" + values + " FROM long_sequence(" + keyCount + ")";
        execute("INSERT INTO tx SELECT '2026-01-02T01:00:00.000000Z'::timestamp + x * 1_000_000, "
                + everyAccount);
        drainWalQueue();
        return everyAccount;
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
        createView(seedRows, isKeyIndexed, 0);
    }

    /**
     * The same view over a base whose account column starts at {@code symbolCapacity}, or at
     * the default capacity when it is 0. A capacity above every account a case inserts keeps
     * the base from growing it on a later commit, which is a metadata change the view
     * recompiles for.
     */
    private void createView(String seedRows, boolean isKeyIndexed, int symbolCapacity) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL"
                + (symbolCapacity > 0 ? " CAPACITY " + symbolCapacity : "") + " NOCACHE"
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

    /**
     * Ten more hours of 2026-01-03, the day {@link #seedFourAccountsOverTwoDays} leaves open,
     * with one row of each account per hour. Each hour is a partition of the view's own table
     * that did not exist before, and all of them sit above every correction on that day the
     * cases make, so the day stays the open segment and the cold keyed route stays available.
     */
    private String laterHoursOfTheSeedsOpenDay() {
        final StringBuilder rows = new StringBuilder();
        for (int hour = 10; hour < 20; hour++) {
            for (int account = 1; account <= 4; account++) {
                if (rows.length() > 0) {
                    rows.append(", ");
                }
                rows.append(row(3, hour, account * 10, "acct-" + account));
            }
        }
        return rows.toString();
    }

    /**
     * The stored-row cursor a keyed repair opens next, closing the reader the fault holds once
     * the real cursor has closed. Everything else reads through the real cursor, so the repair
     * merges exactly the rows it would have merged anyway.
     */
    private static final class StaleReaderClosingCursor implements RecordCursor {
        private final RecordCursor delegate;
        private final StoredRowCloseFault fault;

        private StaleReaderClosingCursor(RecordCursor delegate, StoredRowCloseFault fault) {
            this.delegate = delegate;
            this.fault = fault;
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } finally {
                fault.closeHeldReader();
            }
        }

        @Override
        public Record getRecord() {
            return delegate.getRecord();
        }

        @Override
        public Record getRecordB() {
            return delegate.getRecordB();
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return delegate.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return delegate.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            delegate.recordAt(record, atRowId);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }
    }

    /**
     * The view's ascending full scan, built exactly as the refresh job builds its own, except
     * that the next stored-row cursor it opens also lets go of the fault's held reader when it
     * closes. {@code LiveViewInstance.setStoredRowScanFactory} is how the case swaps it in; the
     * job keeps using any cached scan that is a {@link PageFrameRecordCursorFactory}.
     */
    private static final class StaleReaderStoredRowFactory extends PageFrameRecordCursorFactory {
        private final StoredRowCloseFault fault;

        private StaleReaderStoredRowFactory(
                CairoConfiguration configuration,
                RecordMetadata metadata,
                PartitionFrameCursorFactory partitionFrameCursorFactory,
                IntList columnIndexes,
                IntList columnSizeShifts,
                StoredRowCloseFault fault
        ) {
            super(
                    configuration,
                    metadata,
                    partitionFrameCursorFactory,
                    new PageFrameRowCursorFactory(PartitionFrameCursorFactory.ORDER_ASC),
                    false,
                    null,
                    true,
                    columnIndexes,
                    columnSizeShifts,
                    true,
                    false
            );
            this.fault = fault;
        }

        @Override
        public RecordCursor getCursorInTimestampRange(
                SqlExecutionContext executionContext,
                long timestampLo,
                long timestampHi
        ) throws SqlException {
            return fault.wrapNextStoredRowCursor(super.getCursorInTimestampRange(executionContext, timestampLo, timestampHi));
        }
    }

    /**
     * Makes the close of the next stored-row cursor a keyed repair opens fail the way a pooled
     * reader of the view's own table fails to go passive. A cursor that held such a reader while
     * the table committed releases a txn the table has moved past, so going passive reloads the
     * txn file and extends its mapping to the table's grown partition list - and this facade
     * fails that remap. TableReader.goPassive raises the CairoException from TableUtils.mremap,
     * exactly as a map-count or address-space limit would.
     * <p>
     * Nothing outside the turn can time a remap fault onto the reader inside that cursor, and
     * nothing inside it moves the view's table while the cursor is open: the repair's commits
     * only write the view's WAL, which the refresh job applies after these closes. So the case
     * holds a reader of its own from before the table grew, and the cursor lets go of it once the
     * real cursor has closed. The real cursor's reader goes back to its pool untouched, which
     * keeps the leak check about the repair: the held reader, stranded by the failed close the way
     * the cursor's own would be, goes back through {@link #returnHeldReader()}, as a second close
     * returns it.
     */
    static final class StoredRowCloseFault extends TestFilesFacadeImpl {
        private Throwable closeFailure;
        private TableReader heldReader;
        private boolean isCursorClosed;
        private boolean isHeldReaderReturned;
        private boolean isRemapFaultArmed;
        private boolean isWrapped;
        private int remapFaultCount;

        @Override
        public long mremap(long fd, long addr, long previousSize, long newSize, long offset, int mode, int memoryTag) {
            if (isRemapFaultArmed) {
                isRemapFaultArmed = false;
                remapFaultCount++;
                return FilesFacade.MAP_FAILED;
            }
            return super.mremap(fd, addr, previousSize, newSize, offset, mode, memoryTag);
        }

        void assertCloseFailedOnTheRemap() {
            Assert.assertTrue("the repair never closed the stored-row cursor the case handed it", isCursorClosed);
            Assert.assertEquals("the held reader's close must have reached the remap exactly once", 1, remapFaultCount);
            Assert.assertTrue(
                    "the stored rows' close must have failed with the remap's own error, not " + closeFailure,
                    closeFailure instanceof CairoException e
                            && Chars.contains(e.getFlyweightMessage(), "could not remap file")
            );
        }

        void holdReaderOf(LiveViewInstance instance) {
            heldReader = engine.getReader(instance.getLiveViewToken());
        }

        void installOn(LiveViewInstance instance) {
            final StaleReaderStoredRowFactory factory;
            try (TableReader lvReader = engine.getReader(instance.getLiveViewToken())) {
                final TableReaderMetadata metadata = lvReader.getMetadata();
                final IntList columnIndexes = new IntList();
                final IntList columnSizeShifts = new IntList();
                for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                    columnIndexes.add(i);
                    columnSizeShifts.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
                }
                factory = new StaleReaderStoredRowFactory(
                        engine.getConfiguration(),
                        GenericRecordMetadata.copyOfNew(metadata),
                        new FullPartitionFrameCursorFactory(
                                instance.getLiveViewToken(),
                                metadata.getMetadataVersion(),
                                GenericRecordMetadata.copyOfNew(metadata),
                                PartitionFrameCursorFactory.ORDER_ASC,
                                null,
                                0,
                                false
                        ),
                        columnIndexes,
                        columnSizeShifts,
                        this
                );
            }
            instance.setStoredRowScanFactory(factory);
        }

        void returnHeldReader() {
            if (heldReader != null && !isHeldReaderReturned) {
                isHeldReaderReturned = true;
                heldReader.close();
            }
        }

        private void closeHeldReader() {
            if (isCursorClosed) {
                return;
            }
            isCursorClosed = true;
            isRemapFaultArmed = true;
            try {
                heldReader.close();
                isHeldReaderReturned = true;
            } catch (Throwable t) {
                closeFailure = t;
                throw t;
            } finally {
                isRemapFaultArmed = false;
            }
        }

        private RecordCursor wrapNextStoredRowCursor(RecordCursor cursor) {
            if (isWrapped) {
                return cursor;
            }
            isWrapped = true;
            return new StaleReaderClosingCursor(cursor, this);
        }
    }

    /**
     * Counts the native allocations and reallocations between the two file operations that
     * bracket a worker's next transplant: the last one before it hands its keys back and the
     * first one after. The job's running count of handed-back keys, which the transplant
     * bumps as its last step, tells the two apart. The window spans the whole transplant
     * because the transplant performs no file operation of its own, so both bracketing
     * operations lie outside it. A file operation added inside the transplant would move the
     * window's start up to it and hide every allocation the transplant makes before it. The
     * counters are process wide: the refresh runs on the test's own thread, and no other
     * thread of the test allocates native memory while it does.
     */
    private static final class TransplantAllocationWitness extends TestFilesFacadeImpl {
        private long allocationsAfter = -1;
        private long allocationsBefore = -1;
        // The heap half of the same window, sampled only on the thread that watches it: the
        // counter is per thread, and the refresh runs on the test's own.
        private com.sun.management.ThreadMXBean heapBean;
        private long heapAfter = -1;
        private long heapBefore = -1;
        private Thread heapThread;
        private LiveViewRefreshJob job;
        private long transplantedKeysBefore;

        @Override
        public boolean close(long fd) {
            observe();
            return super.close(fd);
        }

        @Override
        public long mmap(long fd, long len, long offset, int flags, int memoryTag) {
            observe();
            return super.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public void munmap(long address, long size, int memoryTag) {
            observe();
            super.munmap(address, size, memoryTag);
        }

        @Override
        public long openRO(LPSZ name) {
            observe();
            return super.openRO(name);
        }

        @Override
        public long openRW(LPSZ name, int opts) {
            observe();
            return super.openRW(name, opts);
        }

        long getAllocationsAroundTransplant() {
            Assert.assertNotEquals("no file operation preceded the transplant", -1, allocationsBefore);
            Assert.assertNotEquals("no file operation followed the transplant", -1, allocationsAfter);
            return allocationsAfter - allocationsBefore;
        }

        long getHeapAroundTransplant() {
            Assert.assertNotEquals("no file operation preceded the transplant", -1, heapBefore);
            Assert.assertNotEquals("no file operation followed the transplant", -1, heapAfter);
            return heapAfter - heapBefore;
        }

        void watchNextTransplant(LiveViewRefreshJob job) {
            this.job = job;
            transplantedKeysBefore = job.transplantedKeyCountForTest();
            allocationsBefore = -1;
            allocationsAfter = -1;
            heapBefore = -1;
            heapAfter = -1;
        }

        void watchNextTransplant(LiveViewRefreshJob job, com.sun.management.ThreadMXBean heapBean) {
            watchNextTransplant(job);
            this.heapBean = heapBean;
            heapThread = Thread.currentThread();
        }

        private void observe() {
            if (job != null && allocationsAfter == -1) {
                final long allocations = Unsafe.getMallocCount() + Unsafe.getReallocCount();
                final long heap = heapBean != null && Thread.currentThread() == heapThread
                        ? heapBean.getCurrentThreadAllocatedBytes()
                        : -1;
                if (job.transplantedKeyCountForTest() == transplantedKeysBefore) {
                    allocationsBefore = allocations;
                    if (heap != -1) {
                        heapBefore = heap;
                    }
                } else {
                    allocationsAfter = allocations;
                    if (heap != -1) {
                        heapAfter = heap;
                    }
                }
            }
        }
    }
}
