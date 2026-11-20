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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.cairo.lv.LiveViewStateReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A live view keeps the reason and the time of the first invalidation that reached it, in memory and in
 * its state file, whichever writer comes later: the refresh worker's own, once a turn spends the
 * flush-retry budget or the scan finds the view ahead of its base, or the base table's, which a base DROP,
 * RENAME, UPDATE or schema change runs. A later invalidation still writes the state file, with the first
 * reason, so a view whose first write failed is durably invalid all the same. An invalidation that meets a
 * view a DROP LIVE VIEW has marked dropped writes nothing over the drop's teardown.
 * <p>
 * The first invalidation lands where a concurrent one would: from the refresh-fault observer of the turn
 * that spends the budget, once the fault reached the handler and before the turn charges it; from the hook
 * between the two reads of the ahead-of-base guard; or while the later one waits out a checkpoint freeze.
 * <p>
 * A checkpoint freeze that takes the refresh latch while an invalidation writes the state file frees the
 * invalidated view's runtime state once it lets the latch go, since the invalidation's own free lost to it.
 * <p>
 * The base table's writer closes each view's state file before it lets the view's monitor go, so it holds no
 * view's file open while it waits for the next view's checkpoint freeze.
 */
public class LiveViewRepeatedInvalidationTest extends AbstractLiveViewTxTest {
    // A reason for the base-table writer in the tests that call it directly, without changing the base.
    private static final String BASE_REASON = "invalidated by the base";
    private static final String BASE_VIEW_SQL = "SELECT ts, x, count(*) OVER (PARTITION BY pg ORDER BY ts "
            + "ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base";
    private static final String BUDGET_EXHAUSTED_LINE = "live view refresh budget exhausted, invalidating [view=lv";
    private static final String BUDGET_EXHAUSTED_REASON = "flush retry budget exhausted";
    private static final String COULD_NOT_PERSIST_LINE = "could not persist live view invalidation [view=";
    private static final int ERRNO_EMFILE = 24;
    // createFixture's four commits on 2026-01-01, two per account: each account's second row sums both.
    private static final String FIXTURE_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            2026-01-01T09:20:00.000000Z\tacct-1\t5.0\t2
            2026-01-01T09:30:00.000000Z\tacct-2\t10.0\t2
            """;
    private static final String NOT_WAL_REASON = "base table is not WAL table";
    private static final String UPDATE_REASON = "update operation";
    private static final LogCapture capture = new LogCapture();
    private final LiveViewStateFileFault fault = new LiveViewStateFileFault();

    @Before
    public void setUpRepeatedInvalidation() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @After
    public void tearDownRepeatedInvalidation() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Test
    public void testABaseDropAfterTheBudgetKeepsTheBudgetsReason() throws Exception {
        // The spent budget invalidates a seeding view first, and the base's DROP TABLE comes later, to a view
        // that is already invalid: the view keeps the budget's reason and time, in memory and on disk.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                runUntilInvalid(job, instance);
            }
            // The budget's invalidation reads the clock of the turn that spent it, which nothing moved since.
            final long firstUs = currentMicros;
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());

            fault.failOpensIn(null);
            setCurrentMicros(firstUs + 1_000_000);
            execute("DROP TABLE tx");
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            capture.assertNotLogged(COULD_NOT_PERSIST_LINE);
            assertInvalidAfterARestart(BUDGET_EXHAUSTED_REASON, firstUs);
        });
    }

    @Test
    public void testABaseDropDuringADropLiveViewSkipsOnlyTheDroppedView() throws Exception {
        // Two seeding views over tx, lv registered ahead of lv2. DROP LIVE VIEW lv on another thread marks lv
        // dropped and trips its breaker inside lv's first failing sweep turn, then waits for the turn to
        // release the refresh latch, and the base's DROP TABLE runs in that turn. Its invalidation of the base's
        // views leaves the dropped lv alone and goes on to invalidate lv2.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
            final LiveViewInstance sibling = engine.getLiveViewRegistry().getViewInstance("lv2");
            Assert.assertNotNull(sibling);
            fault.watchStateFile(instance);
            fault.failOpensIn("2026-01-01");
            final AtomicReference<Thread> dropper = new AtomicReference<>();
            final AtomicReference<Throwable> dropError = new AtomicReference<>();
            final AtomicReference<Throwable> baseDropError = new AtomicReference<>();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                hooks(job).setRefreshFaultObserver(t -> {
                    // lv's own fault: the job counts it on lv before it calls the observer.
                    if (instance.getRefreshFaultCount() == 1 && dropper.get() == null) {
                        dropper.set(startDropAndAwaitTheTrip(instance, dropError));
                        try {
                            execute("DROP TABLE tx");
                        } catch (Throwable th) {
                            baseDropError.set(th);
                        }
                    }
                });
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && dropper.get() == null; pass++) {
                    advanceClockToNextRefreshPass();
                    job.run();
                }
            }
            Assert.assertNotNull("lv's sweep turn must have failed", dropper.get());
            Assert.assertNull(baseDropError.get());
            assertDropNotWrittenOver(instance, dropper.get(), dropError);
            Assert.assertTrue("the invalidation must go on past the dropped view", sibling.isInvalid());
            TestUtils.assertEquals("base table drop", sibling.getInvalidationReason());

            shutdown();
            engine.buildViewGraphs();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            view_name\tview_status\tinvalidation_reason
                            lv2\tinvalid\tbase table drop
                            """);
        });
    }

    @Test
    public void testABaseDropKeepsAnInvalidViewsReasonAndInvalidatesItsValidSibling() throws Exception {
        // The spent budget invalidates lv, and lv2, created over the same base once the fault lifted, seeds to
        // the end. The base's DROP TABLE reaches both: lv keeps the budget's reason, lv2 takes the drop's.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                runUntilInvalid(job, instance);
            }
            final long firstUs = currentMicros;
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());

            fault.failOpensIn(null);
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
            final LiveViewInstance sibling = engine.getLiveViewRegistry().getViewInstance("lv2");
            Assert.assertNotNull(sibling);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv2");
                driveRefreshToQuiescence(job);
            }
            Assert.assertFalse(sibling.isInvalid());
            Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, sibling.getStateReader().getSeedState());

            final long dropUs = currentMicros + 1_000_000;
            setCurrentMicros(dropUs);
            execute("DROP TABLE tx");
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            Assert.assertTrue(sibling.isInvalid());
            TestUtils.assertEquals("base table drop", sibling.getInvalidationReason());
            Assert.assertEquals(dropUs, sibling.getStateReader().getInvalidationTimestampUs());

            shutdown();
            engine.buildViewGraphs();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views() ORDER BY view_name")
                    .noLeakCheck()
                    .returns("""
                            view_name\tview_status\tinvalidation_reason
                            lv\tinvalid\tflush retry budget exhausted
                            lv2\tinvalid\tbase table drop
                            """);
            Assert.assertEquals(firstUs, instance().getStateReader().getInvalidationTimestampUs());
        });
    }

    @Test
    public void testABaseInvalidationInTheSeedingTurnThatSpendsTheBudgetStands() throws Exception {
        // The base-table writer, which a base DROP, RENAME, UPDATE or schema change runs, invalidates a seeding
        // view inside the sweep turn that spends the flush-retry budget. The turn's own invalidation, which
        // runs once the turn released the refresh latch, keeps the base's reason and time, in memory and on disk.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            final TableToken txToken = engine.verifyTableName("tx");
            fault.failOpensIn("2026-01-01");
            final AtomicLong firstUs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                firstUs = invalidateInTheSpendingTurn(
                        job,
                        instance,
                        () -> engine.invalidateLiveViewsForBaseTable(txToken, "base table drop")
                );
                runUntilInvalid(job, instance);
            }
            assertFirstInvalidationStands(instance, "base table drop", firstUs.get());
            assertInvalidAfterARestart("base table drop", firstUs.get());
        });
    }

    @Test
    public void testABaseInvalidationLetsEachStateFileGoBeforeTheNextView() throws Exception {
        // Two views over base, lv registered ahead of lv2, and a checkpoint freeze of lv2. The base-table writer
        // invalidates lv and then waits for lv2's freeze. It closed lv's state file holding lv's monitor, so it
        // holds no descriptor of the file while it waits, and a DROP LIVE VIEW of lv, or the rollback of a failed
        // CREATE LIVE VIEW, which mark the view dropped under that monitor, find the file closed when they remove
        // it, as Windows requires.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createBaseView();
            execute("CREATE LIVE VIEW lv2 FLUSH EVERY 100ms START FROM NOW AS " + BASE_VIEW_SQL);
            final LiveViewInstance sibling = engine.getLiveViewRegistry().getViewInstance("lv2");
            Assert.assertNotNull(sibling);
            final TableToken baseToken = engine.verifyTableName("base");
            fault.watchStateFile(instance);
            Assert.assertTrue(sibling.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            final AtomicReference<Throwable> invalidatorError = new AtomicReference<>();
            final Thread invalidator;
            try {
                invalidator = startThread(
                        "lv-invalidator",
                        invalidatorError,
                        () -> engine.invalidateLiveViewsForBaseTable(baseToken, BASE_REASON)
                );
                awaitWaiting(invalidator);
                Assert.assertTrue(instance.isInvalid());
                TestUtils.assertEquals(BASE_REASON, instance.getInvalidationReason());
                Assert.assertFalse(sibling.isInvalid());
                Assert.assertEquals("lv's state file must be closed while the writer waits for lv2", 0, fault.getOpenStateFileFdCount());
            } finally {
                sibling.endCheckpoint();
            }
            joinWithoutError(invalidator, invalidatorError);
            Assert.assertEquals("lv's state file must be closed holding lv's monitor", 1, fault.getStateFileClosesHoldingTheMonitor());
            Assert.assertEquals(0, fault.getStateFileClosesWithoutTheMonitor());
            Assert.assertEquals(0, fault.getOpenStateFileFdCount());
            Assert.assertTrue(sibling.isInvalid());
            TestUtils.assertEquals(BASE_REASON, sibling.getInvalidationReason());
            capture.drain();
            capture.assertNotLogged(COULD_NOT_PERSIST_LINE);

            shutdown();
            engine.buildViewGraphs();
            assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views() ORDER BY view_name")
                    .noLeakCheck()
                    .returns("""
                            view_name\tview_status\tinvalidation_reason
                            lv\tinvalid\tinvalidated by the base
                            lv2\tinvalid\tinvalidated by the base
                            """);
        });
    }

    @Test
    public void testABaseInvalidationPersistsTheBudgetsReasonWhoseWriteFailed() throws Exception {
        // The spent budget invalidates a seeding view and its write of the state file fails, so the view is
        // invalid in memory only. The base-table writer comes later, finds the view invalid, and writes the
        // state file with the budget's reason and time, so a restart loads the view invalid with them.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            final TableToken txToken = engine.verifyTableName("tx");
            fault.watchStateFile(instance);
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                failStateFileOpensInTheSpendingTurn(job, instance, 1);
                runUntilInvalid(job, instance);
            }
            final long firstUs = currentMicros;
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            // The open the fault failed was the budget's own write.
            Assert.assertEquals(1, capture.countOccurrences(COULD_NOT_PERSIST_LINE));
            Assert.assertEquals(1, capture.countOccurrences(", reason=" + BUDGET_EXHAUSTED_REASON + ", error="));
            Assert.assertFalse(readStateFile(instance).isInvalid());

            setCurrentMicros(firstUs + 1_000_000);
            engine.invalidateLiveViewsForBaseTable(txToken, BASE_REASON);
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences(COULD_NOT_PERSIST_LINE));
            assertInvalidAfterARestart(BUDGET_EXHAUSTED_REASON, firstUs);
        });
    }

    @Test
    public void testABaseInvalidationQueuedBehindAFreezeKeepsAnInvalidationLandedMeanwhile() throws Exception {
        assertQueuedInvalidationKeepsTheOneLandedMeanwhile(true);
    }

    @Test
    public void testABaseInvalidationQueuedBehindAFreezeLeavesADroppedViewAlone() throws Exception {
        assertQueuedInvalidationLeavesADroppedViewAlone(true, null);
    }

    @Test
    public void testABaseInvalidationQueuedBehindAFreezeLeavesAnInvalidDroppedViewAlone() throws Exception {
        // The view is invalid before the freeze, as a view an operator drops to recreate it is.
        assertQueuedInvalidationLeavesADroppedViewAlone(true, "invalidated before the freeze");
    }

    @Test
    public void testABaseInvalidationWhoseWriteAlsoFailsLogsTheBudgetsReason() throws Exception {
        // As testABaseInvalidationPersistsTheBudgetsReasonWhoseWriteFailed, but the base-table writer's write
        // fails too. Its failure names the budget's reason, the one it would have written, and a restart loads
        // the view from a state file that never recorded the invalidation: seeding again.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            final TableToken txToken = engine.verifyTableName("tx");
            fault.watchStateFile(instance);
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                failStateFileOpensInTheSpendingTurn(job, instance, 2);
                runUntilInvalid(job, instance);
            }
            final long firstUs = currentMicros;
            setCurrentMicros(firstUs + 1_000_000);
            engine.invalidateLiveViewsForBaseTable(txToken, BASE_REASON);
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            Assert.assertEquals(2, capture.countOccurrences(COULD_NOT_PERSIST_LINE));
            Assert.assertEquals(2, capture.countOccurrences(", reason=" + BUDGET_EXHAUSTED_REASON + ", error="));
            capture.assertNotLogged(BASE_REASON);
            shutdown();
            engine.buildViewGraphs();
            assertLiveViewsRow("lv\tseeding\t\n");
        });
    }

    @Test
    public void testADropLiveViewInTheTurnThatSpendsTheBudgetIsNotWrittenOver() throws Exception {
        // DROP LIVE VIEW on another thread marks a seeding view dropped and trips its breaker inside the sweep
        // turn that spends the flush-retry budget, then waits for the turn to release the refresh latch. The
        // turn's invalidation, which runs after that release, leaves the dropped view alone.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            fault.watchStateFile(instance);
            fault.failOpensIn("2026-01-01");
            final int maxRetry = configuration.getLiveViewFlushRetryMax();
            final AtomicReference<Thread> dropper = new AtomicReference<>();
            final AtomicReference<Throwable> dropError = new AtomicReference<>();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                hooks(job).setRefreshFaultObserver(t -> {
                    if (instance.getFlushRetryCount() == maxRetry - 1 && dropper.get() == null) {
                        dropper.set(startDropAndAwaitTheTrip(instance, dropError));
                    }
                });
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && dropper.get() == null; pass++) {
                    advanceClockToNextRefreshPass();
                    job.run();
                }
            }
            Assert.assertNotNull("the turn that spends the budget must have run", dropper.get());
            assertDropNotWrittenOver(instance, dropper.get(), dropError);
            Assert.assertEquals(maxRetry, instance.getFlushRetryCount());
            Assert.assertEquals(1, capture.countOccurrences(BUDGET_EXHAUSTED_LINE));

            shutdown();
            engine.buildViewGraphs();
            assertQuery("SELECT view_name FROM live_views()")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_name\n");
        });
    }

    @Test
    public void testAMultiByteFirstReasonSurvivesALaterInvalidation() throws Exception {
        // A schema change that drops a column the view reads names the column in its reason, here a name of
        // two- and four-byte UTF-8 characters. The base's later DROP TABLE keeps that reason, in memory and on
        // disk.
        assertMemoryLeak(fault, () -> {
            execute("CREATE TABLE b (ts TIMESTAMP, \"сумма💰\" DOUBLE, g SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS SELECT ts, \"сумма💰\", "
                    + "count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn "
                    + "FROM b WHERE \"сумма💰\" > 0");
            final LiveViewInstance instance = instance();
            final long firstUs = 1_000_000;
            setCurrentMicros(firstUs);
            execute("ALTER TABLE b DROP COLUMN \"сумма💰\"");
            drainWalQueue();
            final String reason = "drop column operation [column=сумма💰]";
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(reason, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());

            setCurrentMicros(firstUs + 1_000_000);
            execute("DROP TABLE b");
            TestUtils.assertEquals(reason, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            assertInvalidAfterARestart(reason, firstUs);
        });
    }

    @Test
    public void testASecondInvalidationPersistsTheFirstWhoseWriteFailed() throws Exception {
        // The base-table writer invalidates a seeding view inside the turn that spends the budget, and its
        // write of the state file fails: the view is invalid in memory only. The turn's own invalidation then
        // writes the state file with the first reason and time, so a restart loads the view invalid with them.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            final TableToken txToken = engine.verifyTableName("tx");
            fault.watchStateFile(instance);
            fault.failOpensIn("2026-01-01");
            final AtomicLong firstUs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                firstUs = invalidateInTheSpendingTurn(job, instance, () -> {
                    fault.failStateFileOpens(1);
                    engine.invalidateLiveViewsForBaseTable(txToken, BASE_REASON);
                });
                runUntilInvalid(job, instance);
            }
            Assert.assertNotEquals("the first invalidation must have run", Numbers.LONG_NULL, firstUs.get());
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(BASE_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs.get(), instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences(COULD_NOT_PERSIST_LINE));
            Assert.assertEquals(1, capture.countOccurrences(", reason=" + BASE_REASON + ", error="));
            Assert.assertEquals(1, capture.countOccurrences(BUDGET_EXHAUSTED_LINE));
            assertInvalidAfterARestart(BASE_REASON, firstUs.get());
        });
    }

    @Test
    public void testAnInMemoryInvalidationStandsAndReachesTheStateFile() throws Exception {
        // A restart that finds the base converted to a non-WAL table invalidates the view in memory only: its
        // state file still records it valid. The base's DROP TABLE then reaches an invalid view and writes the
        // state file with the restart's reason and time, not its own, so the next restart loads them.
        assertMemoryLeak(fault, () -> {
            createBaseView();
            execute("ALTER TABLE base SET TYPE BYPASS WAL");
            // Apply the conversion and load the views again, as a restart does.
            engine.releaseInactive();
            engine.load();
            engine.getLiveViewRegistry().clear();
            final long firstUs = 5_000_000;
            setCurrentMicros(firstUs);
            engine.buildViewGraphs();
            Assert.assertFalse(engine.verifyTableName("base").isWal());
            final LiveViewInstance instance = instance();
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(NOT_WAL_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            Assert.assertFalse("the restart invalidates in memory only", readStateFile(instance).isInvalid());

            setCurrentMicros(firstUs + 1_000_000);
            execute("DROP TABLE base");
            TestUtils.assertEquals(NOT_WAL_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            final LiveViewStateReader persisted = readStateFile(instance);
            Assert.assertTrue(persisted.isInvalid());
            TestUtils.assertEquals(NOT_WAL_REASON, persisted.getInvalidationReason());
            Assert.assertEquals(firstUs, persisted.getInvalidationTimestampUs());
            assertInvalidAfterARestart(NOT_WAL_REASON, firstUs);
        });
    }

    @Test
    public void testAnInstanceInvalidationInTheSeedingTurnThatSpendsTheBudgetStands() throws Exception {
        // As testABaseInvalidationInTheSeedingTurnThatSpendsTheBudgetStands, with the first invalidation made
        // on the instance, as the refresh worker's and the scan's are.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            fault.failOpensIn("2026-01-01");
            final AtomicLong firstUs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                firstUs = invalidateInTheSpendingTurn(
                        job,
                        instance,
                        () -> engine.invalidateLiveView(instance, "invalidated during the turn")
                );
                runUntilInvalid(job, instance);
            }
            assertFirstInvalidationStands(instance, "invalidated during the turn", firstUs.get());
            assertInvalidAfterARestart("invalidated during the turn", firstUs.get());
        });
    }

    @Test
    public void testAnInvalidStateWithNoReasonKeepsNoReason() throws Exception {
        // A state file that records the view invalid with no reason and no time, which a hand edit or another
        // build can leave. The base's DROP TABLE reaches the invalid view and keeps both unset.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance created = createBaseView();
            final LiveViewStateReader state = created.getStateReader();
            try (
                    BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    Path path = new Path()
            ) {
                path.of(configuration.getDbRoot()).concat(created.getLiveViewToken()).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
                writer.of(path.$());
                LiveViewState.append(
                        true,
                        null,
                        Numbers.LONG_NULL,
                        state.getSubscribeFromSeqTxn(),
                        state.getLastProcessedSeqTxn(),
                        state.getAppliedWatermark(),
                        state.getLvConsumedSeqTxn(),
                        state.getSeedState(),
                        state.getSeedTargetSeqTxn(),
                        writer
                );
            }
            shutdown();
            engine.buildViewGraphs();
            final LiveViewInstance instance = instance();
            Assert.assertTrue(instance.isInvalid());
            Assert.assertNull(instance.getInvalidationReason());

            setCurrentMicros(1_000_000);
            execute("DROP TABLE base");
            Assert.assertTrue(instance.isInvalid());
            Assert.assertNull(instance.getInvalidationReason());
            Assert.assertEquals(Numbers.LONG_NULL, instance.getStateReader().getInvalidationTimestampUs());
            assertInvalidAfterARestart("", Numbers.LONG_NULL);
            Assert.assertNull(instance().getInvalidationReason());
        });
    }

    @Test
    public void testAnInvalidationQueuedBehindAFreezeKeepsAnInvalidationLandedMeanwhile() throws Exception {
        assertQueuedInvalidationKeepsTheOneLandedMeanwhile(false);
    }

    @Test
    public void testAnInvalidationQueuedBehindAFreezeLeavesADroppedViewAlone() throws Exception {
        assertQueuedInvalidationLeavesADroppedViewAlone(false, null);
    }

    @Test
    public void testAnInvalidationQueuedBehindAFreezeLeavesAnInvalidDroppedViewAlone() throws Exception {
        // The view is invalid before the freeze, as a view an operator drops to recreate it is.
        assertQueuedInvalidationLeavesADroppedViewAlone(false, "invalidated before the freeze");
    }

    @Test
    public void testAnInvalidationWhoseWritesBothFailStaysInvalidInMemoryOnly() throws Exception {
        // As testASecondInvalidationPersistsTheFirstWhoseWriteFailed, but the turn's own write fails too. Both
        // failures name the first reason, the view stays invalid in memory with it, and a restart loads the view
        // from a state file that never recorded the invalidation: seeding, and once the file opens again, it
        // seeds to the end.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            final TableToken txToken = engine.verifyTableName("tx");
            fault.watchStateFile(instance);
            fault.failOpensIn("2026-01-01");
            final AtomicLong firstUs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                firstUs = invalidateInTheSpendingTurn(job, instance, () -> {
                    fault.failStateFileOpens(2);
                    engine.invalidateLiveViewsForBaseTable(txToken, BASE_REASON);
                });
                runUntilInvalid(job, instance);
            }
            Assert.assertNotEquals("the first invalidation must have run", Numbers.LONG_NULL, firstUs.get());
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(BASE_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs.get(), instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            Assert.assertEquals(2, capture.countOccurrences(COULD_NOT_PERSIST_LINE));
            Assert.assertEquals(2, capture.countOccurrences(", reason=" + BASE_REASON + ", error="));

            shutdown();
            engine.buildViewGraphs();
            assertLiveViewsRow("lv\tseeding\t\n");
            fault.failOpensIn(null);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }
            assertLiveViewsRow("lv\tactive\t\n");
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testAnUpdateBetweenTheAheadOfBaseGuardsReadsStands() throws Exception {
        // The view's state file holds a watermark three commits past its base's head, as a replica that applied a
        // live view data block its base never received leaves it, so the scan's ahead-of-base guard invalidates
        // the view. A base UPDATE lands between the guard's two reads, and the apply job invalidates the view
        // first, with the update's reason, which the guard's invalidation keeps.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createBaseView();
            final long aheadSeqTxn = engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("base")) + 3;
            try (
                    BlockFileWriter writer = new BlockFileWriter(configuration.getFilesFacade(), configuration.getCommitMode());
                    Path path = new Path()
            ) {
                engine.applyLiveViewData(engine.verifyTableName("lv"), aheadSeqTxn, writer, path);
            }
            Assert.assertEquals(aheadSeqTxn, instance.getLastProcessedSeqTxn());
            final AtomicLong firstUs = new AtomicLong(Numbers.LONG_NULL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                hooks(job).setSimulateBaseCommitBetweenAheadGuardReads(() -> {
                    firstUs.set(currentMicros);
                    try {
                        execute("UPDATE base SET x = 99 WHERE ts = '2026-06-01T00:00:01.000000Z'");
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    drainWalQueue();
                    setCurrentMicros(currentMicros + 1_000);
                });
                drainJob(job);
            }
            Assert.assertNotEquals("the update must have landed between the guard's reads", Numbers.LONG_NULL, firstUs.get());
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals(UPDATE_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs.get(), instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences("live view is ahead of base table and cannot be synchronized [view="));
            assertInvalidAfterARestart(UPDATE_REASON, firstUs.get());
        });
    }

    @Test
    public void testAnUpdateInTheActiveTurnThatSpendsTheBudgetStands() throws Exception {
        // An active view meets a WAL read error ahead of the first row of every turn, so no recovery and no
        // re-derive runs, and the fifth turn spends the count budget. A base UPDATE lands in that turn, and the
        // apply job invalidates the view with the update's reason, which the turn's own invalidation keeps.
        final LiveViewMidDrainFault walFault = new LiveViewMidDrainFault();
        walFault.reportReadErrno(ERRNO_EMFILE);
        assertMemoryLeak(walFault.facade(), () -> {
            createFixture("");
            walFault.of(engine.verifyTableName("tx").getDirName());
            insert("('2026-01-01T09:40:00.000000Z', 'acct-1', 16.0)");
            final LiveViewInstance instance = instance();
            final AtomicLong firstUs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                firstUs = invalidateInTheSpendingTurn(job, instance, () -> {
                    try {
                        execute("UPDATE tx SET amount = 99.0 WHERE created_at = '2026-01-01T09:00:00.000000Z'");
                    } catch (SqlException e) {
                        throw new AssertionError(e);
                    }
                    drainWalQueue();
                });
                for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !instance.isInvalid(); pass++) {
                    walFault.arm(0);
                    advanceClockToNextRefreshPass();
                    job.run();
                }
                walFault.disarm();
            }
            assertFirstInvalidationStands(instance, UPDATE_REASON, firstUs.get());
            // The fault failed every turn ahead of its first row, so the view holds the fixture's rows.
            assertQuery("SELECT * FROM lv ORDER BY created_at")
                    .noLeakCheck()
                    .timestamp("created_at")
                    .expectSize()
                    .returns(FIXTURE_ROWS);
            assertInvalidAfterARestart(UPDATE_REASON, firstUs.get());
        });
    }

    @Test
    public void testCheckpointFreezeFreesViewItsRefreshTurnInvalidated() throws Exception {
        // The turn that spends the flush-retry budget invalidates the seeding view once it released the refresh
        // latch. A checkpoint freeze requested in that turn waits for the latch behind it, takes it when the turn
        // lets it go, and waits for the view's monitor, which the invalidation holds while it writes the state file:
        // the invalidation's free of the view's runtime state loses its latch CAS to the freeze. startCheckpoint makes
        // the free once it lets the latch go. The test thread holds the monitor across the drive, and the
        // invalidation's write of the state file waits until the freeze blocks on it holding the latch.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture();
            fault.watchStateFile(instance);
            fault.failOpensIn("2026-01-01");
            final int maxRetry = configuration.getLiveViewFlushRetryMax();
            final AtomicReference<Thread> agent = new AtomicReference<>();
            final AtomicBoolean isFrozen = new AtomicBoolean();
            final AtomicReference<Throwable> agentError = new AtomicReference<>();
            final long firstUs;
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                hooks(job).setRefreshFaultObserver(t -> {
                    if (instance.getFlushRetryCount() == maxRetry - 1 && agent.get() == null) {
                        agent.set(startThread(
                                "lv-checkpoint-agent",
                                agentError,
                                () -> isFrozen.set(instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER))
                        ));
                        // The turn's invalidation makes the next write of the state file, once the turn released
                        // the refresh latch. A write ahead of that release fails the wait.
                        fault.runAtNextStateFileOpen(() -> awaitAgentBlockedHoldingTheLatch(agent.get(), instance));
                    }
                });
                synchronized (instance) {
                    runUntilInvalid(job, instance);
                    firstUs = currentMicros;
                    Assert.assertNotNull("the invalidation's free must lose to the freeze", instance.getCompiledFactory());
                    Assert.assertNotNull(instance.getMemoryTracker());
                    Assert.assertNotNull(instance.getAnchorWindow());
                }
            }
            Assert.assertNotNull("the turn that spends the budget must have run", agent.get());
            joinWithoutError(agent.get(), agentError);
            Assert.assertTrue("the freeze of a view that is not dropped must be published", isFrozen.get());
            Assert.assertTrue(instance.isFreezeInProgress());
            TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            Assert.assertNull(instance.getCompiledFactory());
            Assert.assertNull(instance.getInMemoryTier());
            Assert.assertNull(instance.getMemoryTracker());
            Assert.assertNull(instance.getAnchorWindow());
            Assert.assertNull(instance.getRepairRuntime());
            Assert.assertNull(instance.getStoredRowScanFactory());
            Assert.assertNull(instance.getSeedBaseReader());
            instance.endCheckpoint();
            Assert.assertFalse(instance.isFreezeArmed());
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences(BUDGET_EXHAUSTED_LINE));
            capture.assertNotLogged(COULD_NOT_PERSIST_LINE);
            assertInvalidAfterARestart(BUDGET_EXHAUSTED_REASON, firstUs);
        });
    }

    // The view is invalid with the first invalidation's reason and time, the turn charged its fault and logged
    // the spent budget, and no write of the state file failed.
    private static void assertFirstInvalidationStands(LiveViewInstance instance, String reason, long firstUs) {
        Assert.assertNotEquals("the first invalidation must have run", Numbers.LONG_NULL, firstUs);
        Assert.assertTrue(instance.isInvalid());
        TestUtils.assertEquals(reason, instance.getInvalidationReason());
        Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
        Assert.assertEquals(configuration.getLiveViewFlushRetryMax(), instance.getFlushRetryCount());
        capture.drain();
        Assert.assertEquals(1, capture.countOccurrences(BUDGET_EXHAUSTED_LINE));
        capture.assertNotLogged(COULD_NOT_PERSIST_LINE);
    }

    private static void invalidate(boolean isBaseWriter, LiveViewInstance instance, TableToken baseToken, String reason) {
        if (isBaseWriter) {
            engine.invalidateLiveViewsForBaseTable(baseToken, reason);
        } else {
            engine.invalidateLiveView(instance, reason);
        }
    }

    // From the refresh-fault observer of the turn that spends the flush-retry budget, before the turn charges
    // its fault: records the clock, runs the first invalidation, and moves the clock a millisecond on, so the
    // turn's own invalidation carries another time. Returns the recorded clock, LONG_NULL until it ran.
    private static AtomicLong invalidateInTheSpendingTurn(
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            Runnable firstInvalidation
    ) {
        final int maxRetry = configuration.getLiveViewFlushRetryMax();
        final AtomicLong firstUs = new AtomicLong(Numbers.LONG_NULL);
        hooks(job).setRefreshFaultObserver(t -> {
            if (instance.getFlushRetryCount() == maxRetry - 1 && firstUs.get() == Numbers.LONG_NULL) {
                firstUs.set(currentMicros);
                firstInvalidation.run();
                setCurrentMicros(currentMicros + 1_000);
            }
        });
        return firstUs;
    }

    private static LiveViewStateReader readStateFile(LiveViewInstance instance) {
        try (
                BlockFileReader reader = new BlockFileReader(configuration);
                Path path = new Path()
        ) {
            path.of(configuration.getDbRoot()).concat(instance.getLiveViewToken()).concat(LiveViewState.LIVE_VIEW_STATE_FILE_NAME);
            reader.of(path.$());
            return new LiveViewStateReader().of(reader, instance.getLiveViewToken());
        }
    }

    // Drives one pass at a time over a view whose every turn fails, until the view is invalid.
    private static void runUntilInvalid(LiveViewRefreshJob job, LiveViewInstance instance) {
        for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && !instance.isInvalid(); pass++) {
            advanceClockToNextRefreshPass();
            job.run();
            drainWalQueue();
        }
        Assert.assertTrue("the view must have been invalidated", instance.isInvalid());
    }

    // Read as the turn left the instance: the drop marked it and no invalidation flipped it or wrote its state
    // file. Then the drop completes, and the view is gone.
    private void assertDropNotWrittenOver(
            LiveViewInstance instance,
            Thread dropper,
            AtomicReference<Throwable> dropError
    ) throws InterruptedException {
        Assert.assertTrue(instance.isDropped());
        Assert.assertFalse("an invalidation must not flip a view a drop is tearing down", instance.isInvalid());
        Assert.assertNull(instance.getInvalidationReason());
        Assert.assertEquals("an invalidation must not write over a drop", 0, fault.getStateFileOpensAfterDrop());
        joinWithoutError(dropper, dropError);
        Assert.assertEquals(0, fault.getStateFileOpensAfterDrop());
        capture.drain();
        capture.assertNotLogged(COULD_NOT_PERSIST_LINE);
        Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
        Assert.assertNull(engine.getTableTokenIfExists("lv"));
    }

    // A restart loads the reason and the time from the view's state file.
    private void assertInvalidAfterARestart(String reason, long firstUs) throws Exception {
        shutdown();
        engine.buildViewGraphs();
        assertLiveViewsRow("lv\tinvalid\t" + reason + '\n');
        Assert.assertEquals(firstUs, instance().getStateReader().getInvalidationTimestampUs());
    }

    private void assertLiveViewsRow(String expectedRow) throws Exception {
        assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_name\tview_status\tinvalidation_reason\n" + expectedRow);
    }

    // A checkpoint freeze holds back an invalidation, the base-table writer's with isBaseWriter, invalidateLiveView's
    // without. The freeze ends and another invalidation runs before the held one takes the view's monitor back:
    // the held one finds the view invalid and keeps the other's reason and time, in memory and on disk.
    private void assertQueuedInvalidationKeepsTheOneLandedMeanwhile(boolean isBaseWriter) throws Exception {
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createBaseView();
            final TableToken baseToken = engine.verifyTableName("base");
            Assert.assertTrue(instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            final AtomicReference<Throwable> invalidatorError = new AtomicReference<>();
            final long firstUs = 2_000_000;
            final Thread invalidator;
            try {
                setCurrentMicros(1_000_000);
                invalidator = startThread(
                        "lv-invalidator",
                        invalidatorError,
                        () -> invalidate(isBaseWriter, instance, baseToken, "queued behind the freeze")
                );
                awaitWaiting(invalidator);
                setCurrentMicros(firstUs);
                // Under the monitor the held invalidation waits on, so it wakes to the freeze's end only once
                // this one has run.
                synchronized (instance) {
                    instance.endCheckpoint();
                    engine.invalidateLiveView(instance, "invalidated when the freeze ended");
                }
            } finally {
                instance.endCheckpoint();
            }
            joinWithoutError(invalidator, invalidatorError);
            Assert.assertTrue(instance.isInvalid());
            TestUtils.assertEquals("invalidated when the freeze ended", instance.getInvalidationReason());
            Assert.assertEquals(firstUs, instance.getStateReader().getInvalidationTimestampUs());
            capture.drain();
            capture.assertNotLogged(COULD_NOT_PERSIST_LINE);
            assertInvalidAfterARestart("invalidated when the freeze ended", firstUs);
        });
    }

    // A checkpoint freeze holds back an invalidation, the base-table writer's with isBaseWriter, invalidateLiveView's
    // without, and a DROP LIVE VIEW, which marks the view dropped and waits for the freeze too. The freeze ends.
    // Whichever thread takes the view's monitor first, the invalidation finds the view dropped: it writes no
    // state file over the drop's teardown and leaves the instance as the drop found it, valid, or invalid with
    // firstReason when that is not null.
    private void assertQueuedInvalidationLeavesADroppedViewAlone(boolean isBaseWriter, @Nullable String firstReason) throws Exception {
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createBaseView();
            final TableToken baseToken = engine.verifyTableName("base");
            if (firstReason != null) {
                engine.invalidateLiveView(instance, firstReason);
            }
            fault.watchStateFile(instance);
            Assert.assertTrue(instance.startCheckpoint(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER));
            final AtomicReference<Throwable> invalidatorError = new AtomicReference<>();
            final AtomicReference<Throwable> dropError = new AtomicReference<>();
            final Thread invalidator;
            final Thread dropper;
            try {
                invalidator = startThread(
                        "lv-invalidator",
                        invalidatorError,
                        () -> invalidate(isBaseWriter, instance, baseToken, "queued behind the freeze")
                );
                awaitWaiting(invalidator);
                dropper = startThread("lv-dropper", dropError, () -> engine.dropLiveView("lv", AllowAllSecurityContext.INSTANCE));
                awaitDropped(instance, THREAD_JOIN_MILLIS);
                awaitWaiting(dropper);
            } finally {
                instance.endCheckpoint();
            }
            joinWithoutError(invalidator, invalidatorError);
            joinWithoutError(dropper, dropError);
            Assert.assertTrue(instance.isDropped());
            Assert.assertEquals(firstReason != null, instance.isInvalid());
            if (firstReason != null) {
                TestUtils.assertEquals(firstReason, instance.getInvalidationReason());
            } else {
                Assert.assertNull(instance.getInvalidationReason());
            }
            Assert.assertEquals("an invalidation must not write over a drop", 0, fault.getStateFileOpensAfterDrop());
            capture.drain();
            capture.assertNotLogged(COULD_NOT_PERSIST_LINE);
            Assert.assertNull(engine.getLiveViewRegistry().getViewInstance("lv"));
            Assert.assertNull(engine.getTableTokenIfExists("lv"));
            execute("DROP TABLE base");
        });
    }

    // From the refresh-fault observer of the turn that spends the flush-retry budget, before the turn
    // charges its fault: fails the next count writes of the view's state file.
    private void failStateFileOpensInTheSpendingTurn(LiveViewRefreshJob job, LiveViewInstance instance, int count) {
        final int maxRetry = configuration.getLiveViewFlushRetryMax();
        final AtomicBoolean isArmed = new AtomicBoolean();
        hooks(job).setRefreshFaultObserver(t -> {
            if (instance.getFlushRetryCount() == maxRetry - 1 && isArmed.compareAndSet(false, true)) {
                fault.failStateFileOpens(count);
            }
        });
    }

    // base and lv over it, refreshed past one commit.
    private LiveViewInstance createBaseView() throws Exception {
        execute("CREATE TABLE base (ts TIMESTAMP, x LONG, pg SYMBOL) TIMESTAMP(ts) PARTITION BY DAY WAL");
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS " + BASE_VIEW_SQL);
        execute("INSERT INTO base (ts, x) VALUES ('2026-06-01T00:00:01.000000Z', 1), ('2026-06-01T00:00:02.000000Z', 2)");
        drainWalQueue();
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveRefreshToQuiescence(job);
        }
        final LiveViewInstance instance = instance();
        Assert.assertFalse(instance.isInvalid());
        Assert.assertEquals(engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("base")), instance.getLastProcessedSeqTxn());
        return instance;
    }

    // tx with four rows on 2026-01-01 and 2026-01-02 in one commit, lv created over it and seeding up to that
    // commit, and a commit on 2026-01-03. While the fault fails the first day's amount file, every sweep turn
    // fails ahead of its first row, and the fifth spends the default flush-retry budget.
    private LiveViewInstance createSeedingFixture() throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) TIMESTAMP(created_at) PARTITION BY DAY WAL");
        insert("('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0), ('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0), "
                + "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0), ('2026-01-02T09:10:00.000000Z', 'acct-2', 8.0)");
        fault.of(engine.verifyTableName("tx").getDirName());
        // The sweep's reader then opens the base's partitions afresh, which the fault needs.
        engine.releaseAllReaders();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
        insert("('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0)");
        final LiveViewInstance instance = instance();
        Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
        return instance;
    }
}
