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
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A seeding live view whose sweep cannot open a base table file for a whole flush-retry budget is
 * invalidated with the budget's reason, as every seed fault the budget outlives is, and is never
 * re-derived from the applied base. A seeding view reads no base WAL, so the file is no lost WAL
 * segment, and the re-derive would commit the whole view at the base's head without ending the seed:
 * the sweep would then append every row a second time.
 * <p>
 * The fault fails every open of one partition's {@code amount} column file in the base table, reporting
 * a missing file, which is the error that sends a spent budget to the re-derive. The hook ahead of the
 * re-derive's replay records that the re-derive started and lifts the fault, as a fresh reader of the
 * base's head would find the file: the path on which a seeding view got its rows twice.
 */
public class LiveViewSeedRederiveRefusalTest extends AbstractLiveViewTxTest {
    private static final String BUDGET_EXHAUSTED_LINE = "live view refresh budget exhausted, invalidating [view=lv";
    private static final String BUDGET_EXHAUSTED_REASON = "flush retry budget exhausted";
    private static final String DEDUP_CLAUSE = "DEDUP UPSERT KEYS(created_at, account_id)";
    // RECOMPUTE_SQL over the rows a sweep that stops at 2026-01-02 has committed.
    private static final String FIRST_DAY_RECOMPUTE_SQL = """
            (
                SELECT created_at, account_id,
                    sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum,
                    count(account_id) OVER (PARTITION BY account_id, bucket ORDER BY created_at
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_count
                FROM (
                    SELECT created_at, account_id, amount,
                        timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket
                    FROM tx
                    WHERE created_at < '2026-01-02'
                )
            ) ORDER BY 2, 1
            """;
    // The first day holds one row per account, so each row's running sum is its amount and its count 1.
    private static final String FIRST_DAY_ROWS = """
            created_at\taccount_id\tcumulative_sum\tcumulative_count
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0\t1
            2026-01-01T09:10:00.000000Z\tacct-2\t2.0\t1
            """;
    private static final LogCapture capture = new LogCapture();
    private final LiveViewBaseAmountOpenFault fault = new LiveViewBaseAmountOpenFault();

    @Before
    public void setUpSeedRederiveRefusal() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @After
    public void tearDownSeedRederiveRefusal() {
        capture.stop();
        setCurrentMicros(-1);
    }

    @Test
    public void testASeedingViewAtTheCountBudgetsEdgeIsInvalidatedAtOnce() throws Exception {
        // A budget of one failure: the first sweep turn that cannot open the first day's file spends it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, 1);
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture("");
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isRederiveStarted = recordTheRederive(job);
                Assert.assertEquals(1, spendTheFlushRetryBudget(job, instance, isRederiveStarted, 0));
                assertInvalidatedNotRederived(instance, isRederiveStarted, 1);
                assertViewRowCount(0);
                assertStaysInvalidOnceTheFileOpens(job, instance);
                assertViewRowCount(0);
            }
            assertInvalidAfterARestart();
            assertViewRowCount(0);
        });
    }

    @Test
    public void testASeedingViewLoadedFromDiskIsInvalidatedNotRederived() throws Exception {
        // The sweep commits the first day's two rows, one turn each, and the process stops. The restart loads
        // the view seeding from its state file, and its sweep resumes and cannot open the second day's file.
        // The spent budget invalidates the reloaded view, which keeps the two rows.
        assertMemoryLeak(fault, () -> {
            createSeedingFixture("");
            sweepTheFirstDay(instance());
            shutdown();
            engine.buildViewGraphs();
            final LiveViewInstance instance = instance();
            Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
            fault.failOpensIn("2026-01-02");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isRederiveStarted = recordTheRederive(job);
                spendTheFlushRetryBudget(job, instance, isRederiveStarted, 0);
                assertInvalidatedNotRederived(instance, isRederiveStarted, 5);
                assertFirstDayRows();
                assertStaysInvalidOnceTheFileOpens(job, instance);
                assertFirstDayRows();
            }
            assertInvalidAfterARestart();
            assertFirstDayRows();
        });
    }

    @Test
    public void testASeedingViewOverADedupBaseIsInvalidatedNotRederived() throws Exception {
        // testASeedingViewWithNoOutputIsInvalidatedNotRederived over a deduplicating base.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture(DEDUP_CLAUSE);
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isRederiveStarted = recordTheRederive(job);
                spendTheFlushRetryBudget(job, instance, isRederiveStarted, 0);
                assertInvalidatedNotRederived(instance, isRederiveStarted, 5);
                assertViewRowCount(0);
                assertStaysInvalidOnceTheFileOpens(job, instance);
                assertViewRowCount(0);
            }
            assertInvalidAfterARestart();
            assertViewRowCount(0);
        });
    }

    @Test
    public void testASeedingViewPastTheDurationBudgetIsInvalidatedNotRederived() throws Exception {
        // A count budget no run reaches and a one-second duration budget. Every pass moves the clock 400 ms,
        // then the 250 ms a pass moves it when no retry deadline lies ahead: the deadlines the first two
        // failures arm, 100 ms and 200 ms after them, are behind it by then. The streak starts at the first
        // failed turn, so the third one, 1.3 s after it, is the first past the budget.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX, 1000);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_FLUSH_RETRY_MAX_DURATION_MICROS, 1_000_000);
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture("");
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isRederiveStarted = recordTheRederive(job);
                final long extraMicrosPerPass = 400_000;
                Assert.assertEquals(3, spendTheFlushRetryBudget(job, instance, isRederiveStarted, extraMicrosPerPass));
                assertInvalidatedNotRederived(instance, isRederiveStarted, 3);
                final long elapsedUs = 2 * (extraMicrosPerPass + CLOCK_ADVANCE_MICROS);
                Assert.assertTrue(elapsedUs >= 1_000_000);
                Assert.assertEquals(1, capture.countOccurrences(BUDGET_EXHAUSTED_LINE + ", retryCount=3, elapsedUs=" + elapsedUs + ','));
                assertViewRowCount(0);
                assertStaysInvalidOnceTheFileOpens(job, instance);
                assertViewRowCount(0);
            }
            assertInvalidAfterARestart();
            assertViewRowCount(0);
        });
    }

    @Test
    public void testASeedingViewRestoredWithoutBaseWalFinishesItsSweepFromTheBaseTable() throws Exception {
        // The sweep commits the first day's two rows and the process stops, and the base table comes back
        // without its WAL, as a restore of a backup leaves it. The restarted sweep reads the base table and
        // never its WAL, so it finishes the seed with no fault, and the view turns active over every row:
        // a seeding view leaves the re-derive after a base WAL loss nothing to do.
        assertMemoryLeak(fault, () -> {
            createSeedingFixture("");
            sweepTheFirstDay(instance());
            shutdown();
            removeBaseWal("tx");
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }
            final LiveViewInstance instance = instance();
            Assert.assertFalse(instance.isInvalid());
            Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, instance.getStateReader().getSeedState());
            assertNoRefreshFaults("lv");
            assertViewMatchesRecompute();
            assertViewRowCount(5);
            capture.drain();
            Assert.assertEquals(1, capture.countOccurrences("live view seed sweep completed [view=lv"));
            capture.assertNotLogged("live view refresh budget exhausted");
            capture.assertNotLogged("live view re-derived from the applied base after base WAL loss");
            shutdown();
            engine.buildViewGraphs();
            assertLiveViewsRow("lv\tactive\t\n");
            assertViewMatchesRecompute();
        });
    }

    @Test
    public void testASeedingViewWhoseTurnFedRowsIsInvalidatedNotRederived() throws Exception {
        // Three-row turns: each turn feeds the first day's two rows and then cannot open the second day's
        // file, so it commits nothing, and the mid-seed recovery re-arms the sweep's resume before the turn
        // is charged. The spent budget invalidates the view all the same, with no row in it.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 3);
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture("");
            fault.failOpensIn("2026-01-02");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isRederiveStarted = recordTheRederive(job);
                spendTheFlushRetryBudget(job, instance, isRederiveStarted, 0);
                assertInvalidatedNotRederived(instance, isRederiveStarted, 5);
                Assert.assertEquals(5, capture.countOccurrences("live view mid-seed refresh failure, sweep will resume [view=lv"));
                Assert.assertFalse("the last failed turn re-armed the sweep's resume", instance.isSeedResumeAttempted());
                assertViewRowCount(0);
                assertStaysInvalidOnceTheFileOpens(job, instance);
                assertViewRowCount(0);
            }
            assertInvalidAfterARestart();
            assertViewRowCount(0);
        });
    }

    @Test
    public void testASeedingViewWithNoOutputIsInvalidatedNotRederived() throws Exception {
        // The sweep cannot open the first day's file, so no turn feeds a row. The default budget of five
        // failures runs out, the view is invalidated with the budget's reason and holds no row, stays so once
        // the file opens again and across a restart, and DROP and CREATE seed it anew.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture("");
            fault.failOpensIn("2026-01-01");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isRederiveStarted = recordTheRederive(job);
                spendTheFlushRetryBudget(job, instance, isRederiveStarted, 0);
                assertInvalidatedNotRederived(instance, isRederiveStarted, 5);
                assertViewRowCount(0);
                assertStaysInvalidOnceTheFileOpens(job, instance);
                assertViewRowCount(0);
            }
            assertInvalidAfterARestart();
            assertViewRowCount(0);

            execute("DROP LIVE VIEW lv");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv");
                driveRefreshToQuiescence(job);
            }
            final LiveViewInstance recreated = instance();
            Assert.assertFalse(recreated.isInvalid());
            Assert.assertEquals(LiveViewState.SEED_STATE_ACTIVE, recreated.getStateReader().getSeedState());
            assertNoRefreshFaults("lv");
            assertViewMatchesRecompute();
            assertViewRowCount(5);
        });
    }

    @Test
    public void testASeedingViewWithOutputKeepsOnlyWhatItsSweepCommitted() throws Exception {
        // The sweep commits the first day's two rows, one turn each, and then cannot open the second day's
        // file. The spent budget invalidates the view, which keeps those two rows, a prefix of its output,
        // and nothing else, across a restart too.
        assertMemoryLeak(fault, () -> {
            final LiveViewInstance instance = createSeedingFixture("");
            fault.failOpensIn("2026-01-02");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final AtomicBoolean isRederiveStarted = recordTheRederive(job);
                spendTheFlushRetryBudget(job, instance, isRederiveStarted, 0);
                assertInvalidatedNotRederived(instance, isRederiveStarted, 5);
                assertFirstDayRows();
                assertStaysInvalidOnceTheFileOpens(job, instance);
                assertFirstDayRows();
            }
            assertInvalidAfterARestart();
            assertFirstDayRows();
        });
    }

    // What the spent budget leaves a seeding view with: never re-derived and never stopped on a refused
    // rebuild, but invalid with the budget's reason, still seeding, and without the base snapshot its sweep
    // pinned.
    private static void assertInvalidatedNotRederived(LiveViewInstance instance, AtomicBoolean isRederiveStarted, int retryCount) {
        Assert.assertFalse("a seeding view must not be re-derived from the applied base", isRederiveStarted.get());
        Assert.assertTrue(instance.isInvalid());
        TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
        Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
        Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
        Assert.assertEquals(retryCount, instance.getFlushRetryCount());
        Assert.assertNull("the invalidation releases the sweep's base snapshot", instance.getSeedBaseReader());
        capture.drain();
        Assert.assertEquals(retryCount - 1, capture.countOccurrences("live view refresh failed [view=lv"));
        Assert.assertEquals(1, capture.countOccurrences(BUDGET_EXHAUSTED_LINE));
        capture.assertNotLogged("live view re-derived from the applied base after base WAL loss");
        capture.assertNotLogged("live view could not re-derive from the applied base");
        capture.assertNotLogged("live view rebuild from the applied base refused");
        capture.assertNotLogged("live view seed sweep completed");
    }

    // Drives one pass at a time, each moving the clock extraMicrosPerPass further than a pass does, until
    // the view is invalidated, a refused rebuild stops it, or the re-derive starts. Returns the passes run.
    private static int spendTheFlushRetryBudget(
            LiveViewRefreshJob job,
            LiveViewInstance instance,
            AtomicBoolean isRederiveStarted,
            long extraMicrosPerPass
    ) {
        int passes = 0;
        while (passes < REFRESH_QUIESCENCE_PASSES
                && !isRederiveStarted.get()
                && !instance.isInvalid()
                && !instance.isCheckpointRecoveryBlocked()) {
            setCurrentMicros(currentMicros + extraMicrosPerPass);
            advanceClockToNextRefreshPass();
            job.run();
            drainWalQueue();
            passes++;
        }
        return passes;
    }

    private void assertFirstDayRows() throws Exception {
        assertQuery("SELECT * FROM lv ORDER BY created_at")
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(FIRST_DAY_ROWS);
        TestUtils.assertSqlCursors(engine, sqlExecutionContext, FIRST_DAY_RECOMPUTE_SQL, "(lv) ORDER BY 2, 1", LOG, true);
    }

    // A restart loads the invalidation from the view's state file.
    private void assertInvalidAfterARestart() throws Exception {
        shutdown();
        engine.buildViewGraphs();
        assertLiveViewsRow("lv\tinvalid\t" + BUDGET_EXHAUSTED_REASON + '\n');
        Assert.assertTrue(instance().isInvalid());
    }

    private void assertLiveViewsRow(String expectedRow) throws Exception {
        assertQuery("SELECT view_name, view_status, invalidation_reason FROM live_views() WHERE view_name = 'lv'")
                .noLeakCheck()
                .noRandomAccess()
                .returns("view_name\tview_status\tinvalidation_reason\n" + expectedRow);
    }

    // The file opens again and the job runs on: an invalid view refreshes no more, so its sweep never resumes.
    private void assertStaysInvalidOnceTheFileOpens(LiveViewRefreshJob job, LiveViewInstance instance) {
        fault.failOpensIn(null);
        driveRefreshToQuiescence(job);
        Assert.assertTrue(instance.isInvalid());
        TestUtils.assertEquals(BUDGET_EXHAUSTED_REASON, instance.getInvalidationReason());
        Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
        capture.drain();
        capture.assertNotLogged("live view seed sweep completed");
        capture.assertNotLogged("could not write live view seed checkpoint");
    }

    private void assertViewRowCount(long count) throws Exception {
        assertQuery("SELECT count() FROM lv")
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .returns("count\n" + count + '\n');
    }

    // tx with four rows on 2026-01-01 and 2026-01-02 in one commit, lv created over it and seeding up to that
    // commit, and a commit on 2026-01-03 that the base applies before the sweep's first turn, so the base
    // stands past the seed's target, which the re-derive needs to run at all.
    private LiveViewInstance createSeedingFixture(String dedupClause) throws Exception {
        execute("CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE) "
                + "TIMESTAMP(created_at) PARTITION BY DAY WAL " + dedupClause);
        insert("('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0), ('2026-01-01T09:10:00.000000Z', 'acct-2', 2.0), "
                + "('2026-01-02T09:00:00.000000Z', 'acct-1', 4.0), ('2026-01-02T09:10:00.000000Z', 'acct-2', 8.0)");
        fault.of(engine.verifyTableName("tx").getDirName());
        // The sweep's reader then opens the base's partitions afresh, which the fault needs.
        engine.releaseAllReaders();
        execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS " + VIEW_SQL);
        insert("('2026-01-03T09:00:00.000000Z', 'acct-1', 16.0)");
        final LiveViewInstance instance = instance();
        Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
        Assert.assertEquals(1, instance.getStateReader().getSeedTargetSeqTxn());
        Assert.assertEquals(2, engine.getTableSequencerAPI().getTxnTracker(engine.verifyTableName("tx")).getWriterTxn());
        return instance;
    }

    // Ahead of the re-derive's replay: records that the re-derive started, and lifts the fault.
    private AtomicBoolean recordTheRederive(LiveViewRefreshJob job) {
        final AtomicBoolean isRederiveStarted = new AtomicBoolean();
        hooks(job).setSimulateBaseApplyDuringRederive(() -> {
            isRederiveStarted.set(true);
            fault.failOpensIn(null);
        });
        return isRederiveStarted;
    }

    // One-row sweep turns, with no fault, until the sweep has committed the first day's two rows.
    private void sweepTheFirstDay(LiveViewInstance instance) throws Exception {
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            for (int pass = 0; pass < REFRESH_QUIESCENCE_PASSES && instance.getSeedDataOffset() < 2; pass++) {
                advanceClockToNextRefreshPass();
                job.run();
                drainWalQueue();
            }
        }
        Assert.assertEquals(2, instance.getSeedDataOffset());
        Assert.assertEquals(LiveViewState.SEED_STATE_SEEDING, instance.getStateReader().getSeedState());
        assertFirstDayRows();
    }
}
