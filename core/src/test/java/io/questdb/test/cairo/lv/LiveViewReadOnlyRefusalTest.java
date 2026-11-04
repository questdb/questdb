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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointRecoveryPhase;
import io.questdb.cairo.lv.LiveViewInMemoryTier;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * Coverage for the read-only-gate arm of {@code LiveViewRefreshJob.handleRefreshFailure}: an
 * authorization error a refresh turn meets neither invalidates the view nor counts toward its
 * flush-retry budget, and the view converges once the refusal stops.
 * <p>
 * No current refresh path produces such a refusal. The OSS engine does not gate
 * {@code getWalWriter}, and the enterprise engine exempts live-view tokens there, since every node
 * refreshes its live views locally. The engine installed below stands in for a gate on
 * the live-view writer acquire: while armed, its {@code getWalWriter} refuses every live-view
 * token with {@link CairoException#readOnlyAccess()}, the refusal a read-only gate throws.
 */
public class LiveViewReadOnlyRefusalTest extends AbstractLiveViewTest {
    // The gate's restore declines for a view with no timeline, and it falls back to the rebuild.
    private static final String GATE_ABSENT_TIMELINE_REBUILD_LINE = "live view cannot restore its runtime from the checkpoint timeline, "
            + "rebuilding from the applied base [view=lv, cause=mid-drain refresh failure, reason=timeline is absent]";
    private static final String REFUSAL_LINE = "live view refresh refused by read-only gate, retrying later [view=lv";
    // The line the restart's applied-base rebuild logs when it hands a fault a retry may get past
    // to handleRefreshFailure instead of invalidating the view.
    private static final String RESTART_REBUILD_RETRY_LINE = "live view restart rebuild hit a retryable fault, retrying [view=lv";
    private static final String RESTORE_LINE = "live view restored its runtime from the checkpoint timeline [view=lv, cause=mid-drain refresh failure";
    private static final String SIX_ROWS_OUTPUT = """
            created_at\taccount_id\tcumulative_sum
            2026-01-01T09:00:00.000000Z\tacct-1\t1.0
            2026-01-01T09:00:10.000000Z\tacct-2\t11.0
            2026-01-01T09:00:20.000000Z\tacct-1\t22.0
            2026-01-01T09:00:30.000000Z\tacct-2\t42.0
            2026-01-01T09:00:40.000000Z\tacct-1\t122.0
            2026-01-01T09:00:50.000000Z\tacct-2\t47.0
            """;
    private static final LogCapture capture = new LogCapture();
    private static final AtomicBoolean isLiveViewWalWriterRefused = new AtomicBoolean();
    // The view's window-state debt as the first refusal found it.
    private static final AtomicBoolean isWindowStateDirtyAtFirstRefusal = new AtomicBoolean();
    private static final AtomicInteger liveViewWalWriterRefusals = new AtomicInteger();

    @BeforeClass
    public static void setUpStatic() throws Exception {
        // Installed before the engine is built. Inert unless a test arms isLiveViewWalWriterRefused;
        // AbstractCairoTest.tearDownStatic resets the factory for the next class.
        AbstractCairoTest.engineFactory = conf -> new CairoEngine(conf) {
            @Override
            public @NotNull WalWriter getWalWriter(TableToken tableToken) {
                if (tableToken.isLiveView() && isLiveViewWalWriterRefused.get()) {
                    if (liveViewWalWriterRefusals.getAndIncrement() == 0) {
                        final LiveViewInstance instance = getLiveViewRegistry().getViewInstance(tableToken.getTableName());
                        isWindowStateDirtyAtFirstRefusal.set(instance != null && instance.isWindowStateDirty());
                    }
                    throw CairoException.readOnlyAccess();
                }
                return super.getWalWriter(tableToken);
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @After
    public void resetRefusalAndClock() {
        isLiveViewWalWriterRefused.set(false);
        capture.stop();
        setCurrentMicros(-1);
    }

    @Before
    public void setUpRefusalAndCadence() {
        isLiveViewWalWriterRefused.set(false);
        isWindowStateDirtyAtFirstRefusal.set(false);
        liveViewWalWriterRefusals.set(0);
        // One logical boundary per commit, so the view has a timeline for the restart case to remove.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
        capture.start();
    }

    @Test
    public void testARefusalOfTheEmergencyLeadFlushWriterCarriesTheWindowStateDebt() throws Exception {
        // Readers pin both slots of the view's in-memory tier, so the turn's lead publish stalls and
        // finishLeadRefresh flushes the lead straight to disk, and the read-only gate refuses the writer
        // that emergency flush acquires. The turn already fed the commit's row to the window functions,
        // but nothing committed and the refresh cursor stayed in front of the commit, so the next turn
        // drains it again. The refusal must carry the window-state debt onto the view: that turn then
        // restores the runtime before it drains, instead of counting the row a second time.
        assertMemoryLeak(() -> {
            createViewOverFourRows();
            final LiveViewInstance instance = instance();
            final LiveViewInMemoryTier tier = instance.getInMemoryTier();
            Assert.assertNotNull("the fixture must have staged the in-memory tier", tier);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                final boolean isDebtCarried;
                // A reader pins the published slot, so the next lead publish swaps to the other slot,
                // which a second reader pins in turn. FLUSH EVERY stays not due, so the lead stays in RAM.
                final int pinA = tier.acquireRead();
                Assert.assertNotEquals("the tier must hand out a read pin", -1, pinA);
                int pinB = -1;
                try {
                    execute("INSERT INTO tx VALUES ('2026-01-01T09:00:40.000000Z', 'acct-1', 100.0)");
                    drainWalQueue();
                    instance.setLastFlushTimeUs(currentMicros);
                    job.run();
                    pinB = tier.acquireRead();
                    Assert.assertNotEquals("the tier must hand out a second read pin", -1, pinB);
                    Assert.assertNotEquals("both slots must be pinned", pinA, pinB);
                    Assert.assertEquals("the commit must stay an un-flushed lead", 1, instance.getLeadRowCount());
                    final long durableSeqTxn = instance.getLastProcessedSeqTxn();
                    final long refreshedUpToBeforeRefusal = instance.getRefreshedUpToSeqTxn();
                    Assert.assertTrue("the lead must lead the durable output", refreshedUpToBeforeRefusal > durableSeqTxn);

                    // Both slots pinned: the publish stalls, and the emergency flush meets the refusal.
                    execute("INSERT INTO tx VALUES ('2026-01-01T09:00:50.000000Z', 'acct-2', 5.0)");
                    drainWalQueue();
                    instance.setLastFlushTimeUs(currentMicros);
                    isLiveViewWalWriterRefused.set(true);
                    job.run();
                    Assert.assertTrue("the emergency flush must have met the refusal", liveViewWalWriterRefusals.get() > 0);
                    Assert.assertEquals("the refused flush commits nothing", durableSeqTxn, instance.getLastProcessedSeqTxn());
                    Assert.assertEquals(
                            "the refused flush must leave the refresh cursor in front of the commit",
                            refreshedUpToBeforeRefusal,
                            instance.getRefreshedUpToSeqTxn()
                    );
                    assertRefusalNeitherInvalidatesNorCharges(instance);
                    capture.drain();
                    capture.assertLogged("live view in-mem tier stalled, both slots pinned [view=lv");
                    capture.assertLogged(REFUSAL_LINE);
                    isDebtCarried = instance.isWindowStateDirty();
                } finally {
                    if (pinB != -1) {
                        tier.releaseRead(pinB);
                    }
                    tier.releaseRead(pinA);
                }
                Assert.assertTrue("the refusal must carry the debt the turn's drain left onto the view", isDebtCarried);

                // The refusal stops, and the next drive drains the refused commit again.
                isLiveViewWalWriterRefused.set(false);
                driveRefreshToQuiescence(job);
                assertViewMatchesRecompute();
                assertViewRows(SIX_ROWS_OUTPUT);
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(
                        engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tx")),
                        instance.getLastProcessedSeqTxn()
                );
                assertRefusalNeitherInvalidatesNorCharges(instance);
                capture.drain();
                capture.assertLogged(RESTORE_LINE);
                capture.assertNotLogged("live view refresh failed");
            }
        });
    }

    @Test
    public void testARefusalOfTheLeadFlushWriterKeepsTheViewActiveAndUncharged() throws Exception {
        // A running view's turn drains a base commit into its un-flushed lead, and the read-only gate
        // refuses the writer the lead flush acquires. The refusal is no refresh failure: the view
        // stays valid and nothing is charged. The refused turn carries its window-state debt, so once
        // the refusal stops, the next driven turn's gate restores the runtime from the checkpoint
        // timeline, re-derives the lead, and flushes it.
        assertMemoryLeak(() -> {
            createViewOverFourRows();
            final LiveViewInstance instance = instance();
            final long durableSeqTxn = instance.getLastProcessedSeqTxn();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO tx VALUES ('2026-01-01T09:00:40.000000Z', 'acct-1', 100.0)");
                drainWalQueue();
                // Past FLUSH EVERY, so the turn that drains the commit also flushes it.
                setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
                isLiveViewWalWriterRefused.set(true);
                // The commit's notification drives the turn: the lead drain, then the refused flush.
                job.run();
                Assert.assertTrue("the lead flush must have met the refusal", liveViewWalWriterRefusals.get() > 0);
                Assert.assertEquals("the refused flush commits nothing", durableSeqTxn, instance.getLastProcessedSeqTxn());
                assertRefusalNeitherInvalidatesNorCharges(instance);
                Assert.assertTrue(
                        "the turn must have drained the commit into the lead",
                        instance.getRefreshedUpToSeqTxn() > durableSeqTxn
                );

                // A pass while the refusal holds. Whether it drives the view again is the job's pacing,
                // which this case leaves alone; whatever it does, it must neither invalidate nor charge.
                job.run();
                assertRefusalNeitherInvalidatesNorCharges(instance);
                capture.drain();
                capture.assertLogged(REFUSAL_LINE);
                capture.assertNotLogged("live view refresh failed");
                capture.assertNotLogged("live view refresh budget exhausted");

                // The refusal stops; the next driven turn's gate settles the carried debt.
                capture.drain();
                final int restoresBeforeLift = capture.countOccurrences(RESTORE_LINE);
                isLiveViewWalWriterRefused.set(false);
                driveRefreshToQuiescence(job);
                Assert.assertEquals(
                        engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tx")),
                        instance.getLastProcessedSeqTxn()
                );
                assertRefusalNeitherInvalidatesNorCharges(instance);
                Assert.assertFalse(instance.isWindowStateDirty());
                assertViewMatchesRecompute();
                capture.drain();
                Assert.assertTrue(
                        "a gate must restore the runtime once the refusal stops",
                        capture.countOccurrences(RESTORE_LINE) > restoresBeforeLift
                );

                // The runtime has its running totals after the refusal: a cold one would answer this
                // row's own amount.
                insertAccount(job, "2026-01-01T09:00:50.000000Z", "acct-2", 5.0);
                assertViewMatchesRecompute();
                assertViewRows(SIX_ROWS_OUTPUT);
            }
            assertRefusalNeitherInvalidatesNorCharges(instance);
            capture.drain();
            capture.assertNotLogged("live view refresh failed");
        });
    }

    @Test
    public void testARefusalOfTheRestartRebuildsWriterKeepsTheViewActiveAndUncharged() throws Exception {
        // The restart finds no timeline, so it rebuilds the view from the applied base, and the
        // read-only gate refuses the writer the rebuild's replacement acquires. The rebuild hands the
        // refusal to handleRefreshFailure rather than invalidating the view, and that leaves it
        // uncharged. The window-state debt the restart leaves keeps the drain off the wiped runtime
        // until a turn whose rebuild gets its writer settles it.
        assertMemoryLeak(() -> {
            createViewOverFourRows();
            removeTimeline();
            engine.releaseInactive();
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            isLiveViewWalWriterRefused.set(true);
            try (LiveViewRefreshJob restarted = new LiveViewRefreshJob(0, engine, 1)) {
                // The fallback scan drives the restart's one recovery attempt, which reports no work.
                driveRefreshToQuiescence(restarted);
                Assert.assertTrue("the restart rebuild must have met the refusal", liveViewWalWriterRefusals.get() > 0);
                Assert.assertTrue(
                        "the refusal must strike after the rebuild owes the window state",
                        isWindowStateDirtyAtFirstRefusal.get()
                );

                final LiveViewInstance instance = instance();
                assertRefusalNeitherInvalidatesNorCharges(instance);
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertTrue("the restart's recovery attempt is spent", instance.isCheckpointRestoreAttempted());
                // handleRefreshFailure does not recover a refusal in its turn, and nothing drives a view
                // with no commits in front of it, so the debt stands.
                Assert.assertTrue(instance.isWindowStateDirty());
                Assert.assertEquals(1L, instance.getCheckpointRebuildAttempts());
                // A rebuild that hands its fault on records no restart route.
                Assert.assertEquals(
                        LiveViewCheckpointRecoveryPhase.routeName(LiveViewCheckpointRecoveryPhase.ROUTE_NONE),
                        LiveViewCheckpointRecoveryPhase.routeName(instance.getCheckpointRestoreRoute())
                );
                // The refused rebuild never got a writer, so the durable output stands as it was.
                assertViewMatchesRecompute();
                capture.drain();
                capture.assertLogged(REFUSAL_LINE);
                capture.assertOnlyOnce(Pattern.quote(RESTART_REBUILD_RETRY_LINE));
                capture.assertNotLogged("live view restart applied-base rebuild failed [view=");
                capture.assertNotLogged("live view refresh failed");
                capture.assertNotLogged("live view refresh budget exhausted");

                // A commit while the refusal holds. Its notification drives the turn whose gate owes the
                // rebuild, and the gate's rebuild meets the same refusal. Driven one pass at a time: how
                // soon the job re-drives a refused view is its pacing, which this case leaves alone.
                execute("INSERT INTO tx VALUES ('2026-01-01T09:00:40.000000Z', 'acct-1', 100.0)");
                drainWalQueue();
                final int refusalsBeforeCommitTurn = liveViewWalWriterRefusals.get();
                restarted.run();
                Assert.assertTrue(
                        "the commit's turn must have met the refusal",
                        liveViewWalWriterRefusals.get() > refusalsBeforeCommitTurn
                );
                assertRefusalNeitherInvalidatesNorCharges(instance);
                Assert.assertTrue("the debt stands while the rebuild is refused", instance.isWindowStateDirty());

                // The refusal stops. The next drive's gate rebuilds with the restatement guard armed,
                // pins the base's applied head, and so consumes the commit.
                isLiveViewWalWriterRefused.set(false);
                driveRefreshToQuiescence(restarted);
                assertRefusalNeitherInvalidatesNorCharges(instance);
                Assert.assertFalse(instance.isCheckpointRecoveryBlocked());
                Assert.assertFalse(instance.isWindowStateDirty());
                Assert.assertEquals(
                        engine.getTableSequencerAPI().lastTxn(engine.verifyTableName("tx")),
                        instance.getLastProcessedSeqTxn()
                );
                assertViewMatchesRecompute();
                capture.drain();
                // Once for the refused commit turn, once for the converging drive.
                Assert.assertEquals(
                        "the converging drive must have taken the gate's absent-timeline rebuild",
                        2,
                        capture.countOccurrences(GATE_ABSENT_TIMELINE_REBUILD_LINE)
                );
                capture.assertNotLogged("live view refresh failed");

                // The rebuilt runtime keeps the running totals: a cold one would answer this row's own
                // amount.
                insertAccount(restarted, "2026-01-01T09:00:50.000000Z", "acct-2", 5.0);
                assertViewMatchesRecompute();
                assertViewRows(SIX_ROWS_OUTPUT);
            }

            // The gate's rebuild sealed a fresh timeline, which the next restart restores from.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob resumed = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(resumed);
            }
            assertRestoredFromTimeline("lv");
            assertRefusalNeitherInvalidatesNorCharges(instance());
            assertViewMatchesRecompute();
        });
    }

    private static void assertRefusalNeitherInvalidatesNorCharges(LiveViewInstance instance) {
        Assert.assertFalse(
                "a read-only-gate refusal must not invalidate the view [reason=" + instance.getInvalidationReason() + ']',
                instance.isInvalid()
        );
        Assert.assertFalse("a read-only-gate refusal must not stash an invalidation", instance.hasPendingInvalidationReason());
        Assert.assertEquals("a read-only-gate refusal must not count toward the flush-retry budget", 0, instance.getFlushRetryCount());
        Assert.assertEquals(
                "a read-only-gate refusal must not start a flush-retry streak",
                Numbers.LONG_NULL,
                instance.getFlushRetryStartUs()
        );
    }

    private static LiveViewInstance instance() {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
        Assert.assertNotNull("live view 'lv' must be registered", instance);
        return instance;
    }

    private static void removeTimeline() {
        try (
                Path dir = new Path().of(configuration.getDbRoot())
                        .concat(instance().getLiveViewToken())
                        .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
                Path timelinePath = new Path()
        ) {
            LiveViewCheckpointLayout.timelinePath(timelinePath, dir);
            Assert.assertTrue(
                    "the fixture must have published a timeline to remove",
                    configuration.getFilesFacade().removeQuiet(timelinePath.$())
            );
        }
    }

    private void assertViewMatchesRecompute() throws Exception {
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                """
                        (
                            SELECT created_at, account_id,
                                sum(amount) OVER (PARTITION BY account_id, bucket ORDER BY created_at
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sum
                            FROM (
                                SELECT created_at, account_id, amount,
                                    timestamp_floor('1d', created_at, '1970-01-01T00:00:00.000000Z'::timestamp) AS bucket
                                FROM tx
                            )
                        ) ORDER BY 2, 1
                        """,
                "(lv) ORDER BY 2, 1",
                LOG,
                true
        );
    }

    private void assertViewRows(String expected) throws Exception {
        assertQuery("SELECT created_at, account_id, cumulative_sum FROM lv")
                .noLeakCheck()
                .timestamp("created_at")
                .expectSize()
                .returns(expected);
    }

    private void createViewOverFourRows() throws Exception {
        execute("""
                CREATE TABLE tx (created_at TIMESTAMP, account_id SYMBOL, amount DOUBLE)
                TIMESTAMP(created_at) PARTITION BY HOUR WAL
                """);
        execute("""
                CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS
                SELECT created_at, account_id, sum(amount) OVER w AS cumulative_sum
                FROM tx WINDOW w AS (PARTITION BY account_id ORDER BY created_at ANCHOR DAILY '00:00')
                """);
        try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
            driveSeedToCompletion(job, "lv");
            execute("""
                    INSERT INTO tx VALUES
                        ('2026-01-01T09:00:00.000000Z', 'acct-1', 1.0),
                        ('2026-01-01T09:00:10.000000Z', 'acct-2', 11.0),
                        ('2026-01-01T09:00:20.000000Z', 'acct-1', 21.0),
                        ('2026-01-01T09:00:30.000000Z', 'acct-2', 31.0)
                    """);
            drainWalQueue();
            driveRefreshToQuiescence(job);
        }
        assertViewMatchesRecompute();
        assertNoRefreshFaults("lv");
    }

    private void insertAccount(LiveViewRefreshJob job, String timestamp, String account, double amount) throws Exception {
        execute("INSERT INTO tx VALUES ('" + timestamp + "', '" + account + "', " + amount + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }
}
