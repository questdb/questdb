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
import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Correctness suite for live views over a DEDUP-enabled base table. Such a view
 * takes the coupled, applied-reader refresh path: instead of appending the base's
 * raw (pre-dedup) WAL stream, the refresh worker reads the applied, post-dedup base
 * via a {@code TableReader} and routes any timestamp-overlap batch through the O3
 * replay machinery. See {@code LiveViewRefreshJob#drainAppliedBase}.
 * <p>
 * All tests run under the default (V1) sequencer
 * ({@code cairo.default.seq.part.txn.count = 0}), so any accidental reliance on the
 * V2-only {@code TransactionLogCursor.getTxnMinTimestamp()} would throw immediately;
 * the overlap trigger must source min ts from the base WAL-E event file instead.
 */
public class LiveViewDedupBaseTest extends AbstractLiveViewTest {

    // Pin the test clock below all test data before each test. A non-SEED view's
    // lower bound is the CREATE wall-clock moment, and the forward-append refresh path
    // drops rows below it. The test data is timestamped in the past, so without a
    // pinned clock every row would be dropped as pre-CREATE.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    @Test
    public void testAdditiveSameTimestampAcrossCommitsStaysCorrect() throws Exception {
        // Many keys share one ts across separate commits: each later commit's minTs
        // equals the frontier. The applied-reader path would over-trigger a replay here;
        // Phase 2a proves the range clean (no dedup) and takes the cheap raw-WAL append
        // instead. Assert both correctness and that the clean raw-WAL path engaged.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('c', 30, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                // With no dedup in any commit, the additive same-ts commits route through
                // the cheap raw-WAL path (pre-Phase-2a every one of them would have replayed).
                Assert.assertTrue(
                        "additive same-ts commits must take the cheap raw-WAL path, not replay",
                        instance.getDedupRawWalCleanCycles() > 0
                );
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY sym")
                    .noLeakCheck()
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "b\t20\t2026-01-01T00:00:01.000000Z\n" +
                            "c\t30\t2026-01-01T00:00:01.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAlterDedupEnableDisableFlipsCadence() throws Exception {
        // Create over a NON-dedup base (lead-eligible), leave an un-flushed lead, then
        // ALTER ... DEDUP ENABLE. The first coupled cycle must reconcile the stale lead
        // by rebuilding from the applied base, and a frontier-ts duplicate must
        // collapse. Then DISABLE and confirm a later forward row still appends.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: flushed to disk (first flush).
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Batch 2 within FLUSH EVERY (clock still 0): stays as an un-flushed lead.
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 25, '2026-01-01T00:00:03.000000Z'), " +
                        "('a', 35, '2026-01-01T00:00:04.000000Z')");
                drainWalQueue();
                drainJob(job);

                // Enable dedup: the view must flip to the coupled path next cycle.
                execute("ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, sym)");
                drainWalQueue();

                // A dedup replacement at the frontier ts=02 (val 20 -> 99). The first
                // coupled cycle resumes from the disk point, sees the pending lead
                // commits at/below the frontier, replays, and drops the stale lead.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 99, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t99\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t25\t2026-01-01T00:00:03.000000Z\t3\n" +
                            "a\t35\t2026-01-01T00:00:04.000000Z\t4\n");

            // Disable dedup and confirm the view returns to normal forward appending.
            // Let the structural DISABLE commit settle in its own cycle before the
            // data insert, so the two do not share a single lead-drain range.
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("ALTER TABLE base DEDUP DISABLE");
                drainWalQueue();
                setCurrentMicros(4_000_000L);
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(6_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 50, '2026-01-01T00:00:05.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "a\t99\t2026-01-01T00:00:02.000000Z\n" +
                            "a\t25\t2026-01-01T00:00:03.000000Z\n" +
                            "a\t35\t2026-01-01T00:00:04.000000Z\n" +
                            "a\t50\t2026-01-01T00:00:05.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testAnchoredWindowReplayOverDedupBase() throws Exception {
        // Anchored window (a per-hour cumulative sum that resets at each anchor
        // bucket) over a dedup base. A below-frontier dedup replacement inside the
        // FIRST hour bucket routes through the O3 replay, which must restore the
        // anchor-map state and recompute only that bucket's downstream sums while the
        // SECOND bucket (a different anchor value) stays untouched. This exercises the
        // anchor-map snapshot/restore contract on the coupled dedup path, distinct
        // from the un-anchored cumulative/row_number tests.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT sym, val, ts, " +
                    "sum(val) OVER w AS cum FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Bucket 00:00 -> cum 10, 30; bucket 01:00 (anchor reset) -> cum 30, 70.
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10.0, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20.0, '2026-01-01T00:30:00.000000Z'), " +
                        "('a', 30.0, '2026-01-01T01:00:01.000000Z'), " +
                        "('a', 40.0, '2026-01-01T01:30:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace the mid-bucket row ts=00:30 (below the frontier ts=01:30):
                // val 20 -> 200. Only bucket 00:00 recomputes (cum 10, 210); the
                // anchor reset keeps bucket 01:00 at 30, 70.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 200.0, '2026-01-01T00:30:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, cum FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tcum\n" +
                            "a\t10.0\t2026-01-01T00:00:01.000000Z\t10.0\n" +
                            "a\t200.0\t2026-01-01T00:30:00.000000Z\t210.0\n" +
                            "a\t30.0\t2026-01-01T01:00:01.000000Z\t30.0\n" +
                            "a\t40.0\t2026-01-01T01:30:00.000000Z\t70.0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test(timeout = 120_000)
    public void testAppliedBaseDrainDefersOnApplyLagInsteadOfDeadlocking() throws Exception {
        // Cooperative apply-lag handoff on the coupled dedup path (regression for
        // M-1). When the base sequencer head advances past the applied reader, the
        // drainAppliedBase gate must NOT block-spin in waitForApply waiting for the
        // apply: on the single-threaded refresh/drain model the same worker has to
        // advance that apply, so spinning deadlocks (and with the flush clock frozen
        // the retry budget never trips); with a live clock a sustained-lag streak
        // instead ticks the flush-retry budget to exhaustion and durably invalidates
        // the view. ensureBaseApplied now peeks the applied seqTxn BEFORE pinning the
        // scan reader and throws a cooperative signal that unwinds the cycle untouched
        // - no watermark advance, no retry-budget tick, no invalidation - and the next
        // tick converges once the base apply lands.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Seed and fully refresh an initial row so the coupled path has an
                // established frontier and watermarks.
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                final long processedBefore = instance.getLastProcessedSeqTxn();
                final long latestSeenBefore = instance.getLatestSeenTs();
                final int retryBefore = instance.getFlushRetryCount();

                // Commit a second row to the base sequencer but do NOT apply it
                // (skip drainWalQueue): the sequencer head advances past the applied
                // reader, so isRangeProvablyClean fails and the coupled
                // drainAppliedBase gate observes the apply lag. Advance the clock past
                // FLUSH EVERY so the flush-due gate lets this cycle reach the drain.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:02.000000Z')");

                // With the old block-spin this drain never returns (the frozen clock
                // keeps the retry budget from tripping). It must return, and it must
                // leave the view exactly as it was.
                drainJob(job);

                Assert.assertFalse("apply lag must not invalidate the coupled view", instance.isInvalid());
                Assert.assertEquals("watermark must not advance on apply lag",
                        processedBefore, instance.getLastProcessedSeqTxn());
                Assert.assertEquals("frontier must not advance on apply lag",
                        latestSeenBefore, instance.getLatestSeenTs());
                Assert.assertEquals("apply lag must not tick the flush-retry budget",
                        retryBefore, instance.getFlushRetryCount());

                // Apply the base commit, advance the clock past the apply-lag
                // back-off, then let the next tick converge the drain.
                drainWalQueue();
                setCurrentMicros(currentMicros + 1_000_000);
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must stay valid after the drain converges", instance.isInvalid());
            }
            // Both rows land in ts order with a gapless row_number sequence.
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testApplyLagBackOffThrottlesRedrainWithinWindow() throws Exception {
        // The apply-lag back-off floor must THROTTLE re-draining, not just permit
        // convergence. When a coupled dedup drain defers on base apply lag it arms a
        // short back-off (LiveViewRefreshJob.refreshInstance); a refresh tick that
        // enters the view inside that window must skip the whole cycle at the pre-latch
        // guard rather than re-enter drainAppliedBase, re-observe the same lag, and burn
        // a full window recompute every tick until apply lands. The companion
        // testAppliedBaseDrainDefersOnApplyLagInsteadOfDeadlocking proves the
        // cooperative unwind returns and leaves the view untouched; this proves the
        // throttle itself engages. Because the fallback scan gates on the APPLIED base
        // point (getWriterTxn), which does not advance during apply lag, a re-entry has
        // to come from a fresh notification (a new committed-but-unapplied base commit);
        // this test posts one inside the window. Deleting either the arm or the
        // pre-latch guard fails an assertion below.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Establish the coupled frontier and watermarks with a fully applied
                // first row.
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Commit a second row to the base sequencer but do NOT apply it (skip
                // drainWalQueue): the commit posts a refresh notification, and the
                // coupled drainAppliedBase gate then observes the apply lag. Freeze the
                // clock at a known point past FLUSH EVERY so the flush-due gate lets this
                // cycle reach the drain.
                final long deferArmedAtUs = 2_000_000L;
                final long backOffUs = 5_000L; // APPLY_LAG_DEFER_BACKOFF_US
                setCurrentMicros(deferArmedAtUs);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:02.000000Z')");
                drainJob(job);

                // The cooperative unwind armed the back-off floor at now + backOffUs.
                // Deleting the arm leaves the floor LONG_NULL, so this fails.
                Assert.assertEquals("apply-lag defer must arm a back-off floor",
                        deferArmedAtUs + backOffUs, instance.getApplyLagDeferUntilUs());

                // Step the clock forward but stay strictly INSIDE the back-off window,
                // then commit (again without applying) a third row so its notification
                // re-enters the view this tick. The re-entry must be throttled: the
                // pre-latch guard skips the cycle, so the floor is left exactly where it
                // was. Deleting the guard lets the tick re-enter the drain, re-observe
                // the lag, and re-arm the floor to (now + backOffUs) = a strictly LATER
                // value, so the assertion fails.
                setCurrentMicros(deferArmedAtUs + 2_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('c', 30, '2026-01-01T00:00:03.000000Z')");
                drainJob(job);
                Assert.assertEquals("a tick inside the back-off window must not re-drain (floor unchanged)",
                        deferArmedAtUs + backOffUs, instance.getApplyLagDeferUntilUs());
                Assert.assertFalse("a throttled tick must not invalidate the view", instance.isInvalid());

                // Apply the base commits, step past the back-off floor, and let the next
                // tick converge the drain.
                drainWalQueue();
                setCurrentMicros(deferArmedAtUs + backOffUs + 1_000_000L);
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must stay valid after the drain converges", instance.isInvalid());
                // recordRefreshSuccess must clear the back-off floor once the drain
                // converges. Deleting that clear leaves the stale floor here, so this fails.
                Assert.assertEquals("a converged refresh must clear the back-off floor",
                        Numbers.LONG_NULL, instance.getApplyLagDeferUntilUs());
            }
            // All three rows land in ts order with a gapless row_number sequence.
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "c\t30\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testApplyLagBackOffClearsEarlyWhenBaseAppliesBeforeFloor() throws Exception {
        // The apply-lag back-off floor is only an anti-spin bound; the real
        // precondition is the base applying past the seqTxn that forced the defer.
        // When apply catches up while the clock is still strictly BELOW the floor,
        // the pre-latch guard in LiveViewRefreshJob.refreshInstance must re-check
        // getWriterTxn against the armed target, clear the floor, and drain this
        // tick rather than stall until the wall-clock floor elapses - which a
        // frozen test clock never crosses. The companion
        // testApplyLagBackOffThrottlesRedrainWithinWindow proves the throttle
        // engages while apply still lags; this proves the early clear fires the
        // moment apply catches up. Reverting the early-clear re-check leaves the
        // pre-latch guard a plain clock comparison, so under the frozen clock the
        // deferred row never converges and the assertions below fail.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Establish the coupled frontier with a fully applied first row.
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                final long processedAfterFirst = instance.getLastProcessedSeqTxn();

                // Commit a second row to the sequencer but do NOT apply it: the
                // coupled drainAppliedBase gate observes the apply lag and the
                // cooperative unwind arms the back-off floor at now + backOffUs.
                final long deferArmedAtUs = 2_000_000L;
                final long backOffUs = 5_000L; // APPLY_LAG_DEFER_BACKOFF_US
                setCurrentMicros(deferArmedAtUs);
                execute("INSERT INTO base (sym, val, ts) VALUES ('b', 20, '2026-01-01T00:00:02.000000Z')");
                drainJob(job);
                Assert.assertEquals("apply-lag defer must arm a back-off floor",
                        deferArmedAtUs + backOffUs, instance.getApplyLagDeferUntilUs());
                Assert.assertEquals("watermark must not advance while deferred",
                        processedAfterFirst, instance.getLastProcessedSeqTxn());

                // Apply the base commit so getWriterTxn passes the armed target,
                // but keep the clock frozen strictly BELOW the floor. The re-entry
                // now finds apply caught up: the pre-latch guard must clear the
                // floor early and drain this tick.
                drainWalQueue();
                Assert.assertTrue("clock must stay below the floor to isolate the early clear",
                        engine.getConfiguration().getMicrosecondClock().getTicks() < deferArmedAtUs + backOffUs);
                drainJob(job);
                drainWalQueue();

                Assert.assertFalse("early-clear convergence must not invalidate the view", instance.isInvalid());
                Assert.assertEquals("apply catching up below the floor must clear it early",
                        Numbers.LONG_NULL, instance.getApplyLagDeferUntilUs());
                Assert.assertTrue("the deferred row must be processed after the early clear",
                        instance.getLastProcessedSeqTxn() > processedAfterFirst);
            }
            // Both rows land in ts order with a gapless row_number sequence.
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testBaseTruncateFreezesDerivedPrefix() throws Exception {
        // A base TRUNCATE below the frontier is a data-shaped non-DATA commit
        // (walId>0, isDataType=false): the WAL-E walk excludes it from batchMinTs, so
        // no history-rewriting replay fires and the LV's derived prefix stays frozen.
        // A following in-order commit still appends.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Remove all base history below the frontier.
                setCurrentMicros(2_000_000L);
                execute("TRUNCATE TABLE base");
                drainWalQueue();
                drainJob(job); // frozen: no replay, no change to the derived prefix
                drainWalQueue();

                // A later forward row still appends on top of the frozen prefix.
                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\n" +
                            "a\t30\t2026-01-01T00:00:03.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testChangeDedupKeysWhileDedupEnabled() throws Exception {
        // Changing the dedup UPSERT keys on an already-dedup base (no DISABLE first) is a
        // structural op that does NOT invalidate the view - SET_DEDUP_ENABLE carries no
        // mat-view/live-view invalidation reason - and the refresh cadence stays coupled
        // to the applied, post-dedup base. After switching the key set, a below-frontier
        // UPSERT that now collides under the NEW keys must collapse in the view; under the
        // OLD keys the same row would have been a new insert. That the row collapses (not
        // appends) proves the coupled path reads the re-keyed dedup result, not the raw
        // WAL stream.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('b', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must start valid",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());

                // Switch the dedup keys in place: (ts, sym) -> (ts, val). No DISABLE
                // needed; the change must not invalidate the view.
                execute("ALTER TABLE base DEDUP ENABLE UPSERT KEYS(ts, val)");
                drainWalQueue();
                Assert.assertFalse("changing dedup keys must not invalidate the LV",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());

                // A below-frontier UPSERT keyed (ts=01, val=10) now collides with
                // ('a', 10, 01) and replaces its sym; under the OLD (ts, sym) keys it
                // would have been a distinct row. The coupled O3 replay must collapse it.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('c', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                Assert.assertFalse("view must stay valid after the re-keyed replace",
                        engine.getLiveViewRegistry().getViewInstance("lv").isInvalid());
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "c\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "b\t20\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateRejectsUnsnapshottableAnchorKeyOverDedupBase() throws Exception {
        // Replay is the normal correction path for a dedup base, and replay restores
        // window state from a checkpoint whose anchor map is keyed by the PARTITION BY
        // columns. A key type the snapshot codec cannot persist (here UUID) must be
        // rejected at CREATE rather than pass and then serve wrong results at the first
        // replay. The reject is enforced by the per-function supportsCheckpointState() check,
        // which folds in LiveViewSnapshotKeyCodec.isAllTypesSupported over the same
        // partition keys and runs for every view. This test pins the observable contract:
        // such a view does not silently create over a dedup base.
        assertMemoryLeak(() -> {
            // sym drives the dedup keys; u (UUID) is the unsupported anchor partition key.
            execute("CREATE TABLE base (sym SYMBOL, u UUID, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT u, val, ts, " +
                        "sum(val) OVER w AS cum FROM base " +
                        "WINDOW w AS (PARTITION BY u ORDER BY ts ANCHOR EXPRESSION timestamp_floor('1h', ts))");
                Assert.fail("expected SqlException for an unsnapshottable anchor key over a DEDUP base");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("incremental snapshot is not supported")
                );
            }
            Assert.assertNull(
                    "the rejected view must not have been created",
                    engine.getLiveViewRegistry().getViewInstance("lv")
            );
        });
    }

    @Test
    public void testDeepDedupRecomputesDownstreamWindow() throws Exception {
        // Replace the OLDEST row (below the head checkpoint): the head-miss full
        // rebuild must reflect the new value and recompute every downstream cumulative
        // sum in the same partition frame.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT sym, val, ts, " +
                    "sum(val) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS cum FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10.0, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20.0, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30.0, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace ts=01: val 10.0 -> 100.0. cum must become 100/120/150.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 100.0, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, cum FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tcum\n" +
                            "a\t100.0\t2026-01-01T00:00:01.000000Z\t100.0\n" +
                            "a\t20.0\t2026-01-01T00:00:02.000000Z\t120.0\n" +
                            "a\t30.0\t2026-01-01T00:00:03.000000Z\t150.0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFilteredLowestRowReplacementIsDeleted() throws Exception {
        // A below-frontier dedup replacement flips the LOWEST result row's value so it
        // fails the WHERE filter; the row must be DELETED (the recompute over the
        // applied base no longer has it). Regression for the o3HeadMissReplay boundary:
        // the rebuild's first row is above the filtered ts, so replayMinTs jumps past
        // it and a replayMinTs-only REPLACE_RANGE would leave the stale row frozen.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS SELECT sym, val, ts, " +
                    "first_value(val) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS fv " +
                    "FROM base WHERE val > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace the lowest row ts=01: val 10 -> -5. It now fails WHERE val>0,
                // so the recompute drops it and the view must too.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', -5, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, fv FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tfv\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\t20\n" +
                            "a\t30\t2026-01-01T00:00:03.000000Z\t20\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testFrontierTimestampReplacementReflectedNoDuplicate() throws Exception {
        // A dedup UPSERT replaces the most-recent row at exactly the frontier ts. The
        // raw-WAL cross-commit trigger (txnMinTs < latestSeen, strict) would miss this
        // equality case and append a duplicate; the applied-reader path routes it to a
        // replay that rewrites the row in place. Assert the replaced value with no
        // duplicate row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z'), " +
                        "('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 99, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t99\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testIntraCommitEqualTimestampCollapse() throws Exception {
        // A single commit carries two rows with identical (ts, keys) at a brand-new ts
        // strictly above the frontier. The raw-WAL path would append both (equal ts
        // does not set the intra-commit out-of-order flag); the applied-reader path
        // sees the collapsed single row. One output row with the last value proves the
        // forward path reads the post-dedup base, not raw WAL.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 30, '2026-01-01T00:00:03.000000Z'), " +
                        "('a', 40, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t40\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRealDedupFallsBackToAppliedReplay() throws Exception {
        // Phase 2a routing discriminator. A forward (additive) commit over a warm signal
        // takes the cheap raw-WAL path; the next commit is a real dedup replacement, which
        // advances the divergence watermark so the gate fails and that cycle falls back to
        // the applied-reader replay. Assert the clean-cycle counter advances on the forward
        // commit but NOT on the dedup commit, and that the replaced value is reflected.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: initial rows (the first cycle warms the signal).
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Batch 2: a forward row strictly above the frontier, no dedup. The signal
                // is warm and the range is clean -> cheap raw-WAL append.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 30, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long cleanAfterForward = instance.getDedupRawWalCleanCycles();
                Assert.assertTrue(
                        "forward additive commit must take the cheap raw-WAL path",
                        cleanAfterForward > 0
                );

                // Batch 3: a dedup replacement at existing ts=02 (val 20 -> 99). The batch
                // dedups, so the divergence watermark advances past the range's lower bound
                // and the gate fails -> fall back to the applied-reader replay. The clean-
                // cycle counter must not move.
                setCurrentMicros(4_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 99, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "dedup commit must fall back to the applied-reader path, not raw-WAL",
                        cleanAfterForward,
                        instance.getDedupRawWalCleanCycles()
                );
            }
            assertQuery("SELECT sym, val, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\trn\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\t99\t2026-01-01T00:00:02.000000Z\t2\n" +
                            "a\t30\t2026-01-01T00:00:03.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartRebuildPurgesBelowFrontierDedupReplacement() throws Exception {
        // A below-frontier dedup replacement that pushes a row out of the view's WHERE
        // filter, incorporated by the restart-restore rebuild (not the incremental drain).
        // The rebuild recomputes the whole view from the applied base, so it must delete
        // the stale pre-replacement LV row even though the recompute's lowest surviving
        // row sits above it. Regression for LiveViewFuzzTest#testFuzzDedup
        // (seed 661975787194049L/1784178587678L, RANGE_SUM variant).
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG) " +
                    "TIMESTAMP(ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT ts, sym, i, sum(i) OVER (PARTITION BY sym ORDER BY ts " +
                    "RANGE BETWEEN '9' MINUTE PRECEDING AND CURRENT ROW) AS v FROM base WHERE i > 0");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Emit ts=00:01 (i=297) below the frontier, plus higher-ts rows. The refresh
                // seals a head checkpoint at this applied point.
                execute("INSERT INTO base (ts, sym, i) VALUES " +
                        "('2026-01-01T00:01:00.000000Z', 'a', 297), " +
                        "('2026-01-01T00:05:00.000000Z', 'a', 500), " +
                        "('2026-01-01T00:09:00.000000Z', 'a', 900)");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // A zero-row cycle that advances the applied watermark PAST the head
                // checkpoint, so a later restart takes the checkpoint-lags-applied rebuild
                // branch: insert a future band and drop it (the LV never emits it).
                setCurrentMicros(currentMicros + 2_000_000L);
                execute("INSERT INTO base (ts, sym, i) VALUES ('2030-01-01T00:00:00.000000Z', 'z', 1)");
                drainWalQueue();
                execute("ALTER TABLE base DROP PARTITION LIST '2030-01-01'");
                drainWalQueue();
                driveRefreshToQuiescence(job);

                // Below-frontier dedup replacement of ts=00:01: i 297 -> -108, which now
                // fails WHERE i > 0. Apply it to the base but DON'T refresh the LV, so the
                // restart-restore rebuild is what must incorporate it.
                setCurrentMicros(currentMicros + 2_000_000L);
                execute("INSERT INTO base (ts, sym, i) VALUES ('2026-01-01T00:01:00.000000Z', 'a', -108)");
                drainWalQueue();
            }

            // Restart: drop the in-memory registry and rebuild from on-disk state, then
            // refresh. The restore rebuilds the whole view from the applied base (which now
            // holds i=-108 at 00:01) and must purge the stale i=297 row.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveRefreshToQuiescence(job);
            }

            assertQuery("SELECT ts, sym, i FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("ts\tsym\ti\n" +
                            "2026-01-01T00:05:00.000000Z\ta\t500\n" +
                            "2026-01-01T00:09:00.000000Z\ta\t900\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRestartWithDedupCollapseInCheckpointGap() throws Exception {
        // The head .cp is written on a cadence, so it can lag the applied point. When
        // the gap holds an intra-commit equal-ts collapse (Gap B), a raw-WAL
        // replay-to-applied would advance the restored accumulators over BOTH pre-dedup
        // rows, diverging from the post-dedup disk. The dedup restart path must instead
        // rebuild from the applied base. The discriminating assertion is the cumulative
        // sum of a row appended AFTER restart: 120 if reconciled correctly, 150 if the
        // accumulators drifted over the pre-dedup stream.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM NOW AS SELECT sym, val, ts, " +
                    "sum(val) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 100 PRECEDING AND CURRENT ROW) AS cum FROM base");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1 -> applied + first head .cp (at the applied point).
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 10.0, '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 20.0, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Batch 2 (the gap): a single commit with two equal-ts rows above the
                // frontier collapses to one (val 40). Applied advances; the .cp does
                // not (neither row nor duration cadence is met), so head < applied.
                setCurrentMicros(200_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES " +
                        "('a', 30.0, '2026-01-01T00:00:03.000000Z'), " +
                        "('a', 40.0, '2026-01-01T00:00:03.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(instance);
            Assert.assertTrue(
                    "test must create a checkpoint-cadence gap (head < applied)",
                    instance.getHeadCheckpointLvSeqTxn() < instance.getAppliedWatermark()
            );

            // Simulated restart: drop the in-memory registry and rebuild it from disk.
            engine.getLiveViewRegistry().clear();
            engine.buildViewGraphs();
            LiveViewInstance restored = engine.getLiveViewRegistry().getViewInstance("lv");
            Assert.assertNotNull(restored);

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Restore closes the .cp-to-applied gap over the applied base, then a
                // forward row at ts=04 appends. cum(04) = 10 + 20 + 40 + 50 = 120.
                setCurrentMicros(1_000_000L);
                drainJob(job); // restore-from-head (dedup gap) rebuild
                drainWalQueue();
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 50.0, '2026-01-01T00:00:04.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertQuery("SELECT sym, val, ts, cum FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\tcum\n" +
                            "a\t10.0\t2026-01-01T00:00:01.000000Z\t10.0\n" +
                            "a\t20.0\t2026-01-01T00:00:02.000000Z\t30.0\n" +
                            "a\t40.0\t2026-01-01T00:00:03.000000Z\t70.0\n" +
                            "a\t50.0\t2026-01-01T00:00:04.000000Z\t120.0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testSymbolOutputServedOverDedupBase() throws Exception {
        // A SYMBOL output column over a dedup base still refreshes correctly: the
        // coupled forward append eager-interns symbols into the tier's LV-space id
        // set, and a frontier replacement collapses in place.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, tag SYMBOL, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS " +
                    "SELECT sym, tag, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, tag, ts) VALUES " +
                        "('a', 'x', '2026-01-01T00:00:01.000000Z'), " +
                        "('a', 'y', '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Replace the frontier row's tag: 'y' -> 'z'.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, tag, ts) VALUES ('a', 'z', '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }
            assertQuery("SELECT sym, tag, ts, rn FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\ttag\tts\trn\n" +
                            "a\tx\t2026-01-01T00:00:01.000000Z\t1\n" +
                            "a\tz\t2026-01-01T00:00:02.000000Z\t2\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testTruncateInRangeFallsBackToAppliedReader() throws Exception {
        // A data-shaped non-DATA op (TRUNCATE) diverges the applied base from the raw WAL,
        // so its seqTxn advances the divergence watermark and any range covering it fails
        // the clean gate -> applied-reader fallback. Warm the signal with forward commits
        // (clean-cycle counter grows), then TRUNCATE and assert the counter does not move
        // across that cycle, and that the derived prefix stays frozen (no history rewrite).
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 10, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Forward row above the frontier: clean cycle, cheap raw-WAL append.
                setCurrentMicros(2_000_000L);
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 20, '2026-01-01T00:00:02.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance("lv");
                Assert.assertNotNull(instance);
                long cleanBeforeTruncate = instance.getDedupRawWalCleanCycles();
                Assert.assertTrue(
                        "forward commit must take the cheap raw-WAL path",
                        cleanBeforeTruncate > 0
                );

                // TRUNCATE removes applied history the raw append would keep -> divergence.
                setCurrentMicros(4_000_000L);
                execute("TRUNCATE TABLE base");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                Assert.assertEquals(
                        "TRUNCATE must fall back to the applied-reader path, not raw-WAL",
                        cleanBeforeTruncate,
                        instance.getDedupRawWalCleanCycles()
                );
            }
            assertQuery("SELECT sym, val, ts FROM lv ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("sym\tval\tts\n" +
                            "a\t10\t2026-01-01T00:00:01.000000Z\n" +
                            "a\t20\t2026-01-01T00:00:02.000000Z\n");
            execute("DROP LIVE VIEW lv");
        });
    }
}
