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

import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Correctness suite for live views over a DEDUP-enabled base table. Such a view
 * takes the coupled, applied-reader refresh path: instead of appending the base's
 * raw (pre-dedup) WAL stream, the refresh worker reads the applied, post-dedup base
 * via a {@code TableReader} and routes any timestamp-overlap batch through the O3
 * replay machinery. See {@code LIVE_VIEW_DEDUP_BASE_DESIGN} and
 * {@code LiveViewRefreshJob#drainAppliedBase}.
 * <p>
 * All tests run under the default (V1) sequencer
 * ({@code cairo.default.seq.part.txn.count = 0}), so any accidental reliance on the
 * V2-only {@code TransactionLogCursor.getTxnMinTimestamp()} would throw immediately;
 * the overlap trigger must source min ts from the base WAL-E event file instead.
 */
public class LiveViewDedupBaseTest extends AbstractCairoTest {

    // Pin the test clock below all test data before each test. A non-BACKFILL view's
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
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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
    public void testBaseTruncateFreezesDerivedPrefix() throws Exception {
        // A base TRUNCATE below the frontier is a data-shaped non-DATA commit
        // (walId>0, isDataType=false): the WAL-E walk excludes it from batchMinTs, so
        // no history-rewriting replay fires and the LV's derived prefix stays frozen.
        // A following in-order commit still appends.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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
    public void testDeepDedupRecomputesDownstreamWindow() throws Exception {
        // Replace the OLDEST row (below the head checkpoint): the head-miss full
        // rebuild must reflect the new value and recompute every downstream cumulative
        // sum in the same partition frame.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS SELECT sym, val, ts, " +
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
    public void testFrontierTimestampReplacementReflectedNoDuplicate() throws Exception {
        // A dedup UPSERT replaces the most-recent row at exactly the frontier ts. The
        // raw-WAL cross-commit trigger (txnMinTs < latestSeen, strict) would miss this
        // equality case and append a duplicate; the applied-reader path routes it to a
        // replay that rewrites the row in place. Assert the replaced value with no
        // duplicate row.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms AS SELECT sym, val, ts, " +
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
            execute("CREATE TABLE base (sym SYMBOL, tag SYMBOL, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m AS " +
                    "SELECT sym, tag, ts, row_number() OVER () AS rn FROM base");
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
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base");
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

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }
}
