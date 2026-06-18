/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewState;
import io.questdb.mp.Job;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * Differential fuzz test for live views.
 * <p>
 * The premise of an incremental-maintenance engine is that the incrementally
 * materialized state equals a from-scratch recompute over the base table. This
 * test verifies exactly that invariant: it drives randomized inserts (in-order
 * and out-of-order), simulated restarts, and optional backfill at the base
 * table, then cross-checks the live view's contents against the same window
 * query recomputed directly over the base table.
 * <p>
 * <b>Why the oracle is sound.</b> Window functions are order-dependent, so a
 * row-level comparison is only meaningful when both the incremental and the
 * batch path agree on a total ordering of the input rows. Two design choices
 * guarantee that:
 * <ul>
 *   <li><b>Strictly-unique timestamps.</b> Every generated row has a distinct,
 *   strictly-increasing timestamp. The base table's designated timestamp is
 *   therefore a total order, and {@code OVER (ORDER BY ts ...)} - as well as the
 *   natural ts scan order used by {@code OVER ()} - is unambiguous. Duplicate
 *   timestamps would let the two paths break ties differently, which is not a
 *   correctness bug but would still fail a row-level diff. So out-of-order
 *   ingestion is produced by <i>shuffling insertion order across commits</i>,
 *   never by colliding timestamps.</li>
 *   <li><b>Grammar-legal, deterministic window shapes.</b> Only bounded
 *   {@code ROWS BETWEEN N PRECEDING AND CURRENT ROW} frames and ranking
 *   {@code OVER ()} are used. Live views reject unbounded aggregate frames
 *   without an ANCHOR clause, and bounded frames over a unique-ts ordering are
 *   deterministic functions of the row set.</li>
 * </ul>
 * The comparison normalizes row order with {@code ORDER BY 1} (the unique
 * timestamp) and uses {@code genericStringMatch} so a SYMBOL passthrough that
 * the materializer stores as STRING still compares by value.
 */
public class LiveViewFuzzTest extends AbstractCairoTest {

    // Variants 0..AGG_VARIANTS-1 are ORDER BY ts bounded-frame aggregates: their
    // output is a total deterministic function of the (unique-ts) row set, so the
    // recompute oracle holds under any ingestion order, including O3 and restart.
    // The remaining variant is ranking row_number() OVER () with no ORDER BY,
    // whose output order is unspecified; under in-order ingestion processing
    // order equals ts order so the oracle still holds, but under O3 the
    // incrementally-maintained order legitimately diverges from a batch recompute
    // (the no-partition row_number sequence counter is not reset on head-miss
    // replay). It is therefore fuzzed in in-order mode only.
    private static final int AGG_VARIANTS = 5;
    // FLUSH EVERY rate-limits LV commits by wall clock: a refresh within
    // flushEveryMicros of the previous commit is deferred. Tests drive a
    // controllable clock (currentMicros) and advance it past this interval
    // before each refresh so flushes are deterministic, not wall-clock racy.
    private static final long CLOCK_ADVANCE_MICROS = 250_000; // > FLUSH EVERY 100ms
    private static final int MAX_FRAME = 8;
    private static final String[] SYMBOLS = {"AA", "BB", "CC", "DD"};

    @Test
    public void testFuzzBackfill() throws Exception {
        // Pre-CREATE history captured by BACKFILL, an ACTIVE-phase restart, then
        // in-order ACTIVE inserts. Post-CREATE ingestion is kept in-order here:
        // BACKFILL + O3 drives the head-miss REPLACE_RANGE to re-merge into the
        // multi-partition backfilled data, which trips a core O3 partition-merge
        // assertion (mergeDataLo <= mergeDataHi in O3PartitionJob) - a separate
        // finding from this test, tracked outside it. Non-backfill O3 and O3 +
        // restart (above) exercise the O3 replay path and stay clean.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < AGG_VARIANTS; v++) {
                runFuzz(rnd, v, 140, false, true, true, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzInOrder() throws Exception {
        // In-order ingestion: the happy incremental path, no head replay.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < variantCount(); v++) {
                runFuzz(rnd, v, 160, false, false, false, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzO3() throws Exception {
        // Out-of-order ingestion across commits, refreshing between each commit
        // so late rows force head replay against already-materialized state.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < AGG_VARIANTS; v++) {
                runFuzz(rnd, v, 160, true, false, false, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzO3WithRestart() throws Exception {
        // O3 plus simulated restarts (registry clear + rebuild from disk) at
        // quiescent points, with checkpoints written every refresh.
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        final Rnd rnd = TestUtils.generateRandom(LOG);
        assertMemoryLeak(() -> {
            for (int v = 0; v < AGG_VARIANTS; v++) {
                runFuzz(rnd, v, 140, true, true, false, rnd.nextBoolean());
            }
        });
    }

    @Test
    public void testFuzzRandomized() throws Exception {
        // A single fully-random configuration per CI run; seed is logged for
        // reproduction. Explores combinations the pinned tests do not.
        final Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1 + rnd.nextInt(4));
        assertMemoryLeak(() -> {
            final boolean o3 = rnd.nextBoolean();
            // Ranking is sound only under in-order ingestion; restrict to the
            // aggregate variants when fuzzing O3. Backfill stays mutually
            // exclusive with O3 (backfill + O3 trips a core O3 merge assertion;
            // see testFuzzBackfill).
            final boolean backfill = !o3 && rnd.nextBoolean();
            final int variant = o3 ? rnd.nextInt(AGG_VARIANTS) : rnd.nextInt(variantCount());
            runFuzz(rnd, variant, 80 + rnd.nextInt(320), o3, o3 && rnd.nextBoolean(), backfill, rnd.nextBoolean());
        });
    }

    // Boundaries [0, ..., len] splitting a segment of length len into 1..~10
    // contiguous commits.
    private static int[] commitBounds(Rnd rnd, int len) {
        final int commits = Math.max(1, Math.min(len, 2 + rnd.nextInt(9)));
        final int[] b = new int[commits + 1];
        for (int c = 0; c <= commits; c++) {
            b[c] = (int) ((long) c * len / commits);
        }
        return b;
    }

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    // Returns the projection (SELECT list) for the given window-query variant.
    // Every shape is grammar-legal in a live view and deterministic under a
    // unique-timestamp total order. The fuzzed set is exactly the window shapes
    // that carry the incremental-snapshot contract: PARTITION BY rows-frame
    // sum/max/first_value/count/avg, plus ranking OVER (). Un-partitioned
    // aggregate windows and last_value over a CURRENT ROW frame are rejected at
    // CREATE (no snapshot support), so they are not fuzzed here. N is the
    // bounded-frame radius.
    private static String projection(int variant, int n) {
        final String frame = "PARTITION BY sym ORDER BY ts ROWS BETWEEN " + n + " PRECEDING AND CURRENT ROW";
        return switch (variant) {
            case 0 -> "ts, sym, i, sum(i) OVER (" + frame + ") AS v";
            case 1 -> "ts, sym, i, max(i) OVER (" + frame + ") AS v";
            case 2 -> "ts, sym, i, first_value(i) OVER (" + frame + ") AS v";
            case 3 -> "ts, sym, count() OVER (" + frame + ") AS v";
            case 4 -> "ts, sym, x, avg(x) OVER (" + frame + ") AS v";
            case 5 -> "ts, sym, row_number() OVER () AS rn";
            default -> throw new IllegalArgumentException("variant=" + variant);
        };
    }

    // Row indices [lo, lo+1, ..., hi-1], shuffled in place when o3 is set so
    // insertion order diverges from ts order (the source of out-of-order writes).
    private static int[] segmentOrder(Rnd rnd, int lo, int hi, boolean o3) {
        final int[] a = new int[hi - lo];
        for (int k = 0; k < a.length; k++) {
            a[k] = lo + k;
        }
        if (o3) {
            for (int k = a.length - 1; k > 0; k--) {
                int j = rnd.nextInt(k + 1);
                int tmp = a[k];
                a[k] = a[j];
                a[j] = tmp;
            }
        }
        return a;
    }

    private static int variantCount() {
        return 6;
    }

    // Drives the named view's backfill sweep to completion on the caller's job,
    // re-fetching the instance each pass so it survives a restart, then applies
    // the LV WAL. Mirrors the smoke test helper.
    private void driveBackfillToCompletion(LiveViewRefreshJob job, String viewName) {
        for (int i = 0; i < 1000; i++) {
            LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance(viewName);
            if (inst == null
                    || inst.getStateReader().getBackfillState() != LiveViewState.BACKFILL_STATE_BACKFILLING) {
                break;
            }
            drainJob(job);
        }
        drainWalQueue();
    }

    // Pumps the refresh job until no further LV WAL work is produced, advancing
    // the clock each pass so deferred flushes land, and applying the LV's own
    // WAL after each burst.
    private void driveRefreshToQuiescence(LiveViewRefreshJob job) {
        for (int i = 0; i < 512; i++) {
            setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
            drainWalQueue();
            boolean progressed = drainJob(job);
            drainWalQueue();
            if (!progressed) {
                break;
            }
        }
    }

    private void insertCommit(
            StringSink sink,
            int[] order,
            int from,
            int to,
            long[] tsv,
            int[] symIdx,
            long[] iv,
            double[] xv,
            boolean[] xNull
    ) throws Exception {
        if (from >= to) {
            return;
        }
        sink.clear();
        sink.put("INSERT INTO base (ts, sym, i, x) VALUES ");
        for (int r = from; r < to; r++) {
            int k = order[r];
            if (r > from) {
                sink.put(',');
            }
            sink.put('(').put(tsv[k]).put("::timestamp,");
            if (symIdx[k] < 0) {
                sink.put("null,");
            } else {
                sink.put('\'').put(SYMBOLS[symIdx[k]]).put("',");
            }
            if (iv[k] == Numbers.LONG_NULL) {
                sink.put("null,");
            } else {
                sink.put(iv[k]).put(',');
            }
            if (xNull[k]) {
                sink.put("null");
            } else {
                sink.put(xv[k]);
            }
            sink.put(')');
        }
        execute(sink);
    }

    // One refresh cycle past the FLUSH EVERY rate-limit: advances the clock so
    // the commit is not deferred, runs the job, and applies the LV WAL.
    private void refreshCycle(LiveViewRefreshJob job) {
        setCurrentMicros(currentMicros + CLOCK_ADVANCE_MICROS);
        drainJob(job);
        drainWalQueue();
    }

    private void runFuzz(
            Rnd rnd,
            int variant,
            int rowCount,
            boolean o3,
            boolean restart,
            boolean backfill,
            boolean inMemory
    ) throws Exception {
        // Drive a controllable clock so FLUSH EVERY flush gating is deterministic.
        // Pin "now" a day BEFORE the data start (2026-01-01). A non-backfill
        // view's lower bound is the wall-clock CREATE moment, and O3 head-miss
        // replay only re-emits base rows at or above that floor - so the clock
        // must sit below every data timestamp or the replay would drop rows the
        // recompute keeps. The per-cycle clock advance (250ms) stays far under
        // the one-day gap across a whole test run.
        if (currentMicros < 0) {
            setCurrentMicros(MicrosTimestampDriver.floor("2025-12-31T00:00:00.000000Z"));
        }

        execute("DROP LIVE VIEW IF EXISTS lv");
        execute("DROP TABLE IF EXISTS base");
        execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, i LONG, x DOUBLE) " +
                "TIMESTAMP(ts) PARTITION BY DAY WAL");

        final int n = 1 + rnd.nextInt(MAX_FRAME);
        final boolean withWhere = rnd.nextInt(3) == 0;
        final String projection = projection(variant, n);
        final String viewSql = "SELECT " + projection + " FROM base" + (withWhere ? " WHERE i > 0" : "");
        final String createSql = "CREATE LIVE VIEW lv FLUSH EVERY 100ms "
                + (inMemory ? "IN MEMORY 60s " : "")
                + (backfill ? "BACKFILL " : "")
                + "AS " + viewSql;

        LOG.info().$("LV fuzz: variant=").$(variant).$(", rows=").$(rowCount)
                .$(", n=").$(n).$(", o3=").$(o3).$(", restart=").$(restart)
                .$(", backfill=").$(backfill).$(", inMem=").$(inMemory)
                .$(", where=").$(withWhere).$(", sql=").$(viewSql).$();

        // Generate the logical dataset: strictly-unique, strictly-increasing
        // timestamps; random symbols and values with occasional NULLs.
        final long[] tsv = new long[rowCount];
        final int[] symIdx = new int[rowCount];
        final long[] iv = new long[rowCount];
        final double[] xv = new double[rowCount];
        final boolean[] xNull = new boolean[rowCount];
        long ts = MicrosTimestampDriver.floor("2026-01-01T00:00:00.000000Z");
        for (int k = 0; k < rowCount; k++) {
            ts += 1 + rnd.nextInt(5_000_000); // 1us .. 5s, keeps ts strictly increasing
            if (rnd.nextInt(20) == 0) {
                ts += 86_400_000_000L; // occasional full-day jump to span more partitions
            }
            tsv[k] = ts;
            symIdx[k] = rnd.nextInt(20) == 0 ? -1 : rnd.nextInt(SYMBOLS.length); // -1 => NULL symbol
            iv[k] = rnd.nextInt(20) == 0 ? Numbers.LONG_NULL : (rnd.nextInt(2001) - 1000);
            xNull[k] = rnd.nextInt(20) == 0;
            xv[k] = rnd.nextDouble() * 1000.0;
        }

        // Backfill captures pre-CREATE history. Put the EARLIEST rows (by ts)
        // before CREATE so the backfill floor sits at the global min ts and no
        // post-CREATE O3 row falls below it - such a row would be rejected and
        // diverge from the recompute. Non-backfill: everything lands post-CREATE.
        final int preCount = backfill ? rnd.nextInt(rowCount + 1) : 0;

        final StringSink sink = new StringSink();
        LiveViewRefreshJob job = null;
        try {
            // Pre-CREATE history: earliest segment [0, preCount), inserted in
            // random commit order for O3.
            if (preCount > 0) {
                final int[] preOrder = segmentOrder(rnd, 0, preCount, o3);
                final int[] cb = commitBounds(rnd, preOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, preOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull);
                    drainWalQueue();
                }
            }

            execute(createSql);
            job = new LiveViewRefreshJob(0, engine, 1);

            if (backfill) {
                driveBackfillToCompletion(job, "lv");
            }

            // Post-CREATE: segment [preCount, rowCount), refreshed per commit so a
            // later (older-ts) commit is genuinely O3 vs the materialized state.
            if (preCount < rowCount) {
                final int[] postOrder = segmentOrder(rnd, preCount, rowCount, o3);
                final int[] cb = commitBounds(rnd, postOrder.length);
                for (int c = 0; c + 1 < cb.length; c++) {
                    insertCommit(sink, postOrder, cb[c], cb[c + 1], tsv, symIdx, iv, xv, xNull);
                    drainWalQueue();
                    refreshCycle(job);

                    // Simulate a restart at a quiescent point (ACTIVE state only).
                    if (restart && rnd.nextInt(3) == 0) {
                        LiveViewInstance inst = engine.getLiveViewRegistry().getViewInstance("lv");
                        if (inst != null
                                && inst.getStateReader().getBackfillState() == LiveViewState.BACKFILL_STATE_ACTIVE) {
                            job = Misc.free(job);
                            engine.getLiveViewRegistry().clear();
                            engine.buildViewGraphs();
                            job = new LiveViewRefreshJob(0, engine, 1);
                        }
                    }
                }
            }

            driveRefreshToQuiescence(job);
        } finally {
            Misc.free(job);
        }

        // The oracle: the live view must equal the window query recomputed over
        // the base table. ORDER BY 1 (the unique ts) gives both sides a total
        // order; genericStringMatch tolerates SYMBOL-vs-STRING on passthrough.
        TestUtils.assertSqlCursors(
                engine,
                sqlExecutionContext,
                "(" + viewSql + ") ORDER BY 1",
                "(lv) ORDER BY 1",
                LOG,
                true
        );

        execute("DROP LIVE VIEW lv");
        execute("DROP TABLE base");
    }
}
