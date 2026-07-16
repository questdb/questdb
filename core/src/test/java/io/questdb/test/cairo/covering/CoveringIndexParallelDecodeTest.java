/*+*****************************************************************************
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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.DataSource;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.ExtraNullColumnCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.IdentityHashMap;

import static org.junit.Assert.*;

public class CoveringIndexParallelDecodeTest extends AbstractCairoTest {

    @Test
    public void testDefaultColumnSourceIsDirect() {
        assertEquals((byte) 0, DataSource.DIRECT);
        assertEquals((byte) 1, DataSource.COVERED);
    }

    /**
     * PURPOSE-BUILT PERFORMANCE MEASUREMENT (Task 14) — NOT an assertion test.
     * <p>
     * Validates the headline win of this branch: covered-index column decode for
     * aggregation/filter now runs on the async WORKER threads instead of being
     * serialized on the dispatch thread. For {@code covered_agg}
     * ({@code sum(px) WHERE sym = '<hot>'}) and {@code residual}
     * ({@code sum(px) WHERE sym = '<hot>' AND px > <v>}) over a covering-indexed
     * table it times three configs and prints the ratios:
     * <ul>
     *   <li><b>A) parallel</b> — the new path, 8 workers.</li>
     *   <li><b>B) serial</b> — the same covered query at 1 worker (the
     *       parallelism this branch unlocks).</li>
     *   <li><b>C) scan parity</b> — the SAME aggregation over an identical
     *       NON-indexed twin {@code ref} at 8 workers (a full parallel scan; the
     *       target the covered path should reach).</li>
     * </ul>
     * Reports wall-clock for A/B/C and the ratios <b>B/A</b> (parallelism speedup,
     * want &gt;&gt; 1) and <b>A/C</b> (covered-vs-scan; want ~1, i.e. parity).
     * <p>
     * Each config runs under its OWN freshly-built {@link CairoEngine} +
     * {@link WorkerPool} over the SAME on-disk db root (the BYPASS WAL tables
     * persist and a fresh engine reloads them), so the worker-count change
     * genuinely takes effect and readers/engine are freed between configs.
     * <p>
     * It is a perf harness, so it is GATED behind {@code -Dcovering.perf=true} to
     * keep it out of the ordinary {@code mvn test} run. To run:
     * <pre>
     * mvn -f core/pom.xml test -Dtest=CoveringIndexParallelDecodeTest#testCoveredDecodeParallelPerf \
     *     -Dcovering.perf=true -DfailIfNoTests=false -pl core
     * </pre>
     * Tunables (all optional system properties):
     * {@code covering.perf.partitions} (default 16, &gt;= worker count),
     * {@code covering.perf.rowsPerPartition} (default 750000 -> ~12M rows),
     * {@code covering.perf.workers} (default 8),
     * {@code covering.perf.warmup} (default 2),
     * {@code covering.perf.iters} (default 5).
     */
    @Test
    public void testCoveredDecodeParallelPerf() throws Exception {
        if (!"true".equals(System.getProperty("covering.perf"))) {
            LOG.info().$("skipping testCoveredDecodeParallelPerf (enable with -Dcovering.perf=true)").$();
            return;
        }

        final int partitions = Integer.getInteger("covering.perf.partitions", 16);
        final int rowsPerPartition = Integer.getInteger("covering.perf.rowsPerPartition", 750_000);
        final int parallelWorkers = Integer.getInteger("covering.perf.workers", 8);
        final int warmup = Integer.getInteger("covering.perf.warmup", 2);
        final int iters = Integer.getInteger("covering.perf.iters", 5);
        final long rows = (long) rowsPerPartition * partitions;
        final String hotKey = "HOT";
        final int coldKeys = 11; // HOT + 11 cold symbols
        final double residualThreshold = 500.0; // px is x % 997 -> ~half the rows pass

        // A small-ish frame cap (vs the 1M default) so a single hot key spreads
        // into MANY page frames across the partitions, giving the workers real,
        // evenly distributable work. With 16 partitions x ~750k rows and ~40% HOT,
        // a 100k cap yields ~48 covered HOT frames -- comfortably more than the 8
        // workers -- while frames stay large enough that per-frame overhead does
        // not dominate (so the A/C scan-parity comparison is fair).
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100_000);

        // Hot-key skew: ~40% of rows are HOT, the rest spread over coldKeys cold
        // symbols. px = x % 997 (deterministic, ~half pass the residual). The same
        // generator feeds cov (covering) and ref (non-indexed twin) so A vs C is a
        // like-for-like aggregation over an identical row population.
        final String gen =
                "SELECT" +
                        " ('2024-01-01'::TIMESTAMP + (x - 1) * " + (86_400_000_000L / rowsPerPartition) + "L)::timestamp ts," +
                        " (CASE WHEN (x % 10) < 4 THEN '" + hotKey + "' ELSE 'C' || (x % " + coldKeys + ") END)::symbol sym," +
                        " (x % 997)::double px" +
                        " FROM long_sequence(" + rows + ")";

        final String coveredAgg = "SELECT sum(px) FROM %s WHERE sym = '" + hotKey + "'";
        final String residual = "SELECT sum(px) FROM %s WHERE sym = '" + hotKey + "' AND px > " + residualThreshold;

        // --- 1) Build the data ONCE under a parallel engine, then close it so the
        // BYPASS WAL tables are flushed to the shared db root for the timed configs
        // to reopen. (NOT under assertMemoryLeak -- this is a measurement.) ---
        buildPerfData(gen);

        // --- 2) Routing confirmation: EXPLAIN both covered queries at the parallel
        // worker count and assert they run through the ASYNC group-by over the
        // covering index (so we are timing the parallel covered decode, not a
        // serial fallback). If not, SAY SO LOUDLY -- that is itself a finding. ---
        final boolean[] routedAsync = {false, false};
        try (PerfNode node = new PerfNode(configuration, parallelWorkers)) {
            routedAsync[0] = assertOrReportAsyncCovered(node, "covered_agg", String.format(coveredAgg, "cov"));
            routedAsync[1] = assertOrReportAsyncCovered(node, "residual", String.format(residual, "cov"));
        }

        // --- 3) Time A / B / C for both query shapes. ---
        final long[] aAgg = timeConfig("covered_agg", "A) parallel cov", String.format(coveredAgg, "cov"), parallelWorkers, warmup, iters);
        final long[] bAgg = timeConfig("covered_agg", "B) serial   cov", String.format(coveredAgg, "cov"), 1, warmup, iters);
        final long[] cAgg = timeConfig("covered_agg", "C) parallel ref", String.format(coveredAgg, "ref"), parallelWorkers, warmup, iters);

        final long[] aRes = timeConfig("residual", "A) parallel cov", String.format(residual, "cov"), parallelWorkers, warmup, iters);
        final long[] bRes = timeConfig("residual", "B) serial   cov", String.format(residual, "cov"), 1, warmup, iters);
        final long[] cRes = timeConfig("residual", "C) parallel ref", String.format(residual, "ref"), parallelWorkers, warmup, iters);

        // --- 4) Report. ---
        final StringBuilder rpt = new StringBuilder();
        rpt.append('\n');
        rpt.append("==================================================================================\n");
        rpt.append("  COVERED PARALLEL-DECODE PERFORMANCE  (branch nw_covering_parallel_decode)\n");
        rpt.append("==================================================================================\n");
        rpt.append(String.format("  rows/table=%,d  partitions=%d  rowsPerPartition=%,d  frameMaxRows=100,000%n", rows, partitions, rowsPerPartition));
        rpt.append(String.format("  hotKey='%s' (~40%%)  coldKeys=%d  residual: px > %.0f%n", hotKey, coldKeys, residualThreshold));
        rpt.append(String.format("  workers: parallel=%d serial=1  warmup=%d measured=%d%n", parallelWorkers, warmup, iters));
        rpt.append(String.format("  routing: covered_agg async-covered=%s  residual async-covered=%s%n", routedAsync[0], routedAsync[1]));
        rpt.append("----------------------------------------------------------------------------------\n");
        appendPerf(rpt, "covered_agg", aAgg, bAgg, cAgg, warmup, iters);
        appendPerf(rpt, "residual   ", aRes, bRes, cRes, warmup, iters);
        rpt.append("==================================================================================\n");
        rpt.append(verdict("covered_agg", aAgg, bAgg, cAgg, warmup));
        rpt.append(verdict("residual   ", aRes, bRes, cRes, warmup));
        rpt.append("==================================================================================\n");

        // Emit to the log so the report is visible in the test harness output.
        LOG.advisory().$safe(rpt).$();

        // The gated perf run is the only automated check of the headline claim. Assert the verdict
        // so a regression that loses parallelism or scan-parity FAILS the explicit -Dcovering.perf
        // run instead of only printing. covered_agg must both parallelize and reach scan parity;
        // residual must at least parallelize (its A/C is ~parity and noisier).
        assertParallelVerdict("covered_agg", aAgg, bAgg, cAgg, warmup, true);
        assertParallelVerdict("residual", aRes, bRes, cRes, warmup, false);
    }

    @Test
    public void testCountOverCoveringIndex() throws Exception {
        // count(*) WHERE sym = '<lit>' is served from posting-index metadata
        // (SingleKeyCoveringCursor.size() -> countMatchesClamped): the plan is Count over
        // CoveringIndex, and the answer is exact -- no covered decode, no posting walk -- across
        // keys, NULLs, an absent key, and a partial-frame (sub-day) interval. Deterministic data:
        // x=1..100 -> 'A', 101..250 -> 'B', 251..300 -> NULL; one row per hour from 2024-01-01.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE)" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO cov SELECT" +
                    " ('2024-01-01T00:00:00.000000Z'::timestamp + (x - 1) * 3600000000L)::timestamp," +
                    " CASE WHEN x <= 100 THEN 'A' WHEN x <= 250 THEN 'B' ELSE NULL END," +
                    " x::double FROM long_sequence(300)");
            engine.releaseAllWriters();

            assertQuery("SELECT count() FROM cov WHERE sym = 'A'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Count", "CoveringIndex on: sym")
                    .returns("count\n100\n");
            assertQuery("SELECT count() FROM cov WHERE sym = 'B'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Count", "CoveringIndex on: sym")
                    .returns("count\n150\n");
            assertQuery("SELECT count() FROM cov WHERE sym = 'NOPE'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Count", "CoveringIndex on: sym")
                    .returns("count\n0\n");
            // Partial-frame interval: 2024-01-03 06:00..12:00 is hours 6..11 of day 3 = x 55..60,
            // all 'A' -> 6 rows, from the clamped metadata count over a sub-partition range.
            assertQuery("SELECT count() FROM cov WHERE sym = 'A'" +
                    " AND ts >= '2024-01-03T06:00:00.000000Z' AND ts < '2024-01-03T12:00:00.000000Z'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Count", "CoveringIndex on: sym")
                    .returns("count\n6\n");
        });
    }

    @Test
    public void testCountOverCoveringIndexResealedAndMultiGen() throws Exception {
        // Review finding #1: count() WHERE sym=k is answered from posting metadata
        // (SingleKeyCoveringCursor.size() -> countMatchesClamped, which uses the EXACT per-gen
        // first/last, unlike the cursor's slack-bound size() that can false-bail). The existing
        // count test only covers a single freshly-loaded partition; here we prove the end-to-end
        // count() is EXACT across MULTI-GEN (several sealed commits), an O3 RESEAL (out-of-order
        // rows shrink/extend the sealed gen's covered range), and a NULL prefix -- by comparing to
        // a non-indexed twin whose count() WHERE sym=k is a plain full-scan filter with no metadata
        // short-circuit. If the metadata count diverged from the true row count in any gen state,
        // cov != ref here.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE)" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, px DOUBLE)" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");

            // Commit 1: ordered rows on day 1 -> sealed gen.
            final String gen1 = "SELECT ('2024-01-01'::timestamp + (x-1)*3600000000L)::timestamp," +
                    " case when x%5=0 then null else ('K'||(x%4)) end, x::double FROM long_sequence(200)";
            execute("INSERT INTO cov " + gen1);
            execute("INSERT INTO ref " + gen1);
            engine.releaseAllWriters(); // seal gen 0

            // Commit 2: OUT-OF-ORDER rows interleaved into the SAME day-1 partition (offset by 30min,
            // earlier than already-sealed rows) -> O3 reseal, so the sealed gen's covered range no
            // longer ends at the partition's true max row.
            final String gen2 = "SELECT ('2024-01-01T00:30:00'::timestamp + (x-1)*3600000000L)::timestamp," +
                    " case when x%3=0 then null else ('K'||(x%4)) end, (1000+x)::double FROM long_sequence(150)";
            execute("INSERT INTO cov " + gen2);
            execute("INSERT INTO ref " + gen2);
            engine.releaseAllWriters();

            // Commit 3: a later partition (day 10) appended as a third gen.
            final String gen3 = "SELECT ('2024-01-10'::timestamp + (x-1)*3600000000L)::timestamp," +
                    " case when x%2=0 then null else ('K'||(x%4)) end, (2000+x)::double FROM long_sequence(120)";
            execute("INSERT INTO cov " + gen3);
            execute("INSERT INTO ref " + gen3);
            engine.releaseAllWriters();

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // Per-key, NULL, absent key, and a partial-frame interval crossing the O3 reseal.
                for (String pred : new String[]{
                        "sym = 'K0'", "sym = 'K1'", "sym = 'K2'", "sym = 'K3'", "sym IS NULL", "sym = 'NOPE'",
                        "sym = 'K1' AND ts >= '2024-01-01T00:00:00' AND ts < '2024-01-01T06:00:00'",
                        "sym IS NULL AND ts >= '2024-01-10T00:00:00' AND ts < '2024-01-10T03:00:00'"
                }) {
                    final String q = "SELECT count() FROM %s WHERE " + pred;
                    TestUtils.assertSqlCursors(compiler, sqlExecutionContext,
                            String.format(q, "ref"), String.format(q, "cov"), LOG);
                }
            }
            // The metadata fast-path routing (Count over CoveringIndex) for cov count() is pinned by
            // testCountOverCoveringIndex; here the ref-vs-cov equality proves the count VALUE is
            // exact across the multi-gen / O3-reseal / null-prefix states.
        });
    }

    @Test
    public void testCoveringPlanShowsDecodeStrategyViaFilterShape() throws Exception {
        // Review finding #7: rather than emit a separate decode-strategy attr (which would churn
        // every covering-plan golden test), the strategy is intentionally derivable from the filter
        // shape. This test PINS that contract so a future change to either the plan or the routing
        // is caught: a single equality (sym='x') is produced metadata-only and decoded in parallel
        // on the reduce workers; an IN-list (sym IN [...]) is decoded eagerly via the multi-key
        // merge. See CoveringIndexRecordCursorFactory.toPlan.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE)" +
                    " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // 60 rows, sym = 'K' || (x%3) -> K0/K1/K2 each appear exactly 20 times.
            execute("INSERT INTO cov SELECT ('2024-01-01'::timestamp + (x-1)*3600000000L)::timestamp," +
                    " ('K'||(x%3)), x::double FROM long_sequence(60)");
            engine.releaseAllWriters();

            // Single equality -> single-key (metadata-only production + parallel worker decode):
            // the plan renders "filter: sym='K1'".
            assertQuery("SELECT count() FROM cov WHERE sym = 'K1'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("CoveringIndex on: sym", "filter: sym='K1'")
                    .returns("count\n20\n");
            // IN-list -> multi-key (eager multi-key merge at production): the plan renders
            // "filter: sym IN ['K1','K2']".
            assertQuery("SELECT count() FROM cov WHERE sym IN ('K1','K2')")
                    .noRandomAccess()
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("CoveringIndex on: sym", "filter: sym IN ['K1','K2']")
                    .returns("count\n40\n");
        });
    }

    @Test
    public void testParallelCoveredDecodeMatchesReference() throws Exception {
        // THE HEADLINE TEST (Task 9): covered column decode now happens on the
        // async workers inside PageFrameMemoryPool.navigateTo, and must be both
        // correct and free of data races at worker count > 1.
        //
        // Build a covering table `cov` and an identical NON-indexed twin `ref`.
        // With 4 workers and a small page-frame row cap, each covered partition
        // splits into several frames, so multiple workers iterate detached
        // cursors over the SAME per-partition posting reader concurrently -- the
        // exact condition Task 1/2/3 (warm + detached + freeze) make safe.
        //
        // `sum(px)` routes through the vectorized async group-by (workers call
        // navigateTo(int)); the `... AND px > 0.5` residual routes through the
        // non-vectorized async group-by (workers call navigateTo(frameIndex,
        // columnIndexes) on the reduce task). Both covered arms are exercised.
        final int rowsPerPartition = 4000;
        final int partitions = 64; // 256k rows across 64 daily partitions
        final long rows = (long) rowsPerPartition * partitions;
        // 8 symbols => 500 rows per (symbol, partition). With a 200-row frame cap
        // each single-key partition splits into ~3 covered frames, so several
        // workers iterate detached cursors over the SAME per-partition posting
        // reader at once -- the genuine concurrent-same-reader case this task
        // makes safe. (Parallel group-by is enabled by default.)
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px)," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // 8 distinct symbols, spread so each partition holds several
                        // and frames distribute across workers and keys.
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * " + (86_400_000_000L / rowsPerPartition) + "L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 8))::symbol sym," +
                                        " (x % 997)::double px" +
                                        " FROM long_sequence(" + rows + ")";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        // Per-key aggregate + residual, covered (parallel, 4 workers)
                        // vs plain reference. `ref` has no covered decode and no
                        // concurrent-reader hazard, so equality here is the headline
                        // parallel-correctness proof.
                        for (String sym : new String[]{"S0", "S3", "S7"}) {
                            final String agg = "SELECT sum(px), count() FROM %s WHERE sym = '" + sym + "'";
                            final String residual = "SELECT sum(px), count() FROM %s WHERE sym = '" + sym + "' AND px > 0.5";
                            TestUtils.assertSqlCursors(
                                    compiler, sqlExecutionContext,
                                    String.format(agg, "ref"), String.format(agg, "cov"), LOG);
                            TestUtils.assertSqlCursors(
                                    compiler, sqlExecutionContext,
                                    String.format(residual, "ref"), String.format(residual, "cov"), LOG);
                        }

                        // cov parallel (4 workers) == cov single-threaded (1 worker),
                        // asserted directly on the cov result text under each context.
                        try (SqlExecutionContext singleCtx = TestUtils.createSqlExecutionCtx(engine, 1)) {
                            for (String sym : new String[]{"S0", "S3", "S7"}) {
                                final String agg = "SELECT sum(px), count() FROM cov WHERE sym = '" + sym + "'";
                                final String residual = "SELECT sum(px), count() FROM cov WHERE sym = '" + sym + "' AND px > 0.5";
                                assertCovParallelEqualsSingle(compiler, sqlExecutionContext, singleCtx, agg);
                                assertCovParallelEqualsSingle(compiler, sqlExecutionContext, singleCtx, residual);
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelCoveredDecodeAllTypesMatchReference() throws Exception {
        // CRITICAL coverage: the worker covered-decode must be byte-equivalent to a direct read
        // for ALL covered column types, not just DOUBLE/LONG. Build a covering table whose INCLUDE
        // covers a wide type matrix (fixed-width + UUID/LONG256/IPv4 + var-size STRING/VARCHAR/
        // BINARY/ARRAY), insert with per-type NULLs and longish var values, then under 4 workers
        // and a small frame cap (so each partition splits into several worker-decoded frames)
        // compare a projecting residual filter (record fast-path) and first/last aggregates
        // (stable-pointer path) against a non-indexed twin. `ref` is INSERT...SELECT * FROM cov so
        // the rnd-generated values match exactly.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                final String cols =
                        "  a_long LONG, a_int INT, a_short SHORT, a_byte BYTE, a_bool BOOLEAN," +
                                "  a_float FLOAT, a_date DATE, a_uuid UUID, a_l256 LONG256, a_ip IPV4," +
                                "  a_str STRING, a_vc VARCHAR, a_bin BINARY, a_arr DOUBLE[]";
                engine.execute(
                        "CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE " +
                                "(a_long, a_int, a_short, a_byte, a_bool, a_float, a_date, a_uuid, a_l256, a_ip, a_str, a_vc, a_bin, a_arr)," +
                                cols + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL," + cols +
                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                final int rows = 20_000; // ~5 daily partitions; >>200-row frame cap => many frames/partition
                engine.execute("INSERT INTO cov SELECT" +
                        " ('2024-01-01'::TIMESTAMP + (x-1)*60_000_000L)::timestamp," +
                        " ('S' || ((x-1) % 4))::symbol," +
                        " case when x%7=0 then null else (x%1000)::long end," +
                        " case when x%7=0 then null else (x%500)::int end," +
                        " case when x%7=0 then null else (x%200)::short end," +
                        " case when x%7=0 then null else (x%100)::byte end," +
                        " (x%2=0)," +
                        " case when x%7=0 then null else (x%50)::float end," +
                        " case when x%7=0 then null else (x*100000)::long::date end," +
                        " case when x%7=0 then null else rnd_uuid4() end," +
                        " case when x%7=0 then null else rnd_long256() end," +
                        " case when x%7=0 then null else rnd_ipv4() end," +
                        " case when x%7=0 then null else ('str-value-longish-' || (x%300)) end," +
                        " case when x%7=0 then null else ('vc-value-longish-' || (x%400))::varchar end," +
                        " case when x%7=0 then null else rnd_bin(4, 24, 0) end," +
                        " ARRAY[(x%5)::double, ((x+1)%5)::double, ((x+2)%5)::double]" +
                        " FROM long_sequence(" + rows + ")", ctx);
                engine.execute("INSERT INTO ref SELECT * FROM cov", ctx);
                engine.releaseAllWriters();

                for (String sym : new String[]{"S0", "S2"}) {
                    // Record fast-path: project EVERY covered type through a residual filter.
                    final String proj = "SELECT a_long,a_int,a_short,a_byte,a_bool,a_float,a_date,a_uuid,a_l256,a_ip,a_str,a_vc,a_bin,a_arr " +
                            "FROM %s WHERE sym = '" + sym + "' AND a_long > 100";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(proj, "ref"), String.format(proj, "cov"), LOG);
                    // Stable-pointer (first/last) path across scalar + var covered types.
                    final String fl = "SELECT first(a_long), last(a_int), first(a_short), last(a_byte)," +
                            " first(a_float), last(a_date), first(a_str), last(a_vc), first(a_uuid), last(a_l256), count() " +
                            "FROM %s WHERE sym = '" + sym + "'";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(fl, "ref"), String.format(fl, "cov"), LOG);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelCoveredDecodeIsNullMatchesReference() throws Exception {
        // The implicit-null (sym IS NULL, key 0) covered path under parallel decode. The single-key
        // cheap path computes the null prefix with the UNCLAMPED caller max (columnTop only),
        // distinct from the entryMaxValue-clamped data bound; a covered `sym IS NULL` aggregation
        // and residual must match a non-indexed twin under 4 workers. A non-null key on the same
        // table locks the other branch.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute("CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, px DOUBLE) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                final int rows = 40_000;
                // ~1/3 NULL sym (the implicit-null key), the rest spread over 4 symbols.
                engine.execute("INSERT INTO cov SELECT" +
                        " ('2024-01-01'::TIMESTAMP + (x-1)*60_000_000L)::timestamp," +
                        " case when x % 3 = 0 then null else ('S' || (x % 4))::symbol end," +
                        " (x % 997)::double" +
                        " FROM long_sequence(" + rows + ")", ctx);
                engine.execute("INSERT INTO ref SELECT * FROM cov", ctx);
                engine.releaseAllWriters();

                // IS NULL: aggregation (vectorized async group-by) + residual + projection (record path).
                final String agg = "SELECT sum(px), count() FROM %s WHERE sym IS NULL";
                final String residual = "SELECT sum(px), count() FROM %s WHERE sym IS NULL AND px > 0.5";
                final String proj = "SELECT ts, px FROM %s WHERE sym IS NULL AND px > 0.5";
                TestUtils.assertSqlCursors(compiler, ctx, String.format(agg, "ref"), String.format(agg, "cov"), LOG);
                TestUtils.assertSqlCursors(compiler, ctx, String.format(residual, "ref"), String.format(residual, "cov"), LOG);
                TestUtils.assertSqlCursors(compiler, ctx, String.format(proj, "ref"), String.format(proj, "cov"), LOG);
                // Non-null key on the same table: locks the data (non-null-prefix) branch.
                final String aggS1 = "SELECT sum(px), count() FROM %s WHERE sym = 'S1'";
                TestUtils.assertSqlCursors(compiler, ctx, String.format(aggS1, "ref"), String.format(aggS1, "cov"), LOG);
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelCoveredDecodeGeoDecimalCharTimestampMatchReference() throws Exception {
        // Review finding #5: the all-types parallel test omits the most layout-sensitive fixed
        // widths -- GEOHASH (all 4 backing widths byte/short/int/long), DECIMAL (all 6 widths
        // 8/16/32/64/128/256), CHAR, and a covered TIMESTAMP -- which were only exercised
        // single-threaded via the record path. Decode them under 4 workers + a small frame cap so
        // each partition splits into several worker-decoded frames, and compare a projecting
        // residual filter (record fast-path) AND first/last (stable-pointer path, where the type
        // supports it) against a non-indexed twin. rnd seeds match because ref is INSERT...SELECT *.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                final String cols =
                        "  g_b GEOHASH(1c), g_s GEOHASH(3c), g_i GEOHASH(6c), g_l GEOHASH(12c)," +
                                "  d8 DECIMAL(2,1), d16 DECIMAL(4,2), d32 DECIMAL(9,2)," +
                                "  d64 DECIMAL(18,4), d128 DECIMAL(38,10), d256 DECIMAL(40,5)," +
                                "  c CHAR, a_ts TIMESTAMP";
                engine.execute(
                        "CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE " +
                                "(g_b, g_s, g_i, g_l, d8, d16, d32, d64, d128, d256, c, a_ts)," +
                                cols + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL," + cols +
                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                final int rows = 20_000; // ~5 daily partitions; >>200-row frame cap => many frames/partition
                engine.execute("INSERT INTO cov SELECT" +
                        " ('2024-01-01'::TIMESTAMP + (x-1)*60_000_000L)::timestamp," +
                        " ('S' || ((x-1) % 4))::symbol," +
                        " case when x%7=0 then null else rnd_geohash(5) end," +
                        " case when x%7=0 then null else rnd_geohash(15) end," +
                        " case when x%7=0 then null else rnd_geohash(30) end," +
                        " case when x%7=0 then null else rnd_geohash(60) end," +
                        " case when x%7=0 then null else rnd_decimal(2,1,0) end," +
                        " case when x%7=0 then null else rnd_decimal(4,2,0) end," +
                        " case when x%7=0 then null else rnd_decimal(9,2,0) end," +
                        " case when x%7=0 then null else rnd_decimal(18,4,0) end," +
                        " case when x%7=0 then null else rnd_decimal(38,10,0) end," +
                        " case when x%7=0 then null else rnd_decimal(40,5,0) end," +
                        " case when x%7=0 then null else rnd_char() end," +
                        " case when x%7=0 then null else ('2020-01-01'::timestamp + x*1000000L)::timestamp end" +
                        " FROM long_sequence(" + rows + ")", ctx);
                engine.execute("INSERT INTO ref SELECT * FROM cov", ctx);
                engine.releaseAllWriters();

                for (String sym : new String[]{"S0", "S2"}) {
                    // Record fast-path: project every covered type through a residual filter.
                    final String proj = "SELECT g_b,g_s,g_i,g_l,d8,d16,d32,d64,d128,d256,c,a_ts " +
                            "FROM %s WHERE sym = '" + sym + "' AND d64 > 0";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(proj, "ref"), String.format(proj, "cov"), LOG);
                    // Stable-pointer (first/last) over the GEO widths (first/last support GEOHASH).
                    final String fl = "SELECT first(g_b), last(g_s), first(g_i), last(g_l), count() " +
                            "FROM %s WHERE sym = '" + sym + "'";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(fl, "ref"), String.format(fl, "cov"), LOG);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelCoveredFirstLastArrayAcrossManyFrames() throws Exception {
        // Review finding #3: first()/last() over a covered ARRAY is the riskiest lifetime case --
        // the aggregate retains a RAW pointer into a covered decode buffer for the merge phase, so
        // if a covered buffer were freed/rebound while a stable pointer still referenced it the
        // result would be wrong. Drive first(arr)/last(arr) over a covered query spanning MANY
        // frames (small frame cap + many partitions, well past the frame queue capacity) under 4
        // workers, vs a non-indexed twin. (BINARY has no first/last aggregate, so its covered
        // stable-pointer surface is the record/projection path already covered by the all-types
        // test; here we additionally project a_bin across many frames as a belt-and-suspenders.)
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 128);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute(
                        "CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (a_arr, a_bin)," +
                                " a_arr DOUBLE[], a_bin BINARY) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, a_arr DOUBLE[], a_bin BINARY)" +
                        " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                final int rows = 30_000; // many daily partitions x many 128-row frames per key
                engine.execute("INSERT INTO cov SELECT" +
                        " ('2024-01-01'::TIMESTAMP + (x-1)*120_000_000L)::timestamp," +
                        " ('S' || ((x-1) % 4))::symbol," +
                        " case when x%9=0 then null else ARRAY[(x%5)::double, ((x+1)%7)::double, ((x*3)%11)::double] end," +
                        " case when x%9=0 then null else rnd_bin(4, 40, 0) end" +
                        " FROM long_sequence(" + rows + ")", ctx);
                engine.execute("INSERT INTO ref SELECT * FROM cov", ctx);
                engine.releaseAllWriters();

                for (String sym : new String[]{"S0", "S1", "S3"}) {
                    // Stable-pointer ARRAY aggregation across the merge of many covered frames.
                    final String fl = "SELECT first(a_arr), last(a_arr), count() FROM %s WHERE sym = '" + sym + "'";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(fl, "ref"), String.format(fl, "cov"), LOG);
                    // Var-size BINARY + ARRAY projection across many worker-decoded frames.
                    final String proj = "SELECT a_arr, a_bin FROM %s WHERE sym = '" + sym + "'";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(proj, "ref"), String.format(proj, "cov"), LOG);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelCoveredDecodeDescendingTraverseMatchesReference() throws Exception {
        // Descending covered scans route through fillFrameByTraverse (cheapEligible == false; the
        // cheap O(genCount) path is forward-only) using the BACKWARD posting reader, decoded on the
        // async workers -- the same traverse-fallback worker decode a forward MIXED-gen bail
        // reaches. With a residual filter forcing the async path, a 4-worker descending covered
        // query (scalar + var-size covered columns) must match a non-indexed twin.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute("CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px, tag), " +
                        "px DOUBLE, tag STRING) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute("CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, px DOUBLE, tag STRING) " +
                        "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                final int rows = 30_000;
                engine.execute("INSERT INTO cov SELECT" +
                        " ('2024-01-01'::TIMESTAMP + (x-1)*60_000_000L)::timestamp," +
                        " ('S' || ((x-1) % 6))::symbol, (x % 997)::double," +
                        " case when x % 9 = 0 then null else ('tag-' || (x % 80)) end" +
                        " FROM long_sequence(" + rows + ")", ctx);
                engine.execute("INSERT INTO ref SELECT * FROM cov", ctx);
                engine.releaseAllWriters();

                for (String sym : new String[]{"S0", "S3"}) {
                    // Descending projection + residual filter -> async covered filter, descending
                    // frame production -> fillFrameByTraverse on the workers (record path).
                    final String proj = "SELECT ts, px, tag FROM %s WHERE sym = '" + sym + "' AND px > 0.5 ORDER BY ts DESC";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(proj, "ref"), String.format(proj, "cov"), LOG);
                    // first/last over the covered scalar + var columns.
                    final String agg = "SELECT first(px), last(px), first(tag), last(tag), count() FROM %s WHERE sym = '" + sym + "'";
                    TestUtils.assertSqlCursors(compiler, ctx, String.format(agg, "ref"), String.format(agg, "cov"), LOG);
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testRepeatedCoveredQueriesDoNotLeakFrozenReaders() throws Exception {
        // A reader left frozen by a covered query would make reloadConditionally()
        // a permanent no-op and break the NEXT query against the same partition;
        // a per-query native buffer leak would trip assertMemoryLeak. Run a covered
        // query several times in a row, then a covered query that touches the same
        // partitions again (and a plain non-covered query), all under one
        // assertMemoryLeak -- so unfreeze-at-teardown and leak-freedom are proven.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px)," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "INSERT INTO cov SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 20000L)::timestamp," +
                                        " ('S' || ((x - 1) % 4))::symbol," +
                                        " (x % 997)::double" +
                                        " FROM long_sequence(40000)",
                                sqlExecutionContext
                        );
                        engine.releaseAllWriters();

                        // Capture the first result, then prove every repeat matches it
                        // (a frozen-reader hangover would silently return stale data or
                        // throw on reload; a leak would fail assertMemoryLeak).
                        final String q = "SELECT sum(px), count() FROM cov WHERE sym = 'S1'";
                        sink.clear();
                        TestUtils.printSql(compiler, sqlExecutionContext, q, sink);
                        final String expected = sink.toString();
                        for (int i = 0; i < 5; i++) {
                            sink.clear();
                            TestUtils.printSql(compiler, sqlExecutionContext, q, sink);
                            TestUtils.assertEquals("covered query repeat " + i + " must match first run", expected, sink.toString());
                        }
                        // A plain (non-covered) query after the covered ones must also
                        // succeed -- the reader is not left frozen.
                        sink.clear();
                        TestUtils.printSql(compiler, sqlExecutionContext, "SELECT count() FROM cov", sink);
                        assertFalse(sink.isEmpty());
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testCancelMidCoveredQueryUnfreezesReadersAndDoesNotLeak() throws Exception {
        // Task 13 (1): ABNORMAL EXIT -- circuit-breaker cancellation -- must still
        // unfreeze the covered posting readers and free the per-worker covered
        // buffers. The freeze happens in PageFrameSequence.buildAddressCache /
        // GroupByRecordCursorFactory before dispatch; the unfreeze is wired in the
        // teardown that runs UNCONDITIONALLY on close: PageFrameSequence.close() ->
        // await() -> reset() -> frameAddressCache.unfreezeCoveredReaders() (reduce
        // path) and GroupByRecordCursorFactory's finally (vect path). This test
        // proves those teardowns are actually reached when the query is aborted
        // mid-flight, not only on the happy path.
        //
        // Injection: an ARMABLE circuit breaker (see ArmableTimeoutCircuitBreaker)
        // installed on the execution context and ARMED so the async pipeline's
        // per-frame getState / hasNext breaker checks report a cancellation, and the
        // in-flight covered query throws "cancelled by user". Then we DISARM the same
        // breaker and run the SAME covered query on the SAME table/partition: it must
        // equal a non-indexed twin. A reader left frozen by the aborted query would
        // make its reloadConditionally() a permanent no-op -- the rerun would serve a
        // stale/empty snapshot or fail to reload -- so a correct rerun is the proof
        // the readers were unfrozen. The whole body runs under assertMemoryLeak, so a
        // leaked covered buffer / detached cursor from the aborted query also fails.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px)," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // 64k rows / 32 daily partitions, 8 symbols. With a 200-row
                        // frame cap S3 splits into many covered frames across the 4
                        // workers, so the cancel lands while detached cursors are
                        // iterating the frozen readers -- the genuine abnormal case.
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 40000L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 8))::symbol sym," +
                                        " (x % 997)::double px" +
                                        " FROM long_sequence(64000)";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                        final io.questdb.cairo.sql.SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
                        final ArmableTimeoutCircuitBreaker breaker = new ArmableTimeoutCircuitBreaker(engine);
                        try {
                            context.with(breaker);

                            // Cancel targets must be covered queries whose async
                            // pipeline consults the circuit breaker PER FRAME (so an
                            // armed breaker reliably aborts mid-flight, while covered
                            // frames are decoding on the workers under freeze): the
                            // covered residual FILTER projection (Async Filter ->
                            // CoveringIndex, checks the breaker in hasNext) and the
                            // covered residual AGGREGATION (non-vectorized Async Group
                            // By -> reduce, whose getState check cancels the sequence
                            // per frame). A pure sum() with no residual takes the
                            // VECTORIZED group-by, which only checks the breaker under
                            // dispatch-queue contention, so it is not a reliable cancel
                            // target here.
                            final String coveredResidualFilter = "SELECT ts, px FROM cov WHERE sym = 'S3' AND px > 0.5";
                            final String coveredResidualAgg = "SELECT sum(px), count() FROM cov WHERE sym = 'S3' AND px > 0.5";

                            // ARM -> the in-flight covered queries must be cancelled.
                            breaker.arm();
                            assertCoveredQueryCancels(compiler, context, coveredResidualFilter);
                            assertCoveredQueryCancels(compiler, context, coveredResidualAgg);

                            // DISARM -> the same covered queries on the same partition
                            // must now run to completion and equal the non-indexed
                            // twin. Only possible if the aborted query left the
                            // readers UNFROZEN (else reloadConditionally is a no-op
                            // and the snapshot is stale / the reload throws).
                            breaker.disarm();
                            TestUtils.assertSqlCursors(
                                    compiler, context,
                                    "SELECT ts, px FROM ref WHERE sym = 'S3' AND px > 0.5",
                                    coveredResidualFilter, LOG);
                            TestUtils.assertSqlCursors(
                                    compiler, context,
                                    "SELECT sum(px), count() FROM ref WHERE sym = 'S3' AND px > 0.5",
                                    coveredResidualAgg, LOG);
                        } finally {
                            // Restore the original (NOOP) breaker so engine/cursor
                            // teardown is not itself cancelled. The AtomicBoolean
                            // breaker holds no native resources, so nothing to free.
                            context.with(original);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testRepeatedCancelThenRerunCoveredQuerySteadyState() throws Exception {
        // Task 13 (2): repeated abnormal-then-normal cycles must reach a steady
        // state -- no cumulative covered-buffer leak and no stuck-frozen reader
        // across cycles. We cancel a covered query, then run it correctly to
        // completion, several times in a loop, all under one assertMemoryLeak.
        // If unfreeze were skipped on the cancel path, the FIRST successful rerun
        // would already serve stale data (a frozen reader cannot reload); if a
        // covered buffer leaked per aborted cycle it would accumulate and trip the
        // leak detector at the end. (Exception-mid-decode is NOT cleanly injectable
        // from SQL without a fault-injection hook, so the circuit-breaker cancel is
        // used as the representative abnormal exit -- it drives the exact same
        // close() -> await() -> reset() -> unfreeze teardown.)
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px)," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 40000L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 8))::symbol sym," +
                                        " (x % 997)::double px" +
                                        " FROM long_sequence(64000)";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        final SqlExecutionContextImpl context = (SqlExecutionContextImpl) sqlExecutionContext;
                        final io.questdb.cairo.sql.SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
                        final ArmableTimeoutCircuitBreaker breaker = new ArmableTimeoutCircuitBreaker(engine);

                        // The covered residual aggregation routes through the
                        // non-vectorized Async Group By (reduce path), whose per-frame
                        // getState check is what an armed breaker trips -- a reliable
                        // mid-flight cancel target for the covered decode.
                        final String coveredAgg = "SELECT sum(px), count() FROM cov WHERE sym = 'S5' AND px > 0.5";
                        final String refAgg = "SELECT sum(px), count() FROM ref WHERE sym = 'S5' AND px > 0.5";

                        // Reference result computed with the default (non-tripping)
                        // breaker so the rerun has an oracle.
                        sink.clear();
                        TestUtils.printSql(compiler, sqlExecutionContext, refAgg, sink);
                        final String expected = sink.toString();

                        try {
                            context.with(breaker);
                            for (int i = 0; i < 4; i++) {
                                // Abnormal exit: cancel mid-flight.
                                breaker.arm();
                                assertCoveredQueryCancels(compiler, context, coveredAgg);

                                // Normal exit on the SAME partition: must match the
                                // oracle every cycle (proves the previous cancel left
                                // the readers unfrozen and nothing leaked into this run).
                                breaker.disarm();
                                sink.clear();
                                TestUtils.printSql(compiler, context, coveredAgg, sink);
                                TestUtils.assertEquals(
                                        "covered rerun after cancel cycle " + i + " must match the reference",
                                        expected, sink.toString());
                            }
                        } finally {
                            context.with(original);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testWriterCommitDuringCoveredQuerySnapshotStableThenReloads() throws Exception {
        // Task 13 (3): a covered query in flight holds a STABLE SNAPSHOT while a
        // writer commits, and a query started AFTER the commit reloads and sees the
        // new rows. This is the SQL-level, covered-scan counterpart of
        // PostingReaderConcurrentReadTest#testFrozenReaderSuppressesReload (which
        // proves the same suppress-reload-while-pinned / reload-after invariant
        // directly on the frozen posting reader): a concurrent writer that commits
        // MORE rows to the same table must NOT be visible to the in-flight covered
        // query, while a fresh covered query after the commit must reflect them.
        //
        // The in-flight snapshot is held by a COVERING page-frame cursor kept open
        // across the commit: its TableReader (and the per-partition posting readers
        // it carries) is pinned at the query-start txn for the whole iteration, so a
        // second writer publishing a brand-new partition cannot shift it. We drain
        // the held cursor AFTER committing and it still yields only the snapshot
        // rows. After releasing the cursor, a fresh covered SQL aggregation reloads
        // and reflects the committed rows. (Async-path freeze == snapshot isolation
        // is exercised by the cancel tests above, whose disarmed reruns reload
        // correctly, and by the posting-reader freeze test referenced above.)
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px)," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // Initial data: a single day's worth of rows for key 'S0'.
                        engine.execute(
                                "INSERT INTO cov SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 40000L)::timestamp," +
                                        " 'S0'::symbol," +
                                        " (x % 997)::double" +
                                        " FROM long_sequence(2000)",
                                sqlExecutionContext
                        );
                        engine.releaseAllWriters();

                        // Baseline snapshot result (covered aggregation) BEFORE any
                        // concurrent commit.
                        sink.clear();
                        TestUtils.printSql(compiler, sqlExecutionContext, "SELECT sum(px), count() FROM cov WHERE sym = 'S0'", sink);
                        final String snapshotResult = sink.toString();
                        final long snapshotRows = countRows(compiler, sqlExecutionContext, "SELECT count() FROM cov WHERE sym = 'S0'");
                        assertTrue("baseline must have rows", snapshotRows > 0);

                        // --- IN FLIGHT: hold a covering page-frame cursor open, commit
                        // new rows on a SECOND writer, then drain the held cursor. The
                        // held cursor's snapshot must NOT include the new rows. ---
                        final TableToken token = engine.getTableTokenIfExists("cov");
                        assertNotNull(token);
                        long inFlightRows = 0;
                        try (RecordCursorFactory factory = compiler.compile("SELECT ts, sym, px FROM cov WHERE sym = 'S0'", sqlExecutionContext).getRecordCursorFactory();
                             PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                            // Read the first frame so the snapshot (and its posting
                            // reader) is genuinely pinned before the commit lands.
                            PageFrame f = cursor.next(0);
                            assertNotNull("covered query must produce at least one frame", f);
                            inFlightRows += f.getPartitionHi() - f.getPartitionLo();

                            // SECOND writer commits a brand-new partition while the
                            // covering cursor above is mid-iteration.
                            try (TableWriter w = TestUtils.getWriter(engine, token)) {
                                final long base = io.questdb.cairo.MicrosTimestampDriver.INSTANCE.parseFloor(
                                        "2024-02-01T00:00:00.000000Z", 0, "2024-02-01T00:00:00.000000Z".length());
                                for (int i = 0; i < 2000; i++) {
                                    TableWriter.Row r = w.newRow(base + (long) i * 40000L);
                                    r.putSym(1, "S0");
                                    r.putDouble(2, i % 997);
                                    r.append();
                                }
                                w.commit();
                            }

                            // Drain the rest of the held cursor. It must still see only
                            // the pre-commit snapshot row count -- the pinned reader did
                            // not remap to the freshly committed partition.
                            while ((f = cursor.next(0)) != null) {
                                inFlightRows += f.getPartitionHi() - f.getPartitionLo();
                            }
                        }
                        assertEquals("in-flight covered query must see only the snapshot rows, not the rows committed mid-query",
                                snapshotRows, inFlightRows);

                        // --- AFTER COMMIT: a fresh covered query must see the new rows. ---
                        final long afterRows = countRows(compiler, sqlExecutionContext, "SELECT count() FROM cov WHERE sym = 'S0'");
                        assertTrue("a covered query started AFTER the commit must see the new rows (reader unfrozen + reloaded): before="
                                        + snapshotRows + ", after=" + afterRows,
                                afterRows > snapshotRows);
                        assertEquals("exactly the 2000 committed rows must appear", snapshotRows + 2000, afterRows);

                        // And the covered aggregate over the larger snapshot must
                        // differ from the pre-commit aggregate (it now sums more rows),
                        // confirming the new partition is actually decoded, not skipped.
                        sink.clear();
                        TestUtils.printSql(compiler, sqlExecutionContext, "SELECT sum(px), count() FROM cov WHERE sym = 'S0'", sink);
                        final String afterResult = sink.toString();
                        assertNotEquals("post-commit covered aggregate must differ from the pre-commit snapshot", snapshotResult, afterResult);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testCoveredQueryWithManyFramesNoLeak() throws Exception {
        // Regression GUARD for the worker covered-buffer lifecycle on the async
        // REDUCE path (Task 11). Covered columns are decoded on async workers into
        // per-worker PageFrameMemoryPool.CoveringBuffers, cached per frame in
        // coveringByFrame and freed only at a query boundary (of/clear/close), NOT
        // by the per-frame releaseParquetBuffers() -- because a covered frame
        // reports NATIVE, so a zero-copy first()/last() over a covered varchar
        // STORES A RAW POINTER into those buffers for the merge phase, and a
        // per-frame free would dangle it.
        //
        // The concern is the SHARED ring-queue reduce-task pools
        // (PageFrameReduceTask.frameMemoryPool, one per ring slot): on the reduce
        // path PageFrameReduceTask.collected() frees covered buffers via clear()
        // only for the last ~frameQueueCapacity frames, and the non-last branch
        // (releaseFrameMemory -> releaseParquetBuffers) deliberately keeps them.
        // The async FILTER over a covering scan (`SELECT cols ... WHERE key AND
        // residual`, verified via EXPLAIN to be `Async Filter -> CoveringIndex`)
        // is the path that decodes covered frames into those ring-slot pools, so
        // it is what this test drives -- with > frameQueueCapacity covered frames
        // for a single hot key.
        //
        // assertMemoryLeak alone CANNOT catch a ring-slot-pool covered-buffer
        // residue: TestUtils.execute closes the (fresh) engine -- and with it the
        // message bus and every reduce-task pool, freeing any lingering covered
        // buffers -- BEFORE the leak checker snapshots. So we measure
        // NATIVE_INDEX_READER (the tag CoveringBuffers allocate under) directly,
        // AFTER each covered query returns but WHILE the engine/bus still lives,
        // and assert it returns to the pre-query baseline. That is the only signal
        // that observes ring-slot residue, and it locks in the structural reason
        // there is none: the ring queue has exactly frameQueueCapacity slots, so
        // the last frameQueueCapacity frames re-occupy every still-active slot and
        // every slot's final collected task therefore hits the clear() (freeing)
        // branch -- no slot's covered buffers survive the query.
        final int rowsPerPartition = 2000;
        final int partitions = 64; // 128k rows across 64 daily partitions
        final long rows = (long) rowsPerPartition * partitions;
        // 2 symbols => ~64k rows for the hot key S0. With a 200-row frame cap that
        // is ~320 covered frames for S0 -- far more than the default ring-queue
        // capacity (min(4*queryWorkers, 256)), so the non-last (keep) collect
        // branch is exercised heavily and any residue would accumulate.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);

        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (s, px)," +
                                        "  px DOUBLE," +
                                        "  s VARCHAR" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  px DOUBLE," +
                                        "  s VARCHAR" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // A covered VARCHAR (s) is included so first()/last() over a
                        // covered varchar exercises the zero-copy stable-pointer path
                        // whose lifetime the per-query free protects.
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * " + (86_400_000_000L / rowsPerPartition) + "L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 2))::symbol sym," +
                                        " (x % 997)::double px," +
                                        " ('v' || (x % 50))::varchar s" +
                                        " FROM long_sequence(" + rows + ")";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        // Async FILTER over a covering scan: residual `px > 0.5`
                        // forces the row filter (so it is NOT a covered group-by),
                        // and projecting the covered columns forces their decode into
                        // the ring-slot reduce-task pools. > frameQueueCapacity frames.
                        final String coveredFilter = "SELECT ts, sym, s, px FROM cov WHERE sym = 'S0' AND px > 0.5";
                        final String refFilter = "SELECT ts, sym, s, px FROM ref WHERE sym = 'S0' AND px > 0.5";

                        // Correctness: covered (parallel, decoded from the sidecar)
                        // must match the plain non-indexed twin.
                        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, refFilter, coveredFilter, LOG);

                        // Warm once so the posting readers and any steady-state
                        // NATIVE_INDEX_READER allocations are in place, then baseline.
                        sink.clear();
                        TestUtils.printSql(compiler, sqlExecutionContext, coveredFilter, sink);
                        final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);

                        // Run the covered filter several times; after EACH run, with
                        // the engine still alive, NATIVE_INDEX_READER must be back at
                        // the baseline. A ring-slot pool that kept covered buffers
                        // past query end (the leak this guards) would push it above.
                        for (int i = 0; i < 4; i++) {
                            sink.clear();
                            TestUtils.printSql(compiler, sqlExecutionContext, coveredFilter, sink);
                            final long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                            assertEquals(
                                    "covered worker buffers must be freed at query end (run " + i +
                                            "): NATIVE_INDEX_READER delta=" + (after - baseline),
                                    baseline,
                                    after
                            );
                        }

                        // first()/last() over the covered VARCHAR routes through the
                        // async group-by (atom per-worker pools). It stores raw stable
                        // pointers into the covered buffers for the post-await merge,
                        // so it both exercises the stable-pointer path (proving the
                        // free is NOT premature -> no use-after-free) and must match
                        // the reference. > frameQueueCapacity covered frames for S0.
                        final String coveredFirstLast = "SELECT sym, first(s), last(s), count() FROM cov WHERE sym = 'S0'";
                        final String refFirstLast = "SELECT sym, first(s), last(s), count() FROM ref WHERE sym = 'S0'";
                        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, refFirstLast, coveredFirstLast, LOG);

                        final long beforeAgg = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER);
                        sink.clear();
                        TestUtils.printSql(compiler, sqlExecutionContext, coveredFirstLast, sink);
                        assertEquals(
                                "covered first/last worker buffers must be freed at query end: NATIVE_INDEX_READER delta="
                                        + (Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER) - beforeAgg),
                                beforeAgg,
                                Unsafe.getMemUsedByTag(MemoryTag.NATIVE_INDEX_READER)
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testNativeFrameReportsBaseFormatReaderAndColumnSource() throws Exception {
        // Task 6 decorator surface: over NATIVE partitions, the covering page
        // frame must report its base partition format (NATIVE), carry the
        // per-partition posting reader, and report per-column source: COVERED
        // for BOTH the INCLUDE-mapped column and the symbol key — both are served
        // by the covering frame (the key via broadcast). Only a genuinely external
        // column added by a wrapper above the frame is DIRECT.
        //
        // Projection "sym, payload": query col 0 is the indexed symbol key, query
        // col 1 is the covered INCLUDE column; both report COVERED.
        final int symbolColumnIndex = 0;
        final int coveredColumnIndex = 1;
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_surface (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (payload),
                        payload LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_surface
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        x
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            try (var factory = select("SELECT sym, payload FROM t_pf_surface WHERE sym = 'A'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                int frames = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    frames++;
                    assertEquals("base format of a native partition must be NATIVE",
                            PartitionFormat.NATIVE, f.getFormat());
                    assertNotNull("per-partition posting reader must be carried on the frame",
                            f.getIndexReader(symbolColumnIndex, IndexReader.DIR_FORWARD));
                    assertEquals("covered INCLUDE column must report COVERED",
                            DataSource.COVERED, f.getColumnSource(coveredColumnIndex));
                    assertEquals("symbol key column is served by the covering frame (broadcast), so COVERED",
                            DataSource.COVERED, f.getColumnSource(symbolColumnIndex));
                }
                assertTrue("expected at least one covering page frame", frames > 0);
            }
        });
    }

    @Test
    public void testAddressCacheNoCoveredMetadataForNativeFrame() throws Exception {
        // Control: a plain (non-covering) native page frame must report NO
        // covered metadata when driven through the same PageFrameAddressCache.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_plain (
                        ts TIMESTAMP,
                        sym SYMBOL,
                        payload LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_plain
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        x
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            try (RecordCursorFactory factory = select("SELECT sym, payload FROM t_pf_plain");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache()) {
                // Drive the cache the same way the async pipeline does
                // (PageFrameSequence.buildAddressCache).
                addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
                int frameCount = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    addressCache.add(frameCount++, f);
                }
                assertTrue("expected at least one native page frame", frameCount > 0);
                assertFalse("plain native frames must not register as covered", addressCache.hasCoveredFrames());
                for (int i = 0; i < frameCount; i++) {
                    assertFalse("frame " + i + " must report no covered columns", addressCache.isFrameCovered(i));
                    assertEquals("covered key sentinel", SymbolTable.VALUE_NOT_FOUND, addressCache.getCoveredKey(i));
                    assertEquals("covered rowLo sentinel", -1, addressCache.getCoveredRowLo(i));
                    assertEquals("covered rowHi sentinel", -1, addressCache.getCoveredRowHi(i));
                    assertNull("no covered index reader on a plain frame", addressCache.getCoveredIndexReader(i));
                    assertFalse("column 0 not covered", addressCache.isColumnCovered(i, 0));
                    assertFalse("column 1 not covered", addressCache.isColumnCovered(i, 1));
                }
            }
        });
    }

    @Test
    public void testFreezeCoveredReadersWiringFlipsAllReaders() throws Exception {
        // Review finding #2 (negative control for the dispatch freeze WIRING): the parallel-decode
        // pipeline relies on PageFrameSequence.buildAddressCache -> freezeCoveredReaders() to make
        // every covered partition's posting reader read-only BEFORE any worker is dispatched, and
        // unfreezeCoveredReaders() at teardown. The freeze PRIMITIVE already has a red-on-break test
        // (PostingReaderConcurrentReadTest.testFrozenReaderSuppressesReload, which fails if a frozen
        // reader stops suppressing reloadConditionally). This pins the other half -- the wiring that
        // flips the flag on EVERY covered reader -- by driving a covering query's frames through a
        // PageFrameAddressCache exactly as the async pipeline does and asserting the freeze/unfreeze
        // calls actually toggle isFrozen() on each distinct reader. If that loop were dropped or a
        // reader were missed, this goes red.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE cov (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            // Several daily partitions so the cache holds several distinct per-partition readers.
            execute("INSERT INTO cov SELECT ('2024-01-01'::timestamp + (x-1)*3600000000L)::timestamp, " +
                    "('K'||(x%3)), x::double FROM long_sequence(240)");
            engine.releaseAllWriters();

            try (RecordCursorFactory factory = select("SELECT sym, px FROM cov WHERE sym = 'K1'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache()) {
                addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
                int frameCount = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    addressCache.add(frameCount++, f);
                }
                assertTrue("expected covered frames", addressCache.hasCoveredFrames());

                // Distinct covered readers across the frames (one per partition; multiple frames in
                // the same partition share the same reader instance).
                final java.util.IdentityHashMap<IndexReader, Boolean> readers = new java.util.IdentityHashMap<>();
                for (int i = 0; i < frameCount; i++) {
                    IndexReader r = addressCache.getCoveredIndexReader(i);
                    assertNotNull("covered frame " + i + " must carry a posting reader", r);
                    readers.put(r, Boolean.TRUE);
                    assertFalse("reader must start unfrozen", r.isFrozen());
                }
                assertFalse("expected at least one covered reader", readers.isEmpty());

                addressCache.freezeCoveredReaders();
                for (IndexReader r : readers.keySet()) {
                    assertTrue("freezeCoveredReaders must freeze every covered reader", r.isFrozen());
                }

                addressCache.unfreezeCoveredReaders();
                for (IndexReader r : readers.keySet()) {
                    assertFalse("unfreezeCoveredReaders must unfreeze every covered reader", r.isFrozen());
                }
            }
        });
    }

    @Test
    public void testUnorderedSequenceThrowingClearStillUnfreezesAndClosesPopulatedCache() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE unordered_cleanup (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (px), px DOUBLE) " +
                    "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO unordered_cleanup SELECT " +
                    "('2024-01-01'::timestamp + (x - 1) * 3_600_000_000L)::timestamp, " +
                    "'K1', x::double FROM long_sequence(48)");
            engine.releaseAllWriters();

            final RuntimeException clearFailure = new RuntimeException("atom clear");
            final RuntimeException cacheCloseFailure = new RuntimeException("cache close");
            final RuntimeException cursorCloseFailure = new RuntimeException("cursor close");
            final RuntimeException atomCloseFailure = new RuntimeException("atom close");
            final CleanupFailingAtom atom = new CleanupFailingAtom(clearFailure, atomCloseFailure);
            final CleanupTrackingAddressCache cache = new CleanupTrackingAddressCache(cacheCloseFailure);
            final int[] cursorCloseCount = {0};
            final int[] cursorObservedCacheCloseCount = {0};

            try (RecordCursorFactory factory = select("SELECT sym, px FROM unordered_cleanup WHERE sym = 'K1'")) {
                final UnorderedPageFrameSequence<CleanupFailingAtom> sequence = new UnorderedPageFrameSequence<>(
                        engine,
                        configuration,
                        engine.getMessageBus(),
                        atom,
                        (_, _, _, _, _, _) -> {
                        },
                        1
                );
                final PageFrameAddressCache constructorCache = getField(
                        UnorderedPageFrameSequence.class,
                        sequence,
                        "frameAddressCache"
                );
                constructorCache.close();
                setField(UnorderedPageFrameSequence.class, sequence, "frameAddressCache", cache);

                sequence.of(factory, sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                final PageFrameCursor delegateCursor = getField(
                        UnorderedPageFrameSequence.class,
                        sequence,
                        "frameCursor"
                );
                final PageFrameCursor trackingCursor = (PageFrameCursor) Proxy.newProxyInstance(
                        PageFrameCursor.class.getClassLoader(),
                        new Class[]{PageFrameCursor.class},
                        (_, method, args) -> {
                            try {
                                if ("close".equals(method.getName())) {
                                    cursorCloseCount[0]++;
                                    cursorObservedCacheCloseCount[0] = cache.closeCount;
                                    method.invoke(delegateCursor, args);
                                    throw cursorCloseFailure;
                                }
                                return method.invoke(delegateCursor, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        }
                );
                setField(UnorderedPageFrameSequence.class, sequence, "frameCursor", trackingCursor);

                sequence.prepareForDispatch();
                assertTrue("cache must contain page frames", sequence.getFrameCount() > 0);
                assertEquals(sequence.getFrameCount(), cache.getFrameCount());
                assertTrue("cache must contain covered frames", cache.hasCoveredFrames());

                final IdentityHashMap<IndexReader, Boolean> readers = new IdentityHashMap<>();
                for (int i = 0; i < cache.getFrameCount(); i++) {
                    final IndexReader reader = cache.getCoveredIndexReader(i);
                    assertNotNull("covered frame must retain its real posting reader", reader);
                    readers.put(reader, Boolean.TRUE);
                    assertTrue("reader must be frozen before cleanup", reader.isFrozen());
                }
                assertFalse("expected at least one real covered reader", readers.isEmpty());

                try {
                    sequence.close();
                    fail("throwing cleanup should fail");
                } catch (Throwable failure) {
                    assertSame("atom clear must remain primary", failure, clearFailure);
                    assertArrayEquals(
                            new Throwable[]{cacheCloseFailure, cursorCloseFailure, atomCloseFailure},
                            failure.getSuppressed()
                    );
                }

                for (IndexReader reader : readers.keySet()) {
                    assertFalse("cleanup must unfreeze the real posting reader", reader.isFrozen());
                }
                assertEquals(1, atom.clearCount);
                assertEquals(1, atom.closeCount);
                assertEquals(1, cache.unfreezeCount);
                assertEquals(1, cache.closeCount);
                assertTrue("cache close must observe unfrozen readers", cache.wereReadersUnfrozenOnClose);
                assertEquals("cursor must close after cache", 1, cursorObservedCacheCloseCount[0]);
                assertEquals(1, cursorCloseCount[0]);

                // close() consumes every owner before rethrowing, so retry is a no-op.
                sequence.close();
                assertEquals(1, atom.clearCount);
                assertEquals(1, atom.closeCount);
                assertEquals(1, cache.unfreezeCount);
                assertEquals(1, cache.closeCount);
                assertEquals(1, cursorCloseCount[0]);
            }
        });
    }

    @Test
    public void testAddressCacheStoresCoveredMetadata() throws Exception {
        // Task 8: drive a covering query's produced frames through a
        // PageFrameAddressCache the same way the async pipeline does, then read
        // back the per-frame covered decode metadata the worker-side arm (Task 9)
        // will consume: the resolved symbol key, the base row range, the posting
        // reader, and which columns are covered (matching getColumnSource).
        //
        // Projection "sym, payload": query col 0 is the indexed symbol key
        // (COVERED — served by the covering frame via broadcast), query col 1 is
        // the covered INCLUDE column (COVERED).
        final int symbolColumnIndex = 0;
        final int coveredColumnIndex = 1;
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_cache (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (payload),
                        payload LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_cache
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        x
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            // 'A' is the first (and only) symbol inserted -> key 0.
            final int expectedKey = 0;

            try (RecordCursorFactory factory = select("SELECT sym, payload FROM t_pf_cache WHERE sym = 'A'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache()) {
                // Drive the cache exactly like PageFrameSequence.buildAddressCache:
                // of(metadata, columnMapping, external) then add(frameIndex, frame).
                addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
                int frameCount = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    // Capture the live frame's row range so we can cross-check the
                    // cached values (the reusable frame is mutated on each next()).
                    final long expectedRowLo = f.getCoveredRowLo();
                    final long expectedRowHi = f.getCoveredRowHi();
                    final int expectedFrameKey = f.getCoveredKey();
                    addressCache.add(frameCount, f);

                    assertTrue("frame must register covered columns", addressCache.isFrameCovered(frameCount));
                    assertEquals("covered symbol key round-trips", expectedFrameKey, addressCache.getCoveredKey(frameCount));
                    assertEquals("resolved key for sym='A'", expectedKey, addressCache.getCoveredKey(frameCount));
                    assertEquals("covered rowLo round-trips", expectedRowLo, addressCache.getCoveredRowLo(frameCount));
                    assertEquals("covered rowHi round-trips", expectedRowHi, addressCache.getCoveredRowHi(frameCount));
                    assertTrue("covered row range must be non-empty", expectedRowHi > expectedRowLo);
                    assertNotNull("posting reader must be cached", addressCache.getCoveredIndexReader(frameCount));
                    assertEquals("cached reader is the frame's posting reader",
                            f.getIndexReader(symbolColumnIndex, IndexReader.DIR_FORWARD),
                            addressCache.getCoveredIndexReader(frameCount));
                    // Covered-column descriptor must match getColumnSource.
                    assertTrue("symbol key column IS covered (served by the covering frame)",
                            addressCache.isColumnCovered(frameCount, symbolColumnIndex));
                    assertTrue("INCLUDE column IS covered",
                            addressCache.isColumnCovered(frameCount, coveredColumnIndex));
                    assertEquals(DataSource.COVERED, f.getColumnSource(symbolColumnIndex));
                    assertEquals(DataSource.COVERED, f.getColumnSource(coveredColumnIndex));

                    frameCount++;
                }
                assertTrue("expected at least one covering page frame", frameCount > 0);
                assertTrue("cache must flag that it holds covered frames", addressCache.hasCoveredFrames());
                // Task 7: single-key covered frames are produced METADATA-ONLY, so
                // the flat covered (and symbol) page addresses stored here are now
                // PLACEHOLDERS (0). The covered metadata above (key, rowLo/rowHi,
                // reader, isColumnCovered) is still populated; the worker covered
                // arm (PageFrameMemoryPool#patchCoveredFrameMemory) decodes the
                // values and rebinds these addresses. The symbol key column is the
                // -1 mapping (DIRECT) but for a covered frame it too is synthesized
                // on the worker, so its production placeholder is 0 as well.
                for (int i = 0; i < frameCount; i++) {
                    long off = addressCache.toColumnOffset(i);
                    assertEquals("metadata-only single-key frame: covered page address is a placeholder (0)",
                            0L, addressCache.getPageAddresses().get(off + coveredColumnIndex));
                    assertEquals("metadata-only single-key frame: symbol-key page address is a placeholder (0)",
                            0L, addressCache.getPageAddresses().get(off + symbolColumnIndex));
                }
            }
        });
    }

    @Test
    public void testRecordFastPathDecodesCoveredIntoWorkerBuffers() throws Exception {
        // Task 10 white-box headline: navigateTo(int, PageFrameMemoryRecord) -- the
        // RECORD fast-path used by row filters / SelectedRecord / recordAt /
        // negative-limit / PageFrameRecordCursorImpl -- must decode covered columns
        // on the worker into this pool's per-frame CoveringBuffers and bind the
        // RECORD to them, NOT serve them off the eager flat addresses via the
        // frame-index-only NATIVE fast-return.
        //
        // A covered frame reports NATIVE, so before this task the record path took
        // the NATIVE arm's frame-index-only fast-return and left boundPool == null
        // and getCoveredFrameCount() == 0. After this task the covered arm runs:
        // a CoveringBuffers is cached (covered count > 0), the record is stamped
        // with this pool + its bind generation (so the unsafe frame-index-only
        // fast-return cannot fire on a covered record), the parquet cache stays
        // empty (the record did NOT fall through to the parquet branch), and the
        // covered LONG reads correctly.
        final int symbolColumnIndex = 0;
        final int coveredColumnIndex = 1;
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_rec (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (payload),
                        payload LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_rec
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        x
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            try (RecordCursorFactory factory = select("SELECT sym, payload FROM t_pf_rec WHERE sym = 'A'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(0L);
                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
                int frameCount = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    addressCache.add(frameCount++, f);
                }
                assertTrue("expected at least one covering page frame", frameCount > 0);

                pool.of(addressCache);
                record.of(cursor); // symbol table source for getSymA

                // Reconstruct expected payloads from the base table order (sym='A'
                // matches every row; payload == x, 1..10).
                long expectedSum = 0;
                for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                    pool.navigateTo(frameIndex, record);

                    // The covered arm ran for THIS frame: a CoveringBuffers exists.
                    assertTrue(
                            "covered worker buffers must be cached after a record navigate (frame " + frameIndex + ")",
                            pool.getCoveredFrameCount() > 0
                    );
                    // The record is bound to this pool's covered buffers with the
                    // parquet-style guard, so a later covered navigate cannot take
                    // the unsafe frame-index-only NATIVE fast-return.
                    assertEquals(
                            "covered record must be bound to the decoding pool",
                            pool,
                            record.getBoundPool()
                    );
                    assertEquals(
                            "covered record bind generation must match the pool",
                            pool.getBindGeneration(),
                            record.getBoundGeneration()
                    );
                    // It did NOT fall through to the parquet branch.
                    assertEquals(
                            "covered record path must not allocate parquet buffers",
                            0,
                            pool.getCachedFrameCount()
                    );

                    final long frameSize = addressCache.getFrameSize(frameIndex);
                    for (long r = 0; r < frameSize; r++) {
                        record.setRowIndex(r);
                        assertEquals("covered symbol key must read 'A'", "A", record.getSymA(symbolColumnIndex).toString());
                        expectedSum += record.getLong(coveredColumnIndex);
                    }
                }
                // payloads 1..10 -> 55, proving the covered LONG decoded correctly
                // off the worker buffers across every frame.
                assertEquals("covered payload sum over the record path", 55L, expectedSum);

                // Re-navigating the same frame is the idempotent fast-return: it
                // must keep the same binding and not grow the parquet cache.
                pool.navigateTo(0, record);
                assertEquals(pool, record.getBoundPool());
                assertEquals(0, pool.getCachedFrameCount());
            }
        });
    }

    @Test
    public void testCoveredDecodeBuffersAreTrackedAndReleased() throws Exception {
        // Level-3 review follow-up (M1/M2): covered decode buffers charge a tracked byte
        // counter (getCoveredCachedBytes) that grows as covered frames decode and returns
        // to zero once the buffers are released. This keeps the query-lifetime covered
        // buffers observable (they cannot be LRU-evicted mid-query, so the per-query
        // MemoryTracker captured by each CoveringBuffers is the real ceiling) and proves
        // the symmetric free accounts every byte it allocated -- a leak would leave the
        // counter non-zero (and trip releaseCoveringBuffers()'s assert) and assertMemoryLeak
        // would fail on the native allocation.
        final int coveredColumnIndex = 1;
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_pf_bytes (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (payload),
                        payload VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_pf_bytes
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        'payload-value-' || x
                    FROM long_sequence(50)
                    """);
            engine.releaseAllWriters();

            try (RecordCursorFactory factory = select("SELECT sym, payload FROM t_pf_bytes WHERE sym = 'A'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC);
                 PageFrameAddressCache addressCache = new PageFrameAddressCache();
                 PageFrameMemoryPool pool = new PageFrameMemoryPool(0L);
                 PageFrameMemoryRecord record = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER)) {
                addressCache.of(factory.getMetadata(), cursor.getColumnMapping(), cursor.isExternal());
                int frameCount = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    addressCache.add(frameCount++, f);
                }
                assertTrue("expected at least one covering page frame", frameCount > 0);

                pool.of(addressCache);
                record.of(cursor);

                assertEquals("no covered bytes before any decode", 0L, pool.getCoveredCachedBytes());

                // Drive the covered VARCHAR through the worker decode path (aux + data
                // native buffers, both tracked) and read it so the data vector grows.
                final StringSink sink = new StringSink();
                for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                    pool.navigateTo(frameIndex, record);
                    final long frameSize = addressCache.getFrameSize(frameIndex);
                    for (long r = 0; r < frameSize; r++) {
                        record.setRowIndex(r);
                        sink.clear();
                        sink.put(record.getVarcharA(coveredColumnIndex));
                        assertTrue("covered VARCHAR must decode", sink.toString().startsWith("payload-value-"));
                    }
                }
                assertTrue(
                        "covered decode buffers must be tracked after navigation",
                        pool.getCoveredCachedBytes() > 0
                );

                // Releasing the covered buffers (new query / clear path via of()) accounts
                // every byte back to zero and drops the retained per-frame buffers.
                pool.of(addressCache);
                assertEquals("covered byte accounting must net to zero after release", 0L, pool.getCoveredCachedBytes());
                assertEquals("covered frame buffers must be dropped after release", 0, pool.getCoveredFrameCount());
            }
        });
    }

    @Test
    public void testSingleKeyProductionIsMetadataOnlyMultiKeyEager() throws Exception {
        // Task 7 headline: single-key frame production NO LONGER materializes
        // covered values (decode runs only on the workers), while the multi-key
        // merge still materializes eagerly.
        //
        //  (1) single-key (`sym = 'S0'`): no row is written via the cursor's
        //      writeCoveredRow (the @TestOnly counter stays 0), and every produced
        //      frame reports PLACEHOLDER (0) covered + symbol page addresses.
        //  (2) multi-key (`sym IN ('S0','S1')`): the eager merge still runs, so the
        //      counter is > 0 and the produced frames carry REAL page addresses.
        final int coveredColumnIndex = 1; // query col 1 = covered INCLUDE (payload)
        final int symbolColumnIndex = 0;  // query col 0 = indexed symbol key
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_meta_only (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (payload),
                        payload LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            // Two symbols, several rows each, so both keys have covered rows.
            execute("""
                    INSERT INTO t_meta_only
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'S' || (x % 2),
                        x
                    FROM long_sequence(40)
                    """);
            engine.releaseAllWriters();

            // (1) SINGLE-KEY: metadata-only production.
            CoveringIndexRecordCursorFactory.resetCoveredRowsWrittenForTesting();
            try (RecordCursorFactory factory = select("SELECT sym, payload FROM t_meta_only WHERE sym = 'S0'");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                int frames = 0;
                long rows = 0;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    frames++;
                    rows += f.getPartitionHi() - f.getPartitionLo();
                    // The covered column AND the symbol key are decoded on the
                    // worker, so production emits placeholder (0) page addresses.
                    assertEquals("single-key covered column page address is a production placeholder (0)",
                            0L, f.getPageAddress(coveredColumnIndex));
                    assertEquals("single-key symbol-key page address is a production placeholder (0)",
                            0L, f.getPageAddress(symbolColumnIndex));
                    // ... but the covered DECODE metadata is still populated.
                    assertEquals("covered column still reports COVERED",
                            DataSource.COVERED, f.getColumnSource(coveredColumnIndex));
                    assertNotNull("frame still carries its posting reader",
                            f.getIndexReader(symbolColumnIndex, IndexReader.DIR_FORWARD));
                    assertTrue("covered row range is recorded", f.getCoveredRowHi() > f.getCoveredRowLo());
                }
                assertTrue("expected at least one single-key covering frame", frames > 0);
                assertTrue("the single-key traverse must visit rows (warm + count)", rows > 0);
            }
            assertEquals("single-key production must NOT eagerly materialize any covered row",
                    0L, CoveringIndexRecordCursorFactory.getCoveredRowsWrittenForTesting());

            // (2) MULTI-KEY: eager merge retained.
            CoveringIndexRecordCursorFactory.resetCoveredRowsWrittenForTesting();
            try (RecordCursorFactory factory = select("SELECT sym, payload FROM t_meta_only WHERE sym IN ('S0', 'S1')");
                 PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                int frames = 0;
                boolean anyRealCoveredAddr = false;
                PageFrame f;
                while ((f = cursor.next(0)) != null) {
                    frames++;
                    if (f.getPageAddress(coveredColumnIndex) != 0) {
                        anyRealCoveredAddr = true;
                    }
                }
                assertTrue("expected at least one multi-key covering frame", frames > 0);
                assertTrue("multi-key frames must carry REAL (eager) covered page addresses", anyRealCoveredAddr);
            }
            assertTrue("multi-key production must still eagerly materialize covered rows (counter > 0)",
                    CoveringIndexRecordCursorFactory.getCoveredRowsWrittenForTesting() > 0);
        });
    }

    @Test
    public void testRecordFastPathResidualFilterParallelMatchesReference() throws Exception {
        // Task 10 headline (SQL): the residual filter (sym = key AND price > x)
        // compiles to Async Filter -> CoveringIndex, whose row filter and random
        // access run through navigateTo(int, record). Projecting price forces the
        // covered column through the record path. With 4 workers, frames split
        // across workers, so this both parallelizes the residual and proves the
        // worker record arm is correct against a non-indexed twin.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (price)," +
                                        "  price DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  price DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 21600000L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 8))::symbol sym," +
                                        " (x % 997)::double price" +
                                        " FROM long_sequence(32000)";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        // Confirm the projection-with-residual runs Async Filter over
                        // the covering scan (the record fast-path), then equal the
                        // non-indexed twin -- both the projection (record path) and
                        // the aggregate (sum over the same residual).
                        sink.clear();
                        TestUtils.printSql(
                                compiler, sqlExecutionContext,
                                "EXPLAIN SELECT price FROM cov WHERE sym = 'S3' AND price > 500",
                                sink
                        );
                        final String plan = sink.toString();
                        assertTrue("residual projection must run Async Filter over CoveringIndex; plan=\n" + plan,
                                plan.contains("Async Filter") && plan.contains("CoveringIndex on: sym"));

                        for (String sym : new String[]{"S0", "S3", "S7"}) {
                            final String proj = "SELECT price FROM %s WHERE sym = '" + sym + "' AND price > 500 ORDER BY price";
                            final String agg = "SELECT sum(price), count() FROM %s WHERE sym = '" + sym + "' AND price > 500";
                            TestUtils.assertSqlCursors(
                                    compiler, sqlExecutionContext,
                                    String.format(proj, "ref"), String.format(proj, "cov"), LOG);
                            TestUtils.assertSqlCursors(
                                    compiler, sqlExecutionContext,
                                    String.format(agg, "ref"), String.format(agg, "cov"), LOG);
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testRecordFastPathShapesMatchReference() throws Exception {
        // Task 10: the record fast-path is reached by several shapes -- a residual
        // filter (filter -> record), a negative LIMIT (buffers the tail via the
        // record path), and a SelectedRecord projection over a covered column with
        // late-materialized random access (recordAt). Each, over a covered table,
        // must equal a plain non-indexed twin. A 200-row frame cap spreads matches
        // across many frames so a stale cross-frame record binding would surface.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price)," +
                                        "  name VARCHAR," +
                                        "  price DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  name VARCHAR," +
                                        "  price DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 43200000L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 4))::symbol sym," +
                                        " ('n' || (x % 100))::varchar name," +
                                        " (x % 997)::double price" +
                                        " FROM long_sequence(8000)";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        // 1) Residual filter -> record path, projecting covered cols.
                        final String resid = "SELECT name, price FROM %s WHERE sym = 'S1' AND price > 100 ORDER BY ts";
                        TestUtils.assertSqlCursors(
                                compiler, sqlExecutionContext,
                                String.format(resid, "ref"), String.format(resid, "cov"), LOG);

                        // 2) Negative LIMIT over the covered residual (buffers the
                        //    last N via the record path / backward scan).
                        final String neg = "SELECT name, price FROM %s WHERE sym = 'S2' AND price > 0 LIMIT -7";
                        TestUtils.assertSqlCursors(
                                compiler, sqlExecutionContext,
                                String.format(neg, "ref"), String.format(neg, "cov"), LOG);

                        // 3) SelectedRecord projection over a covered column with
                        //    random access re-read (recordAt) across frames.
                        final String sel = "SELECT price, name FROM %s WHERE sym = 'S3' AND price > 250 ORDER BY ts";
                        TestUtils.assertSqlCursors(
                                compiler, sqlExecutionContext,
                                String.format(sel, "ref"), String.format(sel, "cov"), LOG);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testMultiKeyCoveredAggregationMatchesReference() throws Exception {
        // Task 7 multi-key arm stays correct: a `sym IN (...)` covered aggregation
        // (which the worker covered arm SKIPS, relying on the eager merge buffers)
        // equals its non-indexed twin under parallel workers.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px)," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 20000L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 8))::symbol sym," +
                                        " (x % 997)::double px" +
                                        " FROM long_sequence(64000)";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        // sym IN (...) -> multi-key covered merge (eager), parallel.
                        final String agg = "SELECT sum(px), count() FROM %s WHERE sym IN ('S0', 'S3', 'S7')";
                        final String residual = "SELECT sum(px), count() FROM %s WHERE sym IN ('S0', 'S3', 'S7') AND px > 0.5";
                        TestUtils.assertSqlCursors(
                                compiler, sqlExecutionContext,
                                String.format(agg, "ref"), String.format(agg, "cov"), LOG);
                        TestUtils.assertSqlCursors(
                                compiler, sqlExecutionContext,
                                String.format(residual, "ref"), String.format(residual, "cov"), LOG);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testParallelKeyedGroupByOverProjectedCoveringIndexMatchesReference() throws Exception {
        // Locks the SelectedPageFrame covered-metadata propagation in the
        // parallel-decode suite: a parallel KEYED group-by whose key AND aggregate
        // are BOTH covered INCLUDE columns, reached through a SelectedRecord
        // projection over the covering page-frame path, must equal a non-indexed
        // twin at worker > 1.
        //
        // The projection (a SELECT-list that drops `ts`, reordering/narrowing the
        // covering scan's columns) inserts a SelectedPageFrame between the covering
        // frame and PageFrameAddressCache. Before the fix that wrapper reported the
        // PageFrame defaults (DIRECT / VALUE_NOT_FOUND) for the covered getters, so
        // the cache recorded the frame as NON-covered, the worker covered arm
        // no-op'd, and the covered key/value read placeholder zeroes -- collapsing
        // every group to a single empty key. With the per-column getColumnSource /
        // getCoveredIncludeIndex remap (and per-frame key/range/reader pass-through)
        // the covered columns survive the projection and decode on the workers.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 200);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE cov (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (grp, px)," +
                                        "  grp SYMBOL," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        engine.execute(
                                "CREATE TABLE ref (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL," +
                                        "  grp SYMBOL," +
                                        "  px DOUBLE" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        final String gen =
                                "SELECT" +
                                        " ('2024-01-01'::TIMESTAMP + (x - 1) * 20000L)::timestamp ts," +
                                        " ('S' || ((x - 1) % 8))::symbol sym," +
                                        " ('G' || ((x - 1) % 5))::symbol grp," +
                                        " (x % 997)::double px" +
                                        " FROM long_sequence(64000)";
                        engine.execute("INSERT INTO cov " + gen, sqlExecutionContext);
                        engine.execute("INSERT INTO ref " + gen, sqlExecutionContext);
                        engine.releaseAllWriters();

                        // Single-key WHERE -> metadata-only covered frames; the
                        // projection "grp, sum(px)" (drops ts and sym, so a
                        // SelectedRecord sits above the covering scan) keeps the
                        // covered key `grp` and covered value `px`. avg/sum over a
                        // GROUP BY a covered key routes through the Async keyed
                        // group-by, i.e. the SelectedPageFrame projection path.
                        final String groupBy =
                                "SELECT grp, sum(px), count() FROM %s WHERE sym = 'S3' ORDER BY grp";
                        TestUtils.assertSqlCursors(
                                compiler, sqlExecutionContext,
                                String.format(groupBy, "ref"), String.format(groupBy, "cov"), LOG);

                        // And a residual variant (covered key + covered value + a
                        // covered-value predicate) over the same projection path.
                        final String groupByResidual =
                                "SELECT grp, sum(px), count() FROM %s WHERE sym = 'S3' AND px > 0.5 ORDER BY grp";
                        TestUtils.assertSqlCursors(
                                compiler, sqlExecutionContext,
                                String.format(groupByResidual, "ref"), String.format(groupByResidual, "cov"), LOG);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testExtraNullColumnPageFramePropagatesCoveredSourceForBaseColumn() throws Exception {
        // White-box lock for the ExtraNullColumnPageFrame covered-metadata fix.
        // Set-ops/window-join null padding wraps a base frame and appends synthetic
        // null columns above `columnSplit`. Before the fix this wrapper reported the
        // PageFrame default covered getters, so a covered base column (below the
        // split) lost its COVERED source through the wrapper and the worker covered
        // arm would not decode it. Here we wrap a real covering page-frame cursor in
        // an ExtraNullColumnCursorFactory with one extra (synthetic null) column and
        // assert: (a) the covered base column still reports COVERED with the same
        // include index, (b) the per-frame covered metadata (key/range/reader/set)
        // passes through, and (c) the synthetic column above the split reports
        // DIRECT / -1.
        final int coveredColumnIndex = 1;  // base query col 1 = covered INCLUDE (payload)
        final int symbolColumnIndex = 0;   // base query col 0 = indexed symbol key
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t_enc (
                        ts TIMESTAMP,
                        sym SYMBOL INDEX TYPE POSTING INCLUDE (payload),
                        payload LONG
                    ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                    """);
            execute("""
                    INSERT INTO t_enc
                    SELECT
                        '2024-01-01T00:00:00'::TIMESTAMP + x * 1_000_000L,
                        'A',
                        x
                    FROM long_sequence(10)
                    """);
            engine.releaseAllWriters();

            // The query's covering factory is what ExtraNullColumn must wrap. The
            // test harness wraps it in a QueryProgress (whose page-frame cursor is
            // NOT a TablePageFrameCursor and would fail ExtraNullColumn's cast), so
            // unwrap to the underlying factory -- exactly the precedent in
            // CoveringIndexTest#testPageFrameCursor_ScanProfile.
            //
            // OWNERSHIP: the QueryProgress (`outer`) is the SOLE owner of the
            // covering factory and is closed by this try-with-resources. We hand
            // the unwrapped factory to ExtraNullColumn only to build its page-frame
            // cursor; we deliberately do NOT close the ExtraNullColumn factory (its
            // _close() would re-close the covering factory). The ExtraNullColumn
            // factory allocates nothing native and registers nothing, and the
            // covering page-frame CURSOR it opens is closed once via the wrapper
            // cursor below -- so every resource is released exactly once.
            try (RecordCursorFactory outer = select("SELECT sym, payload FROM t_enc WHERE sym = 'A'")) {
                final RecordCursorFactory base = outer instanceof QueryProgress ? outer.getBaseFactory() : outer;
                final RecordMetadata baseMetadata = base.getMetadata();
                final int columnSplit = baseMetadata.getColumnCount(); // 2
                // Base metadata + one synthetic null column above the split.
                // copyOfNew (not copyOf) so we never mutate the base's own metadata.
                final GenericRecordMetadata metadata = GenericRecordMetadata.copyOfNew(baseMetadata);
                metadata.add(new TableColumnMetadata("pad", ColumnType.LONG));
                // 2 (the synthetic null column)

                final ExtraNullColumnCursorFactory enc = new ExtraNullColumnCursorFactory(metadata, columnSplit, base);
                try (PageFrameCursor cursor = enc.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    int frames = 0;
                    PageFrame f;
                    while ((f = cursor.next(0)) != null) {
                        frames++;
                        // (a) covered base column survives the null-pad wrapper.
                        assertEquals("covered base column reports COVERED through ExtraNullColumn",
                                DataSource.COVERED, f.getColumnSource(coveredColumnIndex));
                        assertTrue("covered base column keeps a real sidecar include index (>= 0) through ExtraNullColumn",
                                f.getCoveredIncludeIndex(coveredColumnIndex) >= 0);
                        // symbol key column below the split is COVERED (served by the covering
                        // frame); delegated 1:1 through the wrapper. This is what distinguishes it
                        // from the synthetic null column above the split (DIRECT), which the worker
                        // decode publishes as NULL instead of mis-binding it to the symbol-key buffer.
                        assertEquals("symbol-key base column reports COVERED through ExtraNullColumn",
                                DataSource.COVERED, f.getColumnSource(symbolColumnIndex));
                        // (b) per-frame covered metadata passes through.
                        assertNotNull("covered frame's posting reader passes through ExtraNullColumn",
                                f.getIndexReader(symbolColumnIndex, IndexReader.DIR_FORWARD));
                        assertTrue("covered row range passes through ExtraNullColumn",
                                f.getCoveredRowHi() > f.getCoveredRowLo());
                        assertNotNull("covered include-index set passes through ExtraNullColumn",
                                f.getCoveredIncludeIndices());
                        // (c) synthetic null column above the split is DIRECT / -1 / null.
                        assertEquals("synthetic null column above the split reports DIRECT",
                                DataSource.DIRECT, f.getColumnSource(columnSplit));
                        assertEquals("synthetic null column above the split has no include index",
                                -1, f.getCoveredIncludeIndex(columnSplit));
                        assertNull("synthetic null column above the split has no index reader",
                                f.getIndexReader(columnSplit, IndexReader.DIR_FORWARD));
                    }
                    assertTrue("expected at least one covering page frame through ExtraNullColumn", frames > 0);
                }
            }
        });
    }

    // Drive a covered query to its cursor and drain it; assert it is cancelled by
    // the (armed) circuit breaker. The cancellation surfaces as a CairoException with
    // isInterruption()/isCancellation() set (here the "cancelled by user" variant; we
    // accept the timeout "query aborted" variant too so the helper is robust to which
    // breaker check fires first).
    private static void assertCoveredQueryCancels(
            SqlCompiler compiler,
            SqlExecutionContext context,
            String query
    ) throws Exception {
        try (
                RecordCursorFactory factory = compiler.compile(query, context).getRecordCursorFactory();
                io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(context)
        ) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
                // drain until the breaker trips
            }
            fail("expected the covered query to be cancelled mid-flight: " + query);
        } catch (CairoException e) {
            assertTrue(
                    "covered query must fail as an interruption/cancellation, got: " + e.getFlyweightMessage(),
                    e.isInterruption() || e.isCancellation()
            );
        }
    }

    private static long countRows(SqlCompiler compiler, SqlExecutionContext context, String countQuery) throws Exception {
        try (
                RecordCursorFactory factory = compiler.compile(countQuery, context).getRecordCursorFactory();
                io.questdb.cairo.sql.RecordCursor cursor = factory.getCursor(context)
        ) {
            assertTrue(cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    /**
     * A circuit breaker that can be ARMED and DISARMED between queries while reusing
     * the same instance, used to inject a deterministic mid-query cancellation.
     * <p>
     * It extends {@link io.questdb.cairo.sql.AtomicBooleanCircuitBreaker} for two
     * reasons: (1) that breaker is THREAD-SAFE, so the async pipeline's per-worker
     * SqlExecutionCircuitBreakerWrapper.init() delegates straight to THIS instance
     * (rather than copying only the timeout/cancelledFlag into a private timing
     * breaker, as it does for the non-thread-safe network breaker) -- so every
     * {@code getState}/{@code checkIfTripped}/{@code statefulThrow...} the reduce and
     * vect group-by paths consult is answered HERE; and (2) it holds no native
     * resources, so nothing leaks.
     * <p>
     * Arming is tracked by a private {@code armed} flag that is INDEPENDENT of the
     * inherited {@code cancelledFlag}. This matters because QueryRegistry.register()
     * installs its OWN fresh (false) {@code AtomicBoolean} into the context breaker
     * via {@code setCancelledFlag} at query start (the hook {@code CANCEL QUERY} /
     * {@code cancel_query()} flip to abort a query) and clears it on unregister --
     * which would otherwise wipe an arm expressed through {@code cancelledFlag}. By
     * answering the trip surface from {@code armed} directly, the registry's flag
     * churn is irrelevant:
     * <ul>
     *   <li>ARMED: the in-flight covered query is cancelled -- the reduce path's
     *       {@code getState} returns STATE_CANCELLED -> frameSequence.cancel, and the
     *       vect/collect paths throw {@code queryCancelled()} ("cancelled by user").</li>
     *   <li>DISARMED: every check reports OK, so queries run to completion.</li>
     * </ul>
     */
    private static final class ArmableTimeoutCircuitBreaker extends io.questdb.cairo.sql.AtomicBooleanCircuitBreaker {
        private volatile boolean armed;

        private ArmableTimeoutCircuitBreaker(io.questdb.cairo.CairoEngine engine) {
            super(engine);
        }

        void arm() {
            armed = true;
        }

        @Override
        public boolean checkIfTripped() {
            return armed || super.checkIfTripped();
        }

        @Override
        public boolean checkIfTripped(long millis, long fd) {
            return armed || super.checkIfTripped(millis, fd);
        }

        void disarm() {
            armed = false;
        }

        @Override
        public int getState() {
            return armed ? STATE_CANCELLED : super.getState();
        }

        @Override
        public int getState(long millis, long fd) {
            return armed ? STATE_CANCELLED : super.getState(millis, fd);
        }

        @Override
        public void statefulThrowExceptionIfTripped() {
            if (armed) {
                throw CairoException.queryCancelled(getFd());
            }
            super.statefulThrowExceptionIfTripped();
        }

        @Override
        public void statefulThrowExceptionIfTrippedNoThrottle() {
            if (armed) {
                throw CairoException.queryCancelled(getFd());
            }
            super.statefulThrowExceptionIfTrippedNoThrottle();
        }
    }

    // ===================== Task 14 perf-harness helpers =========================

    /**
     * A self-contained engine + worker pool + execution context bound to a single
     * worker count, over the supplied (shared, on-disk) configuration. Mirrors the
     * wiring {@link TestUtils#execute} does internally, but exposed so the perf
     * harness can stand up THREE such nodes (8 / 1 / 8 workers) over the SAME db
     * root and free each one between configs. The fresh engine reloads the
     * persisted BYPASS WAL tables on construction.
     */
    private static final class CleanupFailingAtom implements StatefulAtom {
        private final RuntimeException clearFailure;
        private final RuntimeException closeFailure;
        private int clearCount;
        private int closeCount;

        private CleanupFailingAtom(RuntimeException clearFailure, RuntimeException closeFailure) {
            this.clearFailure = clearFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        public void clear() {
            clearCount++;
            throw clearFailure;
        }

        @Override
        public void close() {
            closeCount++;
            throw closeFailure;
        }
    }

    private static final class CleanupTrackingAddressCache extends PageFrameAddressCache {
        private final RuntimeException closeFailure;
        private int closeCount;
        private int unfreezeCount;
        private boolean wereReadersUnfrozenOnClose;

        private CleanupTrackingAddressCache(RuntimeException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            wereReadersUnfrozenOnClose = true;
            for (int i = 0; i < getFrameCount(); i++) {
                final IndexReader reader = getCoveredIndexReader(i);
                if (reader != null && reader.isFrozen()) {
                    wereReadersUnfrozenOnClose = false;
                }
            }
            super.close();
            throw closeFailure;
        }

        @Override
        public void unfreezeCoveredReaders() {
            unfreezeCount++;
            super.unfreezeCoveredReaders();
        }
    }

    private static final class PerfNode implements AutoCloseable {
        final SqlCompiler compiler;
        final SqlExecutionContextImpl ctx;
        final CairoEngine engine;
        final WorkerPool pool;

        PerfNode(CairoConfiguration configuration, int workerCount) {
            this.pool = new WorkerPool(() -> workerCount);
            this.engine = new CairoEngine(configuration);
            boolean ok = false;
            try {
                this.compiler = engine.getSqlCompiler();
                this.ctx = TestUtils.createSqlExecutionCtx(engine, workerCount);
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
            } catch (Throwable th) {
                if (!ok) {
                    pool.halt();
                    engine.close();
                }
                throw new RuntimeException(th);
            }
        }

        @Override
        public void close() {
            pool.halt();
            Misc.free(compiler);
            engine.releaseInactive();
            engine.close();
        }
    }

    private static void appendPerf(StringBuilder sb, String label, long[] a, long[] b, long[] c, int warmup, int ignoredIters) {
        sb.append(String.format("  %s  (ns wall-clock per query)%n", label));
        appendRow(sb, "A) parallel cov (8w)", a, warmup);
        appendRow(sb, "B) serial   cov (1w)", b, warmup);
        appendRow(sb, "C) parallel ref (8w)", c, warmup);
        final double aMed = medianTail(a, warmup), bMed = medianTail(b, warmup), cMed = medianTail(c, warmup);
        final double aMean = meanTail(a, warmup), bMean = meanTail(b, warmup), cMean = meanTail(c, warmup);
        sb.append(String.format("      median(measured) A=%,.0f  B=%,.0f  C=%,.0f  ns   (mean A=%,.0f B=%,.0f C=%,.0f)%n",
                aMed, bMed, cMed, aMean, bMean, cMean));
        sb.append(String.format("      [median] B/A (parallelism speedup) = %.2fx    A/C (covered-vs-scan parity) = %.2fx%n",
                bMed / aMed, aMed / cMed));
        // B/C is the SERIAL covered-vs-scan ratio -- i.e. the pre-parallelism gap
        // this branch attacks; A/C is what remains after parallelizing the decode.
        sb.append(String.format("      [median] B/C (serial covered vs scan, pre-parallelism gap) = %.2fx%n", bMed / cMed));
        sb.append("  --------------------------------------------------------------------------------\n");
    }

    private static void appendRow(StringBuilder sb, String name, long[] ns, int warmup) {
        sb.append(String.format("      %-22s raw=[", name));
        for (int i = 0; i < ns.length; i++) {
            if (i == warmup && warmup > 0) {
                sb.append("| "); // separator: warmup (discarded) | measured
            }
            sb.append(String.format("%,d", ns[i]));
            if (i < ns.length - 1) sb.append(", ");
        }
        final double med = medianTail(ns, warmup);
        sb.append(String.format("]  median=%,.0f ns (%.1f ms)%n", med, med / 1e6));
    }

    /**
     * Builds the cov (covering-indexed) and ref (non-indexed twin) tables and
     * inserts the same generated rows into each, then releases writers so the
     * BYPASS WAL data is flushed to disk for the timed configs to reopen.
     */
    private void buildPerfData(String gen) throws Exception {
        try (PerfNode node = new PerfNode(configuration, 8)) {
            node.engine.execute(
                    "CREATE TABLE cov (" +
                            "  ts TIMESTAMP," +
                            "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px)," +
                            "  px DOUBLE" +
                            ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                    node.ctx
            );
            node.engine.execute(
                    "CREATE TABLE ref (" +
                            "  ts TIMESTAMP," +
                            "  sym SYMBOL," +
                            "  px DOUBLE" +
                            ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                    node.ctx
            );
            final long t0 = System.nanoTime();
            node.engine.execute("INSERT INTO cov " + gen, node.ctx);
            node.engine.execute("INSERT INTO ref " + gen, node.ctx);
            node.engine.releaseAllWriters();
            LOG.advisory().$("perf data built in ").$((System.nanoTime() - t0) / 1_000_000).$("ms").$();
        }
    }

    /**
     * EXPLAINs the covered query and checks the plan runs the async (parallel)
     * group-by over the covering index. Returns true if so; otherwise prints the
     * plan loudly (and fails) — a serial fallback would invalidate the measurement.
     */
    private boolean assertOrReportAsyncCovered(PerfNode node, String name, String coveredQuery) throws Exception {
        final StringSink plan = new StringSink();
        TestUtils.printSql(node.compiler, node.ctx, "EXPLAIN " + coveredQuery, plan);
        final String p = plan.toString();
        // covered_agg / residual are NOT-keyed sum() -> AsyncGroupByNotKeyed
        // ("Async Group By" / "Async JIT Group By"), and the scan source must be
        // the covering index ("CoveringIndex on: sym").
        final boolean async = (p.contains("Async Group By") || p.contains("Async JIT Group By")) && p.contains("CoveringIndex on: sym");
        if (!async) {
            final String msg = "\n!!! ROUTING FINDING: covered query '" + name + "' did NOT route through the async covered " +
                    "group-by over the covering index -- the parallel covered decode is NOT being measured for this shape.\n" +
                    "    query: " + coveredQuery + "\n    plan:\n" + p + "\n";
            LOG.error().$safe(msg).$();
            fail(msg);
        } else {
            // Log the full plan so the vectorized-vs-reduce covered path is on
            // the record (covered_agg -> vectorized; residual -> non-vectorized).
            final String okMsg = "routing OK [" + name + "]: async covered group-by over covering index\n    plan:\n" + p;
            LOG.advisory().$safe(okMsg).$();
        }
        return async;
    }

    /**
     * Times one config: a fresh engine/pool at {@code workerCount} runs the query
     * {@code warmup + iters} times (cursor fully drained, a column summed to defeat
     * dead-code elimination), returning the per-iteration wall-clock in ns
     * (warmup iters included at the front; callers slice with {@code warmup}).
     */
    private long[] timeConfig(String shape, String cfg, String query, int workerCount, int warmup, int iters) throws Exception {
        final long[] timings = new long[warmup + iters];
        long guard = 0;
        try (PerfNode node = new PerfNode(configuration, workerCount)) {
            for (int i = 0; i < warmup + iters; i++) {
                final long t0 = System.nanoTime();
                guard += drainSum(node, query);
                timings[i] = System.nanoTime() - t0;
            }
        }
        LOG.advisory().$("timed [").$(shape).$(' ').$(cfg).$("] guard=").$(guard).$();
        return timings;
    }

    @SuppressWarnings("unchecked")
    private static <T> T getField(Class<?> clazz, Object instance, String name) throws NoSuchFieldException {
        final Field field = clazz.getDeclaredField(name);
        return (T) Unsafe.getObject(instance, Unsafe.objectFieldOffset(field));
    }

    private static long drainSum(PerfNode node, String query) throws Exception {
        long sum = 0;
        try (RecordCursorFactory factory = node.compiler.compile(query, node.ctx).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(node.ctx)) {
                final RecordMetadata m = factory.getMetadata();
                final int n = m.getColumnCount();
                final Record rec = cursor.getRecord();
                while (cursor.hasNext()) {
                    for (int c = 0; c < n; c++) {
                        sum += (long) rec.getDouble(c);
                    }
                }
            }
        }
        return sum;
    }

    // Mean of the MEASURED iterations only (the leading `skip` warmup samples are
    // discarded to let the JVM/page-cache settle before timing).
    private static double meanTail(long[] all, int skip) {
        double s = 0;
        int cnt = 0;
        for (int i = skip; i < all.length; i++) {
            s += all[i];
            cnt++;
        }
        return cnt == 0 ? 0 : s / cnt;
    }

    // Median of the MEASURED iterations (skip leading warmup). The median is the
    // headline statistic here because a single cold page-fault / GC pause on the
    // full-scan `ref` config produces a huge outlier that wrecks the mean but not
    // the median -- the raw rows are printed so that noise stays visible.
    private static double medianTail(long[] all, int skip) {
        final int n = all.length - skip;
        if (n <= 0) {
            return 0;
        }
        final long[] m = new long[n];
        System.arraycopy(all, skip, m, 0, n);
        java.util.Arrays.sort(m);
        return (n & 1) == 1 ? m[n / 2] : (m[n / 2 - 1] + m[n / 2]) / 2.0;
    }

    private static void setField(Class<?> clazz, Object instance, String name, Object value) throws NoSuchFieldException {
        final Field field = clazz.getDeclaredField(name);
        Unsafe.putObject(instance, Unsafe.objectFieldOffset(field), value);
    }

    private static String verdict(String label, long[] a, long[] b, long[] c, int warmup) {
        // Median-based: robust to the cold-cache / GC outliers that the full-scan
        // `ref` config (C) exhibits when its column files are remapped per engine.
        final double bm = medianTail(b, warmup), am = medianTail(a, warmup), cm = medianTail(c, warmup);
        final double speedup = bm / am;
        final double parity = am / cm;
        final boolean parallelizes = speedup >= 1.5; // B/A >> 1
        final boolean reachesParity = parity <= 1.5;  // A/C ~ 1 (within 1.5x of scan)
        return String.format("  VERDICT [%s]: parallelizes=%s (B/A=%.2fx)  scan-parity=%s (A/C=%.2fx)  [median]%n",
                label, parallelizes, speedup, reachesParity, parity);
    }

    // Recompute the verdict() booleans and ASSERT them, so the gated perf run fails on a
    // parallelism / scan-parity regression instead of only printing a verdict.
    private static void assertParallelVerdict(String label, long[] a, long[] b, long[] c, int warmup, boolean requireParity) {
        final double am = medianTail(a, warmup), bm = medianTail(b, warmup), cm = medianTail(c, warmup);
        final double speedup = bm / am;
        final double parity = am / cm;
        assertTrue(label + " must parallelize: B/A=" + String.format("%.2f", speedup) + " (>= 1.5)", speedup >= 1.5);
        if (requireParity) {
            assertTrue(label + " must reach scan parity: A/C=" + String.format("%.2f", parity) + " (<= 1.5)", parity <= 1.5);
        }
    }

    private static void assertCovParallelEqualsSingle(
            SqlCompiler compiler,
            SqlExecutionContext parallelCtx,
            SqlExecutionContext singleCtx,
            String coveredQuery
    ) throws Exception {
        final StringSink parallel = new StringSink();
        final StringSink single = new StringSink();
        TestUtils.printSql(compiler, parallelCtx, coveredQuery, parallel);
        TestUtils.printSql(compiler, singleCtx, coveredQuery, single);
        TestUtils.assertEquals(
                "covered parallel result must match single-threaded: " + coveredQuery,
                single.toString(),
                parallel.toString()
        );
    }
}
