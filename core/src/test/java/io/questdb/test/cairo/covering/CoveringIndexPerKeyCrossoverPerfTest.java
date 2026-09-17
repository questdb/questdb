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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.LogCapture;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Measures WHERE the per-key density crossover actually sits, and what the k=4 win does as the
 * partition count grows. Both are gated perf runs -- enable with {@code -Dcovering.crossover.perf=true}.
 * <p>
 * <b>Why the mode is driven and not inferred.</b> {@code EXPLAIN} prints the plan-stable
 * PERMISSION, not the mode an execution chose, so every arm here is forced through
 * {@link CoveringIndexRecordCursorFactory#setMinRowsPerKeyPartitionForTesting(int)} -- 1 admits
 * any non-empty density, {@link Integer#MAX_VALUE} admits none -- and then CONFIRMED twice:
 * once against the per-execution {@code covering scan frame mode [...]} log record (which also
 * pins the estimated {@code rowsPerPair}, so a fixture that did not build the density it claims
 * fails loudly), and once per timed block against the {@code @TestOnly} mode counters, so a
 * single iteration taking the other mode cannot hide inside a best-of-N.
 * <p>
 * Driving the gate rather than the data means both arms read the SAME bytes in the SAME engine,
 * on one build. No cross-build comparison, so no build-to-build drift to argue about.
 */
public class CoveringIndexPerKeyCrossoverPerfTest extends AbstractCoveringIndexQueryTest {

    // Override value that makes the density gate reject every shape.
    private static final int FORCE_MERGED = Integer.MAX_VALUE;
    // Override value that makes the density gate admit every non-empty shape. NOT 0: 0 takes the
    // "density-gate-off" branch, which skips the estimate and so logs rowsPerPair=-1, costing us
    // the independent check that the fixture really holds the density it claims.
    private static final int FORCE_PER_KEY = 1;
    private static final Log LOG = LogFactory.getLog(CoveringIndexPerKeyCrossoverPerfTest.class);
    private static final String TAG = "#XOVER#";

    /**
     * Result lines go to {@code -Dcovering.crossover.out} when set, because the engine's own log
     * writes to the same console and has already torn one result line in half mid-run.
     */
    private static void emit(String line) {
        final String out = System.getProperty("covering.crossover.out");
        if (out != null) {
            try (java.io.PrintWriter w = new java.io.PrintWriter(new java.io.FileWriter(out, true))) {
                w.println(line);
            } catch (java.io.IOException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println(line);
        System.out.flush();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(-1);
        CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
        super.tearDown();
    }

    @Test
    public void testDensityCrossoverSweep() throws Exception {
        if (!"true".equals(System.getProperty("covering.crossover.perf"))) {
            LOG.info().$("skipping testDensityCrossoverSweep (enable with -Dcovering.crossover.perf=true)").$();
            return;
        }
        final int workers = Integer.getInteger("covering.crossover.workers", 8);
        final int warmup = Integer.getInteger("covering.crossover.warmup", 3);
        final int iters = Integer.getInteger("covering.crossover.iters", 9);

        // Three (keys, partitions) pairs holding the PAIR count constant, so the per-key frame
        // count is the same in all three and only the merged frame count (== partitions) moves.
        // The constant is applied uniformly across key and partition counts, so it has to be
        // checked on more than one of each.
        final int[][] configs = parsePairs(System.getProperty("covering.crossover.configs", "4x512,16x128,64x32"));
        final String[] rowsPerPair = System.getProperty("covering.crossover.rpp", "8,16,32,64,128,256,512,1024").split(",");

        emit(TAG + " sweep=density workers=" + workers + " warmup=" + warmup + " iters=" + iters);
        for (int[] cfg : configs) {
            for (String r : rowsPerPair) {
                measureShape(cfg[0], cfg[1], Integer.parseInt(r.trim()), workers, warmup, iters);
            }
        }
    }

    @Test
    public void testKeyFourPartitionSweep() throws Exception {
        if (!"true".equals(System.getProperty("covering.crossover.perf"))) {
            LOG.info().$("skipping testKeyFourPartitionSweep (enable with -Dcovering.crossover.perf=true)").$();
            return;
        }
        final int workers = Integer.getInteger("covering.crossover.workers", 8);
        final int warmup = Integer.getInteger("covering.crossover.warmup", 3);
        final int iters = Integer.getInteger("covering.crossover.iters", 9);

        // k=4 at ~400,000 rows per key, with ONLY the partition count varying. Per-key emits
        // roughly one frame per (key, partition) pair, so this is the axis the headline 3.40x --
        // measured on a single partition -- never varied.
        final int[][] shapes = parsePairs(System.getProperty(
                "covering.crossover.shapes", "1x400000,4x100000,16x25000,64x6250,256x1562,1024x390"));

        emit(TAG + " sweep=partitions workers=" + workers + " warmup=" + warmup + " iters=" + iters);
        for (int[] s : shapes) {
            measureShape(4, s[0], s[1], workers, warmup, iters);
        }
    }

    private static long drain(RecordCursorFactory factory, SqlExecutionContextImpl ctx) throws Exception {
        long guard = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                // k, count(), sum(v), min(v), max(v) -- summed so nothing is dead code.
                guard += rec.getLong(1) + (long) rec.getDouble(2) + (long) rec.getDouble(3) + (long) rec.getDouble(4);
            }
        }
        return guard;
    }

    private static long median(long[] v) {
        final long[] m = v.clone();
        java.util.Arrays.sort(m);
        final int n = m.length;
        return (n & 1) == 1 ? m[n / 2] : (m[n / 2 - 1] + m[n / 2]) / 2;
    }

    private static long min(long[] v) {
        long best = Long.MAX_VALUE;
        for (long x : v) {
            best = Math.min(best, x);
        }
        return best;
    }

    /**
     * Runs the query ONCE with the gate forced, and asserts the per-execution log record names the
     * mode we forced, with the rowsPerPair the fixture was built to hold and the crossover we set.
     * This is the check that the arm is what it says it is; it is deliberately separate from the
     * timed blocks so log capture never sits inside a timing loop.
     */
    private void confirmModeLog(PerfNode node, String query, int override, String mode, int keys, int rowsPerPair) throws Exception {
        CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(override);
        final LogCapture capture = new LogCapture();
        capture.start();
        try {
            TestUtils.printSql(node.compiler, node.ctx, query, new StringSink());
            capture.drain();
            capture.assertLoggedRE(
                    "covering scan frame mode \\[table=t, mode=" + mode + ", reason=density, keys=" + keys
                            + ", partitionsUpper=\\d+, framesUpper=\\d+, frameCeiling=\\d+, rowsPerPair="
                            + rowsPerPair + ", crossover=" + override + "]"
            );
            capture.assertNotLogged("mode=" + ("per-key".equals(mode) ? "merged" : "per-key"));
        } finally {
            capture.stop();
        }
    }

    private void measureShape(int keys, int partitions, int rowsPerPair, int workers, int warmup, int iters) throws Exception {
        final long rows = (long) keys * partitions * rowsPerPair;
        final String in = keyInList(keys);
        final String query = "SELECT k, count() c, sum(v) s, min(v) mn, max(v) mx FROM t WHERE k IN (" + in + ") ORDER BY k";

        buildTable(keys, partitions, rowsPerPair);

        final StringSink plan = new StringSink();
        final StringSink perKeyDigest = new StringSink();
        final StringSink mergedDigest = new StringSink();
        final long[] a1, b, a2;
        try (PerfNode node = new PerfNode(configuration, workers)) {
            // The override is a global static. Every exit from here restores it (see the finally
            // below and tearDown), or a failure mid-sweep would leave the gate off for whatever
            // class the fork runs next.
            // 1. EXPLAIN first, and record the operator.
            CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(FORCE_PER_KEY);
            TestUtils.printSql(node.compiler, node.ctx, "EXPLAIN " + query, plan);
            final String p = plan.toString();
            assertTrue("scan must be the covering index, plan was:\n" + p, p.contains("CoveringIndex on: k"));

            // 2. Confirm each arm from the per-execution mode log, not from the plan.
            confirmModeLog(node, query, FORCE_PER_KEY, "per-key", keys, rowsPerPair);
            confirmModeLog(node, query, FORCE_MERGED, "merged", keys, rowsPerPair);

            // 3. Result digests must match, so a fast arm cannot be a fast EMPTY arm.
            CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(FORCE_PER_KEY);
            TestUtils.printSql(node.compiler, node.ctx, query, perKeyDigest);
            CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(FORCE_MERGED);
            TestUtils.printSql(node.compiler, node.ctx, query, mergedDigest);
            TestUtils.assertEquals("per-key and merged results must match", perKeyDigest, mergedDigest);

            // 4. Time. One compiled factory reused across every iteration, so no compilation sits
            //    in the measurement; the mode is decided per getCursor(), so flipping the override
            //    between blocks still moves the arm.
            try (RecordCursorFactory factory = node.compiler.compile(query, node.ctx).getRecordCursorFactory()) {
                for (int i = 0; i < warmup; i++) {
                    timeBlock(node, factory, FORCE_PER_KEY, 1, true);
                    timeBlock(node, factory, FORCE_MERGED, 1, false);
                }
                a1 = timeBlock(node, factory, FORCE_PER_KEY, iters, true);
                b = timeBlock(node, factory, FORCE_MERGED, iters, false);
                // Bookend: the first arm again, at the end.
                a2 = timeBlock(node, factory, FORCE_PER_KEY, iters, true);
            }
        } finally {
            CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(-1);
        }
        dropTable();

        final long perKeyBest = Math.min(min(a1), min(a2));
        final long mergedBest = min(b);
        final double drift = 100.0 * Math.abs(min(a1) - min(a2)) / Math.min(min(a1), min(a2));
        emit(String.format(
                "%s shape keys=%d partitions=%d rowsPerPair=%d rows=%d | perKey1_min=%.3fms perKey1_med=%.3fms "
                        + "merged_min=%.3fms merged_med=%.3fms perKey2_min=%.3fms perKey2_med=%.3fms "
                        + "| drift=%.1f%% ratio=%.3f resultRows=%d",
                TAG, keys, partitions, rowsPerPair, rows,
                min(a1) / 1e6, median(a1) / 1e6,
                mergedBest / 1e6, median(b) / 1e6,
                min(a2) / 1e6, median(a2) / 1e6,
                drift, (double) mergedBest / perKeyBest, countLines(perKeyDigest)
        ));
        emit(TAG + " plan keys=" + keys + " partitions=" + partitions + " rowsPerPair=" + rowsPerPair
                + " loadavg=" + loadAvg() + " | " + planLine(plan.toString()));
    }

    private static int countLines(StringSink s) {
        int n = 0;
        final String v = s.toString();
        for (int i = 0; i < v.length(); i++) {
            if (v.charAt(i) == '\n') {
                n++;
            }
        }
        return n - 1; // header row
    }

    /**
     * The host's 1/5/15-minute load average, recorded beside every shape: several other agents
     * build on this box concurrently, so a timing without the load it was taken under is not
     * reproducible.
     */
    private static String loadAvg() {
        try {
            return new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get("/proc/loadavg"))).trim();
        } catch (Exception e) {
            return "n/a";
        }
    }

    private static String keyInList(int keys) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('\'').append('k').append(i).append('\'');
        }
        return sb.toString();
    }

    private static String planLine(String plan) {
        final StringBuilder sb = new StringBuilder();
        for (String line : plan.split("\n")) {
            final String t = line.trim();
            if (t.isEmpty() || "QUERY PLAN".equals(t)) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(" / ");
            }
            sb.append(t);
        }
        return sb.toString();
    }

    private static int[][] parsePairs(String spec) {
        final String[] parts = spec.split(",");
        final int[][] out = new int[parts.length][2];
        for (int i = 0; i < parts.length; i++) {
            final String[] ab = parts[i].trim().split("x");
            out[i][0] = Integer.parseInt(ab[0].trim());
            out[i][1] = Integer.parseInt(ab[1].trim());
        }
        return out;
    }

    /**
     * Uniform fixture: {@code partitions} daily partitions, {@code keys} symbols round-robined
     * inside each partition, so EVERY (key, partition) pair holds exactly {@code rowsPerPair} rows
     * and the estimate's answer is exact. {@code v} is the row ordinal as a DOUBLE, so
     * {@code sum(v)} is an exact integer under 2^53 and therefore identical whatever order the
     * rows arrive in -- a float-noise digest mismatch would otherwise be unavoidable between
     * arms that aggregate in different orders.
     */
    private void buildTable(int keys, int partitions, int rowsPerPair) throws Exception {
        final long rowsPerPartition = (long) keys * rowsPerPair;
        final long step = 86_400_000_000L / rowsPerPartition;
        final long rows = rowsPerPartition * partitions;
        try (PerfNode node = new PerfNode(configuration, 8)) {
            node.engine.execute("DROP TABLE IF EXISTS t", node.ctx);
            node.engine.execute(
                    "CREATE TABLE t (" +
                            "  ts TIMESTAMP," +
                            "  k SYMBOL CAPACITY 4096 INDEX TYPE POSTING INCLUDE (ts, v)," +
                            "  v DOUBLE" +
                            ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                    node.ctx
            );
            final long t0 = System.nanoTime();
            node.engine.execute(
                    "INSERT INTO t SELECT" +
                            " ((((x - 1) / " + rowsPerPartition + ") * 86400000000L)" +
                            "   + (((x - 1) % " + rowsPerPartition + ") * " + step + "L))::timestamp," +
                            " concat('k', (x - 1) % " + keys + ")::symbol," +
                            " x::double" +
                            " FROM long_sequence(" + rows + ")",
                    node.ctx
            );
            node.engine.releaseAllWriters();
            LOG.advisory().$("built keys=").$(keys).$(" partitions=").$(partitions)
                    .$(" rowsPerPair=").$(rowsPerPair).$(" rows=").$(rows)
                    .$(" in ").$((System.nanoTime() - t0) / 1_000_000).$("ms").$();
        }
    }

    private void dropTable() throws Exception {
        try (PerfNode node = new PerfNode(configuration, 1)) {
            node.engine.execute("DROP TABLE IF EXISTS t", node.ctx);
        }
    }

    /**
     * Times {@code iters} drains with the gate forced, then ASSERTS from the mode counters that
     * every open in the block took the mode we asked for. Best-of-N over a block that silently
     * contained one open of the other mode would report a number belonging to neither arm.
     */
    private long[] timeBlock(PerfNode node, RecordCursorFactory factory, int override, int iters, boolean expectPerKey) throws Exception {
        CoveringIndexRecordCursorFactory.setMinRowsPerKeyPartitionForTesting(override);
        CoveringIndexRecordCursorFactory.resetModeSelectionsForTesting();
        final long[] timings = new long[iters];
        long guard = 0;
        for (int i = 0; i < iters; i++) {
            final long t0 = System.nanoTime();
            guard += drain(factory, node.ctx);
            timings[i] = System.nanoTime() - t0;
        }
        final long perKeyOpens = CoveringIndexRecordCursorFactory.getPerKeyModeOpensForTesting();
        final long mergedOpens = CoveringIndexRecordCursorFactory.getMergedModeOpensForTesting();
        if (expectPerKey) {
            assertTrue("block must have taken per-key mode at least once", perKeyOpens > 0);
            assertEquals("block must not contain a merged open", 0, mergedOpens);
        } else {
            assertTrue("block must have taken merged mode at least once", mergedOpens > 0);
            assertEquals("block must not contain a per-key open", 0, perKeyOpens);
        }
        LOG.advisory().$("timed block override=").$(override).$(" guard=").$(guard).$();
        return timings;
    }

    /**
     * Engine + worker pool + execution context over the shared on-disk configuration, so both
     * arms run against the same db root. Mirrors the PerfNode in
     * {@code CoveringIndexParallelDecodeTest}.
     */
    private static final class PerfNode implements AutoCloseable {
        final SqlCompiler compiler;
        final SqlExecutionContextImpl ctx;
        final CairoEngine engine;
        final WorkerPool pool;

        PerfNode(CairoConfiguration configuration, int workerCount) {
            this.pool = new TestWorkerPool(workerCount, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            this.engine = new CairoEngine(configuration);
            boolean ok = false;
            try {
                this.compiler = engine.getSqlCompiler();
                this.ctx = TestUtils.createSqlExecutionCtx(engine, workerCount);
                TestUtils.setupWorkerPool(pool, engine);
                pool.start(LOG);
                ok = true;
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
}
