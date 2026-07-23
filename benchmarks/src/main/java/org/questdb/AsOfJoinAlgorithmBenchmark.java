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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Compares keyed ASOF JOIN algorithms across the data distributions we measured, contrasting the NEW
 * defaults/fallbacks against the ORIGINAL plan and explicit hints:
 * <ul>
 *   <li><b>default</b> - no hint: the new default (Dense / DenseSingleSymbol forward scan).</li>
 *   <li><b>adaptive</b> - new default + {@code cairo.sql.asof.adaptive.backscan.budget} enabled (Fast
 *       prelude that switches to Dense once cumulative back-scan exceeds the budget).</li>
 *   <li><b>fast</b> - {@code /*+ asof_fast * /}: the ORIGINAL default (per-master key back-scan).</li>
 *   <li><b>dense / linear / memoized</b> - the explicit hints.</li>
 * </ul>
 * Distributions: dense timestamps (cliff shape), unique timestamps (realistic), dense single symbol, and
 * a small time-filtered left against a large right (the financial / illiquid single-symbol pattern).
 * <p>
 * Run (from the benchmarks jar): {@code java -jar benchmarks.jar AsOfJoinAlgorithmBenchmark}. Tunables:
 * {@code -Dasof.bench.rows}, {@code -Dasof.bench.right.rows}, {@code -Dasof.bench.keys},
 * {@code -Dasof.bench.dense}, {@code -Dasof.bench.adaptive.budget}. Cliff combos (fast/memoized on dense
 * shapes) are intentionally slow - that is the point.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(0)
public class AsOfJoinAlgorithmBenchmark {

    private static final int DENSE = Integer.getInteger("asof.bench.dense", 10_000); // slave rows per timestamp
    private static final int KEYS = Integer.getInteger("asof.bench.keys", 10_000);
    // idx_sweep symbol cardinality: high = illiquid (few rows/symbol), low = liquid (many rows/symbol).
    private static final int IDX_CARD = Integer.getInteger("asof.bench.idx.card", 100_000);
    // idx_sweep master (left) row count: sweep to find the asof_index vs Dense master/slave-ratio crossover.
    private static final long MASTER_ROWS = Long.getLong("asof.bench.master.rows", 1000L);
    private static final long RIGHT_ROWS = Long.getLong("asof.bench.right.rows", 2_000_000L);
    private static final String ROOT = System.getProperty("java.io.tmpdir") + java.io.File.separator + "asof-adaptive-bench";
    private static final long ROWS = Long.getLong("asof.bench.rows", 300_000L);
    // memoized dense-timestamp fallback threshold (-1 or huge = effectively disabled).
    private static final int MEMOIZED_K = Integer.getInteger("asof.bench.memoized.k", Integer.MAX_VALUE);
    // Read by the config below at factory-construction time; setUp() flips it per @Param.
    private static volatile long adaptiveBudget = -1;
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(ROOT) {
        @Override
        public long getSqlAsOfAdaptiveBackScanBudget() {
            return adaptiveBudget;
        }

        @Override
        public int getSqlAsOfMemoizedDenseRunThreshold() {
            return MEMOIZED_K;
        }
    };
    private static CairoEngine engine;
    private static SqlCompiler compiler;
    private static SqlExecutionContext ctx;
    private static WorkerPool pool;

    @Param({"dense_ts", "unique_ts", "dense_sym", "illiquid_sym", "multikey_sym", "nokey", "sparse_tail", "illiquid_idx", "idx_sweep", "noidx_sweep"})
    public String dist;

    @Param({"default", "adaptive", "fast", "dense", "linear", "memoized", "index"})
    public String algo;

    private RecordCursorFactory factory;

    public static void main(String[] args) throws Exception {
        ensureEngine();
        buildData();
        // -Dasof.bench.explain=true: print the selected plan (factory) for each shape's default (new) and
        // fast (old) query, then exit without timing. Proves which cursor auto-selection picked per shape.
        if (Boolean.getBoolean("asof.bench.explain")) {
            explainAll();
            LogFactory.haltInstance();
            System.out.flush();
            System.exit(0);
        }
        final Options opt = args.length > 0
                ? new org.openjdk.jmh.runner.options.CommandLineOptions(args)
                : new OptionsBuilder().include(AsOfJoinAlgorithmBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
        LogFactory.haltInstance();
        // The benchmark WorkerPool is non-daemon and is never stopped, so the JVM would otherwise hang
        // after "Run complete" and keep the table lock (blocking the next run). Exit explicitly.
        System.out.flush();
        System.exit(0);
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                bh.consume(rec.getLong(0));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        ensureEngine();
        adaptiveBudget = "adaptive".equals(algo) ? Long.getLong("asof.bench.adaptive.budget", 1_000_000L) : -1;
        // Recompile per trial so the factory picks up the current adaptive budget and the chosen hint.
        factory = compiler.compile(query(dist, algo), ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
    }

    private static void buildData() throws Exception {
        final long t0 = System.nanoTime();
        // dense_ts: LONG key, DENSE slave rows per timestamp (the cliff shape). left ts shifted +1 so a
        // predecessor match exists.
        createLong("l_dense");
        engine.execute("INSERT INTO l_dense SELECT (x-1)%" + KEYS + ", ((x-1)/" + DENSE + "+1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        createLong("r_dense");
        engine.execute("INSERT INTO r_dense SELECT (x-1)%" + KEYS + ", ((x-1)/" + DENSE + ")::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        // unique_ts: LONG key, unique timestamps (realistic).
        createLong("l_uniq");
        engine.execute("INSERT INTO l_uniq SELECT (x-1)%" + KEYS + ", ((x-1)+1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        createLong("r_uniq");
        engine.execute("INSERT INTO r_uniq SELECT (x-1)%" + KEYS + ", (x-1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        // dense_sym: single SYMBOL key, dense timestamps.
        createSym("l_dsym");
        engine.execute("INSERT INTO l_dsym SELECT ((x-1)%" + KEYS + ")::symbol, ((x-1)/" + DENSE + "+1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        createSym("r_dsym");
        engine.execute("INSERT INTO r_dsym SELECT ((x-1)%" + KEYS + ")::symbol, ((x-1)/" + DENSE + ")::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        // illiquid_sym: small time-filtered left (one illiquid symbol) against a large right with many
        // symbols. Right symbol cardinality high => each symbol is illiquid.
        engine.execute("DROP TABLE IF EXISTS md", ctx);
        engine.execute("CREATE TABLE md (sym SYMBOL, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO md SELECT ((x-1)%100000)::symbol, (x-1)::timestamp, x-1 FROM long_sequence(" + RIGHT_ROWS + ")", ctx);
        engine.execute("DROP TABLE IF EXISTS ord", ctx);
        engine.execute("CREATE TABLE ord (sym SYMBOL, ts TIMESTAMP, oid LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO ord SELECT '500', ((x-1)*" + (RIGHT_ROWS / 1000) + ")::timestamp, x-1 FROM long_sequence(1000)", ctx);
        // multikey_sym: TWO symbol keys (e.g. exchange+ticker), unique timestamps (realistic). Multi-key
        // ASOF is not single-symbol, so the new default is the general Dense cursor; the old default was
        // Fast. This is a do-no-harm shape: Dense must not regress a realistic multi-key workload.
        createMultiKey("l_mk");
        engine.execute("INSERT INTO l_mk SELECT ((x-1)%100)::symbol, (((x-1)/100)%100)::symbol, ((x-1)+1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        createMultiKey("r_mk");
        engine.execute("INSERT INTO r_mk SELECT ((x-1)%100)::symbol, (((x-1)/100)%100)::symbol, (x-1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        // nokey: no join key at all (single stream ASOF), unique timestamps. The Fast->Dense flip does not
        // touch the no-key path, so this is a control: new default must equal the old behaviour.
        createLong("l_nk");
        engine.execute("INSERT INTO l_nk SELECT 0, ((x-1)+1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        createLong("r_nk");
        engine.execute("INSERT INTO r_nk SELECT 0, (x-1)::timestamp, x-1 FROM long_sequence(" + ROWS + ")", ctx);
        // sparse_tail: huge right (RIGHT_ROWS, single key, unique ts), tiny left window at the tail.
        // Each left row has a cheap targeted predecessor (Fast wins); Dense must reach the window first.
        createLong("r_tail");
        engine.execute("INSERT INTO r_tail SELECT 0, (x-1)::timestamp, x-1 FROM long_sequence(" + RIGHT_ROWS + ")", ctx);
        createLong("l_tail");
        engine.execute("INSERT INTO l_tail SELECT 0, (" + RIGHT_ROWS + " - 200 + x)::timestamp, x-1 FROM long_sequence(200)", ctx);
        // illiquid_idx: huge right with an INDEXED, very high-cardinality symbol (1-in-100000 => ~20 rows
        // per symbol). Left is one illiquid symbol '500'. The index path can jump to sym rows; Dense/Fast
        // must scan/back-scan the interleaved bulk.
        engine.execute("DROP TABLE IF EXISTS md_idx", ctx);
        engine.execute("CREATE TABLE md_idx (sym SYMBOL INDEX, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO md_idx SELECT ((x-1)%100000)::symbol, (x-1)::timestamp, x-1 FROM long_sequence(" + RIGHT_ROWS + ")", ctx);
        engine.execute("DROP TABLE IF EXISTS ord_idx", ctx);
        engine.execute("CREATE TABLE ord_idx (sym SYMBOL, ts TIMESTAMP, oid LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO ord_idx SELECT '500', ((x-1)*" + (RIGHT_ROWS / 1000) + ")::timestamp, x-1 FROM long_sequence(1000)", ctx);
        // idx_sweep: indexed right with cardinality IDX_CARD (sweeps liquid<->illiquid). Left is one
        // symbol '0' with 1000 rows spread across the full range.
        engine.execute("DROP TABLE IF EXISTS md_sweep", ctx);
        engine.execute("CREATE TABLE md_sweep (sym SYMBOL INDEX, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO md_sweep SELECT ((x-1)%" + IDX_CARD + ")::symbol, (x-1)::timestamp, x-1 FROM long_sequence(" + RIGHT_ROWS + ")", ctx);
        engine.execute("DROP TABLE IF EXISTS ord_sweep", ctx);
        engine.execute("CREATE TABLE ord_sweep (sym SYMBOL, ts TIMESTAMP, oid LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO ord_sweep SELECT '0', ((x-1)*" + Math.max(1L, RIGHT_ROWS / MASTER_ROWS) + ")::timestamp, x-1 FROM long_sequence(" + MASTER_ROWS + ")", ctx);
        // noidx_sweep: identical to idx_sweep but slave symbol is NOT indexed (asof_index unavailable).
        engine.execute("DROP TABLE IF EXISTS md_nidx", ctx);
        engine.execute("CREATE TABLE md_nidx (sym SYMBOL, ts TIMESTAMP, v LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO md_nidx SELECT ((x-1)%" + IDX_CARD + ")::symbol, (x-1)::timestamp, x-1 FROM long_sequence(" + RIGHT_ROWS + ")", ctx);
        engine.execute("DROP TABLE IF EXISTS ord_nidx", ctx);
        engine.execute("CREATE TABLE ord_nidx (sym SYMBOL, ts TIMESTAMP, oid LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO ord_nidx SELECT '0', ((x-1)*" + Math.max(1L, RIGHT_ROWS / MASTER_ROWS) + ")::timestamp, x-1 FROM long_sequence(" + MASTER_ROWS + ")", ctx);
        engine.releaseAllWriters();
        System.out.println("asof-bench data built (rows/table=" + ROWS + ", right=" + RIGHT_ROWS + ", keys=" + KEYS
                + ", dense=" + DENSE + ") in " + (System.nanoTime() - t0) / 1_000_000 + "ms");
    }

    private static void createLong(String name) throws SqlException {
        engine.execute("DROP TABLE IF EXISTS " + name, ctx);
        engine.execute("CREATE TABLE " + name + " (key LONG, ts TIMESTAMP, payload LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
    }

    private static void createSym(String name) throws SqlException {
        engine.execute("DROP TABLE IF EXISTS " + name, ctx);
        engine.execute("CREATE TABLE " + name + " (sym SYMBOL, ts TIMESTAMP, payload LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
    }

    private static void createMultiKey(String name) throws SqlException {
        engine.execute("DROP TABLE IF EXISTS " + name, ctx);
        engine.execute("CREATE TABLE " + name + " (sym1 SYMBOL, sym2 SYMBOL, ts TIMESTAMP, payload LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
    }

    private static synchronized void ensureEngine() {
        if (engine != null) {
            return;
        }
        //noinspection ResultOfMethodCallIgnored
        new java.io.File(ROOT).mkdirs();
        engine = new CairoEngine(configuration);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "asof-adaptive-bench";
            }

            @Override
            public int getWorkerCount() {
                return 2;
            }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        compiler = engine.getSqlCompiler();
        ctx = newContext(engine, 2);
    }

    private static SqlExecutionContext newContext(CairoEngine engine, int workerCount) {
        return new SqlExecutionContextImpl(engine, workerCount) {
            @Override
            public boolean shouldLogSql() {
                return false;
            }
        }.with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );
    }

    private static void explainAll() throws SqlException {
        final String[] shapes = {"dense_ts", "unique_ts", "dense_sym", "illiquid_sym", "multikey_sym", "nokey", "sparse_tail", "illiquid_idx"};
        System.out.println("=== PLAN SELECTION (shape : default[NEW] | fast[OLD]) ===");
        for (String shape : shapes) {
            System.out.println(shape + "\n    NEW default -> " + topAsofPlanLine(shape, "default")
                    + "\n    OLD fast    -> " + topAsofPlanLine(shape, "fast"));
        }
    }

    // Compile EXPLAIN and return the first "AsOf Join ..." line of the plan (the selected join cursor).
    private static String topAsofPlanLine(String dist, String algo) throws SqlException {
        try (RecordCursorFactory f = compiler.compile("EXPLAIN " + query(dist, algo), ctx).getRecordCursorFactory();
             RecordCursor cursor = f.getCursor(ctx)) {
            final Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                final CharSequence line = rec.getStrA(0);
                if (line != null) {
                    final String s = line.toString().trim();
                    if (s.startsWith("AsOf Join")) {
                        return s;
                    }
                }
            }
            return "(no AsOf Join node)";
        }
    }

    private static String query(String dist, String algo) {
        final String hint;
        switch (algo) {
            case "fast":
                hint = "/*+ asof_fast(l r) */ ";
                break;
            case "dense":
                hint = "/*+ asof_dense(l r) */ ";
                break;
            case "linear":
                hint = "/*+ asof_linear(l r) */ ";
                break;
            case "memoized":
                hint = "/*+ asof_memoized(l r) */ ";
                break;
            case "index":
                hint = "/*+ asof_index(l r) */ ";
                break;
            default: // default + adaptive: no hint, use the plan default (Dense / DenseSingleSymbol)
                hint = "";
        }
        switch (dist) {
            case "dense_ts":
                return "SELECT " + hint + "sum(r.payload) FROM l_dense l ASOF JOIN r_dense r ON (key)";
            case "unique_ts":
                return "SELECT " + hint + "sum(r.payload) FROM l_uniq l ASOF JOIN r_uniq r ON (key)";
            case "dense_sym":
                return "SELECT " + hint + "sum(r.payload) FROM l_dsym l ASOF JOIN r_dsym r ON (sym)";
            case "illiquid_sym":
                return "SELECT " + hint + "sum(r.v) FROM ord l ASOF JOIN md r ON (sym)";
            case "multikey_sym":
                // two symbol keys, realistic unique ts: new default is general Dense, old default was Fast.
                return "SELECT " + hint + "sum(r.payload) FROM l_mk l ASOF JOIN r_mk r ON (sym1, sym2)";
            case "nokey":
                // no join key (single stream ASOF): untouched by the flip. Control shape.
                return "SELECT " + hint + "sum(r.payload) FROM l_nk l ASOF JOIN r_nk r";
            case "sparse_tail":
                // small left window at the TAIL of a huge right: the classic Fast-favourable shape
                // (Dense must walk the right frame forward to reach the window; Fast jumps to it).
                return "SELECT " + hint + "sum(r.payload) FROM l_tail l ASOF JOIN r_tail r ON (key)";
            case "illiquid_idx":
                // very illiquid, INDEXED symbol: the right symbol is 1-in-cardinality sparse, so the
                // index path can jump straight to its rows instead of forward-scanning the whole right.
                return "SELECT " + hint + "sum(r.v) FROM ord_idx l ASOF JOIN md_idx r ON (sym)";
            case "idx_sweep":
                // parameterizable INDEXED shape: symbol cardinality via -Dasof.bench.idx.card to sweep
                // liquid (low card, many rows/symbol) -> illiquid (high card) and find the index crossover.
                return "SELECT " + hint + "sum(r.v) FROM ord_sweep l ASOF JOIN md_sweep r ON (sym)";
            case "noidx_sweep":
                // same as idx_sweep but the slave symbol is NOT indexed: what's the best algo when no
                // index is available? (asof_index cannot apply here.)
                return "SELECT " + hint + "sum(r.v) FROM ord_nidx l ASOF JOIN md_nidx r ON (sym)";
            default:
                throw new IllegalArgumentException("unknown dist: " + dist);
        }
    }
}
