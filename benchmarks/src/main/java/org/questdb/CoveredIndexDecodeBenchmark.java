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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import io.questdb.std.str.Utf8Sequence;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Measures the parallel covered-index decode (PR "parallel covered-index decode for aggregation
 * and filter") across a broad battery of query shapes. The covered table carries
 * DOUBLE / LONG / SYMBOL / VARCHAR covered columns so every decode path is exercised.
 * <p>
 * {@code config} selects one of three comparison points:
 * <ul>
 *   <li>{@code PARALLEL_COV} — the new path: N workers over the covering index.</li>
 *   <li>{@code SERIAL_COV} — the same covered query at 1 worker (the parallelism this branch unlocks;
 *       PARALLEL_COV / SERIAL_COV is the parallelism speedup).</li>
 *   <li>{@code PARALLEL_REF} — the same query over a NON-indexed twin at N workers (a full parallel
 *       scan; PARALLEL_REF / PARALLEL_COV is whether the covering index is actually the better plan).</li>
 * </ul>
 * {@code selectivity} sweeps the filtered key's row fraction (0.1% … 50%): each value maps to a
 * distinct symbol present at exactly that fraction, so a single table serves the whole sweep. This
 * is the dimension that decides covered-vs-scan — a covering index only wins below some selectivity.
 * <p>
 * The data is built ONCE in {@link #main} into a fixed db root and reused by every trial's engine.
 * Tunables (system properties): {@code covered.bench.rows} (default 50,000,000),
 * {@code covered.bench.workers} (default 8).
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core, not the installed jar) and run
 * via this class's {@code main} (which builds the data first), passing the module flags the worker
 * pool needs:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
 *      --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.CoveredIndexDecodeBenchmark \
 *      "CoveredIndexDecodeBenchmark" -r 2 -w 1            # shorter JMH iterations
 * </pre>
 * Extra args are passed through to JMH (e.g. {@code -p shape=sum,count -p config=PARALLEL_COV} to
 * restrict the matrix).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
// @Fork(0): run in-process. A fork-per-combo would cold-start a fresh JVM (loading the ~43 MB shaded
// jar) and reopen the engine for every one of the dozens of @Param combinations, which dominated the
// wall-clock. In-process lets the shared engine/pool (built once per run) be reused across combos.
// The module flags the worker pool needs are passed on the launching `java` command line (see the
// class javadoc), so no forked jvmArgs are required.
@Fork(0)
public class CoveredIndexDecodeBenchmark {

    private static final int PARALLEL_WORKERS = Integer.getInteger("covered.bench.workers", 8);
    private static final long ROWS = Long.getLong("covered.bench.rows", 50_000_000L);
    private static final String ROOT = System.getProperty("java.io.tmpdir") + java.io.File.separator + "covered-index-decode-bench";
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(ROOT) {
        @Override
        public int getSqlPageFrameMaxRows() {
            // Small-ish frames so a single hot key spreads into many frames across the workers.
            return 100_000;
        }

        @Override
        public boolean isSqlParallelGroupByEnabled() {
            return true;
        }
    };

    @Param({"sum", "multi_agg", "first_last", "count", "residual", "filter_project", "groupby_symbol", "groupby_varchar"})
    public String shape;

    @Param({"PARALLEL_COV", "SERIAL_COV", "PARALLEL_REF"})
    public String config;

    // Selectivity of the filtered key, as a percentage of rows. Each value maps to a distinct
    // symbol present at exactly that fraction (see the data generator), so the sweep isolates how
    // covered-vs-scan changes with selectivity.
    @Param({"0.1", "1", "5", "10", "25", "50"})
    public String selectivity;

    // Engines/pools/contexts are built ONCE per forked JVM and shared across every trial (a fresh
    // CairoEngine loads ~1100 functions + opens readers, and a WorkerPool start/stop is not free —
    // doing that per @Param combo dominated the run). Only the per-trial factory is recompiled.
    // A given JMH invocation uses just one of these (the parallel sweep vs. the serial battery are
    // separate invocations), so the two engines never open the same root concurrently.
    private static CairoEngine parEngine;
    private static WorkerPool parPool;
    private static SqlCompiler parCompiler;
    private static SqlExecutionContext parCtx;
    private static CairoEngine serEngine;
    private static WorkerPool serPool;
    private static SqlCompiler serCompiler;
    private static SqlExecutionContext serCtx;

    private RecordCursorFactory factory;
    private SqlExecutionContext ctx;

    public static void main(String[] args) throws Exception {
        // The engine requires its root directory to exist before it opens.
        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(ROOT));
        // Build the data once into the shared root; trials reopen the same root.
        try (CairoEngine engine = new CairoEngine(configuration)) {
            final SqlExecutionContext ctx = newContext(engine, 1);
            engine.execute("DROP TABLE IF EXISTS cov", ctx);
            engine.execute("DROP TABLE IF EXISTS ref", ctx);
            engine.execute(
                    "CREATE TABLE cov (" +
                            "  ts TIMESTAMP," +
                            "  sym SYMBOL INDEX TYPE POSTING INCLUDE (px, qty, grp, tag)," +
                            "  px DOUBLE, qty LONG, grp SYMBOL, tag VARCHAR" +
                            ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            engine.execute(
                    "CREATE TABLE ref (ts TIMESTAMP, sym SYMBOL, px DOUBLE, qty LONG, grp SYMBOL, tag VARCHAR)" +
                            " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
            // Spread the rows over ~16 day-partitions (so frames distribute across workers without
            // drowning in per-partition overhead), regardless of the configured row count.
            final long spacingUs = Math.max(1L, 16L * 86_400_000_000L / ROWS);
            // Controlled selectivity via permille (x % 1000) buckets: each value of the selectivity
            // @Param maps to a distinct symbol present at exactly that fraction of rows, so a single
            // table serves the whole sweep. 0.1% + 1% + 5% + 10% + 25% + 50% = 91.1%; the remaining
            // ~8.9% spreads over 64 noise keys (a realistic symbol table, not one giant key).
            final String gen =
                    "SELECT" +
                            " ('2024-01-01'::TIMESTAMP + (x - 1) * " + spacingUs + "L)::timestamp," +
                            " (CASE" +
                            "   WHEN (x % 1000) = 0 THEN 'sel0_1'" +
                            "   WHEN (x % 1000) BETWEEN 1 AND 10 THEN 'sel1'" +
                            "   WHEN (x % 1000) BETWEEN 11 AND 60 THEN 'sel5'" +
                            "   WHEN (x % 1000) BETWEEN 61 AND 160 THEN 'sel10'" +
                            "   WHEN (x % 1000) BETWEEN 161 AND 410 THEN 'sel25'" +
                            "   WHEN (x % 1000) BETWEEN 411 AND 910 THEN 'sel50'" +
                            "   ELSE 'noise' || (x % 64) END)::symbol," +
                            " (x % 997)::double," +
                            " (x % 1000)::long," +
                            " ('G' || (x % 8))::symbol," +
                            " ('T' || (x % 8))::varchar" +
                            " FROM long_sequence(" + ROWS + ")";
            final long t0 = System.nanoTime();
            engine.execute("INSERT INTO cov " + gen, ctx);
            engine.execute("INSERT INTO ref " + gen, ctx);
            engine.releaseAllWriters();
            System.out.println("covered-bench data built: " + ROWS + " rows/table in " + (System.nanoTime() - t0) / 1_000_000 + "ms");
        }

        // Pass JMH CLI args through when provided (e.g. "CoveredIndexDecodeBenchmark -p shape=sum
        // -p config=PARALLEL_COV -wi 1 -i 3"); otherwise run the full default matrix.
        final Options opt = args.length > 0
                ? new org.openjdk.jmh.runner.options.CommandLineOptions(args)
                : new OptionsBuilder().include(CoveredIndexDecodeBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final RecordMetadata m = factory.getMetadata();
            final int n = m.getColumnCount();
            final Record rec = cursor.getRecord();
            while (cursor.hasNext()) {
                // Touch every column by its real type so the optimizer cannot elide the decode.
                for (int c = 0; c < n; c++) {
                    switch (ColumnType.tagOf(m.getColumnType(c))) {
                        case ColumnType.DOUBLE:
                        case ColumnType.FLOAT:
                            bh.consume(rec.getDouble(c));
                            break;
                        case ColumnType.LONG:
                        case ColumnType.TIMESTAMP:
                        case ColumnType.DATE:
                            bh.consume(rec.getLong(c));
                            break;
                        case ColumnType.INT:
                        case ColumnType.SYMBOL:
                        case ColumnType.IPv4:
                            bh.consume(rec.getInt(c));
                            break;
                        case ColumnType.VARCHAR:
                            final Utf8Sequence v = rec.getVarcharA(c);
                            bh.consume(v == null ? 0 : v.size());
                            break;
                        default:
                            bh.consume(1);
                            break;
                    }
                }
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final SqlCompiler compiler;
        final String table;
        if ("SERIAL_COV".equals(config)) {
            ensureSerial();
            ctx = serCtx;
            compiler = serCompiler;
            table = "cov";
        } else {
            ensureParallel();
            ctx = parCtx;
            compiler = parCompiler;
            table = "PARALLEL_REF".equals(config) ? "ref" : "cov";
        }
        factory = compiler.compile(query(shape, table, keyFor(selectivity)), ctx).getRecordCursorFactory();
    }

    private static synchronized void ensureParallel() {
        if (parEngine != null) {
            return;
        }
        parEngine = new CairoEngine(configuration);
        parPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "covered-index-decode-bench";
            }

            @Override
            public int getWorkerCount() {
                return PARALLEL_WORKERS;
            }
        });
        WorkerPoolUtils.setupQueryJobs(parPool, parEngine);
        parPool.start();
        parCompiler = parEngine.getSqlCompiler();
        parCtx = newContext(parEngine, PARALLEL_WORKERS);
    }

    private static synchronized void ensureSerial() {
        if (serEngine != null) {
            return;
        }
        // A dedicated 1-worker engine+pool: the covered query dispatches through the async path but
        // reduces on a single worker — the pre-parallelism baseline this PR speeds up.
        serEngine = new CairoEngine(configuration);
        serPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "covered-index-decode-bench-serial";
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }
        });
        WorkerPoolUtils.setupQueryJobs(serPool, serEngine);
        serPool.start();
        serCompiler = serEngine.getSqlCompiler();
        serCtx = newContext(serEngine, 1);
    }

    private static String keyFor(String selectivity) {
        switch (selectivity) {
            case "0.1":
                return "sel0_1";
            case "1":
                return "sel1";
            case "5":
                return "sel5";
            case "10":
                return "sel10";
            case "25":
                return "sel25";
            case "50":
                return "sel50";
            default:
                throw new IllegalArgumentException("unknown selectivity: " + selectivity);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Only the per-trial factory is released here; the shared engines/pool live for the JVM
        // and are reclaimed when the forked benchmark JVM exits.
        factory = Misc.free(factory);
    }

    private static SqlExecutionContext newContext(CairoEngine engine, int workerCount) {
        // Suppress per-query progress logging: JMH invokes the query thousands of times per
        // iteration, which would otherwise flood stdout (and slow the run) with one log line each.
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

    private static String query(String shape, String table, String key) {
        final String w = " WHERE sym = '" + key + "'";
        switch (shape) {
            case "sum":
                return "SELECT sum(px) FROM " + table + w;
            case "multi_agg":
                return "SELECT sum(px), count(), avg(px), min(px), max(px) FROM " + table + w;
            case "sum_long":
                return "SELECT sum(qty), max(qty) FROM " + table + w;
            case "first_last":
                return "SELECT first(px), last(px) FROM " + table + w;
            case "count":
                return "SELECT count() FROM " + table + w;
            case "residual":
                return "SELECT sum(px) FROM " + table + w + " AND px > 500";
            case "groupby_symbol":
                return "SELECT grp, sum(px), count() FROM " + table + w + " GROUP BY grp ORDER BY grp";
            case "groupby_varchar":
                return "SELECT tag, sum(px), count() FROM " + table + w + " GROUP BY tag ORDER BY tag";
            case "filter_project":
                return "SELECT ts, px, qty, grp FROM " + table + w;
            default:
                throw new IllegalArgumentException("unknown shape: " + shape);
        }
    }
}
