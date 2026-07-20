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
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
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
 * Measures the aggregation cost of a COMPOSITE table (time + {@code exch} dimension, i.e.
 * {@code partition by day, exch}) against a byte-for-byte identical PLAIN twin ({@code partition by
 * day} only), across SUM / multi-aggregate / COUNT / keyed GROUP BY.
 * <p>
 * Composite reads currently bypass the vectorized/parallel aggregation path that plain tables use,
 * so every {@code *_composite} method here is expected to be visibly slower than its {@code *_plain}
 * counterpart -- that gap is exactly what the follow-up frame-vectorization work is meant to close.
 * This benchmark exists to quantify the gap, not to assert a threshold on it.
 * <p>
 * Twin tables {@code ci} (composite) and {@code pi} (plain) are built ONCE in {@link #main}, seeded
 * with identical rows via a single {@code insert ... select ... from long_sequence(N)} per table, then
 * WAL-drained (both tables are {@code WAL}, so rows are invisible until the WAL apply job runs). {@code
 * exch} takes {@link #NUM_EXCH} distinct values spread evenly over {@link #SEED_DAYS} days, so the
 * composite table gets multiple cells per day; {@code sym} takes {@link #NUM_SYM} distinct values purely
 * to give the keyed GROUP BY benchmark real cardinality.
 * <p>
 * Tunables (system properties): {@code composite.bench.rows} (default 5,000,000),
 * {@code composite.bench.workers} (default 8).
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core, not the installed jar) and run via
 * this class's {@code main} (which builds the data first), passing the module flags the worker pool
 * needs:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
 *      --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED \
 *      -cp benchmarks/target/benchmarks.jar org.questdb.CompositeAggregationBenchmark \
 *      "CompositeAggregationBenchmark" -r 2 -w 1            # shorter JMH iterations
 * </pre>
 * Extra args are passed through to JMH (e.g. {@code "sum_"} to restrict to the sum pair via regex).
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
// @Fork(0): run in-process, same rationale as CoveredIndexDecodeBenchmark -- a fork per @Benchmark
// method would cold-start a fresh JVM and reopen the engine each time; in-process lets the shared
// engine/pool (built once per run, see ensureEngine()) be reused across all 8 methods. The module
// flags the worker pool needs are passed on the launching `java` command line (see the class javadoc).
@Fork(0)
public class CompositeAggregationBenchmark {

    // A handful of exch values so the composite table gets multiple cells/day.
    private static final int NUM_EXCH = 6;
    // Enough sym cardinality to give the keyed GROUP BY something real to do.
    private static final int NUM_SYM = 100;
    // Spread the rows over this many day-partitions (so both tables get a realistic number of
    // partitions -- and the composite table gets NUM_EXCH cells in each -- regardless of ROWS).
    private static final int SEED_DAYS = 20;
    private static final int WORKERS = Integer.getInteger("composite.bench.workers", 8);
    private static final long ROWS = Long.getLong("composite.bench.rows", 5_000_000L);
    private static final String ROOT = System.getProperty("java.io.tmpdir") + java.io.File.separator + "composite-agg-bench";
    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(ROOT) {
        @Override
        public int getSqlPageFrameMaxRows() {
            // Small-ish frames so the plain twin's parallel scan actually spreads across workers
            // instead of collapsing to one frame per day-partition.
            return 100_000;
        }

        @Override
        public boolean isSqlParallelGroupByEnabled() {
            return true;
        }
    };

    // Engine/pool/compiler/context are built ONCE per forked JVM and shared across every @Benchmark
    // method (a fresh CairoEngine loads ~1100 functions + opens readers, and a WorkerPool start/stop
    // is not free -- doing that per method would dominate the run). Only the per-trial factories are
    // recompiled.
    private static CairoEngine engine;
    private static WorkerPool pool;
    private static SqlCompiler compiler;
    private static SqlExecutionContext ctx;

    private RecordCursorFactory sumCompositeFactory;
    private RecordCursorFactory sumPlainFactory;
    private RecordCursorFactory multiAggCompositeFactory;
    private RecordCursorFactory multiAggPlainFactory;
    private RecordCursorFactory countCompositeFactory;
    private RecordCursorFactory countPlainFactory;
    private RecordCursorFactory groupByKeyedCompositeFactory;
    private RecordCursorFactory groupByKeyedPlainFactory;

    public static void main(String[] args) throws Exception {
        // The engine requires its root directory to exist before it opens.
        java.nio.file.Files.createDirectories(java.nio.file.Paths.get(ROOT));
        // Build the data once into the shared root; trials reopen the same root.
        try (CairoEngine engine = new CairoEngine(configuration)) {
            final SqlExecutionContext ctx = newContext(engine, 1);
            engine.execute("drop table if exists ci", ctx);
            engine.execute("drop table if exists pi", ctx);
            engine.execute(
                    "create table ci (ts timestamp, exch symbol, sym symbol, px double)" +
                            " timestamp(ts) partition by day, exch wal", ctx);
            engine.execute(
                    "create table pi (ts timestamp, exch symbol, sym symbol, px double)" +
                            " timestamp(ts) partition by day wal", ctx);
            // Spread the rows over SEED_DAYS day-partitions regardless of the configured row count.
            final long spacingUs = Math.max(1L, SEED_DAYS * 86_400_000_000L / ROWS);
            final String gen =
                    "select" +
                            " ('2024-01-01'::timestamp + (x - 1) * " + spacingUs + "L)::timestamp," +
                            " ('EXCH' || (x % " + NUM_EXCH + "))::symbol," +
                            " ('SYM' || (x % " + NUM_SYM + "))::symbol," +
                            " (x % 997)::double" +
                            " from long_sequence(" + ROWS + ")";
            final long t0 = System.nanoTime();
            engine.execute("insert into ci " + gen, ctx);
            engine.execute("insert into pi " + gen, ctx);
            drainWal(engine);
            engine.releaseAllWriters();
            System.out.println("composite-agg-bench data built: " + ROWS + " rows/table (" + NUM_EXCH +
                    " exch x " + SEED_DAYS + " days) in " + (System.nanoTime() - t0) / 1_000_000 + "ms");
        }

        // Pass JMH CLI args through when provided (e.g. "CompositeAggregationBenchmark sum_ -wi 1 -i 3");
        // otherwise run the full default set of 8 methods.
        final Options opt = args.length > 0
                ? new org.openjdk.jmh.runner.options.CommandLineOptions(args)
                : new OptionsBuilder().include(CompositeAggregationBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
        // The shared engine/pool opened lazily by ensureEngine() (invoked from the first @Setup(Level.Trial))
        // hold non-daemon worker threads that otherwise keep the JVM alive forever after the JMH run
        // prints its results table. Close them explicitly so the process actually exits on its own.
        // The borrowed compiler must be returned to its pool before the engine closes, or engine.close()
        // throws (SqlCompilerPool refuses to shut down while a compiler is still checked out).
        if (compiler != null) {
            compiler.close();
        }
        if (pool != null) {
            pool.close();
        }
        if (engine != null) {
            engine.close();
        }
        LogFactory.haltInstance();
    }

    @Benchmark
    public void sum_composite(Blackhole bh) throws SqlException {
        drain(sumCompositeFactory, bh);
    }

    @Benchmark
    public void sum_plain(Blackhole bh) throws SqlException {
        drain(sumPlainFactory, bh);
    }

    @Benchmark
    public void multiAgg_composite(Blackhole bh) throws SqlException {
        drain(multiAggCompositeFactory, bh);
    }

    @Benchmark
    public void multiAgg_plain(Blackhole bh) throws SqlException {
        drain(multiAggPlainFactory, bh);
    }

    @Benchmark
    public void count_composite(Blackhole bh) throws SqlException {
        drain(countCompositeFactory, bh);
    }

    @Benchmark
    public void count_plain(Blackhole bh) throws SqlException {
        drain(countPlainFactory, bh);
    }

    @Benchmark
    public void groupByKeyed_composite(Blackhole bh) throws SqlException {
        drain(groupByKeyedCompositeFactory, bh);
    }

    @Benchmark
    public void groupByKeyed_plain(Blackhole bh) throws SqlException {
        drain(groupByKeyedPlainFactory, bh);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        ensureEngine();
        sumCompositeFactory = compiler.compile("select sum(px) from ci", ctx).getRecordCursorFactory();
        sumPlainFactory = compiler.compile("select sum(px) from pi", ctx).getRecordCursorFactory();
        multiAggCompositeFactory = compiler.compile(
                "select sum(px), count(), avg(px), min(px), max(px) from ci", ctx).getRecordCursorFactory();
        multiAggPlainFactory = compiler.compile(
                "select sum(px), count(), avg(px), min(px), max(px) from pi", ctx).getRecordCursorFactory();
        countCompositeFactory = compiler.compile("select count() from ci", ctx).getRecordCursorFactory();
        countPlainFactory = compiler.compile("select count() from pi", ctx).getRecordCursorFactory();
        groupByKeyedCompositeFactory = compiler.compile(
                "select sym, sum(px) from ci group by sym", ctx).getRecordCursorFactory();
        groupByKeyedPlainFactory = compiler.compile(
                "select sym, sum(px) from pi group by sym", ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Only the per-trial factories are released here; the shared engine/pool live for the JVM
        // and are reclaimed when the forked benchmark JVM exits.
        sumCompositeFactory = Misc.free(sumCompositeFactory);
        sumPlainFactory = Misc.free(sumPlainFactory);
        multiAggCompositeFactory = Misc.free(multiAggCompositeFactory);
        multiAggPlainFactory = Misc.free(multiAggPlainFactory);
        countCompositeFactory = Misc.free(countCompositeFactory);
        countPlainFactory = Misc.free(countPlainFactory);
        groupByKeyedCompositeFactory = Misc.free(groupByKeyedCompositeFactory);
        groupByKeyedPlainFactory = Misc.free(groupByKeyedPlainFactory);
    }

    private static void drain(RecordCursorFactory factory, Blackhole bh) throws SqlException {
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

    private static synchronized void ensureEngine() {
        if (engine != null) {
            return;
        }
        engine = new CairoEngine(configuration);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "composite-agg-bench";
            }

            @Override
            public int getWorkerCount() {
                return WORKERS;
            }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        compiler = engine.getSqlCompiler();
        ctx = newContext(engine, WORKERS);
    }

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
            if (new CheckWalTransactionsJob(engine).run()) {
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run()) ;
            }
        }
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
}
