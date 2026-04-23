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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory;
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
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end SAMPLE BY 1m FILL(PREV) benchmark that measures the new
 * fast path ({@link SampleByFillRecordCursorFactory}) against the legacy
 * non-parallel cursor ({@link SampleByFillPrevRecordCursorFactory}).
 * <p>
 * The fast path does two scans of the base cursor (pass 1 discovers keys,
 * pass 2 emits rows) plus a per-gap iteration over the materialised keys
 * map. Payoff: parallel Async GroupBy dispatch. Cost: pure overhead in a
 * single-worker context.
 * <p>
 * The path the compiler selects is gated by query shape: ALIGN TO CALENDAR
 * triggers the SAMPLE BY to GROUP BY rewrite (fast path); ALIGN TO FIRST
 * OBSERVATION blocks the rewrite (legacy). There is no config toggle.
 * <p>
 * Two scenarios, two paths each:
 * <ul>
 *   <li>{@code WORST_CASE}: 50k keys, sparse (~1% density), 200 buckets,
 *       single worker. Fast path carries two-pass overhead without
 *       parallelism to offset it.</li>
 *   <li>{@code POSITIVE_CASE}: 1k keys, dense (no gaps), 500 buckets,
 *       worker count configurable via -Dsample.by.fill.workers (default 4).
 *       Parallel Async GroupBy should amortize the two-pass cost.</li>
 * </ul>
 * <p>
 * A {@code @Setup(Level.Trial)} routing guard walks the compiled factory
 * chain (base-factory traversal, the same pattern as
 * {@code RecordCursorMemoryUsageTest.testSampleByCursorReleasesMemoryOnClose})
 * to confirm each scenario+path pair actually hits the expected factory
 * class. A silent routing drift would corrupt the numbers.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class SampleByFillPrevPathBenchmark {

    private static final String BUCKET = "1m";
    private static final int POSITIVE_CASE_STEP_MICROS = 6_000;
    private static final int POSITIVE_CASE_UNIQUE_KEYS = 1_000;
    private static final String START_TS = "2024-01-01T00:00:00.000000Z";
    private static final int WORKER_COUNT_DEFAULT = 4;
    private static final String WORKER_COUNT_PROP = "sample.by.fill.workers";
    // Coprime to WORST_CASE_UNIQUE_KEYS so keys scatter across buckets without
    // structured alignment.
    private static final int WORST_CASE_KEY_MULTIPLIER = 7_919;
    private static final int WORST_CASE_STEP_MICROS = 120_000;
    private static final int WORST_CASE_UNIQUE_KEYS = 50_000;
    // Sized to hold the larger of the two scenario key sets with headroom.
    private static final int SYMBOL_CAPACITY = Math.max(WORST_CASE_UNIQUE_KEYS, POSITIVE_CASE_UNIQUE_KEYS) * 2;

    @Param({"LEGACY", "FAST_PATH"})
    public String path;

    @Param({"100000"})
    public int rowCount;

    @Param({"WORST_CASE", "POSITIVE_CASE"})
    public String scenario;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SampleByFillPrevPathBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bh.consume(record.getSymA(0));
                bh.consume(record.getDouble(1));
                bh.consume(record.getTimestamp(2));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final int workerCount = resolveWorkerCount();
        tempRoot = java.nio.file.Files.createTempDirectory("samplebyfillprevpathbench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString());
        engine = new CairoEngine(configuration);

        if (workerCount > 1) {
            workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "samplebyfillprevpathbench";
                }

                @Override
                public int getWorkerCount() {
                    return workerCount;
                }
            });
            WorkerPoolUtils.setupQueryJobs(workerPool, engine);
            workerPool.start();
        }

        ctx = new SqlExecutionContextImpl(engine, workerCount)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        compiler = new SqlCompilerImpl(engine);

        seedTable();

        final String sql = buildSql();
        factory = compiler.compile(sql, ctx).getRecordCursorFactory();
        assertRouting(factory);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        engine = Misc.free(engine);
        if (tempRoot != null && java.nio.file.Files.exists(tempRoot)) {
            try (java.util.stream.Stream<Path> stream = java.nio.file.Files.walk(tempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        java.nio.file.Files.deleteIfExists(path);
                    } catch (Exception ignore) {
                    }
                });
            }
            tempRoot = null;
        }
    }

    private void assertRouting(RecordCursorFactory root) {
        final Class<?> expected = "FAST_PATH".equals(path)
                ? SampleByFillRecordCursorFactory.class
                : SampleByFillPrevRecordCursorFactory.class;
        RecordCursorFactory cur = root;
        while (cur != null) {
            if (expected.isInstance(cur)) {
                return;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        throw new IllegalStateException(
                "routing drift: expected " + expected.getSimpleName()
                        + " in factory chain but did not find it. path=" + path
                        + " scenario=" + scenario
                        + " root=" + root.getClass().getSimpleName()
        );
    }

    private String buildSql() {
        final String alignment = "FAST_PATH".equals(path) ? "CALENDAR" : "FIRST OBSERVATION";
        return "SELECT sym1, last(d), ts FROM tab SAMPLE BY " + BUCKET + " FILL(PREV) ALIGN TO " + alignment;
    }

    private int resolveWorkerCount() {
        if ("WORST_CASE".equals(scenario)) {
            return 1;
        }
        final String configured = System.getProperty(WORKER_COUNT_PROP);
        if (configured != null) {
            final int parsed = Integer.parseInt(configured);
            if (parsed < 1) {
                throw new IllegalArgumentException(WORKER_COUNT_PROP + " must be >= 1, got " + parsed);
            }
            return parsed;
        }
        return WORKER_COUNT_DEFAULT;
    }

    private void seedTable() throws SqlException {
        engine.execute(
                "CREATE TABLE tab ("
                        + "sym1 SYMBOL CAPACITY " + SYMBOL_CAPACITY + ","
                        + "d DOUBLE,"
                        + "ts TIMESTAMP"
                        + ") TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        final String insert;
        if ("WORST_CASE".equals(scenario)) {
            // High-cardinality sparse data: key chosen by a coprime multiplier
            // so keys scatter across buckets without structured alignment. At
            // the default rowCount (100_000) each bucket sees only a small
            // fraction of keys -> most keys missing per bucket -> fast path
            // emits many PREV fill rows per gap. Stresses keysMap build +
            // per-gap fill emission.
            insert = "INSERT INTO tab "
                    + "SELECT "
                    + "((x * " + WORST_CASE_KEY_MULTIPLIER + ") % " + WORST_CASE_UNIQUE_KEYS + ")::SYMBOL AS sym1, "
                    + "rnd_double() AS d, "
                    + "timestamp_sequence('" + START_TS + "'::timestamp, " + WORST_CASE_STEP_MICROS + ") AS ts "
                    + "FROM long_sequence(" + rowCount + ")";
        } else {
            // Low-cardinality dense data: every key appears in every bucket so
            // aggregation cost dominates and async GroupBy has enough work to
            // dispatch across workers. Few fill gaps -> per-gap iteration over
            // the keysMap is cheap. Scale rowCount up (e.g. 5_000_000) to give
            // parallelism room to breathe.
            insert = "INSERT INTO tab "
                    + "SELECT "
                    + "(x % " + POSITIVE_CASE_UNIQUE_KEYS + ")::SYMBOL AS sym1, "
                    + "rnd_double() AS d, "
                    + "timestamp_sequence('" + START_TS + "'::timestamp, " + POSITIVE_CASE_STEP_MICROS + ") AS ts "
                    + "FROM long_sequence(" + rowCount + ")";
        }
        engine.execute(insert, ctx);
    }
}
