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
import io.questdb.cairo.SampleBySortStrategy;
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

    private static final int BEST_CASE_BUCKET_COUNT = 1_440;
    // BEST_CASE keeps A = K * B small even for huge R: span is fixed (B
    // buckets * 1m), so step = (B * 1m) / R. As R grows, step shrinks but B
    // and A stay constant. With B=1440 (1 day @ 1m) and K=100, A=144_000
    // regardless of R. Resolution is fine down to R~5*10^10 (step ~1.7us).
    private static final long BEST_CASE_BUCKET_MICROS = 60_000_000L; // 1m
    private static final int BEST_CASE_UNIQUE_KEYS = 100;
    private static final String BUCKET = "1m";
    private static final int POSITIVE_CASE_STEP_MICROS = 6_000;
    private static final int POSITIVE_CASE_UNIQUE_KEYS = 1_000;
    private static final String START_TS = "2024-01-01T00:00:00.000000Z";
    // 2 * max(WORST_CASE_UNIQUE_KEYS, POSITIVE_CASE_UNIQUE_KEYS,
    // BEST_CASE_UNIQUE_KEYS). WORST_CASE dominates, so 2 * 50_000 = 100_000.
    // Inlined as a literal because alphabetical ordering puts SYMBOL_CAPACITY
    // before WORST_CASE_UNIQUE_KEYS, and forward references to non-constant
    // expressions resolve to the int default (0) at class init time.
    private static final int SYMBOL_CAPACITY = 100_000;
    // Coprime to WORST_CASE_UNIQUE_KEYS so keys scatter across buckets without
    // structured alignment.
    private static final int WORST_CASE_KEY_MULTIPLIER = 7_919;
    private static final int WORST_CASE_STEP_MICROS = 120_000;
    private static final int WORST_CASE_UNIQUE_KEYS = 50_000;

    @Param({"avg"})
    public String aggFunc;

    @Param({"LEGACY", "FAST_PATH"})
    public String path;

    @Param({"10000", "100000", "5000000"})
    public int rowCount;

    @Param({"WORST_CASE", "POSITIVE_CASE", "BEST_CASE"})
    public String scenario;

    // Sort strategy applied above AGB output before the fill cursor consumes it.
    // Has no effect on LEGACY (single-threaded scan, no sort). On FAST_PATH the
    // strategy is plumbed through CairoConfiguration.getSampleByFillSortStrategy()
    // (overridden in setUp()) and SqlCodeGenerator picks the matching factory.
    //
    // Four supported values:
    //   "light_encoded"     -> EncodedSortLightRecordCursorFactory
    //                          radix/quicksort over byte-encoded ts; rowId-based;
    //                          requires base.recordCursorSupportsRandomAccess()
    //   "full_encoded"      -> EncodedSortRecordCursorFactory
    //                          same sort algorithm; full record materialisation via RecordSink;
    //                          works without random-access requirement
    //   "light_recordchain" -> SortedLightRecordCursorFactory
    //                          red-black tree sort; rowId-based; requires random access
    //   "full_recordchain"  -> SortedRecordCursorFactory
    //                          red-black tree sort; full record materialisation
    //
    // Only "light_encoded" listed here so a default JMH run executes the production default.
    // Pass -p sort=full_encoded,light_recordchain,full_recordchain (or any subset) to compare.
    @Param({"light_encoded"})
    public String sort;

    @Param({"1", "4", "10"})
    public int workers;

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

    private static int parseSortStrategy(String sort) {
        switch (sort) {
            case "light_encoded":
                return SampleBySortStrategy.LIGHT_ENCODED;
            case "full_encoded":
                return SampleBySortStrategy.FULL_ENCODED;
            case "light_recordchain":
                return SampleBySortStrategy.LIGHT_RECORDCHAIN;
            case "full_recordchain":
                return SampleBySortStrategy.FULL_RECORDCHAIN;
            default:
                throw new IllegalArgumentException("unknown sort strategy: " + sort);
        }
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
        // Override sort memory caps so the encoded sort variants can hold
        // ~50M AGB output rows (~1.2 GB packed). Default is 256 MB total,
        // which trips LimitOverflowException on WORST 50M.
        final int sortStrategy = parseSortStrategy(sort);
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString()) {
            @Override
            public int getSampleByFillSortStrategy() {
                return sortStrategy;
            }

            @Override
            public int getSqlSortKeyMaxPages() {
                return 8192; // 8192 * 128 KB = 1 GB
            }

            @Override
            public int getSqlSortLightValueMaxPages() {
                return 8192; // 8192 * 128 KB = 1 GB
            }
        };
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
        return "SELECT sym1, " + aggFunc + "(d), ts FROM tab SAMPLE BY " + BUCKET + " FILL(PREV) ALIGN TO " + alignment;
    }

    private int resolveWorkerCount() {
        return workers;
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
        } else if ("BEST_CASE".equals(scenario)) {
            // Best case for parallel-aggregation paths: high R, low K, low B.
            // Span fixed at BEST_CASE_BUCKET_COUNT * 1m so that A = K*B stays
            // constant regardless of R. Step shrinks inversely with R but
            // stays in microseconds (1.7us at R=5*10^10).
            final long stepMicros = (BEST_CASE_BUCKET_COUNT * BEST_CASE_BUCKET_MICROS) / rowCount;
            insert = "INSERT INTO tab "
                    + "SELECT "
                    + "(x % " + BEST_CASE_UNIQUE_KEYS + ")::SYMBOL AS sym1, "
                    + "rnd_double() AS d, "
                    + "timestamp_sequence('" + START_TS + "'::timestamp, " + stepMicros + ") AS ts "
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
