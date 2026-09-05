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
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
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
 * End-to-end benchmark for the sole, window-only SUBSAMPLE execution path.
 * Ascending input exercises the identity-order fused selector; descending input
 * exercises traversal-ordinal mapping back to incoming row order.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class SubsampleWindowBenchmark {

    private static final String START_TS = "2024-01-01T00:00:00.000000Z";
    private static final long STEP_MICROS = 1_000L;

    @Param({"0.5"})
    public double compdev;

    @Param({"uniform", "cadence", "m4", "minmax", "lttb", "sdt"})
    public String method;

    @Param({"ascending", "descending"})
    public String order;

    @Param({"100000", "1000000"})
    public int rows;

    @Param({"500"})
    public int target;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(SubsampleWindowBenchmark.class.getSimpleName())
                .build();
        new Runner(options).run();
    }

    @Benchmark
    public void run(Blackhole blackhole) throws SqlException {
        final DrainResult result = drain(factory, context);
        blackhole.consume(result.count);
        blackhole.consume(result.checksum);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("subsample-window-bench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString());
        engine = new CairoEngine(configuration);

        final int workers = 4;
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "subsample-window-bench";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        WorkerPoolUtils.setupQueryJobs(workerPool, engine);
        workerPool.start();

        context = new SqlExecutionContextImpl(engine, workers)
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
        factory = compiler.compile(sql, context).getRecordCursorFactory();
        assertWindowRouting(factory);
        assertCorrectness(drain(factory, context), sql);
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

    private static void assertWindowRouting(RecordCursorFactory root) {
        RecordCursorFactory current = root;
        while (current != null) {
            if (current instanceof CachedWindowLightRecordCursorFactory) {
                return;
            }
            final RecordCursorFactory next = current.getBaseFactory();
            if (next == current) {
                break;
            }
            current = next;
        }
        throw new IllegalStateException("routing drift: expected CachedWindowLight in " + root);
    }

    private static DrainResult drain(RecordCursorFactory cursorFactory, SqlExecutionContext executionContext) throws SqlException {
        long count = 0;
        long firstTimestamp = Long.MIN_VALUE;
        long lastTimestamp = Long.MIN_VALUE;
        boolean ascending = true;
        boolean descending = true;
        double checksum = 0;
        try (RecordCursor cursor = cursorFactory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                final long timestamp = record.getTimestamp(0);
                if (count == 0) {
                    firstTimestamp = timestamp;
                } else {
                    ascending &= lastTimestamp < timestamp;
                    descending &= lastTimestamp > timestamp;
                }
                lastTimestamp = timestamp;
                checksum += record.getDouble(1);
                count++;
            }
        }
        return new DrainResult(count, firstTimestamp, lastTimestamp, ascending, descending, checksum);
    }

    private void assertCorrectness(DrainResult result, String sql) {
        if (result.count < 2 || result.count > rows || !Double.isFinite(result.checksum)) {
            throw new IllegalStateException("invalid SUBSAMPLE result [count=" + result.count + ", sql=" + sql + ']');
        }
        if ("ascending".equals(order) ? !result.ascending : !result.descending) {
            throw new IllegalStateException("incoming order not preserved [first=" + result.firstTimestamp
                    + ", last=" + result.lastTimestamp + ", sql=" + sql + ']');
        }

        final long expectedCount;
        switch (method) {
            case "uniform":
            case "lttb":
                expectedCount = target;
                break;
            case "cadence":
                expectedCount = 1L + (rows - 1L) / target + ((rows - 1L) % target == 0 ? 0 : 1);
                break;
            default:
                return; // M4/MinMax can deduplicate bucket roles; SDT cardinality is data-dependent.
        }
        if (result.count != expectedCount) {
            throw new IllegalStateException("unexpected SUBSAMPLE count [expected=" + expectedCount
                    + ", actual=" + result.count + ", sql=" + sql + ']');
        }
    }

    private String buildSql() {
        final String input = "ascending".equals(order)
                ? "x"
                : "(SELECT ts, value FROM x ORDER BY ts DESC)";
        final String clause = switch (method) {
            case "uniform" -> "uniform(" + target + ')';
            case "cadence" -> "cadence(" + target + ')';
            case "m4" -> "m4(value, " + target + ')';
            case "minmax" -> "minmax(value, " + target + ')';
            case "lttb" -> "lttb(value, " + target + ')';
            case "sdt" -> "sdt(value, " + compdev + ')';
            default -> throw new IllegalArgumentException("unknown method: " + method);
        };
        return "SELECT ts, value FROM " + input + " SUBSAMPLE " + clause;
    }

    private void seedTable() throws SqlException {
        engine.execute("CREATE TABLE x (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute(
                "INSERT INTO x SELECT "
                        + "timestamp_sequence('" + START_TS + "'::timestamp, " + STEP_MICROS + ") ts, "
                        + "sin(x::double / 37.0) * 50 + cos(x::double / 11.0) * 25 + (x % 97) value "
                        + "FROM long_sequence(" + rows + ')',
                context
        );
    }

    private record DrainResult(
            long count,
            long firstTimestamp,
            long lastTimestamp,
            boolean ascending,
            boolean descending,
            double checksum
    ) {
    }
}
