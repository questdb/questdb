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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Parallel filters (Async Filter and Async JIT Filter) over a native table: a full read of the matching rows, a
 * LIMIT the first frames satisfy, and a negative LIMIT that reads the frames from the end.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED")
public class AsyncFilterBenchmark {
    @Param({"100000"})
    public int frameRows;
    @Param({"true", "false"})
    public boolean isJit;
    @Param({"10_000_000"})
    public String rowCount;
    @Param({"4"})
    public int workers;
    @Param({"rows", "limit", "negative_limit"})
    public String workload;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        final Options options = new OptionsBuilder()
                .include(AsyncFilterBenchmark.class.getSimpleName())
                .build();
        new Runner(options).run();
    }

    @Benchmark
    public void filter(Blackhole blackhole) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                blackhole.consume(record.getTimestamp(0));
                blackhole.consume(record.getLong(1));
                blackhole.consume(record.getDouble(2));
            }
        }
    }

    @Setup
    public void setUp() throws Exception {
        root = Files.createTempDirectory("async-filter-");
        try {
            final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
                @Override
                public int getSqlPageFrameMaxRows() {
                    return frameRows;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return frameRows;
                }

                @Override
                public int getSqlParallelFilterDispatchLimit() {
                    return workers;
                }

                @Override
                public boolean isSqlParallelFilterEnabled() {
                    return true;
                }
            };
            engine = new CairoEngine(configuration);
            workerPool = new WorkerPool(() -> workers);
            WorkerPoolUtils.setupQueryJobs(workerPool, engine);
            workerPool.start();
            context = new SqlExecutionContextImpl(engine, workers)
                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext());
            context.setJitMode(isJit ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
            compiler = engine.getSqlCompiler();
            engine.execute("CREATE TABLE t AS (SELECT timestamp_sequence('2024-01-01', 1_000L) ts, x v, (x % 31)::DOUBLE d"
                    + " FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts) PARTITION BY DAY", context);
            final String limit = switch (workload) {
                case "rows" -> "";
                case "limit" -> " LIMIT 10";
                case "negative_limit" -> " LIMIT -10";
                default -> throw new IllegalArgumentException("unknown workload: " + workload);
            };
            // d cycles through 0 to 30, so the filter keeps about one row in ten.
            factory = compiler.compile("SELECT ts, v, d FROM t WHERE d < 3" + limit, context)
                    .getRecordCursorFactory();
            final TextPlanSink plan = new TextPlanSink();
            plan.of(factory, context);
            final String expected = isJit ? "Async JIT Filter" : "Async Filter";
            boolean isExpectedPlan = false;
            for (int i = 1; i <= plan.getLineCount(); i++) {
                isExpectedPlan |= plan.getLine(i).toString().trim().startsWith(expected);
            }
            if (!isExpectedPlan) {
                throw new IllegalStateException("expected " + expected);
            }
        } catch (Throwable th) {
            try {
                tearDown();
            } catch (Throwable cleanupFailure) {
                th.addSuppressed(cleanupFailure);
            }
            throw th;
        }
    }

    @TearDown
    public void tearDown() throws Exception {
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        context = Misc.free(context);
        engine = Misc.free(engine);
        if (root != null) {
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
            root = null;
        }
    }
}
