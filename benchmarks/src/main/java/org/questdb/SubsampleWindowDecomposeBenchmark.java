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
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Decomposes the SUBSAMPLE window-vs-cursor gap (m4, 1M rows, target 500) into named components,
 * to attribute the ~3x slowdown to specific window-framework costs:
 * <ul>
 *   <li>{@code raw_scan}: {@code SELECT ts, value FROM x} drained - the page-frame scan floor both paths share.</li>
 *   <li>{@code cursor}: the SUBSAMPLE cursor path (window disabled) - one compact pass + select + emit kept.</li>
 *   <li>{@code window_compute}: {@code SELECT count(*) FROM (SELECT ..., m4() OVER (ORDER BY ts) k FROM x)} -
 *       the full CachedWindowLight compute (buffer + pass1 recordAt + pass2), minimal output. Isolates the
 *       window compute from the outer filter/projection.</li>
 *   <li>{@code window_full}: the SUBSAMPLE window path (window enabled) - compute + Filter(__keep) + project.</li>
 * </ul>
 * {@code window_full - window_compute} approximates the separate Filter+projection cost;
 * {@code window_compute - raw_scan} approximates the window compute overhead over the shared scan floor;
 * comparing both to {@code cursor} shows where the specialization win lives.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class SubsampleWindowDecomposeBenchmark {

    private static final long STEP_MICROS = 1_000L;
    private static final int ROWS = 1_000_000;
    private static final int TARGET = 500;

    @Param({"raw_scan", "cursor", "window_compute", "window_full"})
    public String variant;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private java.nio.file.Path tempRoot;
    private WorkerPool workerPool;
    private boolean projectRows; // true -> read columns per row; false -> count only

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SubsampleWindowDecomposeBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            long count = 0;
            if (projectRows) {
                while (cursor.hasNext()) {
                    bh.consume(record.getTimestamp(0));
                    bh.consume(record.getDouble(1));
                    count++;
                }
            } else {
                while (cursor.hasNext()) {
                    bh.consume(record.getLong(0));
                    count++;
                }
            }
            bh.consume(count);
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final boolean windowEnabled = variant.startsWith("window");
        tempRoot = java.nio.file.Files.createTempDirectory("subsamplewindowdecomposebench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString()) {
            @Override
            public boolean isSubsampleWindowEnabled() {
                return windowEnabled;
            }
        };
        engine = new CairoEngine(configuration);

        final int workers = 4;
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "subsamplewindowdecomposebench";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        WorkerPoolUtils.setupQueryJobs(workerPool, engine);
        workerPool.start();

        ctx = new SqlExecutionContextImpl(engine, workers)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        compiler = new SqlCompilerImpl(engine);
        seedTable();

        final String sql;
        switch (variant) {
            case "raw_scan":
                sql = "SELECT ts, value FROM x";
                projectRows = true;
                break;
            case "cursor":
                sql = "SELECT ts, value FROM x SUBSAMPLE m4(value, " + TARGET + ")";
                projectRows = true;
                break;
            case "window_compute":
                // Full CachedWindowLight compute over all rows, no outer filter/projection.
                sql = "SELECT count(*) FROM (SELECT ts, value, m4(ts, value, " + TARGET
                        + ") OVER (ORDER BY ts) k FROM x)";
                projectRows = false;
                break;
            case "window_full":
                sql = "SELECT ts, value FROM x SUBSAMPLE m4(value, " + TARGET + ")";
                projectRows = true;
                break;
            default:
                throw new IllegalStateException("unknown variant: " + variant);
        }
        factory = compiler.compile(sql, ctx).getRecordCursorFactory();
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
            try (java.util.stream.Stream<java.nio.file.Path> stream = java.nio.file.Files.walk(tempRoot)) {
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

    private void seedTable() throws SqlException {
        engine.execute(
                "CREATE TABLE x (ts TIMESTAMP, value DOUBLE) TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );
        engine.execute(
                "INSERT INTO x SELECT" +
                        " (" + STEP_MICROS + " * x)::timestamp AS ts," +
                        " rnd_double() * 1000.0 AS value" +
                        " FROM long_sequence(" + ROWS + ")",
                ctx
        );
    }
}
