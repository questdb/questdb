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
import io.questdb.mp.WorkerPool;
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

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@Fork(1)
public class StringAggParallelBenchmark {

    private static final int ROW_COUNT = 1_000_000;

    @Param({"10", "10000"})
    public int groupCount;

    @Param({"1", "2", "4", "8"})
    public int workers;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private WorkerPool pool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StringAggParallelBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bh.consume(record.getSymA(0));
                final CharSequence s = record.getStrA(1);
                bh.consume(s == null ? 0 : s.length());
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(
                java.nio.file.Files.createTempDirectory("stringaggbench-").toString()
        ) {
            @Override
            public int getStrFunctionMaxBufferLength() {
                return Integer.MAX_VALUE;
            }
        };
        engine = new CairoEngine(configuration);
        if (workers > 1) {
            pool = new WorkerPool(() -> workers);
            WorkerPoolUtils.setupQueryJobs(pool, engine);
            pool.start();
        }
        ctx = new SqlExecutionContextImpl(engine, workers)
                .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        ctx.setParallelGroupByEnabled(workers > 1);
        compiler = new SqlCompilerImpl(engine);
        engine.execute("CREATE TABLE tab (k SYMBOL, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        engine.execute(
                "INSERT INTO tab SELECT (x % " + groupCount + ")::SYMBOL, rnd_str(6, 10, 0), " +
                        "timestamp_sequence(0, 100000) FROM long_sequence(" + ROW_COUNT + ")",
                ctx
        );
        factory = compiler.compile("SELECT k, string_agg(s, ',') FROM tab", ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        if (pool != null) {
            pool.halt();
            pool = null;
        }
        engine = Misc.free(engine);
    }
}
