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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SqlJitCompilerBenchmark {

    private static final int PARTITION_SIZE_MB = 512;
    private static final int NUM_ROWS = (PARTITION_SIZE_MB * 1024 * 1024) / Long.BYTES;

    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    @Param({"i64", "i32"})
    public String column;
    @Param({"SIMD", "SCALAR", "DISABLED"})
    public JitMode jitMode;
    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;

    public static void main(String[] args) throws RunnerException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.compile("create table if not exists x as (select" +
                        " rnd_long() i64," +
                        " rnd_int() i32," +
                        " timestamp_sequence(400000000000, 500000000) ts" +
                        " from long_sequence(" + NUM_ROWS + ")) timestamp(ts)", sqlExecutionContext);
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }

        Options opt = new OptionsBuilder()
                .include(SqlJitCompilerBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();

        LogFactory.haltInstance();
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1);
        compiler = new SqlCompilerImpl(engine);

        boolean jitShouldBeEnabled = false;
        switch (jitMode) {
            case SIMD:
                ctx.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                jitShouldBeEnabled = true;
                break;
            case SCALAR:
                ctx.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
                jitShouldBeEnabled = true;
                break;
            case DISABLED:
                ctx.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                break;
        }
        compiler = new SqlCompilerImpl(engine);
        factory = compiler.compile("select * from x where " + column + " = 0", ctx).getRecordCursorFactory();
        if (factory.usesCompiledFilter() != jitShouldBeEnabled) {
            throw new IllegalStateException("Unexpected JIT usage reported by factory: " +
                    "expected=" + jitShouldBeEnabled +
                    ", actual=" + factory.usesCompiledFilter());
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        compiler.close();
        engine.close();
        factory.close();
    }

    @Benchmark
    public void testSingleColumnFilter() throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record ignored = cursor.getRecord();
            // noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
                // access 'record' instance for field values
            }
        }
    }

    public enum JitMode {
        SIMD, SCALAR, DISABLED
    }
}
