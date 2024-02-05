/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 2)
@Measurement(iterations = 5, time = 1)
@Timeout(time = 5)
@Fork(1)
public class StringNullCheckBenchmark {

    private static final int NUM_ROWS = 10_000_000;
    private static final CairoConfiguration configuration =
            new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));

    @Param({"0", "1", "3", "100"})
    public int a_nullFreq;
    @Param({"false", "true"})
    public boolean b_useComplexFilter;
    @Param({"SIMD", "SCALAR", "DISABLED"})
    public JitMode c_jitMode;

    private Blackhole blackHole;
    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;

    public static void main(String[] args) throws RunnerException, SqlException {
        Options opt = new OptionsBuilder()
                .include(StringNullCheckBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void globalSetup() throws Exception {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext sqlExecutionContext =
                    new SqlExecutionContextImpl(engine, 8)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                compiler.compile("drop table if exists x", sqlExecutionContext);
                compiler.compile("create table x as (select" +
                        (a_nullFreq != 0
                                ? " rnd_str(100, 70, 140, " + a_nullFreq + ") string_value,"
                                : " rnd_str('', 'a', 'aa') string_value,") +
                        " rnd_long(1, 10, 0) long_value1," +
                        " rnd_long(1, 10, 0) long_value2," +
                        " timestamp_sequence(to_timestamp('2024-02-04', 'yyyy-MM-dd'), 100000L) ts" +
                        " from long_sequence(" + NUM_ROWS + ")) timestamp(ts)", sqlExecutionContext);
            }
        }
    }

    @Setup(Level.Iteration)
    public void setup(Blackhole blackHole) throws Exception {
        this.blackHole = blackHole;
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1);
        compiler = new SqlCompilerImpl(engine);

        boolean shouldBeJitCompiled;
        switch (c_jitMode) {
            case SIMD:
                ctx.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                shouldBeJitCompiled = true;
                break;
            case SCALAR:
                ctx.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
                shouldBeJitCompiled = true;
                break;
            case DISABLED:
                ctx.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                shouldBeJitCompiled = false;
                break;
            default:
                throw new RuntimeException("unreachable");
        }
        compiler = new SqlCompilerImpl(engine);
        factory = compiler.compile("select count(*) from x where " +
                (b_useComplexFilter ? "long_value1 > 0 and long_value2 > 0 and" : "") +
                " string_value is null", ctx).getRecordCursorFactory();
        if (factory.usesCompiledFilter() != shouldBeJitCompiled) {
            throw new IllegalStateException("Unexpected JIT usage reported by factory: " +
                    "expected=" + shouldBeJitCompiled +
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
    public void selectCountWhereVarlenColIsNull() throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            blackHole.consume(count);
        }
    }

    public enum JitMode {
        SIMD, SCALAR, DISABLED
    }
}
