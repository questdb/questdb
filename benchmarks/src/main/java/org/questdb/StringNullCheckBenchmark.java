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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
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
import org.openjdk.jmh.annotations.Timeout;
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
    private RecordCursorFactory stringFilterFactory;
    private RecordCursorFactory varcharFilterFactory;

    public static void main(String[] args) throws RunnerException {
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
            engine.execute("drop table if exists x", sqlExecutionContext);
            engine.execute(
                    "create table x as (select" +
                            " rnd_str(70, 140, " + a_nullFreq + ") string_value," +
                            " rnd_varchar(70, 140, " + a_nullFreq + ") varchar_value," +
                            " rnd_long(1, 10, 0) long_value1," +
                            " rnd_long(1, 10, 0) long_value2," +
                            " timestamp_sequence(to_timestamp('2024-02-04', 'yyyy-MM-dd'), 100000L) ts" +
                            " from long_sequence(" + NUM_ROWS + ")) timestamp(ts) partition by year bypass wal",
                    sqlExecutionContext
            );
        }
    }

    @Benchmark
    public void selectCountWhereStringColIsNull() throws SqlException {
        try (RecordCursor cursor = stringFilterFactory.getCursor(ctx)) {
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            blackHole.consume(count);
        }
    }

    @Benchmark
    public void selectCountWhereVarcharColIsNull() throws SqlException {
        try (RecordCursor cursor = varcharFilterFactory.getCursor(ctx)) {
            long count = 0;
            while (cursor.hasNext()) {
                count++;
            }
            blackHole.consume(count);
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
        stringFilterFactory = compiler.compile("select count(*) from x where " +
                (b_useComplexFilter ? "long_value1 > 0 and long_value2 > 0 and" : "") +
                " string_value is null", ctx).getRecordCursorFactory();
        if (stringFilterFactory.usesCompiledFilter() != shouldBeJitCompiled) {
            throw new IllegalStateException("Unexpected JIT usage reported by factory: " +
                    "expected=" + shouldBeJitCompiled +
                    ", actual=" + stringFilterFactory.usesCompiledFilter());
        }
        varcharFilterFactory = compiler.compile("select count(*) from x where " +
                (b_useComplexFilter ? "long_value1 > 0 and long_value2 > 0 and" : "") +
                " varchar_value is null", ctx).getRecordCursorFactory();
        if (varcharFilterFactory.usesCompiledFilter() != shouldBeJitCompiled) {
            throw new IllegalStateException("Unexpected JIT usage reported by factory: " +
                    "expected=" + shouldBeJitCompiled +
                    ", actual=" + varcharFilterFactory.usesCompiledFilter());
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        stringFilterFactory.close();
        varcharFilterFactory.close();
        compiler.close();
        engine.close();
    }

    public enum JitMode {
        SIMD, SCALAR, DISABLED
    }
}
