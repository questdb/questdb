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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SqlJitCompilerScalarBenchmark {
    private static final int LONG_COLUMN_SIZE_MB = 512;
    private static final int NUM_ROWS = (LONG_COLUMN_SIZE_MB * 1024 * 1024) / Long.BYTES;

    private static final CairoConfiguration configuration = new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));
    @Param({"AND", "OR"})
    public BoolOperation boolOperation;
    @Param({"SCALAR", "DISABLED"})
    public JitMode jitMode;
    @Param({"EQ", "NEQ"})
    public Predicate predicate;
    private SqlCompilerImpl compiler;
    private RecordCursorFactory countFactory;
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
            try {
                engine.execute(
                        "create table if not exists y as (select" +
                                " rnd_long() i64," +
                                " rnd_int() i32," +
                                " rnd_short() i16," +
                                " timestamp_sequence(400000000000, 500000000) ts" +
                                " from long_sequence(" + NUM_ROWS + ")) timestamp(ts)",
                        sqlExecutionContext
                );
            } catch (SqlException e) {
                e.printStackTrace(System.out);
            }
        }

        Options opt = new OptionsBuilder()
                .include(SqlJitCompilerScalarBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(10)
                .forks(1)
                .build();
        new Runner(opt).run();

        LogFactory.haltInstance();
    }

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = new SqlCompilerImpl(engine);

        boolean jitShouldBeEnabled = false;
        switch (jitMode) {
            case SCALAR:
                ctx.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
                jitShouldBeEnabled = true;
                break;
            case DISABLED:
                ctx.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                break;
        }
        compiler = new SqlCompilerImpl(engine);
        final String boolOp = boolOperation == BoolOperation.AND ? "AND" : "OR";
        final String pred = predicate == Predicate.EQ ? "=" : "!=";
        final String query = "y where i16 " + pred + " 0 " + boolOp + " i32 " + pred + " 0 " + boolOp + " i64 " + pred + " 0;";
        factory = compiler.compile(query, ctx).getRecordCursorFactory();
        if (factory.usesCompiledFilter() != jitShouldBeEnabled) {
            throw new IllegalStateException("Unexpected JIT usage reported by factory: " +
                    "expected=" + jitShouldBeEnabled +
                    ", actual=" + factory.usesCompiledFilter());
        }
        final String countQuery = "select count(*) from " + query;
        countFactory = compiler.compile(countQuery, ctx).getRecordCursorFactory();
        if (countFactory.usesCompiledFilter() != jitShouldBeEnabled) {
            throw new IllegalStateException("Unexpected JIT usage reported by count factory: " +
                    "expected=" + jitShouldBeEnabled +
                    ", actual=" + countFactory.usesCompiledFilter());
        }
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        compiler.close();
        engine.close();
        factory.close();
        countFactory.close();
    }

    @Benchmark
    public long testCountOnlyFilter() throws SqlException {
        long count = 0;
        try (RecordCursor cursor = countFactory.getCursor(ctx)) {
            if (cursor.hasNext()) {
                count = cursor.getRecord().getLong(0);
            }
        }
        return count;
    }

    @Benchmark
    public long testFilter() throws SqlException {
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record ignored = cursor.getRecord();
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    public enum BoolOperation {
        AND, OR
    }

    public enum JitMode {
        SCALAR, DISABLED
    }

    public enum Predicate {
        EQ, NEQ
    }
}
