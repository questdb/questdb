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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
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
public class SqlJitCompilerWideningBenchmark {
    private static final int DEFAULT_NUM_ROWS = 64 * 1024 * 1024;
    private static final int NUM_ROWS = Integer.getInteger("questdb.widening.benchmark.rows", DEFAULT_NUM_ROWS);
    private static final String TABLE_NAME = "jit_widening_bench_" + NUM_ROWS;
    private static final CairoConfiguration CONFIGURATION =
            new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));

    @Param({"WIDENED", "WIDENED_SELECTIVE", "ORDINARY"})
    public Predicate predicate;
    @Param({"SIMD", "SCALAR", "DISABLED"})
    public JitMode jitMode;

    private SqlCompilerImpl compiler;
    private RecordCursorFactory countFactory;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory rowIdFactory;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SqlJitCompilerWideningBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(10)
                .forks(1)
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        engine = new CairoEngine(CONFIGURATION);
        ctx = newExecutionContext();
        compiler = new SqlCompilerImpl(engine);

        engine.execute(
                "create table if not exists " + TABLE_NAME + " as (select" +
                        " ((x % 2_000_001) - 1_000_000)::int i32," +
                        " timestamp_sequence(400_000_000_000, 500_000_000) ts" +
                        " from long_sequence(" + NUM_ROWS + ")) timestamp(ts)",
                ctx
        );

        final String filter = predicate.filter;
        final String rowIdQuery = "select i32 from " + TABLE_NAME + " where " + filter;
        final String countQuery = "select count(*) from " + TABLE_NAME + " where " + filter;
        final long expectedCount = javaFilterCount(countQuery);

        final boolean jitShouldBeEnabled;
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
                jitShouldBeEnabled = false;
                break;
            default:
                throw new IllegalStateException("unsupported JIT mode: " + jitMode);
        }

        rowIdFactory = compiler.compile(rowIdQuery, ctx).getRecordCursorFactory();
        assertJitUsage("row-id", rowIdFactory, jitShouldBeEnabled);
        countFactory = compiler.compile(countQuery, ctx).getRecordCursorFactory();
        assertJitUsage("count-only", countFactory, jitShouldBeEnabled);

        final long rowIdCount = countRows(rowIdFactory, ctx);
        final long countOnlyCount = readCount(countFactory, ctx);
        if (rowIdCount != expectedCount || countOnlyCount != expectedCount) {
            throw new IllegalStateException(
                    "filter result differs from Java filter [predicate=" + predicate +
                            ", jitMode=" + jitMode +
                            ", expected=" + expectedCount +
                            ", rowId=" + rowIdCount +
                            ", countOnly=" + countOnlyCount + ']'
            );
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        rowIdFactory.close();
        countFactory.close();
        compiler.close();
        engine.close();
    }

    @Benchmark
    public long testCountOnlyFilter() throws Exception {
        return readCount(countFactory, ctx);
    }

    @Benchmark
    public long testRowIdFilter() throws Exception {
        return countRows(rowIdFactory, ctx);
    }

    private static void assertJitUsage(String path, RecordCursorFactory factory, boolean expected) {
        if (factory.usesCompiledFilter() != expected) {
            throw new IllegalStateException(
                    "unexpected JIT usage [path=" + path +
                            ", expected=" + expected +
                            ", actual=" + factory.usesCompiledFilter() + ']'
            );
        }
    }

    private static long countRows(RecordCursorFactory factory, SqlExecutionContextImpl executionContext) throws Exception {
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    private static long readCount(RecordCursorFactory factory, SqlExecutionContextImpl executionContext) throws Exception {
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            if (cursor.hasNext()) {
                return cursor.getRecord().getLong(0);
            }
        }
        throw new IllegalStateException("count query returned no rows");
    }

    private SqlExecutionContextImpl newExecutionContext() {
        return new SqlExecutionContextImpl(engine, 1).with(
                CONFIGURATION.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
    }

    private long javaFilterCount(String countQuery) throws Exception {
        final SqlExecutionContextImpl javaContext = newExecutionContext();
        javaContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        try (RecordCursorFactory javaFactory = compiler.compile(countQuery, javaContext).getRecordCursorFactory()) {
            if (javaFactory.usesCompiledFilter()) {
                throw new IllegalStateException("Java-filter oracle unexpectedly uses a compiled filter");
            }
            return readCount(javaFactory, javaContext);
        }
    }

    public enum JitMode {
        SIMD,
        SCALAR,
        DISABLED
    }

    public enum Predicate {
        // Direct INT-to-LONG comparison: four-lane AVX2 in SIMD mode.
        WIDENED("i32 < 5_000_000_000"),
        // Same widening plus a selective ordinary INT conjunct; still four-lane AVX2.
        WIDENED_SELECTIVE("i32 < 5_000_000_000 and i32 > 0"),
        // Eight-lane AVX2 throughput control with the same result as WIDENED_SELECTIVE.
        ORDINARY("i32 > 0");

        private final String filter;

        Predicate(String filter) {
            this.filter = filter;
        }
    }
}
