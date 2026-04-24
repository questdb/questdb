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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Measures the overhead of null-sentinel checks in JIT-compiled and Java-interpreted
 * filters for DOUBLE and LONG columns.
 * <p>
 * Compares three nullability scenarios per type:
 * <ul>
 *   <li>NOT_NULL — column marked NOT NULL, JIT omits null checks (bit 6 = 0)</li>
 *   <li>NULLABLE — nullable column with no null data, JIT emits null checks</li>
 *   <li>NULLABLE_WITH_NULLS — nullable column with ~25% nulls</li>
 * </ul>
 * NOT_NULL vs NULLABLE (same data, different metadata) isolates the pure instruction
 * overhead of the sentinel check. NULLABLE_WITH_NULLS shows the additional cost of
 * branch misprediction and reduced selectivity from actual null values.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Timeout(time = 30)
@Fork(1)
public class NotNullFilterBenchmark {
    private static final int NUM_ROWS = 10_000_000;
    private static final CairoConfiguration configuration =
            new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));

    @Param({"DOUBLE", "LONG"})
    public ColumnType a_columnType;

    @Param({"NOT_NULL", "NULLABLE", "NULLABLE_WITH_NULLS"})
    public Nullability b_nullability;

    @Param({"SIMD", "SCALAR", "DISABLED"})
    public JitMode c_jitMode;

    private CairoEngine engine;
    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private RecordCursorFactory factory;

    public static void main(String[] args) throws RunnerException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null
                    );
            try {
                engine.execute("DROP TABLE IF EXISTS bench_nn", ctx);
                engine.execute(
                        "CREATE TABLE bench_nn (" +
                                "d_notnull DOUBLE NOT NULL, " +
                                "d_nullable DOUBLE, " +
                                "d_withnulls DOUBLE, " +
                                "l_notnull LONG NOT NULL, " +
                                "l_nullable LONG, " +
                                "l_withnulls LONG, " +
                                "ts TIMESTAMP NOT NULL" +
                                ") TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL",
                        ctx
                );
                engine.execute(
                        "INSERT INTO bench_nn SELECT " +
                                "rnd_double(), " +
                                "rnd_double(), " +
                                "rnd_double(4), " +
                                "rnd_long(0, 1_000_000, 0), " +
                                "rnd_long(0, 1_000_000, 0), " +
                                "rnd_long(0, 1_000_000, 4), " +
                                "timestamp_sequence('2024-01-01', 10_000_000L) " +
                                "FROM long_sequence(" + NUM_ROWS + ")",
                        ctx
                );
            } catch (SqlException e) {
                e.printStackTrace(System.out);
            }
        }

        Options opt = new OptionsBuilder()
                .include(NotNullFilterBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1);
        compiler = new SqlCompilerImpl(engine);

        switch (c_jitMode) {
            case SIMD -> ctx.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
            case SCALAR -> ctx.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
            case DISABLED -> ctx.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        }

        String col = switch (a_columnType) {
            case DOUBLE -> switch (b_nullability) {
                case NOT_NULL -> "d_notnull";
                case NULLABLE -> "d_nullable";
                case NULLABLE_WITH_NULLS -> "d_withnulls";
            };
            case LONG -> switch (b_nullability) {
                case NOT_NULL -> "l_notnull";
                case NULLABLE -> "l_nullable";
                case NULLABLE_WITH_NULLS -> "l_withnulls";
            };
        };

        String threshold = switch (a_columnType) {
            case DOUBLE -> "0.5";
            case LONG -> "500_000";
        };

        factory = compiler.compile(
                "bench_nn WHERE " + col + " > " + threshold, ctx
        ).getRecordCursorFactory();

        boolean shouldBeJitCompiled = c_jitMode != JitMode.DISABLED;
        if (factory.usesCompiledFilter() != shouldBeJitCompiled) {
            throw new IllegalStateException(
                    "Unexpected JIT usage: expected=" + shouldBeJitCompiled +
                            ", actual=" + factory.usesCompiledFilter()
            );
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory.close();
        compiler.close();
        engine.close();
    }

    @Benchmark
    public long filter() throws SqlException {
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    public enum ColumnType {
        DOUBLE, LONG
    }

    public enum JitMode {
        SIMD, SCALAR, DISABLED
    }

    public enum Nullability {
        NOT_NULL, NULLABLE, NULLABLE_WITH_NULLS
    }
}
