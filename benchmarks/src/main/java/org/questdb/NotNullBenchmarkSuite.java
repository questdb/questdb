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
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Broad regression suite for NOT NULL across the query hot paths: threshold filter,
 * IS NOT NULL filter, compound filter, SUM aggregation, GROUP BY aggregation, inner
 * join, and INSERT INTO ... SELECT.
 * <p>
 * Each benchmark runs against three nullability scenarios per column type so that
 * the NOT NULL metadata overhead and the cost of actual null values can be
 * separated:
 * <ul>
 *   <li>NOT_NULL — column marked NOT NULL, null checks may be elided</li>
 *   <li>NULLABLE — nullable column with no null data (pure metadata overhead)</li>
 *   <li>NULLABLE_WITH_NULLS — nullable column with ~25% nulls (branch-miss and
 *       selectivity cost)</li>
 * </ul>
 * JIT mode is parameterised only for filter-driven benchmarks; it has no effect on
 * aggregation, join, or insert paths but the extra runs are cheap relative to
 * table setup and let the suite be driven with a single set of params.
 * <p>
 * The insert benchmark drops and recreates its target table inside the
 * {@code @Benchmark} body on purpose so that each invocation measures the full
 * constraint-validation cost of a fresh write.
 * <p>
 * Run selectively with JMH params, e.g.:
 * <pre>
 *   -p a_columnType=LONG -p b_nullability=NOT_NULL,NULLABLE -p c_jitMode=SIMD
 * </pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Timeout(time = 120)
@Fork(1)
public class NotNullBenchmarkSuite {
    private static final int DIM_ROWS = 10_000;
    private static final int FACT_ROWS = 10_000_000;
    private static final int INSERT_ROWS = 200_000;
    private static final CairoConfiguration configuration =
            new DefaultCairoConfiguration(System.getProperty("java.io.tmpdir"));

    @Param({"DOUBLE", "LONG"})
    public ColumnType a_columnType;

    @Param({"NOT_NULL", "NULLABLE", "NULLABLE_WITH_NULLS"})
    public Nullability b_nullability;

    @Param({"SIMD", "SCALAR", "DISABLED"})
    public JitMode c_jitMode;

    private SqlCompilerImpl compiler;
    private RecordCursorFactory compoundFilterFactory;
    private String createInsertTargetSql;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory groupBySumFactory;
    private String insertSql;
    private RecordCursorFactory isNotNullFilterFactory;
    private RecordCursorFactory joinFactory;
    private RecordCursorFactory sumFactory;
    private RecordCursorFactory thresholdFilterFactory;

    public static void main(String[] args) throws RunnerException, CommandLineOptionException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null
                    );
            try {
                createFactTable(engine, ctx);
                createDimTable(engine, ctx);
            } catch (SqlException e) {
                e.printStackTrace(System.out);
            }
        }

        OptionsBuilder builder = new OptionsBuilder();
        if (args.length > 0) {
            builder.parent(new CommandLineOptions(args));
        } else {
            builder.include(NotNullBenchmarkSuite.class.getSimpleName());
        }
        Options opt = builder.build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1);
        ctx.with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null
        );
        compiler = new SqlCompilerImpl(engine);

        boolean shouldBeJitCompiled;
        switch (c_jitMode) {
            case SIMD -> {
                ctx.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                shouldBeJitCompiled = true;
            }
            case SCALAR -> {
                ctx.setJitMode(SqlJitMode.JIT_MODE_FORCE_SCALAR);
                shouldBeJitCompiled = true;
            }
            case DISABLED -> {
                ctx.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                shouldBeJitCompiled = false;
            }
            default -> throw new IllegalStateException("unreachable");
        }

        String valueCol = resolveValueColumn();
        String partnerNullableCol = resolvePartnerNullableColumn();
        String groupCol = resolveGroupColumn();
        String keyCol = resolveKeyColumn();
        String threshold = resolveThreshold();

        thresholdFilterFactory = compiler.compile(
                "SELECT count(*) FROM bench_nn_fact WHERE " + valueCol + " > " + threshold,
                ctx
        ).getRecordCursorFactory();
        assertJit(thresholdFilterFactory, shouldBeJitCompiled, "thresholdFilter");

        isNotNullFilterFactory = compiler.compile(
                "SELECT count(*) FROM bench_nn_fact WHERE " + valueCol + " IS NOT NULL",
                ctx
        ).getRecordCursorFactory();

        compoundFilterFactory = compiler.compile(
                "SELECT count(*) FROM bench_nn_fact WHERE " + valueCol + " > " + threshold +
                        " AND " + partnerNullableCol + " > " + threshold,
                ctx
        ).getRecordCursorFactory();
        assertJit(compoundFilterFactory, shouldBeJitCompiled, "compoundFilter");

        sumFactory = compiler.compile(
                "SELECT sum(" + valueCol + ") FROM bench_nn_fact",
                ctx
        ).getRecordCursorFactory();

        groupBySumFactory = compiler.compile(
                "SELECT " + groupCol + ", sum(" + valueCol + ") FROM bench_nn_fact",
                ctx
        ).getRecordCursorFactory();

        joinFactory = compiler.compile(
                "SELECT count(*) FROM bench_nn_fact f " +
                        "JOIN bench_nn_dim d ON f." + keyCol + " = d.id_notnull",
                ctx
        ).getRecordCursorFactory();

        createInsertTargetSql = buildCreateInsertTargetSql();
        insertSql = buildInsertSql();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws SqlException {
        close(thresholdFilterFactory);
        close(isNotNullFilterFactory);
        close(compoundFilterFactory);
        close(sumFactory);
        close(groupBySumFactory);
        close(joinFactory);
        engine.execute("DROP TABLE IF EXISTS bench_nn_insert", ctx);
        compiler.close();
        engine.close();
    }

    @Benchmark
    public long compoundFilter() throws SqlException {
        return drain(compoundFilterFactory);
    }

    @Benchmark
    public long groupBySum() throws SqlException {
        return drain(groupBySumFactory);
    }

    @Benchmark
    public long innerJoin() throws SqlException {
        return drain(joinFactory);
    }

    @Benchmark
    public void insertSelect() throws SqlException {
        engine.execute("DROP TABLE IF EXISTS bench_nn_insert", ctx);
        engine.execute(createInsertTargetSql, ctx);
        engine.execute(insertSql, ctx);
    }

    @Benchmark
    public long isNotNullFilter() throws SqlException {
        return drain(isNotNullFilterFactory);
    }

    @Benchmark
    public long sumAggregate() throws SqlException {
        return drain(sumFactory);
    }

    @Benchmark
    public long thresholdFilter() throws SqlException {
        return drain(thresholdFilterFactory);
    }

    private static void assertJit(RecordCursorFactory f, boolean shouldBeJitCompiled, String name) {
        if (f.usesCompiledFilter() != shouldBeJitCompiled) {
            throw new IllegalStateException(
                    "Unexpected JIT usage on " + name +
                            ": expected=" + shouldBeJitCompiled +
                            ", actual=" + f.usesCompiledFilter()
            );
        }
    }

    private static void close(RecordCursorFactory f) {
        if (f != null) {
            f.close();
        }
    }

    private static void createDimTable(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("DROP TABLE IF EXISTS bench_nn_dim", ctx);
        engine.execute("""
                CREATE TABLE bench_nn_dim (
                    id_notnull LONG NOT NULL,
                    id_nullable LONG,
                    payload LONG NOT NULL,
                    ts TIMESTAMP NOT NULL
                ) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL""", ctx);
        engine.execute(
                "INSERT INTO bench_nn_dim SELECT " +
                        "x - 1, x - 1, rnd_long(), " +
                        "timestamp_sequence('2024-01-01', 1_000_000L) " +
                        "FROM long_sequence(" + DIM_ROWS + ")",
                ctx
        );
    }

    private static void createFactTable(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("DROP TABLE IF EXISTS bench_nn_fact", ctx);
        engine.execute("""
                CREATE TABLE bench_nn_fact (
                    d_notnull DOUBLE NOT NULL,
                    d_nullable DOUBLE,
                    d_withnulls DOUBLE,
                    l_notnull LONG NOT NULL,
                    l_nullable LONG,
                    l_withnulls LONG,
                    g_notnull INT NOT NULL,
                    g_nullable INT,
                    g_withnulls INT,
                    k_notnull LONG NOT NULL,
                    k_nullable LONG,
                    k_withnulls LONG,
                    ts TIMESTAMP NOT NULL
                ) TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL""", ctx);
        engine.execute(
                "INSERT INTO bench_nn_fact SELECT " +
                        "rnd_double(), rnd_double(), rnd_double(4), " +
                        "rnd_long(0, 1_000_000, 0), rnd_long(0, 1_000_000, 0), rnd_long(0, 1_000_000, 4), " +
                        "rnd_int(0, 100, 0), rnd_int(0, 100, 0), rnd_int(0, 100, 4), " +
                        "rnd_long(0, " + (DIM_ROWS - 1) + ", 0), " +
                        "rnd_long(0, " + (DIM_ROWS - 1) + ", 0), " +
                        "rnd_long(0, " + (DIM_ROWS - 1) + ", 4), " +
                        "timestamp_sequence('2024-01-01', 10_000_000L) " +
                        "FROM long_sequence(" + FACT_ROWS + ")",
                ctx
        );
    }

    private String buildCreateInsertTargetSql() {
        String notNull = b_nullability == Nullability.NOT_NULL ? " NOT NULL" : "";
        String valueDecl = switch (a_columnType) {
            case DOUBLE -> "v DOUBLE" + notNull;
            case LONG -> "v LONG" + notNull;
        };
        return "CREATE TABLE bench_nn_insert (" + valueDecl +
                ", ts TIMESTAMP NOT NULL" +
                ") TIMESTAMP(ts) PARTITION BY YEAR BYPASS WAL";
    }

    private String buildInsertSql() {
        String source = switch (a_columnType) {
            case DOUBLE -> b_nullability == Nullability.NULLABLE_WITH_NULLS
                    ? "rnd_double(4)" : "rnd_double()";
            case LONG -> b_nullability == Nullability.NULLABLE_WITH_NULLS
                    ? "rnd_long(0, 1_000_000, 4)" : "rnd_long(0, 1_000_000, 0)";
        };
        return "INSERT INTO bench_nn_insert SELECT " + source +
                ", timestamp_sequence('2024-01-01', 100_000L) " +
                "FROM long_sequence(" + INSERT_ROWS + ")";
    }

    private long drain(RecordCursorFactory f) throws SqlException {
        long count = 0;
        try (RecordCursor cursor = f.getCursor(ctx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    private String resolveGroupColumn() {
        return switch (b_nullability) {
            case NOT_NULL -> "g_notnull";
            case NULLABLE -> "g_nullable";
            case NULLABLE_WITH_NULLS -> "g_withnulls";
        };
    }

    private String resolveKeyColumn() {
        return switch (b_nullability) {
            case NOT_NULL -> "k_notnull";
            case NULLABLE -> "k_nullable";
            case NULLABLE_WITH_NULLS -> "k_withnulls";
        };
    }

    private String resolvePartnerNullableColumn() {
        return switch (a_columnType) {
            case DOUBLE -> "d_withnulls";
            case LONG -> "l_withnulls";
        };
    }

    private String resolveThreshold() {
        return switch (a_columnType) {
            case DOUBLE -> "0.5";
            case LONG -> "500_000";
        };
    }

    private String resolveValueColumn() {
        return switch (a_columnType) {
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
