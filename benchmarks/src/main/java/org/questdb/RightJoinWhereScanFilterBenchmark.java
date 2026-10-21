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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.SqlExecutionContextImpl;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Measures a WHERE on the first table of "t0 CROSS JOIN t1 RIGHT JOIN t2". The RIGHT join NULL-extends
 * t0, so the WHERE filters after the join; when one of its conjuncts is false on the NULL row of t0,
 * SqlOptimiser also filters the scan of t0 with copies of the conjuncts. t0 holds 1M rows over ten days;
 * t1 and t2 hold {@code joinRows} rows each, and every t2 row matches. The shapes ts_ge_le_bind and
 * c_eq_bind accept NULL for a NULL value, so they keep the post-join filter only.
 * <p>
 * Run against the baseline and the candidate build:
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests
 * java -cp benchmarks/target/benchmarks.jar org.questdb.RightJoinWhereScanFilterBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class RightJoinWhereScanFilterBenchmark {
    private static final String JOINS = " FROM t0 CROSS JOIN t1 RIGHT JOIN t2 ON t2.k = t1.k";
    @Param({"10", "100"})
    public int joinRows;
    @Param({"c_eq", "ts_in_day", "ts_between", "c_regex", "keyed_prefix", "ts_gt_lt_bind", "ts_ge_lt_bind", "ts_ge_le_bind", "c_eq_bind"})
    public String shape;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(RightJoinWhereScanFilterBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long count() throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("right-join-where-scan-filter-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean shouldLogSql() {
                return false;
            }
        };
        context.with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                new BindVariableServiceImpl(configuration),
                null,
                -1,
                null
        );
        engine.execute("CREATE TABLE t0 AS (SELECT (x % 1_000)::INT c, (x % " + joinRows + ")::INT k,"
                + " timestamp_sequence('2024-01-01', 864_000) ts FROM long_sequence(1_000_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("CREATE TABLE t1 AS (SELECT (x - 1)::INT k FROM long_sequence(" + joinRows + "))", context);
        engine.execute("CREATE TABLE t2 AS (SELECT (x - 1)::INT k FROM long_sequence(" + joinRows + "))", context);
        context.getBindVariableService().clear();
        // 2024-01-05 and 2024-01-06
        context.getBindVariableService().setTimestamp(0, 1_704_412_800_000_000L);
        context.getBindVariableService().setTimestamp(1, 1_704_499_200_000_000L);
        final String sql = switch (shape) {
            case "c_eq" -> "SELECT count(*)" + JOINS + " WHERE t0.c = 1";
            case "ts_in_day" -> "SELECT count(*)" + JOINS + " WHERE t0.ts IN '2024-01-05'";
            case "ts_between" -> "SELECT count(*)" + JOINS + " WHERE t0.ts BETWEEN '2024-01-05' AND '2024-01-05T23:59:59'";
            case "c_regex" -> "SELECT count(*)" + JOINS + " WHERE t0.c::string ~ '^1$'";
            case "keyed_prefix" -> "SELECT count(*) FROM t0 JOIN t1 ON t1.k = t0.k RIGHT JOIN t2 ON t2.k = t0.k WHERE t0.c = 1";
            case "ts_gt_lt_bind" -> "SELECT count(*)" + JOINS + " WHERE t0.ts > $1 AND t0.ts < $2";
            case "ts_ge_lt_bind" -> "SELECT count(*)" + JOINS + " WHERE t0.ts >= $1 AND t0.ts < $2";
            case "ts_ge_le_bind" -> "SELECT count(*)" + JOINS + " WHERE t0.ts >= $1 AND t0.ts <= $2";
            case "c_eq_bind" -> {
                context.getBindVariableService().clear();
                context.getBindVariableService().setInt(0, 1);
                yield "SELECT count(*)" + JOINS + " WHERE t0.c = $1";
            }
            default -> throw new IllegalArgumentException(shape);
        };
        factory = engine.select(sql, context);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        factory = Misc.free(factory);
        context = Misc.free(context);
        engine = Misc.free(engine);
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
