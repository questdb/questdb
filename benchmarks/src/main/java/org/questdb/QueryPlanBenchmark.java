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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
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
 * Time to read the rows of queries whose plan depends on the planner, with one worker. Each operation opens a cursor over
 * the compiled query and reads every row. All SQL also compiles on a build with the previous planner, so the class runs
 * unchanged against a base commit.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2, jvmArgsAppend = {
        "-Xms4g",
        "-Xmx4g",
        "-XX:+UseParallelGC",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.time.zone=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
})
public class QueryPlanBenchmark {
    private static final String KEY_IN = "s IN ('k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9')";
    @Param({
            "key_order",
            "key_order_filter",
            "key_order_cross_join",
            "key_order_left_join",
            "unread_column_filter",
            "unread_column_aggregate",
            "unread_column_top_k",
            "unread_column_asof",
            "unread_column_window_join",
            "unread_column_hash_join",
            "endpoint_over_sub_query",
            "negative_limit_no_match"
    })
    public String query;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QueryPlanBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long read() throws SqlException {
        long rows = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                rows += record == null ? 0 : 1;
            }
        }
        return rows;
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        root = Files.createTempDirectory("query-plan-bench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        engine.load();
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                new BindVariableServiceImpl(configuration),
                null,
                -1,
                null
        );
        engine.execute("""
                CREATE TABLE t AS (
                    SELECT x a, x * 2 b, rnd_symbol(100, 4, 4, 0) s, timestamp_sequence(0, 10_000) ts FROM long_sequence(2_000_000)
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""", ctx);
        engine.execute("""
                CREATE TABLE p AS (
                    SELECT x::DOUBLE x, x a, -x b, rnd_symbol(100, 4, 4, 0) s, timestamp_sequence(0, 20_000) ts FROM long_sequence(1_000_000)
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""", ctx);
        engine.execute("""
                CREATE TABLE k AS (
                    SELECT x id, ('k' || (x % 10))::SYMBOL s, rpad('w', 64, 'w')::VARCHAR v, timestamp_sequence(0, 100) ts FROM long_sequence(500_000)
                ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL""", ctx);
        engine.execute("INSERT INTO k VALUES (500_001, 'k0', 'w', '1970-01-03')", ctx);
        engine.execute("ALTER TABLE k CONVERT PARTITION TO PARQUET WHERE ts < '1970-01-03'", ctx);
        final String keys = "SELECT id, s, v, ts FROM k WHERE ts IN '1970-01-01' AND " + KEY_IN;
        final String sql = switch (query) {
            case "key_order" -> keys + " ORDER BY s, ts";
            case "key_order_filter" -> keys + " AND id % 3 = 0 ORDER BY s, ts";
            case "key_order_cross_join" ->
                    "SELECT m.x, t.id, t.v FROM long_sequence(5) m CROSS JOIN (" + keys + " AND id % 3 = 0 ORDER BY s, ts) t";
            case "key_order_left_join" ->
                    "SELECT m.x, t.id, t.v FROM long_sequence(5) m LEFT JOIN (" + keys + " AND id % 3 = 0 ORDER BY s, ts) t ON m.x > 0";
            case "unread_column_filter" -> "SELECT a FROM (SELECT a, ts, a + 1 c FROM t) WHERE a > 5";
            case "unread_column_aggregate" -> "SELECT sum(a) FROM (SELECT a, ts, a + 1 c FROM t) WHERE a > 5";
            case "unread_column_top_k" -> "SELECT a FROM (SELECT a, a + 1 c FROM t) ORDER BY a DESC LIMIT 3";
            case "unread_column_asof" ->
                    "SELECT m.a, n.x FROM (SELECT a, ts, a + 1 c FROM t) m ASOF JOIN (SELECT x, ts, x + 1 c FROM p) n";
            case "unread_column_window_join" -> """
                    SELECT m.ts, sum(p.x) FROM (SELECT ts, s, a + 1 c FROM t) m WINDOW JOIN p ON (m.s = p.s)
                    RANGE BETWEEN 1 SECOND PRECEDING AND 1 SECOND FOLLOWING EXCLUDE PREVAILING""";
            case "unread_column_hash_join" ->
                    "SELECT count(*) n, sum(j.x) d, max(r.ts) m FROM (SELECT a, s, dateadd('d', -1, ts) ts FROM t) r JOIN p j ON r.a = j.a WHERE r.ts IN '1969-12-31'";
            case "endpoint_over_sub_query" -> "SELECT max(ts) FROM (SELECT * FROM t WHERE a > 5)";
            case "negative_limit_no_match" -> "SELECT m.a, n.x FROM t m JOIN p n ON m.a = n.b LIMIT -10";
            default -> throw new IllegalArgumentException("unknown query: " + query);
        };
        factory = engine.select(sql, ctx);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        factory.close();
        engine.close();
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }
}
