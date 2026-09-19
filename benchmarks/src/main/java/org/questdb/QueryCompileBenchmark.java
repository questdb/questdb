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
import io.questdb.cairo.mv.MatViewRefreshJob;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.view.ViewCompilerJob;
import io.questdb.cairo.wal.ApplyWal2TableJob;
import io.questdb.cairo.wal.CheckWalTransactionsJob;
import io.questdb.griffin.SqlCompiler;
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
 * Compile time of queries that read no audited view: a table, a table with {@code DECLARE}, a
 * view, a view whose declared variable the caller overrides, and a materialized view. Each
 * operation compiles the query text and closes the factory without opening a cursor, so it covers
 * the lexer, parser, optimiser and code generation, and nothing else.
 * <p>
 * Every query also compiles on a build that predates audited views, so the class runs unchanged
 * against a base commit, to show that the feature costs such queries nothing. Run it with
 * {@code -prof gc} as well: the allocation per compile is exact where the time is not.
 * The compile path does not depend on the row count, so the tables hold a few rows only.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 3, jvmArgsAppend = {
        "-Xms2g",
        "-Xmx2g",
        "-XX:+UseParallelGC",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.time.zone=ALL-UNNAMED",
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"
})
public class QueryCompileBenchmark {
    @Param({"table", "declare", "view", "view_declare", "mat_view"})
    public String query;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private Path root;
    private String sql;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QueryCompileBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public int compile() throws SqlException {
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory()) {
            return factory.getMetadata().getColumnCount();
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        root = Files.createTempDirectory("query-compile-bench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        // swaps the no-op mat view state store for the real one, so that the mat view refreshes
        engine.load();
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                new BindVariableServiceImpl(configuration),
                null,
                -1,
                null
        );
        compiler = engine.getSqlCompiler();

        engine.execute("CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL", ctx);
        engine.execute("""
                INSERT INTO trades VALUES
                    ('2024-01-01T00:00:00.000000Z', 'AAPL', 100.0),
                    ('2024-01-01T01:00:00.000000Z', 'MSFT', 200.0),
                    ('2024-01-02T00:00:00.000000Z', 'AAPL', 101.0)""", ctx);
        drainWal(engine);
        engine.execute("CREATE VIEW v_trades AS (SELECT ts, sym, price FROM trades WHERE sym = 'AAPL')", ctx);
        engine.execute("""
                CREATE VIEW v_trades_sym AS (
                    DECLARE OVERRIDABLE @sym := 'AAPL'
                    SELECT ts, sym, price FROM trades WHERE sym = @sym
                )""", ctx);
        engine.execute("""
                CREATE MATERIALIZED VIEW mv_trades AS (
                    SELECT ts, sym, last(price) price FROM trades SAMPLE BY 1h
                ) PARTITION BY DAY""", ctx);
        drainViews(engine);
        drainWal(engine);
        drainMatViews(engine);
        drainWal(engine);

        sql = switch (query) {
            case "table" -> "SELECT ts, sym, price FROM trades WHERE sym = 'AAPL' AND price > 10";
            case "declare" -> "DECLARE @sym := 'AAPL' SELECT ts, sym, price FROM trades WHERE sym = @sym AND price > 10";
            case "view" -> "SELECT ts, sym, price FROM v_trades WHERE price > 10";
            case "view_declare" -> "DECLARE @sym := 'MSFT' SELECT ts, sym, price FROM v_trades_sym WHERE price > 10";
            case "mat_view" -> "SELECT ts, sym, price FROM mv_trades WHERE sym = 'AAPL' AND price > 10";
            default -> throw new IllegalArgumentException("unknown query: " + query);
        };
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        compiler.close();
        engine.close();
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
        }
    }

    private static void drainMatViews(CairoEngine engine) {
        try (MatViewRefreshJob refreshJob = new MatViewRefreshJob(0, engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (refreshJob.run()) ;
        }
    }

    private static void drainViews(CairoEngine engine) {
        try (ViewCompilerJob compilerJob = new ViewCompilerJob(engine, 1)) {
            //noinspection StatementWithEmptyBody
            while (compilerJob.run()) ;
        }
    }

    private static void drainWal(CairoEngine engine) {
        try (ApplyWal2TableJob walApplyJob = new ApplyWal2TableJob(engine, 0)) {
            //noinspection StatementWithEmptyBody
            while (walApplyJob.run()) ;
            if (new CheckWalTransactionsJob(engine).run()) {
                //noinspection StatementWithEmptyBody
                while (walApplyJob.run()) ;
            }
        }
    }
}
