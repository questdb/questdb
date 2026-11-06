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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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

import java.util.concurrent.TimeUnit;

/**
 * Covering-index read paths that share cursor code, measured in one JVM: a scan feeds the forward
 * cursor through its call sites, and a covering LATEST ON feeds the backward cursor through its own.
 * {@code otherQueriesEvery} runs all three queries before every Nth invocation, outside the
 * measurement; 0 keeps the fork on the measured query only, as a control.
 * <p>
 * Scan and filtered LATEST ON report ns per scanned row; the unfiltered LATEST ON reports ns per query.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class CoveringScanLatestOnBenchmark {
    private static final int KEY_COUNT = 4;
    private static final int PARTITION_COUNT = 4;
    private static final int ROWS_PER_KEY = 400_000;
    private static final String SQL_FILTERED_LATEST = "SELECT price, ts FROM t WHERE sym = 'k1' AND price < 0 LATEST ON ts PARTITION BY sym";
    private static final String SQL_LATEST = "SELECT price, ts FROM t WHERE sym = 'k1' LATEST ON ts PARTITION BY sym";
    private static final String SQL_SCAN = "SELECT price FROM t WHERE sym = 'k1'";
    @Param({"0", "100"})
    public int otherQueriesEvery;
    private SqlCompiler compiler;
    private SqlExecutionContext context;
    private CairoEngine engine;
    private RecordCursorFactory filteredLatestFactory;
    private long invocations;
    private RecordCursorFactory latestFactory;
    private String root;
    private RecordCursorFactory scanFactory;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CoveringScanLatestOnBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    @OperationsPerInvocation(ROWS_PER_KEY)
    public double filteredLatestOn() throws SqlException {
        return drain(filteredLatestFactory);
    }

    @Benchmark
    public double latestOn() throws SqlException {
        return drain(latestFactory);
    }

    @Setup(Level.Invocation)
    public void runOtherQueries() throws SqlException {
        if (otherQueriesEvery > 0 && invocations++ % otherQueriesEvery == 0) {
            drain(scanFactory);
            drain(latestFactory);
            drain(filteredLatestFactory);
        }
    }

    @Benchmark
    @OperationsPerInvocation(ROWS_PER_KEY)
    public double scan() throws SqlException {
        return drain(scanFactory);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        root = java.nio.file.Files.createTempDirectory("covering-scan-latest-on").toString();
        engine = new CairoEngine(new DefaultCairoConfiguration(root));
        context = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean shouldLogSql() {
                return false;
            }
        }.with(
                engine.getConfiguration().getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        engine.execute("""
                CREATE TABLE t (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING INCLUDE (price),
                    price DOUBLE
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """, context);
        final long rows = (long) KEY_COUNT * ROWS_PER_KEY;
        final long stepMicros = PARTITION_COUNT * 86_400_000_000L / rows;
        engine.execute(
                "INSERT INTO t SELECT ((x - 1) * " + stepMicros + ")::TIMESTAMP, 'k' || (x % " + KEY_COUNT + "), x::DOUBLE"
                        + " FROM long_sequence(" + rows + ")",
                context
        );
        compiler = engine.getSqlCompiler();
        scanFactory = compileCovering(SQL_SCAN);
        latestFactory = compileCovering(SQL_LATEST);
        filteredLatestFactory = compileCovering(SQL_FILTERED_LATEST);
        LogFactory.haltInstance();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        Misc.free(scanFactory);
        Misc.free(latestFactory);
        Misc.free(filteredLatestFactory);
        Misc.free(compiler);
        Misc.free(context);
        Misc.free(engine);
        try (Path path = new Path()) {
            Files.rmdir(path.of(root).slash(), true);
        }
    }

    private RecordCursorFactory compileCovering(String sql) throws SqlException {
        boolean isCovering = false;
        try (
                RecordCursorFactory plan = compiler.compile("EXPLAIN " + sql, context).getRecordCursorFactory();
                RecordCursor cursor = plan.getCursor(context)
        ) {
            while (cursor.hasNext()) {
                isCovering |= cursor.getRecord().getStrA(0).toString().contains("CoveringIndex");
            }
        }
        if (!isCovering) {
            throw new IllegalStateException("not a covering plan [sql=" + sql + ']');
        }
        return compiler.compile(sql, context).getRecordCursorFactory();
    }

    private double drain(RecordCursorFactory factory) throws SqlException {
        double sum = 0;
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sum += record.getDouble(0);
            }
        }
        return sum;
    }
}
