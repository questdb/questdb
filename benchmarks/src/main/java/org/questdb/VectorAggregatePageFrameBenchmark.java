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
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.str.StringSink;
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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Vector aggregates over page frames: the queries whose factories hand page-frame column
 * addresses to the native {@code Vect} kernels.
 * <p>
 * The table is partitioned by day over {@value #PARTITIONS} partitions and holds LONG, DOUBLE and
 * INT columns. The INT column is added after half of the first partition is written, so that
 * partition has a column top.
 * <ul>
 *   <li>{@link #notKeyed()}: {@code SELECT sum(l), min(d), max(i) FROM t}, the vectorized
 *       Async Group By path.</li>
 *   <li>{@link #keyed()}: the same aggregates keyed by an INT column, the vectorized GroupBy path
 *       that runs through {@code VectorAggregateEntry}.</li>
 * </ul>
 * The setup checks the plan of both queries and the not-keyed result, so a change that leaves the
 * vectorized path fails the benchmark instead of measuring another plan.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"})
public class VectorAggregatePageFrameBenchmark {
    private static final int KEY_COUNT = 16;
    private static final int PARTITIONS = 10;
    private static final String ROOT = System.getProperty("java.io.tmpdir") + File.separator + "vector-aggregate-page-frame-bench";
    @Param({"1000000", "10000000"})
    public int rows;
    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory keyedFactory;
    private RecordCursorFactory notKeyedFactory;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VectorAggregatePageFrameBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long keyed() throws SqlException {
        long result = 0;
        try (RecordCursor cursor = keyedFactory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                result += record.getLong(1) + record.getInt(3);
            }
        }
        return result;
    }

    @Benchmark
    public double notKeyed() throws SqlException {
        try (RecordCursor cursor = notKeyedFactory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            if (!cursor.hasNext()) {
                throw new IllegalStateException("not-keyed aggregate returned no row");
            }
            return record.getLong(0) + record.getDouble(1) + record.getInt(2);
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Exception {
        Files.createDirectories(Paths.get(ROOT));
        final CairoConfiguration configuration = new DefaultCairoConfiguration(ROOT);
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = new SqlCompilerImpl(engine);

        engine.execute("DROP TABLE IF EXISTS t", ctx);
        engine.execute("CREATE TABLE t (ts TIMESTAMP, k INT, l LONG, d DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        final long stepMicros = PARTITIONS * 86_400_000_000L / rows;
        final long topRows = rows / PARTITIONS / 2;
        engine.execute(insertSql(0, topRows, stepMicros, false), ctx);
        engine.execute("ALTER TABLE t ADD COLUMN i INT", ctx);
        engine.execute(insertSql(topRows, rows - topRows, stepMicros, true), ctx);
        engine.releaseAllWriters();

        notKeyedFactory = compileVectorized("SELECT sum(l), min(d), max(i) FROM t", "Async Group By");
        keyedFactory = compileVectorized("SELECT k, sum(l), min(d), max(i) FROM t", "GroupBy vectorized: true");

        // row r holds l = r, d = r / 3.0 and, from topRows on, i = r
        try (RecordCursor cursor = notKeyedFactory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            final long expectedSum = (long) rows * (rows - 1) / 2;
            if (!cursor.hasNext() || record.getLong(0) != expectedSum || record.getDouble(1) != 0.0 || record.getInt(2) != rows - 1) {
                throw new IllegalStateException("unexpected not-keyed aggregate result [rows=" + rows + ']');
            }
        }
        long keys = 0;
        try (RecordCursor cursor = keyedFactory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                keys++;
            }
        }
        if (keys != KEY_COUNT) {
            throw new IllegalStateException("unexpected key count [expected=" + KEY_COUNT + ", actual=" + keys + ']');
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws SqlException {
        keyedFactory.close();
        notKeyedFactory.close();
        engine.execute("DROP TABLE t", ctx);
        compiler.close();
        engine.close();
    }

    private static String insertSql(long firstRow, long count, long stepMicros, boolean withInt) {
        return "INSERT INTO t SELECT" +
                " ((x - 1 + " + firstRow + ") * " + stepMicros + ")::TIMESTAMP," +
                " ((x - 1 + " + firstRow + ") % " + KEY_COUNT + ")::INT," +
                " x - 1 + " + firstRow + "," +
                " (x - 1 + " + firstRow + ") / 3.0" +
                (withInt ? ", (x - 1 + " + firstRow + ")::INT" : "") +
                " FROM long_sequence(" + count + ")";
    }

    private RecordCursorFactory compileVectorized(String query, String expectedPlanFragment) throws SqlException {
        final StringSink plan = new StringSink();
        try (
                RecordCursorFactory explain = compiler.compile("EXPLAIN " + query, ctx).getRecordCursorFactory();
                RecordCursor cursor = explain.getCursor(ctx)
        ) {
            while (cursor.hasNext()) {
                plan.put(cursor.getRecord().getStrA(0)).put('\n');
            }
        }
        if (!plan.toString().contains(expectedPlanFragment) || !plan.toString().contains("vectorized: true")) {
            throw new IllegalStateException("unexpected plan for " + query + ":\n" + plan);
        }
        return compiler.compile(query, ctx).getRecordCursorFactory();
    }
}
