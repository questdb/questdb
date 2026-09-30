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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
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
 * Per-row cost of four representative functions (a comparison, an arithmetic operator, an
 * aggregate and a cast) over a LONG column, with and without NULL rows. The functions' NULL
 * handling is what the benchmark measures, so a change to how a function tests its operands
 * for NULL shows here first. The JIT is off, so the filter takes the interpreted path.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = {"--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"})
public class FunctionNullWrapperBenchmark {
    private static final int PARTITIONS = 4;
    private static final String ROOT = System.getProperty("java.io.tmpdir") + File.separator + "function-null-wrapper-bench";
    private static final int ROWS = 1_000_000;
    // LONG: no NULL rows; LONG_NULLS: every tenth row NULL
    @Param({"LONG", "LONG_NULLS"})
    public String column;
    private RecordCursorFactory addFactory;
    private RecordCursorFactory castFactory;
    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory eqFactory;
    private RecordCursorFactory sumFactory;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(FunctionNullWrapperBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long add() throws SqlException {
        return firstLong(addFactory);
    }

    @Benchmark
    public long cast() throws SqlException {
        return firstLong(castFactory);
    }

    @Benchmark
    public long eq() throws SqlException {
        return firstLong(eqFactory);
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
        ctx.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        compiler = new SqlCompilerImpl(engine);

        final boolean hasNulls = switch (column) {
            case "LONG" -> false;
            case "LONG_NULLS" -> true;
            default -> throw new IllegalArgumentException(column);
        };
        engine.execute("DROP TABLE IF EXISTS t", ctx);
        engine.execute("CREATE TABLE t (ts TIMESTAMP, a LONG, b LONG) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        // row x: a = x, b = x on even rows and x + 1 on odd rows; with NULLs, both are NULL on
        // every tenth row
        final long stepMicros = PARTITIONS * 86_400_000_000L / ROWS;
        final String nullArm = hasNulls ? " WHEN x % 10 = 0 THEN NULL" : "";
        engine.execute(
                "INSERT INTO t SELECT (x * " + stepMicros + ")::TIMESTAMP," +
                        " CASE" + nullArm + " WHEN x > 0 THEN x END," +
                        " CASE" + nullArm + " WHEN x % 2 = 0 THEN x ELSE x + 1 END" +
                        " FROM long_sequence(" + ROWS + ")",
                ctx
        );
        engine.releaseAllWriters();

        eqFactory = compile("SELECT count(*) FROM t WHERE a = b");
        addFactory = compile("SELECT sum(a + b) FROM t");
        sumFactory = compile("SELECT sum(a) FROM t");
        castFactory = compile("SELECT count(*) FROM t WHERE a::DOUBLE = 0.5");

        // NULL = NULL is true, so the NULL rows count as equal
        final long expectedEq = ROWS / 2;
        final long eq = firstLong(eqFactory);
        if (eq != expectedEq) {
            throw new IllegalStateException("unexpected eq count [column=" + column + ", expected=" + expectedEq + ", actual=" + eq + ']');
        }
        final long cast = firstLong(castFactory);
        if (cast != 0) {
            throw new IllegalStateException("unexpected cast count [column=" + column + ", actual=" + cast + ']');
        }
        firstLong(addFactory);
        firstLong(sumFactory);
    }

    @Benchmark
    public long sum() throws SqlException {
        return firstLong(sumFactory);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws SqlException {
        eqFactory.close();
        addFactory.close();
        sumFactory.close();
        castFactory.close();
        engine.execute("DROP TABLE t", ctx);
        compiler.close();
        engine.close();
    }

    private RecordCursorFactory compile(String query) throws SqlException {
        final RecordCursorFactory factory = compiler.compile(query, ctx).getRecordCursorFactory();
        if (factory.usesCompiledFilter()) {
            factory.close();
            throw new IllegalStateException("JIT filter in " + query);
        }
        return factory;
    }

    private long firstLong(RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            if (!cursor.hasNext()) {
                throw new IllegalStateException("no row");
            }
            return record.getLong(0);
        }
    }
}
