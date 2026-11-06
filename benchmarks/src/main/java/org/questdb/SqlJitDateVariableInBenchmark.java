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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.datetime.microtime.Micros;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Filters a non-designated TIMESTAMP column with a date variable IN whose list also holds static
 * days, with the JIT on and off. The JIT compiles one range check per interval that the
 * expression can yield, and declines an expression with more than
 * cairo.sql.jit.max.in.list.size.threshold (10) of them. "now" is 2024-02-05T01:00Z, and the 5M
 * rows spread evenly over 2023-12-01 to 2024-02-29.
 * <ul>
 *   <li>{@code DAYS_20}: {@code tm IN '[$today, 2024-01-[01..20]]'}</li>
 *   <li>{@code DAYS_5}: {@code tm IN '[$today, 2024-01-[01..05]]'}</li>
 *   <li>{@code WORKDAYS_20}: {@code tm IN '[$today, 2024-01-[01..20]]#workday'}</li>
 *   <li>{@code OR_REFERENCE}: {@code tm IN '$today' OR tm IN '2024-01-[01..20]'}, the rows of
 *   {@code DAYS_20} in two INs</li>
 * </ul>
 * The setup prints whether each factory compiled its filter with the JIT.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class SqlJitDateVariableInBenchmark {
    private static final long NOW_MICROS = Micros.toMicros(2024, 2, 5, 1, 0);
    private static final int ROW_COUNT = 5_000_000;
    // 91 days from 2023-12-01 over ROW_COUNT rows
    private static final long ROW_STEP_MICROS = 91 * Micros.DAY_MICROS / ROW_COUNT;

    @Param({"DAYS_20", "DAYS_5", "WORKDAYS_20", "OR_REFERENCE"})
    public Filter filter;
    @Param({"ENABLED", "DISABLED"})
    public JitMode jitMode;
    private SqlCompilerImpl compiler;
    private RecordCursorFactory countFactory;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory selectFactory;
    private Path tempRoot;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SqlJitDateVariableInBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long testCount() throws SqlException {
        try (RecordCursor cursor = countFactory.getCursor(ctx)) {
            return cursor.hasNext() ? cursor.getRecord().getLong(0) : -1;
        }
    }

    @Benchmark
    public long testSelect() throws SqlException {
        long sum = 0;
        try (RecordCursor cursor = selectFactory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sum += record.getInt(0);
            }
        }
        return sum;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = Files.createTempDirectory("sqljitdatevariableinbench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString());
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = new SqlCompilerImpl(engine);
        engine.execute("CREATE TABLE x (k INT, tm TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        engine.execute(
                "INSERT INTO x SELECT x::INT, t, t FROM (SELECT x, timestamp_sequence('2023-12-01'::TIMESTAMP, "
                        + ROW_STEP_MICROS + ") t FROM long_sequence(" + ROW_COUNT + "))",
                ctx
        );
        ctx.setNowAndFixClock(NOW_MICROS, ColumnType.TIMESTAMP_MICRO);
        ctx.setJitMode(jitMode == JitMode.ENABLED ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
        countFactory = compiler.compile("SELECT count() FROM x WHERE " + filter.sql, ctx).getRecordCursorFactory();
        selectFactory = compiler.compile("SELECT k FROM x WHERE " + filter.sql, ctx).getRecordCursorFactory();
        System.out.println("filter=" + filter + " jitMode=" + jitMode
                + " count usesCompiledFilter=" + countFactory.usesCompiledFilter()
                + " select usesCompiledFilter=" + selectFactory.usesCompiledFilter()
                + " count=" + testCount());
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        countFactory = Misc.free(countFactory);
        selectFactory = Misc.free(selectFactory);
        compiler = Misc.free(compiler);
        engine = Misc.free(engine);
        if (tempRoot != null) {
            try (Stream<Path> paths = Files.walk(tempRoot)) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
            tempRoot = null;
        }
    }

    public enum Filter {
        DAYS_20("tm IN '[$today, 2024-01-[01..20]]'"),
        DAYS_5("tm IN '[$today, 2024-01-[01..05]]'"),
        WORKDAYS_20("tm IN '[$today, 2024-01-[01..20]]#workday'"),
        OR_REFERENCE("tm IN '$today' OR tm IN '2024-01-[01..20]'");

        private final String sql;

        Filter(String sql) {
            this.sql = sql;
        }
    }

    public enum JitMode {
        ENABLED, DISABLED
    }
}
