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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
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
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Measures SAMPLE BY FILL over the SAMPLE BY cursor (ALIGN TO FIRST OBSERVATION), keyed and
 * non-keyed, for each fill kind, against FILL(NONE) as the baseline of the same SAMPLE BY.
 * <p>
 * Every {@code query} value runs in its own forked JVMs, so the JIT profile of one fill kind does
 * not leak into another. Each fork builds a 3,000,000-row table with 100 symbols, one row every 37
 * seconds, so a 1-hour keyed sample has about 97 rows per bucket and leaves gaps for the fill; a
 * 10-second non-keyed sample leaves gaps between most rows. The benchmark drains the cursor
 * without reading values.
 * <p>
 * Build (note {@code -am} so the benchmark links the in-tree core) and run:
 * <pre>
 * mvn -pl benchmarks -am package -o -DskipTests
 * java -cp benchmarks/target/benchmarks.jar org.questdb.SampleByFillBenchmark
 * </pre>
 * Extra args are passed through to JMH, e.g. {@code -p query=KEYED_NULL,KEYED_NONE}.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 3, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class SampleByFillBenchmark {
    private static final long ROWS = 3_000_000;
    @Param({
            "KEYED_NONE",
            "KEYED_NULL",
            "KEYED_VALUE",
            "KEYED_PREV_AND_VALUE",
            "KEYED_PREV",
            "KEYED_PREV_STRING",
            "NON_KEYED_NONE",
            "NON_KEYED_NULL",
            "NON_KEYED_PREV",
            "NON_KEYED_PREV_STRING"
    })
    public String query;
    private SqlCompiler compiler;
    private SqlExecutionContext context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private String root;

    public static void main(String[] args) throws Exception {
        final Options opt = args.length > 0
                ? new CommandLineOptions(args)
                : new OptionsBuilder().include(SampleByFillBenchmark.class.getSimpleName()).build();
        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long drain() throws SqlException {
        long rows = 0;
        try (RecordCursor cursor = factory.getCursor(context)) {
            while (cursor.hasNext()) {
                rows++;
            }
        }
        return rows;
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = System.getProperty("java.io.tmpdir") + java.io.File.separator + "sample-by-fill-bench-" + ProcessHandle.current().pid();
        java.nio.file.Files.createDirectories(Paths.get(root));
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root);
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean shouldLogSql() {
                return false;
            }
        }.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = engine.getSqlCompiler();
        engine.execute(
                "CREATE TABLE p AS (" +
                        "SELECT timestamp_sequence(0, 37_000_000L) ts, rnd_symbol(100, 4, 4, 0) sym, rnd_double() d, rnd_str(5, 10, 0) s" +
                        " FROM long_sequence(" + ROWS + ")" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                context
        );
        factory = compiler.compile(sql(query), context).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        engine = Misc.free(engine);
        try (Path path = new Path()) {
            Files.rmdir(path.of(root), false);
        }
    }

    private static String sql(String query) {
        switch (query) {
            case "KEYED_NONE":
                return "SELECT ts, sym, avg(d) FROM p SAMPLE BY 1h ALIGN TO FIRST OBSERVATION";
            case "KEYED_NULL":
                return "SELECT ts, sym, avg(d) FROM p SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION";
            case "KEYED_VALUE":
                return "SELECT ts, sym, avg(d) FROM p SAMPLE BY 1h FILL(5) ALIGN TO FIRST OBSERVATION";
            case "KEYED_PREV_AND_VALUE":
                return "SELECT ts, sym, avg(d) a, max(d) m FROM p SAMPLE BY 1h FILL(PREV, 5) ALIGN TO FIRST OBSERVATION";
            case "KEYED_PREV":
                return "SELECT ts, sym, avg(d) FROM p SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION";
            case "KEYED_PREV_STRING":
                return "SELECT ts, sym, first(s) FROM p SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION";
            case "NON_KEYED_NONE":
                return "SELECT ts, avg(d) FROM p SAMPLE BY 10s ALIGN TO FIRST OBSERVATION";
            case "NON_KEYED_NULL":
                return "SELECT ts, avg(d) FROM p SAMPLE BY 10s FILL(NULL) ALIGN TO FIRST OBSERVATION";
            case "NON_KEYED_PREV":
                return "SELECT ts, avg(d) FROM p SAMPLE BY 10s FILL(PREV) ALIGN TO FIRST OBSERVATION";
            case "NON_KEYED_PREV_STRING":
                return "SELECT ts, first(s) FROM p SAMPLE BY 10s FILL(PREV) ALIGN TO FIRST OBSERVATION";
            default:
                throw new IllegalArgumentException("unknown query: " + query);
        }
    }
}
