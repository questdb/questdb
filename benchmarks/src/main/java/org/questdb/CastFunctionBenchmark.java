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
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Misc;
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
import org.openjdk.jmh.infra.Blackhole;
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
 * Per-row cost of SQL casts: TIMESTAMP and TIMESTAMP_NS to STRING and to VARCHAR, read through
 * the cast function's text getters, and CHAR to LONG256, printed through the function's sink
 * getter. Each run scans a table of {@code rowCount} rows and reads the cast column of every row;
 * the scan is the same for every cast, so a change shows as a change of the cast.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsAppend = {"--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED", "--enable-native-access=ALL-UNNAMED"})
public class CastFunctionBenchmark {
    @Param({"TIMESTAMP_STRING", "TIMESTAMP_VARCHAR", "TIMESTAMP_NS_STRING", "TIMESTAMP_NS_VARCHAR", "CHAR_LONG256"})
    public String cast;
    @Param({"1000000"})
    public int rowCount;
    private final StringSink sink = new StringSink();
    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CastFunctionBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            switch (cast) {
                case "TIMESTAMP_STRING", "TIMESTAMP_NS_STRING" -> {
                    while (cursor.hasNext()) {
                        bh.consume(record.getStrA(0));
                    }
                }
                case "TIMESTAMP_VARCHAR", "TIMESTAMP_NS_VARCHAR" -> {
                    while (cursor.hasNext()) {
                        bh.consume(record.getVarcharA(0));
                    }
                }
                default -> {
                    while (cursor.hasNext()) {
                        sink.clear();
                        record.getLong256(0, sink);
                        bh.consume(sink.length());
                    }
                }
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = Files.createTempDirectory("castfunctionbench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString());
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        compiler = new SqlCompilerImpl(engine);
        engine.execute("CREATE TABLE t (ts TIMESTAMP, ts_ns TIMESTAMP_NS, c CHAR)", ctx);
        // a step of a second and 37 microseconds varies every field the text form prints; CHAR holds
        // digits, the values its LONG256 cast converts
        engine.execute(
                "INSERT INTO t SELECT "
                        + "timestamp_sequence('2024-01-01T00:00:00.000000Z'::TIMESTAMP, 1_000_037) ts, "
                        + "timestamp_sequence('2024-01-01T00:00:00.000000Z'::TIMESTAMP, 1_000_037)::TIMESTAMP_NS ts_ns, "
                        + "rnd_str('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')::CHAR c "
                        + "FROM long_sequence(" + rowCount + ")",
                ctx
        );
        factory = compiler.compile(buildSql(), ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        engine = Misc.free(engine);
        if (tempRoot != null && Files.exists(tempRoot)) {
            try (Stream<Path> stream = Files.walk(tempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception ignore) {
                    }
                });
            }
            tempRoot = null;
        }
    }

    private String buildSql() {
        return switch (cast) {
            case "TIMESTAMP_STRING" -> "SELECT ts::STRING FROM t";
            case "TIMESTAMP_VARCHAR" -> "SELECT ts::VARCHAR FROM t";
            case "TIMESTAMP_NS_STRING" -> "SELECT ts_ns::STRING FROM t";
            case "TIMESTAMP_NS_VARCHAR" -> "SELECT ts_ns::VARCHAR FROM t";
            case "CHAR_LONG256" -> "SELECT c::LONG256 FROM t";
            default -> throw new IllegalArgumentException("unknown cast: " + cast);
        };
    }
}
