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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
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
import org.openjdk.jmh.infra.Blackhole;
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

// A HORIZON JOIN whose two left-hand keys share a right-hand column, t.k1 = p.k AND t.k2 = p.k, and
// the same join on t.k1 = p.k alone. t has 50,000 rows a second apart, p 500,000 rows over the same
// time with 100 keys, and the join sums p.price at three offsets. k2 differs from k1 in the given
// percentage of the rows of t, which match no row of p.
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class HorizonJoinSharedKeyBenchmark {
    @Param({"SYMBOL", "INT"})
    public String keyType;
    @Param({"0", "1", "50"})
    public int mismatchPercent;
    @Param({"shared", "single"})
    public String on;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(HorizonJoinSharedKeyBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            while (cursor.hasNext()) {
                bh.consume(cursor.getRecord().getDouble(0));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("horizon-join-shared-key-");
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
        final boolean isSymbol = "SYMBOL".equals(keyType);
        final String key = isSymbol ? "'k' || (x % 100)" : "x % 100";
        final String otherKey = isSymbol ? "'k' || ((x + 1) % 100)" : "(x + 1) % 100";
        // every 100 / mismatchPercent-th row holds two keys that differ
        final String k2 = mismatchPercent == 0 ? key : "CASE WHEN x % " + (100 / mismatchPercent) + " = 0 THEN " + otherKey + " ELSE " + key + " END";
        engine.execute("CREATE TABLE t AS (SELECT (" + key + ")::" + keyType + " k1, (" + k2 + ")::" + keyType + " k2, "
                + "timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("CREATE TABLE p AS (SELECT (" + key + ")::" + keyType + " k, (x % 1_000) / 10.0 price, "
                + "timestamp_sequence('2024-01-01', 100_000) ts FROM long_sequence(500_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
        factory = engine.select("SELECT sum(p.price) FROM t HORIZON JOIN p ON ("
                + ("shared".equals(on) ? "t.k1 = p.k AND t.k2 = p.k" : "t.k1 = p.k")
                + ") LIST (-1s, 0s, 1s) AS h", context);
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
