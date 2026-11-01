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

// An INNER hash join with a condition after it that reads the joined table and the key of the other one, which
// the join can evaluate on its own key. b has 1M rows and s 10,000, both over 1,000 keys, and v - k runs over
// 0..98 evenly within each key, so v - k < 1 keeps 1% of the rows of a key and v - k < 98 keeps 99%. With
// hashed=master, "FROM s JOIN b" hashes s, the smaller master, and reads b row by row; with hashed=slave,
// "FROM b JOIN s" hashes s, its slave, and reads b row by row as its master.
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class HashJoinKeyConditionBenchmark {
    @Param({"master", "slave"})
    public String hashed;
    @Param({"1", "99"})
    public int keptPercent;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(HashJoinKeyConditionBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bh.consume(record.getInt(0));
                bh.consume(record.getInt(1));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("hash-join-key-condition-");
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
        // k and v - k take their values from x modulo 1,000 and modulo 99, which are independent
        engine.execute("CREATE TABLE b AS (SELECT x::INT id, (x * 7_919 % 1_000 + 1)::INT k,"
                + " (x * 7_919 % 1_000 + 1 + x * 104_729 % 99)::INT v FROM long_sequence(1_000_000))", context);
        engine.execute("CREATE TABLE s AS (SELECT x::INT id, (x % 1_000 + 1)::INT k,"
                + " (x % 1_000 + 1 + x * 104_729 % 99)::INT v FROM long_sequence(10_000))", context);
        final int bound = keptPercent == 1 ? 1 : 98;
        final String sql = "master".equals(hashed)
                ? "SELECT s.id, b.id FROM s JOIN b ON b.k = s.k WHERE b.v - s.k < " + bound
                : "SELECT b.id, s.id FROM b JOIN s ON s.k = b.k WHERE s.v - b.k < " + bound;
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
