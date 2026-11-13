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

// A LATERAL body whose RIGHT join reads the outer key in its ON clause, filtered by WHERE t.id > o.k, which
// drops every row that the join NULL-extends, against the same body written as an INNER join, and against
// WHERE t.id >= o.k, which drops those rows too while o.k is not NULL, but holds on NULL >= NULL, so its
// RIGHT join keeps its plan. o has 1,000 rows over 100 keys, and t and r have 5,000 rows each over the same
// keys.
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class LateralRightJoinNullRejectionBenchmark {
    @Param({"right", "inner", "right-kept"})
    public String join;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(LateralRightJoinNullRejectionBenchmark.class.getSimpleName())
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
                bh.consume(record.getInt(2));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("lateral-right-join-null-rejection-");
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
        // x and k take their values from x modulo 100, spread by a multiplier coprime to 100
        engine.execute("CREATE TABLE o AS (SELECT x::INT id, (x % 100 + 1)::INT k FROM long_sequence(1_000))", context);
        engine.execute("CREATE TABLE t AS (SELECT x::INT id, (x * 7_919 % 100 + 1)::INT x FROM long_sequence(5_000))", context);
        engine.execute("CREATE TABLE r AS (SELECT x::INT id, (x * 104_729 % 100 + 1)::INT k FROM long_sequence(5_000))", context);
        final String joinType = "inner".equals(join) ? "JOIN" : "RIGHT JOIN";
        final String op = "right-kept".equals(join) ? ">=" : ">";
        factory = engine.select("SELECT o.id, l.tid, l.rid FROM o JOIN LATERAL (SELECT t.id tid, r.id rid FROM t "
                + joinType + " r ON t.x = r.k AND r.k = o.k WHERE t.id " + op + " o.k) l", context);
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
