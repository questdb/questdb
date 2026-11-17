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
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
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

// A condition next to a LATERAL WHERE key that reads the key's outer column, against the same condition on
// the key's inner column, which filters the scan of the inner table. a has 4M rows over 1,000 keys, and
// a.v - a.x runs over 0..98 evenly within each key, so a.v <= x keeps 1% of the rows of a key and a.v > x
// keeps 99%. o has 10,000 rows over every key, or 10 rows of one key.
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class LateralWhereKeyFilterBenchmark {
    @Param({"jit", "nojit"})
    public String condition;
    @Param({"1", "99"})
    public int keptPercent;
    @Param({"every", "one"})
    public String outerKeys;
    // outer: the condition reads o.x; inner: it reads a.x, which WHERE equates to o.x
    @Param({"outer", "inner"})
    public String reads;
    @Param({"1", "4"})
    public int workers;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(LateralWhereKeyFilterBenchmark.class.getSimpleName())
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
        root = Files.createTempDirectory("lateral-where-key-filter-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        if (workers > 1) {
            workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "lateral-where-key-filter";
                }

                @Override
                public int getWorkerCount() {
                    return workers;
                }
            });
            WorkerPoolUtils.setupQueryJobs(workerPool, engine);
            workerPool.start();
        }
        context = new SqlExecutionContextImpl(engine, workers) {
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
        // x and v - x take their values from x modulo 1,000 and modulo 99, which are independent
        engine.execute("CREATE TABLE a AS (SELECT x::INT id, (x * 7_919 % 1_000 + 1)::INT x,"
                + " (x * 7_919 % 1_000 + 1 + x * 104_729 % 99)::INT v FROM long_sequence(4_000_000))", context);
        if ("every".equals(outerKeys)) {
            engine.execute("CREATE TABLE o AS (SELECT x::INT id, (x % 1_000 + 1)::INT x FROM long_sequence(10_000))", context);
        } else {
            engine.execute("CREATE TABLE o AS (SELECT x::INT id, 7 x FROM long_sequence(10))", context);
        }
        final String x = "outer".equals(reads) ? "o.x" : "a.x";
        final String op = keptPercent == 1 ? "<=" : ">";
        final String filter = "jit".equals(condition)
                ? "a.v " + op + " " + x
                : "a.v % 1_000_000 " + op + " " + x + " % 1_000_000";
        factory = engine.select("SELECT o.id, l.aid FROM o JOIN LATERAL (SELECT a.id aid FROM a WHERE a.x = o.x AND " + filter + ") l", context);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        factory = Misc.free(factory);
        context = Misc.free(context);
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        engine = Misc.free(engine);
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
