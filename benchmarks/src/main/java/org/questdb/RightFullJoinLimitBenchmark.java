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

/**
 * Measures a LIMIT over a RIGHT join whose WHERE filters the table before the join, and a LIMIT that reads
 * a range of rows. trades holds 20M rows over 100 symbols; watch holds 5 of them, and inst all of them.
 * WHERE qty > 1000 keeps half the rows of every symbol, so the RIGHT join finds matches in the first rows,
 * while WHERE qty > 1990 keeps rows of one symbol that watch does not hold, so it reads every row. The
 * parallel filter caps its in-flight page frame tasks for a LIMIT at the server default,
 * min(workers, 32).
 * <p>
 * Run against the baseline and the candidate build:
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests
 * java -cp benchmarks/target/benchmarks.jar org.questdb.RightFullJoinLimitBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class RightFullJoinLimitBenchmark {
    @Param({"right_join_limit", "right_join_no_match_limit", "right_join_negative_limit", "filter_limit_range", "inner_join_limit_range"})
    public String shape;
    @Param({"4"})
    public int workers;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(RightFullJoinLimitBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bh.consume(record.getInt(1));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("right-full-join-limit-");
        final int dispatchLimit = Math.min(workers, 32);
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
            @Override
            public int getSqlParallelFilterDispatchLimit() {
                return dispatchLimit;
            }
        };
        engine = new CairoEngine(configuration);
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "right-full-join-limit";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        WorkerPoolUtils.setupQueryJobs(workerPool, engine);
        workerPool.start();
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
        engine.execute("CREATE TABLE trades AS (SELECT ('S' || (x % 100))::SYMBOL sym, (x * 37 % 2_000)::INT qty,"
                + " timestamp_sequence(0, 10_000) ts FROM long_sequence(20_000_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
        engine.execute("CREATE TABLE watch AS (SELECT ('S' || (x * 10))::SYMBOL sym FROM long_sequence(5))", context);
        engine.execute("CREATE TABLE inst AS (SELECT ('S' || (x - 1))::SYMBOL sym, x::INT lot FROM long_sequence(100))", context);
        final String rightJoin = "SELECT t.sym, t.qty, w.sym wsym FROM trades t RIGHT JOIN watch w ON t.sym = w.sym";
        final String sql = switch (shape) {
            case "right_join_limit" -> rightJoin + " WHERE t.qty > 1000 LIMIT 10";
            case "right_join_no_match_limit" -> rightJoin + " WHERE t.qty > 1990 LIMIT 10";
            case "right_join_negative_limit" -> rightJoin + " WHERE t.qty > 1000 LIMIT -10";
            case "filter_limit_range" -> "SELECT sym, qty FROM trades WHERE qty > 1000 LIMIT 5, 15";
            case "inner_join_limit_range" ->
                    "SELECT t.sym, t.qty, i.lot FROM trades t JOIN inst i ON t.sym = i.sym WHERE t.qty > 1000 LIMIT 3_000_000, 3_000_010";
            default -> throw new IllegalArgumentException(shape);
        };
        factory = engine.select(sql, context);
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
