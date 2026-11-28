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
 * Measures JSON UNNEST and json_extract() over 20M rows. Every row holds an empty JSON array in j, two elements
 * in j2, an object with an empty array in d, and an empty DOUBLE[] in arr. A negative LIMIT over UNNEST reads
 * back through every row whose array is empty, so the *_empty_negative_limit shapes read the whole table; the
 * *_read shapes read it to the end as well.
 * <p>
 * Run against the baseline and the candidate build:
 * <pre>
 * mvn -pl benchmarks -am package -DskipTests
 * java -cp benchmarks/target/benchmarks.jar org.questdb.JsonUnnestBenchmark
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class JsonUnnestBenchmark {
    @Param({
            "json_empty_negative_limit", "json_extract_empty_negative_limit", "array_empty_negative_limit",
            "json_elements_read", "json_extract_read"
    })
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
                .include(JsonUnnestBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                bh.consume(record.getInt(0));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws IOException, SqlException {
        root = Files.createTempDirectory("json-unnest-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "json-unnest";
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
        engine.execute("CREATE TABLE t AS (SELECT (11 + x % 100)::INT x, ARRAY[]::DOUBLE[] arr, '[]'::VARCHAR j,"
                + " '[1, 2]'::VARCHAR j2, '{\"a\":[]}'::VARCHAR d, timestamp_sequence(0, 86_400) ts"
                + " FROM long_sequence(20_000_000)) TIMESTAMP(ts) PARTITION BY DAY", context);
        final String sql = switch (shape) {
            case "json_empty_negative_limit" ->
                    "SELECT t.x, u.v FROM t, UNNEST(t.j COLUMNS(v INT)) u WHERE t.x > 10 LIMIT -10";
            case "json_extract_empty_negative_limit" -> "SELECT t.x, u.v FROM t, UNNEST(json_extract(t.d, '$.a')"
                    + " COLUMNS(v INT)) u WHERE t.x > 10 LIMIT -10";
            case "array_empty_negative_limit" -> "SELECT t.x, u.v FROM t, UNNEST(t.arr) u(v) WHERE t.x > 10 LIMIT -10";
            case "json_elements_read" -> "SELECT t.x, u.v FROM t, UNNEST(t.j2 COLUMNS(v INT)) u WHERE t.x > 10";
            case "json_extract_read" -> "SELECT sum(length(json_extract(t.d, '$.a')))::INT s FROM t";
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
