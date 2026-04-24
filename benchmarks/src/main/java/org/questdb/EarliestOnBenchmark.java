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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Compares EARLIEST ON against GROUP BY + JOIN for finding the first row
 * per partition key. 1M rows, 1000 distinct symbols, ~41 day partitions.
 * Factories are compiled once in setup; only cursor iteration is measured.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class EarliestOnBenchmark {
    private SqlCompilerImpl compiler;
    private CairoConfiguration configuration;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory earliestOnFactory;
    private RecordCursorFactory earliestOnFilteredFactory;
    private RecordCursorFactory earliestOnIndexedFactory;
    private RecordCursorFactory earliestOnIntervalFactory;
    private RecordCursorFactory groupByJoinFactory;
    private RecordCursorFactory groupByJoinFilteredFactory;
    private RecordCursorFactory groupByJoinIntervalFactory;
    private Path root;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(EarliestOnBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .measurementIterations(10)
                .forks(1)
                .build();
        new Runner(opt).run();

        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void setup() throws IOException, SqlException {
        // A fresh temp dir per trial prevents CREATE TABLE IF NOT EXISTS from silently
        // reusing leftover data, which would make the benchmark measure stale state.
        root = Files.createTempDirectory("questdb-earliest-on-bench-");
        try {
            setupInternal();
        } catch (Throwable t) {
            // JMH does not invoke @TearDown when @Setup throws, so clean up here to
            // avoid leaking the temp dir into java.io.tmpdir.
            safeCleanup();
            throw t;
        }
    }

    private void setupInternal() throws SqlException {
        configuration = new DefaultCairoConfiguration(root.toString());
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        compiler = new SqlCompilerImpl(engine);
        engine.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data AS (
                    SELECT
                        rnd_symbol(1000, 4, 8, 0) device_id,
                        rnd_double() * 100 temperature,
                        rnd_double() * 50 humidity,
                        timestamp_sequence('2024-01-01', 3_600_000) ts
                    FROM long_sequence(1_000_000)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """, ctx);
        engine.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data_indexed AS (
                    SELECT
                        rnd_symbol(1000, 4, 8, 0) device_id,
                        rnd_double() * 100 temperature,
                        rnd_double() * 50 humidity,
                        timestamp_sequence('2024-01-01', 3_600_000) ts
                    FROM long_sequence(1_000_000)
                ), INDEX(device_id) TIMESTAMP(ts) PARTITION BY DAY
                """, ctx);

        earliestOnFactory = compiler.compile(
                "SELECT * FROM sensor_data EARLIEST ON ts PARTITION BY device_id", ctx
        ).getRecordCursorFactory();
        earliestOnFilteredFactory = compiler.compile(
                "SELECT * FROM sensor_data WHERE temperature > 50 EARLIEST ON ts PARTITION BY device_id", ctx
        ).getRecordCursorFactory();
        earliestOnIndexedFactory = compiler.compile(
                "SELECT * FROM sensor_data_indexed EARLIEST ON ts PARTITION BY device_id", ctx
        ).getRecordCursorFactory();
        earliestOnIntervalFactory = compiler.compile(
                "SELECT * FROM sensor_data WHERE ts IN '2024-01-15' EARLIEST ON ts PARTITION BY device_id", ctx
        ).getRecordCursorFactory();
        groupByJoinFactory = compiler.compile("""
                SELECT d.*
                FROM sensor_data d
                JOIN (
                    SELECT device_id, min(ts) AS min_ts
                    FROM sensor_data
                    GROUP BY device_id
                ) g ON d.device_id = g.device_id AND d.ts = g.min_ts
                """, ctx).getRecordCursorFactory();
        groupByJoinFilteredFactory = compiler.compile("""
                SELECT d.*
                FROM sensor_data d
                JOIN (
                    SELECT device_id, min(ts) AS min_ts
                    FROM sensor_data
                    WHERE temperature > 50
                    GROUP BY device_id
                ) g ON d.device_id = g.device_id AND d.ts = g.min_ts
                WHERE d.temperature > 50
                """, ctx).getRecordCursorFactory();
        groupByJoinIntervalFactory = compiler.compile("""
                SELECT d.*
                FROM sensor_data d
                JOIN (
                    SELECT device_id, min(ts) AS min_ts
                    FROM sensor_data
                    WHERE ts IN '2024-01-15'
                    GROUP BY device_id
                ) g ON d.device_id = g.device_id AND d.ts = g.min_ts
                WHERE d.ts IN '2024-01-15'
                """, ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        safeCleanup();
    }

    private static void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Throwable ignore) {
        }
    }

    private void safeCleanup() throws IOException {
        closeQuietly(earliestOnFactory);
        closeQuietly(earliestOnFilteredFactory);
        closeQuietly(earliestOnIndexedFactory);
        closeQuietly(earliestOnIntervalFactory);
        closeQuietly(groupByJoinFactory);
        closeQuietly(groupByJoinFilteredFactory);
        closeQuietly(groupByJoinIntervalFactory);
        closeQuietly(compiler);
        closeQuietly(engine);
        if (root != null) {
            try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
                paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
            }
        }
    }

    @Benchmark
    public long testEarliestOn() throws SqlException {
        return drainFactory(earliestOnFactory);
    }

    @Benchmark
    public long testEarliestOnFiltered() throws SqlException {
        return drainFactory(earliestOnFilteredFactory);
    }

    @Benchmark
    public long testEarliestOnIndexed() throws SqlException {
        return drainFactory(earliestOnIndexedFactory);
    }

    @Benchmark
    public long testEarliestOnInterval() throws SqlException {
        return drainFactory(earliestOnIntervalFactory);
    }

    @Benchmark
    public long testGroupByJoin() throws SqlException {
        return drainFactory(groupByJoinFactory);
    }

    @Benchmark
    public long testGroupByJoinFiltered() throws SqlException {
        return drainFactory(groupByJoinFilteredFactory);
    }

    @Benchmark
    public long testGroupByJoinInterval() throws SqlException {
        return drainFactory(groupByJoinIntervalFactory);
    }

    private long drainFactory(RecordCursorFactory factory) throws SqlException {
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }
}
