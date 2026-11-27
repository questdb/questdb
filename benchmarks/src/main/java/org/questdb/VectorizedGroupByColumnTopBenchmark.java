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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.vect.GroupByRecordCursorFactory;
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

import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * The vectorized (Rosti) keyed GROUP BY over a key column that the table gained after most of its
 * rows: every earlier row is a column top, which aggregates under the NULL key. A user who adds a
 * column to a large table and groups by it meets this shape.
 * <p>
 * Params:
 * <ul>
 *   <li>{@code layout}: {@code COLUMN_TOP} adds the key column after {@code rowCount} rows, then
 *       writes 1_000 rows with keys; {@code EXPLICIT_NULL} creates the key column up front and writes
 *       NULL into the same rows, the cost of aggregating them through the keyed natives.</li>
 *   <li>{@code shape}: {@code SUM} ({@code sum(v)}), {@code COUNT} ({@code count()}), {@code MIXED}
 *       ({@code avg(v), max(d), min(i)}) and {@code MULTI} ({@code sum(v), sum(d), count()}).</li>
 *   <li>{@code workers}: {@code 0} runs the aggregation on the query's thread; any other value runs
 *       a query worker pool of that size.</li>
 *   <li>{@code rowCount}: rows before the key column holds data.</li>
 * </ul>
 * <p>
 * A {@code @Setup(Level.Trial)} routing guard confirms that the query runs the vectorized GROUP BY.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class VectorizedGroupByColumnTopBenchmark {

    @Param({"COLUMN_TOP", "EXPLICIT_NULL"})
    public String layout;

    @Param({"10000000"})
    public int rowCount;

    @Param({"SUM", "COUNT", "MIXED", "MULTI"})
    public String shape;

    @Param({"0", "4"})
    public int workers;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private boolean isDoubleAggregate;
    private Path tempRoot;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VectorizedGroupByColumnTopBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            // Every shape projects the key first and a LONG or DOUBLE aggregate second.
            while (cursor.hasNext()) {
                bh.consume(record.getInt(0));
                bh.consume(isDoubleAggregate ? record.getDouble(1) : record.getLong(1));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("vectcolumntopbench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString());
        engine = new CairoEngine(configuration);

        if (workers > 0) {
            workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "vectcolumntopbench";
                }

                @Override
                public int getWorkerCount() {
                    return workers;
                }
            });
            WorkerPoolUtils.setupQueryJobs(workerPool, engine);
            workerPool.start();
        }

        ctx = new SqlExecutionContextImpl(engine, Math.max(workers, 1))
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );
        compiler = new SqlCompilerImpl(engine);

        // 100 rows a second: 10M rows span about 28 hours, two DAY partitions.
        final String values = "x v, x / 2.0 d, (x % 1_000)::INT i, timestamp_sequence('2024-01-01', 10_000) ts";
        if ("COLUMN_TOP".equals(layout)) {
            engine.execute("CREATE TABLE t AS (SELECT " + values + " FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts) PARTITION BY DAY", ctx);
            engine.execute("ALTER TABLE t ADD COLUMN k INT", ctx);
        } else {
            engine.execute("CREATE TABLE t AS (SELECT " + values + ", NULL::INT k FROM long_sequence(" + rowCount + ")) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        }
        // 1_000 rows with 10 keys in a later partition.
        engine.execute("INSERT INTO t (v, d, i, ts, k) SELECT x, x / 2.0, x::INT, timestamp_sequence('2024-02-01', 1_000_000), (x % 10)::INT"
                + " FROM long_sequence(1_000)", ctx);

        factory = compiler.compile(buildSql(), ctx).getRecordCursorFactory();
        assertRouting(factory);
        isDoubleAggregate = ColumnType.isDouble(factory.getMetadata().getColumnType(1));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        engine = Misc.free(engine);
        if (tempRoot != null && java.nio.file.Files.exists(tempRoot)) {
            try (java.util.stream.Stream<Path> stream = java.nio.file.Files.walk(tempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        java.nio.file.Files.deleteIfExists(path);
                    } catch (Exception ignore) {
                    }
                });
            }
            tempRoot = null;
        }
    }

    private void assertRouting(RecordCursorFactory root) {
        for (RecordCursorFactory cur = root; cur != null; cur = cur.getBaseFactory()) {
            if (cur instanceof GroupByRecordCursorFactory) {
                return;
            }
        }
        throw new IllegalStateException(
                "routing drift: layout=" + layout + " shape=" + shape + " workers=" + workers
                        + " root=" + root.getClass().getSimpleName()
        );
    }

    private String buildSql() {
        return switch (shape) {
            case "SUM" -> "SELECT k, sum(v) FROM t";
            case "COUNT" -> "SELECT k, count() FROM t";
            case "MIXED" -> "SELECT k, avg(v), max(d), min(i) FROM t";
            case "MULTI" -> "SELECT k, sum(v), sum(d), count() FROM t";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }
}
