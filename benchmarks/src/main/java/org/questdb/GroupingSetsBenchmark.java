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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.groupby.GroupingSetsRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupingSetsRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionAllRecordCursorFactory;
import io.questdb.griffin.engine.union.UnionSymbolCastRecordCursorFactory;
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
 * Compares GROUP BY ROLLUP with the equivalent UNION ALL of plain GROUP BY queries, one per
 * grouping set, which is the rewrite users write without grouping sets.
 * <p>
 * Params:
 * <ul>
 *   <li>{@code form}: {@code GROUPING_SETS} or {@code UNION_ALL}.</li>
 *   <li>{@code shape}: {@code ROLLUP_A} (one INT key), {@code ROLLUP_S} (one SYMBOL key),
 *       {@code ROLLUP_AB} (two INT keys) and {@code FILTERED_ROLLUP_SA} (a SYMBOL and an INT
 *       key under a WHERE filter).</li>
 *   <li>{@code workers}: {@code 0} disables parallel GROUP BY; any other value runs a query
 *       worker pool of that size.</li>
 *   <li>{@code rowCount}: table row count.</li>
 * </ul>
 * <p>
 * A {@code @Setup(Level.Trial)} routing guard walks the compiled factory chain to confirm each
 * form runs on the factory it is meant to measure.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class GroupingSetsBenchmark {

    @Param({"GROUPING_SETS", "UNION_ALL"})
    public String form;

    @Param({"20000000"})
    public int rowCount;

    @Param({"ROLLUP_A", "ROLLUP_S", "ROLLUP_AB", "FILTERED_ROLLUP_SA"})
    public String shape;

    @Param({"0", "4"})
    public int workers;

    private SqlCompilerImpl compiler;
    private SqlExecutionContext ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path tempRoot;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(GroupingSetsBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public void run(Blackhole bh) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            // Every shape projects sum(v) last.
            final int sumColumn = factory.getMetadata().getColumnCount() - 1;
            while (cursor.hasNext()) {
                bh.consume(record.getLong(sumColumn));
            }
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        tempRoot = java.nio.file.Files.createTempDirectory("groupingsetsbench-");
        final CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString());
        engine = new CairoEngine(configuration);

        if (workers > 0) {
            workerPool = new WorkerPool(new WorkerPoolConfiguration() {
                @Override
                public String getPoolName() {
                    return "groupingsetsbench";
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
        ctx.setParallelGroupByEnabled(workers > 0);
        compiler = new SqlCompilerImpl(engine);

        engine.execute(
                "CREATE TABLE t AS (" +
                        "SELECT (x % 100)::INT a, (x % 37)::INT b, rnd_symbol(50, 4, 8, 0) s, x v, " +
                        "(x * 1_000L)::TIMESTAMP ts FROM long_sequence(" + rowCount + ")" +
                        ") TIMESTAMP(ts) PARTITION BY DAY",
                ctx
        );

        factory = compiler.compile(buildSql(), ctx).getRecordCursorFactory();
        assertRouting(factory);
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
        RecordCursorFactory cur = root;
        while (cur != null) {
            final boolean isExpected = "GROUPING_SETS".equals(form)
                    ? (workers > 0 ? cur instanceof AsyncGroupingSetsRecordCursorFactory : cur instanceof GroupingSetsRecordCursorFactory)
                    : cur instanceof UnionAllRecordCursorFactory || cur instanceof UnionSymbolCastRecordCursorFactory;
            if (isExpected) {
                return;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        throw new IllegalStateException(
                "routing drift: form=" + form + " shape=" + shape + " workers=" + workers
                        + " root=" + root.getClass().getSimpleName()
        );
    }

    private String buildSql() {
        final boolean isGroupingSets = "GROUPING_SETS".equals(form);
        return switch (shape) {
            case "ROLLUP_A" -> isGroupingSets
                    ? "SELECT a, sum(v) total FROM t GROUP BY ROLLUP(a)"
                    : "SELECT a, sum(v) total FROM t GROUP BY a " +
                      "UNION ALL SELECT NULL::INT a, sum(v) total FROM t";
            case "ROLLUP_S" -> isGroupingSets
                    ? "SELECT s, sum(v) total FROM t GROUP BY ROLLUP(s)"
                    : "SELECT s, sum(v) total FROM t GROUP BY s " +
                      "UNION ALL SELECT NULL::SYMBOL s, sum(v) total FROM t";
            case "ROLLUP_AB" -> isGroupingSets
                    ? "SELECT a, b, sum(v) total FROM t GROUP BY ROLLUP(a, b)"
                    : "SELECT a, b, sum(v) total FROM t GROUP BY a, b " +
                      "UNION ALL SELECT a, NULL::INT b, sum(v) total FROM t GROUP BY a " +
                      "UNION ALL SELECT NULL::INT a, NULL::INT b, sum(v) total FROM t";
            case "FILTERED_ROLLUP_SA" -> isGroupingSets
                    ? "SELECT s, a, sum(v) total FROM t WHERE b < 20 GROUP BY ROLLUP(s, a)"
                    : "SELECT s, a, sum(v) total FROM t WHERE b < 20 GROUP BY s, a " +
                      "UNION ALL SELECT s, NULL::INT a, sum(v) total FROM t WHERE b < 20 GROUP BY s " +
                      "UNION ALL SELECT NULL::SYMBOL s, NULL::INT a, sum(v) total FROM t WHERE b < 20";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }
}
