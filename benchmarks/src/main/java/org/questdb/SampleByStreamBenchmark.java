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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class SampleByStreamBenchmark {
    @Param({"1h", "1s", "1T"})
    public String bucket;
    @Param({"native"})
    public String dataFormat;
    @Param({"stream"})
    public String expectedStrategy;
    @Param({"7"})
    public int filterModulo;
    @Param({"100000"})
    public int frameRows;
    @Param({"0"})
    public int nullEvery;
    @Param({"2000000"})
    public int rowCount;
    @Param({"64"})
    public int symbolCount;
    @Param({"UTC"})
    public String timeZone;
    @Param({"0", "4"})
    public int workers;
    @Param({"basic", "count", "double", "stats", "decimal", "first_last", "expression", "filtered", "filtered_desc", "hash_filtered", "keyed", "keyed_filtered", "keyed_string", "keyed_multi"})
    public String workload;
    private int[] columnTypes;
    private SqlCompiler compiler;
    private SqlExecutionContextImpl context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private Path root;
    private WorkerPool workerPool;

    @Benchmark
    public void aggregate(Blackhole blackhole) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                for (int i = 0; i < columnTypes.length; i++) {
                    switch (columnTypes[i]) {
                        case ColumnType.TIMESTAMP -> blackhole.consume(record.getTimestamp(i));
                        case ColumnType.LONG -> blackhole.consume(record.getLong(i));
                        case ColumnType.INT, ColumnType.SYMBOL -> blackhole.consume(record.getInt(i));
                        case ColumnType.DOUBLE -> blackhole.consume(record.getDouble(i));
                        case ColumnType.VARCHAR -> blackhole.consume(record.getVarcharA(i));
                        default -> throw new IllegalStateException("unexpected result type: " + columnTypes[i]);
                    }
                }
            }
        }
    }

    @Setup
    public void setUp() throws Exception {
        root = Files.createTempDirectory("sample-by-stream-");
        try {
            final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
                @Override
                public boolean isSqlParallelGroupByEnabled() {
                    return workers > 0;
                }

                @Override
                public int getSqlPageFrameMaxRows() {
                    return frameRows;
                }

                @Override
                public int getSqlPageFrameMinRows() {
                    return frameRows;
                }
            };
            engine = new CairoEngine(configuration);
            if (workers > 0) {
                workerPool = new WorkerPool(() -> workers);
                WorkerPoolUtils.setupQueryJobs(workerPool, engine);
                workerPool.start();
            }
            context = new SqlExecutionContextImpl(engine, Math.max(1, workers))
                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext());
            compiler = engine.getSqlCompiler();
            final String v = nullable("x % 97");
            final String d = nullable("(x % 31)::double");
            engine.execute("create table trades as (select "
                    + "timestamp_sequence('2024-01-01'::timestamp, 1000L) ts, "
                    + v + " v, " + d + " d, (x % " + symbolCount + ")::symbol sym, "
                    + v + "::decimal(18,2) price from long_sequence(" + rowCount + ")) "
                    + "timestamp(ts) partition by day", context);
            if ("parquet".equals(dataFormat)) {
                engine.execute("alter table trades convert partition to parquet where ts >= 0", context);
            } else if (!"native".equals(dataFormat)) {
                throw new IllegalArgumentException("unknown data format: " + dataFormat);
            }
            final String select = switch (workload) {
                case "basic", "filtered", "filtered_desc" -> "count(*), sum(v), avg(d)";
                case "count" -> "count(*)";
                case "double" -> "sum(d), avg(d), min(d), max(d)";
                case "stats" -> "stddev_pop(d), corr(d, v), weighted_avg(d, v)";
                case "decimal" -> "sum(price)::double, avg(price)::double, min(price)::double, max(price)::double";
                case "first_last" -> "first(v), last(v), first_not_null(v), last_not_null(v), first(sym), last(sym)";
                case "expression" -> "sum(v + 1), avg(d * 2), sum(sqrt(d))";
                case "keyed", "keyed_filtered", "hash_filtered" -> "sym, count(*), sum(v), avg(d)";
                case "keyed_string" -> "sym::varchar name, count(*), sum(v), avg(d)";
                case "keyed_multi" -> "sym, v % 7 k, count(*), sum(v), avg(d)";
                default -> throw new IllegalArgumentException("unknown workload: " + workload);
            };
            final boolean isHashControl = "hash_filtered".equals(workload);
            final boolean isFiltered = workload.contains("filtered");
            factory = compiler.compile("select " + (isHashControl ? "" : "ts, ") + select + " from trades "
                    + (isFiltered ? "where v % " + filterModulo + " = 0 " : "")
                    + (isHashControl ? "" : "sample by " + bucket + " align to calendar time zone '" + timeZone + "'")
                    + ("filtered_desc".equals(workload) ? " order by ts desc limit 10" : ""), context).getRecordCursorFactory();
            columnTypes = new int[factory.getMetadata().getColumnCount()];
            for (int i = 0; i < columnTypes.length; i++) {
                columnTypes[i] = ColumnType.tagOf(factory.getMetadata().getColumnType(i));
            }
            final TextPlanSink plan = new TextPlanSink();
            plan.of(factory, context);
            boolean stream = false;
            for (int i = 1; i <= plan.getLineCount(); i++) {
                stream |= plan.getLine(i).toString().contains("Stream Group By");
            }
            final boolean expectStream = "stream".equals(expectedStrategy) && !isHashControl;
            if (stream != expectStream) {
                throw new IllegalStateException("unexpected aggregation strategy: " + expectedStrategy);
            }
        } catch (Throwable th) {
            try {
                tearDown();
            } catch (Throwable cleanupFailure) {
                th.addSuppressed(cleanupFailure);
            }
            throw th;
        }
    }

    @TearDown
    public void tearDown() throws Exception {
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        context = Misc.free(context);
        engine = Misc.free(engine);
        if (root != null) {
            try (var paths = Files.walk(root)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(path);
                }
            }
            root = null;
        }
    }

    private String nullable(String value) {
        return nullEvery > 0 ? "case when x % " + nullEvery + " = 0 then null else " + value + " end" : value;
    }
}
