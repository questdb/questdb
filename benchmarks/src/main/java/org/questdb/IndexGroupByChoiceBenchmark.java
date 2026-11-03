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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/**
 * GROUP BY with {@code =} or {@code IN} on a POSTING-indexed symbol: the Index Or Scan choice and
 * its selectivity estimate, against the plan of the build before them. The source compiles on that
 * build too: the configuration declares the switches it lacks as plain methods, and the SQL is
 * plain.
 * <p>
 * A cell is {@code <shape>-<key>-<partitions>-<switches>}:
 * <ul>
 *   <li>shape {@code eq}: {@code count(), sum(a) ... = 'k7'}; {@code keyed}: the same grouped by the
 *   key; {@code in2}: {@code count(), sum(a) ... IN} two keys;</li>
 *   <li>key {@code 1}: a 1% key of {@code psym} ({@code 'k' || x % 100}); {@code 20}: a 20% key of
 *   {@code p5} ({@code 'k' || x % 5});</li>
 *   <li>1M rows in 100, 1000 or 4000 hourly partitions;</li>
 *   <li>switches {@code on}: the defaults; {@code bothOff}: Index Or Scan and the parallel index
 *   GROUP BY off; {@code allOff}: the frame dispatch switch off as well. The last two need the
 *   build with the switches.</li>
 * </ul>
 * IN-list cells, {@code inK<K>-<partitions>-<switches>}: 100 partitions of 1000 rows, 200 of them
 * rare keys k0..k19999, each in one row, in symbol-id order; the list holds the first K. With one
 * partition, 20,000 rows of one key each. They time the estimate's key resolution and sort.
 * <p>
 * Run every cell with {@code execute}, and {@code compile} with a few cells, e.g.
 * {@code java -jar benchmarks/target/benchmarks.jar 'IndexGroupByChoiceBenchmark.compile' -p cell=eq-1-1000-on}.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class IndexGroupByChoiceBenchmark {
    private static final int ROWS = 1_000_000;
    private static final int WORKERS = 4;

    @Param({
            "eq-1-100-on", "eq-1-1000-on", "eq-1-4000-on",
            "keyed-1-100-on", "keyed-1-1000-on", "keyed-1-4000-on",
            "in2-1-100-on", "in2-1-1000-on", "in2-1-4000-on",
            "eq-20-100-on", "eq-20-1000-on", "eq-20-4000-on",
            "keyed-20-100-on", "keyed-20-1000-on", "keyed-20-4000-on",
            "in2-20-100-on", "in2-20-1000-on", "in2-20-4000-on",
            "inK50-100-on", "inK500-100-on", "inK5000-100-on", "inK20000-100-on", "inK20000-1-on",
    })
    public String cell;

    private SqlCompiler compiler;
    private SqlExecutionContext context;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private WorkerPool pool;
    private String root;
    private String sql;

    public static void main(String[] args) throws RunnerException {
        final Options opt = new OptionsBuilder()
                .include(IndexGroupByChoiceBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long compile() throws Exception {
        try (RecordCursorFactory f = compiler.compile(sql, context).getRecordCursorFactory()) {
            return f.getMetadata().getColumnCount();
        }
    }

    @Benchmark
    public long execute() {
        return drain(factory, context);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        final String[] parts = cell.split("-");
        final String switches = parts[parts.length - 1];
        final boolean isOn = "on".equals(switches);
        final boolean isAllOff = "allOff".equals(switches);
        root = System.getProperty("java.io.tmpdir") + java.io.File.separator + "index-groupby-choice-" + ProcessHandle.current().pid();
        try (Path path = new Path().of(root)) {
            Files.mkdirs(path.slash(), 509);
        }
        final DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(root) {
            // a returned reader keeps its partitions open, so that each open does not reopen
            // (and log) hundreds of partitions: that cost is the same before and after, and
            // would hide the difference
            @Override
            public int getInactiveReaderMaxOpenPartitions() {
                return Integer.MAX_VALUE;
            }

            // declared without @Override: the build before the choice has none of these
            @SuppressWarnings("unused")
            public boolean isSqlIndexScanChoiceEnabled() {
                return isOn;
            }

            @SuppressWarnings("unused")
            public boolean isSqlParallelFrameDispatchEnabled() {
                return !isAllOff;
            }

            @SuppressWarnings("unused")
            public boolean isSqlParallelIndexGroupByEnabled() {
                return isOn;
            }
        };
        engine = new CairoEngine(configuration);
        context = new SqlExecutionContextImpl(engine, WORKERS).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null,
                null,
                -1,
                null
        );
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "index-groupby-choice";
            }

            @Override
            public int getWorkerCount() {
                return WORKERS;
            }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        compiler = engine.getSqlCompiler();

        if (parts[0].startsWith("inK")) {
            final int keys = Integer.parseInt(parts[0].substring(3));
            final int partitions = Integer.parseInt(parts[1]);
            engine.execute("CREATE TABLE t (ts TIMESTAMP, psym SYMBOL INDEX TYPE POSTING, a DOUBLE) TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL", context);
            if (partitions == 1) {
                engine.execute("INSERT INTO t SELECT ((x - 1) * 100_000L)::TIMESTAMP, 'k' || (x - 1), (x % 1000) / 8.0 FROM long_sequence(20_000)", context);
            } else {
                // rows 0-199 of every 1000 are the rare keys, in order
                engine.execute("INSERT INTO t SELECT ((x - 1) * 3_600_000L)::TIMESTAMP, "
                        + "CASE WHEN (x - 1) % 1000 < 200 THEN 'k' || (((x - 1) / 1000) * 200 + (x - 1) % 1000) ELSE 'other' END, "
                        + "(x % 1000) / 8.0 FROM long_sequence(" + partitions * 1000 + ")", context);
            }
            final StringBuilder in = new StringBuilder();
            for (int i = 0; i < keys; i++) {
                in.append(i == 0 ? "'k" : ", 'k").append(i).append('\'');
            }
            sql = "SELECT count(), sum(a) FROM t WHERE psym IN (" + in + ")";
        } else {
            final int partitions = Integer.parseInt(parts[2]);
            engine.execute("CREATE TABLE t (ts TIMESTAMP, psym SYMBOL INDEX TYPE POSTING, p5 SYMBOL INDEX TYPE POSTING, a DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL", context);
            engine.execute("INSERT INTO t SELECT ((x - 1) * " + (partitions * 3_600_000_000L / ROWS) + "L)::TIMESTAMP, "
                    + "'k' || (x % 100), 'k' || (x % 5), (x % 1000) / 8.0 FROM long_sequence(" + ROWS + ")", context);
            final String column = "1".equals(parts[1]) ? "psym" : "p5";
            sql = switch (parts[0]) {
                case "eq" -> "SELECT count(), sum(a) FROM t WHERE " + column + " = 'k2'";
                case "keyed" -> "SELECT " + column + ", count(), sum(a) FROM t WHERE " + column + " = 'k2'";
                case "in2" -> "SELECT count(), sum(a) FROM t WHERE " + column + " IN ('k2', 'k3')";
                default -> throw new IllegalArgumentException(cell);
            };
        }
        engine.releaseAllWriters();
        factory = compiler.compile(sql, context).getRecordCursorFactory();
        // the plan must be the one the cell names
        final String plan = plan(sql);
        if (isOn && plan.contains("Async Index Group By") != plan.contains("Index Or Scan")) {
            throw new IllegalStateException("unexpected plan for " + cell + ":\n" + plan);
        }
        if (!isOn && (plan.contains("Index Or Scan") || plan.contains("Async Index Group By"))) {
            throw new IllegalStateException("switches not off for " + cell + ":\n" + plan);
        }
        drain(factory, context);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
        compiler = Misc.free(compiler);
        if (pool != null) {
            pool.halt();
            pool = null;
        }
        engine = Misc.free(engine);
        try (Path path = new Path().of(root)) {
            Files.rmdir(path.slash(), true);
        }
        LogFactory.haltInstance();
    }

    private static long drain(RecordCursorFactory factory, SqlExecutionContext context) {
        long sum = 0;
        try (RecordCursor cursor = factory.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sum += record.getLong(factory.getMetadata().getColumnCount() - 2);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return sum;
    }

    private String plan(String query) throws Exception {
        final StringBuilder sink = new StringBuilder();
        try (RecordCursorFactory f = compiler.compile("EXPLAIN " + query, context).getRecordCursorFactory();
             RecordCursor cursor = f.getCursor(context)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sink.append(record.getStrA(0)).append('\n');
            }
        }
        return sink.toString();
    }
}
