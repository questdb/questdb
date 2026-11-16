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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * Measures the fused GROUP BY over a hash join ({@code Async Hash Join Group By}), so that a
 * change to the build code it shares with the parallel hash join can be compared with an earlier
 * revision. It uses no API that the fused GROUP BY's first release lacks, so the same file compiles
 * and runs against both revisions; run each revision's build in fresh forks and compare the scores
 * with their errors.
 * <p>
 * Params:
 * <ul>
 *   <li>{@code shape}: {@code INT_SMALL} (INT key, 1,000-row build), {@code INT_LARGE} (INT key,
 *       build of {@code largeDimRows} rows, built in partition rounds), {@code LEFT} (LEFT JOIN,
 *       100,000-row build), {@code PROBE_FILTER} (JIT probe filter keeping 10% of the rows),
 *       {@code SYMBOL} (SYMBOL key translated into the build's dictionary), {@code STAGED} (LONG key,
 *       staged in a map), {@code COUNT} (count of the pairs, no aggregate column),
 *       {@code BUILD_REGEX} (a regex over a 400 to 600 character VARCHAR, which the JIT does not
 *       compile, filters a 250,000-row build over 25 daily partitions) and {@code BUILD_LIKE_TINY} (a
 *       LIKE filters a build of 500 rows over 50 daily partitions, ten rows each, for a probe of the
 *       fact table's first 10 seconds, an interval scan of about 2,800 rows). {@code SYMBOL_10M_*} join
 *       on a SYMBOL key fsym, whose SYMBOL column holds 10,000,000 texts, one per row every half second
 *       from 2024-01-01, and 3,000 more rows over the first hour of 2024-03-01, with a dimension of
 *       1,000 of its texts: {@code SYMBOL_10M_INTERVAL_D1K} reads the March hour,
 *       {@code SYMBOL_10M_BOUNDARY_BELOW} and {@code SYMBOL_10M_BOUNDARY_AT} fsym's first 39,062 and
 *       39,063 rows, one key each, either side of the 256 texts per probe row below which the probe
 *       translates the key through each worker's own cache rather than a cache that the dictionary
 *       sizes, and {@code SYMBOL_10M_FULL} every row.</li>
 *   <li>{@code fused}: false compiles the query with the fused GROUP BY off, the plan it replaces.</li>
 *   <li>{@code workers}: the size of the query worker pool, and the context's worker count.</li>
 *   <li>{@code factRows}, {@code largeDimRows}: table sizes. The fact table holds a million rows
 *       per hourly partition.</li>
 *   <li>{@code pageFrameMaxRows}: {@code cairo.sql.page.frame.max.rows}. The table scan splits
 *       each partition into about one frame per worker, of at most this many rows.</li>
 * </ul>
 * A trial-level routing guard reads the plan and fails the trial unless the query runs the fused
 * GROUP BY. Tables live under {@code questdb.bench.root} (a directory under
 * {@code java.io.tmpdir} by default), named after their sizes, and every trial with those sizes
 * reuses them; delete the directory to reclaim the space.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 2, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class HashJoinGroupByBenchmark {
    private static final String DATA_VERSION = "v3";
    private static final String FUSED_NODE = "Hash Join Group By";
    private static final String SYMBOL_10M_GROUP_BY = "SELECT d.v % 10 k, count() c FROM fsym f JOIN d1k d ON f.sym = d.sym";
    private static final String[] TABLES = {"ready", "fact", "d1", "d2", "d3", "dsym", "dreg", "dtiny", "fsym", "d1k"};

    @Param({"10000000"})
    public long factRows;

    @Param({"true"})
    public boolean fused;

    @Param({"5000000"})
    public long largeDimRows;

    @Param({"250000"})
    public int pageFrameMaxRows;

    @Param({"INT_SMALL", "INT_LARGE", "LEFT", "PROBE_FILTER", "SYMBOL", "STAGED", "COUNT", "BUILD_REGEX", "BUILD_LIKE_TINY",
            "SYMBOL_10M_INTERVAL_D1K", "SYMBOL_10M_BOUNDARY_BELOW", "SYMBOL_10M_BOUNDARY_AT", "SYMBOL_10M_FULL"})
    public String shape;

    @Param({"8"})
    public int workers;

    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private int[] types;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(HashJoinGroupByBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long run() throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final Record record = cursor.getRecord();
            long checksum = 0;
            while (cursor.hasNext()) {
                for (int i = 0; i < types.length; i++) {
                    final long value = switch (ColumnType.tagOf(types[i])) {
                        case ColumnType.LONG -> record.getLong(i);
                        case ColumnType.DOUBLE -> Double.doubleToLongBits(record.getDouble(i));
                        case ColumnType.SYMBOL -> {
                            final CharSequence symbol = record.getSymA(i);
                            yield symbol != null ? symbol.length() : -1;
                        }
                        default -> throw new UnsupportedOperationException("column type: " + ColumnType.nameOf(types[i]));
                    };
                    checksum = checksum * 31 + value;
                }
            }
            return checksum;
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        try {
            open();
        } catch (Throwable th) {
            // JMH runs no tear-down after a failed set-up, and the workers would keep the fork alive.
            tearDown();
            throw th;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        ctx = Misc.free(ctx);
        engine = Misc.free(engine);
    }

    private void createDimension(String name, long rows) throws SqlException {
        engine.execute("CREATE TABLE " + name + " AS (SELECT (x - 1)::INT k, x - 1 kl, ('c' || ((x - 1) % 16))::SYMBOL s, " +
                "(x * 7 % 1_000) / 1000.0 w FROM long_sequence(" + rows + "))", ctx);
    }

    private void createTables(Path root) throws SqlException {
        if (engine.getTableTokenIfExists("ready") != null) {
            return;
        }
        System.out.println("creating benchmark tables in " + root);
        for (String table : TABLES) {
            engine.execute("DROP TABLE IF EXISTS " + table, ctx);
        }
        engine.execute("CREATE TABLE fact AS (SELECT x id, (x * 7_919 % 1_000)::INT k1, " +
                "(x * 2_654_435_761 % 100_000)::INT k2, (x * 2_654_435_761 % 100_000) k2l, " +
                "(x * 2_654_435_761 % " + (largeDimRows * 6 / 5) + ")::INT k3, " +
                "(x * 31 % 100)::INT sel, (x * 17 % 1_000) / 1000.0 v, ('s' || (x % 64))::SYMBOL sym, " +
                "timestamp_sequence(0, 3_600) ts FROM long_sequence(" + factRows + ")) TIMESTAMP(ts) PARTITION BY HOUR", ctx);
        createDimension("d1", 1_000);
        createDimension("d2", 100_000);
        createDimension("d3", largeDimRows);
        // Symbols s70 down to s15, in a dictionary of their own order: fact rows with s0 to s14 find
        // no match, and s64 to s70 match no fact row.
        engine.execute("CREATE TABLE dsym AS (SELECT ('s' || (71 - x))::SYMBOL sym, ('cat' || (x % 5))::SYMBOL cat " +
                "FROM long_sequence(56))", ctx);
        // Builds of fewer rows than parallel.min.rows, over many daily partitions, with VARCHAR texts
        // that a regex or a LIKE filters: 250,000 rows of 400 to 600 characters over 25 days, and 500
        // rows of 8 to 16 characters over 50 days. The seeded generator gives every revision's
        // tables the same texts.
        ctx.setRandom(new Rnd(42, 4242));
        engine.execute("CREATE TABLE dreg AS (SELECT ((x - 1) * 4)::INT k, (x * 7 % 1_000) / 1000.0 w, rnd_str(400, 600, 0)::VARCHAR s, " +
                "timestamp_sequence(0, 8_640_000) ts FROM long_sequence(250_000)) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        engine.execute("CREATE TABLE dtiny AS (SELECT ((x - 1) * 16)::INT k, (x * 7 % 1_000) / 1000.0 w, rnd_str(8, 16, 0)::VARCHAR s, " +
                "timestamp_sequence(0, 8_640_000_000) ts FROM long_sequence(500)) TIMESTAMP(ts) PARTITION BY DAY", ctx);
        engine.execute("CREATE TABLE fsym (sym SYMBOL CAPACITY 16777216 NOCACHE, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
        engine.execute("INSERT INTO fsym SELECT 'k' || x, x, timestamp_sequence('2024-01-01', 500_000) FROM long_sequence(10_000_000)", ctx);
        engine.execute("INSERT INTO fsym SELECT 'k' || ((x - 1) % 1_000 + 1), x, timestamp_sequence('2024-03-01', 1_200_000) FROM long_sequence(3_000)", ctx);
        engine.execute("CREATE TABLE d1k AS (SELECT ('k' || x)::SYMBOL sym, x v FROM long_sequence(1_000))", ctx);
        engine.execute("CREATE TABLE ready (x INT)", ctx);
    }

    private void open() throws Exception {
        final Path root = Paths.get(System.getProperty("questdb.bench.root", Paths.get(
                System.getProperty("java.io.tmpdir"),
                "questdb-hash-join-group-by-bench-" + DATA_VERSION + "-f" + factRows + "-d" + largeDimRows
        ).toString()));
        Files.createDirectories(root);
        final int queueCapacity = Numbers.ceilPow2(Math.min(4 * workers, 256));
        final CairoConfiguration configuration = new DefaultCairoConfiguration(root.toString()) {
            @Override
            public int getPageFrameReduceQueueCapacity() {
                return queueCapacity;
            }

            @Override
            public int getPageFrameReduceShardCount() {
                return Math.min(workers, 4);
            }

            @Override
            public int getSqlPageFrameMaxRows() {
                return pageFrameMaxRows;
            }

            @Override
            public int getSqlParallelFilterDispatchLimit() {
                return Math.min(workers, 32);
            }
        };
        engine = new CairoEngine(configuration);
        workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "hashjoingroupbybench";
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        WorkerPoolUtils.setupQueryJobs(workerPool, engine);
        workerPool.start();

        ctx = new SqlExecutionContextImpl(engine, workers);
        ctx.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        ctx.setParallelHashJoinGroupByEnabled(true);
        createTables(root);

        ctx.setParallelHashJoinGroupByEnabled(fused);
        factory = engine.select(sql(), ctx);
        final TextPlanSink planSink = new TextPlanSink();
        planSink.of(factory, ctx);
        final String text = planSink.getSink().toString();
        if (fused != text.contains(FUSED_NODE)) {
            throw new IllegalStateException("routing drift: shape=" + shape + " fused=" + fused + '\n' + text);
        }
        final RecordMetadata metadata = factory.getMetadata();
        types = new int[metadata.getColumnCount()];
        for (int i = 0; i < types.length; i++) {
            types[i] = metadata.getColumnType(i);
        }
    }

    private String sql() {
        return switch (shape) {
            case "INT_SMALL" -> "SELECT d1.s, count(*) c, sum(f.v) sv FROM fact f JOIN d1 ON f.k1 = d1.k GROUP BY d1.s";
            case "INT_LARGE" -> "SELECT d3.s, count(*) c, sum(f.v) sv, sum(d3.w) sw FROM fact f JOIN d3 ON f.k3 = d3.k GROUP BY d3.s";
            case "LEFT" -> "SELECT f.sym, count(*) c, sum(d2.w) sw FROM fact f LEFT JOIN d2 ON f.k2 = d2.k GROUP BY f.sym";
            case "PROBE_FILTER" -> "SELECT d2.s, count(*) c, sum(f.v) sv FROM fact f JOIN d2 ON f.k2 = d2.k WHERE f.sel < 10 GROUP BY d2.s";
            case "SYMBOL" -> "SELECT ds.cat, count(*) c, sum(f.v) sv FROM fact f JOIN dsym ds ON f.sym = ds.sym GROUP BY ds.cat";
            case "STAGED" -> "SELECT d2.s, count(*) c, sum(f.v) sv FROM fact f JOIN d2 ON f.k2l = d2.kl GROUP BY d2.s";
            case "COUNT" -> "SELECT count(*) FROM fact f JOIN d3 ON f.k3 = d3.k";
            case "BUILD_REGEX" -> "SELECT f.sym, count(*) c, sum(d.w) sw FROM fact f JOIN dreg d ON f.k3 = d.k WHERE d.s ~ '[B-C][X-Z][B-C][X-Z]' GROUP BY f.sym";
            // A LEFT join builds the filtered dimension whatever the probe's interval holds.
            case "BUILD_LIKE_TINY" -> "SELECT f.sym, count(*) c, sum(d.w) sw FROM fact f LEFT JOIN (SELECT * FROM dtiny WHERE s LIKE '%B%') d ON f.k1 = d.k " +
                    "WHERE f.ts IN '1970-01-01T00:00:00;10s' GROUP BY f.sym";
            // fsym's timestamps step by half a second, so ceil(10,000,000 / 256) = 39,063 rows end at 05:25:31.5.
            case "SYMBOL_10M_INTERVAL_D1K" -> SYMBOL_10M_GROUP_BY + " WHERE f.ts IN '2024-03-01T00' GROUP BY k";
            case "SYMBOL_10M_BOUNDARY_BELOW" -> SYMBOL_10M_GROUP_BY + " WHERE f.ts < '2024-01-01T05:25:31.000000Z' GROUP BY k";
            case "SYMBOL_10M_BOUNDARY_AT" -> SYMBOL_10M_GROUP_BY + " WHERE f.ts < '2024-01-01T05:25:31.500000Z' GROUP BY k";
            case "SYMBOL_10M_FULL" -> SYMBOL_10M_GROUP_BY + " GROUP BY k";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }
}
