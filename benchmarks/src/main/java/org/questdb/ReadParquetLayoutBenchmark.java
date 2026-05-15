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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

/**
 * Compares read_parquet performance across five storage layouts of the same
 * dataset. All five hold 30 days * 4 symbols * 50,000 rows = 6,000,000 rows.
 *
 * <pre>
 *   L1_SINGLE_FILE   - one parquet file with the entire dataset
 *   L2_NATIVE_NONE   - QuestDB native table, PARTITION BY NONE
 *   L3_HIVE_DAY      - 30 parquet files, hive 'day=YYYY-MM-DD/data.parquet'
 *   L4_NATIVE_DAY    - QuestDB native table, PARTITION BY DAY (30 partitions)
 *   L5_HIVE_DAY_SYM  - 120 parquet files, hive 'day=.../symbol=.../data.parquet'
 * </pre>
 *
 * Data is built into a stable directory once - subsequent runs skip the
 * generation step. Each (layout, query) parameter combination becomes one
 * JMH trial with its own pre-compiled factory.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ReadParquetLayoutBenchmark {

    private static final String BENCH_DIR = System.getProperty("rpbench.dir", "/tmp/qdb-rp-bench");
    private static final int DAYS = Integer.getInteger("rpbench.days", 30);
    private static final int ROWS_PER_DAY_PER_SYMBOL = Integer.getInteger("rpbench.rows", 50_000);
    private static final LocalDate START_DATE = LocalDate.of(2026, 1, 1);
    private static final String[] SYMBOLS = {"BTC", "ETH", "USDC", "USDT"};
    private static final int WORKER_COUNT = 4;
    private static final String READY_MARKER = "READY";

    @Param({"L1_SINGLE_FILE", "L2_NATIVE_NONE", "L3_HIVE_DAY", "L4_NATIVE_DAY", "L5_HIVE_DAY_SYM"})
    public Layout layout;

    @Param({"COUNT_ALL", "SUM_ALL", "FILTER_ONE_DAY", "FILTER_DAY_RANGE", "GROUP_BY_DAY", "FILTER_SYMBOL", "FILTER_DAY_AND_SYMBOL", "LIMIT100"})
    public Query query;

    // CairoEngine holds a file lock on its db root so we can't have two engines
    // open against the same /tmp/qdb-rp-bench in one JVM. JMH would otherwise try
    // to spin up a fresh engine per trial (40 combos in this matrix). Share one
    // engine, one worker pool, one execution context across every trial in the
    // JVM; only the per-query factory is per-trial.
    private static volatile CairoEngine sharedEngine;
    private static volatile SqlExecutionContext sharedCtx;
    private static volatile WorkerPool sharedPool;

    private RecordCursorFactory factory;

    public static void main(String[] args) throws Exception {
        // Build the dataset up front (outside the JMH lifecycle) so the message
        // shows in the console. @Setup also calls ensureDataBuilt as a safety
        // net for forked JVMs.
        ensureDataBuilt();

        final int forks = Integer.getInteger("rpbench.forks", 1);
        Options opt = new OptionsBuilder()
                .include(ReadParquetLayoutBenchmark.class.getSimpleName())
                .warmupIterations(2)
                .measurementIterations(3)
                .forks(forks)
                .jvmArgs(
                        "-XX:+UseParallelGC",
                        "--sun-misc-unsafe-memory-access=allow",
                        "--enable-native-access=ALL-UNNAMED",
                        "--add-opens=java.base/java.lang=ALL-UNNAMED",
                        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                        "--add-opens=java.base/java.nio=ALL-UNNAMED",
                        "--add-opens=java.base/java.time.zone=ALL-UNNAMED"
                )
                .build();

        new Runner(opt).run();
        LogFactory.haltInstance();
    }

    @Benchmark
    public long run() throws SqlException {
        // Iterate the cursor and count rows. The cursor's hasNext() drives page-frame
        // decode for SELECT * queries (the framework decodes every projected column
        // even if we don't touch them per-row); for SELECT count(*) / sum(...) the
        // cursor only does the work the query asks for. Returning the row count is
        // enough to keep the JIT honest - we don't need column-by-column reads.
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(sharedCtx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        ensureSharedEngine();
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(sharedEngine)) {
            factory = compiler.compile(sqlFor(layout, query), sharedCtx).getRecordCursorFactory();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        factory = Misc.free(factory);
    }

    private static synchronized void ensureSharedEngine() throws Exception {
        if (sharedEngine != null) {
            return;
        }
        // Safety net for forked JVMs - main() pre-builds but a fork starts fresh.
        ensureDataBuilt();
        final CairoConfiguration configuration = newConfiguration();
        final CairoEngine engine = new CairoEngine(configuration);
        engine.load();

        final WorkerPool pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public String getPoolName() {
                return "rp-bench";
            }

            @Override
            public int getWorkerCount() {
                return WORKER_COUNT;
            }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();

        final SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, WORKER_COUNT)
                .with(
                        configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                        null,
                        null,
                        -1,
                        null
                );

        sharedEngine = engine;
        sharedPool = pool;
        sharedCtx = ctx;

        // The forked JVM exits after JMH reports results - no Runtime shutdown hook
        // is strictly required, but free explicitly so a profiler or leak-checker
        // sees clean teardown.
        Runtime.getRuntime().addShutdownHook(new Thread(ReadParquetLayoutBenchmark::tearDownShared, "rp-bench-shutdown"));
    }

    private static synchronized void tearDownShared() {
        if (sharedPool != null) {
            sharedPool.halt();
            sharedPool = null;
        }
        if (sharedEngine != null) {
            sharedEngine.close();
            sharedEngine = null;
        }
        sharedCtx = null;
    }

    static String sqlFor(Layout layout, Query query) {
        final String tableRef = tableRefFor(layout);
        final boolean hive = layout == Layout.L3_HIVE_DAY || layout == Layout.L5_HIVE_DAY_SYM;
        switch (query) {
            case COUNT_ALL:
                return "select count(*) from " + tableRef;
            case SUM_ALL:
                return "select sum(price), sum(amount) from " + tableRef;
            case FILTER_ONE_DAY:
                // Hive layouts filter the partition column (file-level pruning).
                // Native and single-file layouts filter the timestamp directly.
                return hive
                        ? "select * from " + tableRef + " where day = '2026-01-15'"
                        : "select * from " + tableRef + " where ts in '2026-01-15'";
            case FILTER_DAY_RANGE:
                return hive
                        ? "select * from " + tableRef + " where day between '2026-01-10' and '2026-01-12'"
                        : "select * from " + tableRef + " where ts >= '2026-01-10' and ts < '2026-01-13'";
            case GROUP_BY_DAY:
                return hive
                        ? "select day, count(*) c from " + tableRef + " group by day order by day"
                        : "select cast(ts as date) d, count(*) c from " + tableRef + " group by d order by d";
            case FILTER_SYMBOL:
                return "select * from " + tableRef + " where symbol = 'BTC'";
            case FILTER_DAY_AND_SYMBOL:
                return hive
                        ? "select * from " + tableRef + " where day = '2026-01-15' and symbol = 'BTC'"
                        : "select * from " + tableRef + " where ts in '2026-01-15' and symbol = 'BTC'";
            case LIMIT100:
                return "select * from " + tableRef + " limit 100";
            default:
                throw new IllegalArgumentException("unknown query: " + query);
        }
    }

    static String tableRefFor(Layout layout) {
        switch (layout) {
            case L1_SINGLE_FILE:
                return "read_parquet('single.parquet')";
            case L2_NATIVE_NONE:
                return "src_native";
            case L3_HIVE_DAY:
                return "read_parquet('hive_day/day=*/data.parquet')";
            case L4_NATIVE_DAY:
                return "src_native_part";
            case L5_HIVE_DAY_SYM:
                return "read_parquet('hive_day_sym/day=*/symbol=*/data.parquet')";
            default:
                throw new IllegalArgumentException("unknown layout: " + layout);
        }
    }

    private static void buildDataInDir(File benchDir) throws SqlException {
        System.out.println("[bench] Building dataset in " + benchDir);
        final long t0 = System.nanoTime();

        final CairoConfiguration configuration = new DefaultCairoConfiguration(benchDir.getAbsolutePath()) {
            @Override
            public @org.jetbrains.annotations.NotNull CharSequence getSqlCopyInputRoot() {
                return benchDir.getAbsolutePath();
            }
        };
        try (CairoEngine engine = new CairoEngine(configuration)) {
            engine.load();
            final SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(
                            configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null,
                            null,
                            -1,
                            null
                    );
            // src_native: the workload, PARTITION BY NONE so we can dump partition 0
            // as a single parquet file. Also serves as L2 at query time.
            final long totalRows = (long) DAYS * SYMBOLS.length * ROWS_PER_DAY_PER_SYMBOL;
            final long startMicros = toMicros(START_DATE);
            final long dayMicros = 86_400_000_000L;
            // Symbols cycle by row index. Timestamps advance evenly across the
            // dataset so every day gets DAYS_ROWS rows (SYMBOLS.length groups).
            engine.execute(
                    "create table src_native as (" +
                            "select" +
                            " timestamp_sequence(" + startMicros + ", " + (dayMicros / (SYMBOLS.length * ROWS_PER_DAY_PER_SYMBOL)) + ") ts," +
                            " case when x % 4 = 1 then '" + SYMBOLS[0] +
                            "' when x % 4 = 2 then '" + SYMBOLS[1] +
                            "' when x % 4 = 3 then '" + SYMBOLS[2] +
                            "' else '" + SYMBOLS[3] + "' end::varchar symbol," +
                            " cast(timestamp_sequence(" + startMicros + ", " + (dayMicros / (SYMBOLS.length * ROWS_PER_DAY_PER_SYMBOL)) + ") as date) day," +
                            " (100.0 + rnd_double() * 50.0) price," +
                            " (1 + abs(rnd_int()) % 1000)::long amount" +
                            " from long_sequence(" + totalRows + ")" +
                            ") timestamp(ts) partition by none bypass wal",
                    ctx
            );

            // src_native_part: same data, PARTITION BY DAY. Serves both L4 directly
            // and as the source for writing hive day-partitioned parquet files in L3.
            engine.execute(
                    "create table src_native_part as (" +
                            "select * from src_native" +
                            ") timestamp(ts) partition by day bypass wal",
                    ctx
            );

            // L1: dump all 6M rows to a single parquet file.
            System.out.println("[bench]   writing L1 single file...");
            writePartition(engine, "src_native", 0, new File(benchDir, "single.parquet"));

            // L3: 30 files, hive layout 'hive_day/day=YYYY-MM-DD/data.parquet'.
            System.out.println("[bench]   writing L3 hive day partitioned (" + DAYS + " files)...");
            writeHiveByDay(engine, "src_native_part", new File(benchDir, "hive_day"));

            // L5: 30 * 4 = 120 files, 'hive_day_sym/day=.../symbol=.../data.parquet'.
            // Build a per-symbol temp table partitioned by day, then encode each of
            // its DAY partitions to its symbol subdirectory.
            System.out.println("[bench]   writing L5 hive day+symbol partitioned (" + (DAYS * SYMBOLS.length) + " files)...");
            for (String sym : SYMBOLS) {
                final String tmp = "tmp_" + sym;
                engine.execute(
                        "create table " + tmp + " as (" +
                                "select * from src_native where symbol = '" + sym + "'" +
                                ") timestamp(ts) partition by day bypass wal",
                        ctx
                );
                File symRoot = new File(benchDir, "hive_day_sym");
                writeHiveByDayUnderSymbol(engine, tmp, sym, symRoot);
                engine.execute("drop table " + tmp, ctx);
            }
        }
        try {
            Files.writeString(new File(benchDir, READY_MARKER).toPath(), "ok");
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("[bench] Dataset ready in " + ((System.nanoTime() - t0) / 1_000_000) + " ms");
    }

    private static void ensureDataBuilt() throws Exception {
        final File benchDir = new File(BENCH_DIR);
        final File ready = new File(benchDir, READY_MARKER);
        if (ready.exists()) {
            System.out.println("[bench] Reusing existing dataset at " + BENCH_DIR);
            return;
        }
        if (benchDir.exists()) {
            // Partial / stale dataset - wipe and rebuild.
            deleteRecursively(benchDir);
        }
        if (!benchDir.mkdirs()) {
            throw new RuntimeException("could not create " + benchDir);
        }
        buildDataInDir(benchDir);
    }

    private static void deleteRecursively(File f) {
        if (f.isDirectory()) {
            File[] children = f.listFiles();
            if (children != null) {
                for (File c : children) {
                    deleteRecursively(c);
                }
            }
        }
        if (!f.delete() && f.exists()) {
            throw new RuntimeException("could not delete " + f);
        }
    }

    private static long toMicros(LocalDate date) {
        return date.toEpochDay() * 86_400_000_000L;
    }

    private static void writeHiveByDay(CairoEngine engine, String tableName, File rootDir) {
        try (Path path = new Path();
             Path dir = new Path();
             PartitionDescriptor desc = new PartitionDescriptor();
             TableReader reader = engine.getReader(tableName)) {
            final int partitionCount = reader.getPartitionCount();
            for (int p = 0; p < partitionCount; p++) {
                LocalDate day = START_DATE.plusDays(p);
                String dayDir = "day=" + day.toString();
                dir.of(rootDir.getAbsolutePath()).concat(dayDir);
                Files.createDirectories(java.nio.file.Path.of(dir.toString()));
                path.of(rootDir.getAbsolutePath()).concat(dayDir).concat("data.parquet");
                PartitionEncoder.populateFromTableReader(reader, desc, p);
                PartitionEncoder.encode(desc, path);
                desc.clear();
            }
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeHiveByDayUnderSymbol(CairoEngine engine, String tableName, String sym, File rootDir) {
        try (Path path = new Path();
             Path dir = new Path();
             PartitionDescriptor desc = new PartitionDescriptor();
             TableReader reader = engine.getReader(tableName)) {
            final int partitionCount = reader.getPartitionCount();
            for (int p = 0; p < partitionCount; p++) {
                LocalDate day = START_DATE.plusDays(p);
                String relDir = "day=" + day.toString() + java.io.File.separator + "symbol=" + sym;
                dir.of(rootDir.getAbsolutePath()).concat(relDir);
                Files.createDirectories(java.nio.file.Path.of(dir.toString()));
                path.of(rootDir.getAbsolutePath()).concat(relDir).concat("data.parquet");
                PartitionEncoder.populateFromTableReader(reader, desc, p);
                PartitionEncoder.encode(desc, path);
                desc.clear();
            }
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writePartition(CairoEngine engine, String tableName, int partitionIndex, File outFile) {
        try (Path path = new Path();
             PartitionDescriptor desc = new PartitionDescriptor();
             TableReader reader = engine.getReader(tableName)) {
            path.of(outFile.getAbsolutePath());
            PartitionEncoder.populateFromTableReader(reader, desc, partitionIndex);
            PartitionEncoder.encode(desc, path);
        }
    }

    private static CairoConfiguration newConfiguration() {
        return new DefaultCairoConfiguration(BENCH_DIR) {
            @Override
            public @org.jetbrains.annotations.NotNull CharSequence getSqlCopyInputRoot() {
                return BENCH_DIR;
            }
        };
    }

    public enum Layout {
        L1_SINGLE_FILE,
        L2_NATIVE_NONE,
        L3_HIVE_DAY,
        L4_NATIVE_DAY,
        L5_HIVE_DAY_SYM
    }

    public enum Query {
        COUNT_ALL,
        SUM_ALL,
        FILTER_ONE_DAY,
        FILTER_DAY_RANGE,
        GROUP_BY_DAY,
        FILTER_SYMBOL,
        FILTER_DAY_AND_SYMBOL,
        LIMIT100
    }
}
