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
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.str.Utf8Sequence;
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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Compares the ordinary plan of INNER and LEFT hash joins ({@code Hash Join Light} and its outer
 * variants) with the parallel {@code Async Hash Join} operator, over a fact table and dimensions of
 * a thousand, a hundred thousand and {@code largeDimRows} rows.
 * <p>
 * Params:
 * <ul>
 *   <li>{@code plan}: {@code ORDINARY} compiles the query with the operator off, {@code OPERATOR}
 *       with it on. A trial-level routing guard reads the plan and fails the trial when the query
 *       does not run the plan it is meant to measure.</li>
 *   <li>{@code shape}: the query, see {@link #sql()}. {@code STAR_*} join the fact table with the
 *       three dimensions (INNER or LEFT; unfiltered, under a selective probe filter or under a
 *       post-join filter), {@code SINGLE_*} with one dimension each, {@code WIDE} reads every column
 *       of the star (consumer bound), {@code LIMIT_*} stop after 100 rows, {@code FANOUT_*} join a
 *       dimension with hot duplicate keys (no predicate, an ON residual, a post-join filter),
 *       {@code PARQUET_*} and {@code MIXED_PROBE} read Parquet partitions, the Parquet probe also
 *       under a selective probe filter and under an INNER lookup that most rows miss,
 *       {@code COUNT_STAR} counts
 *       the star, {@code STACK4_RESIDUAL} is a worker-bound stack of four LEFT joins with
 *       residuals, and {@code BUILD_ONLY_RESIDUAL} and {@code BUILD_ONLY_RESIDUAL_REGEX} are LEFT
 *       joins of the large dimension under a selective ON residual that reads the dimension alone,
 *       a comparison and a regular expression over a VARCHAR column.</li>
 *   <li>{@code workers}: the size of the query worker pool, and the context's worker count.</li>
 *   <li>{@code factRows}, {@code largeDimRows}: table sizes. The fact table holds a million rows
 *       per hourly partition.</li>
 *   <li>{@code pageFrameMaxRows}: {@code cairo.sql.page.frame.max.rows}, which also sets the
 *       join's task output cap, as on a server. The table scan splits each partition into about
 *       one frame per worker, of at most this many rows: 250,000-row frames up to 4 workers,
 *       125,000 at 8 and 62,500 at 16. With the default of 250,000 the shapes whose surviving
 *       fan-out exceeds two, {@code FANOUT_RESIDUAL} and {@code FANOUT_POST_FILTER}, overflow the
 *       cap at 8 workers and finish part of each frame on the consumer.</li>
 * </ul>
 * The configuration takes the server defaults that depend on the worker count: the reduce queue
 * capacity, its shard count and the parallel filter dispatch limit.
 * <p>
 * Tables live under {@code questdb.bench.root} (a directory under {@code java.io.tmpdir} by
 * default), named after their sizes, and every trial with those sizes reuses them; delete the
 * directory to reclaim the space. After its measurement each trial runs the query once more,
 * untimed, and prints one {@code HASHJOIN-REPORT} line: rows and a checksum (equal across plans),
 * wall and CPU time of the consumer and the workers, the peak of the per-query tracked memory, and
 * the operator's dispatch limit, ring memory, continuation chunks and probe frame count. The
 * {@code LIMIT_*} shapes add the median time to the first row and the worker CPU per execution
 * over 20 executions. With {@code -Dquestdb.bench.report=<file>} in the forked JVM's arguments the
 * line also goes to that file.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {
        "--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED"
})
public class ParallelHashJoinBenchmark {
    private static final String DATA_VERSION = "v1";
    private static final int LIMIT_PROBE_EXECUTIONS = 20;
    private static final String OPERATOR_NODE = "Hash Join workers:";
    private static final String POOL_NAME = "hashjoinbench";
    private static final String STAR_JOINS = " JOIN d1 ON f.k1 = d1.k JOIN d2 ON f.k2 = d2.k JOIN d3 ON f.k3 = d3.k";
    private static final String STAR_LEFT_JOINS = " LEFT JOIN d1 ON f.k1 = d1.k LEFT JOIN d2 ON f.k2 = d2.k LEFT JOIN d3 ON f.k3 = d3.k";
    private static final String STAR_SELECT = "SELECT f.id, f.v, d1.w w1, d2.w w2, d3.w w3 FROM ";
    private static final String[] TABLES = {"ready", "fact", "fact_pq", "fact_mixed", "d1", "d2", "d3", "d3_pq", "dup"};

    @Param({"10000000"})
    public long factRows;

    @Param({"5000000"})
    public long largeDimRows;

    @Param({"250000"})
    public int pageFrameMaxRows;

    @Param({"ORDINARY", "OPERATOR"})
    public String plan;

    @Param({
            "STAR_INNER", "STAR_LEFT",
            "STAR_INNER_PROBE_FILTER", "STAR_LEFT_PROBE_FILTER",
            "STAR_INNER_POST_FILTER", "STAR_LEFT_POST_FILTER",
            "SINGLE_1K", "SINGLE_100K", "SINGLE_LARGE",
            "WIDE",
            "LIMIT_1K", "LIMIT_LARGE",
            "FANOUT_DEFERRED", "FANOUT_RESIDUAL", "FANOUT_POST_FILTER",
            "PARQUET_PROBE", "PARQUET_PROBE_FILTER", "PARQUET_PROBE_INNER_MISS", "PARQUET_BUILD", "MIXED_PROBE",
            "COUNT_STAR",
            "STACK4_RESIDUAL",
            "BUILD_ONLY_RESIDUAL", "BUILD_ONLY_RESIDUAL_REGEX"
    })
    public String shape;

    @Param({"2", "4", "8", "16"})
    public int workers;

    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private int[] types;
    private long[] workerThreadIds;
    private WorkerPool workerPool;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ParallelHashJoinBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    @Benchmark
    public long run() throws SqlException {
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            return consume(cursor, types);
        }
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        try {
            open();
        } catch (Throwable th) {
            // JMH runs no tear-down after a failed set-up, and the workers would keep the fork alive.
            free();
            throw th;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        try {
            if (factory != null) {
                report();
            }
        } finally {
            free();
        }
    }

    private static long consume(RecordCursor cursor, int[] types) {
        final Record record = cursor.getRecord();
        long checksum = 0;
        while (cursor.hasNext()) {
            checksum = checksum * 31 + consumeRow(record, types);
        }
        return checksum;
    }

    private static long consumeRow(Record record, int[] types) {
        long checksum = 0;
        for (int i = 0; i < types.length; i++) {
            final long value = switch (ColumnType.tagOf(types[i])) {
                case ColumnType.INT -> record.getInt(i);
                case ColumnType.LONG -> record.getLong(i);
                case ColumnType.DOUBLE -> Double.doubleToLongBits(record.getDouble(i));
                case ColumnType.TIMESTAMP -> record.getTimestamp(i);
                case ColumnType.SYMBOL -> {
                    final CharSequence symbol = record.getSymA(i);
                    yield symbol != null ? symbol.length() : -1;
                }
                case ColumnType.VARCHAR -> {
                    final Utf8Sequence varchar = record.getVarcharA(i);
                    yield varchar != null ? varchar.size() : -1;
                }
                default -> throw new UnsupportedOperationException("column type: " + ColumnType.nameOf(types[i]));
            };
            checksum = checksum * 31 + value;
        }
        return checksum;
    }

    private static long cpuNanos(ThreadMXBean threads, long[] ids) {
        long nanos = 0;
        for (long id : ids) {
            nanos += Math.max(0, threads.getThreadCpuTime(id));
        }
        return nanos;
    }

    private static AsyncHashJoinRecordCursorFactory findOperator(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof AsyncHashJoinRecordCursorFactory join) {
                return join;
            }
        }
        return null;
    }

    private static String starSql(String fact, String joins) {
        return STAR_SELECT + fact + " f" + joins;
    }

    private void createDimension(String name, long rows) throws SqlException {
        engine.execute("CREATE TABLE " + name + " AS (SELECT (x - 1)::INT k, x - 1 kl, ('c' || ((x - 1) % 16))::SYMBOL s, " +
                "(x * 7 % 1_000) / 1000.0 w, ('name' || x)::VARCHAR name FROM long_sequence(" + rows + "))", ctx);
    }

    private void createTables(Path root) throws SqlException {
        if (engine.getTableTokenIfExists("ready") != null) {
            return;
        }
        System.out.println("creating benchmark tables in " + root);
        for (String table : TABLES) {
            engine.execute("DROP TABLE IF EXISTS " + table, ctx);
        }
        // A million rows per hourly partition. k3 misses the large dimension for a sixth of the rows.
        engine.execute("CREATE TABLE fact AS (SELECT x id, (x * 7_919 % 1_000)::INT k1, " +
                "(x * 2_654_435_761 % 100_000)::INT k2, (x * 2_654_435_761 % " + (largeDimRows * 6 / 5) + ")::INT k3, " +
                "(x * 31 % 100)::INT sel, (x * 17 % 1_000) / 1000.0 v, ('s' || (x % 64))::SYMBOL sym, " +
                "('note' || (x % 1_000))::VARCHAR note, timestamp_sequence(0, 3_600) ts " +
                "FROM long_sequence(" + factRows + ")) TIMESTAMP(ts) PARTITION BY HOUR", ctx);
        createDimension("d1", 1_000);
        createDimension("d2", 100_000);
        createDimension("d3", largeDimRows);
        // 1,000 keys; keys 0 to 249 are hot, with eight rows each.
        engine.execute("CREATE TABLE dup AS (" +
                "SELECT (x - 1)::INT k, (x * 7 % 1_000) / 1000.0 w FROM long_sequence(1_000) " +
                "UNION ALL SELECT ((x - 1) % 250)::INT k, (x * 13 % 997) / 997.0 w FROM long_sequence(1_750))", ctx);
        // The active partition cannot convert, so each Parquet table gets one more row, in a later
        // partition, whose keys match no dimension.
        engine.execute("CREATE TABLE fact_pq AS (SELECT * FROM fact) TIMESTAMP(ts) PARTITION BY HOUR", ctx);
        engine.execute("INSERT INTO fact_pq VALUES (-1, -1, -1, -1, -1, 0.0, 's0', 'note', '2100-01-01')", ctx);
        engine.execute("ALTER TABLE fact_pq CONVERT PARTITION TO PARQUET WHERE ts < '2100-01-01'", ctx);
        engine.execute("CREATE TABLE fact_mixed AS (SELECT * FROM fact) TIMESTAMP(ts) PARTITION BY HOUR", ctx);
        engine.execute("INSERT INTO fact_mixed VALUES (-1, -1, -1, -1, -1, 0.0, 's0', 'note', '2100-01-01')", ctx);
        // Every other partition converts.
        engine.execute("ALTER TABLE fact_mixed CONVERT PARTITION TO PARQUET WHERE ts < '2100-01-01' AND hour(ts) % 2 = 0", ctx);
        // About a million rows per daily partition.
        engine.execute("CREATE TABLE d3_pq AS (SELECT k, kl, s, w, name, timestamp_sequence(0, 86_400) ts FROM d3) " +
                "TIMESTAMP(ts) PARTITION BY DAY", ctx);
        engine.execute("INSERT INTO d3_pq VALUES (-2, -2, 'c0', 0.0, 'name', '2100-01-01')", ctx);
        engine.execute("ALTER TABLE d3_pq CONVERT PARTITION TO PARQUET WHERE ts < '2100-01-01'", ctx);
        engine.execute("CREATE TABLE ready (x INT)", ctx);
    }

    private long[] findWorkerThreads() {
        final List<Long> ids = new ArrayList<>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith(POOL_NAME + '_')) {
                ids.add(thread.getId());
            }
        }
        if (ids.size() != workers) {
            throw new IllegalStateException("found " + ids.size() + " worker threads, expected " + workers);
        }
        return ids.stream().mapToLong(Long::longValue).toArray();
    }

    private void free() {
        factory = Misc.free(factory);
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
        ctx = Misc.free(ctx);
        engine = Misc.free(engine);
    }

    private void open() throws Exception {
        final Path root = Paths.get(System.getProperty("questdb.bench.root", Paths.get(
                System.getProperty("java.io.tmpdir"),
                "questdb-parallel-hash-join-bench-" + DATA_VERSION + "-f" + factRows + "-d" + largeDimRows
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
                return POOL_NAME;
            }

            @Override
            public int getWorkerCount() {
                return workers;
            }
        });
        WorkerPoolUtils.setupQueryJobs(workerPool, engine);
        workerPool.start();
        workerThreadIds = findWorkerThreads();

        ctx = new SqlExecutionContextImpl(engine, workers);
        ctx.with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        createTables(root);

        final boolean isOperator = switch (plan) {
            case "ORDINARY" -> false;
            case "OPERATOR" -> true;
            default -> throw new IllegalArgumentException("unknown plan: " + plan);
        };
        ctx.setParallelHashJoinEnabled(isOperator);
        factory = engine.select(sql(), ctx);
        final TextPlanSink planSink = new TextPlanSink();
        planSink.of(factory, ctx);
        final String text = planSink.getSink().toString();
        if (isOperator != text.contains(OPERATOR_NODE)) {
            throw new IllegalStateException("routing drift: plan=" + plan + " shape=" + shape + '\n' + text);
        }
        final RecordMetadata metadata = factory.getMetadata();
        types = new int[metadata.getColumnCount()];
        for (int i = 0; i < types.length; i++) {
            types[i] = metadata.getColumnType(i);
        }
    }

    private void report() throws SqlException, IOException, InterruptedException {
        final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        final StringBuilder line = new StringBuilder();
        line.append("HASHJOIN-REPORT shape=").append(shape)
                .append(" plan=").append(plan)
                .append(" workers=").append(workers)
                .append(" factRows=").append(factRows)
                .append(" largeDimRows=").append(largeDimRows)
                .append(" pageFrameMaxRows=").append(pageFrameMaxRows)
                .append(" cpus=").append(Runtime.getRuntime().availableProcessors());

        final long workerCpu0 = cpuNanos(threads, workerThreadIds);
        final long consumerCpu0 = threads.getCurrentThreadCpuTime();
        final long wall0 = System.nanoTime();
        long rows = 0;
        long checksum = 0;
        final long peak;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            final MemoryTracker tracker = ctx.getMemoryTracker();
            if (tracker == null) {
                throw new IllegalStateException("no per-query memory tracker");
            }
            final PeakSampler sampler = new PeakSampler(tracker);
            sampler.start();
            try {
                final Record record = cursor.getRecord();
                while (cursor.hasNext()) {
                    checksum = checksum * 31 + consumeRow(record, types);
                    rows++;
                }
            } finally {
                peak = sampler.finish();
            }
            final AsyncHashJoinRecordCursorFactory join = findOperator(factory);
            if (join != null) {
                line.append(" dispatchLimit=").append(join.getAtom().getRing().getDispatchLimit())
                        .append(" ringBytes=").append(join.getAtom().getRing().getAllocatedBytes())
                        .append(" continuations=").append(join.getAtom().getContinuationCount())
                        .append(" probeFrames=").append(join.getProbeFrameCount())
                        .append(" swapped=").append(join.getAtom().isSwapped());
            }
        }
        final long wall = System.nanoTime() - wall0;
        final long consumerCpu = threads.getCurrentThreadCpuTime() - consumerCpu0;
        final long workerCpu = cpuNanos(threads, workerThreadIds) - workerCpu0;
        line.append(" rows=").append(rows)
                .append(" checksum=").append(checksum)
                .append(String.format(Locale.ROOT, " wallMs=%.1f consumerCpuMs=%.1f workerCpuMs=%.1f",
                        wall / 1e6, consumerCpu / 1e6, workerCpu / 1e6))
                .append(" peakTrackedBytes=").append(peak);

        if (shape.startsWith("LIMIT_")) {
            final long[] firstRow = new long[LIMIT_PROBE_EXECUTIONS];
            final long limitWorkerCpu0 = cpuNanos(threads, workerThreadIds);
            final long limitConsumerCpu0 = threads.getCurrentThreadCpuTime();
            for (int i = 0; i < LIMIT_PROBE_EXECUTIONS; i++) {
                final long start = System.nanoTime();
                try (RecordCursor cursor = factory.getCursor(ctx)) {
                    if (cursor.hasNext()) {
                        firstRow[i] = System.nanoTime() - start;
                        consume(cursor, types);
                    }
                }
            }
            final long limitWorkerCpu = cpuNanos(threads, workerThreadIds) - limitWorkerCpu0;
            final long limitConsumerCpu = threads.getCurrentThreadCpuTime() - limitConsumerCpu0;
            Arrays.sort(firstRow);
            line.append(String.format(Locale.ROOT,
                    " firstRowMsMedian=%.3f limitWorkerCpuMsPerOp=%.3f limitConsumerCpuMsPerOp=%.3f",
                    firstRow[LIMIT_PROBE_EXECUTIONS / 2] / 1e6,
                    limitWorkerCpu / 1e6 / LIMIT_PROBE_EXECUTIONS,
                    limitConsumerCpu / 1e6 / LIMIT_PROBE_EXECUTIONS));
        }

        final String text = line.toString();
        System.out.println(text);
        final String reportFile = System.getProperty("questdb.bench.report");
        if (reportFile != null) {
            Files.write(Paths.get(reportFile), (text + '\n').getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    private String sql() {
        return switch (shape) {
            case "STAR_INNER" -> starSql("fact", STAR_JOINS);
            case "STAR_LEFT" -> starSql("fact", STAR_LEFT_JOINS);
            // A JIT-compiled filter on the fact table that keeps 1% of its rows.
            case "STAR_INNER_PROBE_FILTER" -> starSql("fact", STAR_JOINS) + " WHERE f.sel = 7";
            case "STAR_LEFT_PROBE_FILTER" -> starSql("fact", STAR_LEFT_JOINS) + " WHERE f.sel = 7";
            // A filter on the second join's output that keeps about half of its rows.
            case "STAR_INNER_POST_FILTER" -> starSql("fact", STAR_JOINS) + " WHERE d2.w > f.v";
            case "STAR_LEFT_POST_FILTER" -> starSql("fact", STAR_LEFT_JOINS) + " WHERE d2.w > f.v";
            case "SINGLE_1K" -> "SELECT f.id, f.v, d1.w FROM fact f JOIN d1 ON f.k1 = d1.k";
            case "SINGLE_100K" -> "SELECT f.id, f.v, d2.w FROM fact f JOIN d2 ON f.k2 = d2.k";
            case "SINGLE_LARGE" -> "SELECT f.id, f.v, d3.w FROM fact f JOIN d3 ON f.k3 = d3.k";
            case "WIDE" -> "SELECT * FROM fact f" + STAR_JOINS;
            case "LIMIT_1K" -> "SELECT f.id, f.v, d1.w FROM fact f JOIN d1 ON f.k1 = d1.k LIMIT 100";
            case "LIMIT_LARGE" -> "SELECT f.id, f.v, d3.w FROM fact f JOIN d3 ON f.k3 = d3.k LIMIT 100";
            // A quarter of the fact rows meet a hot key with eight matches.
            case "FANOUT_DEFERRED" -> "SELECT f.id, f.v, x.w FROM fact f JOIN dup x ON f.k1 = x.k";
            case "FANOUT_RESIDUAL" -> "SELECT f.id, f.v, x.w FROM fact f LEFT JOIN dup x ON f.k1 = x.k AND x.w > f.v * 0.5";
            case "FANOUT_POST_FILTER" -> "SELECT f.id, f.v, x.w FROM fact f JOIN dup x ON f.k1 = x.k WHERE x.w > f.v * 0.5";
            case "PARQUET_PROBE" -> starSql("fact_pq", STAR_JOINS);
            // The Parquet star under a filter that keeps 1% of the fact rows.
            case "PARQUET_PROBE_FILTER" -> starSql("fact_pq", STAR_JOINS) + " WHERE f.sel = 7";
            // An INNER join that 99% of the Parquet fact rows miss, whose output reads more fact
            // columns than its key.
            case "PARQUET_PROBE_INNER_MISS" -> "SELECT f.id, f.v, f.sym, f.note, d1.w FROM fact_pq f JOIN d1 ON f.k2 = d1.k";
            case "PARQUET_BUILD" -> "SELECT f.id, f.v, d.w FROM fact f JOIN d3_pq d ON f.k3 = d.k";
            case "MIXED_PROBE" -> starSql("fact_mixed", STAR_JOINS);
            case "COUNT_STAR" -> "SELECT count(*) FROM fact f" + STAR_JOINS;
            case "STACK4_RESIDUAL" -> "SELECT f.id, a.w wa, b.w wb, c.w wc, e.w we FROM fact f " +
                    "LEFT JOIN d1 a ON f.k1 = a.k AND sqrt(a.w) > f.v " +
                    "LEFT JOIN d2 b ON f.k2 = b.k AND sqrt(b.w) > f.v " +
                    "LEFT JOIN d3 c ON f.k3 = c.k AND sqrt(c.w) > f.v " +
                    "LEFT JOIN d1 e ON f.sel = e.k AND sqrt(e.w) > f.v";
            // ON residuals of the large dimension alone that keep a sixteenth and a tenth of its rows.
            case "BUILD_ONLY_RESIDUAL" -> "SELECT f.id, f.v, x.w FROM fact f LEFT JOIN d3 x ON f.k3 = x.k AND x.s = 'c3'";
            case "BUILD_ONLY_RESIDUAL_REGEX" -> "SELECT f.id, f.v, x.w FROM fact f LEFT JOIN d3 x ON f.k3 = x.k AND x.name ~ '7$'";
            default -> throw new IllegalArgumentException("unknown shape: " + shape);
        };
    }

    /**
     * Polls the per-query tracker while the query runs. The workers allocate between polls, so the
     * peak is a lower bound of the true one; the memory a query keeps until its cursor closes, its
     * builds and the tuple ring among it, shows at every poll.
     */
    private static final class PeakSampler extends Thread {
        private final MemoryTracker tracker;
        private volatile boolean isRunning = true;
        private volatile long peak;

        private PeakSampler(MemoryTracker tracker) {
            super("hashjoinbench-peak-sampler");
            setDaemon(true);
            this.tracker = tracker;
        }

        @Override
        public void run() {
            long max = 0;
            while (isRunning) {
                max = Math.max(max, tracker.getUsed());
                peak = max;
                LockSupport.parkNanos(20_000);
            }
        }

        private long finish() throws InterruptedException {
            final long last = tracker.getUsed();
            isRunning = false;
            join();
            return Math.max(peak, last);
        }
    }
}
