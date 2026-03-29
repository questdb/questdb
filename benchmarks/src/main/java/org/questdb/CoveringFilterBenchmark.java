package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Compares covering index with residual filter vs non-covering with filter.
 * <p>
 * Scenarios:
 * <ul>
 *   <li>covering_filter — CoveringIndex + FilteredRecordCursor (reads from sidecar)</li>
 *   <li>non_covering_filter — PageFrame scan + filter (reads from column files)</li>
 *   <li>no_index_filter — full table scan + filter (no index at all)</li>
 *   <li>covering_no_filter — CoveringIndex without filter (baseline)</li>
 *   <li>covering_filter_in — CoveringIndex IN-list + filter</li>
 *   <li>non_covering_filter_in — PageFrame IN-list + filter</li>
 *   <li>no_index_filter_in — full table scan IN-list + filter (no index at all)</li>
 * </ul>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class CoveringFilterBenchmark {

    private static final int KEY_COUNT = 200;
    private static final int ROWS_PER_KEY = 5_000;
    private static final long TOTAL_ROWS = (long) KEY_COUNT * ROWS_PER_KEY;

    @Param({
            "covering_filter",
            "non_covering_filter",
            "no_index_filter",
            "covering_no_filter",
            "covering_filter_in",
            "non_covering_filter_in",
            "no_index_filter_in"
    })
    public String queryType;

    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private long[] memBefore;

    public static void main(String[] args) throws RunnerException, IOException {
        Path tmpDir = Files.createTempDirectory("covering-filter-bench");
        CairoConfiguration configuration = new DefaultCairoConfiguration(tmpDir.toString());

        try (CairoEngine engine = new CairoEngine(configuration)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            try {
                StringBuilder symArgs = new StringBuilder();
                for (int k = 0; k < KEY_COUNT; k++) {
                    if (k > 0) symArgs.append(',');
                    symArgs.append("'K").append(k).append("'");
                }

                // Create table with covering index, then insert data so sidecar
                // files are written during insertion (needed for page frame path).
                engine.execute(
                        "CREATE TABLE filtbench ("
                                + " ts TIMESTAMP,"
                                + " sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),"
                                + " price DOUBLE,"
                                + " qty INT"
                                + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);
                engine.execute(
                        "INSERT INTO filtbench"
                                + " SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP,"
                                + "   rnd_symbol(" + symArgs + "),"
                                + "   rnd_double() * 100,"
                                + "   rnd_int(1, 1000, 0)"
                                + " FROM long_sequence(" + TOTAL_ROWS + ")", ctx);
                engine.releaseAllWriters();

                System.out.printf("Data: %d keys, %,d total rows, ~%d rows/key%n",
                        KEY_COUNT, TOTAL_ROWS, ROWS_PER_KEY);
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }

        drainNativeResources();

        Options opt = new OptionsBuilder()
                .include(CoveringFilterBenchmark.class.getSimpleName())
                .jvmArgsAppend("-Dcovering.filter.bench.dir=" + tmpDir)
                .build();
        new Runner(opt).run();

        deleteDir(tmpDir.toFile());
        LogFactory.haltInstance();
    }

    @Benchmark
    public long query() throws SqlException {
        long sum = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            io.questdb.cairo.sql.Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                sum += Double.doubleToRawLongBits(record.getDouble(0));
            }
        }
        return sum;
    }

    @Setup(Level.Trial)
    public void setupEngine() {
        drainNativeResources();
        memBefore = snapshotMemByTag();

        String dir = System.getProperty("covering.filter.bench.dir");
        if (dir == null) {
            throw new IllegalStateException("covering.filter.bench.dir system property not set");
        }
        CairoConfiguration configuration = new DefaultCairoConfiguration(dir);
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null);
        compiler = new SqlCompilerImpl(engine);
    }

    @Setup(Level.Iteration)
    public void setupFactory() throws Exception {
        String sql = switch (queryType) {
            case "covering_filter" -> "SELECT price, qty FROM filtbench WHERE sym = 'K0' AND price > 50";
            case "non_covering_filter" ->
                    "SELECT /*+ no_covering */ price, qty FROM filtbench WHERE sym = 'K0' AND price > 50";
            case "no_index_filter" ->
                    "SELECT /*+ no_index */ price, qty FROM filtbench WHERE sym = 'K0' AND price > 50";
            case "covering_no_filter" -> "SELECT price, qty FROM filtbench WHERE sym = 'K0'";
            case "covering_filter_in" -> "SELECT price FROM filtbench WHERE sym IN ('K0', 'K1', 'K2') AND price > 50";
            case "non_covering_filter_in" ->
                    "SELECT /*+ no_covering */ price FROM filtbench WHERE sym IN ('K0', 'K1', 'K2') AND price > 50";
            case "no_index_filter_in" ->
                    "SELECT /*+ no_index */ price FROM filtbench WHERE sym IN ('K0', 'K1', 'K2') AND price > 50";
            default -> throw new IllegalStateException("Unknown: " + queryType);
        };
        factory = compiler.compile(sql, ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Trial)
    public void tearDownEngine() {
        Misc.free(compiler);
        Misc.free(engine);
        drainNativeResources();
        checkMemoryLeaks(memBefore);
    }

    @TearDown(Level.Iteration)
    public void tearDownFactory() {
        factory.close();
    }

    private static void checkMemoryLeaks(long[] before) {
        long totalBefore = 0;
        long totalAfter = 0;
        StringBuilder leaks = new StringBuilder();
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (i == MemoryTag.NATIVE_SQL_COMPILER) {
                continue;
            }
            long after = Unsafe.getMemUsedByTag(i);
            totalBefore += before[i];
            totalAfter += after;
            long diff = after - before[i];
            if (diff != 0) {
                leaks.append(String.format("  %-30s %+,d bytes (was %,d, now %,d)%n",
                        MemoryTag.nameOf(i), diff, before[i], after));
            }
        }
        if (totalAfter > totalBefore) {
            System.err.printf("WARNING: native memory leak detected: %+,d bytes%n", totalAfter - totalBefore);
            System.err.print(leaks);
        }
    }

    private static long countRows(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String sql) throws SqlException {
        try (RecordCursorFactory f = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor c = f.getCursor(ctx)) {
            long count = 0;
            while (c.hasNext()) count++;
            return count;
        }
    }

    private static void deleteDir(java.io.File dir) {
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    f.delete();
                }
            }
        }
        dir.delete();
    }

    private static void drainNativeResources() {
        io.questdb.std.Files.getMmapCache().asyncMunmap();
        io.questdb.std.str.Path.clearThreadLocals();
        Misc.free(O3PartitionJob.THREAD_LOCAL_CLEANER);
    }

    private static long[] snapshotMemByTag() {
        long[] snapshot = new long[MemoryTag.SIZE];
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            snapshot[i] = Unsafe.getMemUsedByTag(i);
        }
        return snapshot;
    }
}
