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
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgs = {"-Xmx512m"})
public class CoveringIndexO3Benchmark {

    private static final int KEY_COUNT = 50;
    private static final int ROWS_PER_PARTITION = 5_000;
    private static final int PARTITIONS = 10;
    private static final long TOTAL_ROWS = (long) ROWS_PER_PARTITION * PARTITIONS;

    @Param({"covering_where", "covering_latest", "covering_in", "distinct", "non_covering"})
    public String queryType;

    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private long[] memBefore;

    public static void main(String[] args) throws RunnerException, IOException {
        Path tmpDir = Files.createTempDirectory("covering-o3-bench");
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

                long inOrderRows = (long) ROWS_PER_PARTITION * (PARTITIONS - 1);
                engine.execute(
                        "CREATE TABLE o3bench AS ("
                                + " SELECT dateadd('s', x::INT, '2024-01-02')::TIMESTAMP ts,"
                                + "   rnd_symbol(" + symArgs + ") sym,"
                                + "   rnd_double() price, rnd_int() qty"
                                + " FROM long_sequence(" + inOrderRows + ")"
                                + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL", ctx);

                engine.execute("ALTER TABLE o3bench ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, qty)", ctx);
                engine.releaseAllWriters();

                engine.execute(
                        "INSERT INTO o3bench SELECT dateadd('s', x::INT, '2024-01-01')::TIMESTAMP ts,"
                                + "   rnd_symbol(" + symArgs + ") sym,"
                                + "   rnd_double() price, rnd_int() qty"
                                + " FROM long_sequence(" + ROWS_PER_PARTITION + ")", ctx);

                engine.releaseAllWriters();
                System.out.printf("Data: %d keys x %d rows/partition x %d partitions = %,d rows%n",
                        KEY_COUNT, ROWS_PER_PARTITION, PARTITIONS, TOTAL_ROWS);
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }

        // Drain deferred munmaps and thread-locals left by data setup
        drainNativeResources();

        Options opt = new OptionsBuilder()
                .include(CoveringIndexO3Benchmark.class.getSimpleName())
                .jvmArgsAppend("-Dcovering.bench.dir=" + tmpDir)
                .build();
        new Runner(opt).run();

        deleteDir(tmpDir.toFile());
        LogFactory.haltInstance();
    }

    @Setup(Level.Trial)
    public void setupEngine() {
        drainNativeResources();
        memBefore = snapshotMemByTag();

        String dir = System.getProperty("covering.bench.dir");
        if (dir == null) {
            throw new IllegalStateException("covering.bench.dir system property not set");
        }
        CairoConfiguration configuration = new DefaultCairoConfiguration(dir);
        engine = new CairoEngine(configuration);
        ctx = new SqlExecutionContextImpl(engine, 1).with(
                configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                null, null, -1, null);
        compiler = new SqlCompilerImpl(engine);
    }

    @TearDown(Level.Trial)
    public void tearDownEngine() {
        Misc.free(compiler);
        Misc.free(engine);
        drainNativeResources();
        checkMemoryLeaks(memBefore);
    }

    @Setup(Level.Iteration)
    public void setupFactory() throws Exception {
        String sql = switch (queryType) {
            case "covering_where" -> "SELECT price, qty FROM o3bench WHERE sym = 'K0'";
            case "covering_latest" -> "SELECT price, qty FROM o3bench WHERE sym = 'K0' LATEST ON ts PARTITION BY sym";
            case "covering_in" -> "SELECT price FROM o3bench WHERE sym IN ('K0', 'K1', 'K2')";
            case "distinct" -> "SELECT DISTINCT sym FROM o3bench";
            case "non_covering" -> "SELECT * FROM o3bench WHERE sym = 'K0'";
            default -> throw new IllegalStateException("Unknown: " + queryType);
        };
        factory = compiler.compile(sql, ctx).getRecordCursorFactory();
    }

    @TearDown(Level.Iteration)
    public void tearDownFactory() {
        factory.close();
    }

    @Benchmark
    public long query() throws SqlException {
        long count = 0;
        try (RecordCursor cursor = factory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                count++;
            }
        }
        return count;
    }

    private static void checkMemoryLeaks(long[] before) {
        long totalBefore = 0;
        long totalAfter = 0;
        StringBuilder leaks = new StringBuilder();
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (i == MemoryTag.NATIVE_SQL_COMPILER) {
                continue; // pooled, not released immediately
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
