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
import io.questdb.std.str.Utf8Sequence;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmarks covering index performance and storage for VARCHAR columns
 * with FSST compression.
 * <p>
 * Scenarios:
 * <ul>
 *   <li>covering — reads name (VARCHAR) + price (DOUBLE) from sidecar (FSST-compressed)</li>
 *   <li>non_covering — bitmap index lookup, reads from column files</li>
 *   <li>no_index — full table scan + filter</li>
 *   <li>covering_in — IN-list covering scan</li>
 *   <li>non_covering_in — IN-list via column files</li>
 * </ul>
 * <p>
 * The main() method prints storage metrics (sidecar sizes, column file sizes)
 * before launching JMH.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(value = 1, jvmArgs = {"-Xmx8g", "-Dquestdb.log.level=E", "-XX:-UseJVMCICompiler"})
public class CoveringVarcharBenchmark {

    private static final int KEY_COUNT = 200;
    private static final int ROWS_PER_KEY = 1_000_000;
    private static final long TOTAL_ROWS = (long) KEY_COUNT * ROWS_PER_KEY;

    @Param({
            "covering",
            "non_covering",
            "no_index",
            "covering_in",
            "non_covering_in"
    })
    public String queryType;

    private SqlCompilerImpl compiler;
    private SqlExecutionContextImpl ctx;
    private CairoEngine engine;
    private RecordCursorFactory factory;
    private long[] memBefore;

    public static void main(String[] args) throws RunnerException, IOException {
        Path tmpDir = Files.createTempDirectory("covering-varchar-bench");
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

                // Table with VARCHAR + DOUBLE in INCLUDE list
                engine.execute(
                        "CREATE TABLE varbench ("
                                + " ts TIMESTAMP,"
                                + " sym SYMBOL INDEX TYPE POSTING INCLUDE (name, price),"
                                + " name VARCHAR,"
                                + " price DOUBLE"
                                + ") TIMESTAMP(ts) PARTITION BY HOUR BYPASS WAL", ctx);

                // Longer strings (~50 bytes) with repeating sub-patterns for realistic FSST.
                // Insert in batches of 10M to avoid column address staleness across partition switches.
                int batchSize = 10_000_000;
                long inserted = 0;
                for (long batch = 0; inserted < TOTAL_ROWS; batch++) {
                    long remaining = TOTAL_ROWS - inserted;
                    long count = Math.min(batchSize, remaining);
                    long startMs = inserted; // 1ms per row from epoch
                    engine.execute(
                            "INSERT INTO varbench"
                                    + " SELECT dateadd('T', (" + startMs + " + x)::INT, '2024-01-01')::TIMESTAMP,"
                                    + "   rnd_symbol(" + symArgs + "),"
                                    + "   rnd_str("
                                    + "     'order_confirmation_alpha_2024_region_us_east_001',"
                                    + "     'order_confirmation_beta_2024_region_eu_west_002',"
                                    + "     'order_confirmation_gamma_2024_region_ap_south_003',"
                                    + "     'order_confirmation_delta_2024_region_us_west_004',"
                                    + "     'order_confirmation_epsilon_2024_region_eu_east_005',"
                                    + "     'order_confirmation_zeta_2024_region_ap_north_006',"
                                    + "     'order_confirmation_theta_2024_region_sa_east_007',"
                                    + "     'order_confirmation_iota_2024_region_af_south_008'),"
                                    + "   rnd_double() * 100"
                                    + " FROM long_sequence(" + count + ")", ctx);
                    inserted += count;
                    System.out.printf("\r  Inserted %,d / %,d rows (%.0f%%)",
                            inserted, TOTAL_ROWS, 100.0 * inserted / TOTAL_ROWS);
                }
                System.out.println();
                engine.releaseAllWriters();

                System.out.printf("%nData: %d keys, %,d total rows, ~%d rows/key%n%n",
                        KEY_COUNT, TOTAL_ROWS, ROWS_PER_KEY);

                // === Storage metrics ===
                printStorageMetrics(tmpDir);

                // Verify correctness: covering vs non-covering
                long coveredCount = countRows(engine, ctx,
                        "SELECT name, price FROM varbench WHERE sym = 'K0'");
                long uncoveredCount = countRows(engine, ctx,
                        "SELECT /*+ no_covering */ name, price FROM varbench WHERE sym = 'K0'");
                System.out.printf("Correctness: covering=%,d rows, non-covering=%,d rows%n%n",
                        coveredCount, uncoveredCount);
                if (coveredCount != uncoveredCount) {
                    throw new IllegalStateException("Row count mismatch!");
                }
            } catch (SqlException e) {
                e.printStackTrace();
            }
        }

        drainNativeResources();

        Options opt = new OptionsBuilder()
                .include(CoveringVarcharBenchmark.class.getSimpleName())
                .jvmArgsAppend("-Dcovering.varchar.bench.dir=" + tmpDir)
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
                // Touch both columns to force reads
                Utf8Sequence name = record.getVarcharA(0);
                if (name != null) {
                    sum += name.size();
                }
                sum += Double.doubleToRawLongBits(record.getDouble(1));
            }
        }
        return sum;
    }

    @Setup(Level.Trial)
    public void setupEngine() {
        // Suppress QuestDB logging in the forked JMH VM to keep output clean
        LogFactory.haltInstance();
        drainNativeResources();
        memBefore = snapshotMemByTag();

        String dir = System.getProperty("covering.varchar.bench.dir");
        if (dir == null) {
            throw new IllegalStateException("covering.varchar.bench.dir system property not set");
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
            case "covering" -> "SELECT name, price FROM varbench WHERE sym = 'K0'";
            case "non_covering" ->
                    "SELECT /*+ no_covering */ name, price FROM varbench WHERE sym = 'K0'";
            case "no_index" ->
                    "SELECT /*+ no_index */ name, price FROM varbench WHERE sym = 'K0'";
            case "covering_in" ->
                    "SELECT name, price FROM varbench WHERE sym IN ('K0', 'K1', 'K2')";
            case "non_covering_in" ->
                    "SELECT /*+ no_covering */ name, price FROM varbench WHERE sym IN ('K0', 'K1', 'K2')";
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

    private static long countRows(CairoEngine engine, SqlExecutionContextImpl ctx, String sql) throws SqlException {
        try (SqlCompilerImpl c = new SqlCompilerImpl(engine);
             RecordCursorFactory f = c.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = f.getCursor(ctx)) {
            long count = 0;
            while (cursor.hasNext()) count++;
            return count;
        }
    }

    private static void printStorageMetrics(Path dbDir) throws IOException {
        // Walk the table directory and categorize file sizes
        Path tableDir = dbDir.resolve("varbench");
        if (!Files.exists(tableDir)) {
            System.out.println("Table directory not found");
            return;
        }

        AtomicLong sidecarTotal = new AtomicLong();
        AtomicLong postingTotal = new AtomicLong();
        AtomicLong columnTotal = new AtomicLong();
        AtomicLong allTotal = new AtomicLong();

        Files.walkFileTree(tableDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String name = file.getFileName().toString();
                long size = attrs.size();
                allTotal.addAndGet(size);

                if (name.contains(".pc") && !name.endsWith(".pci")) {
                    // Sidecar data files (.pc0, .pc1, ...)
                    sidecarTotal.addAndGet(size);
                } else if (name.endsWith(".pk") || name.endsWith(".pv") || name.endsWith(".pci")) {
                    // Posting index files
                    postingTotal.addAndGet(size);
                } else if (name.startsWith("name.") || name.startsWith("price.")) {
                    // Column data files
                    columnTotal.addAndGet(size);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        System.out.println("=== Storage Metrics ===");
        System.out.printf("  Sidecar files (.pc*):      %,12d bytes (%.1f MB)%n",
                sidecarTotal.get(), sidecarTotal.get() / 1_048_576.0);
        System.out.printf("  Posting index (.pk/.pv):   %,12d bytes (%.1f MB)%n",
                postingTotal.get(), postingTotal.get() / 1_048_576.0);
        System.out.printf("  Column files (name+price): %,12d bytes (%.1f MB)%n",
                columnTotal.get(), columnTotal.get() / 1_048_576.0);
        System.out.printf("  Total table directory:     %,12d bytes (%.1f MB)%n",
                allTotal.get(), allTotal.get() / 1_048_576.0);
        if (columnTotal.get() > 0) {
            System.out.printf("  Sidecar / column ratio:    %.1f%%%n",
                    100.0 * sidecarTotal.get() / columnTotal.get());
        }
        System.out.println();
    }

    private static void checkMemoryLeaks(long[] before) {
        long totalBefore = 0;
        long totalAfter = 0;
        StringBuilder leaks = new StringBuilder();
        for (int i = 0; i < MemoryTag.SIZE; i++) {
            if (i == MemoryTag.NATIVE_SQL_COMPILER) continue;
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

    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
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
