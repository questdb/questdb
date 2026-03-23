package org.questdb;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Benchmark for covering index performance after O3 merges.
 * Measures query latency for:
 * - Regular covering query (SELECT price FROM t WHERE sym = 'A')
 * - LATEST ON covering query
 * - DISTINCT covering query
 * - All after O3 merges that trigger sidecar rebuild
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx4g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.CoveringIndexO3Benchmark
 * </pre>
 */
public class CoveringIndexO3Benchmark {

    private static final int KEY_COUNT = 1_000;
    private static final int ROWS_PER_KEY_PER_PARTITION = 100;
    private static final int PARTITIONS = 10;
    private static final int WARMUP = 5;
    private static final int ITERS = 20;

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("covering-o3-bench");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString());

        try (CairoEngine engine = new CairoEngine(config)) {
            SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1);
            try (SqlCompiler compiler = engine.getSqlCompilerFactory().getInstance(engine, ctx)) {
                System.out.printf("=== Covering Index O3 Benchmark ===%n");
                System.out.printf("    %,d keys x %,d rows/key x %d partitions = %,d total rows%n",
                        KEY_COUNT, ROWS_PER_KEY_PER_PARTITION, PARTITIONS,
                        (long) KEY_COUNT * ROWS_PER_KEY_PER_PARTITION * PARTITIONS);

                // Create table with covering index
                exec(compiler, ctx, """
                        CREATE TABLE bench (
                            ts TIMESTAMP,
                            sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                            price DOUBLE,
                            qty INT
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL
                        """);

                // Insert data in order (partitions 2-10)
                System.out.print("Inserting in-order data...");
                long t0 = System.nanoTime();
                for (int p = 1; p < PARTITIONS; p++) {
                    StringBuilder sb = new StringBuilder("INSERT INTO bench VALUES ");
                    for (int k = 0; k < KEY_COUNT; k++) {
                        for (int r = 0; r < ROWS_PER_KEY_PER_PARTITION; r++) {
                            if (k > 0 || r > 0) sb.append(',');
                            int hour = (k * ROWS_PER_KEY_PER_PARTITION + r) % 24;
                            int min = ((k * ROWS_PER_KEY_PER_PARTITION + r) / 24) % 60;
                            int sec = ((k * ROWS_PER_KEY_PER_PARTITION + r) / (24 * 60)) % 60;
                            sb.append(String.format("('2024-01-%02dT%02d:%02d:%02d', 'K%d', %f, %d)",
                                    p + 1, hour, min, sec, k, k * 1.5 + r * 0.1, k * 10 + r));
                        }
                    }
                    exec(compiler, ctx, sb.toString());
                }
                engine.getDdlListener().drainWalQueue(engine);
                System.out.printf(" %.1fs%n", (System.nanoTime() - t0) / 1e9);

                // O3 insert: partition 1 (before all existing data)
                System.out.print("Inserting O3 data (partition 1)...");
                t0 = System.nanoTime();
                StringBuilder sb = new StringBuilder("INSERT INTO bench VALUES ");
                for (int k = 0; k < KEY_COUNT; k++) {
                    for (int r = 0; r < ROWS_PER_KEY_PER_PARTITION; r++) {
                        if (k > 0 || r > 0) sb.append(',');
                        int hour = (k * ROWS_PER_KEY_PER_PARTITION + r) % 24;
                        int min = ((k * ROWS_PER_KEY_PER_PARTITION + r) / 24) % 60;
                        int sec = ((k * ROWS_PER_KEY_PER_PARTITION + r) / (24 * 60)) % 60;
                        sb.append(String.format("('2024-01-01T%02d:%02d:%02d', 'K%d', %f, %d)",
                                hour, min, sec, k, k * 1.5 + r * 0.1, k * 10 + r));
                    }
                }
                exec(compiler, ctx, sb.toString());
                engine.getDdlListener().drainWalQueue(engine);
                engine.releaseAllWriters();
                long o3Ms = (System.nanoTime() - t0) / 1_000_000;
                System.out.printf(" %dms%n", o3Ms);

                // Benchmark queries
                System.out.println();

                // 1. Covering WHERE query
                benchQuery(compiler, ctx, "Covering WHERE sym='K0'",
                        "SELECT price, qty FROM bench WHERE sym = 'K0'");

                // 2. Covering LATEST ON query
                benchQuery(compiler, ctx, "Covering LATEST ON sym='K0'",
                        "SELECT price, qty FROM bench WHERE sym = 'K0' LATEST ON ts PARTITION BY sym");

                // 3. DISTINCT query
                benchQuery(compiler, ctx, "DISTINCT sym",
                        "SELECT DISTINCT sym FROM bench");

                // 4. Covering IN-list query
                benchQuery(compiler, ctx, "Covering IN ('K0','K1','K2')",
                        "SELECT price FROM bench WHERE sym IN ('K0', 'K1', 'K2')");

                // 5. Non-covering for comparison
                benchQuery(compiler, ctx, "Non-covering (SELECT *)",
                        "SELECT * FROM bench WHERE sym = 'K0'");
            }
        } finally {
            deleteDir(tmpDir.toFile());
        }
    }

    private static void benchQuery(SqlCompiler compiler, SqlExecutionContext ctx, String label, String sql) throws Exception {
        // Warmup
        for (int i = 0; i < WARMUP; i++) {
            try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(ctx)) {
                int count = 0;
                while (cursor.hasNext()) count++;
            }
        }

        // Measure
        long totalNs = 0;
        int rowCount = 0;
        for (int i = 0; i < ITERS; i++) {
            long t0 = System.nanoTime();
            try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
                 RecordCursor cursor = factory.getCursor(ctx)) {
                rowCount = 0;
                while (cursor.hasNext()) rowCount++;
            }
            totalNs += System.nanoTime() - t0;
        }

        double avgUs = totalNs / (double) ITERS / 1_000.0;
        System.out.printf("  %-35s %,8d rows  %,.0f µs/query%n", label, rowCount, avgUs);
    }

    private static void exec(SqlCompiler compiler, SqlExecutionContext ctx, String sql) throws Exception {
        compiler.compile(sql, ctx).execute(null);
    }

    private static void deleteDir(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDir(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
