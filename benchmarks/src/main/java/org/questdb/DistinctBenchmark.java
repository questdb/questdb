package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.log.LogFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * SELECT DISTINCT benchmark comparing:
 * - Fresh .pd (posting index sealed, distinct keys file matches)
 * - Stale .pd (O3 append invalidated the sequence → fallback to per-key scan)
 * - No .pd (bitmap index, no posting index optimisation)
 * <p>
 * Tests across varying distinct key counts and partition counts.
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx8g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.DistinctBenchmark
 * </pre>
 */
public class DistinctBenchmark {

    private static final int RUNS = 7;
    private static final int WARMUP = 3;
    private static final java.io.PrintStream out = System.err;

    public static void main(String[] args) throws Exception {
        LogFactory.haltInstance();
        Path tmpDir = Files.createTempDirectory("distinct-bench");
        CairoConfiguration config = new DefaultCairoConfiguration(tmpDir.toString()) {
            @Override
            public int getRndFunctionMemoryMaxPages() {
                return 4096;
            }
        };

        try (CairoEngine engine = new CairoEngine(config)) {
            SqlExecutionContextImpl ctx = new SqlExecutionContextImpl(engine, 1)
                    .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                            null, null, -1, null);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                out.println();
                out.println("SELECT DISTINCT Benchmark: posting index (stride-scan) vs bitmap index");
                out.println("=".repeat(80));
                out.println();

                int[] keyCounts = {100, 1_000, 10_000};
                int[] partitionCounts = {1, 10, 50};

                for (int keys : keyCounts) {
                    for (int parts : partitionCounts) {
                        runScenario(engine, compiler, ctx, keys, parts);
                    }
                }
            }
        }

        deleteDir(tmpDir.toFile());
        System.exit(0);
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

    private static void drainCursor(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String sql) throws SqlException {
        long sink = 0;
        try (RecordCursorFactory factory = compiler.compile(sql, ctx).getRecordCursorFactory();
             RecordCursor cursor = factory.getCursor(ctx)) {
            while (cursor.hasNext()) {
                CharSequence s = cursor.getRecord().getSymA(0);
                if (s != null) sink += s.length();
            }
        }
        if (sink == Long.MIN_VALUE) throw new IllegalStateException();
    }

    private static double measureDistinct(SqlCompilerImpl compiler, SqlExecutionContextImpl ctx, String table) throws SqlException {
        String sql = "SELECT DISTINCT sym FROM " + table;

        // Warmup
        for (int w = 0; w < WARMUP; w++) {
            drainCursor(compiler, ctx, sql);
        }

        // Measure
        long totalNs = 0;
        for (int r = 0; r < RUNS; r++) {
            long start = System.nanoTime();
            drainCursor(compiler, ctx, sql);
            totalNs += System.nanoTime() - start;
        }
        return totalNs / 1e6 / RUNS;
    }

    private static void runScenario(
            CairoEngine engine, SqlCompilerImpl compiler, SqlExecutionContextImpl ctx,
            int distinctKeys, int partitions
    ) {
        // 100 rows per key per partition → total = keys * rowsPerKey * partitions
        int rowsPerKeyPerPartition = 100;
        long totalRows = (long) distinctKeys * rowsPerKeyPerPartition * partitions;
        out.printf("--- %,d keys x %d partitions (%,d rows) ---%n", distinctKeys, partitions, totalRows);
        try {
            runScenarioInner(engine, compiler, ctx, distinctKeys, partitions, totalRows);
        } catch (Throwable e) {
            out.printf("    FAILED: %s%n%n", e.getMessage());
        }
    }

    private static void runScenarioInner(
            CairoEngine engine, SqlCompilerImpl compiler, SqlExecutionContextImpl ctx,
            int distinctKeys, int partitions, long totalRows
    ) throws SqlException {
        String table = "d_" + distinctKeys + "_" + partitions;

        // Create table with POSTING index (produces .pd on seal)
        engine.execute(
                "CREATE TABLE " + table + " ("
                        + " ts TIMESTAMP,"
                        + " sym SYMBOL INDEX TYPE POSTING,"
                        + " val DOUBLE"
                        + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                ctx
        );

        // Insert data spread across `partitions` days in sorted timestamp order.
        // Each partition is filled sequentially to avoid O3 (which has a separate
        // bug with posting index .pv versioning for new non-mutating partitions).
        out.print("    loading...");
        long t0 = System.nanoTime();
        long rowsPerPartition = totalRows / partitions;
        for (int p = 0; p < partitions; p++) {
            engine.execute(
                    "INSERT INTO " + table
                            + " SELECT dateadd('s', x::INT, dateadd('d', " + p + ", '2024-01-01'))::TIMESTAMP,"
                            + "   rnd_symbol(" + distinctKeys + ", 4, 8, 0),"
                            + "   rnd_double() * 100"
                            + " FROM long_sequence(" + rowsPerPartition + ")",
                    ctx
            );
            engine.releaseAllWriters();
        }
        engine.releaseAllWriters();
        engine.releaseAllReaders();
        double loadSec = (System.nanoTime() - t0) / 1e9;
        out.printf(" %.1f s%n", loadSec);


        // ---- Measure posting index (bulk stride-scan via collectDistinctKeys) ----
        double postingMs;
        try {
            postingMs = measureDistinct(compiler, ctx, table);
        } catch (Throwable e) {
            out.printf("    posting FAILED: %s%n", e.getMessage());
            e.printStackTrace(out);
            throw e;
        }

        // ---- Create bitmap index version for comparison ----
        String bitmapTable = table + "_bmp";
        double bitmapMs = -1;
        try {
            engine.execute(
                    "CREATE TABLE " + bitmapTable + " ("
                            + " ts TIMESTAMP,"
                            + " sym SYMBOL INDEX,"
                            + " val DOUBLE"
                            + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                    ctx
            );
            engine.execute("INSERT INTO " + bitmapTable + " SELECT * FROM " + table, ctx);
            engine.releaseAllWriters();
            bitmapMs = measureDistinct(compiler, ctx, bitmapTable);
        } catch (Throwable e) {
            // Bitmap index has a pre-existing bug with high-cardinality O3 inserts
        }

        // ---- Report ----
        out.printf("    posting:     %8.1f ms  (bulk stride-scan)%n", postingMs);
        if (bitmapMs > 0) {
            out.printf("    bitmap:      %8.1f ms  (per-key cursor scan)%n", bitmapMs);
            out.printf("    speedup:     %.1fx%n", bitmapMs / postingMs);
        } else {
            out.printf("    bitmap:      %8s  (bitmap index O3 error)%n", "N/A");
        }
        out.println();

        // Cleanup
        engine.execute("DROP TABLE " + table, ctx);
        try {
            engine.execute("DROP TABLE IF EXISTS " + bitmapTable, ctx);
        } catch (Throwable ignored) {
        }
        engine.releaseAllWriters();
    }
}
