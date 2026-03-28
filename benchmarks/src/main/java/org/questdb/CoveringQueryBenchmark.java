package org.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;

import java.io.File;
import java.util.Arrays;

/**
 * SQL-level benchmark comparing covering index vs regular index scan.
 * Uses the {@code no_covering} hint to force the fallback path, then
 * compares latency for common query patterns.
 *
 * <pre>
 * mvn install -pl questdb/core -DskipTests -q
 * mvn package -pl questdb/benchmarks -DskipTests -q
 * java -Xmx4g -cp questdb/benchmarks/target/benchmarks.jar org.questdb.CoveringQueryBenchmark
 * </pre>
 */
public class CoveringQueryBenchmark {

    private static final int KEY_COUNT = 500;
    private static final int TOTAL_ROWS = 2_000_000;
    private static final int WARMUP = 3;
    private static final int ITERS = 7;

    public static void main(String[] args) throws Exception {
        String dbRoot = System.getProperty("java.io.tmpdir") + File.separator + "covering_bench_" + System.currentTimeMillis();
        new File(dbRoot).mkdirs();
        try {
            run(dbRoot);
        } finally {
            deleteRecursive(new File(dbRoot));
        }
    }

    private static void run(String dbRoot) throws Exception {
        DefaultCairoConfiguration config = new DefaultCairoConfiguration(dbRoot);
        try (
                CairoEngine engine = new CairoEngine(config);
                SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, 1)
                        .with(config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null)
        ) {
            System.out.printf("=== Covering Index Query Benchmark ===%n");
            System.out.printf("    %,d keys, %,d total rows (~%,d rows/key)%n%n", KEY_COUNT, TOTAL_ROWS, TOTAL_ROWS / KEY_COUNT);

            setupTable(engine, ctx);

            String k = sampleKey;
            String inList = sampleInList;
            int rpk = TOTAL_ROWS / KEY_COUNT;

            String[][] queries = {
                    {"LIMIT 1",
                            "SELECT price, qty FROM bench WHERE sym = '" + k + "' LIMIT 1",
                            "SELECT /*+ no_covering */ price, qty FROM bench WHERE sym = '" + k + "' LIMIT 1"},
                    {"LIMIT 10",
                            "SELECT price, qty FROM bench WHERE sym = '" + k + "' LIMIT 10",
                            "SELECT /*+ no_covering */ price, qty FROM bench WHERE sym = '" + k + "' LIMIT 10"},
                    {"Full key scan (~" + rpk + ")",
                            "SELECT price, qty FROM bench WHERE sym = '" + k + "'",
                            "SELECT /*+ no_covering */ price, qty FROM bench WHERE sym = '" + k + "'"},
                    {"3-col scan (~" + rpk + ")",
                            "SELECT sym, price, qty FROM bench WHERE sym = '" + k + "'",
                            "SELECT /*+ no_covering */ sym, price, qty FROM bench WHERE sym = '" + k + "'"},
                    {"IN-list 5 keys",
                            "SELECT price, qty FROM bench WHERE sym IN (" + inList + ")",
                            "SELECT /*+ no_covering */ price, qty FROM bench WHERE sym IN (" + inList + ")"},
                    {"LATEST BY single",
                            "SELECT price, qty FROM bench WHERE sym = '" + k + "' LATEST ON ts PARTITION BY sym",
                            "SELECT /*+ no_covering */ price, qty FROM bench WHERE sym = '" + k + "' LATEST ON ts PARTITION BY sym"},
                    {"LATEST BY IN-list (5)",
                            "SELECT price, qty FROM bench WHERE sym IN (" + inList + ") LATEST ON ts PARTITION BY sym",
                            "SELECT /*+ no_covering */ price, qty FROM bench WHERE sym IN (" + inList + ") LATEST ON ts PARTITION BY sym"},
                    {"count()",
                            "SELECT count() FROM bench WHERE sym = '" + k + "'",
                            "SELECT /*+ no_covering */ count() FROM bench WHERE sym = '" + k + "'"},
                    {"sum(price)",
                            "SELECT sum(price) FROM bench WHERE sym = '" + k + "'",
                            "SELECT /*+ no_covering */ sum(price) FROM bench WHERE sym = '" + k + "'"},
            };

            System.out.printf("  %-25s %12s %12s %10s%n", "Query", "Covering", "Fallback", "Speedup");
            System.out.println("  " + "-".repeat(63));

            for (String[] q : queries) {
                String label = q[0];
                double coveringMs = benchQuery(engine, ctx, q[1]);
                double fallbackMs = benchQuery(engine, ctx, q[2]);
                double speedup = fallbackMs / coveringMs;
                System.out.printf("  %-25s %10.3f ms %10.3f ms %9.2fx%n",
                        label, coveringMs, fallbackMs, speedup);
            }
            System.out.println();
        }
    }

    private static void setupTable(CairoEngine engine, SqlExecutionContext ctx) throws Exception {
        System.out.print("  Creating table and inserting data... ");
        System.out.flush();
        long t0 = System.nanoTime();

        engine.execute("""
                CREATE TABLE bench (
                    ts TIMESTAMP,
                    sym SYMBOL INDEX TYPE POSTING INCLUDE (price, qty),
                    price DOUBLE,
                    qty INT
                ) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL
                """, ctx);

        // Insert in per-day chunks — each partition gets ~50k rows with 500 random symbols.
        // rnd_symbol(count, minLen, maxLen, nullRate) generates `count` distinct symbols.
        int rowsPerDay = 50_000;
        int days = TOTAL_ROWS / rowsPerDay;
        for (int d = 0; d < days; d++) {
            engine.execute(
                    "INSERT INTO bench SELECT" +
                            " dateadd('s', x::INT, '2022-01-" + String.format("%02d", (d % 28) + 1) +
                            "T00:00:00') + ((" + (d / 28) + ")::LONG * 2592000000000)," +
                            " rnd_symbol(" + KEY_COUNT + ", 4, 8, 0)," +
                            " rnd_double() * 100," +
                            " rnd_int(0, 1_000_000, 0)" +
                            " FROM long_sequence(" + rowsPerDay + ")",
                    ctx
            );
        }

        // Force writers closed so readers see all data
        engine.releaseAllWriters();

        // Pick sample keys using a non-covering query to avoid sidecar reads
        try (
                RecordCursorFactory factory = engine.select(
                        "SELECT /*+ no_covering */ sym FROM bench WHERE sym IS NOT NULL LIMIT 5", ctx);
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            StringBuilder sb = new StringBuilder();
            int i = 0;
            while (cursor.hasNext()) {
                String s = cursor.getRecord().getSymA(0).toString();
                if (i == 0) sampleKey = s;
                if (!sb.isEmpty()) sb.append(',');
                sb.append('\'').append(s).append('\'');
                i++;
            }
            sampleInList = sb.toString();
        }

        double elapsed = (System.nanoTime() - t0) / 1e9;
        System.out.printf("done in %.1f s (key='%s')%n%n", elapsed, sampleKey);
    }

    private static String sampleKey = "UNKNOWN";
    private static String sampleInList = "'UNKNOWN'";

    private static double benchQuery(CairoEngine engine, SqlExecutionContext ctx, String sql) throws Exception {
        // Warmup
        for (int i = 0; i < WARMUP; i++) {
            drainQuery(engine, ctx, sql);
        }
        // Measure
        long[] times = new long[ITERS];
        for (int i = 0; i < ITERS; i++) {
            long t0 = System.nanoTime();
            drainQuery(engine, ctx, sql);
            times[i] = System.nanoTime() - t0;
        }
        Arrays.sort(times);
        // Return median
        return times[ITERS / 2] / 1e6;
    }

    private static long drainQuery(CairoEngine engine, SqlExecutionContext ctx, String sql) throws Exception {
        try (
                RecordCursorFactory factory = engine.select(sql, ctx);
                RecordCursor cursor = factory.getCursor(ctx)
        ) {
            long count = 0;
            while (cursor.hasNext()) {
                cursor.getRecord();
                count++;
            }
            return count;
        }
    }

    private static void deleteRecursive(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursive(child);
                }
            }
        }
        file.delete();
    }
}
