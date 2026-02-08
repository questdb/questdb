/*******************************************************************************
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Measures query latency and throughput for GROUP BY and TOP-K queries under
 * varying concurrency. Designed to expose head-of-line (HOL) blocking in the
 * ordered PageFrameSequence by creating skewed partitions: 10 large partitions
 * (500K rows each) interleaved among 490 small partitions (10K rows each).
 * <p>
 * Run against a QuestDB server (old or new page-frame-sequence code) to compare
 * latency distributions and throughput scaling.
 * <p>
 * Usage:
 * <pre>
 *   mvn -pl benchmarks package -DskipTests
 *   java -cp benchmarks/target/benchmarks.jar org.questdb.HolBlockingBenchmark
 * </pre>
 */
public class HolBlockingBenchmark {
    private static final int[] CONCURRENCY_LEVELS = {1, 2, 4, 8};
    private static final int LARGE_PARTITION_ROWS = 500_000;
    private static final int MEASUREMENT_ITERATIONS = 100;
    private static final String[][] QUERIES = {
            {"GroupBy", "SELECT category, count(), sum(value) FROM events GROUP BY category"},
            {"GroupByNotKeyed", "SELECT count(), sum(value) FROM events"},
            {"GroupByFiltered", "SELECT category, count() FROM events WHERE value > 990000 GROUP BY category"},
            {"TopK", "SELECT ts, category, value FROM events ORDER BY value DESC LIMIT 10"},
    };
    private static final int SMALL_PARTITION_ROWS = 10_000;
    private static final int TOTAL_PARTITIONS = 500;
    private static final int WARMUP_ITERATIONS = 10;

    public static void main(String[] args) throws Exception {
        setup();

        int totalLarge = 0;
        int totalSmall = 0;
        for (int hour = 0; hour < TOTAL_PARTITIONS; hour++) {
            if (hour % 50 == 0) {
                totalLarge++;
            } else {
                totalSmall++;
            }
        }
        long totalRows = (long) totalLarge * LARGE_PARTITION_ROWS + (long) totalSmall * SMALL_PARTITION_ROWS;

        System.out.println("=== Head-of-Line Blocking Benchmark ===");
        System.out.printf("Table: %d partitions (%d x %dK rows + %d x %dK rows, total ~%.1fM rows)%n%n",
                TOTAL_PARTITIONS,
                totalSmall, SMALL_PARTITION_ROWS / 1000,
                totalLarge, LARGE_PARTITION_ROWS / 1000,
                totalRows / 1_000_000.0);

        for (String[] queryDef : QUERIES) {
            String label = queryDef[0];
            String sql = queryDef[1];
            System.out.printf("Query [%s]: %s%n", label, sql);
            for (int concurrency : CONCURRENCY_LEVELS) {
                benchmark(sql, concurrency);
            }
            System.out.println();
        }
    }

    private static long avg(long[] sorted) {
        long sum = 0;
        for (long v : sorted) {
            sum += v;
        }
        return sum / sorted.length;
    }

    private static void benchmark(String sql, int concurrency) throws Exception {
        long[][] allLatencies = new long[concurrency][];
        CyclicBarrier barrier = new CyclicBarrier(concurrency);
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        long overallStart = System.nanoTime();

        for (int t = 0; t < concurrency; t++) {
            final int threadIdx = t;
            pool.submit(() -> {
                try (Connection conn = createConnection();
                     PreparedStatement ps = conn.prepareStatement(sql)) {
                    // warmup
                    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                        drainResultSet(ps);
                    }

                    long[] latencies = new long[MEASUREMENT_ITERATIONS];
                    barrier.await(); // synchronize measurement start
                    for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                        long start = System.nanoTime();
                        drainResultSet(ps);
                        latencies[i] = System.nanoTime() - start;
                    }
                    allLatencies[threadIdx] = latencies;
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);
        long overallElapsed = System.nanoTime() - overallStart;

        // merge latencies from all threads
        int totalSamples = 0;
        for (long[] arr : allLatencies) {
            if (arr != null) {
                totalSamples += arr.length;
            }
        }
        long[] merged = new long[totalSamples];
        int pos = 0;
        for (long[] arr : allLatencies) {
            if (arr != null) {
                System.arraycopy(arr, 0, merged, pos, arr.length);
                pos += arr.length;
            }
        }
        Arrays.sort(merged);

        if (merged.length == 0) {
            System.out.printf("  Concurrency %d:   NO DATA (all threads failed)%n", concurrency);
            return;
        }

        double avgMs = toMs(avg(merged));
        double p50Ms = toMs(percentile(merged, 50));
        double p90Ms = toMs(percentile(merged, 90));
        double p99Ms = toMs(percentile(merged, 99));
        double maxMs = toMs(merged[merged.length - 1]);
        long totalQueries = (long) concurrency * MEASUREMENT_ITERATIONS;
        double qps = totalQueries * 1_000_000_000.0 / overallElapsed;

        System.out.printf("  Concurrency %d:   avg=%.1fms  p50=%.1fms  p90=%.1fms  p99=%.1fms  max=%.1fms  qps=%.0f%n",
                concurrency, avgMs, p50Ms, p90Ms, p99Ms, maxMs, qps);
    }

    private static Connection createConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", "true");
        properties.setProperty("preferQueryMode", "extended");
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        return DriverManager.getConnection("jdbc:postgresql://127.0.0.1:8812/qdb", properties);
    }

    private static void drainResultSet(PreparedStatement ps) throws Exception {
        try (ResultSet rs = ps.executeQuery()) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int c = 1; c <= cols; c++) {
                    rs.getObject(c);
                }
            }
        }
    }

    private static long percentile(long[] sorted, int pct) {
        int idx = (int) Math.ceil(sorted.length * pct / 100.0) - 1;
        return sorted[Math.max(0, idx)];
    }

    private static void setup() throws Exception {
        System.out.println("Setting up benchmark table...");
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS events");
            stmt.execute(
                    "CREATE TABLE events (" +
                            "ts TIMESTAMP, category SYMBOL, value LONG" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR"
            );

            for (int hour = 0; hour < TOTAL_PARTITIONS; hour++) {
                boolean large = hour % 50 == 0;
                int rows = large ? LARGE_PARTITION_ROWS : SMALL_PARTITION_ROWS;
                int day = hour / 24;
                int hourOfDay = hour % 24;
                stmt.execute(String.format(
                        "INSERT INTO events " +
                                "SELECT dateadd('s', x::int, '2024-01-01T00:00:00') + %d * 86400000000L + %d * 3600000000L, " +
                                "rnd_symbol('A','B','C','D','E'), " +
                                "rnd_long(0, 1000000, 0) " +
                                "FROM long_sequence(%d)",
                        day, hourOfDay, rows
                ));
                System.out.printf("  Inserted partition %d/%d (%s, %dK rows)%n",
                        hour + 1, TOTAL_PARTITIONS, large ? "LARGE" : "small", rows / 1000);
            }
            System.out.println("Setup complete.");
        }
    }

    private static double toMs(long nanos) {
        return nanos / 1_000_000.0;
    }
}
