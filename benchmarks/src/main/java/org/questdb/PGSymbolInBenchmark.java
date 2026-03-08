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

import io.questdb.std.Rnd;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Benchmark comparing bind variable vs non-bind variable performance for symbol IN queries.
 * <p>
 * This benchmark creates a table similar to the cpu table used in time-series monitoring:
 * CREATE TABLE cpu (
 * timestamp TIMESTAMP,
 * hostname SYMBOL,
 * usage_user DOUBLE
 * ) TIMESTAMP(timestamp) PARTITION BY DAY;
 * <p>
 * It then compares two query approaches:
 * 1. Bind variable: SELECT hostname, max(usage_user) FROM cpu WHERE hostname IN ($1) AND timestamp >= $2 AND timestamp < $3 SAMPLE BY 1h
 * 2. Non-bind variable: SELECT hostname, max(usage_user) FROM cpu WHERE hostname IN ('host1', 'host2', ...) AND timestamp >= '...' AND timestamp < '...' SAMPLE BY 1h
 * <p>
 * Prerequisites:
 * - QuestDB server running on localhost:8812
 * <p>
 * Usage:
 * 1. Start QuestDB server
 * 2. Run this benchmark
 */
public class PGSymbolInBenchmark {

    private static final int HOSTS_IN_QUERY = 2;
    private static final long HOUR_MICROS = 3600_000_000L;
    private static final int NUM_BENCHMARK_ITERATIONS = 1000;
    private static final int NUM_HOSTS = 20;
    private static final int NUM_ROWS = 1000_0000;
    private static final int NUM_WARMUP_ITERATIONS = 1;
    private static final long QUERY_RANGE_HOURS = 2400;
    private static final long START_TIMESTAMP_MICROS = 1704067200000000L; // 2024-01-01 00:00:00 UTC

    public static void main(String[] args) throws Exception {
        System.out.println("PGSymbolInBenchmark - Comparing bind variable vs inline query performance\n");

        try (Connection connection = createConnection()) {
            setupTable(connection);
            insertData(connection);
            Rnd rnd = new Rnd(System.nanoTime(), System.currentTimeMillis());
            long startTs = START_TIMESTAMP_MICROS;
            long endTs = startTs + QUERY_RANGE_HOURS * HOUR_MICROS;

            System.out.println("\n=== Warmup Phase ===");
            for (int i = 0; i < NUM_WARMUP_ITERATIONS; i++) {
                String[] hostnames = generateRandomHostnames(rnd, HOSTS_IN_QUERY);
                runBindVariableQuery(connection, hostnames, startTs, endTs);
                runInlineQuery(connection, hostnames, startTs, endTs);
            }

            System.out.println("\n=== Benchmark Phase ===");
            // Benchmark bind variable query
            long bindVariableTotalNanos = 0;
            for (int i = 0; i < NUM_BENCHMARK_ITERATIONS; i++) {
                String[] hostnames = generateRandomHostnames(rnd, HOSTS_IN_QUERY);
                long start = System.nanoTime();
                int rows = runBindVariableQuery(connection, hostnames, startTs, endTs);
                long elapsed = System.nanoTime() - start;
                bindVariableTotalNanos += elapsed;
                if (i == 0) {
                    System.out.printf("Bind variable query returned %d rows\n", rows);
                }
            }

            // Benchmark inline query
            long inlineTotalNanos = 0;
            for (int i = 0; i < NUM_BENCHMARK_ITERATIONS; i++) {
                String[] hostnames = generateRandomHostnames(rnd, HOSTS_IN_QUERY);
                long start = System.nanoTime();
                int rows = runInlineQuery(connection, hostnames, startTs, endTs);
                long elapsed = System.nanoTime() - start;
                inlineTotalNanos += elapsed;
                if (i == 0) {
                    System.out.printf("Inline query returned %d rows\n", rows);
                }
            }

            // Report results
            double bindVariableAvgMs = bindVariableTotalNanos / (double) NUM_BENCHMARK_ITERATIONS / 1_000_000.0;
            double inlineAvgMs = inlineTotalNanos / (double) NUM_BENCHMARK_ITERATIONS / 1_000_000.0;

            System.out.println("\n=== Results ===");
            System.out.printf("Bind variable query:  avg %.2f ms (total %.2f ms over %d iterations)\n",
                    bindVariableAvgMs, bindVariableTotalNanos / 1_000_000.0, NUM_BENCHMARK_ITERATIONS);
            System.out.printf("Inline query:         avg %.2f ms (total %.2f ms over %d iterations)\n",
                    inlineAvgMs, inlineTotalNanos / 1_000_000.0, NUM_BENCHMARK_ITERATIONS);
            System.out.printf("Ratio (inline/bind):  %.2fx\n", inlineAvgMs / bindVariableAvgMs);
        }
    }

    private static Connection createConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", "true");
        properties.setProperty("preferQueryMode", "extended");

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        final String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", 8812);
        return DriverManager.getConnection(url, properties);
    }

    private static String[] generateRandomHostnames(Rnd rnd, int count) {
        String[] hostnames = new String[count];
        for (int i = 0; i < count; i++) {
            if (i == 0) {
                hostnames[i] = "host_" + rnd.nextInt(NUM_HOSTS);
            } else {
                hostnames[i] = "host_" + rnd.nextInt(10000);
            }
        }
        return hostnames;
    }

    private static void insertData(Connection connection) throws Exception {
        System.out.printf("Inserting %,d rows with %d hosts...\n", NUM_ROWS, NUM_HOSTS);

        Rnd rnd = new Rnd();
        long startTime = System.currentTimeMillis();

        try (PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO cpu (timestamp, hostname, usage_user) VALUES (?, ?, ?)"
        )) {
            for (int i = 0; i < NUM_ROWS; i++) {
                long ts = START_TIMESTAMP_MICROS + (i * 60_000_000L / NUM_HOSTS); // ~1 minute apart
                String hostname = "host_" + (i % NUM_HOSTS);
                double usageUser = rnd.nextDouble() * 100;

                stmt.setTimestamp(1, new Timestamp(ts / 1000)); // Convert micros to millis
                stmt.setString(2, hostname);
                stmt.setDouble(3, usageUser);
                stmt.addBatch();

                if ((i + 1) % 10000 == 0) {
                    stmt.executeBatch();
                }
            }
            stmt.executeBatch();
        }

        long elapsed = System.currentTimeMillis() - startTime;
        System.out.printf("Inserted %,d rows in %,d ms (%,.0f rows/sec)\n",
                NUM_ROWS, elapsed, NUM_ROWS * 1000.0 / elapsed);
    }

    private static int runBindVariableQuery(Connection connection, String[] hostnames, long startTs, long endTs) throws Exception {
        String sql = "SELECT hostname, max(usage_user) FROM cpu " +
                "WHERE hostname IN (?) " +
                "AND timestamp >= ? AND timestamp < ? " +
                "SAMPLE BY 1h";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            Array hostnameArray = connection.createArrayOf("varchar", hostnames);
            stmt.setArray(1, hostnameArray);
            stmt.setTimestamp(2, new Timestamp(startTs / 1000));
            stmt.setTimestamp(3, new Timestamp(endTs / 1000));

            int rowCount = 0;
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    rowCount++;
                }
            }
            return rowCount;
        }
    }

    private static int runInlineQuery(Connection connection, String[] hostnames, long startTs, long endTs) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT hostname, max(usage_user) FROM cpu WHERE hostname IN (");
        for (int i = 0; i < hostnames.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append("'").append(hostnames[i]).append("'");
        }
        sb.append(") AND timestamp >= '");
        sb.append(new Timestamp(startTs / 1000).toString().replace(" ", "T")).append("Z'");
        sb.append(" AND timestamp < '");
        sb.append(new Timestamp(endTs / 1000).toString().replace(" ", "T")).append("Z'");
        sb.append(" SAMPLE BY 1h");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sb.toString())) {
            int rowCount = 0;
            while (rs.next()) {
                rs.getString(1);
                rs.getDouble(2);
                rowCount++;
            }
            return rowCount;
        }
    }

    private static void setupTable(Connection connection) throws Exception {
        System.out.println("Setting up table...");
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS cpu");
            stmt.execute(
                    "CREATE TABLE cpu (" +
                            "timestamp TIMESTAMP, " +
                            "hostname SYMBOL, " +
                            "usage_user DOUBLE" +
                            ") TIMESTAMP(timestamp) PARTITION BY DAY"
            );
        }
    }
}
