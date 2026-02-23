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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Client benchmark for double-to-decimal and double-to-varchar conversions.
 * Connects to an already-running QuestDB server via PGWire and measures
 * query execution time. Run with the server on this branch, then on master,
 * and compare the results.
 * <p>
 * The table uses exp() to generate a log-uniform distribution of doubles
 * across many orders of magnitude, giving fair coverage to all code paths.
 */
public class DoubleToDecimalBenchmark {
    private static final int ITERATIONS = 10;
    private static final String[] LABELS = {
            "double -> DECIMAL(18,2)  [Decimal64] ",
            "double -> DECIMAL(38,10) [Decimal128]",
            "double -> DECIMAL(76,20) [Decimal256]",
            "double -> VARCHAR        [append]    ",
    };
    private static final int NUM_ROWS = 10_000_000;
    private static final String[] QUERIES = {
            "SELECT sum(d::DECIMAL(18,2)) FROM bench_double",
            "SELECT sum(d::DECIMAL(38,10)) FROM bench_double",
            "SELECT sum(d::DECIMAL(76,20)) FROM bench_double",
            "SELECT sum(length(d::VARCHAR)) FROM bench_double",
    };
    private static final int WARMUP = 3;

    public static void main(String[] args) throws Exception {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            // Create the table if it doesn't exist
            stmt.execute("DROP TABLE IF EXISTS bench_double");
            stmt.execute(
                    "CREATE TABLE bench_double AS (" +
                            " SELECT exp(rnd_double(0) * 46 - 11.5) AS d" +
                            " FROM long_sequence(" + NUM_ROWS + ")" +
                            ")"
            );
            System.out.println(NUM_ROWS + " rows created\n");

            // Warmup
            System.out.println("Warming up (" + WARMUP + " rounds)...");
            for (int w = 0; w < WARMUP; w++) {
                for (String query : QUERIES) {
                    try (ResultSet rs = stmt.executeQuery(query)) {
                        rs.next();
                    }
                }
            }

            // Measure
            System.out.println("Measuring (" + ITERATIONS + " rounds)...");
            long[][] timings = new long[QUERIES.length][ITERATIONS];
            for (int i = 0; i < ITERATIONS; i++) {
                for (int q = 0; q < QUERIES.length; q++) {
                    long start = System.nanoTime();
                    try (ResultSet rs = stmt.executeQuery(QUERIES[q])) {
                        rs.next();
                    }
                    timings[q][i] = System.nanoTime() - start;
                }
            }

            // Report
            System.out.println("\nResults (" + ITERATIONS + " iterations, " + NUM_ROWS + " rows):\n");
            System.out.println("Query                                     median      min      max   ns/row");
            System.out.println("--------------------------------------------------------------------------");
            for (int q = 0; q < QUERIES.length; q++) {
                long[] t = timings[q];
                java.util.Arrays.sort(t);
                long median = t[ITERATIONS / 2];
                long min = t[0];
                long max = t[ITERATIONS - 1];
                double nsPerRow = (double) median / NUM_ROWS;
                System.out.printf("%s  %6d   %6d   %6d   %.1f ns%n",
                        LABELS[q],
                        median / 1_000_000,
                        min / 1_000_000,
                        max / 1_000_000,
                        nsPerRow
                );
            }
        }
    }

    private static Connection createConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", "true");
        String url = "jdbc:postgresql://127.0.0.1:8812/qdb";
        return DriverManager.getConnection(url, properties);
    }
}
