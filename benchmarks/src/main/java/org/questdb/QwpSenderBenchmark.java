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

import io.questdb.client.Sender;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that ingests rows with complex column types via the QWP (WebSocket)
 * protocol, then validates the table contents via PgWire. The table includes
 * Decimal64/128/256, 1-D and 2-D double arrays, a long array, doubles, varchars,
 * timestamps, and booleans.
 *
 * <p>Run against a local QuestDB instance listening on port 9000 (HTTP/WS) and
 * 8812 (PgWire).
 */
public class QwpSenderBenchmark {
    private static final int MAX_SAMPLE_WINDOWS = 100_000;
    private static final long PROGRESS_INTERVAL = 1_000_000;
    private static final long ROW_COUNT = 100_000_000;
    private static final String TABLE_NAME = "complex_types";

    public static void main(String[] args) throws Exception {
        recreateTable();
        long tookNs = insertRows();
        System.out.printf("Inserted %,d rows in %,d ms%n", ROW_COUNT, TimeUnit.NANOSECONDS.toMillis(tookNs));
        validateContents();
        System.out.println("Validation passed.");
    }

    private static void assertApproxEqual(String label, double expected, double actual) {
        if (Math.abs(expected - actual) > Math.abs(expected) * 1e-9 + 1e-12) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static void assertEqual(String label, Object expected, Object actual) {
        if (!expected.equals(actual)) {
            throw new AssertionError(label + ": expected " + expected + " but got " + actual);
        }
    }

    private static Connection createConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", "admin");
        properties.setProperty("password", "quest");
        properties.setProperty("sslmode", "disable");
        properties.setProperty("binaryTransfer", Boolean.toString(true));
        properties.setProperty("preferQueryMode", "extended");
        TimeZone.setDefault(TimeZone.getTimeZone("EDT"));
        String url = String.format("jdbc:postgresql://127.0.0.1:%d/qdb", 8812);
        return DriverManager.getConnection(url, properties);
    }

    // --- data generation helpers, all keyed off the 1-based row index i ---

    private static void recreateTable() throws Exception {
        try (Connection conn = createConnection();
             Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            st.execute("""
                    CREATE TABLE complex_types (
                        ts TIMESTAMP,
                        d_val DOUBLE,
                        v_text VARCHAR,
                        ts_event TIMESTAMP,
                        dec64_val DECIMAL(18, 4),
                        dec128_val DECIMAL(38, 8),
                        dec256_val DECIMAL(76, 10),
                        dbl_arr DOUBLE[],
                        dbl_arr2d DOUBLE[][],
                        is_even BOOLEAN
                    ) TIMESTAMP(ts) PARTITION BY HOUR WAL""");
        }
    }

    private static double[] expectedDblArr(long i) {
        return new double[]{i * 0.1, i * 0.2, i * 0.3};
    }

    private static double[][] expectedDblArr2d(long i) {
        return new double[][]{{i, i * 2.0}, {i * 3.0, i * 4.0}};
    }

    private static Decimal128 expectedDec128(long i) {
        // i * 10000 + 1234 with scale 4 → e.g., i=1 → 1.1234
        return Decimal128.fromLong(i * 10_000 + 1234, 4);
    }

    private static Decimal256 expectedDec256(long i) {
        // i * 10_000_000_000L + 1_234_567_890 with scale 10 → e.g., i=1 → 1.1234567890
        return Decimal256.fromLong(i * 10_000_000_000L + 1_234_567_890L, 10);
    }

    private static Decimal64 expectedDec64(long i) {
        // i * 10000 + 99 with scale 4 → e.g., i=1 → 1.0099
        return Decimal64.fromLong(i * 10_000 + 99, 4);
    }

    private static double expectedDouble(long i) {
        return i * 1.5;
    }

    private static boolean expectedIsEven(long i) {
        return i % 2 == 0;
    }

    private static String expectedText(long i) {
        return "row-" + i;
    }

    private static long expectedTsEventMicros(long i) {
        // offset by 1_000_000 from designated timestamp
        return makeTimestampMicros(i) + 1_000_000;
    }

    private static long insertRows() {
        long start = System.nanoTime();
        long lastReportNs = start;
        long lastReportRow = 0;
        try (Sender sender = Sender.fromConfig("ws::addr=localhost:9000;")) {
            for (long i = 1; i <= ROW_COUNT; i++) {
                sender.table(TABLE_NAME)
                        .doubleColumn("d_val", expectedDouble(i))
                        .stringColumn("v_text", expectedText(i))
                        .timestampColumn("ts_event", expectedTsEventMicros(i), ChronoUnit.MICROS)
                        .decimalColumn("dec64_val", expectedDec64(i))
                        .decimalColumn("dec128_val", expectedDec128(i))
                        .decimalColumn("dec256_val", expectedDec256(i))
                        .doubleArray("dbl_arr", expectedDblArr(i))
                        .doubleArray("dbl_arr2d", expectedDblArr2d(i))
                        .boolColumn("is_even", expectedIsEven(i))
                        .at(makeTimestampMicros(i), ChronoUnit.MICROS);
                if (i % PROGRESS_INTERVAL == 0) {
                    long now = System.nanoTime();
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(now - start);
                    long intervalRows = i - lastReportRow;
                    long intervalNs = now - lastReportNs;
                    long intervalRate = intervalNs > 0 ? intervalRows * TimeUnit.SECONDS.toNanos(1) / intervalNs : 0;
                    long overallRate = elapsedMs > 0 ? i * 1000 / elapsedMs : 0;
                    System.out.printf("  %,12d / %,d rows  |  elapsed %,d ms  |  interval %,d rows/s  |  overall %,d rows/s%n",
                            i, ROW_COUNT, elapsedMs, intervalRate, overallRate);
                    lastReportNs = now;
                    lastReportRow = i;
                }
            }
            sender.flush();
        }
        return System.nanoTime() - start;
    }

    private static long makeTimestampMicros(long rowIndex) {
        return TimeUnit.MILLISECONDS.toMicros(10 * rowIndex);
    }

    private static void validateAggregates(Statement st) throws Exception {
        // Validate sum of d_val: sum(i*1.5 for i in 1..ROW_COUNT) = 1.5 * ROW_COUNT*(ROW_COUNT+1)/2
        try (ResultSet rs = st.executeQuery(
                "SELECT sum(d_val), count() FROM " + TABLE_NAME + " WHERE is_even = true")) {
            rs.next();
            double sumDVal = rs.getDouble(1);
            long countEven = rs.getLong(2);
            // Even rows: 2,4,6,...,ROW_COUNT. Count = ROW_COUNT/2
            long expectedCount = ROW_COUNT / 2;
            assertEqual("even row count", expectedCount, countEven);
            // sum of i*1.5 for even i: 1.5 * sum(2+4+...+ROW_COUNT)
            // = 1.5 * 2 * (ROW_COUNT/2)*(ROW_COUNT/2 + 1) / 2
            // = 1.5 * (ROW_COUNT/2)*(ROW_COUNT/2 + 1)
            long n = ROW_COUNT / 2;
            double expectedSum = 1.5 * n * (n + 1);
            assertApproxEqual("sum(d_val) for even rows", expectedSum, sumDVal);
        }
        System.out.println("  Scalar aggregates validated.");

        // Validate array aggregates.
        // dbl_arr per row i = {i*0.1, i*0.2, i*0.3}, so array_sum = i*0.6
        // sum(array_sum(dbl_arr)) = 0.6 * ROW_COUNT*(ROW_COUNT+1)/2
        // dbl_arr2d per row i = {{i, i*2}, {i*3, i*4}}, so array_sum = i*10
        // sum(array_sum(dbl_arr2d)) = 10 * ROW_COUNT*(ROW_COUNT+1)/2
        try (ResultSet rs = st.executeQuery(
                "SELECT sum(array_sum(dbl_arr)), sum(array_sum(dbl_arr2d)) FROM " + TABLE_NAME)) {
            rs.next();
            double sumArr1d = rs.getDouble(1);
            double sumArr2d = rs.getDouble(2);
            double triangular = (double) ROW_COUNT * (ROW_COUNT + 1) / 2;
            assertApproxEqual("sum(array_sum(dbl_arr))", 0.6 * triangular, sumArr1d);
            assertApproxEqual("sum(array_sum(dbl_arr2d))", 10.0 * triangular, sumArr2d);
        }
        System.out.println("  Array aggregates validated.");
    }

    private static void validateContents() throws Exception {
        // Wait for WAL to apply
        try (Connection conn = createConnection();
             Statement st = conn.createStatement()) {
            waitForRows(st);
            validateRowCount(st);
            validateFirstRow(st);
            validateLastRow(st);
            validateAggregates(st);
            validateSampleBy(st);
        }
    }

    private static void validateFirstRow(Statement st) throws Exception {
        try (ResultSet rs = st.executeQuery(
                "SELECT d_val, v_text, ts_event, dec64_val, dec128_val, dec256_val, " +
                        "dbl_arr, dbl_arr2d, is_even " +
                        "FROM " + TABLE_NAME + " ORDER BY ts LIMIT 1")) {
            rs.next();
            long i = 1;
            assertApproxEqual("first d_val", expectedDouble(i), rs.getDouble("d_val"));
            assertEqual("first v_text", expectedText(i), rs.getString("v_text"));
            assertEqual("first is_even", expectedIsEven(i), rs.getBoolean("is_even"));
            // dec64_val: 1.0099
            assertApproxEqual("first dec64_val", 1.0099, rs.getDouble("dec64_val"));
            // dec128_val: 1.1234
            assertApproxEqual("first dec128_val", 1.1234, rs.getDouble("dec128_val"));
            System.out.println("  First row validated.");
        }
    }

    private static void validateLastRow(Statement st) throws Exception {
        try (ResultSet rs = st.executeQuery(
                "SELECT d_val, v_text, is_even " +
                        "FROM " + TABLE_NAME + " ORDER BY ts DESC LIMIT 1")) {
            rs.next();
            long i = ROW_COUNT;
            assertApproxEqual("last d_val", expectedDouble(i), rs.getDouble("d_val"));
            assertEqual("last v_text", expectedText(i), rs.getString("v_text"));
            assertEqual("last is_even", expectedIsEven(i), rs.getBoolean("is_even"));
            System.out.println("  Last row validated.");
        }
    }

    private static void validateRowCount(Statement st) throws Exception {
        try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE_NAME)) {
            rs.next();
            long count = rs.getLong(1);
            assertEqual("row count", ROW_COUNT, count);
            System.out.printf("  Row count: %,d%n", count);
        }
    }

    private static void validateSampleBy(Statement st) throws Exception {
        // Rows are 10ms apart (100 rows/s). Choose a sampling period that yields
        // at most MAX_SAMPLE_WINDOWS windows, rounding up to whole seconds.
        long sampleSecs = Math.max(1, (ROW_COUNT + MAX_SAMPLE_WINDOWS * 100L - 1) / (MAX_SAMPLE_WINDOWS * 100L));
        long rowsPerWindow = sampleSecs * 100; // 100 rows/s * sampleSecs

        // Row i has ts = i * 10ms. Window w covers rows [w*rowsPerWindow, (w+1)*rowsPerWindow).
        // Window 0 starts at epoch 0 but row indices start at 1, so it has one fewer row.
        // The last window may be partial.
        try (ResultSet rs = st.executeQuery(
                "SELECT count(), sum(d_val), sum(array_sum(dbl_arr)), sum(array_sum(dbl_arr2d)) " +
                        "FROM " + TABLE_NAME + " SAMPLE BY " + sampleSecs + "s ORDER BY ts")) {
            long windowCount = 0;
            while (rs.next()) {
                long w = windowCount;
                long firstRow = Math.max(1, w * rowsPerWindow);
                long lastRow = Math.min(ROW_COUNT, (w + 1) * rowsPerWindow - 1);
                long expectedCount = lastRow - firstRow + 1;

                assertEqual("count at window " + w, expectedCount, rs.getLong(1));

                // sum(firstRow..lastRow) = (lastRow - firstRow + 1) * (firstRow + lastRow) / 2
                double sumRange = (double) (lastRow - firstRow + 1) * (firstRow + lastRow) / 2;
                // d_val = i * 1.5
                assertApproxEqual("sum(d_val) at window " + w, 1.5 * sumRange, rs.getDouble(2));
                // array_sum(dbl_arr) = i * 0.6
                assertApproxEqual("sum(array_sum(dbl_arr)) at window " + w, 0.6 * sumRange, rs.getDouble(3));
                // array_sum(dbl_arr2d) = i * 10
                assertApproxEqual("sum(array_sum(dbl_arr2d)) at window " + w, 10.0 * sumRange, rs.getDouble(4));

                windowCount++;
            }
            long expectedWindows = (ROW_COUNT + rowsPerWindow - 1) / rowsPerWindow + 1;
            assertEqual("SAMPLE BY window count", expectedWindows, windowCount);
        }
        System.out.printf("  SAMPLE BY aggregates validated (SAMPLE BY %ds, %,d windows).%n",
                sampleSecs, (ROW_COUNT + rowsPerWindow - 1) / rowsPerWindow + 1);
    }

    private static void waitForRows(Statement st) throws Exception {
        System.out.println("Waiting for WAL to apply...");
        long lastReported = -1;
        for (int attempt = 0; attempt < 600; attempt++) {
            try (ResultSet rs = st.executeQuery("SELECT count() FROM " + TABLE_NAME)) {
                rs.next();
                long count = rs.getLong(1);
                if (count == ROW_COUNT) {
                    return;
                }
                if (count / PROGRESS_INTERVAL != lastReported / PROGRESS_INTERVAL) {
                    System.out.printf("  WAL apply progress: %,d / %,d rows%n", count, ROW_COUNT);
                    lastReported = count;
                }
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Timed out waiting for all rows to be applied");
    }
}
