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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that ingests 10M rows of equities L1 market data via the QWP
 * (WebSocket) protocol, then validates the table contents via PgWire.
 *
 * <p>Schema models a typical equities Level 1 quote feed:
 * <ul>
 *   <li>{@code ts} — exchange timestamp (designated, microsecond precision)</li>
 *   <li>{@code symbol} — ticker symbol (SYMBOL type, ~500 distinct values)</li>
 *   <li>{@code bid} — best bid price (DOUBLE)</li>
 *   <li>{@code ask} — best ask price (DOUBLE)</li>
 *   <li>{@code bid_size} — bid depth in lots (INT)</li>
 *   <li>{@code ask_size} — ask depth in lots (INT)</li>
 *   <li>{@code last} — last trade price (DOUBLE)</li>
 *   <li>{@code last_size} — last trade size in lots (INT)</li>
 *   <li>{@code volume} — cumulative session volume (LONG)</li>
 *   <li>{@code exchange} — exchange code (SYMBOL, ~15 values)</li>
 *   <li>{@code condition} — trade condition code (VARCHAR, short strings)</li>
 * </ul>
 *
 * <p>Run against a local QuestDB instance listening on port 9000 (HTTP/WS)
 * and 8812 (PgWire).
 */
public class QwpEquitiesL1Benchmark {
    private static final String[] CONDITIONS = {
            "R", "T", "I", "X", "Z", "F", "O", "4", "W", "7"
    };
    private static final String[] EXCHANGES = {
            "NYSE", "NASDAQ", "ARCA", "BATS", "IEX",
            "EDGX", "EDGA", "BYX", "BZX", "CHX",
            "NSX", "PHLX", "MEMX", "LTSE", "MIAX"
    };
    private static final long PROGRESS_INTERVAL = 1_000_000;
    private static final long ROW_COUNT = 10_000_000;
    private static final int SYMBOL_COUNT = 500;
    private static final String TABLE_NAME = "equities_l1";

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

    private static String[] buildSymbols() {
        String[] symbols = new String[SYMBOL_COUNT];
        for (int i = 0; i < SYMBOL_COUNT; i++) {
            // 4-letter base-26 encoding guarantees 26^4 = 456,976 unique values
            char[] chars = new char[4];
            int v = i;
            for (int c = 3; c >= 0; c--) {
                chars[c] = (char) ('A' + (v % 26));
                v /= 26;
            }
            symbols[i] = new String(chars);
        }
        return symbols;
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

    private static long insertRows() {
        String[] symbols = buildSymbols();
        Random rng = new Random(42);

        // Pre-generate base prices per symbol (50-500 range)
        double[] basePrices = new double[SYMBOL_COUNT];
        for (int i = 0; i < SYMBOL_COUNT; i++) {
            basePrices[i] = 50.0 + rng.nextDouble() * 450.0;
        }

        long start = System.nanoTime();
        long lastReportNs = start;
        long lastReportRow = 0;

        // Base timestamp: 2024-01-02 09:30:00 UTC (market open)
        long baseTsMicros = 1_704_186_600_000_000L;
        // ~10 microseconds between ticks on average
        long tickIntervalMicros = 10;

        try (Sender sender = Sender.fromConfig("ws::addr=localhost:9000;transaction=on;")) {
            for (long i = 1; i <= ROW_COUNT; i++) {
                int symIdx = (int) (i % SYMBOL_COUNT);
                // Random walk: small perturbation per tick
                basePrices[symIdx] += (rng.nextDouble() - 0.5) * 0.02;
                double bid = basePrices[symIdx];
                double spread = 0.01 + rng.nextDouble() * 0.04;
                double ask = bid + spread;
                double last = bid + rng.nextDouble() * spread;

                sender.table(TABLE_NAME)
                        .symbol("symbol", symbols[symIdx])
                        .doubleColumn("bid", bid)
                        .doubleColumn("ask", ask)
                        .longColumn("bid_size", 100 + rng.nextInt(1000) * 100L)
                        .longColumn("ask_size", 100 + rng.nextInt(1000) * 100L)
                        .doubleColumn("last", last)
                        .longColumn("last_size", 100 + rng.nextInt(500) * 100L)
                        .longColumn("volume", i * 1000)
                        .symbol("exchange", EXCHANGES[(int) (i % EXCHANGES.length)])
                        .stringColumn("condition", CONDITIONS[(int) (i % CONDITIONS.length)])
                        .at(baseTsMicros + i * tickIntervalMicros, ChronoUnit.MICROS);

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

    private static void recreateTable() throws Exception {
        try (Connection conn = createConnection();
             Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            st.execute("""
                    CREATE TABLE equities_l1 (
                        ts TIMESTAMP,
                        symbol SYMBOL CAPACITY 1024,
                        bid DOUBLE,
                        ask DOUBLE,
                        bid_size LONG,
                        ask_size LONG,
                        last DOUBLE,
                        last_size LONG,
                        volume LONG,
                        exchange SYMBOL CAPACITY 32,
                        condition VARCHAR
                    ) TIMESTAMP(ts) PARTITION BY HOUR WAL""");
        }
    }

    private static void validateContents() throws Exception {
        try (Connection conn = createConnection();
             Statement st = conn.createStatement()) {
            waitForRows(st);
            validateRowCount(st);
            validateSymbolDistribution(st);
            validatePriceAggregates(st);
            validateSampleBy(st);
        }
    }

    private static void validatePriceAggregates(Statement st) throws Exception {
        // Volume column is deterministic: row i has volume = i * 1000
        // sum(volume) = 1000 * ROW_COUNT * (ROW_COUNT + 1) / 2
        try (ResultSet rs = st.executeQuery(
                "SELECT sum(volume), avg(volume) FROM " + TABLE_NAME)) {
            rs.next();
            double sumVolume = rs.getDouble(1);
            double avgVolume = rs.getDouble(2);
            double expectedSum = 1000.0 * ROW_COUNT * (ROW_COUNT + 1) / 2;
            assertApproxEqual("sum(volume)", expectedSum, sumVolume);
            double expectedAvg = 1000.0 * (ROW_COUNT + 1) / 2;
            assertApproxEqual("avg(volume)", expectedAvg, avgVolume);
        }
        System.out.println("  Volume aggregates validated.");

        // Verify spread is always positive: ask > bid
        try (ResultSet rs = st.executeQuery(
                "SELECT count() FROM " + TABLE_NAME + " WHERE ask <= bid")) {
            rs.next();
            long badRows = rs.getLong(1);
            assertEqual("rows with non-positive spread", 0L, badRows);
        }
        System.out.println("  Spread invariant validated (ask > bid for all rows).");
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
        // Rows are ~10us apart. Sample by 1 second = ~100_000 rows per window.
        try (ResultSet rs = st.executeQuery(
                "SELECT count(), min(bid), max(ask), sum(volume) " +
                        "FROM " + TABLE_NAME + " SAMPLE BY 1s ORDER BY ts LIMIT 5")) {
            int windows = 0;
            while (rs.next()) {
                long count = rs.getLong(1);
                double minBid = rs.getDouble(2);
                double maxAsk = rs.getDouble(3);
                System.out.printf("  SAMPLE BY window %d: count=%,d, min(bid)=%.4f, max(ask)=%.4f%n",
                        windows, count, minBid, maxAsk);
                windows++;
            }
            System.out.printf("  SAMPLE BY validation done (%d windows shown).%n", windows);
        }
    }

    private static void validateSymbolDistribution(Statement st) throws Exception {
        try (ResultSet rs = st.executeQuery(
                "SELECT count_distinct(symbol) FROM " + TABLE_NAME)) {
            rs.next();
            long distinctSymbols = rs.getLong(1);
            assertEqual("distinct symbols", (long) SYMBOL_COUNT, distinctSymbols);
            System.out.printf("  Distinct symbols: %,d%n", distinctSymbols);
        }

        try (ResultSet rs = st.executeQuery(
                "SELECT count_distinct(exchange) FROM " + TABLE_NAME)) {
            rs.next();
            long distinctExchanges = rs.getLong(1);
            assertEqual("distinct exchanges", (long) EXCHANGES.length, distinctExchanges);
            System.out.printf("  Distinct exchanges: %,d%n", distinctExchanges);
        }
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
