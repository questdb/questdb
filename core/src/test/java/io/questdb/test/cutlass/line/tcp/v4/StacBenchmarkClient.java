/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.cutlass.line.tcp.v4;

import io.questdb.client.Sender;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * STAC benchmark ingestion test client.
 * <p>
 * Tests ingestion performance for a STAC-like quotes table with this schema:
 * <pre>
 * CREATE TABLE q (
 *     s SYMBOL,     -- 4-letter ticker symbol (8512 unique)
 *     x CHAR,       -- exchange code
 *     b FLOAT,      -- bid price
 *     a FLOAT,      -- ask price
 *     v SHORT,      -- bid volume
 *     w SHORT,      -- ask volume
 *     m BOOLEAN,    -- market flag
 *     T TIMESTAMP   -- designated timestamp
 * ) timestamp(T) PARTITION BY DAY WAL;
 * </pre>
 * <p>
 * The table MUST be pre-created before running this test so the server uses
 * the correct narrow column types (FLOAT, SHORT, CHAR). Otherwise ILP
 * auto-creation would use DOUBLE, LONG, STRING.
 * <p>
 * Supports 3 protocol modes:
 * <ul>
 *   <li>ilp-tcp: Old ILP text protocol over TCP (port 9009)</li>
 *   <li>ilp-http: Old ILP text protocol over HTTP (port 9000)</li>
 *   <li>ilpv4-websocket: New ILPv4 binary protocol over WebSocket (port 9000)</li>
 * </ul>
 */
public class StacBenchmarkClient {

    private static final String PROTOCOL_ILP_TCP = "ilp-tcp";
    private static final String PROTOCOL_ILP_HTTP = "ilp-http";
    private static final String PROTOCOL_ILPV4_WEBSOCKET = "ilpv4-websocket";

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_ROWS = 80_000_000;
    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_FLUSH_BYTES = 0;
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 0;
    private static final int DEFAULT_IN_FLIGHT_WINDOW = 0;
    private static final int DEFAULT_SEND_QUEUE = 0;
    private static final int DEFAULT_WARMUP_ROWS = 100_000;
    private static final int DEFAULT_REPORT_INTERVAL = 1_000_000;
    private static final String DEFAULT_TABLE = "q";

    // 8512 unique 4-letter symbols, as per STAC NYSE benchmark
    private static final int SYMBOL_COUNT = 8512;
    private static final String[] SYMBOLS = generateSymbols(SYMBOL_COUNT);

    // Exchange codes (single characters)
    private static final char[] EXCHANGES = {'N', 'Q', 'A', 'B', 'C', 'D', 'P', 'Z'};
    // Pre-computed single-char strings to avoid allocation
    private static final String[] EXCHANGE_STRINGS = new String[EXCHANGES.length];

    static {
        for (int i = 0; i < EXCHANGES.length; i++) {
            EXCHANGE_STRINGS[i] = String.valueOf(EXCHANGES[i]);
        }
    }

    // Pre-computed bid base prices per symbol (to generate realistic spreads)
    private static final float[] BASE_PRICES = generateBasePrices(SYMBOL_COUNT);

    public static void main(String[] args) {
        String protocol = PROTOCOL_ILPV4_WEBSOCKET;
        String host = DEFAULT_HOST;
        int port = -1;
        int totalRows = DEFAULT_ROWS;
        int batchSize = DEFAULT_BATCH_SIZE;
        int flushBytes = DEFAULT_FLUSH_BYTES;
        long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        int inFlightWindow = DEFAULT_IN_FLIGHT_WINDOW;
        int sendQueue = DEFAULT_SEND_QUEUE;
        int warmupRows = DEFAULT_WARMUP_ROWS;
        int reportInterval = DEFAULT_REPORT_INTERVAL;
        String table = DEFAULT_TABLE;

        for (String arg : args) {
            if (arg.equals("--help") || arg.equals("-h")) {
                printUsage();
                System.exit(0);
            } else if (arg.startsWith("--protocol=")) {
                protocol = arg.substring("--protocol=".length()).toLowerCase();
            } else if (arg.startsWith("--host=")) {
                host = arg.substring("--host=".length());
            } else if (arg.startsWith("--port=")) {
                port = Integer.parseInt(arg.substring("--port=".length()));
            } else if (arg.startsWith("--rows=")) {
                totalRows = Integer.parseInt(arg.substring("--rows=".length()));
            } else if (arg.startsWith("--batch=")) {
                batchSize = Integer.parseInt(arg.substring("--batch=".length()));
            } else if (arg.startsWith("--flush-bytes=")) {
                flushBytes = Integer.parseInt(arg.substring("--flush-bytes=".length()));
            } else if (arg.startsWith("--flush-interval-ms=")) {
                flushIntervalMs = Long.parseLong(arg.substring("--flush-interval-ms=".length()));
            } else if (arg.startsWith("--in-flight-window=")) {
                inFlightWindow = Integer.parseInt(arg.substring("--in-flight-window=".length()));
            } else if (arg.startsWith("--send-queue=")) {
                sendQueue = Integer.parseInt(arg.substring("--send-queue=".length()));
            } else if (arg.startsWith("--warmup=")) {
                warmupRows = Integer.parseInt(arg.substring("--warmup=".length()));
            } else if (arg.startsWith("--report=")) {
                reportInterval = Integer.parseInt(arg.substring("--report=".length()));
            } else if (arg.startsWith("--table=")) {
                table = arg.substring("--table=".length());
            } else if (arg.equals("--no-warmup")) {
                warmupRows = 0;
            } else {
                System.err.println("Unknown option: " + arg);
                printUsage();
                System.exit(1);
            }
        }

        if (port == -1) {
            port = getDefaultPort(protocol);
        }

        System.out.println("STAC Benchmark Ingestion Client");
        System.out.println("================================");
        System.out.println("Protocol: " + protocol);
        System.out.println("Host: " + host);
        System.out.println("Port: " + port);
        System.out.println("Table: " + table);
        System.out.println("Total rows: " + String.format("%,d", totalRows));
        System.out.println("Batch size (rows): " + String.format("%,d", batchSize) + (batchSize == 0 ? " (default)" : ""));
        System.out.println("Flush bytes: " + (flushBytes == 0 ? "(default)" : String.format("%,d", flushBytes)));
        System.out.println("Flush interval: " + (flushIntervalMs == 0 ? "(default)" : flushIntervalMs + " ms"));
        System.out.println("In-flight window: " + (inFlightWindow == 0 ? "(default: 8)" : inFlightWindow));
        System.out.println("Send queue: " + (sendQueue == 0 ? "(default: 16)" : sendQueue));
        System.out.println("Warmup rows: " + String.format("%,d", warmupRows));
        System.out.println("Report interval: " + String.format("%,d", reportInterval));
        System.out.println("Symbols: " + String.format("%,d", SYMBOL_COUNT) + " unique 4-letter tickers");
        System.out.println();

        try {
            runTest(protocol, host, port, table, totalRows, batchSize, flushBytes, flushIntervalMs,
                    inFlightWindow, sendQueue, warmupRows, reportInterval);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("STAC Benchmark Ingestion Client");
        System.out.println();
        System.out.println("Tests ingestion performance for a STAC-like quotes table.");
        System.out.println("The table must be pre-created with the correct schema.");
        System.out.println();
        System.out.println("Usage: StacBenchmarkClient [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --protocol=PROTOCOL      Protocol to use (default: ilpv4-websocket)");
        System.out.println("  --host=HOST              Server host (default: localhost)");
        System.out.println("  --port=PORT              Server port (default: 9009 for TCP, 9000 for HTTP/WebSocket)");
        System.out.println("  --table=TABLE            Table name (default: q)");
        System.out.println("  --rows=N                 Total rows to send (default: 80000000)");
        System.out.println("  --batch=N                Auto-flush after N rows (default: 10000)");
        System.out.println("  --flush-bytes=N          Auto-flush after N bytes (default: protocol default)");
        System.out.println("  --flush-interval-ms=N    Auto-flush after N ms (default: protocol default)");
        System.out.println("  --in-flight-window=N     Max batches awaiting server ACK (default: 8, WebSocket only)");
        System.out.println("  --send-queue=N           Max batches waiting to send (default: 16, WebSocket only)");
        System.out.println("  --warmup=N               Warmup rows (default: 100000)");
        System.out.println("  --report=N               Report progress every N rows (default: 1000000)");
        System.out.println("  --no-warmup              Skip warmup phase");
        System.out.println("  --help                   Show this help");
        System.out.println();
        System.out.println("Protocols:");
        System.out.println("  ilp-tcp          Old ILP text protocol over TCP (default port: 9009)");
        System.out.println("  ilp-http         Old ILP text protocol over HTTP (default port: 9000)");
        System.out.println("  ilpv4-websocket  New ILPv4 binary protocol over WebSocket (default port: 9000)");
        System.out.println();
        System.out.println("Table schema (must be pre-created):");
        System.out.println("  CREATE TABLE q (");
        System.out.println("      s SYMBOL, x CHAR, b FLOAT, a FLOAT,");
        System.out.println("      v SHORT, w SHORT, m BOOLEAN, T TIMESTAMP");
        System.out.println("  ) timestamp(T) PARTITION BY DAY WAL;");
    }

    private static int getDefaultPort(String protocol) {
        return switch (protocol) {
            case PROTOCOL_ILP_HTTP, PROTOCOL_ILPV4_WEBSOCKET -> 9000;
            default -> 9009;
        };
    }

    private static void runTest(String protocol, String host, int port, String table,
                                int totalRows, int batchSize, int flushBytes, long flushIntervalMs,
                                int inFlightWindow, int sendQueue,
                                int warmupRows, int reportInterval) throws IOException {
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (Sender sender = createSender(protocol, host, port, batchSize, flushBytes, flushIntervalMs,
                inFlightWindow, sendQueue)) {
            System.out.println("Connected! Protocol: " + protocol);
            System.out.println();

            // Warmup phase
            if (warmupRows > 0) {
                System.out.println("Warming up (" + String.format("%,d", warmupRows) + " rows)...");
                long warmupStart = System.nanoTime();
                for (int i = 0; i < warmupRows; i++) {
                    sendQuoteRow(sender, table, i);
                }
                sender.flush();
                long warmupTime = System.nanoTime() - warmupStart;
                double warmupRowsPerSec = warmupRows / (warmupTime / 1_000_000_000.0);
                System.out.printf("Warmup complete in %d ms (%.0f rows/sec)%n",
                        TimeUnit.NANOSECONDS.toMillis(warmupTime), warmupRowsPerSec);
                System.out.println();

                System.gc();
                Thread.sleep(100);
            }

            // Main test phase
            System.out.println("Starting main test (" + String.format("%,d", totalRows) + " rows)...");
            if (reportInterval > 0 && reportInterval <= totalRows) {
                System.out.println("Progress will be reported every " + String.format("%,d", reportInterval) + " rows");
            }
            System.out.println();

            long startTime = System.nanoTime();
            long lastReportTime = startTime;
            int lastReportRows = 0;

            for (int i = 0; i < totalRows; i++) {
                sendQuoteRow(sender, table, i);

                if (reportInterval > 0 && (i + 1) % reportInterval == 0) {
                    long now = System.nanoTime();
                    long elapsedSinceReport = now - lastReportTime;
                    int rowsSinceReport = (i + 1) - lastReportRows;
                    double rowsPerSec = rowsSinceReport / (elapsedSinceReport / 1_000_000_000.0);
                    long totalElapsed = now - startTime;
                    double overallRowsPerSec = (i + 1) / (totalElapsed / 1_000_000_000.0);

                    System.out.printf("Progress: %,d / %,d rows (%.1f%%) - %.0f rows/sec (interval) - %.0f rows/sec (overall)%n",
                            i + 1, totalRows,
                            (i + 1) * 100.0 / totalRows,
                            rowsPerSec, overallRowsPerSec);

                    lastReportTime = now;
                    lastReportRows = i + 1;
                }
            }

            sender.flush();

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            double totalSeconds = totalTime / 1_000_000_000.0;
            double rowsPerSecond = totalRows / totalSeconds;

            System.out.println();
            System.out.println("Test Complete!");
            System.out.println("==============");
            System.out.println("Protocol: " + protocol);
            System.out.println("Table: " + table);
            System.out.println("Total rows: " + String.format("%,d", totalRows));
            System.out.println("Batch size: " + String.format("%,d", batchSize));
            System.out.println("Total time: " + String.format("%.2f", totalSeconds) + " seconds");
            System.out.println("Throughput: " + String.format("%,.0f", rowsPerSecond) + " rows/second");
            System.out.println("Data rate (before compression): " + String.format("%.2f",
                    ((long) totalRows * ESTIMATED_ROW_SIZE) / (1024.0 * 1024.0 * totalSeconds)) + " MB/s (estimated)");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    private static Sender createSender(String protocol, String host, int port,
                                       int batchSize, int flushBytes, long flushIntervalMs,
                                       int inFlightWindow, int sendQueue) {
        return switch (protocol) {
            case PROTOCOL_ILP_TCP -> Sender.builder(Sender.Transport.TCP)
                    .address(host)
                    .port(port)
                    .build();
            case PROTOCOL_ILP_HTTP -> Sender.builder(Sender.Transport.HTTP)
                    .address(host)
                    .port(port)
                    .autoFlushRows(batchSize)
                    .build();
            case PROTOCOL_ILPV4_WEBSOCKET -> {
                Sender.LineSenderBuilder b = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address(host)
                        .port(port)
                        .asyncMode(true);
                if (batchSize > 0) b.autoFlushRows(batchSize);
                if (flushBytes > 0) b.autoFlushBytes(flushBytes);
                if (flushIntervalMs > 0) b.autoFlushIntervalMillis((int) flushIntervalMs);
                if (inFlightWindow > 0) b.inFlightWindowSize(inFlightWindow);
                if (sendQueue > 0) b.sendQueueCapacity(sendQueue);
                yield b.build();
            }
            default -> throw new IllegalArgumentException("Unknown protocol: " + protocol +
                    ". Use one of: ilp-tcp, ilp-http, ilpv4-websocket");
        };
    }

    /**
     * Sends a single quote row matching the STAC schema.
     * <p>
     * Schema: s SYMBOL, x CHAR, b FLOAT, a FLOAT, v SHORT, w SHORT, m BOOLEAN, T TIMESTAMP
     * <p>
     * The server downcasts doubleColumn->FLOAT, longColumn->SHORT, stringColumn->CHAR
     * when the table is pre-created with the correct schema.
     */
    private static void sendQuoteRow(Sender sender, String table, int rowIndex) {
        int symbolIdx = rowIndex % SYMBOL_COUNT;
        int exchangeIdx = rowIndex % EXCHANGES.length;

        // Bid/ask prices: base price with small variation
        float basePrice = BASE_PRICES[symbolIdx];
        // Use rowIndex bits for fast pseudo-random variation without Random object
        float variation = ((rowIndex * 7 + symbolIdx * 13) % 200 - 100) * 0.01f;
        float bid = basePrice + variation;
        float ask = bid + 0.01f + (rowIndex % 10) * 0.01f; // spread: 1-10 cents

        // Volumes: 100-32000 range fits SHORT
        short bidVol = (short) (100 + ((rowIndex * 3 + symbolIdx) % 31901));
        short askVol = (short) (100 + ((rowIndex * 7 + symbolIdx * 5) % 31901));

        // Timestamp: 1 day of data with microsecond precision
        // 86,400,000,000 micros per day, spread across totalRows
        long baseTimestamp = 1704067200000000L; // 2024-01-01 00:00:00 UTC in micros
        long timestamp = baseTimestamp + (rowIndex * 10L) + (rowIndex % 7);

        sender.table(table)
                .symbol("s", SYMBOLS[symbolIdx])
                .stringColumn("x", EXCHANGE_STRINGS[exchangeIdx])
                .doubleColumn("b", bid)
                .doubleColumn("a", ask)
                .longColumn("v", bidVol)
                .longColumn("w", askVol)
                .boolColumn("m", (rowIndex & 1) == 0)
                .at(timestamp, ChronoUnit.MICROS);
    }

    /**
     * Generates N unique 4-letter symbols.
     * Uses combinations of uppercase letters to produce predictable, reproducible symbols.
     */
    private static String[] generateSymbols(int count) {
        String[] symbols = new String[count];
        int idx = 0;
        // 26^4 = 456,976 possible 4-letter combinations, far more than 8512
        outer:
        for (char a = 'A'; a <= 'Z' && idx < count; a++) {
            for (char b = 'A'; b <= 'Z' && idx < count; b++) {
                for (char c = 'A'; c <= 'Z' && idx < count; c++) {
                    for (char d = 'A'; d <= 'Z' && idx < count; d++) {
                        symbols[idx++] = new String(new char[]{a, b, c, d});
                        if (idx >= count) break outer;
                    }
                }
            }
        }
        return symbols;
    }

    /**
     * Generates pseudo-random base prices for each symbol.
     * Prices range from $1 to $500 to simulate realistic stock prices.
     */
    private static float[] generateBasePrices(int count) {
        float[] prices = new float[count];
        Random rng = new Random(42); // fixed seed for reproducibility
        for (int i = 0; i < count; i++) {
            prices[i] = 1.0f + rng.nextFloat() * 499.0f;
        }
        return prices;
    }

    // Estimated row size for throughput calculation:
    // - 1 symbol: ~6 bytes (4-char + overhead)
    // - 1 char: 2 bytes
    // - 2 floats: 4 bytes each = 8 bytes
    // - 2 shorts: 2 bytes each = 4 bytes
    // - 1 boolean: 1 byte
    // - 1 timestamp: 8 bytes
    // - overhead: ~10 bytes
    // Total: ~39 bytes
    private static final int ESTIMATED_ROW_SIZE = 39;
}