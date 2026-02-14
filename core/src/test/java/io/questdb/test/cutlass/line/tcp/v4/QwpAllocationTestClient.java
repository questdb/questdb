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
import java.util.concurrent.TimeUnit;

/**
 * Test client for ILP allocation profiling.
 * <p>
 * Supports 3 protocol modes:
 * <ul>
 *   <li>ilp-tcp: Old ILP text protocol over TCP (port 9009)</li>
 *   <li>ilp-http: Old ILP text protocol over HTTP (port 9000)</li>
 *   <li>qwp-websocket: New QWP binary protocol over WebSocket (port 9000)</li>
 * </ul>
 * <p>
 * Sends rows with various column types to exercise all code paths.
 * Run with an allocation profiler (async-profiler, JFR, etc.) to find hotspots.
 * <p>
 * Usage:
 * <pre>
 * java -cp ... QwpAllocationTestClient [options]
 *
 * Options:
 *   --protocol=PROTOCOL   Protocol: ilp-tcp, ilp-http, qwp-websocket (default: qwp-websocket)
 *   --host=HOST           Server host (default: localhost)
 *   --port=PORT           Server port (default: 9009 for TCP, 9000 for HTTP)
 *   --rows=N              Total rows to send (default: 10000000)
 *   --batch=N             Batch/flush size (default: 10000)
 *   --warmup=N            Warmup rows (default: 100000)
 *   --report=N            Report progress every N rows (default: 1000000)
 *   --no-warmup           Skip warmup phase
 *   --help                Show this help
 *
 * Examples:
 *   QwpAllocationTestClient --protocol=qwp-websocket --rows=1000000 --batch=5000
 *   QwpAllocationTestClient --protocol=ilp-tcp --host=remote-server --port=9009
 * </pre>
 */
public class QwpAllocationTestClient {

    // Protocol modes
    private static final String PROTOCOL_ILP_TCP = "ilp-tcp";
    private static final String PROTOCOL_ILP_HTTP = "ilp-http";
    private static final String PROTOCOL_QWP_WEBSOCKET = "qwp-websocket";


    // Default configuration
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_ROWS = 80_000_000;
    private static final int DEFAULT_BATCH_SIZE = 10_000;
    private static final int DEFAULT_FLUSH_BYTES = 0; // 0 = use protocol default
    private static final long DEFAULT_FLUSH_INTERVAL_MS = 0; // 0 = use protocol default
    private static final int DEFAULT_IN_FLIGHT_WINDOW = 0; // 0 = use protocol default (8)
    private static final int DEFAULT_SEND_QUEUE = 0; // 0 = use protocol default (16)
    private static final int DEFAULT_WARMUP_ROWS = 100_000;
    private static final int DEFAULT_REPORT_INTERVAL = 1_000_000;

    // Pre-computed test data to avoid allocation during the test
    private static final String[] SYMBOLS = {
            "AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "TSLA", "BRK.A", "JPM", "JNJ",
            "V", "PG", "UNH", "HD", "MA", "DIS", "PYPL", "BAC", "ADBE", "CMCSA"
    };

    private static final String[] STRINGS = {
            "New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Toronto", "Singapore",
            "Hong Kong", "Dubai", "Mumbai", "Shanghai", "Moscow", "Seoul", "Bangkok",
            "Amsterdam", "Zurich", "Frankfurt", "Milan", "Madrid"
    };

    public static void main(String[] args) {
        // Parse command-line options
        String protocol = PROTOCOL_QWP_WEBSOCKET;
        String host = DEFAULT_HOST;
        int port = -1; // -1 means use default for protocol
        int totalRows = DEFAULT_ROWS;
        int batchSize = DEFAULT_BATCH_SIZE;
        int flushBytes = DEFAULT_FLUSH_BYTES;
        long flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        int inFlightWindow = DEFAULT_IN_FLIGHT_WINDOW;
        int sendQueue = DEFAULT_SEND_QUEUE;
        int warmupRows = DEFAULT_WARMUP_ROWS;
        int reportInterval = DEFAULT_REPORT_INTERVAL;

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
            } else if (arg.equals("--no-warmup")) {
                warmupRows = 0;
            } else if (!arg.startsWith("--")) {
                // Legacy positional args: protocol [host] [port] [rows]
                protocol = arg.toLowerCase();
            } else {
                System.err.println("Unknown option: " + arg);
                printUsage();
                System.exit(1);
            }
        }

        // Use default port if not specified
        if (port == -1) {
            port = getDefaultPort(protocol);
        }

        System.out.println("ILP Allocation Test Client");
        System.out.println("==========================");
        System.out.println("Protocol: " + protocol);
        System.out.println("Host: " + host);
        System.out.println("Port: " + port);
        System.out.println("Total rows: " + String.format("%,d", totalRows));
        System.out.println("Batch size (rows): " + String.format("%,d", batchSize) + (batchSize == 0 ? " (default)" : ""));
        System.out.println("Flush bytes: " + (flushBytes == 0 ? "(default)" : String.format("%,d", flushBytes)));
        System.out.println("Flush interval: " + (flushIntervalMs == 0 ? "(default)" : flushIntervalMs + " ms"));
        System.out.println("In-flight window: " + (inFlightWindow == 0 ? "(default: 8)" : inFlightWindow));
        System.out.println("Send queue: " + (sendQueue == 0 ? "(default: 16)" : sendQueue));
        System.out.println("Warmup rows: " + String.format("%,d", warmupRows));
        System.out.println("Report interval: " + String.format("%,d", reportInterval));
        System.out.println();

        try {
            runTest(protocol, host, port, totalRows, batchSize, flushBytes, flushIntervalMs,
                    inFlightWindow, sendQueue, warmupRows, reportInterval);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("ILP Allocation Test Client");
        System.out.println();
        System.out.println("Usage: QwpAllocationTestClient [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --protocol=PROTOCOL      Protocol to use (default: qwp-websocket)");
        System.out.println("  --host=HOST              Server host (default: localhost)");
        System.out.println("  --port=PORT              Server port (default: 9009 for TCP, 9000 for HTTP/WebSocket)");
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
        System.out.println("  qwp-websocket    New QWP binary protocol over WebSocket (default port: 9000)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  QwpAllocationTestClient --protocol=qwp-websocket --rows=1000000 --batch=5000");
        System.out.println("  QwpAllocationTestClient --protocol=ilp-tcp --host=remote-server");
        System.out.println("  QwpAllocationTestClient --protocol=ilp-tcp --rows=100000 --no-warmup");
    }

    private static int getDefaultPort(String protocol) {
        return switch (protocol) {
            case PROTOCOL_ILP_HTTP, PROTOCOL_QWP_WEBSOCKET -> 9000;
            default -> 9009;
        };
    }

    private static void runTest(String protocol, String host, int port, int totalRows,
                                  int batchSize, int flushBytes, long flushIntervalMs,
                                  int inFlightWindow, int sendQueue,
                                  int warmupRows, int reportInterval) throws IOException {
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (Sender sender = createSender(protocol, host, port, batchSize, flushBytes, flushIntervalMs,
                inFlightWindow, sendQueue)) {
            System.out.println("Connected! Protocol: " + protocol);
            System.out.println();

            // Warm-up phase
            if (warmupRows > 0) {
                System.out.println("Warming up (" + String.format("%,d", warmupRows) + " rows)...");
                long warmupStart = System.nanoTime();
                for (int i = 0; i < warmupRows; i++) {
                    sendRow(sender, i);
                }
                sender.flush();
                long warmupTime = System.nanoTime() - warmupStart;
                System.out.println("Warmup complete in " + TimeUnit.NANOSECONDS.toMillis(warmupTime) + " ms");
                System.out.println();

                // Give GC a chance to clean up warmup allocations
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
                sendRow(sender, i);

                // Report progress
                if (reportInterval > 0 && (i + 1) % reportInterval == 0) {
                    long now = System.nanoTime();
                    long elapsedSinceReport = now - lastReportTime;
                    int rowsSinceReport = (i + 1) - lastReportRows;
                    double rowsPerSec = rowsSinceReport / (elapsedSinceReport / 1_000_000_000.0);

                    System.out.printf("Progress: %,d / %,d rows (%.1f%%) - %.0f rows/sec%n",
                            i + 1, totalRows,
                            (i + 1) * 100.0 / totalRows,
                            rowsPerSec);

                    lastReportTime = now;
                    lastReportRows = i + 1;
                }
            }

            // Final flush
            sender.flush();

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            double totalSeconds = totalTime / 1_000_000_000.0;
            double rowsPerSecond = totalRows / totalSeconds;

            System.out.println();
            System.out.println("Test Complete!");
            System.out.println("==============");
            System.out.println("Protocol: " + protocol);
            System.out.println("Total rows: " + String.format("%,d", totalRows));
            System.out.println("Batch size: " + String.format("%,d", batchSize));
            System.out.println("Total time: " + String.format("%.2f", totalSeconds) + " seconds");
            System.out.println("Throughput: " + String.format("%,.0f", rowsPerSecond) + " rows/second");
            System.out.println("Data rate (before compression): " + String.format("%.2f", ((long)totalRows * estimatedRowSize()) / (1024.0 * 1024.0 * totalSeconds)) + " MB/s (estimated)");

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
            case PROTOCOL_QWP_WEBSOCKET -> {
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
                    ". Use one of: ilp-tcp, ilp-http, qwp-websocket");
        };
    }

    private static void sendRow(Sender sender, int rowIndex) {
        // Base timestamp with small variations
        long baseTimestamp = 1704067200000000L; // 2024-01-01 00:00:00 UTC in micros
        long timestamp = baseTimestamp + (rowIndex * 1000L) + (rowIndex % 100);

        sender.table("ilp_alloc_test")
                // Symbol columns
                .symbol("exchange", SYMBOLS[rowIndex % SYMBOLS.length])
                .symbol("currency", rowIndex % 2 == 0 ? "USD" : "EUR")

                // Numeric columns
                .longColumn("trade_id", rowIndex)
                .longColumn("volume", 100 + (rowIndex % 10000))
                .doubleColumn("price", 100.0 + (rowIndex % 1000) * 0.01)
                .doubleColumn("bid", 99.5 + (rowIndex % 1000) * 0.01)
                .doubleColumn("ask", 100.5 + (rowIndex % 1000) * 0.01)
                .longColumn("sequence", rowIndex % 1000000)
                .doubleColumn("spread", 0.5 + (rowIndex % 100) * 0.01)

                // String column
                .stringColumn("venue", STRINGS[rowIndex % STRINGS.length])

                // Boolean column
                .boolColumn("is_buy", rowIndex % 2 == 0)

                // Additional timestamp column
                .timestampColumn("event_time", timestamp - 1000, ChronoUnit.MICROS)

                // Designated timestamp
                .at(timestamp, ChronoUnit.MICROS);
    }

    /**
     * Estimates the size of a single row in bytes for throughput calculation.
     */
    private static int estimatedRowSize() {
        // Rough estimate (binary protocol):
        // - 2 symbols: ~10 bytes each = 20 bytes
        // - 3 longs: 8 bytes each = 24 bytes
        // - 4 doubles: 8 bytes each = 32 bytes
        // - 1 string: ~10 bytes average
        // - 1 boolean: 1 byte
        // - 2 timestamps: 8 bytes each = 16 bytes
        // - Overhead: ~20 bytes
        // Total: ~123 bytes
        return 123;
    }
}
