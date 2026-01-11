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
import io.questdb.cutlass.line.tcp.v4.IlpV4Sender;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Test client for ILP allocation profiling.
 * <p>
 * Supports 4 protocol modes:
 * <ul>
 *   <li>ilp-tcp: Old ILP text protocol over TCP (port 9009)</li>
 *   <li>ilp-http: Old ILP text protocol over HTTP (port 9000)</li>
 *   <li>ilpv4-tcp: New ILPv4 binary protocol over TCP (port 9009)</li>
 *   <li>ilpv4-http: New ILPv4 binary protocol over HTTP (port 9000)</li>
 * </ul>
 * <p>
 * Sends rows with various column types to exercise all code paths.
 * Run with an allocation profiler (async-profiler, JFR, etc.) to find hotspots.
 * <p>
 * Usage:
 * <pre>
 * java -cp ... IlpV4AllocationTestClient [options]
 *
 * Options:
 *   --protocol=PROTOCOL   Protocol: ilp-tcp, ilp-http, ilpv4-tcp, ilpv4-http (default: ilpv4-tcp)
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
 *   IlpV4AllocationTestClient --protocol=ilpv4-http --rows=1000000 --batch=5000
 *   IlpV4AllocationTestClient --protocol=ilp-tcp --host=remote-server --port=9009
 * </pre>
 */
public class IlpV4AllocationTestClient {

    // Protocol modes
    private static final String PROTOCOL_ILP_TCP = "ilp-tcp";
    private static final String PROTOCOL_ILP_HTTP = "ilp-http";
    private static final String PROTOCOL_ILPV4_TCP = "ilpv4-tcp";
    private static final String PROTOCOL_ILPV4_HTTP = "ilpv4-http";

    // Default configuration
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_ROWS = 10_000_000;
    private static final int DEFAULT_BATCH_SIZE = 10_000;
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
        String protocol = PROTOCOL_ILPV4_TCP;
        String host = DEFAULT_HOST;
        int port = -1; // -1 means use default for protocol
        int totalRows = DEFAULT_ROWS;
        int batchSize = DEFAULT_BATCH_SIZE;
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
        System.out.println("Batch size: " + String.format("%,d", batchSize));
        System.out.println("Warmup rows: " + String.format("%,d", warmupRows));
        System.out.println("Report interval: " + String.format("%,d", reportInterval));
        System.out.println();

        try {
            runTest(protocol, host, port, totalRows, batchSize, warmupRows, reportInterval);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("ILP Allocation Test Client");
        System.out.println();
        System.out.println("Usage: IlpV4AllocationTestClient [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --protocol=PROTOCOL   Protocol to use (default: ilpv4-tcp)");
        System.out.println("  --host=HOST           Server host (default: localhost)");
        System.out.println("  --port=PORT           Server port (default: 9009 for TCP, 9000 for HTTP)");
        System.out.println("  --rows=N              Total rows to send (default: 10000000)");
        System.out.println("  --batch=N             Batch/flush size (default: 10000)");
        System.out.println("  --warmup=N            Warmup rows (default: 100000)");
        System.out.println("  --report=N            Report progress every N rows (default: 1000000)");
        System.out.println("  --no-warmup           Skip warmup phase");
        System.out.println("  --help                Show this help");
        System.out.println();
        System.out.println("Protocols:");
        System.out.println("  ilp-tcp     Old ILP text protocol over TCP (default port: 9009)");
        System.out.println("  ilp-http    Old ILP text protocol over HTTP (default port: 9000)");
        System.out.println("  ilpv4-tcp   New ILPv4 binary protocol over TCP (default port: 9009)");
        System.out.println("  ilpv4-http  New ILPv4 binary protocol over HTTP (default port: 9000)");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  IlpV4AllocationTestClient --protocol=ilpv4-http --rows=1000000 --batch=5000");
        System.out.println("  IlpV4AllocationTestClient --protocol=ilp-tcp --host=remote-server");
        System.out.println("  IlpV4AllocationTestClient --protocol=ilpv4-tcp --rows=100000 --no-warmup");
    }

    private static int getDefaultPort(String protocol) {
        switch (protocol) {
            case PROTOCOL_ILP_HTTP:
            case PROTOCOL_ILPV4_HTTP:
                return 9000;
            case PROTOCOL_ILP_TCP:
            case PROTOCOL_ILPV4_TCP:
            default:
                return 9009;
        }
    }

    private static void runTest(String protocol, String host, int port, int totalRows,
                                  int batchSize, int warmupRows, int reportInterval) throws IOException {
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (Sender sender = createSender(protocol, host, port, batchSize)) {
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
            System.out.println("Data rate: " + String.format("%.2f", (totalRows * estimatedRowSize()) / (1024.0 * 1024.0 * totalSeconds)) + " MB/s (estimated)");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    private static Sender createSender(String protocol, String host, int port, int batchSize) throws IOException {
        switch (protocol) {
            case PROTOCOL_ILP_TCP:
                return Sender.builder(Sender.Transport.TCP)
                        .address(host)
                        .port(port)
                        .build();

            case PROTOCOL_ILP_HTTP:
                return Sender.builder(Sender.Transport.HTTP)
                        .address(host)
                        .port(port)
                        .autoFlushRows(batchSize)
                        .build();

            case PROTOCOL_ILPV4_TCP:
                // ILPv4 over TCP requires direct instantiation (builder only supports HTTP for binary)
                IlpV4Sender tcpSender = IlpV4Sender.connect(host, port);
                tcpSender.autoFlushRows(batchSize);
                return tcpSender;

            case PROTOCOL_ILPV4_HTTP:
                return Sender.builder(Sender.Transport.HTTP)
                        .address(host)
                        .port(port)
                        .binaryTransfer()
                        .autoFlushRows(batchSize)
                        .build();

            default:
                throw new IllegalArgumentException("Unknown protocol: " + protocol +
                        ". Use one of: ilp-tcp, ilp-http, ilpv4-tcp, ilpv4-http");
        }
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
        // Rough estimate:
        // - 2 symbols: ~10 bytes each
        // - 4 longs: 32 bytes
        // - 4 doubles: 32 bytes
        // - 1 string: ~10 bytes average
        // - 1 boolean: 1 byte
        // - 1 timestamp: 8 bytes
        // - Overhead: ~20 bytes
        return 120;
    }
}
