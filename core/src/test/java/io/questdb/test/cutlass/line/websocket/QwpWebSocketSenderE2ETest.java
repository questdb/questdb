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

package io.questdb.test.cutlass.line.websocket;

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_LONG256;
import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_TIMESTAMP;
import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_UUID;

/**
 * End-to-end integration tests for ILP v4 WebSocket sender.
 * <p>
 * These tests verify that data sent via QwpWebSocketSender over WebSocket
 * is correctly written to QuestDB tables and can be queried.
 * <p>
 * <b>NOTE:</b> These tests require server-side WebSocket support to be enabled.
 * The WebSocket endpoint must be registered in HttpServer for the /write/v4 path.
 * Currently, only HTTP POST is supported on /write/v4.
 * <p>
 * To enable these tests, the server needs to register QwpWebSocketHttpProcessor
 * to handle WebSocket upgrade requests on /write/v4.
 */
public class QwpWebSocketSenderE2ETest extends AbstractBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAsyncModeAutoFlushOnClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Don't call flush() - close() should flush automatically
                try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync("localhost", httpPort, false)) {
                    for (int i = 0; i < 25; i++) {
                        sender.table("async_auto_flush")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    // No explicit flush() - close() handles it
                }

                serverMain.awaitTable("async_auto_flush");
                serverMain.assertSql("select count() from async_auto_flush", "count\n25\n");
            }
        });
    }

    @Test
    public void testAsyncModeLargeNumberOfRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync("localhost", httpPort, false)) {
                    for (int i = 0; i < 100_000_000; i++) {
                        sender.table("async_large")
                                .longColumn("id", i)
                                .doubleColumn("value", i * 1.1)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("async_large");
                serverMain.assertSql("select count() from async_large", "count\n100000000\n");
            }
        });
    }

    @Test
    public void testAsyncModeMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync("localhost", httpPort, false)) {
                    for (int i = 0; i < 200000; i++) {
                        sender.table("async_multi")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("async_multi");
                serverMain.assertSql("select count() from async_multi", "count\n200000\n");
            }
        });
    }

    @Test
    public void testAsyncModeSingleRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync("localhost", httpPort, false)) {
                    sender.table("async_single")
                            .longColumn("value", 42L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("async_single");
                serverMain.assertSql("select count() from async_single", "count\n1\n");
                serverMain.assertSql("select value from async_single", "value\n42\n");
            }
        });
    }

    /**
     * Stress test for server ACK mechanism.
     * Creates many small batches (autoFlushRows=2) to force frequent buffer recycling.
     * With 200 rows and autoFlushRows=2, this creates 100 batches.
     * Since we only have 2 buffers, this requires ACKs to work correctly for buffer recycling.
     */
    @Test
    public void testAsyncModeStressAcks() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Configure to flush every 2 rows - creates many small batches
                try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
                        "localhost", httpPort, false,
                        2, // autoFlushRows - very small to force many batches
                        1024 * 1024, // autoFlushBytes
                        100_000_000L // autoFlushIntervalNanos
                )) {
                    // 200 rows / 2 per batch = 100 batches
                    for (int i = 0; i < 200; i++) {
                        sender.table("ack_stress")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ack_stress");
                serverMain.assertSql("select count() from ack_stress", "count\n200\n");
            }
        });
    }

    @Test
    public void testAsyncModeWithMultipleTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync("localhost", httpPort, false)) {
                    for (int i = 0; i < 50; i++) {
                        // Interleave writes to two tables
                        sender.table("async_table_a")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                        sender.table("async_table_b")
                                .doubleColumn("value", i * 2.5)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("async_table_a");
                serverMain.awaitTable("async_table_b");
                serverMain.assertSql("select count() from async_table_a", "count\n50\n");
                serverMain.assertSql("select count() from async_table_b", "count\n50\n");
            }
        });
    }

    @Test
    public void testAsyncModeWithRowBasedFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Configure to flush every 10 rows
                try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
                        "localhost", httpPort, false,
                        10, // autoFlushRows
                        1024 * 1024, // autoFlushBytes
                        100_000_000L // autoFlushIntervalNanos
                )) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("async_row_flush")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("async_row_flush");
                serverMain.assertSql("select count() from async_row_flush", "count\n50\n");
            }
        });
    }

    @Test
    public void testConcurrentSenders_differentTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int senderCount = 3;
                int rowsPerSender = 1000;
                int autoFlushRows = 10;
                CyclicBarrier barrier = new CyclicBarrier(senderCount);
                AtomicReference<Throwable> error = new AtomicReference<>();

                Thread[] threads = new Thread[senderCount];
                for (int s = 0; s < senderCount; s++) {
                    final int senderIdx = s;
                    threads[s] = new Thread(() -> {
                        try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
                                "localhost", httpPort, false,
                                autoFlushRows,
                                1024 * 1024,
                                100_000_000L
                        )) {
                            barrier.await();
                            for (int i = 0; i < rowsPerSender; i++) {
                                sender.table("concurrent_diff_" + senderIdx)
                                        .longColumn("id", i)
                                        .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                            }
                            sender.flush();
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    threads[s].start();
                }

                for (Thread t : threads) {
                    t.join();
                }
                if (error.get() != null) {
                    throw new RuntimeException("sender thread failed", error.get());
                }

                for (int s = 0; s < senderCount; s++) {
                    serverMain.awaitTable("concurrent_diff_" + s);
                    serverMain.assertSql(
                            "SELECT count() FROM concurrent_diff_" + s,
                            "count\n" + rowsPerSender + "\n"
                    );
                }
            }
        });
    }

    @Test
    public void testConcurrentSenders_sameTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int senderCount = 3;
                int rowsPerSender = 1000;
                int autoFlushRows = 10;
                CyclicBarrier barrier = new CyclicBarrier(senderCount);
                AtomicReference<Throwable> error = new AtomicReference<>();

                Thread[] threads = new Thread[senderCount];
                for (int s = 0; s < senderCount; s++) {
                    final int senderIdx = s;
                    threads[s] = new Thread(() -> {
                        try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
                                "localhost", httpPort, false,
                                autoFlushRows,
                                1024 * 1024,
                                100_000_000L
                        )) {
                            barrier.await();
                            for (int i = 0; i < rowsPerSender; i++) {
                                sender.table("concurrent_same")
                                        .longColumn("sender_id", senderIdx)
                                        .longColumn("row_id", i)
                                        .at(1_000_000_000_000L + senderIdx * 1_000_000L + i * 1000L, ChronoUnit.MICROS);
                            }
                            sender.flush();
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    threads[s].start();
                }

                for (Thread t : threads) {
                    t.join();
                }
                if (error.get() != null) {
                    throw new RuntimeException("sender thread failed", error.get());
                }

                serverMain.awaitTable("concurrent_same");
                serverMain.assertSql(
                        "SELECT count() FROM concurrent_same",
                        "count\n" + (senderCount * rowsPerSender) + "\n"
                );
                // Verify each sender contributed its rows
                for (int s = 0; s < senderCount; s++) {
                    serverMain.assertSql(
                            "SELECT count() FROM concurrent_same WHERE sender_id = " + s,
                            "count\n" + rowsPerSender + "\n"
                    );
                }
            }
        });
    }

    @Test
    public void testConcurrentSenders_sameTable_sameSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int senderCount = 3;
                int rowsPerSender = 1000;
                int autoFlushRows = 10;
                String[] symbols = {"alpha", "beta", "gamma"};
                CyclicBarrier barrier = new CyclicBarrier(senderCount);
                AtomicReference<Throwable> error = new AtomicReference<>();

                Thread[] threads = new Thread[senderCount];
                for (int s = 0; s < senderCount; s++) {
                    final int senderIdx = s;
                    threads[s] = new Thread(() -> {
                        try (QwpWebSocketSender sender = QwpWebSocketSender.connectAsync(
                                "localhost", httpPort, false,
                                autoFlushRows,
                                1024 * 1024,
                                100_000_000L
                        )) {
                            barrier.await();
                            for (int i = 0; i < rowsPerSender; i++) {
                                // All senders use the same set of symbol values
                                sender.table("concurrent_sym")
                                        .symbol("sym", symbols[i % symbols.length])
                                        .longColumn("sender_id", senderIdx)
                                        .longColumn("row_id", i)
                                        .at(1_000_000_000_000L + senderIdx * 1_000_000L + i * 1000L, ChronoUnit.MICROS);
                            }
                            sender.flush();
                        } catch (Throwable t) {
                            error.compareAndSet(null, t);
                        }
                    });
                    threads[s].start();
                }

                for (Thread t : threads) {
                    t.join();
                }
                if (error.get() != null) {
                    throw new RuntimeException("sender thread failed", error.get());
                }

                serverMain.awaitTable("concurrent_sym");
                serverMain.assertSql(
                        "SELECT count() FROM concurrent_sym",
                        "count\n" + (senderCount * rowsPerSender) + "\n"
                );
                // Verify all 3 symbol values are present
                serverMain.assertSql(
                        "SELECT count_distinct(sym) FROM concurrent_sym",
                        "count_distinct\n" + symbols.length + "\n"
                );
                // Verify each sender contributed its rows
                for (int s = 0; s < senderCount; s++) {
                    serverMain.assertSql(
                            "SELECT count() FROM concurrent_sym WHERE sender_id = " + s,
                            "count\n" + rowsPerSender + "\n"
                    );
                }
            }
        });
    }

    @Test
    public void testAtNowServerAssignedTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_at_now")
                            .longColumn("value", 100L)
                            .atNow();
                }

                serverMain.awaitTable("test_at_now");
                serverMain.assertSql("select count() from test_at_now", "count\n1\n");
            }
        });
    }

    @Test
    public void testDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_array")
                            .doubleArray("values", new double[]{1.0, 2.0, 3.0})
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_array");
                serverMain.assertSql("select count() from test_array", "count\n1\n");
            }
        });
    }

    @Test
    public void testLargeNumberOfRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 1000; i++) {
                        sender.table("large_test")
                                .longColumn("id", i)
                                .doubleColumn("value", i * 1.1)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("large_test");
                serverMain.assertSql("select count() from large_test", "count\n1000\n");
            }
        });
    }

    @Test
    public void testMultipleColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("weather")
                            .doubleColumn("temperature", 23.5)
                            .longColumn("humidity", 65L)
                            .boolColumn("sunny", true)
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("weather");
                serverMain.assertSql("select count() from weather", "count\n1\n");
            }
        });
    }

    @Test
    public void testMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("multi_row")
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("multi_row");
                serverMain.assertSql("select count() from multi_row", "count\n10\n");
            }
        });
    }

    @Test
    public void testNullDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_null_double")
                            .doubleColumn("value", 3.14)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    // NaN is the NULL sentinel for DOUBLE columns
                    sender.table("test_null_double")
                            .doubleColumn("value", Double.NaN)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_null_double")
                            .doubleColumn("value", 2.72)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_null_double");
                serverMain.assertSql(
                        "SELECT value FROM test_null_double ORDER BY timestamp",
                        """
                                value
                                3.14
                                null
                                2.72
                                """
                );
                serverMain.assertSql(
                        "SELECT count() FROM test_null_double WHERE value IS NULL",
                        "count\n1\n"
                );
            }
        });
    }

    @Test
    public void testNullLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_null_long")
                            .longColumn("value", 42L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    // Long.MIN_VALUE is the NULL sentinel for LONG columns
                    sender.table("test_null_long")
                            .longColumn("value", Long.MIN_VALUE)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_null_long")
                            .longColumn("value", 99L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_null_long");
                serverMain.assertSql(
                        "SELECT value FROM test_null_long ORDER BY timestamp",
                        """
                                value
                                42
                                null
                                99
                                """
                );
                serverMain.assertSql(
                        "SELECT count() FROM test_null_long WHERE value IS NULL",
                        "count\n1\n"
                );
            }
        });
    }

    @Test
    public void testNullLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Use fast-path API to send null LONG256 via null bitmap
                    QwpTableBuffer buf = sender.getTableBuffer("test_null_long256");
                    QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("value", TYPE_LONG256, true);

                    // Row 1: non-null value
                    col.addLong256(1L, 2L, 3L, 4L);
                    sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                    // Row 2: null
                    col.addNull();
                    sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                    // Row 3: non-null value
                    col.addLong256(5L, 6L, 7L, 8L);
                    sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_null_long256");
                serverMain.assertSql(
                        "SELECT count() FROM test_null_long256 WHERE value IS NULL",
                        "count\n1\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM test_null_long256 WHERE value IS NOT NULL",
                        "count\n2\n"
                );
            }
        });
    }

    @Test
    public void testNullMixed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 20; i++) {
                        sender.table("test_null_mixed")
                                .stringColumn("s", i % 2 == 0 ? "val_" + i : null)
                                .longColumn("l", i % 3 == 0 ? Long.MIN_VALUE : i)
                                .doubleColumn("d", i % 4 == 0 ? Double.NaN : i * 1.5)
                                .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("test_null_mixed");
                serverMain.assertSql(
                        "SELECT count() FROM test_null_mixed",
                        "count\n20\n"
                );
                // 10 odd rows have null strings
                serverMain.assertSql(
                        "SELECT count() FROM test_null_mixed WHERE s IS NULL",
                        "count\n10\n"
                );
                // Rows 0, 3, 6, 9, 12, 15, 18 → 7 null longs
                serverMain.assertSql(
                        "SELECT count() FROM test_null_mixed WHERE l IS NULL",
                        "count\n7\n"
                );
                // Rows 0, 4, 8, 12, 16 → 5 null doubles
                serverMain.assertSql(
                        "SELECT count() FROM test_null_mixed WHERE d IS NULL",
                        "count\n5\n"
                );
            }
        });
    }

    @Test
    public void testNullString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_null_string")
                            .stringColumn("message", "hello")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    sender.table("test_null_string")
                            .stringColumn("message", null)
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);
                    sender.table("test_null_string")
                            .stringColumn("message", "world")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_null_string");
                serverMain.assertSql(
                        "SELECT count() FROM test_null_string",
                        "count\n3\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM test_null_string WHERE message IS NULL",
                        "count\n1\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM test_null_string WHERE message IS NOT NULL",
                        "count\n2\n"
                );
            }
        });
    }

    @Test
    public void testNullTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Use fast-path API to send null timestamp via null bitmap
                    QwpTableBuffer buf = sender.getTableBuffer("test_null_ts");
                    QwpTableBuffer.ColumnBuffer tsCol = buf.getOrCreateColumn("event_time", TYPE_TIMESTAMP, true);

                    // Row 1: non-null timestamp
                    tsCol.addLong(1_609_459_200_000_000L);
                    sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                    // Row 2: null timestamp
                    tsCol.addNull();
                    sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                    // Row 3: non-null timestamp
                    tsCol.addLong(1_609_459_200_000_001L);
                    sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_null_ts");
                serverMain.assertSql(
                        "SELECT count() FROM test_null_ts WHERE event_time IS NULL",
                        "count\n1\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM test_null_ts WHERE event_time IS NOT NULL",
                        "count\n2\n"
                );
            }
        });
    }

    @Test
    public void testNullUuid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Use fast-path API to send null UUID via null bitmap
                    QwpTableBuffer buf = sender.getTableBuffer("test_null_uuid");
                    QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn("id", TYPE_UUID, true);

                    // Row 1: non-null UUID
                    col.addUuid(0x0123456789ABCDEFL, 0xFEDCBA9876543210L);
                    sender.at(1_000_000_000_000L, ChronoUnit.MICROS);
                    // Row 2: null UUID
                    col.addNull();
                    sender.at(1_000_000_000_001L, ChronoUnit.MICROS);
                    // Row 3: non-null UUID
                    col.addUuid(0xAAAABBBBCCCCDDDDL, 0x1111222233334444L);
                    sender.at(1_000_000_000_002L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_null_uuid");
                serverMain.assertSql(
                        "SELECT count() FROM test_null_uuid WHERE id IS NULL",
                        "count\n1\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM test_null_uuid WHERE id IS NOT NULL",
                        "count\n2\n"
                );
            }
        });
    }

    @Test
    public void testSingleRowWithBooleanColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_bool")
                            .boolColumn("active", true)
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_bool");
                serverMain.assertSql("select count() from test_bool", "count\n1\n");
                serverMain.assertSql("select active from test_bool", "active\ntrue\n");
            }
        });
    }

    @Test
    public void testSingleRowWithDoubleColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_double")
                            .doubleColumn("temperature", 23.5)
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_double");
                serverMain.assertSql("select count() from test_double", "count\n1\n");
            }
        });
    }

    @Test
    public void testSingleRowWithLongColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_long")
                            .longColumn("value", 12345L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_long");
                serverMain.assertSql("select count() from test_long", "count\n1\n");
                serverMain.assertSql("select value from test_long", "value\n12345\n");
            }
        });
    }

    @Test
    public void testSingleRowWithStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_string")
                            .stringColumn("message", "hello world")
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_string");
                serverMain.assertSql("select count() from test_string", "count\n1\n");
                serverMain.assertSql("select message from test_string", "message\nhello world\n");
            }
        });
    }

    @Test
    public void testTimestampColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_ts_col")
                            .timestampColumn("event_time", 1609459200000000L, ChronoUnit.MICROS)
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_ts_col");
                serverMain.assertSql("select count() from test_ts_col", "count\n1\n");
            }
        });
    }
}
