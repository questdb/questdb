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
import io.questdb.client.cutlass.ilpv4.client.IlpV4WebSocketSender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * End-to-end integration tests for ILP v4 WebSocket sender.
 * <p>
 * These tests verify that data sent via IlpV4WebSocketSender over WebSocket
 * is correctly written to QuestDB tables and can be queried.
 * <p>
 * <b>NOTE:</b> These tests require server-side WebSocket support to be enabled.
 * The WebSocket endpoint must be registered in HttpServer for the /write/v4 path.
 * Currently, only HTTP POST is supported on /write/v4.
 * <p>
 * To enable these tests, the server needs to register IlpV4WebSocketHttpProcessor
 * to handle WebSocket upgrade requests on /write/v4.
 */
public class IlpV4WebSocketSenderE2ETest extends AbstractBootstrapTest {

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testSingleRowWithLongColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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
    public void testSingleRowWithDoubleColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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
    public void testSingleRowWithStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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
    public void testSingleRowWithBooleanColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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
    public void testMultipleColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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
    public void testAtNowServerAssignedTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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
    public void testTimestampColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
                    sender.table("test_ts_col")
                            .timestampColumn("event_time", 1609459200000000L, ChronoUnit.MICROS)
                            .at(1000000000000L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("test_ts_col");
                serverMain.assertSql("select count() from test_ts_col", "count\n1\n");
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

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connect("localhost", httpPort)) {
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

    // ==================== ASYNC MODE TESTS ====================

    @Test
    public void testAsyncModeSingleRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync("localhost", httpPort, false)) {
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

    @Test
    public void testAsyncModeMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync("localhost", httpPort, false)) {
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
    public void testAsyncModeWithRowBasedFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Configure to flush every 10 rows
                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
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
                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
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
    public void testAsyncModeLargeNumberOfRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync("localhost", httpPort, false)) {
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
    public void testAsyncModeAutoFlushOnClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Don't call flush() - close() should flush automatically
                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync("localhost", httpPort, false)) {
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
    public void testAsyncModeWithMultipleTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync("localhost", httpPort, false)) {
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
}
