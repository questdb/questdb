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

package io.questdb.test.cutlass.qwp.e2e;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end tests for the QWP (QuestDB Wire Protocol) WebSocket sender.
 * <p>
 * Tests verify that all QWP native types arrive correctly (exact type match)
 * and that reasonable type coercions work (e.g., client sends INT but server
 * column is LONG).
 * <p>
 * Each test starts an in-process QuestDB server, sends data via
 * {@link QwpWebSocketSender}, and asserts the results with SQL queries.
 */
public class QwpSenderE2ETest extends AbstractBootstrapTest {

    public static <T> T createDoubleArray(int... shape) {
        int[] indices = new int[shape.length];
        return buildNestedArray(ArrayDataType.DOUBLE, shape, 0, indices);
    }

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
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort, false)) {
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

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort, false)) {
                    for (int i = 0; i < 25_000_000; i++) {
                        sender.table("async_large")
                                .longColumn("id", i)
                                .doubleColumn("value", i * 1.1)
                                .at(1000000000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("async_large");
                serverMain.assertSql("select count() from async_large", "count\n25000000\n");
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

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort, false)) {
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

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort, false)) {
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
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", httpPort, false,
                        2, // autoFlushRows - very small to force many batches
                        1024 * 1024, // autoFlushBytes
                        100_000_000L, // autoFlushIntervalNanos
                        QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                        null
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

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort, false)) {
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
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", httpPort, false,
                        10, // autoFlushRows
                        1024 * 1024, // autoFlushBytes
                        100_000_000L, // autoFlushIntervalNanos
                        QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                        null
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
    public void testBoolean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("b", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .boolColumn("b", false)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT b, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                b\ttimestamp
                                true\t1970-01-01T00:00:01.000000Z
                                false\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("b", (byte) -1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("b", (byte) 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("b", (byte) 127)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
            }
        });
    }

    @Test
    public void testChar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("c", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .charColumn("c", 'Z')
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT c, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                c\ttimestamp
                                A\t1970-01-01T00:00:01.000000Z
                                Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testCoercionToBoolean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_boolean";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_string BOOLEAN, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("from_string", "true")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_string FROM " + table,
                        """
                                from_string
                                true
                                """);
            }
        });
    }

    @Test
    public void testCoercionToBooleanErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_boolean_err";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).byteColumn("v", (byte) 1).at(1_000_000, ChronoUnit.MICROS),
                        "BYTE", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "cannot write", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).decimalColumn("v", Decimal64.fromLong(100, 2)).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DECIMAL64", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DOUBLE", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.5f).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write FLOAT", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).intColumn("v", 1).at(1_000_000, ChronoUnit.MICROS),
                        "INT", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", 1L).at(1_000_000, ChronoUnit.MICROS),
                        "LONG", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write LONG256", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).shortColumn("v", (short) 1).at(1_000_000, ChronoUnit.MICROS),
                        "SHORT", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "cannot write UUID", "BOOLEAN");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "yes").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse boolean from string", "cannot parse boolean from string");
            }
        });
    }

    @Test
    public void testCoercionToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_double BYTE, from_float BYTE, from_int BYTE, from_long BYTE, from_short BYTE, from_string BYTE, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("from_double", 42.0)
                            .floatColumn("from_float", 7.0f)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "42")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_double, from_float, from_int, from_long, from_short, from_string FROM " + table,
                        """
                                from_double\tfrom_float\tfrom_int\tfrom_long\tfrom_short\tfrom_string
                                42\t7\t42\t42\t42\t42
                                """);
            }
        });
    }

    @Test
    public void testCoercionToByteErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_byte_err";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 200.0).at(1_000_000, ChronoUnit.MICROS),
                        "integer value 200 out of range for BYTE", "integer value 200 out of range for BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 42.5).at(1_000_000, ChronoUnit.MICROS),
                        "loses precision", "42.5");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).intColumn("v", 128).at(1_000_000, ChronoUnit.MICROS),
                        "integer value 128 out of range for BYTE", "integer value 128 out of range for BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", 128L).at(1_000_000, ChronoUnit.MICROS),
                        "integer value 128 out of range for BYTE", "integer value 128 out of range for BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).shortColumn("v", (short) 200).at(1_000_000, ChronoUnit.MICROS),
                        "integer value 200 out of range for BYTE", "integer value 200 out of range for BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "BYTE");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "abc").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse BYTE from string", "cannot parse BYTE from string");
            }
        });
    }

    @Test
    public void testCoercionToChar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_char";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_string CHAR, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("from_string", "A")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_string FROM " + table,
                        """
                                from_string
                                A
                                """);
            }
        });
    }

    @Test
    public void testCoercionToCharErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_char_err";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "CHAR");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).byteColumn("v", (byte) 65).at(1_000_000, ChronoUnit.MICROS),
                        "BYTE", "CHAR");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DOUBLE", "CHAR");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.5f).at(1_000_000, ChronoUnit.MICROS),
                        "CHAR", "FLOAT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).intColumn("v", 65).at(1_000_000, ChronoUnit.MICROS),
                        "INT", "CHAR");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", 65L).at(1_000_000, ChronoUnit.MICROS),
                        "LONG", "CHAR");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "CHAR", "LONG256");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).shortColumn("v", (short) 65).at(1_000_000, ChronoUnit.MICROS),
                        "CHAR", "SHORT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "CHAR");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "CHAR");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "cannot write UUID", "CHAR");
            }
        });
    }

    @Test
    public void testCoercionToDate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_date";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte DATE, from_int DATE, from_long DATE, from_short DATE, from_string DATE, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 0)
                            .intColumn("from_int", 86_400_000)
                            .longColumn("from_long", 86_400_000L)
                            .shortColumn("from_short", (short) 0)
                            .stringColumn("from_string", "2022-02-25T00:00:00.000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_byte, from_int, from_long, from_short, from_string FROM " + table,
                        """
                                from_byte\tfrom_int\tfrom_long\tfrom_short\tfrom_string
                                1970-01-01T00:00:00.000Z\t1970-01-02T00:00:00.000Z\t1970-01-02T00:00:00.000Z\t1970-01-01T00:00:00.000Z\t2022-02-25T00:00:00.000Z
                                """);
            }
        });
    }

    @Test
    public void testCoercionToDateErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_date_err";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "DATE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "DATE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).decimalColumn("v", Decimal64.fromLong(100, 2)).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DECIMAL64", "DATE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from DOUBLE to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.5f).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from FLOAT to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "DATE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "DATE");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_date").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse DATE from string", "not_a_date");
            }
        });
    }

    @Test
    public void testCoercionToDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_decimal";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte DECIMAL(10,2), from_double DECIMAL(10,2), from_float DECIMAL(10,2), " +
                        "from_int DECIMAL(10,2), from_long DECIMAL(10,2), from_string DECIMAL(10,2), ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 42)
                            .doubleColumn("from_double", 123.45)
                            .floatColumn("from_float", 1.5f)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .stringColumn("from_string", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_byte, from_double, from_float, from_int, from_long, from_string FROM " + table,
                        """
                                from_byte\tfrom_double\tfrom_float\tfrom_int\tfrom_long\tfrom_string
                                42.00\t123.45\t1.50\t42.00\t42.00\t123.45
                                """);
            }
        });
    }

    @Test
    public void testCoercionToDecimalErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_decimal_err";
                serverMain.execute("CREATE TABLE " + table + " (v DECIMAL(2,1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "DECIMAL");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 123.456).at(1_000_000, ChronoUnit.MICROS),
                        "cannot be converted to", "scale=1");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.25f).at(1_000_000, ChronoUnit.MICROS),
                        "cannot be converted to", "scale=1");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 1, 1, 1).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write LONG256", "DECIMAL");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "DECIMAL");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "DECIMAL");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "cannot write UUID", "DECIMAL");
            }
        });
    }

    @Test
    public void testCoercionToDecimalVariants() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // dec8: DECIMAL(2,1)
                String dec8 = "test_qwp_dec8_coerce";
                serverMain.execute("CREATE TABLE " + dec8 + " (from_int DECIMAL(2,1), from_long DECIMAL(2,1), from_byte DECIMAL(2,1), from_short DECIMAL(2,1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(dec8)
                            .intColumn("from_int", 5)
                            .longColumn("from_long", 5)
                            .byteColumn("from_byte", (byte) 5)
                            .shortColumn("from_short", (short) 5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(dec8);
                serverMain.assertSql("SELECT from_int, from_long, from_byte, from_short FROM " + dec8,
                        "from_int\tfrom_long\tfrom_byte\tfrom_short\n5.0\t5.0\t5.0\t5.0\n");

                // dec16: DECIMAL(4,1)
                String dec16 = "test_qwp_dec16_coerce";
                serverMain.execute("CREATE TABLE " + dec16 + " (from_int DECIMAL(4,1), from_long DECIMAL(4,1), from_byte DECIMAL(4,1), from_short DECIMAL(4,1), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(dec16)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .byteColumn("from_byte", (byte) 42)
                            .shortColumn("from_short", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(dec16);
                serverMain.assertSql("SELECT from_int, from_long, from_byte, from_short FROM " + dec16,
                        "from_int\tfrom_long\tfrom_byte\tfrom_short\n42.0\t42.0\t42.0\t42.0\n");

                // dec32: DECIMAL(6,2)
                String dec32 = "test_qwp_dec32_coerce";
                serverMain.execute("CREATE TABLE " + dec32 + " (from_int DECIMAL(6,2), from_long DECIMAL(6,2), from_byte DECIMAL(6,2), from_short DECIMAL(6,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(dec32)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .byteColumn("from_byte", (byte) 42)
                            .shortColumn("from_short", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(dec32);
                serverMain.assertSql("SELECT from_int, from_long, from_byte, from_short FROM " + dec32,
                        "from_int\tfrom_long\tfrom_byte\tfrom_short\n42.00\t42.00\t42.00\t42.00\n");

                // dec64: DECIMAL(18,2)
                String dec64 = "test_qwp_dec64_coerce";
                serverMain.execute("CREATE TABLE " + dec64 + " (from_int DECIMAL(18,2), from_long DECIMAL(18,2), from_byte DECIMAL(18,2), from_short DECIMAL(18,2), from_string DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(dec64)
                            .intColumn("from_int", Integer.MAX_VALUE)
                            .longColumn("from_long", 42)
                            .byteColumn("from_byte", (byte) 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(dec64);
                serverMain.assertSql("SELECT from_int, from_long, from_byte, from_short, from_string FROM " + dec64,
                        "from_int\tfrom_long\tfrom_byte\tfrom_short\tfrom_string\n2147483647.00\t42.00\t42.00\t42.00\t123.45\n");

                // dec128: DECIMAL(38,2)
                String dec128 = "test_qwp_dec128_coerce";
                serverMain.execute("CREATE TABLE " + dec128 + " (from_int DECIMAL(38,2), from_long DECIMAL(38,2), from_byte DECIMAL(38,2), from_short DECIMAL(38,2), from_string DECIMAL(38,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(dec128)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 1_000_000_000L)
                            .byteColumn("from_byte", (byte) 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(dec128);
                serverMain.assertSql("SELECT from_int, from_long, from_byte, from_short, from_string FROM " + dec128,
                        "from_int\tfrom_long\tfrom_byte\tfrom_short\tfrom_string\n42.00\t1000000000.00\t42.00\t42.00\t123.45\n");

                // dec256: DECIMAL(76,2)
                String dec256 = "test_qwp_dec256_coerce";
                serverMain.execute("CREATE TABLE " + dec256 + " (from_int DECIMAL(76,2), from_long DECIMAL(76,2), from_byte DECIMAL(76,2), from_short DECIMAL(76,2), from_string DECIMAL(76,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(dec256)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", Long.MAX_VALUE)
                            .byteColumn("from_byte", (byte) 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(dec256);
                serverMain.assertSql("SELECT from_int, from_long, from_byte, from_short, from_string FROM " + dec256,
                        "from_int\tfrom_long\tfrom_byte\tfrom_short\tfrom_string\n42.00\t9223372036854775807.00\t42.00\t42.00\t123.45\n");

                // cross-decimal: dec64 from dec128/dec256
                String xDec64 = "test_qwp_x_dec64_coerce";
                serverMain.execute("CREATE TABLE " + xDec64 + " (from_dec128 DECIMAL(18,2), from_dec256 DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(xDec64)
                            .decimalColumn("from_dec128", Decimal128.fromLong(12345, 2))
                            .decimalColumn("from_dec256", Decimal256.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(xDec64);
                serverMain.assertSql("SELECT from_dec128, from_dec256 FROM " + xDec64,
                        "from_dec128\tfrom_dec256\n123.45\t123.45\n");

                // cross-decimal: dec128 from dec64/dec256
                String xDec128 = "test_qwp_x_dec128_coerce";
                serverMain.execute("CREATE TABLE " + xDec128 + " (from_dec64 DECIMAL(38,2), from_dec256 DECIMAL(38,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(xDec128)
                            .decimalColumn("from_dec64", Decimal64.fromLong(12345, 2))
                            .decimalColumn("from_dec256", Decimal256.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(xDec128);
                serverMain.assertSql("SELECT from_dec64, from_dec256 FROM " + xDec128,
                        "from_dec64\tfrom_dec256\n123.45\t123.45\n");

                // cross-decimal: dec256 from dec64/dec128
                String xDec256 = "test_qwp_x_dec256_coerce";
                serverMain.execute("CREATE TABLE " + xDec256 + " (from_dec64 DECIMAL(76,2), from_dec128 DECIMAL(76,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(xDec256)
                            .decimalColumn("from_dec64", Decimal64.fromLong(12345, 2))
                            .decimalColumn("from_dec128", Decimal128.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(xDec256);
                serverMain.assertSql("SELECT from_dec64, from_dec128 FROM " + xDec256,
                        "from_dec64\tfrom_dec128\n123.45\t123.45\n");
            }
        });
    }

    @Test
    public void testCoercionToDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_double";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte DOUBLE, from_float DOUBLE, from_int DOUBLE, from_long DOUBLE, from_short DOUBLE, from_string DOUBLE, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 42)
                            .floatColumn("from_float", 1.5f)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "3.14")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
            }
        });
    }

    @Test
    public void testCoercionToDoubleArrayErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE test_da_int_err (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                assertCoercionError(httpPort, "test_da_int_err",
                        (s, t) -> s.table(t).doubleArray("v", new double[]{1.0, 2.0}).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DOUBLE_ARRAY", "INT");

                serverMain.execute("CREATE TABLE test_da_str_err (v STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                assertCoercionError(httpPort, "test_da_str_err",
                        (s, t) -> s.table(t).doubleArray("v", new double[]{1.0, 2.0}).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DOUBLE_ARRAY", "STRING");

                serverMain.execute("CREATE TABLE test_da_sym_err (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                assertCoercionError(httpPort, "test_da_sym_err",
                        (s, t) -> s.table(t).doubleArray("v", new double[]{1.0, 2.0}).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DOUBLE_ARRAY", "SYMBOL");

                serverMain.execute("CREATE TABLE test_da_ts_err (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                assertCoercionError(httpPort, "test_da_ts_err",
                        (s, t) -> s.table(t).doubleArray("v", new double[]{1.0, 2.0}).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DOUBLE_ARRAY", "TIMESTAMP");
            }
        });
    }

    @Test
    public void testCoercionToDoubleErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_double_err";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "DOUBLE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "DOUBLE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "DOUBLE");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "DOUBLE");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_number").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse DOUBLE from string", "not_a_number");
            }
        });
    }

    @Test
    public void testCoercionToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_float";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte FLOAT, from_double FLOAT, from_int FLOAT, from_long FLOAT, from_short FLOAT, from_string FLOAT, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 7)
                            .doubleColumn("from_double", 1.5)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 1000)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "3.14")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
            }
        });
    }

    @Test
    public void testCoercionToFloatErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_float_err";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "FLOAT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "FLOAT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "FLOAT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "FLOAT");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_number").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse FLOAT from string", "not_a_number");
            }
        });
    }

    @Test
    public void testCoercionToGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_geohash";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_string GEOHASH(5c), ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("from_string", "s24se")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_string FROM " + table,
                        """
                                from_string
                                s24se
                                """);
            }
        });
    }

    @Test
    public void testCoercionToGeoHashErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_geohash_err";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "GEOHASH");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).byteColumn("v", (byte) 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from BYTE to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from DOUBLE to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.5f).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from FLOAT to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).intColumn("v", 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from INT", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", 1L).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).shortColumn("v", (short) 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from SHORT to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "GEOHASH");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "GEOHASH");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "!!!").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse geohash from string", "!!!");
            }
        });
    }

    @Test
    public void testCoercionToInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_int";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte INT, from_double INT, from_float INT, from_long INT, from_short INT, from_string INT, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 42)
                            .doubleColumn("from_double", 100_000.0)
                            .floatColumn("from_float", 42.0f)
                            .longColumn("from_long", 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "42")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_byte, from_double, from_float, from_long, from_short, from_string FROM " + table,
                        """
                                from_byte\tfrom_double\tfrom_float\tfrom_long\tfrom_short\tfrom_string
                                42\t100000\t42\t42\t42\t42
                                """);
            }
        });
    }

    @Test
    public void testCoercionToIntErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_int_err";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "INT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "INT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "loses precision", "3.14");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 3.14f).at(1_000_000, ChronoUnit.MICROS),
                        "loses precision", "loses precision");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", (long) Integer.MAX_VALUE + 1).at(1_000_000, ChronoUnit.MICROS),
                        "integer value 2147483648 out of range for INT", "integer value 2147483648 out of range for INT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "INT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "INT");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_number").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse INT from string", "not_a_number");
            }
        });
    }

    @Test
    public void testCoercionToLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_long";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte LONG, from_double LONG, from_float LONG, from_int LONG, from_short LONG, from_string LONG, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 42)
                            .doubleColumn("from_double", 42.0)
                            .floatColumn("from_float", 1000.0f)
                            .intColumn("from_int", 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "1000000000000")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_byte, from_double, from_float, from_int, from_short, from_string FROM " + table,
                        """
                                from_byte\tfrom_double\tfrom_float\tfrom_int\tfrom_short\tfrom_string
                                42\t42\t1000\t42\t42\t1000000000000
                                """);
            }
        });
    }

    @Test
    public void testCoercionToLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_long256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_string LONG256, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("from_string", "0x04000000000000000300000000000000020000000000000001")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_string FROM " + table,
                        """
                                from_string
                                0x04000000000000000300000000000000020000000000000001
                                """);
            }
        });
    }

    @Test
    public void testCoercionToLong256Errors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_long256_err";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "LONG256");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).byteColumn("v", (byte) 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from BYTE to LONG256 is not supported", "type coercion from BYTE to LONG256 is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "LONG256");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from DOUBLE to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.5f).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from FLOAT to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).intColumn("v", 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from INT to LONG256 is not supported", "type coercion from INT to LONG256 is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", 1L).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG to LONG256 is not supported", "type coercion from LONG to LONG256 is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).shortColumn("v", (short) 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from SHORT to LONG256 is not supported", "type coercion from SHORT to LONG256 is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "LONG256");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "LONG256");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_long256").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse long256 from string", "not_a_long256");
            }
        });
    }

    @Test
    public void testCoercionToLongErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_long_err";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "LONG");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "LONG");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "LONG");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "LONG");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_number").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse LONG from string", "not_a_number");
            }
        });
    }

    @Test
    public void testCoercionToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_short";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte SHORT, from_double SHORT, from_float SHORT, from_int SHORT, from_long SHORT, from_string SHORT, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 42)
                            .doubleColumn("from_double", 100.0)
                            .floatColumn("from_float", 42.0f)
                            .intColumn("from_int", 1000)
                            .longColumn("from_long", 42)
                            .stringColumn("from_string", "42")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_byte, from_double, from_float, from_int, from_long, from_string FROM " + table,
                        """
                                from_byte\tfrom_double\tfrom_float\tfrom_int\tfrom_long\tfrom_string
                                42\t100\t42\t1000\t42\t42
                                """);
            }
        });
    }

    @Test
    public void testCoercionToShortErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_short_err";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "SHORT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "SHORT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).intColumn("v", 40000).at(1_000_000, ChronoUnit.MICROS),
                        "integer value 40000 out of range for SHORT", "integer value 40000 out of range for SHORT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", 40000L).at(1_000_000, ChronoUnit.MICROS),
                        "integer value 40000 out of range for SHORT", "integer value 40000 out of range for SHORT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "SHORT");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "SHORT");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to SHORT is not supported", "type coercion from UUID to SHORT is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_number").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse SHORT from string", "not_a_number");
            }
        });
    }

    @Test
    public void testCoercionToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_bool STRING, from_byte STRING, from_char STRING, from_decimal STRING, " +
                        "from_double STRING, from_float STRING, from_int STRING, from_long STRING, " +
                        "from_long256 STRING, from_symbol STRING, from_timestamp STRING, from_uuid STRING, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                long tsMicros = 1_645_747_200_000_000L;

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("from_bool", true)
                            .byteColumn("from_byte", (byte) 42)
                            .charColumn("from_char", 'Z')
                            .doubleColumn("from_double", 3.14)
                            .floatColumn("from_float", 1.5f)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .long256Column("from_long256", 1, 2, 3, 4)
                            .symbol("from_symbol", "sym_val")
                            .timestampColumn("from_timestamp", tsMicros, ChronoUnit.MICROS)
                            .uuidColumn("from_uuid", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .decimalColumn("from_decimal", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_bool, from_byte, from_char, from_decimal, from_symbol FROM " + table,
                        """
                                from_bool\tfrom_byte\tfrom_char\tfrom_decimal\tfrom_symbol
                                true\t42\tZ\t123.45\tsym_val
                                """);
                serverMain.assertSql(
                        "SELECT from_uuid, from_timestamp FROM " + table,
                        """
                                from_uuid\tfrom_timestamp
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t2022-02-25T00:00:00.000Z
                                """);
            }
        });
    }

    @Test
    public void testCoercionToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte SYMBOL, from_double SYMBOL, from_float SYMBOL, from_int SYMBOL, from_long SYMBOL, from_short SYMBOL, from_string SYMBOL, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 42)
                            .doubleColumn("from_double", 3.14)
                            .floatColumn("from_float", 1.5f)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .shortColumn("from_short", (short) 42)
                            .stringColumn("from_string", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
            }
        });
    }

    @Test
    public void testCoercionToSymbolErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_symbol_err";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "SYMBOL");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "cannot write", "SYMBOL");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).decimalColumn("v", Decimal64.fromLong(100, 2)).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DECIMAL64", "SYMBOL");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write LONG256", "SYMBOL");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "SYMBOL");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "cannot write UUID", "SYMBOL");
            }
        });
    }

    @Test
    public void testCoercionToTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_timestamp";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_byte TIMESTAMP, from_int TIMESTAMP, from_long TIMESTAMP, from_short TIMESTAMP, from_string TIMESTAMP, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("from_byte", (byte) 0)
                            .intColumn("from_int", 1_000_000)
                            .longColumn("from_long", 1_000_000L)
                            .shortColumn("from_short", (short) 0)
                            .stringColumn("from_string", "2022-02-25T00:00:00.000000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_byte, from_int, from_long, from_short, from_string FROM " + table,
                        """
                                from_byte\tfrom_int\tfrom_long\tfrom_short\tfrom_string
                                1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:00.000000Z\t2022-02-25T00:00:00.000000Z
                                """);
            }
        });
    }

    @Test
    public void testCoercionToTimestampErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_timestamp_err";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "TIMESTAMP");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "TIMESTAMP");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).decimalColumn("v", Decimal64.fromLong(100, 2)).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write DECIMAL64", "TIMESTAMP");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from DOUBLE to TIMESTAMP is not supported", "type coercion from DOUBLE to TIMESTAMP is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.5f).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from FLOAT to TIMESTAMP is not supported", "type coercion from FLOAT to TIMESTAMP is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to TIMESTAMP is not supported", "type coercion from LONG256 to TIMESTAMP is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "TIMESTAMP");
                assertCoercionError(httpPort, table,
                        (s, t) -> {
                            UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                            s.table(t).uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()).at(1_000_000, ChronoUnit.MICROS);
                        },
                        "type coercion from UUID to TIMESTAMP is not supported", "type coercion from UUID to TIMESTAMP is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not_a_timestamp").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse timestamp from string", "not_a_timestamp");
            }
        });
    }

    @Test
    public void testCoercionToTimestampNs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_timestamp_ns";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_string TIMESTAMP_NS, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("from_string", "2022-02-25T00:00:00.000000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_string FROM " + table,
                        """
                                from_string
                                2022-02-25T00:00:00.000000000Z
                                """);
            }
        });
    }

    @Test
    public void testCoercionToTimestampNsErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_ts_ns_err";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "TIMESTAMP");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "TIMESTAMP");
            }
        });
    }

    @Test
    public void testCoercionToUuid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_uuid";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_string UUID, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("from_string", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_string FROM " + table,
                        """
                                from_string
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                                """);
            }
        });
    }

    @Test
    public void testCoercionToUuidErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_uuid_err";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).boolColumn("v", true).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write BOOLEAN", "UUID");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).byteColumn("v", (byte) 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from BYTE to UUID is not supported", "type coercion from BYTE to UUID is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).charColumn("v", 'A').at(1_000_000, ChronoUnit.MICROS),
                        "not supported", "UUID");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).doubleColumn("v", 3.14).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from DOUBLE to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).floatColumn("v", 1.5f).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from FLOAT to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).intColumn("v", 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from INT to UUID is not supported", "type coercion from INT to UUID is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).longColumn("v", 1L).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG to UUID is not supported", "type coercion from LONG to UUID is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).long256Column("v", 1, 0, 0, 0).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from LONG256 to", "is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).shortColumn("v", (short) 1).at(1_000_000, ChronoUnit.MICROS),
                        "type coercion from SHORT to UUID is not supported", "type coercion from SHORT to UUID is not supported");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS),
                        "cannot write SYMBOL", "UUID");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS).at(1_000_000, ChronoUnit.MICROS),
                        "cannot write TIMESTAMP", "UUID");
                assertCoercionError(httpPort, table,
                        (s, t) -> s.table(t).stringColumn("v", "not-a-uuid").at(1_000_000, ChronoUnit.MICROS),
                        "cannot parse UUID from string", "not-a-uuid");
            }
        });
    }

    @Test
    public void testCoercionToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_coerce_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "from_bool VARCHAR, from_byte VARCHAR, from_char VARCHAR, from_decimal VARCHAR, " +
                        "from_double VARCHAR, from_float VARCHAR, from_int VARCHAR, from_long VARCHAR, " +
                        "from_long256 VARCHAR, from_symbol VARCHAR, from_timestamp VARCHAR, from_uuid VARCHAR, from_string VARCHAR, ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                long tsMicros = 1_645_747_200_000_000L;

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("from_bool", true)
                            .byteColumn("from_byte", (byte) 42)
                            .charColumn("from_char", 'Z')
                            .doubleColumn("from_double", 3.14)
                            .floatColumn("from_float", 1.5f)
                            .intColumn("from_int", 42)
                            .longColumn("from_long", 42)
                            .long256Column("from_long256", 1, 2, 3, 4)
                            .symbol("from_symbol", "sym_val")
                            .timestampColumn("from_timestamp", tsMicros, ChronoUnit.MICROS)
                            .uuidColumn("from_uuid", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .stringColumn("from_string", "hello")
                            .decimalColumn("from_decimal", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT from_bool, from_byte, from_char, from_decimal, from_symbol, from_string FROM " + table,
                        """
                                from_bool\tfrom_byte\tfrom_char\tfrom_decimal\tfrom_symbol\tfrom_string
                                true\t42\tZ\t123.45\tsym_val\thello
                                """);
                serverMain.assertSql(
                        "SELECT from_uuid, from_timestamp FROM " + table,
                        """
                                from_uuid\tfrom_timestamp
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t2022-02-25T00:00:00.000Z
                                """);
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
                        try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                                "localhost", httpPort, false,
                                autoFlushRows,
                                1024 * 1024,
                                100_000_000L,
                                QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                                null
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
                        try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                                "localhost", httpPort, false,
                                autoFlushRows,
                                1024 * 1024,
                                100_000_000L,
                                QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                                null
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
                        try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                                "localhost", httpPort, false,
                                autoFlushRows,
                                1024 * 1024,
                                100_000_000L,
                                QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE,
                                null
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
    public void testDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("d", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", "-999.99")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", "0.01")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal256.fromLong(42_000, 2))
                            .at(4_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n4\n");
            }
        });
    }

    @Test
    public void testDecimalRescale() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_rescale";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(10, 4), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Send with scale=2, but column expects scale=4 - should rescale
                    sender.table(table)
                            .decimalColumn("d", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT d FROM " + table,
                        """
                                d
                                123.4500
                                """);
            }
        });
    }

    @Test
    public void testDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("value", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("value", -2.718)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("value", 0.0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("value", Double.MAX_VALUE)
                            .at(4_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("value", Double.MIN_VALUE)
                            .at(5_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n5\n");
                serverMain.assertSql(
                        "SELECT value FROM " + table + " ORDER BY timestamp LIMIT 3",
                        """
                                value
                                3.14
                                -2.718
                                0.0
                                """);
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

                String table = "test_qwp_double_array";

                double[] arr1d = createDoubleArray(5);
                double[][] arr2d = createDoubleArray(2, 3);
                double[][][] arr3d = createDoubleArray(1, 2, 3);

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleArray("a1", arr1d)
                            .doubleArray("a2", arr2d)
                            .doubleArray("a3", arr3d)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
            }
        });
    }

    @Test
    public void testEmptyColumnNameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("ws_empty_col_name")
                            .longColumn("", 42)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    Assert.fail("Expected LineSenderException for empty column name");
                } catch (LineSenderException e) {
                    Assert.assertTrue("Error should mention empty column name: " + e.getMessage(),
                            e.getMessage().contains("column name cannot be empty"));
                }
            }
        });
    }

    @Test
    public void testEmptyTableNameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("")
                            .longColumn("value", 42)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    Assert.fail("Expected LineSenderException for empty table name");
                } catch (LineSenderException e) {
                    Assert.assertTrue("Error should mention empty table name: " + e.getMessage(),
                            e.getMessage().contains("table name cannot be empty"));
                }
            }
        });
    }

    @Test
    public void testFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("f", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("f", -2.25f)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("f", 0.0f)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT f FROM " + table + " ORDER BY timestamp",
                        """
                                f
                                1.5
                                -2.25
                                0.0
                                """);
            }
        });
    }

    @Test
    public void testInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("value", Integer.MIN_VALUE + 1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("value", 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("value", Integer.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    // Integer.MIN_VALUE is the null sentinel for INT
                    sender.table(table)
                            .intColumn("value", Integer.MIN_VALUE)
                            .at(4_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n4\n");
                serverMain.assertSql(
                        "SELECT value FROM " + table + " ORDER BY timestamp",
                        """
                                value
                                -2147483647
                                0
                                2147483647
                                null
                                """);
            }
        });
    }

    @Test
    public void testLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("value", Long.MIN_VALUE + 1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("value", 0L)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("value", Long.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    // Long.MIN_VALUE is the null sentinel for LONG
                    sender.table(table)
                            .longColumn("value", Long.MIN_VALUE)
                            .at(4_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n4\n");
                serverMain.assertSql(
                        "SELECT value FROM " + table + " ORDER BY timestamp",
                        """
                                value
                                -9223372036854775807
                                0
                                9223372036854775807
                                null
                                """);
            }
        });
    }

    @Test
    public void testLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // 256-bit value: 4 x 64-bit longs in little-endian order
                    sender.table(table)
                            .long256Column("value", 1, 2, 3, 4)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT value FROM " + table,
                        """
                                value
                                0x04000000000000000300000000000000020000000000000001
                                """);
            }
        });
    }

    @Test
    public void testMixedTimestampModesMicro() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_mixed_ts";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Row 1: microsecond timestamp
                    sender.table(table)
                            .longColumn("id", 1L)
                            .at(1_645_747_200_000_000L, ChronoUnit.MICROS);

                    // Row 2: nanosecond timestamp
                    sender.table(table)
                            .longColumn("id", 2L)
                            .at(1_645_747_201_000_000L, ChronoUnit.MICROS);

                    // Row 3: server-assigned
                    sender.table(table)
                            .longColumn("id", 3L)
                            .atNow();

                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT id FROM " + table + " ORDER BY timestamp LIMIT 2",
                        """
                                id
                                1
                                2
                                """);
            }
        });
    }

    @Test
    public void testMixedTimestampModesMicroTableNanoSender() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create a table with TIMESTAMP (microsecond) designated timestamp
                String table = "test_qwp_micro_table_nano_sender";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "value LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Send nanosecond timestamp to microsecond table
                    long tsNanos = 1_645_747_200_123_456_789L; // 2022-02-25T00:00:00Z + some nanos
                    sender.table(table)
                            .longColumn("value", 42L)
                            .at(tsNanos, ChronoUnit.NANOS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                // Nanoseconds should be truncated to microseconds
                serverMain.assertSql(
                        "SELECT value, ts FROM " + table,
                        """
                                value\tts
                                42\t2022-02-25T00:00:00.123456Z
                                """);
            }
        });
    }

    @Test
    public void testMixedTimestampModesNano() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_mixed_ts_nano";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Row 1: nanosecond timestamp
                    sender.table(table)
                            .longColumn("id", 1L)
                            .at(1_645_747_200_000_000_000L, ChronoUnit.NANOS);

                    // Row 2: microsecond timestamp
                    sender.table(table)
                            .longColumn("id", 2L)
                            .at(1_645_747_201_000_000_000L, ChronoUnit.NANOS);

                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT id FROM " + table + " ORDER BY timestamp",
                        """
                                id
                                1
                                2
                                """);
            }
        });
    }

    @Test
    public void testMixedTimestampModesNanoTableMicroSender() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create a table with TIMESTAMP_NS (nanosecond) designated timestamp
                String table = "test_qwp_nano_table_micro_sender";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "value LONG, " +
                        "ts TIMESTAMP_NS" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Send microsecond timestamp to nanosecond table
                    long tsMicros = 1_645_747_200_111_111L; // 2022-02-25T00:00:00Z + some micros
                    sender.table(table)
                            .longColumn("value", 99L)
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                // Microseconds should be scaled to nanoseconds
                serverMain.assertSql(
                        "SELECT value, ts FROM " + table,
                        """
                                value\tts
                                99\t2022-02-25T00:00:00.111111000Z
                                """);
            }
        });
    }

    @Test
    public void testMultipleRowsAndBatching() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_multiple_rows";

                int rowCount = 1000;
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < rowCount; i++) {
                        sender.table(table)
                                .symbol("sym", "s" + (i % 10))
                                .longColumn("val", i)
                                .doubleColumn("dbl", i * 1.5)
                                .at((long) (i + 1) * 1_000_000, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n" + rowCount + "\n");
            }
        });
    }

    @Test
    public void testNullColumnNameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("ws_null_col_name")
                            .longColumn(null, 42)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    Assert.fail("Expected LineSenderException for null column name");
                } catch (LineSenderException e) {
                    Assert.assertTrue("Error should mention empty column name: " + e.getMessage(),
                            e.getMessage().contains("column name cannot be empty"));
                }
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
                // Rows 0, 3, 6, 9, 12, 15, 18 -> 7 null longs
                serverMain.assertSql(
                        "SELECT count() FROM test_null_mixed WHERE l IS NULL",
                        "count\n7\n"
                );
                // Rows 0, 4, 8, 12, 16 -> 5 null doubles
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
    public void testNullStringCoercion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // boolean: null string -> false
                String boolTable = "test_qwp_null_string_to_boolean";
                serverMain.execute("CREATE TABLE " + boolTable + " (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(boolTable).stringColumn("b", "true").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(boolTable).stringColumn("b", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(boolTable);
                serverMain.assertSql("SELECT b, ts FROM " + boolTable + " ORDER BY ts",
                        "b\tts\ntrue\t1970-01-01T00:00:01.000000Z\nfalse\t1970-01-01T00:00:02.000000Z\n");

                // byte: null string -> 0
                String byteTable = "test_qwp_null_string_to_byte";
                serverMain.execute("CREATE TABLE " + byteTable + " (b BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(byteTable).stringColumn("b", "42").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(byteTable).stringColumn("b", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(byteTable);
                serverMain.assertSql("SELECT b, ts FROM " + byteTable + " ORDER BY ts",
                        "b\tts\n42\t1970-01-01T00:00:01.000000Z\n0\t1970-01-01T00:00:02.000000Z\n");

                // char: null string -> empty
                String charTable = "test_qwp_null_string_to_char";
                serverMain.execute("CREATE TABLE " + charTable + " (c CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(charTable).stringColumn("c", "A").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(charTable).stringColumn("c", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(charTable);
                serverMain.assertSql("SELECT c, ts FROM " + charTable + " ORDER BY ts",
                        "c\tts\nA\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // date: null string -> empty
                String dateTable = "test_qwp_null_string_to_date";
                serverMain.execute("CREATE TABLE " + dateTable + " (d DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(dateTable).stringColumn("d", "2022-02-25T00:00:00.000Z").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(dateTable).stringColumn("d", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(dateTable);
                serverMain.assertSql("SELECT d, ts FROM " + dateTable + " ORDER BY ts",
                        "d\tts\n2022-02-25T00:00:00.000Z\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // decimal: null string -> empty
                String decTable = "test_qwp_null_string_to_decimal";
                serverMain.execute("CREATE TABLE " + decTable + " (d DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(decTable).stringColumn("d", "123.45").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(decTable).stringColumn("d", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(decTable);
                serverMain.assertSql("SELECT d, ts FROM " + decTable + " ORDER BY ts",
                        "d\tts\n123.45\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // float: null string -> null
                String floatTable = "test_qwp_null_string_to_float";
                serverMain.execute("CREATE TABLE " + floatTable + " (f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(floatTable).stringColumn("f", "3.14").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(floatTable).stringColumn("f", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(floatTable);
                serverMain.assertSql("SELECT f, ts FROM " + floatTable + " ORDER BY ts",
                        "f\tts\n3.14\t1970-01-01T00:00:01.000000Z\nnull\t1970-01-01T00:00:02.000000Z\n");

                // geohash: null string -> empty
                String geoTable = "test_qwp_null_string_to_geohash";
                serverMain.execute("CREATE TABLE " + geoTable + " (g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(geoTable).stringColumn("g", "s09wh").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(geoTable).stringColumn("g", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(geoTable);
                serverMain.assertSql("SELECT g, ts FROM " + geoTable + " ORDER BY ts",
                        "g\tts\ns09wh\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // int/long/double: null string -> null
                String numTable = "test_qwp_null_string_to_numeric";
                serverMain.execute("CREATE TABLE " + numTable + " (i INT, l LONG, d DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(numTable).stringColumn("i", "42").stringColumn("l", "100").stringColumn("d", "3.14").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(numTable).stringColumn("i", null).stringColumn("l", null).stringColumn("d", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(numTable);
                serverMain.assertSql("SELECT i, l, d, ts FROM " + numTable + " ORDER BY ts",
                        "i\tl\td\tts\n42\t100\t3.14\t1970-01-01T00:00:01.000000Z\nnull\tnull\tnull\t1970-01-01T00:00:02.000000Z\n");

                // long256: null string -> empty
                String l256Table = "test_qwp_null_string_to_long256";
                serverMain.execute("CREATE TABLE " + l256Table + " (l LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(l256Table).stringColumn("l", "0x01").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(l256Table).stringColumn("l", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(l256Table);
                serverMain.assertSql("SELECT l, ts FROM " + l256Table + " ORDER BY ts",
                        "l\tts\n0x01\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // short: null string -> 0
                String shortTable = "test_qwp_null_string_to_short";
                serverMain.execute("CREATE TABLE " + shortTable + " (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(shortTable).stringColumn("s", "42").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(shortTable).stringColumn("s", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(shortTable);
                serverMain.assertSql("SELECT s, ts FROM " + shortTable + " ORDER BY ts",
                        "s\tts\n42\t1970-01-01T00:00:01.000000Z\n0\t1970-01-01T00:00:02.000000Z\n");

                // symbol: null string -> empty
                String symTable = "test_qwp_null_string_to_symbol";
                serverMain.execute("CREATE TABLE " + symTable + " (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(symTable).stringColumn("s", "alpha").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(symTable).stringColumn("s", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(symTable);
                serverMain.assertSql("SELECT s, ts FROM " + symTable + " ORDER BY ts",
                        "s\tts\nalpha\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // timestamp: null string -> empty
                String tsTable = "test_qwp_null_string_to_timestamp";
                serverMain.execute("CREATE TABLE " + tsTable + " (t TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(tsTable).stringColumn("t", "2022-02-25T00:00:00.000000Z").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(tsTable).stringColumn("t", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(tsTable);
                serverMain.assertSql("SELECT t, ts FROM " + tsTable + " ORDER BY ts",
                        "t\tts\n2022-02-25T00:00:00.000000Z\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // timestamp_ns: null string -> empty
                String tsNsTable = "test_qwp_null_string_to_timestamp_ns";
                serverMain.execute("CREATE TABLE " + tsNsTable + " (t TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(tsNsTable).stringColumn("t", "2022-02-25T00:00:00.000000Z").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(tsNsTable).stringColumn("t", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(tsNsTable);
                serverMain.assertSql("SELECT t, ts FROM " + tsNsTable + " ORDER BY ts",
                        "t\tts\n2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // uuid: null string -> empty
                String uuidTable = "test_qwp_null_string_to_uuid";
                serverMain.execute("CREATE TABLE " + uuidTable + " (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(uuidTable).stringColumn("u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(uuidTable).stringColumn("u", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(uuidTable);
                serverMain.assertSql("SELECT u, ts FROM " + uuidTable + " ORDER BY ts",
                        "u\tts\na0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // varchar: null string -> empty
                String varcharTable = "test_qwp_null_string_to_varchar";
                serverMain.execute("CREATE TABLE " + varcharTable + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(varcharTable).stringColumn("v", "hello").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(varcharTable).stringColumn("v", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(varcharTable);
                serverMain.assertSql("SELECT v, ts FROM " + varcharTable + " ORDER BY ts",
                        "v\tts\nhello\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");
            }
        });
    }

    @Test
    public void testNullSymbolCoercion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // null symbol to STRING
                String strTable = "test_qwp_null_symbol_to_string";
                serverMain.execute("CREATE TABLE " + strTable + " (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(strTable).symbol("s", "hello").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(strTable).symbol("s", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(strTable);
                serverMain.assertSql("SELECT s, ts FROM " + strTable + " ORDER BY ts",
                        "s\tts\nhello\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // null symbol to SYMBOL
                String symTable = "test_qwp_null_symbol_to_symbol";
                serverMain.execute("CREATE TABLE " + symTable + " (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(symTable).symbol("s", "alpha").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(symTable).symbol("s", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(symTable);
                serverMain.assertSql("SELECT s, ts FROM " + symTable + " ORDER BY ts",
                        "s\tts\nalpha\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");

                // null symbol to VARCHAR
                String varcharTable = "test_qwp_null_symbol_to_varchar";
                serverMain.execute("CREATE TABLE " + varcharTable + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(varcharTable).symbol("v", "hello").at(1_000_000, ChronoUnit.MICROS);
                    sender.table(varcharTable).symbol("v", null).at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(varcharTable);
                serverMain.assertSql("SELECT v, ts FROM " + varcharTable + " ORDER BY ts",
                        "v\tts\nhello\t1970-01-01T00:00:01.000000Z\n\t1970-01-01T00:00:02.000000Z\n");
            }
        });
    }

    @Test
    public void testNullTableNameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(null)
                            .longColumn("value", 42)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    Assert.fail("Expected LineSenderException for null table name");
                } catch (LineSenderException e) {
                    Assert.assertTrue("Error should mention empty table name: " + e.getMessage(),
                            e.getMessage().contains("table name cannot be empty"));
                }
            }
        });
    }

    @Test
    public void testOmittedColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("""
                        CREATE TABLE omit_all (
                            bool_col BOOLEAN,
                            byte_col BYTE,
                            char_col CHAR,
                            double_col DOUBLE,
                            float_col FLOAT,
                            int_col INT,
                            long256_col LONG256,
                            long_col LONG,
                            short_col SHORT,
                            string_col STRING,
                            symbol_col SYMBOL,
                            timestamp_col TIMESTAMP,
                            uuid_col UUID,
                            varchar_col VARCHAR,
                            ts TIMESTAMP
                        ) TIMESTAMP(ts) PARTITION BY DAY WAL""");

                UUID uuid1 = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                UUID uuid2 = UUID.fromString("11111111-2222-3333-4444-555555555555");
                long tsMicros = 1_645_747_200_000_000L;

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // row 1: all columns set
                    sender.table("omit_all")
                            .boolColumn("bool_col", true)
                            .byteColumn("byte_col", (byte) 1)
                            .charColumn("char_col", 'A')
                            .doubleColumn("double_col", 1.5)
                            .floatColumn("float_col", 1.5f)
                            .intColumn("int_col", 42)
                            .long256Column("long256_col", 1, 0, 0, 0)
                            .longColumn("long_col", 100L)
                            .shortColumn("short_col", (short) 100)
                            .stringColumn("string_col", "hello")
                            .symbol("symbol_col", "alpha")
                            .timestampColumn("timestamp_col", tsMicros, ChronoUnit.MICROS)
                            .uuidColumn("uuid_col", uuid1.getLeastSignificantBits(), uuid1.getMostSignificantBits())
                            .stringColumn("varchar_col", "hello")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    // row 2: all columns omitted
                    sender.table("omit_all")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    // row 3: all columns set with different values
                    sender.table("omit_all")
                            .boolColumn("bool_col", false)
                            .byteColumn("byte_col", (byte) -1)
                            .charColumn("char_col", 'Z')
                            .doubleColumn("double_col", -2.5)
                            .floatColumn("float_col", -2.5f)
                            .intColumn("int_col", -100)
                            .long256Column("long256_col", 0, 0, 0, 2)
                            .longColumn("long_col", -200L)
                            .shortColumn("short_col", (short) -200)
                            .stringColumn("string_col", "world")
                            .symbol("symbol_col", "beta")
                            .timestampColumn("timestamp_col", tsMicros + 1_000_000L, ChronoUnit.MICROS)
                            .uuidColumn("uuid_col", uuid2.getLeastSignificantBits(), uuid2.getMostSignificantBits())
                            .stringColumn("varchar_col", "world")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    // row 4: all columns omitted
                    sender.table("omit_all")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_all");

                serverMain.assertSql(
                        "SELECT bool_col FROM omit_all ORDER BY ts",
                        "bool_col\ntrue\nfalse\nfalse\nfalse\n"
                );
                serverMain.assertSql(
                        "SELECT byte_col FROM omit_all ORDER BY ts",
                        "byte_col\n1\n0\n-1\n0\n"
                );
                serverMain.assertSql(
                        "SELECT char_col FROM omit_all ORDER BY ts",
                        "char_col\nA\n\nZ\n\n"
                );
                serverMain.assertSql(
                        "SELECT double_col FROM omit_all ORDER BY ts",
                        "double_col\n1.5\nnull\n-2.5\nnull\n"
                );
                serverMain.assertSql(
                        "SELECT float_col FROM omit_all ORDER BY ts",
                        "float_col\n1.5\nnull\n-2.5\nnull\n"
                );
                serverMain.assertSql(
                        "SELECT int_col FROM omit_all ORDER BY ts",
                        "int_col\n42\nnull\n-100\nnull\n"
                );
                serverMain.assertSql(
                        "SELECT long256_col FROM omit_all ORDER BY ts",
                        "long256_col\n0x01\n\n0x02000000000000000000000000000000000000000000000000\n\n"
                );
                serverMain.assertSql(
                        "SELECT long_col FROM omit_all ORDER BY ts",
                        "long_col\n100\nnull\n-200\nnull\n"
                );
                serverMain.assertSql(
                        "SELECT short_col FROM omit_all ORDER BY ts",
                        "short_col\n100\n0\n-200\n0\n"
                );
                serverMain.assertSql(
                        "SELECT string_col FROM omit_all ORDER BY ts",
                        "string_col\nhello\n\nworld\n\n"
                );
                serverMain.assertSql(
                        "SELECT symbol_col FROM omit_all ORDER BY ts",
                        "symbol_col\nalpha\n\nbeta\n\n"
                );
                serverMain.assertSql(
                        "SELECT timestamp_col FROM omit_all ORDER BY ts",
                        """
                                timestamp_col
                                2022-02-25T00:00:00.000000Z
                                \

                                2022-02-25T00:00:01.000000Z
                                \

                                """
                );
                serverMain.assertSql(
                        "SELECT uuid_col FROM omit_all ORDER BY ts",
                        """
                                uuid_col
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                                \

                                11111111-2222-3333-4444-555555555555
                                \

                                """
                );
                serverMain.assertSql(
                        "SELECT varchar_col FROM omit_all ORDER BY ts",
                        "varchar_col\nhello\n\nworld\n\n"
                );
            }
        });
    }

    @Test
    public void testSameColumnNameDifferentTypesDifferentTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 10; i++) {
                        // table A: "value" is LONG
                        sender.table("schema_iso_a")
                                .longColumn("value", i * 100L)
                                .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                        // table B: "value" is DOUBLE
                        sender.table("schema_iso_b")
                                .doubleColumn("value", i * 1.5)
                                .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("schema_iso_a");
                serverMain.awaitTable("schema_iso_b");

                // verify row counts
                serverMain.assertSql("SELECT count() FROM schema_iso_a", "count\n10\n");
                serverMain.assertSql("SELECT count() FROM schema_iso_b", "count\n10\n");

                // verify table A stores LONG values
                serverMain.assertSql(
                        "SELECT value FROM schema_iso_a ORDER BY timestamp LIMIT 3",
                        """
                                value
                                0
                                100
                                200
                                """
                );

                // verify table B stores DOUBLE values
                serverMain.assertSql(
                        "SELECT value FROM schema_iso_b ORDER BY timestamp LIMIT 3",
                        """
                                value
                                0.0
                                1.5
                                3.0
                                """
                );
            }
        });
    }

    @Test
    public void testSenderBuilderWebSocket() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_sender_builder_ws";

                try (Sender sender = Sender.builder(Sender.Transport.WEBSOCKET)
                        .address("localhost:" + httpPort)
                        .build()) {
                    sender.table(table)
                            .symbol("city", "London")
                            .doubleColumn("temp", 22.5)
                            .longColumn("humidity", 48)
                            .boolColumn("sunny", true)
                            .stringColumn("note", "clear sky")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .symbol("city", "Berlin")
                            .doubleColumn("temp", 18.3)
                            .longColumn("humidity", 65)
                            .boolColumn("sunny", false)
                            .stringColumn("note", "overcast")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT city, temp, humidity, sunny, note, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                city\ttemp\thumidity\tsunny\tnote\ttimestamp
                                London\t22.5\t48\ttrue\tclear sky\t1970-01-01T00:00:01.000000Z
                                Berlin\t18.3\t65\tfalse\tovercast\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Short.MIN_VALUE is the null sentinel for SHORT
                    sender.table(table)
                            .shortColumn("s", Short.MIN_VALUE)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("s", (short) 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("s", Short.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
            }
        });
    }

    @Test
    public void testString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("message", "Hello, World!")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("message", "")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("message", "unicode: éàü")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT message FROM " + table + " ORDER BY timestamp",
                        """
                                message
                                Hello, World!
                                \
                                
                                unicode: éàü
                                """);
            }
        });
    }

    @Test
    public void testSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("s", "alpha")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .symbol("s", "beta")
                            .at(2_000_000, ChronoUnit.MICROS);
                    // repeated value reuses dictionary entry
                    sender.table(table)
                            .symbol("s", "alpha")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                s\ttimestamp
                                alpha\t1970-01-01T00:00:01.000000Z
                                beta\t1970-01-01T00:00:02.000000Z
                                alpha\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testTimestampMicros() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_micros";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z in micros
                    sender.table(table)
                            .timestampColumn("ts_col", tsMicros, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
            }
        });
    }

    @Test
    public void testTimestampMicrosToNanos() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_micros_to_nanos";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "ts_col TIMESTAMP_NS, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    long tsMicros = 1_645_747_200_111_111L; // 2022-02-25T00:00:00Z
                    sender.table(table)
                            .timestampColumn("ts_col", tsMicros, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                // Microseconds scaled to nanoseconds
                serverMain.assertSql(
                        "SELECT ts_col, ts FROM " + table,
                        """
                                ts_col\tts
                                2022-02-25T00:00:00.111111000Z\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testTimestampNanos() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_nanos";

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    long tsNanos = 1_645_747_200_000_000_000L; // 2022-02-25T00:00:00Z in nanos
                    sender.table(table)
                            .timestampColumn("ts_col", tsNanos, ChronoUnit.NANOS)
                            .at(tsNanos, ChronoUnit.NANOS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
            }
        });
    }

    @Test
    public void testTimestampNanosToMicros() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_nanos_to_micros";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "ts_col TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    long tsNanos = 1_645_747_200_123_456_789L;
                    sender.table(table)
                            .timestampColumn("ts_col", tsNanos, ChronoUnit.NANOS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                // Nanoseconds truncated to microseconds
                serverMain.assertSql(
                        "SELECT ts_col, ts FROM " + table,
                        """
                                ts_col\tts
                                2022-02-25T00:00:00.123456Z\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testUuid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid";

                UUID uuid1 = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                UUID uuid2 = UUID.fromString("11111111-2222-3333-4444-555555555555");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .uuidColumn("u", uuid1.getLeastSignificantBits(), uuid1.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .uuidColumn("u", uuid2.getLeastSignificantBits(), uuid2.getMostSignificantBits())
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT u, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                u\ttimestamp
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000Z
                                11111111-2222-3333-4444-555555555555\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testWhitespaceTableNameRejected() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("   ")
                            .longColumn("value", 42)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);
                    Assert.fail("Expected LineSenderException for whitespace-only table name");
                } catch (LineSenderException e) {
                    Assert.assertTrue("Error should mention illegal characters: " + e.getMessage(),
                            e.getMessage().contains("table name contains illegal characters"));
                }
            }
        });
    }

    @Test
    public void testWriteAllTypesInOneRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_all_types";

                UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                double[] arr1d = {1.0, 2.0, 3.0};
                long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("sym", "test_symbol")
                            .boolColumn("bool_col", true)
                            .shortColumn("short_col", (short) 42)
                            .intColumn("int_col", 100_000)
                            .longColumn("long_col", 1_000_000_000L)
                            .floatColumn("float_col", 2.5f)
                            .doubleColumn("double_col", 3.14)
                            .stringColumn("string_col", "hello")
                            .charColumn("char_col", 'Z')
                            .timestampColumn("ts_col", tsMicros, ChronoUnit.MICROS)
                            .uuidColumn("uuid_col", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .long256Column("long256_col", 1, 0, 0, 0)
                            .doubleArray("arr_col", arr1d)
                            .decimalColumn("decimal_col", "99.99")
                            .at(tsMicros, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
            }
        });
    }

    private static void assertCoercionError(
            int httpPort, String table,
            java.util.function.BiConsumer<QwpWebSocketSender, String> sendAction,
            String expectedMsgPart1, String expectedMsgPart2
    ) {
        try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
            sendAction.accept(sender, table);
            sender.flush();
            Assert.fail("Expected LineSenderException");
        } catch (LineSenderException e) {
            String msg = e.getMessage();
            Assert.assertTrue("Expected error containing '" + expectedMsgPart1 +
                            "' and '" + expectedMsgPart2 + "' but got: " + msg,
                    msg.contains(expectedMsgPart1) && msg.contains(expectedMsgPart2));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T buildNestedArray(ArrayDataType dataType, int[] shape, int currentDim, int[] indices) {
        if (currentDim == shape.length - 1) {
            Object arr = dataType.createArray(shape[currentDim]);
            for (int i = 0; i < Array.getLength(arr); i++) {
                indices[currentDim] = i;
                dataType.setElement(arr, i, indices);
            }
            return (T) arr;
        } else {
            Class<?> componentType = dataType.getComponentType(shape.length - currentDim - 1);
            Object arr = Array.newInstance(componentType, shape[currentDim]);
            for (int i = 0; i < shape[currentDim]; i++) {
                indices[currentDim] = i;
                Object subArr = buildNestedArray(dataType, shape, currentDim + 1, indices);
                Array.set(arr, i, subArr);
            }
            return (T) arr;
        }
    }

    private enum ArrayDataType {
        DOUBLE(double.class) {
            @Override
            public Object createArray(int length) {
                return new double[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                double[] arr = (double[]) array;
                double product = 1.0;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        },
        LONG(long.class) {
            @Override
            public Object createArray(int length) {
                return new long[length];
            }

            @Override
            public void setElement(Object array, int index, int[] indices) {
                long[] arr = (long[]) array;
                long product = 1L;
                for (int idx : indices) {
                    product *= (idx + 1);
                }
                arr[index] = product;
            }
        };

        private final Class<?> baseType;
        private final Class<?>[] componentTypes = new Class<?>[17];

        ArrayDataType(Class<?> baseType) {
            this.baseType = baseType;
            initComponentTypes();
        }

        public abstract Object createArray(int length);

        public Class<?> getComponentType(int dimsRemaining) {
            if (dimsRemaining < 0 || dimsRemaining > 16) {
                throw new RuntimeException("Array dimension too large");
            }
            return componentTypes[dimsRemaining];
        }

        public abstract void setElement(Object array, int index, int[] indices);

        private void initComponentTypes() {
            componentTypes[0] = baseType;
            for (int dim = 1; dim <= 16; dim++) {
                componentTypes[dim] = Array.newInstance(componentTypes[dim - 1], 0).getClass();
            }
        }
    }
}
