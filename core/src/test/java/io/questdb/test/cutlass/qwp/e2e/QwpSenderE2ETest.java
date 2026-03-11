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
import io.questdb.cairo.ColumnType;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.std.Os;
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
    public void testBoolToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_bool_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("s", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .boolColumn("s", false)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                true\t1970-01-01T00:00:01.000000Z
                                false\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testBoolToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_bool_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .boolColumn("v", false)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                true\t1970-01-01T00:00:01.000000Z
                                false\t1970-01-01T00:00:02.000000Z
                                """);
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
    public void testBooleanToByteCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_byte_error";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("DATE")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToDecimalCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_decimal_error";
                serverMain.execute("CREATE TABLE " + table + " (v DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("DECIMAL")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToDoubleCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_double_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("DOUBLE")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToFloatCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_float_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("FLOAT")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("GEOHASH")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("INT")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("LONG256")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToLongCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_long_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("LONG")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToShortCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_short_error";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("SHORT")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToSymbolCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_symbol_error";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("SYMBOL")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToTimestampCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_timestamp_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("TIMESTAMP")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToTimestampNsCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_timestamp_ns_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("TIMESTAMP")
                    );
                }
            }
        });
    }

    @Test
    public void testBooleanToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_boolean_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .boolColumn("v", true)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write BOOLEAN") && msg.contains("UUID")
                    );
                }
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
                            .shortColumn("b", (short) -1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("b", (short) 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("b", (short) 127)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
            }
        });
    }

    @Test
    public void testByteToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("b", (byte) 1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning BYTE and BOOLEAN but got: " + msg,
                            msg.contains("BYTE") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testByteToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "c CHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("c", (byte) 65)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning BYTE and CHAR but got: " + msg,
                            msg.contains("BYTE") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testByteToDate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_date";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DATE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 100)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1970-01-01T00:00:00.100Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_decimal";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(6, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_decimal128";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(38, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -1.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToDecimal16() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_decimal16";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(4, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) -9)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -9.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_decimal256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(76, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -1.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_decimal64";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(18, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -1.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToDecimal8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_decimal8";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(2, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) -9)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                5.0\t1970-01-01T00:00:01.000000Z
                                -9.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_double";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("d", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("d", (byte) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_float";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "f FLOAT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("f", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("f", (byte) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT f, ts FROM " + table + " ORDER BY ts",
                        """
                                f\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "g GEOHASH(4c), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("g", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error mentioning BYTE but got: " + msg,
                            msg.contains("type coercion from BYTE to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testByteToInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_int";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("i", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("i", Byte.MAX_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("i", Byte.MIN_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT i, ts FROM " + table + " ORDER BY ts",
                        """
                                i\tts
                                42\t1970-01-01T00:00:01.000000Z
                                127\t1970-01-01T00:00:02.000000Z
                                -128\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_long";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "l LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("l", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("l", Byte.MAX_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("l", Byte.MIN_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                42\t1970-01-01T00:00:01.000000Z
                                127\t1970-01-01T00:00:02.000000Z
                                -128\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v LONG256, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("v", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from BYTE to LONG256 is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testByteToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_short";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("s", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("s", Byte.MIN_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("s", Byte.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -128\t1970-01-01T00:00:02.000000Z
                                127\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("s", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("s", (byte) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("s", (byte) 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("s", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("s", (byte) -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("s", (byte) 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -1\t1970-01-01T00:00:02.000000Z
                                0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_timestamp";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "t TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("t", (byte) 100)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("t", (byte) 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT t, ts FROM " + table + " ORDER BY ts",
                        """
                                t\tts
                                1970-01-01T00:00:00.000100Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testByteToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "u UUID, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("u", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from BYTE to UUID is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testByteToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_byte_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .byteColumn("v", (byte) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("v", (byte) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .byteColumn("v", Byte.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                127\t1970-01-01T00:00:03.000000Z
                                """);
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
                            .charColumn("c", 'ü') // ü
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .charColumn("c", '中') // 中
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT c, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                c\ttimestamp
                                A\t1970-01-01T00:00:01.000000Z
                                ü\t1970-01-01T00:00:02.000000Z
                                中\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testCharToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToByteCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_byte_error";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("DATE")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToDoubleCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_double_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("DOUBLE")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToFloatCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_float_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("FLOAT")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("GEOHASH")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("INT")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("LONG256")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToLongCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_long_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("LONG")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToShortCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_short_error";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("SHORT")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("s", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .charColumn("s", 'Z')
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                A\t1970-01-01T00:00:01.000000Z
                                Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testCharToSymbolCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_symbol_error";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write") && msg.contains("SYMBOL")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("not supported") && msg.contains("UUID")
                    );
                }
            }
        });
    }

    @Test
    public void testCharToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_char_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .charColumn("v", 'A')
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .charColumn("v", 'Z')
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                A\t1970-01-01T00:00:01.000000Z
                                Z\t1970-01-01T00:00:02.000000Z
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
    public void testDecimal128ToDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec128_to_dec256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(76, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("d", Decimal128.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal128.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDecimal128ToDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec128_to_dec64";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(18, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("d", Decimal128.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal128.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDecimal256ToDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec256_to_dec128";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(38, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("d", Decimal256.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal256.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDecimal256ToDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec256_to_dec64";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(18, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Send DECIMAL256 wire type to DECIMAL64 column
                    sender.table(table)
                            .decimalColumn("d", Decimal256.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal256.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDecimal256ToDecimal64OverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec256_to_dec64_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(18, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Create a value that fits in Decimal256 but overflows Decimal64
                    // Decimal256 with hi bits set will overflow 64-bit storage
                    Decimal256 bigValue = Decimal256.fromBigDecimal(new java.math.BigDecimal("99999999999999999999.99"));
                    sender.table(table)
                            .decimalColumn("d", bigValue)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("decimal value overflows")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimal256ToDecimal8OverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec256_to_dec8_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(2, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // 999.9 with scale=1 → unscaled 9999, which doesn't fit in a byte (-128..127)
                    sender.table(table)
                            .decimalColumn("d", Decimal256.fromLong(9999, 1))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("decimal value overflows")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimal64ToDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec64_to_dec128";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(38, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Send DECIMAL64 wire type to DECIMAL128 column (widening)
                    sender.table(table)
                            .decimalColumn("d", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal64.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDecimal64ToDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_dec64_to_dec256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(76, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("d", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal64.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
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
                        "d DECIMAL(18, 4), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Send scale=2 wire data to scale=4 column: server should rescale
                    sender.table(table)
                            .decimalColumn("d", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("d", Decimal64.fromLong(-100, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.4500\t1970-01-01T00:00:01.000000Z
                                -1.0000\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDecimalToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToByteCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_byte_error";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("DATE")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToDoubleCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_double_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("DOUBLE")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToFloatCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_float_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("FLOAT")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("GEOHASH")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("INT")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("LONG256")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToLongCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_long_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("LONG")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToShortCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_short_error";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("SHORT")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("s", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("s", Decimal64.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDecimalToSymbolCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_symbol_error";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("SYMBOL")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToTimestampCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_timestamp_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("TIMESTAMP")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToTimestampNsCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_timestamp_ns_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("TIMESTAMP")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345L, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DECIMAL64") && msg.contains("UUID")
                    );
                }
            }
        });
    }

    @Test
    public void testDecimalToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_decimal_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(12345, 2))
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .decimalColumn("v", Decimal64.fromLong(-9999, 2))
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
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
                            .doubleColumn("d", 42.5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("d", -1.0E10)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("d", Double.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("d", Double.MIN_VALUE)
                            .at(4_000_000, ChronoUnit.MICROS);
                    // NaN and Inf should be stored as null
                    sender.table(table)
                            .doubleColumn("d", Double.NaN)
                            .at(5_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("d", Double.POSITIVE_INFINITY)
                            .at(6_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("d", Double.NEGATIVE_INFINITY)
                            .at(7_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n7\n");
                serverMain.assertSql(
                        "SELECT d, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                d\ttimestamp
                                42.5\t1970-01-01T00:00:01.000000Z
                                -1.0E10\t1970-01-01T00:00:02.000000Z
                                1.7976931348623157E308\t1970-01-01T00:00:03.000000Z
                                5.0E-324\t1970-01-01T00:00:04.000000Z
                                null\t1970-01-01T00:00:05.000000Z
                                null\t1970-01-01T00:00:06.000000Z
                                null\t1970-01-01T00:00:07.000000Z
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
    public void testDoubleArrayToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_doublearray_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleArray("v", new double[]{1.0, 2.0})
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("INT")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleArrayToStringCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_doublearray_to_string_error";
                serverMain.execute("CREATE TABLE " + table + " (v STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleArray("v", new double[]{1.0, 2.0})
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("STRING")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleArrayToSymbolCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_doublearray_to_symbol_error";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleArray("v", new double[]{1.0, 2.0})
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("SYMBOL")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleArrayToTimestampCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_doublearray_to_timestamp_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleArray("v", new double[]{1.0, 2.0})
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DOUBLE_ARRAY") && msg.contains("TIMESTAMP")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DOUBLE") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("b", 42.0)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("b", -100.0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDoubleToByteOverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_byte_ovf";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("b", 200.0)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("integer value 200 out of range for BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToBytePrecisionLossError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_byte_prec";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("b", 42.5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected precision loss error but got: " + msg,
                            msg.contains("loses precision") && msg.contains("42.5")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write DOUBLE") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_decimal";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(10, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("d", 123.45)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("d", -42.10)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -42.10\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDoubleToDecimalPrecisionLossError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_decimal_prec";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(10, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("d", 123.456)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected precision loss error but got: " + msg,
                            msg.contains("cannot be converted to") && msg.contains("123.456") && msg.contains("scale=2")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_float";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "f FLOAT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("f", 1.5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("f", -42.25)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
            }
        });
    }

    @Test
    public void testDoubleToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_int";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("i", 100_000.0)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("i", -42.0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT i, ts FROM " + table + " ORDER BY ts",
                        """
                                i\tts
                                100000\t1970-01-01T00:00:01.000000Z
                                -42\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDoubleToIntPrecisionLossError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_int_prec";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("i", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected precision loss error but got: " + msg,
                            msg.contains("loses precision") && msg.contains("3.14")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_long";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "l LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("l", 1_000_000.0)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("l", -42.0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                1000000\t1970-01-01T00:00:01.000000Z
                                -42\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDoubleToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_short";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 100.0)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("v", -200.0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                100\t1970-01-01T00:00:01.000000Z
                                -200\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDoubleToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("s", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("s", -42.0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                3.14\t1970-01-01T00:00:01.000000Z
                                -42.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDoubleToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "sym SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("sym", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT sym, ts FROM " + table + " ORDER BY ts",
                        """
                                sym\tts
                                3.14\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testDoubleToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from DOUBLE to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testDoubleToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_double_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .doubleColumn("v", 3.14)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .doubleColumn("v", -42.0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                3.14\t1970-01-01T00:00:01.000000Z
                                -42.0\t1970-01-01T00:00:02.000000Z
                                """);
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
                            .floatColumn("f", -42.25f)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("f", 0.0f)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
            }
        });
    }

    @Test
    public void testFloatToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write FLOAT") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 7.0f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("v", -100.0f)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                7\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write FLOAT") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_decimal";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(10, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("d", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("d", -42.25f)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1.50\t1970-01-01T00:00:01.000000Z
                                -42.25\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToDecimalPrecisionLossError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_decimal_prec";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(10, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("d", 1.25f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected precision loss error but got: " + msg,
                            msg.contains("cannot be converted to") && msg.contains("scale=1")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_double";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("d", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("d", -42.25f)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1.5\t1970-01-01T00:00:01.000000Z
                                -42.25\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_int";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("i", 42.0f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("i", -100.0f)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT i, ts FROM " + table + " ORDER BY ts",
                        """
                                i\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToIntPrecisionLossError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_int_prec";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("i", 3.14f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected precision loss error but got: " + msg,
                            msg.contains("loses precision")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_long";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "l LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("l", 1000.0f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                1000\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_short";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 42.0f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .floatColumn("v", -1000.0f)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -1000\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("s", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                1.5\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "sym SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("sym", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT sym, ts FROM " + table + " ORDER BY ts",
                        """
                                sym\tts
                                1.5\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testFloatToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from FLOAT to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testFloatToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_float_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .floatColumn("v", 1.5f)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                1.5\t1970-01-01T00:00:01.000000Z
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
                    // Integer.MIN_VALUE is the null sentinel for INT
                    sender.table(table)
                            .intColumn("i", Integer.MIN_VALUE)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("i", 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("i", Integer.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("i", -42)
                            .at(4_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n4\n");
                serverMain.assertSql(
                        "SELECT i, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                i\ttimestamp
                                null\t1970-01-01T00:00:01.000000Z
                                0\t1970-01-01T00:00:02.000000Z
                                2147483647\t1970-01-01T00:00:03.000000Z
                                -42\t1970-01-01T00:00:04.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("b", 1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning INT and BOOLEAN but got: " + msg,
                            msg.contains("INT") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testIntToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("b", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("b", -128)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("b", 127)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -128\t1970-01-01T00:00:02.000000Z
                                127\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToByteOverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_byte_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("b", 128)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("integer value 128 out of range for BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testIntToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "c CHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("c", 65)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning INT and CHAR but got: " + msg,
                            msg.contains("INT") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testIntToDate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_date";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DATE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // 86_400_000 millis = 1 day
                    sender.table(table)
                            .intColumn("d", 86_400_000)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1970-01-02T00:00:00.000Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_decimal";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(6, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_decimal128";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(38, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                0.00\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToDecimal16() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_decimal16";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(4, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                0.0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_decimal256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(76, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                0.00\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_decimal64";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(18, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("d", Integer.MAX_VALUE)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                2147483647.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                0.00\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToDecimal8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_decimal8";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(2, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("d", 5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", -9)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                5.0\t1970-01-01T00:00:01.000000Z
                                -9.0\t1970-01-01T00:00:02.000000Z
                                0.0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_double";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_float";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "f FLOAT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("f", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("f", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("f", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT f, ts FROM " + table + " ORDER BY ts",
                        """
                                f\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                0.0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "g GEOHASH(4c), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("g", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error mentioning INT but got: " + msg,
                            msg.contains("type coercion from INT to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testIntToLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_long";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "l LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("l", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("l", Integer.MAX_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("l", -1)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                42\t1970-01-01T00:00:01.000000Z
                                2147483647\t1970-01-01T00:00:02.000000Z
                                -1\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v LONG256, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("v", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from INT to LONG256 is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testIntToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_short";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("s", 1000)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("s", -32768)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("s", 32767)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                1000\t1970-01-01T00:00:01.000000Z
                                -32768\t1970-01-01T00:00:02.000000Z
                                32767\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToShortOverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_short_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("s", 32768)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("integer value 32768 out of range for SHORT")
                    );
                }
            }
        });
    }

    @Test
    public void testIntToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("s", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("s", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("s", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("s", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("s", -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("s", 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -1\t1970-01-01T00:00:02.000000Z
                                0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_timestamp";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "t TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // 1_000_000 micros = 1 second
                    sender.table(table)
                            .intColumn("t", 1_000_000)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("t", 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT t, ts FROM " + table + " ORDER BY ts",
                        """
                                t\tts
                                1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testIntToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "u UUID, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("u", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from INT to UUID is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testIntToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_int_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .intColumn("v", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("v", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .intColumn("v", Integer.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                2147483647\t1970-01-01T00:00:03.000000Z
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
                    // Long.MIN_VALUE is the null sentinel for LONG
                    sender.table(table)
                            .longColumn("l", Long.MIN_VALUE)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("l", 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("l", Long.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT l, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                l\ttimestamp
                                null\t1970-01-01T00:00:01.000000Z
                                0\t1970-01-01T00:00:02.000000Z
                                9223372036854775807\t1970-01-01T00:00:03.000000Z
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
                    // All zeros
                    sender.table(table)
                            .long256Column("v", 0, 0, 0, 0)
                            .at(1_000_000, ChronoUnit.MICROS);
                    // Mixed values
                    sender.table(table)
                            .long256Column("v", 1, 2, 3, 4)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
            }
        });
    }

    @Test
    public void testLong256ToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write LONG256") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToByteCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_byte_error";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write LONG256") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToDoubleCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_double_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToFloatCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_float_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToLongCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_long_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToShortCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_short_error";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("s", 1, 2, 3, 4)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table,
                        """
                                s\tts
                                0x04000000000000000300000000000000020000000000000001\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLong256ToSymbolCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_symbol_error";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write LONG256") && msg.contains("SYMBOL")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1L, 0L, 0L, 0L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG256 to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLong256ToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long256_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .long256Column("v", 1, 2, 3, 4)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table,
                        """
                                v\tts
                                0x04000000000000000300000000000000020000000000000001\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("b", 1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning LONG and BOOLEAN but got: " + msg,
                            msg.contains("LONG") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("b", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("b", -128)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("b", 127)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -128\t1970-01-01T00:00:02.000000Z
                                127\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToByteOverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_byte_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("b", 128)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("integer value 128 out of range for BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "c CHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("c", 65)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning LONG and CHAR but got: " + msg,
                            msg.contains("LONG") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToDate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_date";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DATE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", 86_400_000L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", 0L)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1970-01-02T00:00:00.000Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_decimal";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(10, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_decimal128";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(38, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", 1_000_000_000L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", -1_000_000_000L)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1000000000.00\t1970-01-01T00:00:01.000000Z
                                -1000000000.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToDecimal16() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_decimal16";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(4, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_decimal256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(76, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", Long.MAX_VALUE)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", -1_000_000_000_000L)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                9223372036854775807.00\t1970-01-01T00:00:01.000000Z
                                -1000000000000.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToDecimal32() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_decimal32";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(6, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToDecimal8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_decimal8";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(2, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", 5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", -9)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                5.0\t1970-01-01T00:00:01.000000Z
                                -9.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_double";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("d", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("d", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_float";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "f FLOAT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("f", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("f", -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT f, ts FROM " + table + " ORDER BY ts",
                        """
                                f\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "g GEOHASH(4c), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("g", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error mentioning LONG but got: " + msg,
                            msg.contains("type coercion from LONG to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_int";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Value in INT range should succeed
                    sender.table(table)
                            .longColumn("i", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("i", -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT i, ts FROM " + table + " ORDER BY ts",
                        """
                                i\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -1\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToIntOverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_int_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("i", (long) Integer.MAX_VALUE + 1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("integer value 2147483648 out of range for INT")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v LONG256, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("v", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG to LONG256 is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_short";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Value in SHORT range should succeed
                    sender.table(table)
                            .longColumn("s", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("s", -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
            }
        });
    }

    @Test
    public void testLongToShortOverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_short_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("s", 32768)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("integer value 32768 out of range for SHORT")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("s", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("s", Long.MAX_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                9223372036854775807\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("s", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("s", -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -1\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_timestamp";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "t TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("t", 1_000_000L)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("t", 0L)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT t, ts FROM " + table + " ORDER BY ts",
                        """
                                t\tts
                                1970-01-01T00:00:01.000000Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testLongToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "u UUID, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("u", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from LONG to UUID is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testLongToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_long_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .longColumn("v", 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .longColumn("v", Long.MAX_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                42\t1970-01-01T00:00:01.000000Z
                                9223372036854775807\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testMixedTimestampModes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                long ts1 = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z
                long ts3 = 1_645_747_400_000_000L; // 2022-02-25T00:03:20Z

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // row 1: explicit timestamp
                    sender.table("mixed_ts")
                            .symbol("sym", "explicit1")
                            .longColumn("val", 1)
                            .at(ts1, ChronoUnit.MICROS);
                    // row 2: server-assigned timestamp
                    sender.table("mixed_ts")
                            .symbol("sym", "server1")
                            .longColumn("val", 2)
                            .atNow();
                    // row 3: explicit timestamp again
                    sender.table("mixed_ts")
                            .symbol("sym", "explicit2")
                            .longColumn("val", 3)
                            .at(ts3, ChronoUnit.MICROS);
                    // row 4: server-assigned timestamp
                    sender.table("mixed_ts")
                            .symbol("sym", "server2")
                            .longColumn("val", 4)
                            .atNow();
                }

                serverMain.awaitTable("mixed_ts");
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts",
                        "count\n4\n"
                );
                // verify explicit timestamps are exact
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts WHERE sym = 'explicit1'",
                        """
                                sym\tval\ttimestamp
                                explicit1\t1\t2022-02-25T00:00:00.000000Z
                                """
                );
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts WHERE sym = 'explicit2'",
                        """
                                sym\tval\ttimestamp
                                explicit2\t3\t2022-02-25T00:03:20.000000Z
                                """
                );
                // verify server-assigned rows have recent timestamps (not epoch or explicit ones)
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts WHERE sym IN ('server1', 'server2') AND timestamp > '2024-01-01'",
                        "count\n2\n"
                );
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

                // Pre-create table with TIMESTAMP (micros), but sender sends nanos.
                // Server must convert nano -> micro (truncation).
                serverMain.execute("CREATE TABLE mixed_ts_micro_table (" +
                        "sym SYMBOL, " +
                        "val LONG, " +
                        "timestamp TIMESTAMP" +
                        ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

                long ts1 = 1_645_747_200_000_000_000L; // 2022-02-25T00:00:00Z in nanos
                long ts2 = 1_645_747_300_000_000_000L; // 2022-02-25T00:01:40Z in nanos
                long ts3 = 1_645_747_400_000_000_000L; // 2022-02-25T00:03:20Z in nanos
                serverMain.setNowAndFixClock(Os.currentTimeMicros(), ColumnType.TIMESTAMP_MICRO);

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // row 1: explicit nano timestamp into micro table
                    sender.table("mixed_ts_micro_table")
                            .symbol("sym", "explicit1")
                            .longColumn("val", 1)
                            .at(ts1, ChronoUnit.NANOS);
                    // row 2: explicit nano timestamp into micro table
                    sender.table("mixed_ts_micro_table")
                            .symbol("sym", "explicit2")
                            .longColumn("val", 2)
                            .at(ts2, ChronoUnit.NANOS);
                    // row 3: server-assigned timestamp
                    sender.table("mixed_ts_micro_table")
                            .symbol("sym", "server1")
                            .longColumn("val", 3)
                            .atNow();
                    // row 4: explicit nano timestamp into micro table
                    sender.table("mixed_ts_micro_table")
                            .symbol("sym", "explicit3")
                            .longColumn("val", 4)
                            .at(ts3, ChronoUnit.NANOS);
                    // row 5: server-assigned timestamp
                    sender.table("mixed_ts_micro_table")
                            .symbol("sym", "server2")
                            .longColumn("val", 5)
                            .atNow();
                }

                serverMain.awaitTable("mixed_ts_micro_table");
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts_micro_table",
                        "count\n5\n"
                );
                // verify explicit timestamps are exact (nano truncated to micro)
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts_micro_table WHERE sym = 'explicit1'",
                        """
                                sym\tval\ttimestamp
                                explicit1\t1\t2022-02-25T00:00:00.000000Z
                                """
                );
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts_micro_table WHERE sym = 'explicit3'",
                        """
                                sym\tval\ttimestamp
                                explicit3\t4\t2022-02-25T00:03:20.000000Z
                                """
                );
                // verify server-assigned rows are within [now - 1 hour, now + 1 hour]
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts_micro_table" +
                                " WHERE sym IN ('server1', 'server2')" +
                                " AND timestamp BETWEEN dateadd('h', -1, now()) AND dateadd('h', 1, now())",
                        "count\n2\n"
                );
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

                // Pre-create table with TIMESTAMP_NS designated timestamp
                serverMain.execute("CREATE TABLE mixed_ts_nano (" +
                        "sym SYMBOL, " +
                        "val LONG, " +
                        "timestamp TIMESTAMP_NS" +
                        ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

                long ts1 = 1_645_747_200_000_000_000L; // 2022-02-25T00:00:00Z in nanos
                long ts2 = 1_645_747_300_000_000_000L; // 2022-02-25T00:01:40Z in nanos
                long ts3 = 1_645_747_400_000_000_000L; // 2022-02-25T00:03:20Z in nanos

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // Gorilla enabled by default. Need >= 3 explicit timestamps
                    // so Gorilla encoding kicks in and the conversion path is used
                    // in putTimestampColumnWithConversion. That path incorrectly
                    // multiplies the server timestamp (already in nanos) by 1000.

                    // row 1: explicit nano timestamp
                    sender.table("mixed_ts_nano")
                            .symbol("sym", "explicit1")
                            .longColumn("val", 1)
                            .at(ts1, ChronoUnit.NANOS);
                    // row 2: explicit nano timestamp
                    sender.table("mixed_ts_nano")
                            .symbol("sym", "explicit2")
                            .longColumn("val", 2)
                            .at(ts2, ChronoUnit.NANOS);
                    // row 3: server-assigned timestamp
                    sender.table("mixed_ts_nano")
                            .symbol("sym", "server1")
                            .longColumn("val", 4)
                            .atNow();
                    // row 4: explicit nano timestamp
                    sender.table("mixed_ts_nano")
                            .symbol("sym", "explicit3")
                            .longColumn("val", 3)
                            .at(ts3, ChronoUnit.NANOS);
                    // row 5: server-assigned timestamp
                    sender.table("mixed_ts_nano")
                            .symbol("sym", "server2")
                            .longColumn("val", 5)
                            .atNow();
                }

                serverMain.awaitTable("mixed_ts_nano");
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts_nano",
                        "count\n5\n"
                );
                // verify explicit timestamps are exact
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts_nano WHERE sym = 'explicit1'",
                        """
                                sym\tval\ttimestamp
                                explicit1\t1\t2022-02-25T00:00:00.000000000Z
                                """
                );
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts_nano WHERE sym = 'explicit3'",
                        """
                                sym\tval\ttimestamp
                                explicit3\t3\t2022-02-25T00:03:20.000000000Z
                                """
                );
                // verify server-assigned rows have recent timestamps (not epoch or explicit ones)
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts_nano WHERE sym IN ('server1', 'server2') AND timestamp > '2024-01-01'",
                        "count\n2\n"
                );
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

                // Pre-create table with TIMESTAMP_NS, but sender sends micros.
                // Server must convert micro -> nano (multiplication).
                serverMain.execute("CREATE TABLE mixed_ts_nano_table (" +
                        "sym SYMBOL, " +
                        "val LONG, " +
                        "timestamp TIMESTAMP_NS" +
                        ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

                long ts1 = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z in micros
                long ts2 = 1_645_747_300_000_000L; // 2022-02-25T00:01:40Z in micros
                long ts3 = 1_645_747_400_000_000L; // 2022-02-25T00:03:20Z in micros
                serverMain.setNowAndFixClock(Os.currentTimeMicros(), ColumnType.TIMESTAMP_MICRO);

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // row 1: explicit micro timestamp into nano table
                    sender.table("mixed_ts_nano_table")
                            .symbol("sym", "explicit1")
                            .longColumn("val", 1)
                            .at(ts1, ChronoUnit.MICROS);
                    // row 2: explicit micro timestamp into nano table
                    sender.table("mixed_ts_nano_table")
                            .symbol("sym", "explicit2")
                            .longColumn("val", 2)
                            .at(ts2, ChronoUnit.MICROS);
                    // row 3: server-assigned timestamp
                    sender.table("mixed_ts_nano_table")
                            .symbol("sym", "server1")
                            .longColumn("val", 3)
                            .atNow();
                    // row 4: explicit micro timestamp into nano table
                    sender.table("mixed_ts_nano_table")
                            .symbol("sym", "explicit3")
                            .longColumn("val", 4)
                            .at(ts3, ChronoUnit.MICROS);
                    // row 5: server-assigned timestamp
                    sender.table("mixed_ts_nano_table")
                            .symbol("sym", "server2")
                            .longColumn("val", 5)
                            .atNow();
                }

                serverMain.awaitTable("mixed_ts_nano_table");
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts_nano_table",
                        "count\n5\n"
                );
                // verify explicit timestamps are exact (micro upscaled to nano)
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts_nano_table WHERE sym = 'explicit1'",
                        """
                                sym\tval\ttimestamp
                                explicit1\t1\t2022-02-25T00:00:00.000000000Z
                                """
                );
                serverMain.assertSql(
                        "SELECT sym, val, timestamp FROM mixed_ts_nano_table WHERE sym = 'explicit3'",
                        """
                                sym\tval\ttimestamp
                                explicit3\t4\t2022-02-25T00:03:20.000000000Z
                                """
                );
                // verify server-assigned rows are within [now - 1 hour, now + 1 hour]
                serverMain.assertSql(
                        "SELECT count() FROM mixed_ts_nano_table" +
                                " WHERE sym IN ('server1', 'server2')" +
                                " AND timestamp BETWEEN dateadd('h', -1, now()) AND dateadd('h', 1, now())",
                        "count\n2\n"
                );
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
    public void testNullStringToBoolean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_boolean";
                serverMain.execute("CREATE TABLE " + table + " (b BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("b", "true")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                true\t1970-01-01T00:00:01.000000Z
                                false\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (b BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("b", "42")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                42\t1970-01-01T00:00:01.000000Z
                                0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToChar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_char";
                serverMain.execute("CREATE TABLE " + table + " (c CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("c", "A")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("c", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT c, ts FROM " + table + " ORDER BY ts",
                        """
                                c\tts
                                A\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToDate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_date";
                serverMain.execute("CREATE TABLE " + table + " (d DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "2022-02-25T00:00:00.000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                2022-02-25T00:00:00.000Z\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToDecimal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_decimal";
                serverMain.execute("CREATE TABLE " + table + " (d DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_float";
                serverMain.execute("CREATE TABLE " + table + " (f FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("f", "3.14")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("f", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT f, ts FROM " + table + " ORDER BY ts",
                        """
                                f\tts
                                3.14\t1970-01-01T00:00:01.000000Z
                                null\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_geohash";
                serverMain.execute("CREATE TABLE " + table + " (g GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("g", "s09wh")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("g", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT g, ts FROM " + table + " ORDER BY ts",
                        """
                                g\tts
                                s09wh\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_long256";
                serverMain.execute("CREATE TABLE " + table + " (l LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("l", "0x01")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("l", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                0x01\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToNumeric() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_numeric";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "l LONG, " +
                        "d DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("i", "42")
                            .stringColumn("l", "100")
                            .stringColumn("d", "3.14")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("i", null)
                            .stringColumn("l", null)
                            .stringColumn("d", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT i, l, d, ts FROM " + table + " ORDER BY ts",
                        """
                                i\tl\td\tts
                                42\t100\t3.14\t1970-01-01T00:00:01.000000Z
                                null\tnull\tnull\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_short";
                serverMain.execute("CREATE TABLE " + table + " (s SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("s", "42")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("s", "alpha")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                alpha\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_timestamp";
                serverMain.execute("CREATE TABLE " + table + " (t TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("t", "2022-02-25T00:00:00.000000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("t", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT t, ts FROM " + table + " ORDER BY ts",
                        """
                                t\tts
                                2022-02-25T00:00:00.000000Z\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToTimestampNs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_timestamp_ns";
                serverMain.execute("CREATE TABLE " + table + " (t TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("t", "2022-02-25T00:00:00.000000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("t", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT t, ts FROM " + table + " ORDER BY ts",
                        """
                                t\tts
                                2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToUuid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_uuid";
                serverMain.execute("CREATE TABLE " + table + " (u UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("u", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT u, ts FROM " + table + " ORDER BY ts",
                        """
                                u\tts
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullStringToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_string_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("v", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                hello\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullSymbolToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_symbol_to_string";
                serverMain.execute("CREATE TABLE " + table + " (s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("s", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .symbol("s", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                hello\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullSymbolToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_symbol_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("s", "alpha")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .symbol("s", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                alpha\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testNullSymbolToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_null_symbol_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .symbol("v", null)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                hello\t1970-01-01T00:00:01.000000Z
                                \t1970-01-01T00:00:02.000000Z
                                """);
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
    public void testOmittedBoolColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_bool (" +
                        "col BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_bool")
                            .boolColumn("col", true)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_bool")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_bool")
                            .boolColumn("col", false)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_bool")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_bool")
                            .boolColumn("col", true)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_bool");
                serverMain.assertSql(
                        "SELECT col FROM omit_bool ORDER BY ts",
                        "col\ntrue\nfalse\nfalse\nfalse\ntrue\n"
                );
            }
        });
    }

    @Test
    public void testOmittedByteColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_byte (" +
                        "col BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_byte")
                            .byteColumn("col", (byte) 1)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_byte")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_byte")
                            .byteColumn("col", (byte) -1)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_byte")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_byte")
                            .byteColumn("col", (byte) 127)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_byte");
                serverMain.assertSql(
                        "SELECT col FROM omit_byte ORDER BY ts",
                        "col\n1\n0\n-1\n0\n127\n"
                );
            }
        });
    }

    @Test
    public void testOmittedCharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_char (" +
                        "col CHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_char")
                            .charColumn("col", 'A')
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_char")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_char")
                            .charColumn("col", 'Z')
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_char")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_char")
                            .charColumn("col", 'a')
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_char");
                serverMain.assertSql(
                        "SELECT col FROM omit_char ORDER BY ts",
                        "col\nA\n\nZ\n\na\n"
                );
            }
        });
    }

    @Test
    public void testOmittedDoubleColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_double (" +
                        "col DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_double")
                            .doubleColumn("col", 3.14)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_double")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_double")
                            .doubleColumn("col", -2.5)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_double")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_double")
                            .doubleColumn("col", 100.0)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_double");
                serverMain.assertSql(
                        "SELECT col FROM omit_double ORDER BY ts",
                        "col\n3.14\nnull\n-2.5\nnull\n100.0\n"
                );
            }
        });
    }

    @Test
    public void testOmittedFloatColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_float (" +
                        "col FLOAT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_float")
                            .floatColumn("col", 1.5f)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_float")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_float")
                            .floatColumn("col", -2.5f)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_float")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_float")
                            .floatColumn("col", 0.5f)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_float");
                serverMain.assertSql(
                        "SELECT count() FROM omit_float WHERE col IS NULL",
                        "count\n2\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM omit_float WHERE col IS NOT NULL",
                        "count\n3\n"
                );
            }
        });
    }

    @Test
    public void testOmittedIntColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_int (" +
                        "col INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_int")
                            .intColumn("col", 42)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_int")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_int")
                            .intColumn("col", -7)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_int")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_int")
                            .intColumn("col", 0)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_int");
                serverMain.assertSql(
                        "SELECT col FROM omit_int ORDER BY ts",
                        "col\n42\nnull\n-7\nnull\n0\n"
                );
            }
        });
    }

    @Test
    public void testOmittedLong256Column() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_long256 (" +
                        "col LONG256, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_long256")
                            .long256Column("col", 1L, 2L, 3L, 4L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_long256")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_long256")
                            .long256Column("col", 5L, 6L, 7L, 8L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_long256")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_long256")
                            .long256Column("col", 9L, 10L, 11L, 12L)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_long256");
                serverMain.assertSql(
                        "SELECT count() FROM omit_long256 WHERE col IS NULL",
                        "count\n2\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM omit_long256 WHERE col IS NOT NULL",
                        "count\n3\n"
                );
            }
        });
    }

    @Test
    public void testOmittedLongColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_long (" +
                        "col LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_long")
                            .longColumn("col", 12345L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_long")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_long")
                            .longColumn("col", -999L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_long")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_long")
                            .longColumn("col", 0L)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_long");
                serverMain.assertSql(
                        "SELECT col FROM omit_long ORDER BY ts",
                        "col\n12345\nnull\n-999\nnull\n0\n"
                );
            }
        });
    }

    @Test
    public void testOmittedShortColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_short (" +
                        "col SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_short")
                            .shortColumn("col", (short) 100)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_short")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_short")
                            .shortColumn("col", (short) -200)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_short")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_short")
                            .shortColumn("col", (short) 50)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_short");
                serverMain.assertSql(
                        "SELECT col FROM omit_short ORDER BY ts",
                        "col\n100\n0\n-200\n0\n50\n"
                );
            }
        });
    }

    @Test
    public void testOmittedStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_string (" +
                        "col STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_string")
                            .stringColumn("col", "hello")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_string")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_string")
                            .stringColumn("col", "world")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_string")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_string")
                            .stringColumn("col", "test")
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_string");
                serverMain.assertSql(
                        "SELECT col FROM omit_string ORDER BY ts",
                        "col\nhello\n\nworld\n\ntest\n"
                );
            }
        });
    }

    @Test
    public void testOmittedSymbolColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_symbol (" +
                        "col SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_symbol")
                            .symbol("col", "alpha")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_symbol")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_symbol")
                            .symbol("col", "beta")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_symbol")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_symbol")
                            .symbol("col", "alpha")
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_symbol");
                serverMain.assertSql(
                        "SELECT col FROM omit_symbol ORDER BY ts",
                        "col\nalpha\n\nbeta\n\nalpha\n"
                );
                serverMain.assertSql(
                        "SELECT count_distinct(col) FROM omit_symbol",
                        "count_distinct\n2\n"
                );
            }
        });
    }

    @Test
    public void testOmittedTimestampColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_timestamp (" +
                        "col TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // 2024-01-01T00:00:00Z in micros
                    sender.table("omit_timestamp")
                            .timestampColumn("col", 1_704_067_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_timestamp")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    // 2024-06-15T11:30:00Z in micros
                    sender.table("omit_timestamp")
                            .timestampColumn("col", 1_718_451_000_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_timestamp")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    // 2023-01-01T00:00:00Z in micros
                    sender.table("omit_timestamp")
                            .timestampColumn("col", 1_672_531_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_timestamp");
                serverMain.assertSql(
                        "SELECT count() FROM omit_timestamp WHERE col IS NULL",
                        "count\n2\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM omit_timestamp WHERE col IS NOT NULL",
                        "count\n3\n"
                );
            }
        });
    }

    @Test
    public void testOmittedUuidColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_uuid (" +
                        "col UUID, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // UUID: a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
                    sender.table("omit_uuid")
                            .uuidColumn("col", 0xbb6d6bb9bd380a11L, 0xa0eebc999c0b4ef8L)
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_uuid")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    // UUID: 550e8400-e29b-41d4-a716-446655440000
                    sender.table("omit_uuid")
                            .uuidColumn("col", 0xa716446655440000L, 0x550e8400e29b41d4L)
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_uuid")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    // UUID: 12345678-1234-5678-1234-567812345678
                    sender.table("omit_uuid")
                            .uuidColumn("col", 0x1234567812345678L, 0x1234567812345678L)
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_uuid");
                serverMain.assertSql(
                        "SELECT count() FROM omit_uuid WHERE col IS NULL",
                        "count\n2\n"
                );
                serverMain.assertSql(
                        "SELECT count() FROM omit_uuid WHERE col IS NOT NULL",
                        "count\n3\n"
                );
            }
        });
    }

    @Test
    public void testOmittedVarcharColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                serverMain.execute("CREATE TABLE omit_varchar (" +
                        "col VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table("omit_varchar")
                            .stringColumn("col", "hello")
                            .at(1_000_000_000_000L, ChronoUnit.MICROS);

                    sender.table("omit_varchar")
                            .at(1_000_000_000_001L, ChronoUnit.MICROS);

                    sender.table("omit_varchar")
                            .stringColumn("col", "world")
                            .at(1_000_000_000_002L, ChronoUnit.MICROS);

                    sender.table("omit_varchar")
                            .at(1_000_000_000_003L, ChronoUnit.MICROS);

                    sender.table("omit_varchar")
                            .stringColumn("col", "test")
                            .at(1_000_000_000_004L, ChronoUnit.MICROS);
                }

                serverMain.awaitTable("omit_varchar");
                serverMain.assertSql(
                        "SELECT col FROM omit_varchar ORDER BY ts",
                        "col\nhello\n\nworld\n\ntest\n"
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
    public void testShortToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("b", (short) 1)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning SHORT and BOOLEAN but got: " + msg,
                            msg.contains("SHORT") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testShortToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("b", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("b", (short) -128)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("b", (short) 127)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -128\t1970-01-01T00:00:02.000000Z
                                127\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToByteOverflowError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_byte_overflow";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("b", (short) 128)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected overflow error but got: " + msg,
                            msg.contains("integer value 128 out of range for BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testShortToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "c CHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("c", (short) 65)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected error mentioning SHORT and CHAR but got: " + msg,
                            msg.contains("SHORT") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testShortToDate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_date";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DATE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    // 1000 millis = 1 second
                    sender.table(table)
                            .shortColumn("d", (short) 1000)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", (short) 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1970-01-01T00:00:01.000Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_decimal128";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(38, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("d", Short.MAX_VALUE)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", Short.MIN_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                32767.00\t1970-01-01T00:00:01.000000Z
                                -32768.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToDecimal16() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_decimal16";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(4, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("d", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_decimal256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(76, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("d", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToDecimal32() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_decimal32";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(6, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("d", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_decimal64";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(18, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("d", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.00\t1970-01-01T00:00:01.000000Z
                                -100.00\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToDecimal8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_decimal8";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(2, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("d", (short) 5)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", (short) -9)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                5.0\t1970-01-01T00:00:01.000000Z
                                -9.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_double";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("d", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("d", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_float";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "f FLOAT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("f", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("f", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT f, ts FROM " + table + " ORDER BY ts",
                        """
                                f\tts
                                42.0\t1970-01-01T00:00:01.000000Z
                                -100.0\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "g GEOHASH(4c), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("g", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error mentioning SHORT but got: " + msg,
                            msg.contains("type coercion from SHORT to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testShortToInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_int";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("i", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("i", Short.MAX_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT i, ts FROM " + table + " ORDER BY ts",
                        """
                                i\tts
                                42\t1970-01-01T00:00:01.000000Z
                                32767\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_long";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "l LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("l", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("l", Short.MAX_VALUE)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                42\t1970-01-01T00:00:01.000000Z
                                32767\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v LONG256, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("v", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from SHORT to LONG256 is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testShortToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("s", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("s", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("s", (short) 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("s", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("s", (short) -1)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("s", (short) 0)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -1\t1970-01-01T00:00:02.000000Z
                                0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_timestamp";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "t TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("t", (short) 1000)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("t", (short) 0)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT t, ts FROM " + table + " ORDER BY ts",
                        """
                                t\tts
                                1970-01-01T00:00:00.001000Z\t1970-01-01T00:00:01.000000Z
                                1970-01-01T00:00:00.000000Z\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testShortToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "u UUID, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("u", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from SHORT to UUID is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testShortToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_short_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .shortColumn("v", (short) 42)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("v", (short) -100)
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .shortColumn("v", Short.MAX_VALUE)
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                32767\t1970-01-01T00:00:03.000000Z
                                """);
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
                            .stringColumn("s", "hello world")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", "non-ascii äöü")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", "")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", null)
                            .at(4_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n4\n");
                serverMain.assertSql(
                        "SELECT s, timestamp FROM " + table + " ORDER BY timestamp",
                        """
                                s\ttimestamp
                                hello world\t1970-01-01T00:00:01.000000Z
                                non-ascii äöü\t1970-01-01T00:00:02.000000Z
                                \t1970-01-01T00:00:03.000000Z
                                \t1970-01-01T00:00:04.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToBoolean() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_boolean";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("b", "true")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", "false")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", "1")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", "0")
                            .at(4_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", "TRUE")
                            .at(5_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n5\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                true\t1970-01-01T00:00:01.000000Z
                                false\t1970-01-01T00:00:02.000000Z
                                true\t1970-01-01T00:00:03.000000Z
                                false\t1970-01-01T00:00:04.000000Z
                                true\t1970-01-01T00:00:05.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToBooleanParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_boolean_err";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BOOLEAN, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("b", "yes")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse boolean from string")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToByte() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_byte";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("b", "42")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", "-128")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("b", "127")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT b, ts FROM " + table + " ORDER BY ts",
                        """
                                b\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -128\t1970-01-01T00:00:02.000000Z
                                127\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToByteParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_byte_err";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "b BYTE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("b", "abc")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse BYTE from string")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToChar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_char";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "c CHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("c", "A")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("c", "Hello")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT c, ts FROM " + table + " ORDER BY ts",
                        """
                                c\tts
                                A\t1970-01-01T00:00:01.000000Z
                                H\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDate() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_date";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DATE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "2022-02-25T00:00:00.000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                2022-02-25T00:00:00.000Z\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDateParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_date_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_date")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse DATE from string") && msg.contains("not_a_date")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_dec128";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(38, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", "-99.99")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDecimal16() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_dec16";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(4, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "12.5")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", "-99.9")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                12.5\t1970-01-01T00:00:01.000000Z
                                -99.9\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_dec256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(76, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", "-99.99")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDecimal32() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_dec32";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(6, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "1234.56")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", "-999.99")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1234.56\t1970-01-01T00:00:01.000000Z
                                -999.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_dec64";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(18, 2), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "123.45")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", "-99.99")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                123.45\t1970-01-01T00:00:01.000000Z
                                -99.99\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDecimal8() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_dec8";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DECIMAL(2, 1), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "1.5")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", "-9.9")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                1.5\t1970-01-01T00:00:01.000000Z
                                -9.9\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDouble() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_double";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "d DOUBLE, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("d", "3.14")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("d", "-2.718")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT d, ts FROM " + table + " ORDER BY ts",
                        """
                                d\tts
                                3.14\t1970-01-01T00:00:01.000000Z
                                -2.718\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToDoubleParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_double_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_number")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse DOUBLE from string") && msg.contains("not_a_number")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_float";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "f FLOAT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("f", "3.14")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("f", "-2.5")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT f, ts FROM " + table + " ORDER BY ts",
                        """
                                f\tts
                                3.14\t1970-01-01T00:00:01.000000Z
                                -2.5\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToFloatParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_float_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_number")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse FLOAT from string") && msg.contains("not_a_number")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToGeoHash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_geohash";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "g GEOHASH(5c), " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("g", "s24se")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("g", "u33dc")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT g, ts FROM " + table + " ORDER BY ts",
                        """
                                g\tts
                                s24se\t1970-01-01T00:00:01.000000Z
                                u33dc\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToGeoHashParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_geohash_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "!!!")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse geohash from string") && msg.contains("!!!")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToInt() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_int";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "i INT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("i", "42")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("i", "-100")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("i", "0")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT i, ts FROM " + table + " ORDER BY ts",
                        """
                                i\tts
                                42\t1970-01-01T00:00:01.000000Z
                                -100\t1970-01-01T00:00:02.000000Z
                                0\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToIntParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_int_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_number")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse INT from string") && msg.contains("not_a_number")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_long";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "l LONG, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("l", "1000000000000")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("l", "-1")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                1000000000000\t1970-01-01T00:00:01.000000Z
                                -1\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToLong256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_long256";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "l LONG256, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("l", "0x01")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT l, ts FROM " + table + " ORDER BY ts",
                        """
                                l\tts
                                0x01\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToLong256ParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_long256_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_long256")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse long256 from string") && msg.contains("not_a_long256")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToLongParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_long_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_number")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse LONG from string") && msg.contains("not_a_number")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_short";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("s", "1000")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", "-32768")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", "32767")
                            .at(3_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n3\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                1000\t1970-01-01T00:00:01.000000Z
                                -32768\t1970-01-01T00:00:02.000000Z
                                32767\t1970-01-01T00:00:03.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToShortParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_short_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_number")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse SHORT from string") && msg.contains("not_a_number")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_symbol";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SYMBOL, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("s", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("s", "world")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                hello\t1970-01-01T00:00:01.000000Z
                                world\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_timestamp";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "t TIMESTAMP, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("t", "2022-02-25T00:00:00.000000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT t, ts FROM " + table + " ORDER BY ts",
                        """
                                t\tts
                                2022-02-25T00:00:00.000000Z\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToTimestampNs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_timestamp_ns";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "ts_col TIMESTAMP_NS, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("ts_col", "2022-02-25T00:00:00.000000Z")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT ts_col, ts FROM " + table,
                        """
                                ts_col\tts
                                2022-02-25T00:00:00.000000000Z\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToTimestampParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_timestamp_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not_a_timestamp")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse timestamp from string") && msg.contains("not_a_timestamp")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToUuid() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_uuid";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "u UUID, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT u, ts FROM " + table + " ORDER BY ts",
                        """
                                u\tts
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testStringToUuidParseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_uuid_parse_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "not-a-uuid")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected parse error but got: " + msg,
                            msg.contains("cannot parse UUID from string") && msg.contains("not-a-uuid")
                    );
                }
            }
        });
    }

    @Test
    public void testStringToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_string_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (v VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .stringColumn("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .stringColumn("v", "world")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }
                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                hello\t1970-01-01T00:00:01.000000Z
                                world\t1970-01-01T00:00:02.000000Z
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
    public void testSymbolToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToByteCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_byte_error";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("DATE")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToDecimalCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_decimal_error";
                serverMain.execute("CREATE TABLE " + table + " (v DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("DECIMAL")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToDoubleCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_double_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("DOUBLE")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToFloatCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_float_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("FLOAT")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("GEOHASH")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("INT")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("LONG256")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToLongCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_long_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("LONG")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToShortCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_short_error";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("SHORT")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("s", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .symbol("s", "world")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                hello\t1970-01-01T00:00:01.000000Z
                                world\t1970-01-01T00:00:02.000000Z
                                """);
            }
        });
    }

    @Test
    public void testSymbolToTimestampCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_timestamp_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("TIMESTAMP")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToTimestampNsCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_timestamp_ns_error";
                serverMain.execute("CREATE TABLE " + table + " (v TIMESTAMP_NS, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("TIMESTAMP")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write SYMBOL") && msg.contains("UUID")
                    );
                }
            }
        });
    }

    @Test
    public void testSymbolToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_symbol_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .symbol("v", "hello")
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.table(table)
                            .symbol("v", "world")
                            .at(2_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n2\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                hello\t1970-01-01T00:00:01.000000Z
                                world\t1970-01-01T00:00:02.000000Z
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
                serverMain.assertSql(
                        "SELECT ts_col, timestamp FROM " + table,
                        """
                                ts_col\ttimestamp
                                2022-02-25T00:00:00.000000Z\t1970-01-01T00:00:01.000000Z
                                """);
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
    public void testTimestampToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToByteCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_byte_error";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("BYTE")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("DATE")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToDecimalCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_decimal_error";
                serverMain.execute("CREATE TABLE " + table + " (v DECIMAL(18,2), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("DECIMAL")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToDoubleCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_double_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("DOUBLE")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToFloatCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_float_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("FLOAT")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("GEOHASH")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("INT")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("LONG256")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToLongCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_long_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("LONG")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToShortCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_short_error";
                serverMain.execute("CREATE TABLE " + table + " (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("SHORT")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z in micros
                    sender.table(table)
                            .timestampColumn("s", tsMicros, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table + " ORDER BY ts",
                        """
                                s\tts
                                2022-02-25T00:00:00.000Z\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testTimestampToSymbolCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_symbol_error";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("SYMBOL")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToUuidCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_uuid_error";
                serverMain.execute("CREATE TABLE " + table + " (v UUID, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .timestampColumn("v", 1_645_747_200_000_000L, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write TIMESTAMP") && msg.contains("UUID")
                    );
                }
            }
        });
    }

    @Test
    public void testTimestampToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_timestamp_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    long tsMicros = 1_645_747_200_000_000L; // 2022-02-25T00:00:00Z in micros
                    sender.table(table)
                            .timestampColumn("v", tsMicros, ChronoUnit.MICROS)
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table + " ORDER BY ts",
                        """
                                v\tts
                                2022-02-25T00:00:00.000Z\t1970-01-01T00:00:01.000000Z
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
    public void testUuidToBooleanCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_boolean_error";
                serverMain.execute("CREATE TABLE " + table + " (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write UUID") && msg.contains("BOOLEAN")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToByteCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_byte_error";
                serverMain.execute("CREATE TABLE " + table + " (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToCharCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_char_error";
                serverMain.execute("CREATE TABLE " + table + " (v CHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write UUID") && msg.contains("CHAR")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToDateCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_date_error";
                serverMain.execute("CREATE TABLE " + table + " (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToDoubleCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_double_error";
                serverMain.execute("CREATE TABLE " + table + " (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToFloatCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_float_error";
                serverMain.execute("CREATE TABLE " + table + " (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToGeoHashCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_geohash_error";
                serverMain.execute("CREATE TABLE " + table + " (v GEOHASH(5c), ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToIntCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_int_error";
                serverMain.execute("CREATE TABLE " + table + " (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToLong256CoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_long256_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG256, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToLongCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_long_error";
                serverMain.execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to") && msg.contains("is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToShortCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_short_error";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s SHORT, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("s", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("type coercion from UUID to SHORT is not supported")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_string";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "s STRING, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .uuidColumn("s", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT s, ts FROM " + table,
                        """
                                s\tts
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000Z
                                """);
            }
        });
    }

    @Test
    public void testUuidToSymbolCoercionError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_symbol_error";
                serverMain.execute("CREATE TABLE " + table + " (v SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    String msg = e.getMessage();
                    Assert.assertTrue(
                            "Expected coercion error but got: " + msg,
                            msg.contains("cannot write UUID") && msg.contains("SYMBOL")
                    );
                }
            }
        });
    }

    @Test
    public void testUuidToVarchar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                String table = "test_qwp_uuid_to_varchar";
                serverMain.execute("CREATE TABLE " + table + " (" +
                        "v VARCHAR, " +
                        "ts TIMESTAMP" +
                        ") TIMESTAMP(ts) PARTITION BY DAY WAL");

                UUID uuid = UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", httpPort)) {
                    sender.table(table)
                            .uuidColumn("v", uuid.getLeastSignificantBits(), uuid.getMostSignificantBits())
                            .at(1_000_000, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable(table);
                serverMain.assertSql("SELECT count() FROM " + table, "count\n1\n");
                serverMain.assertSql(
                        "SELECT v, ts FROM " + table,
                        """
                                v\tts
                                a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\t1970-01-01T00:00:01.000000Z
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
