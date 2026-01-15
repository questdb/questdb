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
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.websocket.IlpV4WebSocketSender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end integration tests for ILP v4 WebSocket sender and receiver.
 * <p>
 * These tests mirror the HTTP sender/receiver tests to ensure feature parity
 * between HTTP and WebSocket transports.
 * <p>
 * Tests verify that data sent via IlpV4WebSocketSender over WebSocket is correctly
 * written to QuestDB tables and can be queried.
 * <p>
 * Tests are parametrized to run in both sync and async modes.
 */
@RunWith(Parameterized.class)
public class IlpV4WebSocketSenderReceiverTest extends AbstractBootstrapTest {

    @Parameterized.Parameters(name = "asyncMode={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false},  // sync mode
                {true}    // async mode
        });
    }

    private final boolean asyncMode;

    public IlpV4WebSocketSenderReceiverTest(boolean asyncMode) {
        this.asyncMode = asyncMode;
    }

    /**
     * Creates a sender with the appropriate mode (sync or async).
     */
    private IlpV4WebSocketSender createSender(int port) {
        if (asyncMode) {
            return IlpV4WebSocketSender.connectAsync("localhost", port, false);
        } else {
            return IlpV4WebSocketSender.connect("localhost", port);
        }
    }

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    // ==================== Basic Data Type Tests ====================

    @Test
    public void testAllDataTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_all_types")
                            .boolColumn("bool_col", true)
                            .longColumn("long_col", 9999999999L)
                            .longColumn("int_col", 123456L)
                            .doubleColumn("double_col", 3.14159265359)
                            .doubleColumn("float_col", 2.71828)
                            .stringColumn("string_col", "hello world")
                            .symbol("symbol_col", "sym_value")
                            .timestampColumn("ts_col", 1609459200000000L, ChronoUnit.MICROS)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_all_types");
                serverMain.assertSql("select count() from ws_all_types", "count\n1\n");
            }
        });
    }

    @Test
    public void testAllNumericTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("ws_all_numeric")
                                .longColumn("byte_col", (long) (i % 128))
                                .longColumn("short_col", (long) (i * 100))
                                .longColumn("int_col", (long) (i * 10000))
                                .longColumn("long_col", (long) i * 100000000L)
                                .doubleColumn("float_col", i * 1.1)
                                .doubleColumn("double_col", i * 1.111111)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_all_numeric");
                serverMain.assertSql("select count() from ws_all_numeric", "count\n100\n");
            }
        });
    }

    // ==================== Timestamp Tests ====================

    @Test
    public void testAtNowServerAssignedTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = createSender(httpPort)) {
                    sender.table("ws_test_at_now")
                            .symbol("tag", "row1")
                            .longColumn("value", 100)
                            .atNow();
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_at_now");
                serverMain.assertSql("select count() from ws_test_at_now", "count\n1\n");

                // Verify a timestamp column was auto-created
                serverMain.assertSql(
                        "select \"column\" from table_columns('ws_test_at_now') order by \"column\"",
                        "column\ntag\ntimestamp\nvalue\n"
                );

                // Verify the timestamp was assigned by the server (should be recent)
                serverMain.assertSql(
                        "select count() from ws_test_at_now where timestamp >= '2025-01-01'",
                        "count\n1\n"
                );
            }
        });
    }

    @Ignore("WebSocket transport doesn't support pre-created tables with custom timestamp columns yet - needs feature parity with HTTP")
    @Test
    public void testAtNowWithCustomTimestampColumnName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create table with custom designated timestamp column named 'ts'
                serverMain.execute("CREATE TABLE ws_custom_ts_table (sym SYMBOL, value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Ingest data using atNow()
                try (Sender sender = createSender(httpPort)) {
                    sender.table("ws_custom_ts_table")
                            .symbol("sym", "test")
                            .longColumn("value", 42)
                            .atNow();
                    sender.flush();
                }

                serverMain.awaitTable("ws_custom_ts_table");

                // Verify row was inserted
                serverMain.assertSql("select count() from ws_custom_ts_table", "count\n1\n");

                // Verify the table has ONLY the expected columns (sym, value, ts)
                serverMain.assertSql(
                        "select \"column\" from table_columns('ws_custom_ts_table') order by \"column\"",
                        "column\nsym\nts\nvalue\n"
                );

                // Verify the timestamp was assigned by the server
                serverMain.assertSql(
                        "select count() from ws_custom_ts_table where ts >= '2025-01-01'",
                        "count\n1\n"
                );
            }
        });
    }

    @Ignore("WebSocket transport doesn't support pre-created tables with custom timestamp columns yet - needs feature parity with HTTP")
    @Test
    public void testAtWithCustomTimestampColumnName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create table with custom designated timestamp column named 'ts'
                serverMain.execute("CREATE TABLE ws_custom_ts_at_table (sym SYMBOL, value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Ingest data using at() with explicit timestamp
                long explicitTimestamp = 1700000000000000L; // 2023-11-14T22:13:20Z in micros
                try (Sender sender = createSender(httpPort)) {
                    sender.table("ws_custom_ts_at_table")
                            .symbol("sym", "test")
                            .longColumn("value", 42)
                            .at(explicitTimestamp, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_custom_ts_at_table");

                // Verify row was inserted
                serverMain.assertSql("select count() from ws_custom_ts_at_table", "count\n1\n");

                // Verify the table has ONLY the expected columns
                serverMain.assertSql(
                        "select \"column\" from table_columns('ws_custom_ts_at_table') order by \"column\"",
                        "column\nsym\nts\nvalue\n"
                );

                // Verify the explicit timestamp was correctly stored
                serverMain.assertSql(
                        "select ts from ws_custom_ts_at_table",
                        "ts\n2023-11-14T22:13:20.000000Z\n"
                );
            }
        });
    }

    // ==================== Batch Insertion Tests ====================

    @Test
    public void testBatchInsertion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = createSender(httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("ws_test_batch")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_batch");
                serverMain.assertSql("select count() from ws_test_batch", "count\n100\n");
            }
        });
    }

    @Test
    public void test10Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("ws_test_10rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_10rows");
                serverMain.assertSql("select count() from ws_test_10rows", "count\n10\n");
            }
        });
    }

    @Test
    public void test100Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("ws_test_100_rows")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_100_rows");
                serverMain.assertSql("select count() from ws_test_100_rows", "count\n100\n");
                serverMain.assertSql("select sum(value) from ws_test_100_rows", "sum\n4950\n");
            }
        });
    }

    @Test
    public void test1000Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "262144"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 1000; i++) {
                        sender.table("ws_test_1000_rows")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_1000_rows");
                serverMain.assertSql("select count() from ws_test_1000_rows", "count\n1000\n");
            }
        });
    }

    @Test
    public void test10000Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "262144"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 10000; i++) {
                        sender.table("ws_test_10000rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        // Flush every 1000 rows to avoid buffer overflow
                        if ((i + 1) % 1000 == 0) {
                            sender.flush();
                        }
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_10000rows");
                serverMain.assertSql("select count() from ws_test_10000rows", "count\n10000\n");
            }
        });
    }

    // ==================== Boolean Tests ====================

    @Test
    public void testBooleanValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = createSender(httpPort)) {
                    sender.table("ws_bool_test")
                            .boolColumn("val", true)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("ws_bool_test")
                            .boolColumn("val", false)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    // Alternating pattern
                    for (int i = 0; i < 10; i++) {
                        sender.table("ws_bool_test")
                                .boolColumn("val", i % 2 == 0)
                                .at(1000002000000L + i * 1000000L, ChronoUnit.MICROS);
                    }

                    sender.flush();
                }

                serverMain.awaitTable("ws_bool_test");
                serverMain.assertSql("select count() from ws_bool_test", "count\n12\n");
            }
        });
    }

    @Test
    public void testBooleanFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_bool_false")
                            .symbol("id", "f")
                            .boolColumn("flag", false)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_bool_false");
                serverMain.assertSql("select flag from ws_test_bool_false", "flag\nfalse\n");
            }
        });
    }

    @Test
    public void testBooleanTrue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_bool_true")
                            .symbol("id", "t")
                            .boolColumn("flag", true)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_bool_true");
                serverMain.assertSql("select flag from ws_test_bool_true", "flag\ntrue\n");
            }
        });
    }

    @Test
    public void testFiveBoolsAllTrue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_bools_true")
                            .boolColumn("a", true)
                            .boolColumn("b", true)
                            .boolColumn("c", true)
                            .boolColumn("d", true)
                            .boolColumn("e", true)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_bools_true");
                serverMain.assertSql("select a,b,c,d,e from ws_test_bools_true", "a\tb\tc\td\te\ntrue\ttrue\ttrue\ttrue\ttrue\n");
            }
        });
    }

    // ==================== Symbol Tests ====================

    @Test
    public void testFiveSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_five_syms")
                            .symbol("a", "1")
                            .symbol("b", "2")
                            .symbol("c", "3")
                            .symbol("d", "4")
                            .symbol("e", "5")
                            .longColumn("value", 42)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_five_syms");
                serverMain.assertSql("select a,b,c,d,e from ws_test_five_syms", "a\tb\tc\td\te\n1\t2\t3\t4\t5\n");
            }
        });
    }

    @Test
    public void testSymbolDeduplication() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Send many rows with same symbol values to test deduplication
                    for (int i = 0; i < 100; i++) {
                        sender.table("ws_test_sym_dedup")
                                .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_sym_dedup");
                serverMain.assertSql("select count() from ws_test_sym_dedup", "count\n100\n");
                serverMain.assertSql("select count(distinct region) from ws_test_sym_dedup", "count_distinct\n3\n");
            }
        });
    }

    // ==================== String Tests ====================

    @Test
    public void testLargeStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                StringBuilder largeString = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    largeString.append("x");
                }

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_large_string")
                            .symbol("id", "row1")
                            .stringColumn("large_data", largeString.toString())
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_large_string");
                serverMain.assertSql("select length(large_data) from ws_test_large_string", "length\n1000\n");
            }
        });
    }

    @Test
    public void testUnicodeInString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_unicode")
                            .symbol("id", "row1")
                            .stringColumn("unicode_str", "こんにちは世界")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_unicode");
                serverMain.assertSql("select count() from ws_test_unicode", "count\n1\n");
            }
        });
    }

    @Test
    public void testSpecialCharactersInString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_special_str")
                            .symbol("id", "row1")
                            .stringColumn("special", "hello\tworld\nnewline")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_special_str");
                serverMain.assertSql("select count() from ws_test_special_str", "count\n1\n");
            }
        });
    }

    @Test
    public void testSpecialCharactersInSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_special_sym")
                            .symbol("special", "hello-world_123")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_special_sym");
                serverMain.assertSql(
                        "select special from ws_test_special_sym",
                        "special\nhello-world_123\n"
                );
            }
        });
    }

    // ==================== Numeric Edge Cases ====================

    @Test
    public void testByteRangeLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_byte_range")
                            .symbol("id", "b1")
                            .longColumn("byte_val", 127)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_byte_range");
                serverMain.assertSql("select byte_val from ws_test_byte_range", "byte_val\n127\n");
            }
        });
    }

    @Test
    public void testIntRangeLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_int_range")
                            .symbol("id", "i1")
                            .longColumn("int_val", Integer.MAX_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_int_range");
                serverMain.assertSql("select int_val from ws_test_int_range", "int_val\n" + Integer.MAX_VALUE + "\n");
            }
        });
    }

    @Test
    public void testLongMinMax() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_long_min")
                            .longColumn("value", Long.MIN_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.table("ws_test_long_min")
                            .longColumn("value", Long.MAX_VALUE)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_long_min");
                serverMain.assertSql("select count() from ws_test_long_min", "count\n2\n");
            }
        });
    }

    @Test
    public void testNegativeNumbers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_negative")
                            .longColumn("long_val", -12345L)
                            .longColumn("int_val", -999L)
                            .doubleColumn("double_val", -3.14)
                            .doubleColumn("float_val", -2.71)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_negative");
                serverMain.assertSql("select count() from ws_test_negative", "count\n1\n");
            }
        });
    }

    @Test
    public void testZeroValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_zeros")
                            .longColumn("long_val", 0L)
                            .longColumn("int_val", 0L)
                            .doubleColumn("double_val", 0.0)
                            .doubleColumn("float_val", 0.0)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_zeros");
                serverMain.assertSql("select count() from ws_test_zeros", "count\n1\n");
            }
        });
    }

    @Test
    public void testDoubleSpecialValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_double_special")
                            .doubleColumn("pi", Math.PI)
                            .doubleColumn("e", Math.E)
                            .doubleColumn("sqrt2", Math.sqrt(2))
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_double_special");
                serverMain.assertSql("select count() from ws_test_double_special", "count\n1\n");
            }
        });
    }

    // ==================== Column Name Tests ====================

    @Test
    public void testColumnNameShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_col_short")
                            .longColumn("x", 42)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_col_short");
                serverMain.assertSql("select x from ws_test_col_short", "x\n42\n");
            }
        });
    }

    @Test
    public void testColumnNameWithUnderscore() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_col_underscore")
                            .longColumn("my_column", 42)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_col_underscore");
                serverMain.assertSql("select my_column from ws_test_col_underscore", "my_column\n42\n");
            }
        });
    }

    // ==================== Complex Schema Tests ====================

    @Test
    public void testComplexSchema1() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_complex1")
                            .symbol("region", "us-east")
                            .symbol("host", "server-01")
                            .symbol("dc", "dc1")
                            .longColumn("cpu", 50)
                            .longColumn("mem", 80)
                            .doubleColumn("disk_usage", 0.45)
                            .boolColumn("healthy", true)
                            .stringColumn("status", "running")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_complex1");
                serverMain.assertSql("select region,host,dc,cpu,mem from ws_complex1",
                        "region\thost\tdc\tcpu\tmem\nus-east\tserver-01\tdc1\t50\t80\n");
            }
        });
    }

    @Test
    public void testComplexSchema2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_complex2")
                            .symbol("type", "temperature")
                            .symbol("unit", "celsius")
                            .symbol("sensor_id", "t-001")
                            .doubleColumn("value", 23.5)
                            .doubleColumn("min", 20.0)
                            .doubleColumn("max", 30.0)
                            .longColumn("reading_count", 1000)
                            .boolColumn("calibrated", true)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_complex2");
                serverMain.assertSql("select type,unit,sensor_id from ws_complex2",
                        "type\tunit\tsensor_id\ntemperature\tcelsius\tt-001\n");
            }
        });
    }

    @Test
    public void testComplexSchemaMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("ws_complex_multi")
                                .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                                .symbol("host", "host-" + (i % 10))
                                .longColumn("metric1", i * 10)
                                .longColumn("metric2", i * 20)
                                .doubleColumn("ratio", i / 100.0)
                                .boolColumn("active", i % 2 == 0)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_complex_multi");
                serverMain.assertSql("select count() from ws_complex_multi", "count\n50\n");
            }
        });
    }

    // ==================== Flush Patterns ====================

    @Test
    public void testFlushAfterEveryRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("ws_test_flush_each")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        sender.flush();
                    }
                }

                serverMain.awaitTable("ws_test_flush_each");
                serverMain.assertSql("select count() from ws_test_flush_each", "count\n10\n");
            }
        });
    }

    @Test
    public void testFlushEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Flush with no data should not throw
                    sender.flush();
                    sender.flush();
                    sender.flush();
                }
                // If we get here without exception, test passes
            }
        });
    }

    @Test
    public void testFlushEvery5Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("ws_test_flush_5")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        if ((i + 1) % 5 == 0) {
                            sender.flush();
                        }
                    }
                    sender.flush(); // flush remaining
                }

                serverMain.awaitTable("ws_test_flush_5");
                serverMain.assertSql("select count() from ws_test_flush_5", "count\n50\n");
            }
        });
    }

    @Test
    public void testFlushEvery10Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("ws_test_flush_10")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        if ((i + 1) % 10 == 0) {
                            sender.flush();
                        }
                    }
                }

                serverMain.awaitTable("ws_test_flush_10");
                serverMain.assertSql("select count() from ws_test_flush_10", "count\n100\n");
            }
        });
    }

    // ==================== Interleaved Tables ====================

    @Test
    public void testInterleavedTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Interleave rows between tables
                    for (int i = 0; i < 20; i++) {
                        sender.table("ws_interleave_a")
                                .symbol("id", "a" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 2, ChronoUnit.MICROS);

                        sender.table("ws_interleave_b")
                                .symbol("id", "b" + i)
                                .longColumn("value", i * 10)
                                .at(1000000000000L + i * 2 + 1, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_interleave_a");
                serverMain.awaitTable("ws_interleave_b");
                serverMain.assertSql("select count() from ws_interleave_a", "count\n20\n");
                serverMain.assertSql("select count() from ws_interleave_b", "count\n20\n");
            }
        });
    }

    // ==================== Array Tests ====================

    @Test
    public void test1DDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_1d_double_array")
                            .doubleArray("values", new double[]{1.1, 2.2, 3.3, 4.4, 5.5})
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_1d_double_array");
                serverMain.assertSql("select count() from ws_test_1d_double_array", "count\n1\n");
            }
        });
    }

    @Test
    public void test2DDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    double[][] matrix = {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};
                    sender.table("ws_test_2d_double_array")
                            .doubleArray("matrix", matrix)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_2d_double_array");
                serverMain.assertSql("select count() from ws_test_2d_double_array", "count\n1\n");
            }
        });
    }

    @Test
    public void test3DDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    double[][][] cube = {{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}};
                    sender.table("ws_test_3d_double_array")
                            .doubleArray("cube", cube)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_3d_double_array");
                serverMain.assertSql("select count() from ws_test_3d_double_array", "count\n1\n");
            }
        });
    }

    @Test
    public void testLargeArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    double[] largeArray = new double[1000];
                    for (int i = 0; i < largeArray.length; i++) {
                        largeArray[i] = i * 0.1;
                    }
                    sender.table("ws_test_large_array")
                            .doubleArray("values", largeArray)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_large_array");
                serverMain.assertSql("select count() from ws_test_large_array", "count\n1\n");
            }
        });
    }

    // ==================== Decimal Tests ====================

    @Test
    public void testDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    io.questdb.std.Decimal64 value = new io.questdb.std.Decimal64(12345, 2); // 123.45
                    sender.table("ws_test_decimal64")
                            .decimalColumn("price", value)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal64");
                serverMain.assertSql("select count() from ws_test_decimal64", "count\n1\n");
            }
        });
    }

    @Test
    public void testDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    io.questdb.std.Decimal128 value = new io.questdb.std.Decimal128(0, 12345678901234L, 2); // 123456789012.34
                    sender.table("ws_test_decimal128")
                            .decimalColumn("amount", value)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal128");
                serverMain.assertSql("select count() from ws_test_decimal128", "count\n1\n");
            }
        });
    }

    @Test
    public void testDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    io.questdb.std.Decimal256 value = io.questdb.std.Decimal256.fromLong(123456789L, 4);
                    sender.table("ws_test_decimal256")
                            .decimalColumn("big_value", value)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal256");
                serverMain.assertSql("select count() from ws_test_decimal256", "count\n1\n");
            }
        });
    }

    @Test
    public void testDecimalFromString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_test_decimal_string")
                            .decimalColumn("price", "123.456789")
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal_string");
                serverMain.assertSql("select count() from ws_test_decimal_string", "count\n1\n");
            }
        });
    }

    @Test
    public void testDecimalMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Same scale for all values in the column
                    io.questdb.std.Decimal64 v1 = new io.questdb.std.Decimal64(100, 2); // 1.00
                    io.questdb.std.Decimal64 v2 = new io.questdb.std.Decimal64(200, 2); // 2.00
                    io.questdb.std.Decimal64 v3 = new io.questdb.std.Decimal64(300, 2); // 3.00

                    sender.table("ws_test_decimal_multi")
                            .decimalColumn("price", v1)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.table("ws_test_decimal_multi")
                            .decimalColumn("price", v2)
                            .at(2000000000L, ChronoUnit.MICROS);
                    sender.table("ws_test_decimal_multi")
                            .decimalColumn("price", v3)
                            .at(3000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal_multi");
                serverMain.assertSql("select count() from ws_test_decimal_multi", "count\n3\n");
            }
        });
    }

    @Test
    public void testDecimalNegativeValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    io.questdb.std.Decimal64 negative = new io.questdb.std.Decimal64(-5000, 2); // -50.00
                    sender.table("ws_test_decimal_negative")
                            .decimalColumn("loss", negative)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal_negative");
                serverMain.assertSql("select count() from ws_test_decimal_negative", "count\n1\n");
            }
        });
    }

    @Test
    public void testDecimalZeroValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    io.questdb.std.Decimal64 zero = new io.questdb.std.Decimal64(0, 2); // 0.00
                    sender.table("ws_test_decimal_zero")
                            .decimalColumn("balance", zero)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal_zero");
                serverMain.assertSql("select count() from ws_test_decimal_zero", "count\n1\n");
            }
        });
    }

    @Test
    public void testDecimalWithScalarColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    io.questdb.std.Decimal64 price = new io.questdb.std.Decimal64(9999, 2); // 99.99
                    sender.table("ws_test_decimal_mixed")
                            .symbol("product", "Widget")
                            .longColumn("quantity", 10)
                            .decimalColumn("price", price)
                            .doubleColumn("discount", 0.1)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal_mixed");
                serverMain.assertSql("select count() from ws_test_decimal_mixed", "count\n1\n");
            }
        });
    }

    @Test
    public void testDecimalNullSkipped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Null decimals should be skipped without error
                    sender.table("ws_test_decimal_null")
                            .symbol("name", "test")
                            .decimalColumn("value", (io.questdb.std.Decimal64) null)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_test_decimal_null");
                serverMain.assertSql("select count() from ws_test_decimal_null", "count\n1\n");
            }
        });
    }

    @Ignore("WebSocket sender doesn't validate decimal scale on client side yet - needs feature parity with HTTP")
    @Test
    public void testDecimalScaleMismatchThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // First value with scale 2
                    io.questdb.std.Decimal64 v1 = new io.questdb.std.Decimal64(100, 2);
                    sender.table("ws_test_decimal_scale_mismatch")
                            .decimalColumn("price", v1)
                            .at(1000000000L, ChronoUnit.MICROS);

                    // Second value with scale 4 - should throw
                    io.questdb.std.Decimal64 v2 = new io.questdb.std.Decimal64(10000, 4);
                    try {
                        sender.table("ws_test_decimal_scale_mismatch")
                                .decimalColumn("price", v2)
                                .at(2000000000L, ChronoUnit.MICROS);
                        Assert.fail("Expected LineSenderException for scale mismatch");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage().contains("scale mismatch"));
                    }
                }
            }
        });
    }

    // ==================== Wide Table Test ====================

    @Test
    public void testWideTable100Columns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "262144"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int row = 0; row < 10; row++) {
                        Sender rowBuilder = sender.table("ws_wide_table");
                        for (int col = 0; col < 100; col++) {
                            rowBuilder.longColumn("col" + col, row * 100 + col);
                        }
                        rowBuilder.at(1000000000000L + row, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_wide_table");
                serverMain.assertSql("select count() from ws_wide_table", "count\n10\n");
            }
        });
    }

    // ==================== ACK Handling Tests ====================

    /**
     * Tests that cumulative ACKs work correctly with high in-flight windows.
     * <p>
     * This test verifies that the cumulative ACK mechanism properly handles
     * large numbers of batches without dropping any ACKs.
     * <p>
     * Previous bug: Server silently dropped ACKs when PeerIsSlowToReadException
     * was caught. Fix: Use cumulative ACKs so later ACKs cover earlier ones.
     */
    @Test
    public void testHighInFlightWindowWithCumulativeAcks() throws Exception {
        // This test is specific to async mode where cumulative ACKs are most important
        Assume.assumeTrue(asyncMode);

        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int inFlightWindowSize = 100;
                int sendQueueCapacity = 200;
                int totalRows = 10000;

                // Create sender with high in-flight window
                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                        "localhost", httpPort, false,
                        10,                             // autoFlushRows = 10: batch every 10 rows
                        Integer.MAX_VALUE,              // autoFlushBytes: disabled
                        TimeUnit.HOURS.toNanos(1),      // autoFlushInterval: disabled
                        inFlightWindowSize,
                        sendQueueCapacity
                )) {
                    for (int i = 0; i < totalRows; i++) {
                        sender.table("ws_ack_test")
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    // This should succeed - cumulative ACKs handle the batch volume
                    sender.flush();
                }

                serverMain.awaitTable("ws_ack_test");
                serverMain.assertSql("select count() from ws_ack_test", "count\n" + totalRows + "\n");
            }
        });
    }

    // ==================== Delta Symbol Dictionary Tests ====================

    /**
     * Tests that multiple batches with the same symbols work correctly.
     * <p>
     * After the first batch, the server knows all symbols in the dictionary.
     * Subsequent batches using the same symbols should have empty deltas
     * (deltaCount=0) since no new symbols need to be sent.
     */
    @Test
    public void testDeltaSymbolDict_multipleBatches_sameSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Batch 1: introduces AAPL, GOOG
                    for (int i = 0; i < 10; i++) {
                        sender.table("ws_delta_same_syms")
                                .symbol("ticker", i % 2 == 0 ? "AAPL" : "GOOG")
                                .longColumn("price", 100 + i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();

                    // Batch 2: reuses same symbols - delta should be empty
                    for (int i = 10; i < 20; i++) {
                        sender.table("ws_delta_same_syms")
                                .symbol("ticker", i % 2 == 0 ? "AAPL" : "GOOG")
                                .longColumn("price", 100 + i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();

                    // Batch 3: still reusing same symbols
                    for (int i = 20; i < 30; i++) {
                        sender.table("ws_delta_same_syms")
                                .symbol("ticker", i % 2 == 0 ? "GOOG" : "AAPL")
                                .longColumn("price", 100 + i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_same_syms");
                serverMain.assertSql("select count() from ws_delta_same_syms", "count\n30\n");
                serverMain.assertSql("select count(distinct ticker) from ws_delta_same_syms", "count_distinct\n2\n");
            }
        });
    }

    /**
     * Tests progressive symbol accumulation across multiple batches.
     * <p>
     * Each batch introduces new symbols, so the delta grows progressively.
     * The global dictionary accumulates all symbols across batches.
     */
    @Test
    public void testDeltaSymbolDict_progressiveSymbolAccumulation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Batch 1: AAPL
                    sender.table("ws_delta_progressive")
                            .symbol("ticker", "AAPL")
                            .longColumn("price", 150)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 2: AAPL + GOOG (new)
                    sender.table("ws_delta_progressive")
                            .symbol("ticker", "AAPL")
                            .longColumn("price", 151)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.table("ws_delta_progressive")
                            .symbol("ticker", "GOOG")
                            .longColumn("price", 2800)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 3: GOOG + MSFT (new)
                    sender.table("ws_delta_progressive")
                            .symbol("ticker", "GOOG")
                            .longColumn("price", 2801)
                            .at(1000000000003L, ChronoUnit.MICROS);
                    sender.table("ws_delta_progressive")
                            .symbol("ticker", "MSFT")
                            .longColumn("price", 300)
                            .at(1000000000004L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 4: All three symbols + TSLA (new)
                    sender.table("ws_delta_progressive")
                            .symbol("ticker", "AAPL")
                            .longColumn("price", 152)
                            .at(1000000000005L, ChronoUnit.MICROS);
                    sender.table("ws_delta_progressive")
                            .symbol("ticker", "TSLA")
                            .longColumn("price", 700)
                            .at(1000000000006L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_progressive");
                serverMain.assertSql("select count() from ws_delta_progressive", "count\n7\n");
                serverMain.assertSql("select count(distinct ticker) from ws_delta_progressive", "count_distinct\n4\n");
            }
        });
    }

    /**
     * Tests that reconnection resets the symbol dictionary watermark.
     * <p>
     * After disconnecting and reconnecting, the server's connection-level
     * dictionary is cleared. The client must send the full dictionary again.
     */
    @Test
    public void testDeltaSymbolDict_reconnection_resetsWatermark() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // First connection: establish symbols
                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("ws_delta_reconnect")
                                .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
                // Connection closed here

                // Second connection: must re-send dictionary
                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    for (int i = 10; i < 20; i++) {
                        sender.table("ws_delta_reconnect")
                                .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_reconnect");
                serverMain.assertSql("select count() from ws_delta_reconnect", "count\n20\n");
                serverMain.assertSql("select count(distinct region) from ws_delta_reconnect", "count_distinct\n3\n");
            }
        });
    }

    /**
     * Tests that multiple tables share the same global symbol dictionary.
     * <p>
     * When the same symbol value (e.g., "AAPL") is used across different tables,
     * it should be stored once in the global dictionary and reused.
     */
    @Test
    public void testDeltaSymbolDict_multipleTables_sharedDictionary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Table A uses AAPL, GOOG
                    sender.table("ws_delta_table_a")
                            .symbol("ticker", "AAPL")
                            .longColumn("value", 100)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.table("ws_delta_table_a")
                            .symbol("ticker", "GOOG")
                            .longColumn("value", 200)
                            .at(1000000000001L, ChronoUnit.MICROS);

                    // Table B uses AAPL, MSFT (AAPL is shared, MSFT is new)
                    sender.table("ws_delta_table_b")
                            .symbol("ticker", "AAPL")
                            .longColumn("price", 150)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.table("ws_delta_table_b")
                            .symbol("ticker", "MSFT")
                            .longColumn("price", 300)
                            .at(1000000000003L, ChronoUnit.MICROS);

                    // Table C uses GOOG, MSFT (both already in dictionary)
                    sender.table("ws_delta_table_c")
                            .symbol("ticker", "GOOG")
                            .doubleColumn("metric", 2800.5)
                            .at(1000000000004L, ChronoUnit.MICROS);
                    sender.table("ws_delta_table_c")
                            .symbol("ticker", "MSFT")
                            .doubleColumn("metric", 299.5)
                            .at(1000000000005L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_table_a");
                serverMain.awaitTable("ws_delta_table_b");
                serverMain.awaitTable("ws_delta_table_c");
                serverMain.assertSql("select count() from ws_delta_table_a", "count\n2\n");
                serverMain.assertSql("select count() from ws_delta_table_b", "count\n2\n");
                serverMain.assertSql("select count() from ws_delta_table_c", "count\n2\n");
            }
        });
    }

    /**
     * Tests multiple symbol columns sharing the global dictionary.
     * <p>
     * Different symbol columns (e.g., "region" and "currency") share the
     * same global dictionary, so a symbol value used in one column can
     * be referenced in another.
     */
    @Test
    public void testDeltaSymbolDict_multipleColumns_sharedDictionary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Multiple symbol columns: region, currency, status
                    sender.table("ws_delta_multi_col")
                            .symbol("region", "us")
                            .symbol("currency", "USD")
                            .symbol("status", "active")
                            .longColumn("value", 100)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("ws_delta_multi_col")
                            .symbol("region", "eu")
                            .symbol("currency", "EUR")
                            .symbol("status", "active")  // Reuses "active"
                            .longColumn("value", 200)
                            .at(1000000000001L, ChronoUnit.MICROS);

                    sender.table("ws_delta_multi_col")
                            .symbol("region", "asia")
                            .symbol("currency", "JPY")
                            .symbol("status", "pending")
                            .longColumn("value", 300)
                            .at(1000000000002L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_multi_col");
                serverMain.assertSql("select count() from ws_delta_multi_col", "count\n3\n");
                serverMain.assertSql("select count(distinct region) from ws_delta_multi_col", "count_distinct\n3\n");
                serverMain.assertSql("select count(distinct currency) from ws_delta_multi_col", "count_distinct\n3\n");
                serverMain.assertSql("select count(distinct status) from ws_delta_multi_col", "count_distinct\n2\n");
            }
        });
    }

    /**
     * Tests high volume ingestion with many unique symbols.
     * <p>
     * This stresses the delta encoding by having a large number of unique
     * symbols that need to be tracked. The dictionary grows over batches.
     */
    @Test
    public void testDeltaSymbolDict_highVolume_manySymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "262144"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                int numSymbols = 100;
                int rowsPerBatch = 50;
                int numBatches = 5;

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    int row = 0;
                    for (int batch = 0; batch < numBatches; batch++) {
                        for (int i = 0; i < rowsPerBatch; i++) {
                            // Cycle through symbols, introducing new ones in each batch
                            int symbolIdx = (batch * 20 + i) % numSymbols;
                            sender.table("ws_delta_high_vol")
                                    .symbol("device", "device-" + symbolIdx)
                                    .longColumn("reading", row)
                                    .at(1000000000000L + row, ChronoUnit.MICROS);
                            row++;
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("ws_delta_high_vol");
                serverMain.assertSql("select count() from ws_delta_high_vol",
                        "count\n" + (numBatches * rowsPerBatch) + "\n");
            }
        });
    }

    /**
     * Tests that unicode symbols work correctly with delta encoding.
     */
    @Test
    public void testDeltaSymbolDict_unicodeSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Batch 1: unicode symbols
                    sender.table("ws_delta_unicode")
                            .symbol("city", "東京")
                            .longColumn("temp", 20)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.table("ws_delta_unicode")
                            .symbol("city", "北京")
                            .longColumn("temp", 15)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 2: reuse unicode symbols + add new
                    sender.table("ws_delta_unicode")
                            .symbol("city", "東京")  // Reuse
                            .longColumn("temp", 21)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.table("ws_delta_unicode")
                            .symbol("city", "서울")  // New
                            .longColumn("temp", 18)
                            .at(1000000000003L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_unicode");
                serverMain.assertSql("select count() from ws_delta_unicode", "count\n4\n");
                serverMain.assertSql("select count(distinct city) from ws_delta_unicode", "count_distinct\n3\n");
            }
        });
    }

    /**
     * Tests empty symbols (empty string) with delta encoding.
     */
    @Test
    public void testDeltaSymbolDict_emptySymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    sender.table("ws_delta_empty_sym")
                            .symbol("tag", "")  // Empty symbol
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.table("ws_delta_empty_sym")
                            .symbol("tag", "nonempty")
                            .longColumn("value", 2)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.table("ws_delta_empty_sym")
                            .symbol("tag", "")  // Reuse empty
                            .longColumn("value", 3)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_empty_sym");
                serverMain.assertSql("select count() from ws_delta_empty_sym", "count\n3\n");
            }
        });
    }

    /**
     * Tests rapid reconnection cycles with delta dictionary.
     * <p>
     * Multiple quick connect/send/disconnect cycles ensure the server
     * properly resets connection state each time.
     */
    @Test
    public void testDeltaSymbolDict_rapidReconnects() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // 5 rapid reconnection cycles
                for (int cycle = 0; cycle < 5; cycle++) {
                    try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                        // Same symbols each cycle - each connection starts fresh
                        for (int i = 0; i < 5; i++) {
                            sender.table("ws_delta_rapid_reconnect")
                                    .symbol("cycle", "cycle-" + cycle)
                                    .symbol("idx", "idx-" + i)
                                    .longColumn("value", cycle * 100 + i)
                                    .at(1000000000000L + cycle * 10 + i, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("ws_delta_rapid_reconnect");
                serverMain.assertSql("select count() from ws_delta_rapid_reconnect", "count\n25\n");
                serverMain.assertSql("select count(distinct cycle) from ws_delta_rapid_reconnect", "count_distinct\n5\n");
            }
        });
    }

    /**
     * Tests that batches without any symbols still work correctly.
     * <p>
     * When a batch has no symbol columns, the delta encoding should
     * handle this gracefully (empty delta section).
     */
    @Test
    public void testDeltaSymbolDict_batchWithNoSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Batch 1: with symbols
                    sender.table("ws_delta_no_sym")
                            .symbol("tag", "first")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 2: no symbols at all
                    sender.table("ws_delta_no_sym_data")
                            .longColumn("value", 2)
                            .doubleColumn("metric", 3.14)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 3: symbols again
                    sender.table("ws_delta_no_sym")
                            .symbol("tag", "second")
                            .longColumn("value", 3)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_no_sym");
                serverMain.awaitTable("ws_delta_no_sym_data");
                serverMain.assertSql("select count() from ws_delta_no_sym", "count\n2\n");
                serverMain.assertSql("select count() from ws_delta_no_sym_data", "count\n1\n");
            }
        });
    }

    /**
     * Tests interleaved tables with symbols being added across batches.
     * <p>
     * Multiple tables interleaved in a single batch, each potentially
     * adding new symbols to the shared global dictionary.
     */
    @Test
    public void testDeltaSymbolDict_interleavedTables_multipleBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Batch 1: interleaved tables
                    for (int i = 0; i < 10; i++) {
                        if (i % 2 == 0) {
                            sender.table("ws_delta_inter_a")
                                    .symbol("type", "even")
                                    .longColumn("idx", i)
                                    .at(1000000000000L + i, ChronoUnit.MICROS);
                        } else {
                            sender.table("ws_delta_inter_b")
                                    .symbol("type", "odd")
                                    .longColumn("idx", i)
                                    .at(1000000000000L + i, ChronoUnit.MICROS);
                        }
                    }
                    sender.flush();

                    // Batch 2: more interleaved, adding new symbols
                    for (int i = 10; i < 20; i++) {
                        if (i % 3 == 0) {
                            sender.table("ws_delta_inter_a")
                                    .symbol("type", "triple")  // New symbol
                                    .longColumn("idx", i)
                                    .at(1000000000000L + i, ChronoUnit.MICROS);
                        } else if (i % 3 == 1) {
                            sender.table("ws_delta_inter_b")
                                    .symbol("type", "even")  // Reuse from table_a
                                    .longColumn("idx", i)
                                    .at(1000000000000L + i, ChronoUnit.MICROS);
                        } else {
                            sender.table("ws_delta_inter_c")  // New table
                                    .symbol("type", "remainder")  // New symbol
                                    .longColumn("idx", i)
                                    .at(1000000000000L + i, ChronoUnit.MICROS);
                        }
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_inter_a");
                serverMain.awaitTable("ws_delta_inter_b");
                serverMain.awaitTable("ws_delta_inter_c");
                // Table A: 5 even (batch 1) + 3 triple (batch 2) = 8 rows
                serverMain.assertSql("select count() from ws_delta_inter_a", "count\n8\n");
                // Table B: 5 odd (batch 1) + 4 even-reuse (batch 2) = 9 rows
                serverMain.assertSql("select count() from ws_delta_inter_b", "count\n9\n");
                // Table C: 3 remainder rows (batch 2)
                serverMain.assertSql("select count() from ws_delta_inter_c", "count\n3\n");
            }
        });
    }

    /**
     * Tests long symbol strings with delta encoding.
     */
    @Test
    public void testDeltaSymbolDict_longSymbolStrings() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create long symbol strings
                StringBuilder longSymbol1 = new StringBuilder();
                StringBuilder longSymbol2 = new StringBuilder();
                for (int i = 0; i < 100; i++) {
                    longSymbol1.append("a");
                    longSymbol2.append("b");
                }

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    // Batch 1
                    sender.table("ws_delta_long_sym")
                            .symbol("tag", longSymbol1.toString())
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 2: add second long symbol
                    sender.table("ws_delta_long_sym")
                            .symbol("tag", longSymbol2.toString())
                            .longColumn("value", 2)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 3: reuse first
                    sender.table("ws_delta_long_sym")
                            .symbol("tag", longSymbol1.toString())
                            .longColumn("value", 3)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_long_sym");
                serverMain.assertSql("select count() from ws_delta_long_sym", "count\n3\n");
                serverMain.assertSql("select count(distinct tag) from ws_delta_long_sym", "count_distinct\n2\n");
            }
        });
    }

    /**
     * Tests async mode specifically with delta symbol dictionaries.
     * <p>
     * In async mode, ACKs are received asynchronously and the watermark
     * must be updated correctly to enable delta optimization.
     */
    @Test
    public void testDeltaSymbolDict_asyncMode_watermarkUpdate() throws Exception {
        Assume.assumeTrue(asyncMode);

        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = IlpV4WebSocketSender.connectAsync(
                        "localhost", httpPort, false,
                        5,                              // autoFlushRows = 5: small batches
                        Integer.MAX_VALUE,              // autoFlushBytes: disabled
                        TimeUnit.HOURS.toNanos(1),      // autoFlushInterval: disabled
                        10,                             // inFlightWindow
                        20                              // sendQueueCapacity
                )) {
                    // Send multiple small batches
                    for (int batch = 0; batch < 10; batch++) {
                        for (int i = 0; i < 5; i++) {
                            sender.table("ws_delta_async")
                                    .symbol("batch", "batch-" + batch)
                                    .symbol("ticker", i % 2 == 0 ? "AAPL" : "GOOG")
                                    .longColumn("value", batch * 10 + i)
                                    .at(1000000000000L + batch * 10 + i, ChronoUnit.MICROS);
                        }
                        // Auto-flush triggers every 5 rows
                    }
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_async");
                serverMain.assertSql("select count() from ws_delta_async", "count\n50\n");
                serverMain.assertSql("select count(distinct batch) from ws_delta_async", "count_distinct\n10\n");
                serverMain.assertSql("select count(distinct ticker) from ws_delta_async", "count_distinct\n2\n");
            }
        });
    }

    /**
     * Tests symbols combined with arrays and decimals.
     * <p>
     * Ensures delta encoding works correctly when rows have complex
     * schemas including arrays and decimal columns.
     */
    @Test
    public void testDeltaSymbolDict_withArraysAndDecimals() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4WebSocketSender sender = createSender(httpPort)) {
                    io.questdb.std.Decimal64 price1 = new io.questdb.std.Decimal64(15099, 2);
                    io.questdb.std.Decimal64 price2 = new io.questdb.std.Decimal64(28005, 2);

                    // Batch 1
                    sender.table("ws_delta_complex")
                            .symbol("ticker", "AAPL")
                            .decimalColumn("price", price1)
                            .doubleArray("features", new double[]{1.0, 2.0, 3.0})
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Batch 2: new symbol + reuse
                    sender.table("ws_delta_complex")
                            .symbol("ticker", "GOOG")
                            .decimalColumn("price", price2)
                            .doubleArray("features", new double[]{4.0, 5.0, 6.0})
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.table("ws_delta_complex")
                            .symbol("ticker", "AAPL")  // Reuse
                            .decimalColumn("price", price1)
                            .doubleArray("features", new double[]{7.0, 8.0, 9.0})
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ws_delta_complex");
                serverMain.assertSql("select count() from ws_delta_complex", "count\n3\n");
                serverMain.assertSql("select count(distinct ticker) from ws_delta_complex", "count_distinct\n2\n");
            }
        });
    }
}
