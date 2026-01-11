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

package io.questdb.test.cutlass.http.line;

import io.questdb.PropertyKey;
import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.http.IlpV4HttpSender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import io.questdb.test.tools.TlsProxyRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * End-to-end integration tests for ILP v4 HTTP sender and receiver.
 * <p>
 * These tests verify that data sent via IlpV4HttpSender over HTTP is correctly
 * written to QuestDB tables and can be queried.
 */
public class IlpV4HttpSenderReceiverTest extends AbstractBootstrapTest {

    @Rule
    public TlsProxyRule tlsProxy = TlsProxyRule.toHostAndPort("localhost", HTTP_PORT);

    @Override
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testSingleRowInsertion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                System.out.println("Starting HTTP test on port: " + httpPort);

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("test_single")
                            .symbol("city", "London")
                            .doubleColumn("temperature", 23.5)
                            .longColumn("humidity", 65)
                            .at(1000000000000L, ChronoUnit.MICROS); // Fixed timestamp
                    System.out.println("About to flush...");
                    sender.flush();
                    System.out.println("Flush completed");
                }

                serverMain.awaitTable("test_single");
                serverMain.assertSql("select count() from test_single", "count\n1\n");
            }
        });
    }

    @Test
    public void testBatchInsertion() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_batch")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_batch");
                serverMain.assertSql("select count() from test_batch", "count\n100\n");
            }
        });
    }

    @Test
    public void testMultiTableBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write to multiple tables in a single batch
                    sender.table("weather")
                            .symbol("city", "London")
                            .doubleColumn("temp", 20.0)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("sensors")
                            .symbol("id", "S1")
                            .longColumn("reading", 100)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("weather")
                            .symbol("city", "Paris")
                            .doubleColumn("temp", 22.0)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("sensors")
                            .symbol("id", "S2")
                            .longColumn("reading", 200)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("weather");
                serverMain.awaitTable("sensors");
                serverMain.assertSql("select count() from weather", "count\n2\n");
                serverMain.assertSql("select count() from sensors", "count\n2\n");
            }
        });
    }

    @Test
    public void testAllDataTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("all_types")
                            .boolColumn("bool_col", true)
                            .longColumn("long_col", 9999999999L)
                            .intColumn("int_col", 123456)
                            .doubleColumn("double_col", 3.14159265359)
                            .floatColumn("float_col", 2.71828f)
                            .stringColumn("string_col", "hello world")
                            .symbol("symbol_col", "sym_value")
                            .timestampColumn("ts_col", 1609459200000000L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("all_types");
                serverMain.assertSql("select count() from all_types", "count\n1\n");
            }
        });
    }

    @Test
    public void testNullValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // First row with all values
                    sender.table("test_nulls")
                            .symbol("tag", "full")
                            .doubleColumn("value", 1.0)
                            .stringColumn("name", "first")
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // Second row with some nulls (missing columns)
                    sender.table("test_nulls")
                            .symbol("tag", "partial")
                            .doubleColumn("value", 2.0)
                            // name is missing - should be null
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("test_nulls");
                serverMain.assertSql("select count() from test_nulls", "count\n2\n");
            }
        });
    }

    @Test
    public void testLargeDataset() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int rowCount = 10000;

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.autoFlushRows(1000); // Auto-flush every 1000 rows

                    for (int i = 0; i < rowCount; i++) {
                        sender.table("large_test")
                                .symbol("partition", "p" + (i % 10))
                                .longColumn("id", i)
                                .doubleColumn("value", i * 0.1)
                                .stringColumn("data", "row_" + i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush(); // Final flush for remaining rows
                }

                serverMain.awaitTable("large_test");
                serverMain.assertSql("select count() from large_test", "count\n10000\n");
            }
        });
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // First write with initial schema
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("evolving")
                            .symbol("tag", "v1")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("evolving");
                serverMain.assertSql("select count() from evolving", "count\n1\n");

                // Add new column in second write
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("evolving")
                            .symbol("tag", "v2")
                            .longColumn("value", 2)
                            .stringColumn("new_col", "added") // New column
                            .at(1000001000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("evolving");
                serverMain.assertSql("select count() from evolving", "count\n2\n");
            }
        });
    }

    @Test
    public void testTimestampOrdering() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Send out-of-order timestamps
                    sender.table("ts_order")
                            .longColumn("seq", 3)
                            .at(3000000000000L, ChronoUnit.MICROS);
                    sender.table("ts_order")
                            .longColumn("seq", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.table("ts_order")
                            .longColumn("seq", 2)
                            .at(2000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("ts_order");
                serverMain.assertSql("select count() from ts_order", "count\n3\n");
            }
        });
    }

    @Test
    public void testSymbolDeduplication() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write many rows with repeated symbol values
                    for (int i = 0; i < 1000; i++) {
                        sender.table("symbol_test")
                                .symbol("category", "cat_" + (i % 5)) // Only 5 unique values
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("symbol_test");
                serverMain.assertSql("select count() from symbol_test", "count\n1000\n");
            }
        });
    }

    @Test
    public void testUnicodeData() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("unicode_test")
                            .symbol("lang", "Japanese")
                            .stringColumn("text", "Hello World")
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("unicode_test")
                            .symbol("lang", "Chinese")
                            .stringColumn("text", "Hello World")
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("unicode_test")
                            .symbol("lang", "Emoji")
                            .stringColumn("text", "Hello World")
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("unicode_test");
                serverMain.assertSql("select count() from unicode_test", "count\n3\n");
            }
        });
    }

    @Test
    public void testGorillaTimestampCompression() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    Assert.assertTrue("Gorilla should be enabled", sender.isGorillaEnabled());

                    // Send timestamps with constant interval (best case for Gorilla)
                    long baseTs = 1000000000000L;
                    for (int i = 0; i < 1000; i++) {
                        sender.table("gorilla_test")
                                .longColumn("value", i)
                                .at(baseTs + i * 1000000L); // 1ms interval
                    }
                    sender.flush();
                }

                serverMain.awaitTable("gorilla_test");
                serverMain.assertSql("select count() from gorilla_test", "count\n1000\n");
            }
        });
    }

    @Test
    public void testMultipleFlushes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Multiple flush cycles
                    for (int batch = 0; batch < 5; batch++) {
                        for (int i = 0; i < 100; i++) {
                            sender.table("multi_flush")
                                    .longColumn("batch", batch)
                                    .longColumn("row", i)
                                    .at(1000000000000L + batch * 100000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("multi_flush");
                serverMain.assertSql("select count() from multi_flush", "count\n500\n");
            }
        });
    }

    @Test
    public void testEmptyStringAndSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("empty_strings")
                            .symbol("sym", "")
                            .stringColumn("str", "")
                            .longColumn("id", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("empty_strings")
                            .symbol("sym", "non-empty")
                            .stringColumn("str", "has value")
                            .longColumn("id", 2)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("empty_strings");
                serverMain.assertSql("select count() from empty_strings", "count\n2\n");
            }
        });
    }

    @Test
    public void testSpecialNumericValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("special_nums")
                            .longColumn("max_long", Long.MAX_VALUE)
                            .longColumn("min_long", Long.MIN_VALUE)
                            .doubleColumn("pos_inf", Double.POSITIVE_INFINITY)
                            .doubleColumn("neg_inf", Double.NEGATIVE_INFINITY)
                            .doubleColumn("nan", Double.NaN)
                            .doubleColumn("max_double", Double.MAX_VALUE)
                            .doubleColumn("min_double", Double.MIN_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("special_nums");
                serverMain.assertSql("select count() from special_nums", "count\n1\n");
            }
        });
    }

    @Test
    public void testReconnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // First connection
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("reconnect_test")
                            .longColumn("conn", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("reconnect_test");
                serverMain.assertSql("select count() from reconnect_test", "count\n1\n");

                // Second connection (simulates reconnection)
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("reconnect_test")
                            .longColumn("conn", 2)
                            .at(1000001000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("reconnect_test");
                serverMain.assertSql("select count() from reconnect_test", "count\n2\n");
            }
        });
    }

    // ==================== Schema Evolution Tests ====================

    @Test
    public void testSchemaEvolutionMultipleColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // First write with minimal schema
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v1")
                            .longColumn("a", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("schema_multi");
                serverMain.assertSql("select count() from schema_multi", "count\n1\n");

                // Add column b
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v2")
                            .longColumn("a", 2)
                            .doubleColumn("b", 2.0)
                            .at(1000001000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("schema_multi");
                serverMain.assertSql("select count() from schema_multi", "count\n2\n");

                // Add columns c and d
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v3")
                            .longColumn("a", 3)
                            .doubleColumn("b", 3.0)
                            .stringColumn("c", "hello")
                            .boolColumn("d", true)
                            .at(1000002000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("schema_multi");
                serverMain.assertSql("select count() from schema_multi", "count\n3\n");
            }
        });
    }

    @Test
    public void testSchemaEvolutionAcrossConnections() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Multiple separate connections each adding columns
                for (int round = 0; round < 5; round++) {
                    try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        sender.table("schema_conn");
                        sender.symbol("round", "r" + round);

                        // Each round adds a new column
                        for (int col = 0; col <= round; col++) {
                            sender.longColumn("col_" + col, col);
                        }
                        sender.at(1000000000000L + round * 1000000L, ChronoUnit.MICROS);
                        sender.flush();
                    }

                    serverMain.awaitTable("schema_conn");
                }

                serverMain.assertSql("select count() from schema_conn", "count\n5\n");
            }
        });
    }

    @Test
    public void testSparseColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Create table with many columns
                    sender.table("sparse")
                            .longColumn("a", 1)
                            .longColumn("b", 2)
                            .longColumn("c", 3)
                            .longColumn("d", 4)
                            .longColumn("e", 5)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // Write rows with only some columns (sparse)
                    sender.table("sparse")
                            .longColumn("a", 10)
                            .longColumn("c", 30)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("sparse")
                            .longColumn("b", 20)
                            .longColumn("e", 50)
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.table("sparse")
                            .longColumn("d", 40)
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("sparse");
                serverMain.assertSql("select count() from sparse", "count\n4\n");
            }
        });
    }

    // ==================== Data Type Edge Cases ====================

    @Test
    public void testNumericBoundaryValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Long boundaries
                    sender.table("numeric_bounds")
                            .longColumn("max_long", Long.MAX_VALUE)
                            .longColumn("min_long", Long.MIN_VALUE)
                            .longColumn("zero_long", 0L)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // Integer boundaries
                    sender.table("numeric_bounds")
                            .intColumn("max_int", Integer.MAX_VALUE)
                            .intColumn("min_int", Integer.MIN_VALUE)
                            .intColumn("zero_int", 0)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    // Short boundaries
                    sender.table("numeric_bounds")
                            .shortColumn("max_short", Short.MAX_VALUE)
                            .shortColumn("min_short", Short.MIN_VALUE)
                            .shortColumn("zero_short", (short) 0)
                            .at(1000002000000L, ChronoUnit.MICROS);

                    // Byte boundaries
                    sender.table("numeric_bounds")
                            .byteColumn("max_byte", Byte.MAX_VALUE)
                            .byteColumn("min_byte", Byte.MIN_VALUE)
                            .byteColumn("zero_byte", (byte) 0)
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("numeric_bounds");
                serverMain.assertSql("select count() from numeric_bounds", "count\n4\n");
            }
        });
    }

    @Test
    public void testSpecialFloatValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Float special values
                    sender.table("float_special")
                            .floatColumn("pos_inf", Float.POSITIVE_INFINITY)
                            .floatColumn("neg_inf", Float.NEGATIVE_INFINITY)
                            .floatColumn("nan", Float.NaN)
                            .floatColumn("max", Float.MAX_VALUE)
                            .floatColumn("min", Float.MIN_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // Double special values
                    sender.table("float_special")
                            .doubleColumn("d_pos_inf", Double.POSITIVE_INFINITY)
                            .doubleColumn("d_neg_inf", Double.NEGATIVE_INFINITY)
                            .doubleColumn("d_nan", Double.NaN)
                            .doubleColumn("d_max", Double.MAX_VALUE)
                            .doubleColumn("d_min", Double.MIN_VALUE)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("float_special");
                serverMain.assertSql("select count() from float_special", "count\n2\n");
            }
        });
    }

    @Test
    public void testBooleanValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("bool_test")
                            .boolColumn("val", true)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("bool_test")
                            .boolColumn("val", false)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    // Alternating pattern
                    for (int i = 0; i < 10; i++) {
                        sender.table("bool_test")
                                .boolColumn("val", i % 2 == 0)
                                .at(1000002000000L + i * 1000000L, ChronoUnit.MICROS);
                    }

                    sender.flush();
                }

                serverMain.awaitTable("bool_test");
                serverMain.assertSql("select count() from bool_test", "count\n12\n");
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("all_numeric")
                                .byteColumn("byte_col", (byte) (i % 128))
                                .shortColumn("short_col", (short) (i * 100))
                                .intColumn("int_col", i * 10000)
                                .longColumn("long_col", (long) i * 100000000L)
                                .floatColumn("float_col", i * 1.1f)
                                .doubleColumn("double_col", i * 1.111111)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("all_numeric");
                serverMain.assertSql("select count() from all_numeric", "count\n100\n");
            }
        });
    }

    // ==================== String/Symbol Tests ====================

    @Test
    public void testLongStringValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Create strings of various lengths
                    StringBuilder sb = new StringBuilder();
                    for (int len = 1; len <= 1000; len *= 10) {
                        sb.setLength(0);
                        for (int i = 0; i < len; i++) {
                            sb.append((char) ('a' + (i % 26)));
                        }
                        sender.table("long_strings")
                                .longColumn("len", len)
                                .stringColumn("str", sb.toString())
                                .at(1000000000000L + len * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("long_strings");
                serverMain.assertSql("select count() from long_strings", "count\n4\n");
            }
        });
    }

    @Test
    public void testUnicodeEdgeCases() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Various Unicode edge cases
                    sender.table("unicode_edge")
                            .stringColumn("ascii", "hello")
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("unicode_edge")
                            .stringColumn("latin", "caf\u00e9") // café
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("unicode_edge")
                            .stringColumn("cyrillic", "\u041f\u0440\u0438\u0432\u0435\u0442") // Привет
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.table("unicode_edge")
                            .stringColumn("arabic", "\u0645\u0631\u062d\u0628\u0627") // مرحبا
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.table("unicode_edge")
                            .stringColumn("cjk", "\u4f60\u597d") // 你好
                            .at(1000004000000L, ChronoUnit.MICROS);

                    sender.table("unicode_edge")
                            .stringColumn("emoji", "\uD83D\uDE00\uD83C\uDF89") // 😀🎉
                            .at(1000005000000L, ChronoUnit.MICROS);

                    sender.table("unicode_edge")
                            .stringColumn("mixed", "Hello \u4e16\u754c \uD83C\uDF0D") // Hello 世界 🌍
                            .at(1000006000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("unicode_edge");
                serverMain.assertSql("select count() from unicode_edge", "count\n7\n");
            }
        });
    }

    @Test
    public void testManyUniqueSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write many rows with unique symbol values
                    for (int i = 0; i < 1000; i++) {
                        sender.table("many_symbols")
                                .symbol("unique_sym", "sym_" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("many_symbols");
                serverMain.assertSql("select count() from many_symbols", "count\n1000\n");
                serverMain.assertSql("select count(distinct unique_sym) from many_symbols", "count_distinct\n1000\n");
            }
        });
    }

    @Test
    public void testSymbolsWithSpecialCharacters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("special_symbols")
                            .symbol("sym", "with-dash")
                            .longColumn("id", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("sym", "with_underscore")
                            .longColumn("id", 2)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("sym", "with.dot")
                            .longColumn("id", 3)
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("sym", "with spaces")
                            .longColumn("id", 4)
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("sym", "CamelCase")
                            .longColumn("id", 5)
                            .at(1000004000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("sym", "ALLCAPS")
                            .longColumn("id", 6)
                            .at(1000005000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("special_symbols");
                serverMain.assertSql("select count() from special_symbols", "count\n6\n");
            }
        });
    }

    @Test
    public void testMultipleSymbolColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("multi_sym")
                                .symbol("sym1", "a" + (i % 5))
                                .symbol("sym2", "b" + (i % 10))
                                .symbol("sym3", "c" + (i % 3))
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("multi_sym");
                serverMain.assertSql("select count() from multi_sym", "count\n100\n");
            }
        });
    }

    // ==================== Timestamp Tests ====================

    @Test
    public void testTimestampBoundaries() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Very old timestamp
                    sender.table("ts_bounds")
                            .longColumn("id", 1)
                            .at(1000000L, ChronoUnit.MICROS); // 1970

                    // Recent timestamp
                    sender.table("ts_bounds")
                            .longColumn("id", 2)
                            .at(1609459200000000L, ChronoUnit.MICROS); // 2021

                    // Far future
                    sender.table("ts_bounds")
                            .longColumn("id", 3)
                            .at(4102444800000000L, ChronoUnit.MICROS); // 2100

                    sender.flush();
                }

                serverMain.awaitTable("ts_bounds");
                serverMain.assertSql("select count() from ts_bounds", "count\n3\n");
            }
        });
    }

    @Test
    public void testMicrosecondPrecision() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    long baseTs = 1000000000000L;
                    for (int i = 0; i < 100; i++) {
                        sender.table("micro_precision")
                                .longColumn("seq", i)
                                .at(baseTs + i, ChronoUnit.MICROS); // Increment by 1 microsecond
                    }
                    sender.flush();
                }

                serverMain.awaitTable("micro_precision");
                serverMain.assertSql("select count() from micro_precision", "count\n100\n");
            }
        });
    }

    @Test
    public void testDecreasingTimestamps() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    long baseTs = 2000000000000L;
                    for (int i = 0; i < 100; i++) {
                        sender.table("dec_ts")
                                .longColumn("seq", i)
                                .at(baseTs - i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("dec_ts");
                serverMain.assertSql("select count() from dec_ts", "count\n100\n");
            }
        });
    }

    @Test
    public void testSameTimestampDifferentRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    long ts = 1000000000000L;
                    for (int i = 0; i < 100; i++) {
                        sender.table("same_ts")
                                .symbol("id", "row" + i)
                                .longColumn("seq", i)
                                .at(ts, ChronoUnit.MICROS); // Same timestamp for all rows
                    }
                    sender.flush();
                }

                serverMain.awaitTable("same_ts");
                serverMain.assertSql("select count() from same_ts", "count\n100\n");
            }
        });
    }

    @Test
    public void testDataAcrossPartitions() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write data spanning multiple days (multiple partitions)
                    for (int day = 0; day < 10; day++) {
                        for (int i = 0; i < 10; i++) {
                            sender.table("multi_part")
                                    .symbol("day", "d" + day)
                                    .longColumn("seq", day * 10 + i)
                                    .at(1000000000000L + day * 86400000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                    }
                    sender.flush();
                }

                serverMain.awaitTable("multi_part");
                serverMain.assertSql("select count() from multi_part", "count\n100\n");
            }
        });
    }

    // ==================== Batch/Flush Patterns ====================

    @Test
    public void testVerySmallBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Flush after every single row
                    for (int i = 0; i < 50; i++) {
                        sender.table("small_batch")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                        sender.flush();
                    }
                }

                serverMain.awaitTable("small_batch");
                serverMain.assertSql("select count() from small_batch", "count\n50\n");
            }
        });
    }

    @Test
    public void testManySmallBatches() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Many small batches
                    for (int batch = 0; batch < 100; batch++) {
                        for (int i = 0; i < 5; i++) {
                            sender.table("many_small")
                                    .longColumn("batch", batch)
                                    .longColumn("row", i)
                                    .at(1000000000000L + batch * 10000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("many_small");
                serverMain.assertSql("select count() from many_small", "count\n500\n");
            }
        });
    }

    @Test
    public void testVariableRowSizes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 100; i++) {
                        // Create variable-length strings
                        sb.setLength(0);
                        for (int j = 0; j < (i % 50) + 1; j++) {
                            sb.append((char) ('a' + (j % 26)));
                        }

                        sender.table("var_rows")
                                .symbol("id", "r" + i)
                                .stringColumn("data", sb.toString())
                                .longColumn("len", sb.length())
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("var_rows");
                serverMain.assertSql("select count() from var_rows", "count\n100\n");
            }
        });
    }

    // ==================== Table/Column Name Tests ====================

    @Test
    public void testTableNameVariations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("lower_case")
                            .longColumn("id", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("UPPER_CASE")
                            .longColumn("id", 2)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("MixedCase")
                            .longColumn("id", 3)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("with_underscore")
                            .longColumn("id", 4)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("with123numbers")
                            .longColumn("id", 5)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("lower_case");
                serverMain.awaitTable("UPPER_CASE");
                serverMain.awaitTable("MixedCase");
                serverMain.awaitTable("with_underscore");
                serverMain.awaitTable("with123numbers");

                serverMain.assertSql("select count() from lower_case", "count\n1\n");
            }
        });
    }

    @Test
    public void testColumnNameVariations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("col_names")
                            .longColumn("lower", 1)
                            .longColumn("UPPER", 2)
                            .longColumn("MixedCase", 3)
                            .longColumn("with_underscore", 4)
                            .longColumn("col123", 5)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("col_names");
                serverMain.assertSql("select count() from col_names", "count\n1\n");
            }
        });
    }

    @Test
    public void testWideTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Create table with many columns
                    for (int row = 0; row < 10; row++) {
                        sender.table("wide_table");
                        for (int col = 0; col < 100; col++) {
                            sender.longColumn("col_" + col, row * 100 + col);
                        }
                        sender.at(1000000000000L + row * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("wide_table");
                serverMain.assertSql("select count() from wide_table", "count\n10\n");
            }
        });
    }

    // ==================== Multi-Connection Tests ====================

    @Test
    public void testMultipleConnectionsSameTable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Multiple sequential connections to same table
                for (int conn = 0; conn < 10; conn++) {
                    try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        for (int i = 0; i < 10; i++) {
                            sender.table("multi_conn")
                                    .longColumn("conn", conn)
                                    .longColumn("row", i)
                                    .at(1000000000000L + conn * 100000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("multi_conn");
                serverMain.assertSql("select count() from multi_conn", "count\n100\n");
            }
        });
    }

    @Test
    public void testMultipleConnectionsDifferentTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Each connection writes to a different table
                for (int conn = 0; conn < 5; conn++) {
                    try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        for (int i = 0; i < 20; i++) {
                            sender.table("table_" + conn)
                                    .longColumn("row", i)
                                    .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                for (int conn = 0; conn < 5; conn++) {
                    serverMain.awaitTable("table_" + conn);
                    serverMain.assertSql("select count() from table_" + conn, "count\n20\n");
                }
            }
        });
    }

    @Test
    public void testRapidReconnection() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Rapid connect/write/disconnect cycles
                for (int i = 0; i < 50; i++) {
                    try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        sender.table("rapid_reconnect")
                                .longColumn("seq", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                        sender.flush();
                    }
                }

                serverMain.awaitTable("rapid_reconnect");
                serverMain.assertSql("select count() from rapid_reconnect", "count\n50\n");
            }
        });
    }

    // ==================== Large Data Tests ====================

    @Test
    public void testLargeDataset50K() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "4194304"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int rowCount = 50000;

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.autoFlushRows(5000);

                    for (int i = 0; i < rowCount; i++) {
                        sender.table("large_50k")
                                .symbol("partition", "p" + (i % 20))
                                .longColumn("id", i)
                                .doubleColumn("value", i * 0.1)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("large_50k");
                serverMain.assertSql("select count() from large_50k", "count\n50000\n");
            }
        });
    }

    @Test
    public void testHighThroughput() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "4194304"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.autoFlushRows(10000);

                    // Send as fast as possible
                    for (int i = 0; i < 100000; i++) {
                        sender.table("high_throughput")
                                .longColumn("id", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("high_throughput");
                serverMain.assertSql("select count() from high_throughput", "count\n100000\n");
            }
        });
    }

    // ==================== Mixed Pattern Tests ====================

    @Test
    public void testMixedTableWritesInterleaved() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Interleave writes to multiple tables
                    for (int i = 0; i < 100; i++) {
                        sender.table("interleaved_a")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);

                        sender.table("interleaved_b")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);

                        sender.table("interleaved_c")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("interleaved_a");
                serverMain.awaitTable("interleaved_b");
                serverMain.awaitTable("interleaved_c");
                serverMain.assertSql("select count() from interleaved_a", "count\n100\n");
                serverMain.assertSql("select count() from interleaved_b", "count\n100\n");
                serverMain.assertSql("select count() from interleaved_c", "count\n100\n");
            }
        });
    }

    @Test
    public void testMixedStringAndSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("mixed_str_sym")
                                .symbol("sym1", "s" + (i % 5))
                                .stringColumn("str1", "string_" + i)
                                .symbol("sym2", "t" + (i % 3))
                                .stringColumn("str2", "data_" + (i * 2))
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("mixed_str_sym");
                serverMain.assertSql("select count() from mixed_str_sym", "count\n100\n");
            }
        });
    }

    @Test
    public void testDuplicateColumnValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // All rows have same column values, only timestamp differs
                    for (int i = 0; i < 100; i++) {
                        sender.table("dup_values")
                                .symbol("tag", "same")
                                .longColumn("value", 42)
                                .stringColumn("str", "constant")
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("dup_values");
                serverMain.assertSql("select count() from dup_values", "count\n100\n");
            }
        });
    }

    @Test
    public void testContinuousWriteWithPeriodicFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "1048576"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    int rowsPerFlush = 100;
                    int flushCount = 20;

                    for (int f = 0; f < flushCount; f++) {
                        for (int i = 0; i < rowsPerFlush; i++) {
                            int rowNum = f * rowsPerFlush + i;
                            sender.table("continuous_write")
                                    .longColumn("flush", f)
                                    .longColumn("row", i)
                                    .at(1000000000000L + rowNum * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("continuous_write");
                serverMain.assertSql("select count() from continuous_write", "count\n2000\n");
            }
        });
    }

    // ==================== Large Batch Buffer Growth Tests ====================

    /**
     * Tests that HTTP transport can handle a single large batch that exceeds the initial receive buffer.
     * <p>
     * This test sends 10,000 rows in a single flush with multiple columns per row,
     * producing a message of approximately 600KB+. This exercises the buffer growth
     * and chunked transfer handling in the HTTP processor.
     * <p>
     * This is a regression test to ensure large ILPv4 HTTP messages are handled correctly.
     */
    @Test
    public void testLargeBatchExceedsInitialBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "131072" // 128KB initial buffer
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int rowCount = 10_000;

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // NO autoFlushRows - send everything in a single batch
                    for (int i = 0; i < rowCount; i++) {
                        sender.table("large_batch_test")
                                .symbol("exchange", "NYSE")
                                .symbol("currency", i % 2 == 0 ? "USD" : "EUR")
                                .longColumn("trade_id", i)
                                .longColumn("volume", 100 + (i % 10000))
                                .doubleColumn("price", 100.0 + (i % 1000) * 0.01)
                                .doubleColumn("bid", 99.5 + (i % 1000) * 0.01)
                                .doubleColumn("ask", 100.5 + (i % 1000) * 0.01)
                                .intColumn("sequence", i % 1000000)
                                .floatColumn("spread", 0.5f + (i % 100) * 0.01f)
                                .stringColumn("venue", "New York")
                                .boolColumn("is_buy", i % 2 == 0)
                                .at(1704067200000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    // Single flush - creates one large HTTP request
                    sender.flush();
                }

                serverMain.awaitTable("large_batch_test");
                serverMain.assertSql("select count() from large_batch_test", "count\n10000\n");
            }
        });
    }

    /**
     * Tests an even larger batch (50K rows) to ensure HTTP can handle very large payloads.
     */
    @Test
    public void testVeryLargeBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "131072" // 128KB initial buffer
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int rowCount = 50_000;

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // NO autoFlushRows - send everything in a single batch
                    for (int i = 0; i < rowCount; i++) {
                        sender.table("very_large_batch")
                                .symbol("partition", "p" + (i % 10))
                                .longColumn("id", i)
                                .doubleColumn("value", i * 0.1)
                                .stringColumn("data", "row_" + i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("very_large_batch");
                serverMain.assertSql("select count() from very_large_batch", "count\n50000\n");
            }
        });
    }

    /**
     * Tests large batch with long string values to maximize row size.
     * This creates a message with larger per-row overhead.
     */
    @Test
    public void testLargeBatchWithLongStrings() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "131072" // 128KB initial buffer
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create a moderately long string (500 chars) to increase message size
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 50; i++) {
                    sb.append("0123456789");
                }
                String longString = sb.toString();

                // 5,000 rows with 500-char strings = ~2.5MB of string data alone
                int rowCount = 5_000;

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < rowCount; i++) {
                        sender.table("large_strings_batch")
                                .symbol("tag", "batch")
                                .stringColumn("long_data", longString)
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("large_strings_batch");
                serverMain.assertSql("select count() from large_strings_batch", "count\n5000\n");
            }
        });
    }

    /**
     * Tests multiple large batches in sequence without reconnecting.
     * This ensures HTTP connection handling works correctly across multiple large requests.
     */
    @Test
    public void testMultipleLargeBatchesSequential() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "131072" // 128KB initial buffer
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int batchCount = 5;
                int rowsPerBatch = 10_000;

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int batch = 0; batch < batchCount; batch++) {
                        for (int i = 0; i < rowsPerBatch; i++) {
                            sender.table("multi_large_batch")
                                    .symbol("batch", "b" + batch)
                                    .longColumn("id", batch * rowsPerBatch + i)
                                    .doubleColumn("value", i * 0.1)
                                    .stringColumn("info", "batch" + batch + "_row" + i)
                                    .at(1000000000000L + batch * 100000000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        // Flush each batch separately - each is a large HTTP request
                        sender.flush();
                    }
                }

                serverMain.awaitTable("multi_large_batch");
                serverMain.assertSql("select count() from multi_large_batch", "count\n50000\n");
            }
        });
    }

    /**
     * Tests large batch with all column types to ensure comprehensive coverage.
     */
    @Test
    public void testLargeBatchAllColumnTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "131072" // 128KB initial buffer
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int rowCount = 10_000;

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < rowCount; i++) {
                        sender.table("all_types_batch")
                                // Multiple symbol columns
                                .symbol("sym1", "s" + (i % 10))
                                .symbol("sym2", "t" + (i % 5))
                                // All numeric types
                                .byteColumn("byte_col", (byte) (i % 128))
                                .shortColumn("short_col", (short) (i % 32000))
                                .intColumn("int_col", i)
                                .longColumn("long_col", (long) i * 1000000L)
                                .floatColumn("float_col", i * 0.1f)
                                .doubleColumn("double_col", i * 0.001)
                                // Boolean
                                .boolColumn("bool_col", i % 2 == 0)
                                // String
                                .stringColumn("str_col", "value_" + i)
                                // Timestamp column
                                .timestampColumn("ts_col", 1704067200000000L + i * 1000L)
                                // Designated timestamp
                                .at(1704067200000000L + i * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("all_types_batch");
                serverMain.assertSql("select count() from all_types_batch", "count\n10000\n");
            }
        });
    }

    // ==================== Server-Assigned Timestamp Tests ====================

    /**
     * Tests that atNow() results in server-assigned timestamps.
     * <p>
     * When atNow() is called, the client should NOT send a timestamp value.
     * Instead, the server should assign the timestamp when the row is received.
     * This matches the behavior of the old ILP protocol.
     */
    @Test
    public void testAtNowServerAssignedTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("test_at_now")
                            .symbol("tag", "row1")
                            .longColumn("value", 100)
                            .atNow();
                    sender.flush();
                }

                serverMain.awaitTable("test_at_now");
                serverMain.assertSql("select count() from test_at_now", "count\n1\n");

                // Verify a timestamp column was auto-created
                serverMain.assertSql(
                        "select \"column\" from table_columns('test_at_now') order by \"column\"",
                        "column\ntag\ntimestamp\nvalue\n"
                );

                // Verify the timestamp was assigned by the server (should be recent)
                serverMain.assertSql(
                        "select count() from test_at_now where timestamp >= '2025-01-01'",
                        "count\n1\n"
                );
            }
        });
    }

    /**
     * Tests mixing at() and atNow() in the same batch.
     * <p>
     * Rows with at() should have client-specified timestamps.
     * Rows with atNow() should have server-assigned timestamps.
     */
    @Test
    public void testMixedAtAndAtNow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                long fixedTimestamp = 1704067200000000L; // 2024-01-01 00:00:00 UTC in micros

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Row 1: client-specified timestamp
                    sender.table("test_mixed_ts")
                            .symbol("type", "fixed")
                            .longColumn("value", 1)
                            .at(fixedTimestamp, ChronoUnit.MICROS);

                    // Row 2: server-assigned timestamp
                    sender.table("test_mixed_ts")
                            .symbol("type", "server")
                            .longColumn("value", 2)
                            .atNow();

                    // Row 3: another client-specified timestamp
                    sender.table("test_mixed_ts")
                            .symbol("type", "fixed")
                            .longColumn("value", 3)
                            .at(fixedTimestamp + 1000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("test_mixed_ts");
                serverMain.assertSql("select count() from test_mixed_ts", "count\n3\n");

                // Verify the fixed timestamps (2024-01-01)
                serverMain.assertSql(
                        "select count() from test_mixed_ts where type = 'fixed' and timestamp >= '2024-01-01' and timestamp < '2024-01-02'",
                        "count\n2\n"
                );

                // Verify the server-assigned timestamp is recent (not in 2024, should be 2025+)
                serverMain.assertSql(
                        "select count() from test_mixed_ts where type = 'server' and timestamp >= '2025-01-01'",
                        "count\n1\n"
                );
            }
        });
    }

    /**
     * Tests multiple consecutive atNow() calls.
     */
    @Test
    public void testMultipleAtNow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();
                int rowCount = 100;

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < rowCount; i++) {
                        sender.table("test_multi_at_now")
                                .longColumn("id", i)
                                .atNow();
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_multi_at_now");
                serverMain.assertSql("select count() from test_multi_at_now", "count\n" + rowCount + "\n");

                // All timestamps should be server-assigned (recent - 2025+)
                serverMain.assertSql(
                        "select count() from test_multi_at_now where timestamp >= '2025-01-01'",
                        "count\n" + rowCount + "\n"
                );
            }
        });
    }

    /**
     * Tests atNow() with a table that has a custom designated timestamp column name.
     * <p>
     * The designated timestamp column is NOT always named "timestamp" - it can be
     * any name when the table is created explicitly via SQL.
     * <p>
     * This test verifies that:
     * 1. atNow() works with custom timestamp column names
     * 2. No spurious "timestamp" column is created
     */
    @Test
    public void testAtNowWithCustomTimestampColumnName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create table with custom designated timestamp column named 'ts'
                serverMain.execute("CREATE TABLE custom_ts_table (sym SYMBOL, value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Ingest data using atNow()
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("custom_ts_table")
                            .symbol("sym", "test")
                            .longColumn("value", 42)
                            .atNow();
                    sender.flush();
                }

                serverMain.awaitTable("custom_ts_table");

                // Verify row was inserted
                serverMain.assertSql("select count() from custom_ts_table", "count\n1\n");

                // Verify the table has ONLY the expected columns (sym, value, ts)
                // There should be NO "timestamp" column created
                serverMain.assertSql(
                        "select \"column\" from table_columns('custom_ts_table') order by \"column\"",
                        "column\nsym\nts\nvalue\n"
                );

                // Verify the timestamp was assigned by the server (should be recent)
                serverMain.assertSql(
                        "select count() from custom_ts_table where ts >= '2025-01-01'",
                        "count\n1\n"
                );
            }
        });
    }

    /**
     * Tests at() with explicit timestamp on a table with custom designated timestamp column name.
     * <p>
     * The designated timestamp column is NOT always named "timestamp" - it can be
     * any name when the table is created explicitly via SQL.
     * <p>
     * This test verifies that:
     * 1. at() with explicit timestamp works with custom timestamp column names
     * 2. No spurious "timestamp" column is created
     * 3. The explicit timestamp value is correctly stored in the designated timestamp column
     */
    @Test
    public void testAtWithCustomTimestampColumnName() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create table with custom designated timestamp column named 'ts'
                serverMain.execute("CREATE TABLE custom_ts_at_table (sym SYMBOL, value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Ingest data using at() with explicit timestamp
                long explicitTimestamp = 1700000000000000L; // 2023-11-14T22:13:20Z in micros
                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("custom_ts_at_table")
                            .symbol("sym", "test")
                            .longColumn("value", 42)
                            .at(explicitTimestamp, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("custom_ts_at_table");

                // Verify row was inserted
                serverMain.assertSql("select count() from custom_ts_at_table", "count\n1\n");

                // Verify the table has ONLY the expected columns (sym, value, ts)
                // There should be NO "timestamp" column created
                serverMain.assertSql(
                        "select \"column\" from table_columns('custom_ts_at_table') order by \"column\"",
                        "column\nsym\nts\nvalue\n"
                );

                // Verify the explicit timestamp was correctly stored
                serverMain.assertSql(
                        "select ts from custom_ts_at_table",
                        "ts\n2023-11-14T22:13:20.000000Z\n"
                );
            }
        });
    }

    /**
     * Tests nanosecond precision - verifies 1 nanosecond accuracy.
     * <p>
     * Sends timestamps with nanosecond precision and verifies:
     * 1. Correct conversion to microseconds (QuestDB's storage precision)
     * 2. Nanosecond values differing by just 1ns are correctly handled
     */
    @Test
    public void testNanosecondAccuracy() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Test nanosecond precision - values that differ by 1 nanosecond
                // 1700000000000000000 nanos = 2023-11-14T22:13:20.000000000Z
                // 1700000000000000001 nanos = 2023-11-14T22:13:20.000000001Z (1ns later)
                // 1700000000000000999 nanos = 2023-11-14T22:13:20.000000999Z (999ns later)
                // 1700000000000001000 nanos = 2023-11-14T22:13:20.000001000Z (1000ns = 1 micro later)

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // These should all map to the same microsecond (truncation)
                    sender.table("test_nanos_accuracy")
                            .symbol("label", "base")
                            .longColumn("nanos_value", 1700000000000000000L)
                            .at(1700000000000000000L, ChronoUnit.NANOS);

                    sender.table("test_nanos_accuracy")
                            .symbol("label", "plus_1ns")
                            .longColumn("nanos_value", 1700000000000000001L)
                            .at(1700000000000000001L, ChronoUnit.NANOS);

                    sender.table("test_nanos_accuracy")
                            .symbol("label", "plus_999ns")
                            .longColumn("nanos_value", 1700000000000000999L)
                            .at(1700000000000000999L, ChronoUnit.NANOS);

                    // This should map to the NEXT microsecond
                    sender.table("test_nanos_accuracy")
                            .symbol("label", "plus_1000ns")
                            .longColumn("nanos_value", 1700000000000001000L)
                            .at(1700000000000001000L, ChronoUnit.NANOS);

                    // Test with sub-microsecond precision in the middle of a second
                    // 1700000000123456789 nanos should become 1700000000123456 micros
                    sender.table("test_nanos_accuracy")
                            .symbol("label", "with_submicros")
                            .longColumn("nanos_value", 1700000000123456789L)
                            .at(1700000000123456789L, ChronoUnit.NANOS);

                    sender.flush();
                }

                serverMain.awaitTable("test_nanos_accuracy");

                // Verify full nanosecond precision is preserved
                // TIMESTAMP_NANO stores nanoseconds natively with 9 decimal digits
                serverMain.assertSql(
                        "select label, timestamp from test_nanos_accuracy order by nanos_value",
                        "label\ttimestamp\n" +
                                "base\t2023-11-14T22:13:20.000000000Z\n" +
                                "plus_1ns\t2023-11-14T22:13:20.000000001Z\n" +
                                "plus_999ns\t2023-11-14T22:13:20.000000999Z\n" +
                                "plus_1000ns\t2023-11-14T22:13:20.000001000Z\n" +
                                "with_submicros\t2023-11-14T22:13:20.123456789Z\n"
                );
            }
        });
    }

    /**
     * Tests microsecond timestamps beyond nanosecond range.
     * <p>
     * Year 3000 cannot be represented in nanoseconds (would overflow 64-bit signed long),
     * but can be represented in microseconds. This test verifies that ILP v4 can handle
     * such far-future dates when sent as microseconds.
     * <p>
     * Max nanoseconds: ~292 years from epoch (year ~2262)
     * Max microseconds: ~292,000 years from epoch
     */
    @Test
    public void testMicrosecondRangeBeyondNanos() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Year 3000: approximately 1030 years from 1970
                // In microseconds: 1030 * 365.25 * 24 * 60 * 60 * 1_000_000 ≈ 32.5 * 10^15
                // This fits in a long (max ~9.2 * 10^18) but would overflow if converted to nanos

                // 3000-01-01T00:00:00Z in microseconds
                long year3000Micros = 32503680000000000L; // ~Jan 1, 3000

                // Verify this would overflow in nanos
                // year3000Micros * 1000 would overflow
                assert year3000Micros > Long.MAX_VALUE / 1000 : "Test value should overflow if converted to nanos";

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("test_future_date")
                            .symbol("era", "far_future")
                            .longColumn("value", 42)
                            .at(year3000Micros, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_future_date");

                // Verify the far-future timestamp is correctly stored
                serverMain.assertSql(
                        "select timestamp from test_future_date",
                        "timestamp\n3000-01-01T00:00:00.000000Z\n"
                );
            }
        });
    }

    /**
     * Tests timestampColumn() with nanosecond precision for a regular timestamp column.
     */
    @Test
    public void testTimestampColumnWithNanoseconds() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create table with a regular timestamp column (not designated)
                serverMain.execute("CREATE TABLE test_ts_col (sym SYMBOL, event_time TIMESTAMP, value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Timestamp with nanosecond precision
                long nanosTimestamp = 1700000000123456789L;
                long designatedTs = 1700000000000000L; // micros

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("test_ts_col")
                            .symbol("sym", "test")
                            .timestampColumn("event_time", nanosTimestamp, ChronoUnit.NANOS)
                            .longColumn("value", 42)
                            .at(designatedTs, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_col");

                // Verify the nanosecond precision is preserved (converted to micros)
                serverMain.assertSql(
                        "select event_time from test_ts_col",
                        "event_time\n2023-11-14T22:13:20.123456Z\n"
                );
            }
        });
    }

    /**
     * Tests timestampColumn() with microsecond value beyond nanosecond range.
     */
    @Test
    public void testTimestampColumnBeyondNanosRange() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Create table with a regular timestamp column
                serverMain.execute("CREATE TABLE test_ts_col_future (sym SYMBOL, event_time TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Year 3000 in microseconds - beyond nanos range
                long year3000Micros = 32503680000000000L;
                long designatedTs = 1700000000000000L; // 2023 in micros

                try (Sender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("test_ts_col_future")
                            .symbol("sym", "future")
                            .timestampColumn("event_time", year3000Micros, ChronoUnit.MICROS)
                            .at(designatedTs, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_col_future");

                serverMain.assertSql(
                        "select event_time from test_ts_col_future",
                        "event_time\n3000-01-01T00:00:00.000000Z\n"
                );
            }
        });
    }

    // Builder integration tests

    @Test
    public void testBuilderBinaryTransfer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Use SenderBuilder with binaryTransfer()
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_builder")
                            .symbol("city", "Paris")
                            .doubleColumn("temperature", 25.0)
                            .longColumn("humidity", 70)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_builder");
                serverMain.assertSql("select count() from test_builder", "count\n1\n");
                serverMain.assertSql(
                        "select city, temperature, humidity from test_builder",
                        "city\ttemperature\thumidity\nParis\t25.0\t70\n"
                );
            }
        });
    }

    @Test
    public void testBuilderBinaryTransferWithAutoFlushRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Use SenderBuilder with binaryTransfer() and autoFlushRows
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .autoFlushRows(50)  // Auto-flush after 50 rows
                        .build()) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_builder_autoflush")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    // Should have auto-flushed at row 50, flush remaining
                    sender.flush();
                }

                serverMain.awaitTable("test_builder_autoflush");
                serverMain.assertSql("select count() from test_builder_autoflush", "count\n100\n");
            }
        });
    }

    // TLS tests

    @Test
    public void testBuilderBinaryTransferWithTls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int tlsPort = tlsProxy.getListeningPort();

                // Use SenderBuilder with binaryTransfer() and TLS
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + tlsPort)
                        .binaryTransfer()
                        .enableTls()
                        .advancedTls()
                        .disableCertificateValidation()
                        .build()) {
                    sender.table("test_tls")
                            .symbol("city", "Berlin")
                            .doubleColumn("temperature", 20.0)
                            .longColumn("humidity", 60)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_tls");
                serverMain.assertSql("select count() from test_tls", "count\n1\n");
                serverMain.assertSql(
                        "select city, temperature, humidity from test_tls",
                        "city\ttemperature\thumidity\nBerlin\t20.0\t60\n"
                );
            }
        });
    }

    // Config string tests

    @Test
    public void testConfigStringBinaryTransfer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Use fromConfig() with binary_transfer=on
                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + httpPort + ";binary_transfer=on;")) {
                    sender.table("test_config")
                            .symbol("city", "Tokyo")
                            .doubleColumn("temperature", 30.0)
                            .longColumn("humidity", 80)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_config");
                serverMain.assertSql("select count() from test_config", "count\n1\n");
                serverMain.assertSql(
                        "select city, temperature, humidity from test_config",
                        "city\ttemperature\thumidity\nTokyo\t30.0\t80\n"
                );
            }
        });
    }

    @Test
    public void testConfigStringBinaryTransferWithTls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "2048"
            )) {
                int tlsPort = tlsProxy.getListeningPort();

                // Use fromConfig() with binary_transfer=on and TLS
                try (Sender sender = Sender.fromConfig(
                        "https::addr=localhost:" + tlsPort + ";binary_transfer=on;tls_verify=unsafe_off;")) {
                    sender.table("test_config_tls")
                            .symbol("city", "Sydney")
                            .doubleColumn("temperature", 28.0)
                            .longColumn("humidity", 55)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_config_tls");
                serverMain.assertSql("select count() from test_config_tls", "count\n1\n");
            }
        });
    }

    // ==================== Extensive Builder Integration Tests ====================

    @Test
    public void testBuilderAllDataTypes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_all_types")
                            .symbol("sym", "test_symbol")
                            .boolColumn("bool_col", true)
                            .longColumn("long_col", 123456789L)
                            .doubleColumn("double_col", 3.14159)
                            .stringColumn("string_col", "hello world")
                            .timestampColumn("ts_col", 1609459200000000L, ChronoUnit.MICROS)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_all_types");
                serverMain.assertSql("select count() from test_all_types", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderMultipleTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    // Insert into first table
                    sender.table("table_a")
                            .symbol("name", "first")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // Insert into second table
                    sender.table("table_b")
                            .symbol("name", "second")
                            .longColumn("value", 2)
                            .at(1000000000001L, ChronoUnit.MICROS);

                    // Insert into third table
                    sender.table("table_c")
                            .symbol("name", "third")
                            .longColumn("value", 3)
                            .at(1000000000002L, ChronoUnit.MICROS);

                    sender.flush();
                }

                serverMain.awaitTable("table_a");
                serverMain.awaitTable("table_b");
                serverMain.awaitTable("table_c");
                serverMain.assertSql("select count() from table_a", "count\n1\n");
                serverMain.assertSql("select count() from table_b", "count\n1\n");
                serverMain.assertSql("select count() from table_c", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderMultipleSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_multi_sym")
                            .symbol("sym1", "value1")
                            .symbol("sym2", "value2")
                            .symbol("sym3", "value3")
                            .longColumn("data", 100)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_multi_sym");
                serverMain.assertSql(
                        "select sym1, sym2, sym3, data from test_multi_sym",
                        "sym1\tsym2\tsym3\tdata\nvalue1\tvalue2\tvalue3\t100\n"
                );
            }
        });
    }

    @Test
    public void testBuilderLargeStringColumn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                StringBuilder largeString = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    largeString.append("x");
                }

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_large_string")
                            .symbol("id", "row1")
                            .stringColumn("large_data", largeString.toString())
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_large_string");
                serverMain.assertSql("select length(large_data) from test_large_string", "length\n1000\n");
            }
        });
    }

    @Test
    public void testBuilderNegativeNumbers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_negative")
                            .symbol("id", "neg")
                            .longColumn("neg_long", -123456789L)
                            .doubleColumn("neg_double", -3.14159)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_negative");
                serverMain.assertSql(
                        "select neg_long, neg_double from test_negative",
                        "neg_long\tneg_double\n-123456789\t-3.14159\n"
                );
            }
        });
    }

    @Test
    public void testBuilderZeroValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_zero")
                            .symbol("id", "zero")
                            .longColumn("zero_long", 0L)
                            .doubleColumn("zero_double", 0.0)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_zero");
                serverMain.assertSql(
                        "select zero_long, zero_double from test_zero",
                        "zero_long\tzero_double\n0\t0.0\n"
                );
            }
        });
    }

    @Test
    public void testBuilderMaxLongValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_max_long")
                            .symbol("id", "max")
                            .longColumn("max_val", Long.MAX_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_max_long");
                serverMain.assertSql(
                        "select max_val from test_max_long",
                        "max_val\n" + Long.MAX_VALUE + "\n"
                );
            }
        });
    }

    @Test
    public void testBuilderMinLongValue() throws Exception {
        // Note: Long.MIN_VALUE is a null sentinel in QuestDB, so we use MIN_VALUE + 1
        long minNonNull = Long.MIN_VALUE + 1;
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_min_long")
                            .symbol("id", "min")
                            .longColumn("min_val", minNonNull)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_min_long");
                serverMain.assertSql(
                        "select min_val from test_min_long",
                        "min_val\n" + minNonNull + "\n"
                );
            }
        });
    }

    @Test
    public void testBuilderBooleanTrue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_bool_true")
                            .symbol("id", "t")
                            .boolColumn("flag", true)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_bool_true");
                serverMain.assertSql("select flag from test_bool_true", "flag\ntrue\n");
            }
        });
    }

    @Test
    public void testBuilderBooleanFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_bool_false")
                            .symbol("id", "f")
                            .boolColumn("flag", false)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_bool_false");
                serverMain.assertSql("select flag from test_bool_false", "flag\nfalse\n");
            }
        });
    }

    @Test
    public void testBuilderMultipleFlushes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    // First flush
                    sender.table("test_multi_flush")
                            .symbol("batch", "1")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();

                    // Second flush
                    sender.table("test_multi_flush")
                            .symbol("batch", "2")
                            .longColumn("value", 2)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.flush();

                    // Third flush
                    sender.table("test_multi_flush")
                            .symbol("batch", "3")
                            .longColumn("value", 3)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_multi_flush");
                serverMain.assertSql("select count() from test_multi_flush", "count\n3\n");
            }
        });
    }

    @Test
    public void testBuilder100Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_100_rows")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_100_rows");
                serverMain.assertSql("select count() from test_100_rows", "count\n100\n");
                serverMain.assertSql("select sum(value) from test_100_rows", "sum\n4950\n");
            }
        });
    }

    @Test
    public void testBuilder1000Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "262144"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 1000; i++) {
                        sender.table("test_1000_rows")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_1000_rows");
                serverMain.assertSql("select count() from test_1000_rows", "count\n1000\n");
            }
        });
    }

    @Test
    public void testBuilderTimestampMicros() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_micros")
                            .symbol("id", "micros")
                            .longColumn("value", 1)
                            .at(1609459200000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_micros");
                serverMain.assertSql(
                        "select timestamp from test_ts_micros",
                        "timestamp\n2021-01-01T00:00:00.000000Z\n"
                );
            }
        });
    }

    @Test
    public void testBuilderTimestampNanos() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_nanos")
                            .symbol("id", "nanos")
                            .longColumn("value", 1)
                            .at(1609459200000000000L, ChronoUnit.NANOS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_nanos");
                serverMain.assertSql(
                        "select timestamp from test_ts_nanos",
                        "timestamp\n2021-01-01T00:00:00.000000000Z\n"
                );
            }
        });
    }

    @Test
    public void testBuilderEmptySymbolValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_empty_sym")
                            .symbol("empty", "")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_empty_sym");
                serverMain.assertSql("select count() from test_empty_sym", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderEmptyStringValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_empty_str")
                            .symbol("id", "row1")
                            .stringColumn("empty_str", "")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_empty_str");
                serverMain.assertSql("select count() from test_empty_str", "count\n1\n");
                serverMain.assertSql("select length(empty_str) from test_empty_str", "length\n0\n");
            }
        });
    }

    @Test
    public void testBuilderSpecialCharactersInSymbol() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_special_sym")
                            .symbol("special", "hello-world_123")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_special_sym");
                serverMain.assertSql(
                        "select special from test_special_sym",
                        "special\nhello-world_123\n"
                );
            }
        });
    }

    @Test
    public void testBuilderSpecialCharactersInString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_special_str")
                            .symbol("id", "row1")
                            .stringColumn("special", "hello\tworld\nnewline")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_special_str");
                serverMain.assertSql("select count() from test_special_str", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderUnicodeInString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_unicode")
                            .symbol("id", "row1")
                            .stringColumn("unicode_str", "こんにちは世界")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_unicode");
                serverMain.assertSql("select count() from test_unicode", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleNaN() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_nan")
                            .symbol("id", "nan")
                            .doubleColumn("nan_val", Double.NaN)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_nan");
                serverMain.assertSql("select count() from test_nan", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleInfinity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_inf")
                            .symbol("id", "pos_inf")
                            .doubleColumn("inf_val", Double.POSITIVE_INFINITY)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.table("test_inf")
                            .symbol("id", "neg_inf")
                            .doubleColumn("inf_val", Double.NEGATIVE_INFINITY)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_inf");
                serverMain.assertSql("select count() from test_inf", "count\n2\n");
            }
        });
    }

    @Test
    public void testBuilderAutoFlushRows10() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .autoFlushRows(10)
                        .build()) {
                    // Insert 25 rows, should auto-flush at 10 and 20
                    for (int i = 0; i < 25; i++) {
                        sender.table("test_autoflush_10")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush(); // Flush remaining 5
                }

                serverMain.awaitTable("test_autoflush_10");
                serverMain.assertSql("select count() from test_autoflush_10", "count\n25\n");
            }
        });
    }

    @Test
    public void testBuilderSameTableMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("test_same_table")
                                .symbol("type", i % 2 == 0 ? "even" : "odd")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_same_table");
                serverMain.assertSql("select count() from test_same_table", "count\n50\n");
                serverMain.assertSql("select count() from test_same_table where type = 'even'", "count\n25\n");
                serverMain.assertSql("select count() from test_same_table where type = 'odd'", "count\n25\n");
            }
        });
    }

    @Test
    public void testBuilderFlushEmpty() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
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
    public void testBuilderInterleavedTables() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    // Interleave rows between tables
                    for (int i = 0; i < 20; i++) {
                        sender.table("interleave_a")
                                .symbol("id", "a" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 2, ChronoUnit.MICROS);

                        sender.table("interleave_b")
                                .symbol("id", "b" + i)
                                .longColumn("value", i * 10)
                                .at(1000000000000L + i * 2 + 1, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("interleave_a");
                serverMain.awaitTable("interleave_b");
                serverMain.assertSql("select count() from interleave_a", "count\n20\n");
                serverMain.assertSql("select count() from interleave_b", "count\n20\n");
            }
        });
    }

    @Test
    public void testBuilderSmallDoubleAsFloat() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_small_double")
                            .symbol("id", "f1")
                            .doubleColumn("float_val", 3.14)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_small_double");
                serverMain.assertSql("select count() from test_small_double", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderByteRangeLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_byte_range")
                            .symbol("id", "b1")
                            .longColumn("byte_val", 127)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_byte_range");
                serverMain.assertSql("select byte_val from test_byte_range", "byte_val\n127\n");
            }
        });
    }

    @Test
    public void testBuilderShortRangeLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_short_range")
                            .symbol("id", "s1")
                            .longColumn("short_val", 32767)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_short_range");
                serverMain.assertSql("select short_val from test_short_range", "short_val\n32767\n");
            }
        });
    }

    @Test
    public void testBuilderIntRangeLong() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_int_range")
                            .symbol("id", "i1")
                            .longColumn("int_val", Integer.MAX_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_int_range");
                serverMain.assertSql("select int_val from test_int_range", "int_val\n" + Integer.MAX_VALUE + "\n");
            }
        });
    }

    // =====================================================================
    // Additional extensive tests - Multiple column combinations
    // =====================================================================

    @Test
    public void testBuilderAllColumnTypesInOneRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_all_types")
                            .symbol("sym", "s1")
                            .stringColumn("str", "hello")
                            .longColumn("lng", 123L)
                            .doubleColumn("dbl", 3.14)
                            .boolColumn("bool", true)
                            .timestampColumn("ts", 1000000L, ChronoUnit.MICROS)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_all_types");
                serverMain.assertSql("select count() from test_all_types", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTwoLongColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_two_longs")
                            .longColumn("a", 100L)
                            .longColumn("b", 200L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_two_longs");
                serverMain.assertSql("select a, b from test_two_longs", "a\tb\n100\t200\n");
            }
        });
    }

    @Test
    public void testBuilderThreeDoubleColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_three_doubles")
                            .doubleColumn("x", 1.1)
                            .doubleColumn("y", 2.2)
                            .doubleColumn("z", 3.3)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_three_doubles");
                serverMain.assertSql("select count() from test_three_doubles", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTwoStringColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_two_strings")
                            .stringColumn("first", "hello")
                            .stringColumn("second", "world")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_two_strings");
                serverMain.assertSql("select first, second from test_two_strings", "first\tsecond\nhello\tworld\n");
            }
        });
    }

    @Test
    public void testBuilderTwoBoolColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_two_bools")
                            .boolColumn("enabled", true)
                            .boolColumn("active", false)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_two_bools");
                serverMain.assertSql("select enabled, active from test_two_bools", "enabled\tactive\ntrue\tfalse\n");
            }
        });
    }

    @Test
    public void testBuilderFiveSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_five_syms")
                            .symbol("a", "1")
                            .symbol("b", "2")
                            .symbol("c", "3")
                            .symbol("d", "4")
                            .symbol("e", "5")
                            .longColumn("value", 42)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_five_syms");
                serverMain.assertSql("select a,b,c,d,e from test_five_syms", "a\tb\tc\td\te\n1\t2\t3\t4\t5\n");
            }
        });
    }

    @Test
    public void testBuilderMixedSymbolsAndLongs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_mixed")
                            .symbol("sym1", "a")
                            .symbol("sym2", "b")
                            .longColumn("val1", 10)
                            .longColumn("val2", 20)
                            .longColumn("val3", 30)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_mixed");
                serverMain.assertSql("select val1+val2+val3 as total from test_mixed", "total\n60\n");
            }
        });
    }

    // =====================================================================
    // Row batch tests
    // =====================================================================

    @Test
    public void testBuilder5Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("test_5rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_5rows");
                serverMain.assertSql("select count() from test_5rows", "count\n5\n");
            }
        });
    }

    @Test
    public void testBuilder10Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("test_10rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_10rows");
                serverMain.assertSql("select count() from test_10rows", "count\n10\n");
            }
        });
    }

    @Test
    public void testBuilder50Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("test_50rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_50rows");
                serverMain.assertSql("select count() from test_50rows", "count\n50\n");
            }
        });
    }

    @Test
    public void testBuilder500Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 500; i++) {
                        sender.table("test_500rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_500rows");
                serverMain.assertSql("select count() from test_500rows", "count\n500\n");
            }
        });
    }

    @Test
    public void testBuilder5000Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 5000; i++) {
                        sender.table("test_5000rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_5000rows");
                serverMain.assertSql("select count() from test_5000rows", "count\n5000\n");
            }
        });
    }

    // =====================================================================
    // Flush behavior tests
    // =====================================================================

    @Test
    public void testBuilderFlushAfterEveryRow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("test_flush_each")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        sender.flush();
                    }
                }

                serverMain.awaitTable("test_flush_each");
                serverMain.assertSql("select count() from test_flush_each", "count\n10\n");
            }
        });
    }

    @Test
    public void testBuilderFlushEvery5Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("test_flush_5")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        if ((i + 1) % 5 == 0) {
                            sender.flush();
                        }
                    }
                    sender.flush(); // flush remaining
                }

                serverMain.awaitTable("test_flush_5");
                serverMain.assertSql("select count() from test_flush_5", "count\n50\n");
            }
        });
    }

    @Test
    public void testBuilderFlushEvery10Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_flush_10")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        if ((i + 1) % 10 == 0) {
                            sender.flush();
                        }
                    }
                }

                serverMain.awaitTable("test_flush_10");
                serverMain.assertSql("select count() from test_flush_10", "count\n100\n");
            }
        });
    }

    // =====================================================================
    // Double value edge cases
    // =====================================================================

    @Test
    public void testBuilderDoubleZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_zero")
                            .doubleColumn("val", 0.0)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_zero");
                serverMain.assertSql("select val from test_dbl_zero", "val\n0.0\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleNegativeZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_neg_zero")
                            .doubleColumn("val", -0.0)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_neg_zero");
                // IEEE 754 -0.0 is preserved by QuestDB
                serverMain.assertSql("select count() from test_dbl_neg_zero", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleSmallPositive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_small")
                            .doubleColumn("val", 0.000001)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_small");
                serverMain.assertSql("select count() from test_dbl_small", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleSmallNegative() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_small_neg")
                            .doubleColumn("val", -0.000001)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_small_neg");
                serverMain.assertSql("select count() from test_dbl_small_neg", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleLargePositive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_large")
                            .doubleColumn("val", 1.0e100)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_large");
                serverMain.assertSql("select count() from test_dbl_large", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleLargeNegative() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_large_neg")
                            .doubleColumn("val", -1.0e100)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_large_neg");
                serverMain.assertSql("select count() from test_dbl_large_neg", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleMinValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_min")
                            .doubleColumn("val", Double.MIN_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_min");
                serverMain.assertSql("select count() from test_dbl_min", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleMaxValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_dbl_max")
                            .doubleColumn("val", Double.MAX_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_dbl_max");
                serverMain.assertSql("select count() from test_dbl_max", "count\n1\n");
            }
        });
    }

    // =====================================================================
    // Long value edge cases
    // =====================================================================

    @Test
    public void testBuilderLongZero() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_long_zero")
                            .longColumn("val", 0L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_long_zero");
                serverMain.assertSql("select val from test_long_zero", "val\n0\n");
            }
        });
    }

    @Test
    public void testBuilderLongOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_long_one")
                            .longColumn("val", 1L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_long_one");
                serverMain.assertSql("select val from test_long_one", "val\n1\n");
            }
        });
    }

    @Test
    public void testBuilderLongNegativeOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_long_neg1")
                            .longColumn("val", -1L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_long_neg1");
                serverMain.assertSql("select val from test_long_neg1", "val\n-1\n");
            }
        });
    }

    @Test
    public void testBuilderLong1000() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_long_1000")
                            .longColumn("val", 1000L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_long_1000");
                serverMain.assertSql("select val from test_long_1000", "val\n1000\n");
            }
        });
    }

    @Test
    public void testBuilderLongNegative1000() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_long_neg1000")
                            .longColumn("val", -1000L)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_long_neg1000");
                serverMain.assertSql("select val from test_long_neg1000", "val\n-1000\n");
            }
        });
    }

    // =====================================================================
    // String value edge cases
    // =====================================================================

    @Test
    public void testBuilderStringSingleChar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_single")
                            .stringColumn("val", "a")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_single");
                serverMain.assertSql("select val from test_str_single", "val\na\n");
            }
        });
    }

    @Test
    public void testBuilderStringWithSpaces() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_spaces")
                            .stringColumn("val", "hello world")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_spaces");
                serverMain.assertSql("select val from test_str_spaces", "val\nhello world\n");
            }
        });
    }

    @Test
    public void testBuilderStringWithTabs() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_tabs")
                            .stringColumn("val", "a\tb\tc")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_tabs");
                serverMain.assertSql("select count() from test_str_tabs", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringWithNewlines() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_newlines")
                            .stringColumn("val", "line1\nline2")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_newlines");
                serverMain.assertSql("select count() from test_str_newlines", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringWithQuotes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_quotes")
                            .stringColumn("val", "say \"hello\"")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_quotes");
                serverMain.assertSql("select count() from test_str_quotes", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringWithBackslash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_backslash")
                            .stringColumn("val", "path\\to\\file")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_backslash");
                serverMain.assertSql("select count() from test_str_backslash", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringNumeric() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_numeric")
                            .stringColumn("val", "12345")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_numeric");
                serverMain.assertSql("select val from test_str_numeric", "val\n12345\n");
            }
        });
    }

    @Test
    public void testBuilderString100Chars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 100; i++) {
                    sb.append('x');
                }
                String longStr = sb.toString();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_100")
                            .stringColumn("val", longStr)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_100");
                serverMain.assertSql("select length(val) from test_str_100", "length\n100\n");
            }
        });
    }

    // =====================================================================
    // Symbol edge cases
    // =====================================================================

    @Test
    public void testBuilderSymbolSingleChar() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_sym_single")
                            .symbol("s", "x")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_sym_single");
                serverMain.assertSql("select s from test_sym_single", "s\nx\n");
            }
        });
    }

    @Test
    public void testBuilderSymbolNumeric() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_sym_num")
                            .symbol("s", "123")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_sym_num");
                serverMain.assertSql("select s from test_sym_num", "s\n123\n");
            }
        });
    }

    @Test
    public void testBuilderSymbolMixedCase() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_sym_mixed")
                            .symbol("s", "AbCdEf")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_sym_mixed");
                serverMain.assertSql("select s from test_sym_mixed", "s\nAbCdEf\n");
            }
        });
    }

    @Test
    public void testBuilderSymbolWithUnderscore() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_sym_underscore")
                            .symbol("s", "hello_world")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_sym_underscore");
                serverMain.assertSql("select s from test_sym_underscore", "s\nhello_world\n");
            }
        });
    }

    @Test
    public void testBuilderSymbolWithDash() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_sym_dash")
                            .symbol("s", "hello-world")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_sym_dash");
                serverMain.assertSql("select s from test_sym_dash", "s\nhello-world\n");
            }
        });
    }

    // =====================================================================
    // Timestamp edge cases
    // =====================================================================

    @Test
    public void testBuilderTimestampZeroMicros() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_zero")
                            .longColumn("val", 1)
                            .at(0L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_zero");
                serverMain.assertSql("select timestamp from test_ts_zero", "timestamp\n1970-01-01T00:00:00.000000Z\n");
            }
        });
    }

    @Test
    public void testBuilderTimestampOneMicro() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_one")
                            .longColumn("val", 1)
                            .at(1L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_one");
                serverMain.assertSql("select timestamp from test_ts_one", "timestamp\n1970-01-01T00:00:00.000001Z\n");
            }
        });
    }

    @Test
    public void testBuilderTimestampOneSecond() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_1sec")
                            .longColumn("val", 1)
                            .at(1000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_1sec");
                serverMain.assertSql("select timestamp from test_ts_1sec", "timestamp\n1970-01-01T00:00:01.000000Z\n");
            }
        });
    }

    @Test
    public void testBuilderTimestampOneMinute() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_1min")
                            .longColumn("val", 1)
                            .at(60000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_1min");
                serverMain.assertSql("select timestamp from test_ts_1min", "timestamp\n1970-01-01T00:01:00.000000Z\n");
            }
        });
    }

    @Test
    public void testBuilderTimestampYear2020() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // 2020-01-01 00:00:00 UTC in micros
                long ts2020 = 1577836800000000L;
                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_2020")
                            .longColumn("val", 1)
                            .at(ts2020, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_2020");
                serverMain.assertSql("select timestamp from test_ts_2020", "timestamp\n2020-01-01T00:00:00.000000Z\n");
            }
        });
    }

    // =====================================================================
    // Multiple table interleaving tests
    // =====================================================================

    @Test
    public void testBuilderTwoTablesInterleaved() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("table_a")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        sender.table("table_b")
                                .longColumn("val", i * 2)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("table_a");
                serverMain.awaitTable("table_b");
                serverMain.assertSql("select count() from table_a", "count\n10\n");
                serverMain.assertSql("select count() from table_b", "count\n10\n");
            }
        });
    }

    @Test
    public void testBuilderThreeTablesInterleaved() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("tbl_x")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        sender.table("tbl_y")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                        sender.table("tbl_z")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("tbl_x");
                serverMain.awaitTable("tbl_y");
                serverMain.awaitTable("tbl_z");
                serverMain.assertSql("select count() from tbl_x", "count\n10\n");
                serverMain.assertSql("select count() from tbl_y", "count\n10\n");
                serverMain.assertSql("select count() from tbl_z", "count\n10\n");
            }
        });
    }

    // =====================================================================
    // Auto-flush configuration tests
    // =====================================================================

    @Test
    public void testBuilderAutoFlush1Row() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .autoFlushRows(1)
                        .build()) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("test_auto1")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("test_auto1");
                serverMain.assertSql("select count() from test_auto1", "count\n5\n");
            }
        });
    }

    @Test
    public void testBuilderAutoFlush2Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .autoFlushRows(2)
                        .build()) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("test_auto2")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("test_auto2");
                serverMain.assertSql("select count() from test_auto2", "count\n10\n");
            }
        });
    }

    @Test
    public void testBuilderAutoFlush5Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .autoFlushRows(5)
                        .build()) {
                    for (int i = 0; i < 25; i++) {
                        sender.table("test_auto5")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("test_auto5");
                serverMain.assertSql("select count() from test_auto5", "count\n25\n");
            }
        });
    }

    @Test
    public void testBuilderAutoFlush50Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .autoFlushRows(50)
                        .build()) {
                    for (int i = 0; i < 200; i++) {
                        sender.table("test_auto50")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("test_auto50");
                serverMain.assertSql("select count() from test_auto50", "count\n200\n");
            }
        });
    }

    @Test
    public void testBuilderAutoFlush100Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .autoFlushRows(100)
                        .build()) {
                    for (int i = 0; i < 500; i++) {
                        sender.table("test_auto100")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("test_auto100");
                serverMain.assertSql("select count() from test_auto100", "count\n500\n");
            }
        });
    }

    // =====================================================================
    // Config string tests for binary transfer
    // =====================================================================

    @Test
    public void testBuilderConfigStringBinaryTransferOn() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + httpPort + ";binary_transfer=on;")) {
                    sender.table("test_config_on")
                            .longColumn("val", 42)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_config_on");
                serverMain.assertSql("select val from test_config_on", "val\n42\n");
            }
        });
    }

    @Test
    public void testBuilderConfigStringBinaryTransferOff() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + httpPort + ";binary_transfer=off;")) {
                    sender.table("test_config_off")
                            .longColumn("val", 42)
                            .atNow();
                }

                serverMain.awaitTable("test_config_off");
                serverMain.assertSql("select val from test_config_off", "val\n42\n");
            }
        });
    }

    @Test
    public void testBuilderConfigStringWithAutoFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.fromConfig("http::addr=localhost:" + httpPort + ";binary_transfer=on;auto_flush_rows=5;")) {
                    for (int i = 0; i < 20; i++) {
                        sender.table("test_config_auto")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                }

                serverMain.awaitTable("test_config_auto");
                serverMain.assertSql("select count() from test_config_auto", "count\n20\n");
            }
        });
    }

    // =====================================================================
    // Unicode tests
    // =====================================================================

    @Test
    public void testBuilderStringUnicodeEmoji() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_emoji")
                            .stringColumn("val", "\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_emoji");
                serverMain.assertSql("select count() from test_emoji", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringUnicodeChinese() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_chinese")
                            .stringColumn("val", "\u4e2d\u6587\u6d4b\u8bd5")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_chinese");
                serverMain.assertSql("select count() from test_chinese", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringUnicodeJapanese() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_japanese")
                            .stringColumn("val", "\u65e5\u672c\u8a9e")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_japanese");
                serverMain.assertSql("select count() from test_japanese", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringUnicodeArabic() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_arabic")
                            .stringColumn("val", "\u0627\u0644\u0639\u0631\u0628\u064a\u0629")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_arabic");
                serverMain.assertSql("select count() from test_arabic", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderStringUnicodeRussian() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_russian")
                            .stringColumn("val", "\u0420\u0443\u0441\u0441\u043a\u0438\u0439")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_russian");
                serverMain.assertSql("select count() from test_russian", "count\n1\n");
            }
        });
    }

    // =====================================================================
    // Sum and aggregate verification tests
    // =====================================================================

    @Test
    public void testBuilderSumOf10Values() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 1; i <= 10; i++) {
                        sender.table("test_sum10")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_sum10");
                serverMain.assertSql("select sum(val) from test_sum10", "sum\n55\n");
            }
        });
    }

    @Test
    public void testBuilderSumOf100Values() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 1; i <= 100; i++) {
                        sender.table("test_sum100")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_sum100");
                serverMain.assertSql("select sum(val) from test_sum100", "sum\n5050\n");
            }
        });
    }

    @Test
    public void testBuilderMinMax() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_minmax")
                            .longColumn("val", 100)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.table("test_minmax")
                            .longColumn("val", 50)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.table("test_minmax")
                            .longColumn("val", 200)
                            .at(1000000000003L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_minmax");
                serverMain.assertSql("select min(val), max(val) from test_minmax", "min\tmax\n50\t200\n");
            }
        });
    }

    @Test
    public void testBuilderAverage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_avg")
                            .longColumn("val", 10)
                            .at(1000000000001L, ChronoUnit.MICROS);
                    sender.table("test_avg")
                            .longColumn("val", 20)
                            .at(1000000000002L, ChronoUnit.MICROS);
                    sender.table("test_avg")
                            .longColumn("val", 30)
                            .at(1000000000003L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_avg");
                serverMain.assertSql("select avg(val) from test_avg", "avg\n20.0\n");
            }
        });
    }

    // =====================================================================
    // More row count tests
    // =====================================================================

    @Test
    public void testBuilder10000Rows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 10000; i++) {
                        sender.table("test_10000rows")
                                .longColumn("value", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_10000rows");
                serverMain.assertSql("select count() from test_10000rows", "count\n10000\n");
            }
        });
    }

    @Test
    public void testBuilderMultipleFlushes50Each() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int batch = 0; batch < 10; batch++) {
                        for (int i = 0; i < 50; i++) {
                            sender.table("test_multi_flush")
                                    .longColumn("value", batch * 50 + i)
                                    .at(1000000000000L + batch * 50 + i, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("test_multi_flush");
                serverMain.assertSql("select count() from test_multi_flush", "count\n500\n");
            }
        });
    }

    // =====================================================================
    // Multiple data columns tests
    // =====================================================================

    @Test
    public void testBuilderTenLongColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ten_longs")
                            .longColumn("c1", 1)
                            .longColumn("c2", 2)
                            .longColumn("c3", 3)
                            .longColumn("c4", 4)
                            .longColumn("c5", 5)
                            .longColumn("c6", 6)
                            .longColumn("c7", 7)
                            .longColumn("c8", 8)
                            .longColumn("c9", 9)
                            .longColumn("c10", 10)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ten_longs");
                serverMain.assertSql("select c1+c2+c3+c4+c5+c6+c7+c8+c9+c10 as total from test_ten_longs", "total\n55\n");
            }
        });
    }

    @Test
    public void testBuilderTenStringColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ten_strings")
                            .stringColumn("s1", "a")
                            .stringColumn("s2", "b")
                            .stringColumn("s3", "c")
                            .stringColumn("s4", "d")
                            .stringColumn("s5", "e")
                            .stringColumn("s6", "f")
                            .stringColumn("s7", "g")
                            .stringColumn("s8", "h")
                            .stringColumn("s9", "i")
                            .stringColumn("s10", "j")
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ten_strings");
                serverMain.assertSql("select count() from test_ten_strings", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTenDoubleColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ten_doubles")
                            .doubleColumn("d1", 1.1)
                            .doubleColumn("d2", 2.2)
                            .doubleColumn("d3", 3.3)
                            .doubleColumn("d4", 4.4)
                            .doubleColumn("d5", 5.5)
                            .doubleColumn("d6", 6.6)
                            .doubleColumn("d7", 7.7)
                            .doubleColumn("d8", 8.8)
                            .doubleColumn("d9", 9.9)
                            .doubleColumn("d10", 10.0)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ten_doubles");
                serverMain.assertSql("select count() from test_ten_doubles", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTenBoolColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ten_bools")
                            .boolColumn("b1", true)
                            .boolColumn("b2", false)
                            .boolColumn("b3", true)
                            .boolColumn("b4", false)
                            .boolColumn("b5", true)
                            .boolColumn("b6", false)
                            .boolColumn("b7", true)
                            .boolColumn("b8", false)
                            .boolColumn("b9", true)
                            .boolColumn("b10", false)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ten_bools");
                serverMain.assertSql("select count() from test_ten_bools", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTenSymbols() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ten_syms")
                            .symbol("s1", "v1")
                            .symbol("s2", "v2")
                            .symbol("s3", "v3")
                            .symbol("s4", "v4")
                            .symbol("s5", "v5")
                            .symbol("s6", "v6")
                            .symbol("s7", "v7")
                            .symbol("s8", "v8")
                            .symbol("s9", "v9")
                            .symbol("s10", "v10")
                            .longColumn("val", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ten_syms");
                serverMain.assertSql("select count() from test_ten_syms", "count\n1\n");
            }
        });
    }

    // =====================================================================
    // String length edge cases
    // =====================================================================

    @Test
    public void testBuilderString500Chars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 500; i++) {
                    sb.append('a');
                }
                String str = sb.toString();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_500")
                            .stringColumn("val", str)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_500");
                serverMain.assertSql("select length(val) from test_str_500", "length\n500\n");
            }
        });
    }

    @Test
    public void testBuilderString1000Chars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 1000; i++) {
                    sb.append('b');
                }
                String str = sb.toString();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_1000")
                            .stringColumn("val", str)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_1000");
                serverMain.assertSql("select length(val) from test_str_1000", "length\n1000\n");
            }
        });
    }

    @Test
    public void testBuilderString5000Chars() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 5000; i++) {
                    sb.append('c');
                }
                String str = sb.toString();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_str_5000")
                            .stringColumn("val", str)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_str_5000");
                serverMain.assertSql("select length(val) from test_str_5000", "length\n5000\n");
            }
        });
    }

    // =====================================================================
    // Different symbol cardinality tests
    // =====================================================================

    @Test
    public void testBuilderSymbol100DistinctValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_sym_100")
                                .symbol("sym", "value_" + i)
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_sym_100");
                serverMain.assertSql("select count_distinct(sym) from test_sym_100", "count_distinct\n100\n");
            }
        });
    }

    @Test
    public void testBuilderSymbolSameValue100Times() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_sym_same")
                                .symbol("sym", "constant")
                                .longColumn("val", i)
                                .at(1000000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_sym_same");
                serverMain.assertSql("select count_distinct(sym) from test_sym_same", "count_distinct\n1\n");
            }
        });
    }

    // =====================================================================
    // Timestamp column tests
    // =====================================================================

    @Test
    public void testBuilderTwoTimestampColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_two_ts")
                            .timestampColumn("ts1", 1000000L, ChronoUnit.MICROS)
                            .timestampColumn("ts2", 2000000L, ChronoUnit.MICROS)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_two_ts");
                serverMain.assertSql("select count() from test_two_ts", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTimestampWithInstant() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_ts_instant")
                            .timestampColumn("ts", java.time.Instant.parse("2021-01-01T00:00:00Z"))
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_ts_instant");
                serverMain.assertSql("select count() from test_ts_instant", "count\n1\n");
            }
        });
    }

    // =====================================================================
    // Table name edge cases
    // =====================================================================

    @Test
    public void testBuilderTableNameShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("t")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("t");
                serverMain.assertSql("select count() from t", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTableNameWithUnderscore() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("my_table_name")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("my_table_name");
                serverMain.assertSql("select count() from my_table_name", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderTableNameWithNumbers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("table123")
                            .longColumn("v", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("table123");
                serverMain.assertSql("select count() from table123", "count\n1\n");
            }
        });
    }

    // =====================================================================
    // Column name edge cases
    // =====================================================================

    @Test
    public void testBuilderColumnNameShort() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_col_short")
                            .longColumn("x", 42)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_col_short");
                serverMain.assertSql("select x from test_col_short", "x\n42\n");
            }
        });
    }

    @Test
    public void testBuilderColumnNameWithUnderscore() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_col_underscore")
                            .longColumn("my_column", 42)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_col_underscore");
                serverMain.assertSql("select my_column from test_col_underscore", "my_column\n42\n");
            }
        });
    }

    // =====================================================================
    // More double precision tests
    // =====================================================================

    @Test
    public void testBuilderDoublePi() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_pi")
                            .doubleColumn("val", Math.PI)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_pi");
                serverMain.assertSql("select count() from test_pi", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleE() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_e")
                            .doubleColumn("val", Math.E)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_e");
                serverMain.assertSql("select count() from test_e", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDoubleSqrt2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_sqrt2")
                            .doubleColumn("val", Math.sqrt(2))
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_sqrt2");
                serverMain.assertSql("select count() from test_sqrt2", "count\n1\n");
            }
        });
    }

    // =====================================================================
    // Boolean combinations
    // =====================================================================

    @Test
    public void testBuilderFiveBoolsAllTrue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_bools_true")
                            .boolColumn("a", true)
                            .boolColumn("b", true)
                            .boolColumn("c", true)
                            .boolColumn("d", true)
                            .boolColumn("e", true)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_bools_true");
                serverMain.assertSql("select a,b,c,d,e from test_bools_true", "a\tb\tc\td\te\ntrue\ttrue\ttrue\ttrue\ttrue\n");
            }
        });
    }

    @Test
    public void testBuilderFiveBoolsAllFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_bools_false")
                            .boolColumn("a", false)
                            .boolColumn("b", false)
                            .boolColumn("c", false)
                            .boolColumn("d", false)
                            .boolColumn("e", false)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_bools_false");
                serverMain.assertSql("select a,b,c,d,e from test_bools_false", "a\tb\tc\td\te\nfalse\tfalse\tfalse\tfalse\tfalse\n");
            }
        });
    }

    // =====================================================================
    // Complex schema tests
    // =====================================================================

    @Test
    public void testBuilderComplexSchema1() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("complex1")
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

                serverMain.awaitTable("complex1");
                serverMain.assertSql("select region,host,dc,cpu,mem from complex1",
                        "region\thost\tdc\tcpu\tmem\nus-east\tserver-01\tdc1\t50\t80\n");
            }
        });
    }

    @Test
    public void testBuilderComplexSchema2() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("complex2")
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

                serverMain.awaitTable("complex2");
                serverMain.assertSql("select type,unit,sensor_id from complex2",
                        "type\tunit\tsensor_id\ntemperature\tcelsius\tt-001\n");
            }
        });
    }

    @Test
    public void testBuilderComplexSchemaMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 50; i++) {
                        sender.table("complex_multi")
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

                serverMain.awaitTable("complex_multi");
                serverMain.assertSql("select count() from complex_multi", "count\n50\n");
            }
        });
    }

    // =====================================================================
    // atNow tests with binary transfer
    // =====================================================================

    @Test
    public void testBuilderAtNowSimple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_atnow")
                            .longColumn("val", 42)
                            .atNow();
                    sender.flush();
                }

                serverMain.awaitTable("test_atnow");
                serverMain.assertSql("select count() from test_atnow", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderAtNowMultiple() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("test_atnow_multi")
                                .longColumn("val", i)
                                .atNow();
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_atnow_multi");
                serverMain.assertSql("select count() from test_atnow_multi", "count\n10\n");
            }
        });
    }

    // ==================== Array Tests ====================
    // NOTE: These tests require server-side ILP v4 parser updates to handle array types (0x11, 0x12).
    // The client-side implementation is complete, but server-side support is not yet available.
    // The tests are disabled until server-side support is added.

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilder1DDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_1d_double_array")
                            .doubleArray("values", new double[]{1.1, 2.2, 3.3, 4.4, 5.5})
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_1d_double_array");
                serverMain.assertSql("select count() from test_1d_double_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilder1DLongArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_1d_long_array")
                            .longArray("values", new long[]{100, 200, 300, 400, 500})
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_1d_long_array");
                serverMain.assertSql("select count() from test_1d_long_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilder2DDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    double[][] matrix = {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};
                    sender.table("test_2d_double_array")
                            .doubleArray("matrix", matrix)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_2d_double_array");
                serverMain.assertSql("select count() from test_2d_double_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilder2DLongArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    long[][] matrix = {{1, 2, 3}, {4, 5, 6}};
                    sender.table("test_2d_long_array")
                            .longArray("matrix", matrix)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_2d_long_array");
                serverMain.assertSql("select count() from test_2d_long_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilder3DDoubleArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    double[][][] cube = {{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}};
                    sender.table("test_3d_double_array")
                            .doubleArray("cube", cube)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_3d_double_array");
                serverMain.assertSql("select count() from test_3d_double_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilder3DLongArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    long[][][] cube = {{{1, 2}, {3, 4}}, {{5, 6}, {7, 8}}};
                    sender.table("test_3d_long_array")
                            .longArray("cube", cube)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_3d_long_array");
                serverMain.assertSql("select count() from test_3d_long_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilderArrayWithScalarColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_mixed_array")
                            .symbol("location", "NYC")
                            .doubleArray("readings", new double[]{1.1, 2.2, 3.3})
                            .longColumn("count", 42)
                            .stringColumn("status", "OK")
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_mixed_array");
                serverMain.assertSql("select count() from test_mixed_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilderMultipleArrayRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_multi_array")
                                .doubleArray("values", new double[]{i * 1.0, i * 2.0, i * 3.0})
                                .at(1000000000L + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                serverMain.awaitTable("test_multi_array");
                serverMain.assertSql("select count() from test_multi_array", "count\n100\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilderEmptyArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_empty_array")
                            .doubleArray("values", new double[]{})
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_empty_array");
                serverMain.assertSql("select count() from test_empty_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilderNullArraySkipped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_null_array")
                            .doubleArray("values", (double[]) null)
                            .longColumn("marker", 1)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_null_array");
                serverMain.assertSql("select count() from test_null_array", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support array types yet")
    public void testBuilderLargeArray() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    double[] largeArray = new double[1000];
                    for (int i = 0; i < largeArray.length; i++) {
                        largeArray[i] = i * 0.1;
                    }
                    sender.table("test_large_array")
                            .doubleArray("values", largeArray)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_large_array");
                serverMain.assertSql("select count() from test_large_array", "count\n1\n");
            }
        });
    }

    // ==================== Decimal column tests ====================

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimal64() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    io.questdb.std.Decimal64 value = new io.questdb.std.Decimal64(12345, 2); // 123.45
                    sender.table("test_decimal64")
                            .decimalColumn("price", value)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal64");
                serverMain.assertSql("select count() from test_decimal64", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimal128() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    io.questdb.std.Decimal128 value = new io.questdb.std.Decimal128(0, 12345678901234L, 2); // 123456789012.34
                    sender.table("test_decimal128")
                            .decimalColumn("amount", value)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal128");
                serverMain.assertSql("select count() from test_decimal128", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimal256() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    io.questdb.std.Decimal256 value = io.questdb.std.Decimal256.fromLong(123456789L, 4);
                    sender.table("test_decimal256")
                            .decimalColumn("big_value", value)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal256");
                serverMain.assertSql("select count() from test_decimal256", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimalFromString() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    sender.table("test_decimal_string")
                            .decimalColumn("price", "123.456789")
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal_string");
                serverMain.assertSql("select count() from test_decimal_string", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimalMultipleRows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    // Same scale for all values in the column
                    io.questdb.std.Decimal64 v1 = new io.questdb.std.Decimal64(100, 2); // 1.00
                    io.questdb.std.Decimal64 v2 = new io.questdb.std.Decimal64(200, 2); // 2.00
                    io.questdb.std.Decimal64 v3 = new io.questdb.std.Decimal64(300, 2); // 3.00

                    sender.table("test_decimal_multi")
                            .decimalColumn("price", v1)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_multi")
                            .decimalColumn("price", v2)
                            .at(2000000000L, ChronoUnit.MICROS);
                    sender.table("test_decimal_multi")
                            .decimalColumn("price", v3)
                            .at(3000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal_multi");
                serverMain.assertSql("select count() from test_decimal_multi", "count\n3\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimalWithScalarColumns() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    io.questdb.std.Decimal64 price = new io.questdb.std.Decimal64(9999, 2); // 99.99
                    sender.table("test_decimal_mixed")
                            .symbol("product", "Widget")
                            .longColumn("quantity", 10)
                            .decimalColumn("price", price)
                            .doubleColumn("discount", 0.1)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal_mixed");
                serverMain.assertSql("select count() from test_decimal_mixed", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimalNullSkipped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    // Null decimals should be skipped without error
                    sender.table("test_decimal_null")
                            .symbol("name", "test")
                            .decimalColumn("value", (io.questdb.std.Decimal64) null)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal_null");
                serverMain.assertSql("select count() from test_decimal_null", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimalZeroValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    io.questdb.std.Decimal64 zero = new io.questdb.std.Decimal64(0, 2); // 0.00
                    sender.table("test_decimal_zero")
                            .decimalColumn("balance", zero)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal_zero");
                serverMain.assertSql("select count() from test_decimal_zero", "count\n1\n");
            }
        });
    }

    @Test
    @org.junit.Ignore("Server-side ILP v4 parser doesn't support decimal types yet")
    public void testBuilderDecimalNegativeValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    io.questdb.std.Decimal64 negative = new io.questdb.std.Decimal64(-5000, 2); // -50.00
                    sender.table("test_decimal_negative")
                            .decimalColumn("loss", negative)
                            .at(1000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                serverMain.awaitTable("test_decimal_negative");
                serverMain.assertSql("select count() from test_decimal_negative", "count\n1\n");
            }
        });
    }

    @Test
    public void testBuilderDecimalScaleMismatchThrows() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                try (Sender sender = Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {
                    // First value with scale 2
                    io.questdb.std.Decimal64 v1 = new io.questdb.std.Decimal64(100, 2);
                    sender.table("test_decimal_scale_mismatch")
                            .decimalColumn("price", v1)
                            .at(1000000000L, ChronoUnit.MICROS);

                    // Second value with scale 4 - should throw
                    io.questdb.std.Decimal64 v2 = new io.questdb.std.Decimal64(10000, 4);
                    try {
                        sender.table("test_decimal_scale_mismatch")
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

    // ==================== Regression tests ====================

    /**
     * Regression test for flushIntervalNanos not being initialized.
     *
     * When flushIntervalNanos was not initialized (defaulting to 0), time-based
     * auto-flush would trigger on almost every row, causing severe performance
     * degradation. This test verifies that rows are properly batched when
     * auto-flush is disabled (the default).
     */
    @Test
    public void testBuilderRowsBatchedByDefault() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (final TestServerMain serverMain = startWithEnvVariables(
                    PropertyKey.HTTP_RECEIVE_BUFFER_SIZE.getEnvVarName(), "65536"
            )) {
                int httpPort = serverMain.getHttpServerPort();

                // Use IlpV4HttpSender directly to access pendingRows
                try (IlpV4HttpSender sender = (IlpV4HttpSender) Sender.builder(Sender.Transport.HTTP)
                        .address("localhost:" + httpPort)
                        .binaryTransfer()
                        .build()) {

                    // Send 100 rows without explicit flush
                    // If the bug exists, each row would trigger auto-flush
                    // and pendingRows would never accumulate
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_batching")
                                .symbol("tag", "value")
                                .longColumn("counter", i)
                                .at(1000000000L + i * 1000L, ChronoUnit.MICROS);
                    }

                    // Verify rows are pending (not auto-flushed)
                    Assert.assertEquals("Rows should be batched, not auto-flushed individually",
                            100, sender.getPendingRows());

                    // Now flush all at once
                    sender.flush();

                    // Verify all rows were sent
                    Assert.assertEquals("All rows should be flushed", 0, sender.getPendingRows());
                }

                serverMain.awaitTable("test_batching");
                serverMain.assertSql("select count() from test_batching", "count\n100\n");
            }
        });
    }
}
