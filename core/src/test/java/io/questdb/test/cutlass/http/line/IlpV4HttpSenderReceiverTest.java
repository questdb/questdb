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
import io.questdb.cutlass.line.http.IlpV4HttpSender;
import io.questdb.test.AbstractBootstrapTest;
import java.time.temporal.ChronoUnit;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.time.temporal.ChronoUnit;

/**
 * End-to-end integration tests for ILP v4 HTTP sender and receiver.
 * <p>
 * These tests verify that data sent via IlpV4HttpSender over HTTP is correctly
 * written to QuestDB tables and can be queried.
 */
public class IlpV4HttpSenderReceiverTest extends AbstractBootstrapTest {

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
}
