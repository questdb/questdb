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
import io.questdb.cairo.TableReader;
import io.questdb.cutlass.line.http.IlpV4HttpSender;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("test_single")
                            .symbol("city", "London")
                            .doubleColumn("temperature", 23.5)
                            .longColumn("humidity", 65)
                            .at(1000000000000L); // Fixed timestamp
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_batch")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write to multiple tables in a single batch
                    sender.table("weather")
                            .symbol("city", "London")
                            .doubleColumn("temp", 20.0)
                            .at(1000000000000L);

                    sender.table("sensors")
                            .symbol("id", "S1")
                            .longColumn("reading", 100)
                            .at(1000000000000L);

                    sender.table("weather")
                            .symbol("city", "Paris")
                            .doubleColumn("temp", 22.0)
                            .at(1000001000000L);

                    sender.table("sensors")
                            .symbol("id", "S2")
                            .longColumn("reading", 200)
                            .at(1000001000000L);

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
                            .at(1000000000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // First row with all values
                    sender.table("test_nulls")
                            .symbol("tag", "full")
                            .doubleColumn("value", 1.0)
                            .stringColumn("name", "first")
                            .at(1000000000000L);

                    // Second row with some nulls (missing columns)
                    sender.table("test_nulls")
                            .symbol("tag", "partial")
                            .doubleColumn("value", 2.0)
                            // name is missing - should be null
                            .at(1000001000000L);

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
                                .at(1000000000000L + i * 1000000L);
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
                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("evolving")
                            .symbol("tag", "v1")
                            .longColumn("value", 1)
                            .at(1000000000000L);
                    sender.flush();
                }

                serverMain.awaitTable("evolving");
                serverMain.assertSql("select count() from evolving", "count\n1\n");

                // Add new column in second write
                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("evolving")
                            .symbol("tag", "v2")
                            .longColumn("value", 2)
                            .stringColumn("new_col", "added") // New column
                            .at(1000001000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Send out-of-order timestamps
                    sender.table("ts_order")
                            .longColumn("seq", 3)
                            .at(3000000000000L);
                    sender.table("ts_order")
                            .longColumn("seq", 1)
                            .at(1000000000000L);
                    sender.table("ts_order")
                            .longColumn("seq", 2)
                            .at(2000000000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write many rows with repeated symbol values
                    for (int i = 0; i < 1000; i++) {
                        sender.table("symbol_test")
                                .symbol("category", "cat_" + (i % 5)) // Only 5 unique values
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("unicode_test")
                            .symbol("lang", "Japanese")
                            .stringColumn("text", "Hello World")
                            .at(1000000000000L);

                    sender.table("unicode_test")
                            .symbol("lang", "Chinese")
                            .stringColumn("text", "Hello World")
                            .at(1000001000000L);

                    sender.table("unicode_test")
                            .symbol("lang", "Emoji")
                            .stringColumn("text", "Hello World")
                            .at(1000002000000L);

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Multiple flush cycles
                    for (int batch = 0; batch < 5; batch++) {
                        for (int i = 0; i < 100; i++) {
                            sender.table("multi_flush")
                                    .longColumn("batch", batch)
                                    .longColumn("row", i)
                                    .at(1000000000000L + batch * 100000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("empty_strings")
                            .symbol("sym", "")
                            .stringColumn("str", "")
                            .longColumn("id", 1)
                            .at(1000000000000L);

                    sender.table("empty_strings")
                            .symbol("sym", "non-empty")
                            .stringColumn("str", "has value")
                            .longColumn("id", 2)
                            .at(1000001000000L);

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("special_nums")
                            .longColumn("max_long", Long.MAX_VALUE)
                            .longColumn("min_long", Long.MIN_VALUE)
                            .doubleColumn("pos_inf", Double.POSITIVE_INFINITY)
                            .doubleColumn("neg_inf", Double.NEGATIVE_INFINITY)
                            .doubleColumn("nan", Double.NaN)
                            .doubleColumn("max_double", Double.MAX_VALUE)
                            .doubleColumn("min_double", Double.MIN_VALUE)
                            .at(1000000000000L);
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
                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("reconnect_test")
                            .longColumn("conn", 1)
                            .at(1000000000000L);
                    sender.flush();
                }

                serverMain.awaitTable("reconnect_test");
                serverMain.assertSql("select count() from reconnect_test", "count\n1\n");

                // Second connection (simulates reconnection)
                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("reconnect_test")
                            .longColumn("conn", 2)
                            .at(1000001000000L);
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
                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v1")
                            .longColumn("a", 1)
                            .at(1000000000000L);
                    sender.flush();
                }

                serverMain.awaitTable("schema_multi");
                serverMain.assertSql("select count() from schema_multi", "count\n1\n");

                // Add column b
                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v2")
                            .longColumn("a", 2)
                            .doubleColumn("b", 2.0)
                            .at(1000001000000L);
                    sender.flush();
                }

                serverMain.awaitTable("schema_multi");
                serverMain.assertSql("select count() from schema_multi", "count\n2\n");

                // Add columns c and d
                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v3")
                            .longColumn("a", 3)
                            .doubleColumn("b", 3.0)
                            .stringColumn("c", "hello")
                            .boolColumn("d", true)
                            .at(1000002000000L);
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
                    try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        sender.table("schema_conn");
                        sender.symbol("round", "r" + round);

                        // Each round adds a new column
                        for (int col = 0; col <= round; col++) {
                            sender.longColumn("col_" + col, col);
                        }
                        sender.at(1000000000000L + round * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Create table with many columns
                    sender.table("sparse")
                            .longColumn("a", 1)
                            .longColumn("b", 2)
                            .longColumn("c", 3)
                            .longColumn("d", 4)
                            .longColumn("e", 5)
                            .at(1000000000000L);

                    // Write rows with only some columns (sparse)
                    sender.table("sparse")
                            .longColumn("a", 10)
                            .longColumn("c", 30)
                            .at(1000001000000L);

                    sender.table("sparse")
                            .longColumn("b", 20)
                            .longColumn("e", 50)
                            .at(1000002000000L);

                    sender.table("sparse")
                            .longColumn("d", 40)
                            .at(1000003000000L);

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
                            .at(1000000000000L);

                    // Integer boundaries
                    sender.table("numeric_bounds")
                            .intColumn("max_int", Integer.MAX_VALUE)
                            .intColumn("min_int", Integer.MIN_VALUE)
                            .intColumn("zero_int", 0)
                            .at(1000001000000L);

                    // Short boundaries
                    sender.table("numeric_bounds")
                            .shortColumn("max_short", Short.MAX_VALUE)
                            .shortColumn("min_short", Short.MIN_VALUE)
                            .shortColumn("zero_short", (short) 0)
                            .at(1000002000000L);

                    // Byte boundaries
                    sender.table("numeric_bounds")
                            .byteColumn("max_byte", Byte.MAX_VALUE)
                            .byteColumn("min_byte", Byte.MIN_VALUE)
                            .byteColumn("zero_byte", (byte) 0)
                            .at(1000003000000L);

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
                            .at(1000000000000L);

                    // Double special values
                    sender.table("float_special")
                            .doubleColumn("d_pos_inf", Double.POSITIVE_INFINITY)
                            .doubleColumn("d_neg_inf", Double.NEGATIVE_INFINITY)
                            .doubleColumn("d_nan", Double.NaN)
                            .doubleColumn("d_max", Double.MAX_VALUE)
                            .doubleColumn("d_min", Double.MIN_VALUE)
                            .at(1000001000000L);

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("bool_test")
                            .boolColumn("val", true)
                            .at(1000000000000L);

                    sender.table("bool_test")
                            .boolColumn("val", false)
                            .at(1000001000000L);

                    // Alternating pattern
                    for (int i = 0; i < 10; i++) {
                        sender.table("bool_test")
                                .boolColumn("val", i % 2 == 0)
                                .at(1000002000000L + i * 1000000L);
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
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
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
                                .at(1000000000000L + len * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Various Unicode edge cases
                    sender.table("unicode_edge")
                            .stringColumn("ascii", "hello")
                            .at(1000000000000L);

                    sender.table("unicode_edge")
                            .stringColumn("latin", "caf\u00e9") // café
                            .at(1000001000000L);

                    sender.table("unicode_edge")
                            .stringColumn("cyrillic", "\u041f\u0440\u0438\u0432\u0435\u0442") // Привет
                            .at(1000002000000L);

                    sender.table("unicode_edge")
                            .stringColumn("arabic", "\u0645\u0631\u062d\u0628\u0627") // مرحبا
                            .at(1000003000000L);

                    sender.table("unicode_edge")
                            .stringColumn("cjk", "\u4f60\u597d") // 你好
                            .at(1000004000000L);

                    sender.table("unicode_edge")
                            .stringColumn("emoji", "\uD83D\uDE00\uD83C\uDF89") // 😀🎉
                            .at(1000005000000L);

                    sender.table("unicode_edge")
                            .stringColumn("mixed", "Hello \u4e16\u754c \uD83C\uDF0D") // Hello 世界 🌍
                            .at(1000006000000L);

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write many rows with unique symbol values
                    for (int i = 0; i < 1000; i++) {
                        sender.table("many_symbols")
                                .symbol("unique_sym", "sym_" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("special_symbols")
                            .symbol("sym", "with-dash")
                            .longColumn("id", 1)
                            .at(1000000000000L);

                    sender.table("special_symbols")
                            .symbol("sym", "with_underscore")
                            .longColumn("id", 2)
                            .at(1000001000000L);

                    sender.table("special_symbols")
                            .symbol("sym", "with.dot")
                            .longColumn("id", 3)
                            .at(1000002000000L);

                    sender.table("special_symbols")
                            .symbol("sym", "with spaces")
                            .longColumn("id", 4)
                            .at(1000003000000L);

                    sender.table("special_symbols")
                            .symbol("sym", "CamelCase")
                            .longColumn("id", 5)
                            .at(1000004000000L);

                    sender.table("special_symbols")
                            .symbol("sym", "ALLCAPS")
                            .longColumn("id", 6)
                            .at(1000005000000L);

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("multi_sym")
                                .symbol("sym1", "a" + (i % 5))
                                .symbol("sym2", "b" + (i % 10))
                                .symbol("sym3", "c" + (i % 3))
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Very old timestamp
                    sender.table("ts_bounds")
                            .longColumn("id", 1)
                            .at(1000000L); // 1970

                    // Recent timestamp
                    sender.table("ts_bounds")
                            .longColumn("id", 2)
                            .at(1609459200000000L); // 2021

                    // Far future
                    sender.table("ts_bounds")
                            .longColumn("id", 3)
                            .at(4102444800000000L); // 2100

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    long baseTs = 1000000000000L;
                    for (int i = 0; i < 100; i++) {
                        sender.table("micro_precision")
                                .longColumn("seq", i)
                                .at(baseTs + i); // Increment by 1 microsecond
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    long baseTs = 2000000000000L;
                    for (int i = 0; i < 100; i++) {
                        sender.table("dec_ts")
                                .longColumn("seq", i)
                                .at(baseTs - i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    long ts = 1000000000000L;
                    for (int i = 0; i < 100; i++) {
                        sender.table("same_ts")
                                .symbol("id", "row" + i)
                                .longColumn("seq", i)
                                .at(ts); // Same timestamp for all rows
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Write data spanning multiple days (multiple partitions)
                    for (int day = 0; day < 10; day++) {
                        for (int i = 0; i < 10; i++) {
                            sender.table("multi_part")
                                    .symbol("day", "d" + day)
                                    .longColumn("seq", day * 10 + i)
                                    .at(1000000000000L + day * 86400000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Flush after every single row
                    for (int i = 0; i < 50; i++) {
                        sender.table("small_batch")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Many small batches
                    for (int batch = 0; batch < 100; batch++) {
                        for (int i = 0; i < 5; i++) {
                            sender.table("many_small")
                                    .longColumn("batch", batch)
                                    .longColumn("row", i)
                                    .at(1000000000000L + batch * 10000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
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
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("lower_case")
                            .longColumn("id", 1)
                            .at(1000000000000L);

                    sender.table("UPPER_CASE")
                            .longColumn("id", 2)
                            .at(1000000000000L);

                    sender.table("MixedCase")
                            .longColumn("id", 3)
                            .at(1000000000000L);

                    sender.table("with_underscore")
                            .longColumn("id", 4)
                            .at(1000000000000L);

                    sender.table("with123numbers")
                            .longColumn("id", 5)
                            .at(1000000000000L);

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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    sender.table("col_names")
                            .longColumn("lower", 1)
                            .longColumn("UPPER", 2)
                            .longColumn("MixedCase", 3)
                            .longColumn("with_underscore", 4)
                            .longColumn("col123", 5)
                            .at(1000000000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Create table with many columns
                    for (int row = 0; row < 10; row++) {
                        sender.table("wide_table");
                        for (int col = 0; col < 100; col++) {
                            sender.longColumn("col_" + col, row * 100 + col);
                        }
                        sender.at(1000000000000L + row * 1000000L);
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
                    try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        for (int i = 0; i < 10; i++) {
                            sender.table("multi_conn")
                                    .longColumn("conn", conn)
                                    .longColumn("row", i)
                                    .at(1000000000000L + conn * 100000000L + i * 1000000L);
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
                    try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        for (int i = 0; i < 20; i++) {
                            sender.table("table_" + conn)
                                    .longColumn("row", i)
                                    .at(1000000000000L + i * 1000000L);
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
                    try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                        sender.table("rapid_reconnect")
                                .longColumn("seq", i)
                                .at(1000000000000L + i * 1000000L);
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
                                .at(1000000000000L + i * 1000000L);
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
                                .at(1000000000000L + i);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // Interleave writes to multiple tables
                    for (int i = 0; i < 100; i++) {
                        sender.table("interleaved_a")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L);

                        sender.table("interleaved_b")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L);

                        sender.table("interleaved_c")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("mixed_str_sym")
                                .symbol("sym1", "s" + (i % 5))
                                .stringColumn("str1", "string_" + i)
                                .symbol("sym2", "t" + (i % 3))
                                .stringColumn("str2", "data_" + (i * 2))
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    // All rows have same column values, only timestamp differs
                    for (int i = 0; i < 100; i++) {
                        sender.table("dup_values")
                                .symbol("tag", "same")
                                .longColumn("value", 42)
                                .stringColumn("str", "constant")
                                .at(1000000000000L + i * 1000000L);
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

                try (IlpV4HttpSender sender = IlpV4HttpSender.connect("localhost", httpPort)) {
                    int rowsPerFlush = 100;
                    int flushCount = 20;

                    for (int f = 0; f < flushCount; f++) {
                        for (int i = 0; i < rowsPerFlush; i++) {
                            int rowNum = f * rowsPerFlush + i;
                            sender.table("continuous_write")
                                    .longColumn("flush", f)
                                    .longColumn("row", i)
                                    .at(1000000000000L + rowNum * 1000000L);
                        }
                        sender.flush();
                    }
                }

                serverMain.awaitTable("continuous_write");
                serverMain.assertSql("select count() from continuous_write", "count\n2000\n");
            }
        });
    }
}
