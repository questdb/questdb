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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cutlass.line.tcp.v4.IlpV4Sender;
import io.questdb.test.cutlass.line.tcp.AbstractLineTcpReceiverTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;

/**
 * End-to-end integration tests for ILP v4 sender and receiver.
 * <p>
 * These tests verify that data sent via IlpV4Sender is correctly
 * written to QuestDB tables and can be queried.
 */
public class IlpV4SenderReceiverTest extends AbstractLineTcpReceiverTest {

    @Test
    public void testSingleRowInsertion() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("test_single")
                            .symbol("city", "London")
                            .doubleColumn("temperature", 23.5)
                            .longColumn("humidity", 65)
                            .at(1000000000000L, ChronoUnit.MICROS); // Fixed timestamp
                    sender.flush();
                }

                String expected = "city\ttemperature\thumidity\ttimestamp\n" +
                        "London\t23.5\t65\t1970-01-12T13:46:40.000000Z\n";
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    assertTable(expected, "test_single");
                });
            });
        });
    }

    @Test
    public void testBatchInsertion() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("test_batch")
                                .symbol("id", "row" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                // Verify row count
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("test_batch")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testMultiTableBatch() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                // Verify weather table
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("weather")) {
                        Assert.assertEquals(2, reader.size());
                    }

                    // Verify sensors table
                    try (TableReader reader = getReader("sensors")) {
                        Assert.assertEquals(2, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testAllDataTypes() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("all_types")) {
                        Assert.assertEquals(1, reader.size());
                        Assert.assertEquals(9, reader.getMetadata().getColumnCount());
                    }
                });
            });
        });
    }

    @Test
    public void testNullValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                String expected = "tag\tvalue\tname\ttimestamp\n" +
                        "full\t1.0\tfirst\t1970-01-12T13:46:40.000000Z\n" +
                        "partial\t2.0\t\t1970-01-12T13:46:41.000000Z\n";
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    assertTable(expected, "test_nulls");
                });
            });
        });
    }

    @Test
    public void testLargeDataset() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int rowCount = 10000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("large_test")) {
                        Assert.assertEquals(rowCount, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // First write with initial schema
                    sender.table("evolving")
                            .symbol("tag", "v1")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                // Wait for first write to be applied
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("evolving")) {
                        Assert.assertEquals(1, reader.size());
                    }
                });

                // Add new column in second write
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("evolving")
                            .symbol("tag", "v2")
                            .longColumn("value", 2)
                            .stringColumn("new_col", "added") // New column
                            .at(1000001000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("evolving")) {
                        Assert.assertEquals(2, reader.size());
                        Assert.assertEquals(4, reader.getMetadata().getColumnCount());
                        // Verify new column exists
                        Assert.assertTrue(reader.getMetadata().getColumnIndex("new_col") >= 0);
                    }
                });
            });
        });
    }

    @Test
    public void testTimestampOrdering() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                // Data should be ordered by timestamp
                String expected = "seq\ttimestamp\n" +
                        "1\t1970-01-12T13:46:40.000000Z\n" +
                        "2\t1970-01-24T03:33:20.000000Z\n" +
                        "3\t1970-02-04T17:20:00.000000Z\n";
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    assertTable(expected, "ts_order");
                });
            });
        });
    }

    @Test
    public void testSymbolDeduplication() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Write many rows with repeated symbol values
                    for (int i = 0; i < 1000; i++) {
                        sender.table("symbol_test")
                                .symbol("category", "cat_" + (i % 5)) // Only 5 unique values
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("symbol_test")) {
                        Assert.assertEquals(1000, reader.size());
                        // Symbol column should have deduplication
                        int symColIdx = reader.getMetadata().getColumnIndex("category");
                        Assert.assertTrue(symColIdx >= 0);
                    }
                });
            });
        });
    }

    @Test
    public void testUnicodeData() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("unicode_test")
                            .symbol("lang", "Japanese")
                            .stringColumn("text", "こんにちは世界")
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("unicode_test")
                            .symbol("lang", "Chinese")
                            .stringColumn("text", "你好世界")
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("unicode_test")
                            .symbol("lang", "Emoji")
                            .stringColumn("text", "Hello 🌍🎉")
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("unicode_test")) {
                        Assert.assertEquals(3, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testGorillaTimestampCompression() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("gorilla_test")) {
                        Assert.assertEquals(1000, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testMultipleFlushes() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("multi_flush")) {
                        Assert.assertEquals(500, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testEmptyStringAndSymbol() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("empty_strings")) {
                        Assert.assertEquals(2, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testSpecialNumericValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("special_nums")) {
                        Assert.assertEquals(1, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testReconnection() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // First connection
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("reconnect_test")
                            .longColumn("conn", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                // Wait for first write to be applied before opening second connection
                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("reconnect_test")) {
                        Assert.assertEquals(1, reader.size());
                    }
                });

                // Second connection (simulates reconnection)
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("reconnect_test")
                            .longColumn("conn", 2)
                            .at(1000001000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("reconnect_test")) {
                        Assert.assertEquals(2, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testTableAutoCreation() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                String uniqueTableName = "auto_create_" + System.currentTimeMillis();

                // Table doesn't exist, should be auto-created
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table(uniqueTableName)
                            .symbol("tag", "test")
                            .doubleColumn("value", 42.0)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    // Verify table was created
                    TableToken token = engine.getTableTokenIfExists(uniqueTableName);
                    Assert.assertNotNull("Table should exist", token);

                    try (TableReader reader = getReader(uniqueTableName)) {
                        Assert.assertEquals(1, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testDataPersistence() throws Exception {
        assertMemoryLeak(() -> {
            // Write data
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("persistence_test")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("persistence_test")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });

            // Read data outside of server context
            try (TableReader reader = getReader("persistence_test")) {
                Assert.assertEquals(100, reader.size());
            }
        });
    }

    // ========== LARGE DATASET TESTS ==========

    @Test
    public void testLargeDataset100K() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int rowCount = 100_000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.autoFlushRows(10000); // Flush every 10K rows

                    for (int i = 0; i < rowCount; i++) {
                        sender.table("large_100k")
                                .symbol("partition", "p" + (i % 100))
                                .longColumn("id", i)
                                .doubleColumn("value", i * 0.001)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("large_100k")) {
                        Assert.assertEquals(rowCount, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testManySmallBatches() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int batchCount = 100;
                int rowsPerBatch = 100;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int batch = 0; batch < batchCount; batch++) {
                        for (int i = 0; i < rowsPerBatch; i++) {
                            sender.table("many_batches")
                                    .longColumn("batch", batch)
                                    .longColumn("row", i)
                                    .at(1000000000000L + batch * 1000000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("many_batches")) {
                        Assert.assertEquals(batchCount * rowsPerBatch, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testWideTable() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int columnCount = 30;
                int rowCount = 100;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.autoFlushRows(10); // Flush frequently for wide rows

                    for (int row = 0; row < rowCount; row++) {
                        sender.table("wide_table");
                        for (int col = 0; col < columnCount; col++) {
                            sender.longColumn("col_" + col, row * columnCount + col);
                        }
                        sender.at(1000000000000L + row * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("wide_table")) {
                        Assert.assertEquals(rowCount, reader.size());
                        // +1 for timestamp column
                        Assert.assertEquals(columnCount + 1, reader.getMetadata().getColumnCount());
                    }
                });
            });
        });
    }

    // ========== SCHEMA EVOLUTION TESTS ==========

    @Test
    public void testSchemaEvolutionMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // Phase 1: Initial schema with 2 columns
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v1")
                            .longColumn("value", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("schema_multi")) {
                        Assert.assertEquals(1, reader.size());
                        Assert.assertEquals(3, reader.getMetadata().getColumnCount()); // tag, value, timestamp
                    }
                });

                // Phase 2: Add 3 new columns
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v2")
                            .longColumn("value", 2)
                            .stringColumn("str_col", "new string")
                            .doubleColumn("dbl_col", 3.14)
                            .boolColumn("bool_col", true)
                            .at(1000001000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("schema_multi")) {
                        Assert.assertEquals(2, reader.size());
                        Assert.assertEquals(6, reader.getMetadata().getColumnCount());
                    }
                });

                // Phase 3: Add even more columns
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("schema_multi")
                            .symbol("tag", "v3")
                            .longColumn("value", 3)
                            .stringColumn("str_col", "updated")
                            .doubleColumn("dbl_col", 2.71)
                            .boolColumn("bool_col", false)
                            .floatColumn("flt_col", 1.5f)
                            .intColumn("int_col", 42)
                            .at(1000002000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("schema_multi")) {
                        Assert.assertEquals(3, reader.size());
                        Assert.assertEquals(8, reader.getMetadata().getColumnCount());
                    }
                });
            });
        });
    }

    @Test
    public void testSparseColumns() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Row 1: All columns present
                    sender.table("sparse")
                            .symbol("tag", "full")
                            .longColumn("a", 1)
                            .longColumn("b", 2)
                            .longColumn("c", 3)
                            .longColumn("d", 4)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // Row 2: Only a and c
                    sender.table("sparse")
                            .symbol("tag", "ac")
                            .longColumn("a", 10)
                            .longColumn("c", 30)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    // Row 3: Only b and d
                    sender.table("sparse")
                            .symbol("tag", "bd")
                            .longColumn("b", 20)
                            .longColumn("d", 40)
                            .at(1000002000000L, ChronoUnit.MICROS);

                    // Row 4: Only tag (all other columns null)
                    sender.table("sparse")
                            .symbol("tag", "empty")
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("sparse")) {
                        Assert.assertEquals(4, reader.size());
                        Assert.assertEquals(6, reader.getMetadata().getColumnCount()); // tag, a, b, c, d, timestamp
                    }
                });
            });
        });
    }

    @Test
    public void testSchemaEvolutionAcrossConnections() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // Connection 1: Create table with initial schema
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("evolve_conn")
                                .symbol("source", "conn1")
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("evolve_conn")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });

                // Connection 2: Add new column
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("evolve_conn")
                                .symbol("source", "conn2")
                                .longColumn("value", i + 100)
                                .stringColumn("new_str", "str_" + i)
                                .at(1000100000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("evolve_conn")) {
                        Assert.assertEquals(200, reader.size());
                        Assert.assertTrue(reader.getMetadata().getColumnIndex("new_str") >= 0);
                    }
                });

                // Connection 3: Add another column
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("evolve_conn")
                                .symbol("source", "conn3")
                                .longColumn("value", i + 200)
                                .stringColumn("new_str", "str_" + i)
                                .doubleColumn("another", i * 0.5)
                                .at(1000200000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("evolve_conn")) {
                        Assert.assertEquals(300, reader.size());
                        Assert.assertTrue(reader.getMetadata().getColumnIndex("another") >= 0);
                    }
                });
            });
        });
    }

    // ========== CONCURRENT CONNECTION TESTS ==========

    @Test
    public void testMultipleConnectionsSameTable() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int connCount = 5;
                int rowsPerConn = 200;

                Thread[] threads = new Thread[connCount];
                for (int c = 0; c < connCount; c++) {
                    final int connId = c;
                    threads[c] = new Thread(() -> {
                        try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                            for (int i = 0; i < rowsPerConn; i++) {
                                sender.table("concurrent_same")
                                        .longColumn("conn", connId)
                                        .longColumn("row", i)
                                        .at(1000000000000L + connId * 1000000000L + i * 1000000L, ChronoUnit.MICROS);
                            }
                            sender.flush();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                for (Thread t : threads) {
                    t.start();
                }
                for (Thread t : threads) {
                    t.join();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("concurrent_same")) {
                        Assert.assertEquals(connCount * rowsPerConn, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testMultipleConnectionsDifferentTables() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int tableCount = 10;
                int rowsPerTable = 100;

                Thread[] threads = new Thread[tableCount];
                for (int t = 0; t < tableCount; t++) {
                    final int tableId = t;
                    threads[t] = new Thread(() -> {
                        try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                            for (int i = 0; i < rowsPerTable; i++) {
                                sender.table("table_" + tableId)
                                        .longColumn("value", i)
                                        .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                            }
                            sender.flush();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
                }

                for (Thread thread : threads) {
                    thread.start();
                }
                for (Thread thread : threads) {
                    thread.join();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    for (int t = 0; t < tableCount; t++) {
                        try (TableReader reader = getReader("table_" + t)) {
                            Assert.assertEquals(rowsPerTable, reader.size());
                        }
                    }
                });
            });
        });
    }

    @Test
    public void testRapidReconnection() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int reconnects = 50;
                int rowsPerConnection = 10;

                for (int r = 0; r < reconnects; r++) {
                    try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                        for (int i = 0; i < rowsPerConnection; i++) {
                            sender.table("rapid_reconnect")
                                    .longColumn("conn", r)
                                    .longColumn("row", i)
                                    .at(1000000000000L + r * 1000000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                    }
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("rapid_reconnect")) {
                        Assert.assertEquals(reconnects * rowsPerConnection, reader.size());
                    }
                });
            });
        });
    }

    // ========== DATA TYPE EDGE CASES ==========

    @Test
    public void testLongStringValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 10000; i++) {
                    sb.append("a");
                }
                String longString = sb.toString();

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("long_strings")
                            .stringColumn("short", "hello")
                            .stringColumn("medium", longString.substring(0, 1000))
                            .stringColumn("long", longString)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("long_strings")) {
                        Assert.assertEquals(1, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testNumericBoundaryValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Row with max values
                    sender.table("boundaries")
                            .symbol("type", "max")
                            .longColumn("lng", Long.MAX_VALUE)
                            .intColumn("integer", Integer.MAX_VALUE)
                            .doubleColumn("dbl", Double.MAX_VALUE)
                            .floatColumn("flt", Float.MAX_VALUE)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // Row with min values
                    sender.table("boundaries")
                            .symbol("type", "min")
                            .longColumn("lng", Long.MIN_VALUE)
                            .intColumn("integer", Integer.MIN_VALUE)
                            .doubleColumn("dbl", Double.MIN_VALUE)
                            .floatColumn("flt", Float.MIN_VALUE)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    // Row with zero
                    sender.table("boundaries")
                            .symbol("type", "zero")
                            .longColumn("lng", 0)
                            .intColumn("integer", 0)
                            .doubleColumn("dbl", 0.0)
                            .floatColumn("flt", 0.0f)
                            .at(1000002000000L, ChronoUnit.MICROS);

                    // Row with negative zero
                    sender.table("boundaries")
                            .symbol("type", "neg_zero")
                            .longColumn("lng", -0L)
                            .intColumn("integer", -0)
                            .doubleColumn("dbl", -0.0)
                            .floatColumn("flt", -0.0f)
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("boundaries")) {
                        Assert.assertEquals(4, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testSpecialFloatValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("special_floats")
                            .symbol("type", "positive_infinity")
                            .doubleColumn("dbl", Double.POSITIVE_INFINITY)
                            .floatColumn("flt", Float.POSITIVE_INFINITY)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("special_floats")
                            .symbol("type", "negative_infinity")
                            .doubleColumn("dbl", Double.NEGATIVE_INFINITY)
                            .floatColumn("flt", Float.NEGATIVE_INFINITY)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("special_floats")
                            .symbol("type", "nan")
                            .doubleColumn("dbl", Double.NaN)
                            .floatColumn("flt", Float.NaN)
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.table("special_floats")
                            .symbol("type", "subnormal")
                            .doubleColumn("dbl", Double.MIN_NORMAL / 2)
                            .floatColumn("flt", Float.MIN_NORMAL / 2)
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("special_floats")) {
                        Assert.assertEquals(4, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testUnicodeEdgeCases() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Basic multilingual plane
                    sender.table("unicode_edge")
                            .symbol("type", "basic")
                            .stringColumn("text", "Hello World")
                            .at(1000000000000L, ChronoUnit.MICROS);

                    // CJK characters
                    sender.table("unicode_edge")
                            .symbol("type", "cjk")
                            .stringColumn("text", "日本語中文한국어")
                            .at(1000001000000L, ChronoUnit.MICROS);

                    // Emoji (supplementary plane)
                    sender.table("unicode_edge")
                            .symbol("type", "emoji")
                            .stringColumn("text", "🎉🚀💻🌍")
                            .at(1000002000000L, ChronoUnit.MICROS);

                    // Mixed scripts
                    sender.table("unicode_edge")
                            .symbol("type", "mixed")
                            .stringColumn("text", "Hello 世界 مرحبا 🌍")
                            .at(1000003000000L, ChronoUnit.MICROS);

                    // RTL text (Arabic)
                    sender.table("unicode_edge")
                            .symbol("type", "rtl")
                            .stringColumn("text", "مرحبا بالعالم")
                            .at(1000004000000L, ChronoUnit.MICROS);

                    // Combining characters
                    sender.table("unicode_edge")
                            .symbol("type", "combining")
                            .stringColumn("text", "e\u0301")  // é as e + combining accent
                            .at(1000005000000L, ChronoUnit.MICROS);

                    // Zero-width characters
                    sender.table("unicode_edge")
                            .symbol("type", "zero_width")
                            .stringColumn("text", "hello\u200Bworld")  // zero-width space
                            .at(1000006000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("unicode_edge")) {
                        Assert.assertEquals(7, reader.size());
                    }
                });
            });
        });
    }

    // ========== PARTITION TESTS ==========

    @Test
    public void testDataAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Data spanning multiple days (default partition is DAY)
                    long day1 = 1609459200000000L; // 2021-01-01 00:00:00
                    long day2 = day1 + 86400000000L; // 2021-01-02
                    long day3 = day2 + 86400000000L; // 2021-01-03

                    for (int i = 0; i < 100; i++) {
                        sender.table("multi_partition")
                                .symbol("day", "day1")
                                .longColumn("value", i)
                                .at(day1 + i * 1000000L);
                    }

                    for (int i = 0; i < 100; i++) {
                        sender.table("multi_partition")
                                .symbol("day", "day2")
                                .longColumn("value", i + 100)
                                .at(day2 + i * 1000000L);
                    }

                    for (int i = 0; i < 100; i++) {
                        sender.table("multi_partition")
                                .symbol("day", "day3")
                                .longColumn("value", i + 200)
                                .at(day3 + i * 1000000L);
                    }

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("multi_partition")) {
                        Assert.assertEquals(300, reader.size());
                        Assert.assertTrue(reader.getPartitionCount() >= 3);
                    }
                });
            });
        });
    }

    @Test
    public void testOutOfOrderAcrossPartitions() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    long day1 = 1609459200000000L; // 2021-01-01
                    long day2 = day1 + 86400000000L; // 2021-01-02
                    long day3 = day2 + 86400000000L; // 2021-01-03

                    // Send in random order across partitions
                    sender.table("ooo_partitions")
                            .longColumn("seq", 3)
                            .at(day3);
                    sender.table("ooo_partitions")
                            .longColumn("seq", 1)
                            .at(day1);
                    sender.table("ooo_partitions")
                            .longColumn("seq", 2)
                            .at(day2);
                    sender.table("ooo_partitions")
                            .longColumn("seq", 5)
                            .at(day3 + 1000000L);
                    sender.table("ooo_partitions")
                            .longColumn("seq", 4)
                            .at(day1 + 1000000L);
                    sender.table("ooo_partitions")
                            .longColumn("seq", 6)
                            .at(day2 + 1000000L);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("ooo_partitions")) {
                        Assert.assertEquals(6, reader.size());
                    }
                });
            });
        });
    }

    // ========== SYMBOL TESTS ==========

    @Test
    public void testManyUniqueSymbols() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int symbolCount = 10000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.autoFlushRows(1000);

                    for (int i = 0; i < symbolCount; i++) {
                        sender.table("many_symbols")
                                .symbol("unique_sym", "sym_" + i)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("many_symbols")) {
                        Assert.assertEquals(symbolCount, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testSymbolsWithSpecialCharacters() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("special_symbols")
                            .symbol("tag", "normal")
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("tag", "with space")
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("tag", "with-dash")
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("tag", "with_underscore")
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("tag", "with.dot")
                            .at(1000004000000L, ChronoUnit.MICROS);

                    sender.table("special_symbols")
                            .symbol("tag", "日本語")
                            .at(1000005000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("special_symbols")) {
                        Assert.assertEquals(6, reader.size());
                    }
                });
            });
        });
    }

    // ========== TIMESTAMP TESTS ==========

    @Test
    public void testTimestampBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Very early timestamp
                    sender.table("ts_boundaries")
                            .symbol("type", "early")
                            .at(1000000L, ChronoUnit.MICROS); // 1970-01-01 00:00:01

                    // Recent timestamp
                    sender.table("ts_boundaries")
                            .symbol("type", "recent")
                            .at(1704067200000000L, ChronoUnit.MICROS); // 2024-01-01 00:00:00

                    // Future timestamp
                    sender.table("ts_boundaries")
                            .symbol("type", "future")
                            .at(2000000000000000L, ChronoUnit.MICROS); // Far future

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("ts_boundaries")) {
                        Assert.assertEquals(3, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testMicrosecondPrecision() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    long baseTs = 1000000000000L;

                    // Send timestamps with microsecond differences
                    for (int i = 0; i < 100; i++) {
                        sender.table("microsecond_test")
                                .longColumn("seq", i)
                                .at(baseTs + i); // 1 microsecond apart
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("microsecond_test")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });
        });
    }

    // ========== MIXED WORKLOAD TESTS ==========

    @Test
    public void testMixedTableWritesInterleaved() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Interleave writes to multiple tables
                    for (int i = 0; i < 1000; i++) {
                        int tableNum = i % 5;
                        sender.table("mixed_" + tableNum)
                                .symbol("source", "interleaved")
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    for (int t = 0; t < 5; t++) {
                        try (TableReader reader = getReader("mixed_" + t)) {
                            Assert.assertEquals(200, reader.size()); // 1000 / 5 = 200 per table
                        }
                    }
                });
            });
        });
    }

    @Test
    public void testVariableRowSizes() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Rows with varying number of columns
                    for (int i = 0; i < 500; i++) {
                        sender.table("variable_size");
                        sender.longColumn("id", i);

                        // Add varying number of additional columns
                        int extraCols = i % 10;
                        for (int c = 0; c < extraCols; c++) {
                            sender.longColumn("extra_" + c, i * 100 + c);
                        }

                        sender.at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("variable_size")) {
                        Assert.assertEquals(500, reader.size());
                        // Should have id + 9 extra columns + timestamp
                        Assert.assertEquals(11, reader.getMetadata().getColumnCount());
                    }
                });
            });
        });
    }

    // ========== DATA INTEGRITY TESTS ==========

    @Test
    public void testDataIntegrityLargeValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                long[] testValues = {
                        0L, 1L, -1L, 42L, -42L,
                        Long.MAX_VALUE, Long.MIN_VALUE,
                        Long.MAX_VALUE - 1, Long.MIN_VALUE + 1,
                        1_000_000_000_000L, -1_000_000_000_000L
                };

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < testValues.length; i++) {
                        sender.table("integrity_test")
                                .longColumn("value", testValues[i])
                                .longColumn("expected_index", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("integrity_test")) {
                        Assert.assertEquals(testValues.length, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testBooleanValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Test various boolean patterns
                    for (int i = 0; i < 100; i++) {
                        sender.table("bool_test")
                                .boolColumn("even", i % 2 == 0)
                                .boolColumn("divisible_by_3", i % 3 == 0)
                                .boolColumn("divisible_by_5", i % 5 == 0)
                                .longColumn("value", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("bool_test")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });
        });
    }

    // ========== STRESS TESTS ==========

    @Test
    public void testHighThroughput() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int totalRows = 50000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.autoFlushRows(5000);

                    long startTime = System.nanoTime();
                    for (int i = 0; i < totalRows; i++) {
                        sender.table("throughput_test")
                                .symbol("partition", "p" + (i % 10))
                                .longColumn("id", i)
                                .doubleColumn("value", i * 0.1)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                    long endTime = System.nanoTime();

                    // Just log the time, don't assert on it since CI machines vary
                    double seconds = (endTime - startTime) / 1_000_000_000.0;
                    System.out.println("High throughput test: " + totalRows + " rows in " + seconds + " seconds");
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("throughput_test")) {
                        Assert.assertEquals(totalRows, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testContinuousWriteWithPeriodicFlush() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int batches = 20;
                int rowsPerBatch = 500;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int batch = 0; batch < batches; batch++) {
                        for (int i = 0; i < rowsPerBatch; i++) {
                            sender.table("continuous_write")
                                    .longColumn("batch", batch)
                                    .longColumn("row", i)
                                    .doubleColumn("value", batch * 1000 + i)
                                    .at(1000000000000L + batch * 1000000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        sender.flush();
                        // Verify data after each batch
                        final int expectedRows = (batch + 1) * rowsPerBatch;
                        TestUtils.assertEventually(() -> {
                            drainWalQueue();
                            try (TableReader reader = getReader("continuous_write")) {
                                Assert.assertEquals(expectedRows, reader.size());
                            }
                        }, 30);
                    }
                }
            });
        });
    }

    // ========== EDGE CASE SCENARIOS ==========

    @Test
    public void testVerySmallBatches() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // Many flushes with single rows
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("single_row_batches")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                        sender.flush();
                    }
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("single_row_batches")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testTableNameVariations() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Various valid table names
                    sender.table("simple")
                            .longColumn("id", 1)
                            .at(1000000000000L, ChronoUnit.MICROS);

                    sender.table("with_underscore")
                            .longColumn("id", 2)
                            .at(1000001000000L, ChronoUnit.MICROS);

                    sender.table("CamelCase")
                            .longColumn("id", 3)
                            .at(1000002000000L, ChronoUnit.MICROS);

                    sender.table("MixedCase_123")
                            .longColumn("id", 4)
                            .at(1000003000000L, ChronoUnit.MICROS);

                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("simple")) {
                        Assert.assertEquals(1, reader.size());
                    }
                    try (TableReader reader = getReader("with_underscore")) {
                        Assert.assertEquals(1, reader.size());
                    }
                    try (TableReader reader = getReader("CamelCase")) {
                        Assert.assertEquals(1, reader.size());
                    }
                    try (TableReader reader = getReader("MixedCase_123")) {
                        Assert.assertEquals(1, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testColumnNameVariations() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("col_names")
                            .longColumn("simple", 1)
                            .longColumn("with_underscore", 2)
                            .longColumn("CamelCase", 3)
                            .longColumn("lower123", 4)
                            .at(1000000000000L, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("col_names")) {
                        Assert.assertEquals(1, reader.size());
                        Assert.assertTrue(reader.getMetadata().getColumnIndex("simple") >= 0);
                        Assert.assertTrue(reader.getMetadata().getColumnIndex("with_underscore") >= 0);
                        Assert.assertTrue(reader.getMetadata().getColumnIndex("CamelCase") >= 0);
                        Assert.assertTrue(reader.getMetadata().getColumnIndex("lower123") >= 0);
                    }
                });
            });
        });
    }

    @Test
    public void testDuplicateColumnValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Many rows with same values
                    for (int i = 0; i < 1000; i++) {
                        sender.table("duplicates")
                                .symbol("constant", "same_value")
                                .longColumn("same_long", 42)
                                .doubleColumn("same_double", 3.14)
                                .stringColumn("same_string", "constant")
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("duplicates")) {
                        Assert.assertEquals(1000, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testAlternatingSymbolValues() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    // Alternating between two symbol values
                    for (int i = 0; i < 1000; i++) {
                        sender.table("alternating")
                                .symbol("toggle", i % 2 == 0 ? "A" : "B")
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("alternating")) {
                        Assert.assertEquals(1000, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testSameTimestampDifferentRows() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    long sameTimestamp = 1000000000000L;

                    // Multiple rows with same timestamp but different data
                    for (int i = 0; i < 100; i++) {
                        sender.table("same_ts")
                                .symbol("partition", "p" + (i % 10))
                                .longColumn("id", i)
                                .at(sameTimestamp);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("same_ts")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testDecreasingTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    long baseTs = 1000000000000L;

                    // Send timestamps in strictly decreasing order
                    for (int i = 99; i >= 0; i--) {
                        sender.table("decreasing_ts")
                                .longColumn("seq", 99 - i)
                                .at(baseTs + i * 1000000L);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("decreasing_ts")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testRandomTimestampOrder() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    long baseTs = 1000000000000L;

                    // Send timestamps in pseudo-random order
                    int[] order = {5, 2, 8, 1, 9, 3, 7, 0, 6, 4};
                    for (int i = 0; i < 100; i++) {
                        int offset = order[i % 10] + (i / 10) * 10;
                        sender.table("random_ts")
                                .longColumn("seq", i)
                                .at(baseTs + offset * 1000000L);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("random_ts")) {
                        Assert.assertEquals(100, reader.size());
                    }
                });
            });
        });
    }

    // ========== MULTI-TYPE COLUMN TESTS ==========

    @Test
    public void testAllNumericTypes() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 100; i++) {
                        sender.table("all_numeric")
                                .longColumn("long_val", i * 1000000L)
                                .intColumn("int_val", i * 1000)
                                .doubleColumn("double_val", i * 0.123456789)
                                .floatColumn("float_val", i * 0.5f)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("all_numeric")) {
                        Assert.assertEquals(100, reader.size());
                        Assert.assertEquals(5, reader.getMetadata().getColumnCount());
                    }
                });
            });
        });
    }

    @Test
    public void testMultipleSymbolColumns() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 1000; i++) {
                        sender.table("multi_symbol")
                                .symbol("sym1", "a" + (i % 10))
                                .symbol("sym2", "b" + (i % 20))
                                .symbol("sym3", "c" + (i % 5))
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("multi_symbol")) {
                        Assert.assertEquals(1000, reader.size());
                    }
                });
            });
        });
    }

    @Test
    public void testMixedStringAndSymbol() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < 500; i++) {
                        sender.table("mixed_str_sym")
                                .symbol("category", "cat_" + (i % 5))  // Low cardinality -> symbol
                                .stringColumn("description", "This is row number " + i)  // High cardinality -> string
                                .symbol("status", i % 2 == 0 ? "active" : "inactive")  // 2 values -> symbol
                                .stringColumn("uuid", java.util.UUID.randomUUID().toString())  // All unique -> string
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("mixed_str_sym")) {
                        Assert.assertEquals(500, reader.size());
                    }
                });
            });
        });
    }

    // ========== LARGE BATCH BUFFER GROWTH TESTS ==========

    /**
     * Tests that the server can handle a single large batch that exceeds the initial recv buffer size.
     * <p>
     * The default recv buffer is 128KB. This test sends ~10,000 rows in a single flush,
     * which produces a message of approximately 600KB+. This exercises the buffer growth
     * code path in LineTcpConnectionContext.handleV4Protocol().
     * <p>
     * This is a regression test for the issue where large ILPv4 messages would cause
     * an infinite loop because the buffer couldn't grow to accommodate the full message.
     */
    @Test
    public void testLargeBatchExceedsInitialBuffer() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // 10,000 rows with multiple columns produces a ~600KB message,
                // which exceeds the default 128KB recv buffer
                int rowCount = 10_000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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
                    // Single flush - creates one large message
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("large_batch_test")) {
                        Assert.assertEquals(rowCount, reader.size());
                    }
                });
            });
        });
    }

    /**
     * Tests an even larger batch (50K rows) to ensure buffer can grow multiple times if needed.
     */
    @Test
    public void testVeryLargeBatch() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // 50,000 rows produces a ~3MB message
                int rowCount = 50_000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("very_large_batch")) {
                        Assert.assertEquals(rowCount, reader.size());
                    }
                });
            });
        });
    }

    /**
     * Tests large batch with long string values to maximize row size.
     * This creates a message with larger per-row overhead.
     */
    @Test
    public void testLargeBatchWithLongStrings() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // Create a moderately long string (500 chars) to increase message size
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 50; i++) {
                    sb.append("0123456789");
                }
                String longString = sb.toString();

                // 5,000 rows with 500-char strings = ~2.5MB of string data alone
                int rowCount = 5_000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < rowCount; i++) {
                        sender.table("large_strings_batch")
                                .symbol("tag", "batch")
                                .stringColumn("long_data", longString)
                                .longColumn("id", i)
                                .at(1000000000000L + i * 1000000L, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("large_strings_batch")) {
                        Assert.assertEquals(rowCount, reader.size());
                    }
                });
            });
        });
    }

    /**
     * Tests multiple large batches in sequence without reconnecting.
     * This ensures buffer handling works correctly across multiple messages.
     */
    @Test
    public void testMultipleLargeBatchesSequential() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int batchCount = 5;
                int rowsPerBatch = 10_000;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int batch = 0; batch < batchCount; batch++) {
                        for (int i = 0; i < rowsPerBatch; i++) {
                            sender.table("multi_large_batch")
                                    .symbol("batch", "b" + batch)
                                    .longColumn("id", batch * rowsPerBatch + i)
                                    .doubleColumn("value", i * 0.1)
                                    .stringColumn("info", "batch" + batch + "_row" + i)
                                    .at(1000000000000L + batch * 100000000000L + i * 1000000L, ChronoUnit.MICROS);
                        }
                        // Flush each batch separately - each is a large message
                        sender.flush();
                    }
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("multi_large_batch")) {
                        Assert.assertEquals(batchCount * rowsPerBatch, reader.size());
                    }
                });
            });
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
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("test_at_now")
                            .symbol("tag", "row1")
                            .longColumn("value", 100)
                            .atNow();
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("test_at_now")) {
                        Assert.assertEquals(1, reader.size());
                        // Verify a timestamp column was auto-created
                        Assert.assertTrue("Should have timestamp column",
                                reader.getMetadata().getColumnIndex("timestamp") >= 0);
                        // Verify the timestamp is the designated timestamp
                        Assert.assertEquals("timestamp should be designated timestamp",
                                reader.getMetadata().getColumnIndex("timestamp"),
                                reader.getMetadata().getTimestampIndex());
                    }
                });
            });
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
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                long fixedTimestamp = 1704067200000000L; // 2024-01-01 00:00:00 UTC in micros

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
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

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("test_mixed_ts")) {
                        Assert.assertEquals(3, reader.size());
                    }
                });
            });
        });
    }

    /**
     * Tests multiple consecutive atNow() calls.
     */
    @Test
    public void testMultipleAtNow() throws Exception {
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                int rowCount = 100;

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    for (int i = 0; i < rowCount; i++) {
                        sender.table("test_multi_at_now")
                                .longColumn("id", i)
                                .atNow();
                    }
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("test_multi_at_now")) {
                        Assert.assertEquals(rowCount, reader.size());
                    }
                });
            });
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
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // Create table with custom designated timestamp column named 'ts'
                engine.execute("CREATE TABLE custom_ts_table (sym SYMBOL, value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("custom_ts_table")
                            .symbol("sym", "test")
                            .longColumn("value", 42)
                            .atNow();
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("custom_ts_table")) {
                        Assert.assertEquals(1, reader.size());
                        // Verify the table has ONLY the expected columns (sym, value, ts)
                        // There should be NO "timestamp" column created
                        Assert.assertEquals("Should have exactly 3 columns", 3, reader.getMetadata().getColumnCount());
                        Assert.assertTrue("Should have sym column", reader.getMetadata().getColumnIndexQuiet("sym") >= 0);
                        Assert.assertTrue("Should have value column", reader.getMetadata().getColumnIndexQuiet("value") >= 0);
                        Assert.assertTrue("Should have ts column", reader.getMetadata().getColumnIndexQuiet("ts") >= 0);
                        Assert.assertEquals("ts should be designated timestamp",
                                reader.getMetadata().getColumnIndexQuiet("ts"),
                                reader.getMetadata().getTimestampIndex());
                        // Verify NO "timestamp" column exists
                        Assert.assertEquals("Should NOT have timestamp column", -1,
                                reader.getMetadata().getColumnIndexQuiet("timestamp"));
                    }
                });
            });
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
        assertMemoryLeak(() -> {
            runInContext(receiver -> {
                // Create table with custom designated timestamp column named 'ts'
                engine.execute("CREATE TABLE custom_ts_at_table (sym SYMBOL, value LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");

                // Ingest data using at() with explicit timestamp
                long explicitTimestamp = 1700000000000000L; // 2023-11-14T22:13:20Z in micros
                try (IlpV4Sender sender = IlpV4Sender.connect("127.0.0.1", bindPort)) {
                    sender.table("custom_ts_at_table")
                            .symbol("sym", "test")
                            .longColumn("value", 42)
                            .at(explicitTimestamp, ChronoUnit.MICROS);
                    sender.flush();
                }

                TestUtils.assertEventually(() -> {
                    drainWalQueue();
                    try (TableReader reader = getReader("custom_ts_at_table")) {
                        Assert.assertEquals(1, reader.size());
                        // Verify the table has ONLY the expected columns (sym, value, ts)
                        // There should be NO "timestamp" column created
                        Assert.assertEquals("Should have exactly 3 columns", 3, reader.getMetadata().getColumnCount());
                        Assert.assertTrue("Should have sym column", reader.getMetadata().getColumnIndexQuiet("sym") >= 0);
                        Assert.assertTrue("Should have value column", reader.getMetadata().getColumnIndexQuiet("value") >= 0);
                        Assert.assertTrue("Should have ts column", reader.getMetadata().getColumnIndexQuiet("ts") >= 0);
                        Assert.assertEquals("ts should be designated timestamp",
                                reader.getMetadata().getColumnIndexQuiet("ts"),
                                reader.getMetadata().getTimestampIndex());
                        // Verify NO "timestamp" column exists
                        Assert.assertEquals("Should NOT have timestamp column", -1,
                                reader.getMetadata().getColumnIndexQuiet("timestamp"));
                    }
                });
            });
        });
    }
}
