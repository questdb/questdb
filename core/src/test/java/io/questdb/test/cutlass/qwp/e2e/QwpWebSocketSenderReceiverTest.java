/*+*****************************************************************************
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

import io.questdb.cairo.GeoHashes;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal64;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_DATE;
import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_DOUBLE_ARRAY;
import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.TYPE_GEOHASH;

/**
 * End-to-end integration tests for QWP v1 WebSocket sender and receiver.
 * <p>
 * These tests mirror the HTTP sender/receiver tests to ensure feature parity
 * between HTTP and WebSocket transports.
 * <p>
 * Tests verify that data sent via QwpWebSocketSender over WebSocket is correctly
 * written to QuestDB tables and can be queried.
 * <p>
 * Tests are parametrized to run with different window sizes:
 * - windowSize=1 for sync behavior (no I/O thread, direct send + waitForAck)
 * - windowSize=8 for async behavior (I/O thread, sendQueue, double buffers)
 */
@RunWith(Parameterized.class)
public class QwpWebSocketSenderReceiverTest extends AbstractQwpWebSocketTest {

    private final int windowSize;

    @SuppressWarnings("unused")
    public QwpWebSocketSenderReceiverTest(int windowSize) {
        this.windowSize = windowSize;
    }

    @Parameterized.Parameters(name = "windowSize={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {1},   // window=1 (sync behavior)
                {8}    // window=8 (async behavior)
        });
    }

    @Test
    public void test10000Rows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10_000; i++) {
                    sender.table("ws_test_10000rows")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    // Flush every 1000 rows to avoid buffer overflow
                    if ((i + 1) % 1000 == 0) {
                        sender.flush();
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_10000rows", "count\n10000\n");
        });
    }

    @Test
    public void test1000Rows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 1000; i++) {
                    sender.table("ws_test_1000_rows")
                            .symbol("id", "row" + i)
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_1000_rows", "count\n1000\n");
        });
    }

    @Test
    public void test100Rows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 100; i++) {
                    sender.table("ws_test_100_rows")
                            .symbol("id", "row" + i)
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_100_rows", "count\n100\n");
            assertSql("SELECT sum(value) FROM ws_test_100_rows", "sum\n4950\n");
        });
    }

    @Test
    public void test10Rows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_test_10rows")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_10rows", "count\n10\n");
        });
    }

    @Test
    public void test1DDoubleArray() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_1d_double_array")
                        .doubleArray("values", new double[]{1.1, 2.2, 3.3, 4.4, 5.5})
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_1d_double_array", "count\n1\n");
        });
    }

    @Test
    public void test2DDoubleArray() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                double[][] matrix = {{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}};
                sender.table("ws_test_2d_double_array")
                        .doubleArray("matrix", matrix)
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_2d_double_array", "count\n1\n");
        });
    }

    @Test
    public void test3DDoubleArray() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                double[][][] cube = {{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}};
                sender.table("ws_test_3d_double_array")
                        .doubleArray("cube", cube)
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_3d_double_array", "count\n1\n");
        });
    }

    @Test
    public void testMixedNullAndNonNullArrayRowsAutoCreateTable() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.getTableBuffer("ws_mixed_null_array_new_table")
                        .getOrCreateColumn("arr", TYPE_DOUBLE_ARRAY, true)
                        .addNull();
                sender.at(1_000_000_000L, ChronoUnit.MICROS);

                double[][][] cube = {{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}};
                sender.table("ws_mixed_null_array_new_table")
                        .doubleArray("arr", cube)
                        .at(2_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql(
                    "SELECT arr FROM ws_mixed_null_array_new_table ORDER BY timestamp",
                    "arr\nnull\n[[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]\n"
            );
        });
    }

    @Test
    public void testMixedNullAndNonNullArrayRowsExistingTable() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_mixed_null_array_existing (arr DOUBLE[][][], timestamp TIMESTAMP NOT NULL) TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.getTableBuffer("ws_mixed_null_array_existing")
                        .getOrCreateColumn("arr", TYPE_DOUBLE_ARRAY, true)
                        .addNull();
                sender.at(1_000_000_000L, ChronoUnit.MICROS);

                double[][][] cube = {{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}};
                sender.table("ws_mixed_null_array_existing")
                        .doubleArray("arr", cube)
                        .at(2_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql(
                    "SELECT arr FROM ws_mixed_null_array_existing ORDER BY timestamp",
                    "arr\nnull\n[[[1.0,2.0],[3.0,4.0]],[[5.0,6.0],[7.0,8.0]]]\n"
            );
        });
    }

    @Test
    public void testAllDataTypes() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_all_types")
                        .boolColumn("bool_col", true)
                        .longColumn("long_col", 9_999_999_999L)
                        .longColumn("int_col", 123_456L)
                        .doubleColumn("double_col", 3.14159265359)
                        .doubleColumn("float_col", 2.71828)
                        .stringColumn("string_col", "hello world")
                        .symbol("symbol_col", "sym_value")
                        .timestampColumn("ts_col", 1_609_459_200_000_000L, ChronoUnit.MICROS)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_all_types", "count\n1\n");
        });
    }

    /**
     * Tests sending all narrow types (BYTE, SHORT, INT, FLOAT, CHAR) in a single row
     * using the direct narrow-type methods on QwpWebSocketSender.
     */
    @Test
    public void testAllNarrowTypes_mixedRow() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_narrow_mixed_direct (" +
                    "b BYTE, " +
                    "s SHORT, " +
                    "i INT, " +
                    "f FLOAT, " +
                    "c CHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_narrow_mixed_direct")
                        .byteColumn("b", (byte) 42)
                        .shortColumn("s", (short) 1000)
                        .intColumn("i", 100_000)
                        .floatColumn("f", 1.5f)
                        .charColumn("c", 'A')
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql(
                    "SELECT b, s, i, f, c FROM ws_narrow_mixed_direct",
                    "b\ts\ti\tf\tc\n42\t1000\t100000\t1.5\tA\n"
            );
        });
    }

    @Test
    public void testAllNumericTypes() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 100; i++) {
                    sender.table("ws_all_numeric")
                            .longColumn("byte_col", i % 128)
                            .longColumn("short_col", i * 100)
                            .longColumn("int_col", i * 10_000)
                            .longColumn("long_col", (long) i * 100_000_000L)
                            .doubleColumn("float_col", i * 1.1)
                            .doubleColumn("double_col", i * 1.111111)
                            .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_all_numeric", "count\n100\n");
        });
    }

    @Test
    public void testAtNowServerAssignedTimestamp() throws Exception {
        runInContext((port) -> {
            try (Sender sender = createSender(port)) {
                sender.table("ws_test_at_now")
                        .symbol("tag", "row1")
                        .longColumn("value", 100)
                        .atNow();
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_at_now", "count\n1\n");

            // Verify a timestamp column was auto-created
            assertSql(
                    "SELECT \"column\" FROM table_columns('ws_test_at_now') ORDER BY \"column\"",
                    "column\ntag\ntimestamp\nvalue\n"
            );

            // Verify the timestamp was assigned by the server (should be recent)
            assertSql(
                    "SELECT count() FROM ws_test_at_now WHERE timestamp >= '2025-01-01'",
                    "count\n1\n"
            );
        });
    }

    /**
     * Tests that multiple rows sent with atNow() in the same batch get per-row timestamps.
     * <p>
     * The columnar path should call getTicks() per row (like the row-by-row path does),
     * rather than using a single timestamp for all rows.
     * <p>
     * Note: We can't assert all timestamps are unique because multiple rows may be
     * processed within the same microsecond. Instead, we verify that NOT all rows
     * have identical timestamps (which would indicate the bug where a single getTicks()
     * call was used for the entire batch).
     */
    @Test
    @Ignore
    public void testAtNowTimestampsAreUniquePerRow() throws Exception {
        runInContext((port) -> {
            // Send multiple rows with atNow() - timestamps should be assigned individually
            // Use more rows to increase chance of timestamp variation
            int rowCount = 20;
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < rowCount; i++) {
                    sender.table("ws_unique_ts_test")
                            .symbol("tag", "row" + i)
                            .longColumn("value", i)
                            .atNow();
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_unique_ts_test", "count\n" + rowCount + "\n");

            // Verify that timestamps are NOT all identical.
            // If bug exists: all rows have identical timestamps, so count_distinct = 1
            // If fixed: rows have per-row timestamps, so count_distinct > 1
            // (may not be exactly rowCount due to microsecond resolution)
            // Use a query that returns 'true' if we have more than 1 distinct timestamp
            assertSql(
                    "SELECT count_distinct(timestamp) > 1 AS has_multiple_timestamps FROM ws_unique_ts_test",
                    "has_multiple_timestamps\ntrue\n"
            );
        });
    }

    @Test
    public void testAtNowWithCustomTimestampColumnName() throws Exception {
        runInContext((port) -> {
            // Create table with custom designated timestamp column named 'ts'
            execute("CREATE TABLE ws_custom_ts_table (sym SYMBOL, value LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Ingest data using atNow()
            try (Sender sender = createSender(port)) {
                sender.table("ws_custom_ts_table")
                        .symbol("sym", "test")
                        .longColumn("value", 42)
                        .atNow();
                sender.flush();
            }

            drainWalQueue();

            // Verify row was inserted
            assertSql("SELECT count() FROM ws_custom_ts_table", "count\n1\n");

            // Verify the table has ONLY the expected columns (sym, value, ts)
            assertSql(
                    "SELECT \"column\" FROM table_columns('ws_custom_ts_table') ORDER BY \"column\"",
                    "column\nsym\nts\nvalue\n"
            );

            // Verify the timestamp was assigned by the server
            assertSql(
                    "SELECT count() FROM ws_custom_ts_table WHERE ts >= '2025-01-01'",
                    "count\n1\n"
            );
        });
    }

    @Test
    public void testAtWithCustomTimestampColumnName() throws Exception {
        runInContext((port) -> {
            // Create table with custom designated timestamp column named 'ts'
            execute("CREATE TABLE ws_custom_ts_at_table (sym SYMBOL, value LONG, ts TIMESTAMP NOT NULL) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // Ingest data using at() with explicit timestamp
            long explicitTimestamp = 1_700_000_000_000_000L; // 2023-11-14T22:13:20Z in micros
            try (Sender sender = createSender(port)) {
                sender.table("ws_custom_ts_at_table")
                        .symbol("sym", "test")
                        .longColumn("value", 42)
                        .at(explicitTimestamp, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            // Verify row was inserted
            assertSql("SELECT count() FROM ws_custom_ts_at_table", "count\n1\n");

            // Verify the table has ONLY the expected columns
            assertSql(
                    "SELECT \"column\" FROM table_columns('ws_custom_ts_at_table') ORDER BY \"column\"",
                    "column\nsym\nts\nvalue\n"
            );

            // Verify the explicit timestamp was correctly stored
            assertSql(
                    "SELECT ts FROM ws_custom_ts_at_table",
                    "ts\n2023-11-14T22:13:20.000000Z\n"
            );
        });
    }

    /**
     * Tests that auto-created columns are correctly mapped when writer index differs from column index.
     * <p>
     * This test exposes a bug in QwpWalAppender (lines 194 and 224) where:
     * <ul>
     *   <li>Line 194 converts column index to writer index:
     *       {@code columnWriterIndex = metadata.getWriterIndex(metadata.getColumnIndexQuiet(columnName));}</li>
     *   <li>Line 224 converts again, treating the writer index as a column index:
     *       {@code columnIndexMap[i] = metadata.getWriterIndex(columnWriterIndex);}</li>
     * </ul>
     * <p>
     * The fix is to change line 194 to just get the column index (not the writer index):
     * {@code columnWriterIndex = metadata.getColumnIndexQuiet(columnName);}
     * <p>
     * This test fails with "Invalid column: col_c" because the double conversion causes
     * the auto-created column to be incorrectly mapped, resulting in data loss.
     */
    @Test
    public void testAutoCreateColumnAfterColumnDrop() throws Exception {
        runInContext((port) -> {
            // Step 1: Create table with columns and insert initial data
            execute("CREATE TABLE ws_drop_add_test (" +
                    "tag SYMBOL, " +
                    "col_a LONG, " +
                    "col_b LONG, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            // Insert initial data to establish the table
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_drop_add_test")
                        .symbol("tag", "initial")
                        .longColumn("col_a", 100)
                        .longColumn("col_b", 200)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }
            drainWalQueue();

            // Step 2: Drop col_a - this creates a gap between writer index and column index
            // After dropping, column indices are reordered but writer indices keep the gaps
            execute("ALTER TABLE ws_drop_add_test DROP COLUMN col_a");
            drainWalQueue();

            // Step 3: Send ILP data with a NEW column (col_c) - this triggers auto-create
            // The bug causes double getWriterIndex conversion when the new column is created
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_drop_add_test")
                        .symbol("tag", "after_drop")
                        .longColumn("col_b", 300)
                        .longColumn("col_c", 999)  // New column - auto-created
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            // Step 4: Verify the new column value is correct
            // If bug exists: col_c value will be wrong or in wrong column
            // If fixed: col_c should be 999
            assertSql(
                    "SELECT tag, col_b, col_c FROM ws_drop_add_test WHERE tag = 'after_drop'",
                    "tag\tcol_b\tcol_c\nafter_drop\t300\t999\n"
            );
        });
    }

    /**
     * Tests auto-creation of a new column on an existing pre-created table.
     * This is a simpler version of testAutoCreateColumnAfterColumnDrop to verify
     * basic auto-column creation works.
     */
    @Test
    public void testAutoCreateColumnOnExistingTable() throws Exception {
        runInContext((port) -> {
            // Create a table with existing columns
            execute("CREATE TABLE ws_autocreate_test (" +
                    "tag SYMBOL, " +
                    "existing_col LONG, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            // Send ILP data with a NEW column (new_col) - this triggers auto-create
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_autocreate_test")
                        .symbol("tag", "test")
                        .longColumn("existing_col", 100)
                        .longColumn("new_col", 42)  // New column - auto-created
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            drainWalQueue();

            // Verify both columns have correct values
            assertSql(
                    "SELECT tag, existing_col, new_col FROM ws_autocreate_test",
                    "tag\texisting_col\tnew_col\ntest\t100\t42\n"
            );
        });
    }

    /**
     * Tests that auto-flush triggers based on byte threshold.
     * <p>
     * Disables row-count and interval triggers, sets a low byte threshold (1024),
     * and sends rows with large string payloads. Verifies that data reaches the
     * server before any explicit flush() or close() — proving the byte threshold
     * triggered the auto-flush. If auto-flush is broken, awaitTable() times out.
     */
    @Test
    public void testAutoFlushByBytes() throws Exception {
        Assume.assumeTrue("Async mode only (window > 1)", windowSize > 1);

        runInContext((port) -> {
            // 1024 byte threshold; row-count and interval triggers disabled
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null,
                    Integer.MAX_VALUE,                      // autoFlushRows: disabled
                    1024,                                   // autoFlushBytes: 1 KB
                    TimeUnit.HOURS.toNanos(1),      // autoFlushInterval: disabled
                    windowSize,
                    null
            )) {
                // ~200 bytes per row; 20 rows = ~4 KB >> 1 KB threshold
                String largePayload = "A".repeat(180);
                for (int i = 0; i < 20; i++) {
                    sender.table("ws_autoflush_bytes")
                            .longColumn("value", i)
                            .stringColumn("payload", largePayload)
                            .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                }

                // Verify data arrived BEFORE flush()/close().
                // If auto-flush by bytes didn't trigger, the table won't exist
                // and awaitTable() times out, failing the test.
                drainWalQueue();

                // Flush remaining buffered rows
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_autoflush_bytes", "count\n20\n");
        });
    }

    /**
     * Tests that auto-flush triggers based on time interval.
     * <p>
     * Disables row-count and byte triggers, sets a short interval (50 ms),
     * sends one row, sleeps past the interval, then sends another row which
     * triggers the interval check. Verifies the first row reached the server
     * before any explicit flush() or close() — proving the interval trigger
     * fired. If auto-flush is broken, awaitTable() times out.
     */
    @Test
    public void testAutoFlushByInterval() throws Exception {
        Assume.assumeTrue("Async mode only (window > 1)", windowSize > 1);

        runInContext((port) -> {
            // 50 ms interval; row-count and byte triggers disabled
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null,
                    Integer.MAX_VALUE,                      // autoFlushRows: disabled
                    Integer.MAX_VALUE,                      // autoFlushBytes: disabled
                    TimeUnit.MILLISECONDS.toNanos(50),      // autoFlushInterval: 50 ms
                    windowSize,
                    null
            )) {
                // Send first row — stays buffered (interval hasn't elapsed yet)
                sender.table("ws_autoflush_interval")
                        .longColumn("value", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                // Sleep well past the 50 ms interval
                Thread.sleep(150);

                // Second row triggers the interval check, auto-flushing row 1
                sender.table("ws_autoflush_interval")
                        .longColumn("value", 2)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);

                // Verify row 1 arrived BEFORE flush()/close().
                // If the interval trigger didn't fire, the table won't exist
                // and awaitTable() times out, failing the test.
                drainWalQueue();

                // Flush the second row
                sender.flush();
            }
            drainWalQueue();

            assertSql("SELECT count() FROM ws_autoflush_interval", "count\n2\n");
            assertSql(
                    "SELECT value FROM ws_autoflush_interval ORDER BY timestamp",
                    """
                            value
                            1
                            2
                            """
            );
        });
    }

    @Test
    public void testBatchInsertion() throws Exception {
        runInContext((port) -> {
            try (Sender sender = createSender(port)) {
                for (int i = 0; i < 100; i++) {
                    sender.table("ws_test_batch")
                            .symbol("id", "row" + i)
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_batch", "count\n100\n");
        });
    }

    @Test
    public void testBooleanValues() throws Exception {
        runInContext((port) -> {
            try (Sender sender = createSender(port)) {
                sender.table("ws_bool_test")
                        .boolColumn("val", true)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("ws_bool_test")
                        .boolColumn("val", false)
                        .at(1_000_001_000_000L, ChronoUnit.MICROS);

                // Alternating pattern
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_bool_test")
                            .boolColumn("val", i % 2 == 0)
                            .at(1_000_002_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                }

                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_bool_test", "count\n12\n");
        });
    }

    /**
     * Tests the QWP-specific byteColumn() method that encodes a native BYTE wire type.
     * Pre-creates a BYTE column to verify the client sends the correct type code.
     */
    @Test
    public void testByteColumn_directWrite() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_byte_direct (" +
                    "value BYTE, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_byte_direct")
                        .byteColumn("value", (byte) 0)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_byte_direct")
                        .byteColumn("value", (byte) 127)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_byte_direct")
                        .byteColumn("value", (byte) -128)
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_byte_direct")
                        .byteColumn("value", (byte) -1)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_byte_direct", "count\n4\n");
            assertSql(
                    "SELECT value FROM ws_byte_direct ORDER BY ts",
                    "value\n0\n127\n-128\n-1\n"
            );
        });
    }

    @Test
    public void testByteRangeLong() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_byte_range")
                        .symbol("id", "b1")
                        .longColumn("byte_val", 127)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT byte_val FROM ws_test_byte_range", "byte_val\n127\n");
        });
    }

    @Test
    public void testCancelRow() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.cancelRow(); // no-op without a table

                // this row should be inserted
                sender.table("ws_cancel_row")
                        .symbol("tag", "kept")
                        .longColumn("value", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                // this row is cancelled mid-build
                sender.table("ws_cancel_row")
                        .symbol("tag", "dropped")
                        .longColumn("value", 2);
                sender.cancelRow();

                // this row should also be inserted
                sender.table("ws_cancel_row")
                        .symbol("tag", "also_kept")
                        .longColumn("value", 3)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);

                sender.flush();
            }

            drainWalQueue();
            assertSql(
                    "SELECT tag, value FROM ws_cancel_row ORDER BY timestamp",
                    """
                            tag\tvalue
                            kept\t1
                            also_kept\t3
                            """
            );
        });
    }

    /**
     * Tests that a STRING value sent via ILP is correctly stored in a pre-created CHAR column.
     * CHAR is stored as a 16-bit UTF-16 code unit; the server must extract the first character
     * from the incoming string.
     */
    @Test
    public void testCharColumnFromString() throws Exception {
        runInContext((port) -> {
            // Pre-create table with CHAR column
            execute("CREATE TABLE ws_char_test (" +
                    "tag SYMBOL, " +
                    "x CHAR, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_char_test")
                        .symbol("tag", "test")
                        .stringColumn("x", "A")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_char_test", "count\n1\n");
            assertSql("SELECT x FROM ws_char_test", "x\nA\n");
        });
    }

    /**
     * Tests the QWP-specific charColumn() method that encodes a native CHAR wire type.
     * Pre-creates a CHAR column to verify the client sends the correct type code.
     */
    @Test
    public void testCharColumn_directWrite() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_char_direct (" +
                    "value CHAR, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_char_direct")
                        .charColumn("value", 'Z')
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_char_direct")
                        .charColumn("value", 'a')
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_char_direct")
                        .charColumn("value", '0')
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_char_direct", "count\n3\n");
            assertSql(
                    "SELECT value FROM ws_char_direct ORDER BY ts",
                    "value\nZ\na\n0\n"
            );
        });
    }

    @Test
    public void testColumnNameShort() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_col_short")
                        .longColumn("x", 42)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT x FROM ws_test_col_short", "x\n42\n");
        });
    }

    @Test
    public void testColumnNameWithUnderscore() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_col_underscore")
                        .longColumn("my_column", 42)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT my_column FROM ws_test_col_underscore", "my_column\n42\n");
        });
    }

    @Test
    public void testColumnTypeMismatchThrowsClientSide() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // First row: "value" is a long column
                sender.table("ws_test_col_type_mismatch")
                        .longColumn("value", 42)
                        .at(1_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Second row: same column name but double — must throw immediately
                try {
                    sender.table("ws_test_col_type_mismatch")
                            .doubleColumn("value", 3.14)
                            .at(2_000_000L, ChronoUnit.MICROS);
                    Assert.fail("Expected LineSenderException for column type mismatch");
                } catch (LineSenderException e) {
                    Assert.assertTrue(
                            "Error should mention type mismatch: " + e.getMessage(),
                            e.getMessage().contains("Column type mismatch")
                    );
                }
            }

            // The first row should still be intact on the server
            drainWalQueue();
            assertSql(
                    "SELECT value FROM ws_test_col_type_mismatch",
                    "value\n42\n"
            );
        });
    }

    @Test
    public void testComplexSchemaMultipleRows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 50; i++) {
                    sender.table("ws_complex_multi")
                            .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                            .symbol("host", "host-" + (i % 10))
                            .longColumn("metric1", i * 10)
                            .longColumn("metric2", i * 20)
                            .doubleColumn("ratio", i / 100.0)
                            .boolColumn("active", i % 2 == 0)
                            .stringColumn("status", "running")
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_complex_multi", "count\n50\n");
        });
    }

    @Test
    public void testDateColumn() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_test_date (" +
                    "event_date DATE, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                QwpTableBuffer buf = sender.getTableBuffer("ws_test_date");
                QwpTableBuffer.ColumnBuffer dateCol = buf.getOrCreateColumn("event_date", TYPE_DATE, false);

                // Row 1: 2024-01-01 00:00:00 UTC (epoch millis)
                dateCol.addLong(1_704_067_200_000L);
                sender.at(1_000_000_000_000L, ChronoUnit.MICROS);

                // Row 2: 2024-06-15 12:30:00 UTC (epoch millis)
                dateCol.addLong(1_718_454_600_000L);
                sender.at(1_000_000_000_001L, ChronoUnit.MICROS);

                // Row 3: 1970-01-01 00:00:00 UTC (epoch zero)
                dateCol.addLong(0L);
                sender.at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_date", "count\n3\n");
            assertSql(
                    "SELECT event_date FROM ws_test_date ORDER BY ts",
                    "event_date\n2024-01-01T00:00:00.000Z\n2024-06-15T12:30:00.000Z\n1970-01-01T00:00:00.000Z\n"
            );
        });
    }

    @Test
    public void testDecimalNegativeValue() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                Decimal64 negative = new Decimal64(-5000, 2); // -50.00
                sender.table("ws_test_decimal_negative")
                        .decimalColumn("loss", negative)
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_decimal_negative", "count\n1\n");
        });
    }

    @Test
    public void testDecimalNullSkipped() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Null decimals should be skipped without error
                sender.table("ws_test_decimal_null")
                        .symbol("name", "test")
                        .decimalColumn("value", (Decimal64) null)
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_decimal_null", "count\n1\n");
        });
    }

    @Test
    public void testDecimalScaleDownPrecisionLossThrows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // First value with scale 2 — sets column scale to 2
                Decimal64 v1 = new Decimal64(100, 2);
                sender.table("ws_test_decimal_precision_loss")
                        .decimalColumn("price", v1)
                        .at(1_000_000_000L, ChronoUnit.MICROS);

                // Second value with scale 4 whose trailing digits would be lost
                // when rescaling from scale 4 to scale 2 (1.2345 -> cannot be 1.23 exactly)
                Decimal64 v2 = new Decimal64(12_345, 4);
                try {
                    sender.table("ws_test_decimal_precision_loss")
                            .decimalColumn("price", v2)
                            .at(2_000_000_000L, ChronoUnit.MICROS);
                    Assert.fail("Expected LineSenderException for precision loss");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("precision loss"));
                }
            }
        });
    }

    @Test
    public void testDecimalWithScalarColumns() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                Decimal64 price = new Decimal64(9999, 2); // 99.99
                sender.table("ws_test_decimal_mixed")
                        .symbol("product", "Widget")
                        .longColumn("quantity", 10)
                        .decimalColumn("price", price)
                        .doubleColumn("discount", 0.1)
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_decimal_mixed", "count\n1\n");
        });
    }

    @Test
    public void testDecimalZeroValue() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                Decimal64 zero = new Decimal64(0, 2); // 0.00
                sender.table("ws_test_decimal_zero")
                        .decimalColumn("balance", zero)
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_decimal_zero", "count\n1\n");
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
        Assume.assumeTrue("Async mode only (window > 1)", windowSize > 1);

        runInContext((port) -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null,
                    5,                              // autoFlushRows = 5: small batches
                    Integer.MAX_VALUE,              // autoFlushBytes: disabled
                    TimeUnit.HOURS.toNanos(1),      // autoFlushInterval: disabled
                    10,                             // inFlightWindow
                    null
            )) {
                // Send multiple small batches
                for (int batch = 0; batch < 10; batch++) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("ws_delta_async")
                                .symbol("batch", "batch-" + batch)
                                .symbol("ticker", i % 2 == 0 ? "AAPL" : "GOOG")
                                .longColumn("value", batch * 10 + i)
                                .at(1_000_000_000_000L + batch * 10 + i, ChronoUnit.MICROS);
                    }
                    // Auto-flush triggers every 5 rows
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_async", "count\n50\n");
            assertSql("SELECT count(distinct batch) FROM ws_delta_async", "count_distinct\n10\n");
            assertSql("SELECT count(distinct ticker) FROM ws_delta_async", "count_distinct\n2\n");
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
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: with symbols
                sender.table("ws_delta_no_sym")
                        .symbol("tag", "first")
                        .longColumn("value", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: no symbols at all
                sender.table("ws_delta_no_sym_data")
                        .longColumn("value", 2)
                        .doubleColumn("metric", 3.14)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 3: symbols again
                sender.table("ws_delta_no_sym")
                        .symbol("tag", "second")
                        .longColumn("value", 3)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_no_sym", "count\n2\n");
            assertSql("SELECT count() FROM ws_delta_no_sym_data", "count\n1\n");
        });
    }

    /**
     * Tests empty symbols (empty string) with delta encoding.
     */
    @Test
    public void testDeltaSymbolDict_emptySymbol() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_delta_empty_sym")
                        .symbol("tag", "")  // Empty symbol
                        .longColumn("value", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_delta_empty_sym")
                        .symbol("tag", "nonempty")
                        .longColumn("value", 2)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.table("ws_delta_empty_sym")
                        .symbol("tag", "")  // Reuse empty
                        .longColumn("value", 3)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_empty_sym", "count\n3\n");
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
        runInContext((port) -> {
            int numSymbols = 100;
            int rowsPerBatch = 50;
            int numBatches = 5;

            try (QwpWebSocketSender sender = createSender(port)) {
                int row = 0;
                for (int batch = 0; batch < numBatches; batch++) {
                    for (int i = 0; i < rowsPerBatch; i++) {
                        // Cycle through symbols, introducing new ones in each batch
                        int symbolIdx = (batch * 20 + i) % numSymbols;
                        sender.table("ws_delta_high_vol")
                                .symbol("device", "device-" + symbolIdx)
                                .longColumn("reading", row)
                                .at(1_000_000_000_000L + row, ChronoUnit.MICROS);
                        row++;
                    }
                    sender.flush();
                }
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_high_vol",
                    "count\n" + (numBatches * rowsPerBatch) + "\n");
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
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: interleaved tables
                for (int i = 0; i < 10; i++) {
                    if (i % 2 == 0) {
                        sender.table("ws_delta_inter_a")
                                .symbol("type", "even")
                                .longColumn("idx", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    } else {
                        sender.table("ws_delta_inter_b")
                                .symbol("type", "odd")
                                .longColumn("idx", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                }
                sender.flush();

                // Batch 2: more interleaved, adding new symbols
                for (int i = 10; i < 20; i++) {
                    if (i % 3 == 0) {
                        sender.table("ws_delta_inter_a")
                                .symbol("type", "triple")  // New symbol
                                .longColumn("idx", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    } else if (i % 3 == 1) {
                        sender.table("ws_delta_inter_b")
                                .symbol("type", "even")  // Reuse from table_a
                                .longColumn("idx", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    } else {
                        sender.table("ws_delta_inter_c")  // New table
                                .symbol("type", "remainder")  // New symbol
                                .longColumn("idx", i)
                                .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            drainWalQueue();
            // Table A: 5 even (batch 1) + 3 triple (batch 2) = 8 rows
            assertSql("SELECT count() FROM ws_delta_inter_a", "count\n8\n");
            // Table B: 5 odd (batch 1) + 4 even-reuse (batch 2) = 9 rows
            assertSql("SELECT count() FROM ws_delta_inter_b", "count\n9\n");
            // Table C: 3 remainder rows (batch 2)
            assertSql("SELECT count() FROM ws_delta_inter_c", "count\n3\n");
        });
    }

    /**
     * Tests long symbol strings with delta encoding.
     */
    @Test
    public void testDeltaSymbolDict_longSymbolStrings() throws Exception {
        runInContext((port) -> {
            // Create long symbol strings
            StringBuilder longSymbol1 = new StringBuilder();
            StringBuilder longSymbol2 = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                longSymbol1.append("a");
                longSymbol2.append("b");
            }

            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1
                sender.table("ws_delta_long_sym")
                        .symbol("tag", longSymbol1.toString())
                        .longColumn("value", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: add second long symbol
                sender.table("ws_delta_long_sym")
                        .symbol("tag", longSymbol2.toString())
                        .longColumn("value", 2)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 3: reuse first
                sender.table("ws_delta_long_sym")
                        .symbol("tag", longSymbol1.toString())
                        .longColumn("value", 3)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_long_sym", "count\n3\n");
            assertSql("SELECT count(distinct tag) FROM ws_delta_long_sym", "count_distinct\n2\n");
        });
    }

    /**
     * Tests that multiple batches with the same symbols work correctly.
     * <p>
     * After the first batch, the server knows all symbols in the dictionary.
     * Subsequent batches using the same symbols should have empty deltas
     * (deltaCount=0) since no new symbols need to be sent.
     */
    @Test
    public void testDeltaSymbolDict_multipleBatches_sameSymbols() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: introduces AAPL, GOOG
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_delta_same_syms")
                            .symbol("ticker", i % 2 == 0 ? "AAPL" : "GOOG")
                            .longColumn("price", 100 + i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                // Batch 2: reuses same symbols - delta should be empty
                for (int i = 10; i < 20; i++) {
                    sender.table("ws_delta_same_syms")
                            .symbol("ticker", i % 2 == 0 ? "AAPL" : "GOOG")
                            .longColumn("price", 100 + i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                // Batch 3: still reusing same symbols
                for (int i = 20; i < 30; i++) {
                    sender.table("ws_delta_same_syms")
                            .symbol("ticker", i % 2 == 0 ? "GOOG" : "AAPL")
                            .longColumn("price", 100 + i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_same_syms", "count\n30\n");
            assertSql("SELECT count(distinct ticker) FROM ws_delta_same_syms", "count_distinct\n2\n");
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
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Multiple symbol columns: region, currency, status
                sender.table("ws_delta_multi_col")
                        .symbol("region", "us")
                        .symbol("currency", "USD")
                        .symbol("status", "active")
                        .longColumn("value", 100)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("ws_delta_multi_col")
                        .symbol("region", "eu")
                        .symbol("currency", "EUR")
                        .symbol("status", "active")  // Reuses "active"
                        .longColumn("value", 200)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("ws_delta_multi_col")
                        .symbol("region", "asia")
                        .symbol("currency", "JPY")
                        .symbol("status", "pending")
                        .longColumn("value", 300)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);

                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_multi_col", "count\n3\n");
            assertSql("SELECT count(distinct region) FROM ws_delta_multi_col", "count_distinct\n3\n");
            assertSql("SELECT count(distinct currency) FROM ws_delta_multi_col", "count_distinct\n3\n");
            assertSql("SELECT count(distinct status) FROM ws_delta_multi_col", "count_distinct\n2\n");
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
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Table A uses AAPL, GOOG
                sender.table("ws_delta_table_a")
                        .symbol("ticker", "AAPL")
                        .longColumn("value", 100)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_delta_table_a")
                        .symbol("ticker", "GOOG")
                        .longColumn("value", 200)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                // Table B uses AAPL, MSFT (AAPL is shared, MSFT is new)
                sender.table("ws_delta_table_b")
                        .symbol("ticker", "AAPL")
                        .longColumn("price", 150)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.table("ws_delta_table_b")
                        .symbol("ticker", "MSFT")
                        .longColumn("price", 300)
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);

                // Table C uses GOOG, MSFT (both already in dictionary)
                sender.table("ws_delta_table_c")
                        .symbol("ticker", "GOOG")
                        .doubleColumn("metric", 2800.5)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
                sender.table("ws_delta_table_c")
                        .symbol("ticker", "MSFT")
                        .doubleColumn("metric", 299.5)
                        .at(1_000_000_000_005L, ChronoUnit.MICROS);

                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_table_a", "count\n2\n");
            assertSql("SELECT count() FROM ws_delta_table_b", "count\n2\n");
            assertSql("SELECT count() FROM ws_delta_table_c", "count\n2\n");
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
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: AAPL
                sender.table("ws_delta_progressive")
                        .symbol("ticker", "AAPL")
                        .longColumn("price", 150)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: AAPL + GOOG (new)
                sender.table("ws_delta_progressive")
                        .symbol("ticker", "AAPL")
                        .longColumn("price", 151)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.table("ws_delta_progressive")
                        .symbol("ticker", "GOOG")
                        .longColumn("price", 2800)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 3: GOOG + MSFT (new)
                sender.table("ws_delta_progressive")
                        .symbol("ticker", "GOOG")
                        .longColumn("price", 2801)
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);
                sender.table("ws_delta_progressive")
                        .symbol("ticker", "MSFT")
                        .longColumn("price", 300)
                        .at(1_000_000_000_004L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 4: All three symbols + TSLA (new)
                sender.table("ws_delta_progressive")
                        .symbol("ticker", "AAPL")
                        .longColumn("price", 152)
                        .at(1_000_000_000_005L, ChronoUnit.MICROS);
                sender.table("ws_delta_progressive")
                        .symbol("ticker", "TSLA")
                        .longColumn("price", 700)
                        .at(1_000_000_000_006L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_progressive", "count\n7\n");
            assertSql("SELECT count(distinct ticker) FROM ws_delta_progressive", "count_distinct\n4\n");
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
        runInContext((port) -> {
            // 5 rapid reconnection cycles
            for (int cycle = 0; cycle < 5; cycle++) {
                try (QwpWebSocketSender sender = createSender(port)) {
                    // Same symbols each cycle - each connection starts fresh
                    for (int i = 0; i < 5; i++) {
                        sender.table("ws_delta_rapid_reconnect")
                                .symbol("cycle", "cycle-" + cycle)
                                .symbol("idx", "idx-" + i)
                                .longColumn("value", cycle * 100 + i)
                                .at(1_000_000_000_000L + cycle * 10 + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_rapid_reconnect", "count\n25\n");
            assertSql("SELECT count(distinct cycle) FROM ws_delta_rapid_reconnect", "count_distinct\n5\n");
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
        runInContext((port) -> {
            // First connection: establish symbols
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_delta_reconnect")
                            .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }
            // Connection closed here

            // Second connection: must re-send dictionary
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 10; i < 20; i++) {
                    sender.table("ws_delta_reconnect")
                            .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_reconnect", "count\n20\n");
            assertSql("SELECT count(distinct region) FROM ws_delta_reconnect", "count_distinct\n3\n");
        });
    }

    /**
     * Tests that unicode symbols work correctly with delta encoding.
     */
    @Test
    public void testDeltaSymbolDict_unicodeSymbols() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: unicode symbols
                sender.table("ws_delta_unicode")
                        .symbol("city", "東京")
                        .longColumn("temp", 20)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_delta_unicode")
                        .symbol("city", "北京")
                        .longColumn("temp", 15)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: reuse unicode symbols + add new
                sender.table("ws_delta_unicode")
                        .symbol("city", "東京")  // Reuse
                        .longColumn("temp", 21)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.table("ws_delta_unicode")
                        .symbol("city", "서울")  // New
                        .longColumn("temp", 18)
                        .at(1_000_000_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_unicode", "count\n4\n");
            assertSql("SELECT count(distinct city) FROM ws_delta_unicode", "count_distinct\n3\n");
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
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                Decimal64 price1 = new Decimal64(15_099, 2);
                Decimal64 price2 = new Decimal64(28_005, 2);

                // Batch 1
                sender.table("ws_delta_complex")
                        .symbol("ticker", "AAPL")
                        .decimalColumn("price", price1)
                        .doubleArray("features", new double[]{1.0, 2.0, 3.0})
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: new symbol + reuse
                sender.table("ws_delta_complex")
                        .symbol("ticker", "GOOG")
                        .decimalColumn("price", price2)
                        .doubleArray("features", new double[]{4.0, 5.0, 6.0})
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.table("ws_delta_complex")
                        .symbol("ticker", "AAPL")  // Reuse
                        .decimalColumn("price", price1)
                        .doubleArray("features", new double[]{7.0, 8.0, 9.0})
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_delta_complex", "count\n3\n");
            assertSql("SELECT count(distinct ticker) FROM ws_delta_complex", "count_distinct\n2\n");
        });
    }

    @Test
    public void testDoubleSpecialValues() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_double_special")
                        .doubleColumn("pi", Math.PI)
                        .doubleColumn("e", Math.E)
                        .doubleColumn("sqrt2", Math.sqrt(2))
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_double_special", "count\n1\n");
        });
    }

    /**
     * Tests that in async mode (window > 1) a server error from a bad batch
     * does not surface until the user thread calls flush().
     * <p>
     * With autoFlushRows=1, each at() call triggers an auto-flush that enqueues
     * the batch to the I/O thread without waiting for ACKs. The user thread
     * keeps producing rows obliviously. The error only surfaces when flush()
     * calls awaitEmpty(), which checks the in-flight window's lastError.
     * <p>
     * This is fundamentally different from sync mode where flush() blocks for
     * each batch's ACK inline, so the error surfaces on the flush() that sent
     * the bad data.
     * <p>
     * The bad row targets a pre-existing table via a fresh connection so the
     * client doesn't know the server-side schema of this table and cannot
     * detect the mismatch locally — only the server can reject it.
     */
    @Test
    public void testErrorPropagation_asyncMultipleBatchesInFlight() throws Exception {
        Assume.assumeTrue("Async mode only (window > 1)", windowSize > 1);

        runInContext((port) -> {
            // Pre-create a table with a LONG column
            try (QwpWebSocketSender setupSender = QwpWebSocketSender.connect("localhost", port)) {
                setupSender.table("ws_async_multi_err")
                        .longColumn("value", 0)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                setupSender.flush();
            }
            drainWalQueue();

            // Fresh async sender: autoFlushRows=1 so each row is enqueued
            // immediately, window=8. The sender doesn't know the server-side
            // schema of "ws_async_multi_err", so it cannot detect the type mismatch.
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null,
                    1,                              // autoFlushRows: every row
                    Integer.MAX_VALUE,              // autoFlushBytes: disabled
                    TimeUnit.HOURS.toNanos(1),      // autoFlushInterval: disabled
                    windowSize,
                    null
            )) {
                // Good rows to a separate table — auto-flushed, no ACK wait
                for (int i = 1; i <= 3; i++) {
                    sender.table("ws_async_multi_ok")
                            .longColumn("v", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }

                // Bad row to the pre-existing table — STRING into LONG.
                // Client doesn't know the server-side schema, so this passes client-side
                // validation. The I/O thread sends it; the server rejects it.
                sender.table("ws_async_multi_err")
                        .stringColumn("value", "not a number")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                // More good rows — user thread doesn't know about the error yet
                for (int i = 4; i <= 6; i++) {
                    sender.table("ws_async_multi_ok")
                            .longColumn("v", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }

                // flush() drains the window via awaitEmpty() — error surfaces here
                sender.flush();
                Assert.fail("Expected LineSenderException from bad batch");
            } catch (LineSenderException e) {
                Assert.assertTrue(
                        "Error message should indicate server error: " + e.getMessage(),
                        e.getMessage().contains("WRITE_ERROR")
                                || e.getMessage().contains("Processing failed")
                                || e.getMessage().contains("Server error")
                                || e.getMessage().contains("failed")
                );
            }
            // The initial setup row (value=0) must be present
            assertSql(
                    "SELECT value FROM ws_async_multi_err WHERE value = 0",
                    "value\n0\n"
            );
        });
    }

    /**
     * Tests that a sender can recover after a type-mismatch error by reconnecting.
     * <p>
     * Sends a bad batch (string into a long column), catches the error,
     * creates a new sender (fresh connection), sends a valid batch, and
     * verifies the valid data landed.
     * <p>
     * Only runs in sync mode (window=1) where error propagation is immediate.
     */
    @Test
    public void testErrorRecovery_reconnectAfterTypeMismatch() throws Exception {
        Assume.assumeTrue("Window=1 only", windowSize == 1);

        runInContext((port) -> {
            // Step 1: create table with a long column
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_error_recovery")
                        .longColumn("value", 100)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }
            drainWalQueue();

            // Step 2: send a type-mismatch batch — string into long column
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_error_recovery")
                        .stringColumn("value", "not a number")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();
                Assert.fail("Expected LineSenderException for type mismatch");
            } catch (LineSenderException e) {
                Assert.assertTrue("Error should indicate server error: " + e.getMessage(),
                        e.getMessage().contains("WRITE_ERROR") ||
                                e.getMessage().contains("Processing failed") ||
                                e.getMessage().contains("Server error"));
            }

            // Step 3: reconnect with a fresh sender, send valid data
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_error_recovery")
                        .longColumn("value", 200)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.flush();
            }
            drainWalQueue();

            // Step 4: verify both valid rows landed (the bad row should not)
            assertSql(
                    "SELECT value FROM ws_error_recovery ORDER BY value",
                    "value\n100\n200\n"
            );
        });
    }

    /**
     * Tests error recovery with multiple valid batches after a failure.
     * <p>
     * Ensures the server accepts multiple successive batches from a new
     * connection after a previous connection's batch failed.
     * <p>
     * Only runs in sync mode (window=1) where error propagation is immediate.
     */
    @Test
    public void testErrorRecovery_reconnectMultipleBatchesAfterFailure() throws Exception {
        Assume.assumeTrue("Window=1 only", windowSize == 1);

        runInContext((port) -> {
            // Step 1: create table with a long column
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_error_recovery_multi")
                        .longColumn("value", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }
            drainWalQueue();

            // Step 2: send a type-mismatch batch
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_error_recovery_multi")
                        .stringColumn("value", "bad data")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();
                Assert.fail("Expected LineSenderException for type mismatch");
            } catch (LineSenderException e) {
                // expected
            }

            // Step 3: reconnect and send three valid batches
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int batch = 0; batch < 3; batch++) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("ws_error_recovery_multi")
                                .longColumn("value", (batch + 1) * 100 + i)
                                .at(1_000_000_000_002L + batch * 5 + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
            }

            // Wait for WAL to apply all transactions
            drainWalQueue();

            // Step 4: verify 16 rows total (1 initial + 15 recovered)
            assertSql(
                    "SELECT count() FROM ws_error_recovery_multi",
                    "count\n16\n"
            );
        });
    }

    @Test
    public void testFiveBoolsAllTrue() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_bools_true")
                        .boolColumn("a", true)
                        .boolColumn("b", true)
                        .boolColumn("c", true)
                        .boolColumn("d", true)
                        .boolColumn("e", true)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT a,b,c,d,e FROM ws_test_bools_true", "a\tb\tc\td\te\ntrue\ttrue\ttrue\ttrue\ttrue\n");
        });
    }

    @Test
    public void testFiveSymbols() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_five_syms")
                        .symbol("a", "1")
                        .symbol("b", "2")
                        .symbol("c", "3")
                        .symbol("d", "4")
                        .symbol("e", "5")
                        .longColumn("value", 42)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT a,b,c,d,e FROM ws_test_five_syms", "a\tb\tc\td\te\n1\t2\t3\t4\t5\n");
        });
    }

    /**
     * Tests the QWP-specific floatColumn() method that encodes a native FLOAT wire type.
     * Pre-creates a FLOAT column to verify the client sends the correct type code.
     */
    @Test
    public void testFloatColumn_directWrite() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_float_direct (" +
                    "value FLOAT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_float_direct")
                        .floatColumn("value", 0.0f)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_float_direct")
                        .floatColumn("value", 1.5f)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_float_direct")
                        .floatColumn("value", -3.75f)
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_float_direct")
                        .floatColumn("value", 1000.5f)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_float_direct", "count\n4\n");
            assertSql(
                    "SELECT value FROM ws_float_direct ORDER BY ts",
                    "value\n0.0\n1.5\n-3.75\n1000.5\n"
            );
        });
    }

    @Test
    public void testFlushAfterEveryRow() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_test_flush_each")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    sender.flush();
                }
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_flush_each", "count\n10\n");
        });
    }

    @Test
    public void testFlushEmpty() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Flush with no data should not throw
                sender.flush();
                sender.flush();
                sender.flush();
            }
            // If we get here without exception, test passes
        });
    }

    @Test
    public void testFlushEvery10Rows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 100; i++) {
                    sender.table("ws_test_flush_10")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    if ((i + 1) % 10 == 0) {
                        sender.flush();
                    }
                }
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_flush_10", "count\n100\n");
        });
    }

    @Test
    public void testFlushEvery5Rows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 50; i++) {
                    sender.table("ws_test_flush_5")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    if ((i + 1) % 5 == 0) {
                        sender.flush();
                    }
                }
                sender.flush(); // flush remaining
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_flush_5", "count\n50\n");
        });
    }

    /**
     * Tests GEOHASH(1c) = 5 bits (GEOBYTE) via the native GeoHash wire protocol.
     * Pre-creates the table, sends a GeoHash value through the full path:
     * client sender -> WebSocket -> server decoder -> QwpWalAppender -> WAL -> SQL query.
     */
    @Test
    public void testGeoHash_byteResolution() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_geohash_byte (" +
                    "geo GEOHASH(1c), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            String geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz";

            try (QwpWebSocketSender sender = createSender(port)) {
                QwpTableBuffer buf = sender.getTableBuffer("ws_geohash_byte");
                QwpTableBuffer.ColumnBuffer geoCol = buf.getOrCreateColumn("geo", TYPE_GEOHASH, false);

                for (int i = 0; i < 30; i++) {
                    geoCol.addGeoHash(GeoHashes.fromString(String.valueOf(geoAlphabet.charAt(i))), 5);
                    sender.at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_geohash_byte", "count\n30\n");
            assertSql(
                    "SELECT geo FROM ws_geohash_byte ORDER BY ts LIMIT 3",
                    "geo\n0\n1\n2\n"
            );
            assertSql(
                    "SELECT geo FROM ws_geohash_byte ORDER BY ts DESC LIMIT 3",
                    "geo\nx\nw\nv\n"
            );
        });
    }

    /**
     * Tests GEOHASH(6c) = 30 bits (GEOINT) via the native GeoHash wire protocol.
     */
    @Test
    public void testGeoHash_intResolution() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_geohash_int (" +
                    "geo GEOHASH(6c), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            String geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz";

            try (QwpWebSocketSender sender = createSender(port)) {
                QwpTableBuffer buf = sender.getTableBuffer("ws_geohash_int");
                QwpTableBuffer.ColumnBuffer geoCol = buf.getOrCreateColumn("geo", TYPE_GEOHASH, false);

                for (int i = 0; i < 30; i++) {
                    String hash = String.valueOf(geoAlphabet.charAt(i)).repeat(6);
                    geoCol.addGeoHash(GeoHashes.fromString(hash), 30);
                    sender.at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_geohash_int", "count\n30\n");
            assertSql(
                    "SELECT geo FROM ws_geohash_int ORDER BY ts LIMIT 3",
                    "geo\n000000\n111111\n222222\n"
            );
            assertSql(
                    "SELECT geo FROM ws_geohash_int ORDER BY ts DESC LIMIT 3",
                    "geo\nxxxxxx\nwwwwww\nvvvvvv\n"
            );
        });
    }

    /**
     * Tests GEOHASH(12c) = 60 bits (GEOLONG) via the native GeoHash wire protocol.
     */
    @Test
    public void testGeoHash_longResolution() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_geohash_long (" +
                    "geo GEOHASH(12c), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            String geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz";

            try (QwpWebSocketSender sender = createSender(port)) {
                QwpTableBuffer buf = sender.getTableBuffer("ws_geohash_long");
                QwpTableBuffer.ColumnBuffer geoCol = buf.getOrCreateColumn("geo", TYPE_GEOHASH, false);

                for (int i = 0; i < 30; i++) {
                    String hash = String.valueOf(geoAlphabet.charAt(i)).repeat(12);
                    geoCol.addGeoHash(GeoHashes.fromString(hash), 60);
                    sender.at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_geohash_long", "count\n30\n");
            assertSql(
                    "SELECT geo FROM ws_geohash_long ORDER BY ts LIMIT 3",
                    "geo\n000000000000\n111111111111\n222222222222\n"
            );
            assertSql(
                    "SELECT geo FROM ws_geohash_long ORDER BY ts DESC LIMIT 3",
                    "geo\nxxxxxxxxxxxx\nwwwwwwwwwwww\nvvvvvvvvvvvv\n"
            );
        });
    }

    /**
     * Tests sending multiple GeoHash rows via the native GeoHash wire protocol.
     * Verifies all rows are stored and queryable.
     */
    @Test
    public void testGeoHash_multipleRows() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_geohash_multi (" +
                    "geo GEOHASH(6c), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            String geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz";

            try (QwpWebSocketSender sender = createSender(port)) {
                QwpTableBuffer buf = sender.getTableBuffer("ws_geohash_multi");
                QwpTableBuffer.ColumnBuffer geoCol = buf.getOrCreateColumn("geo", TYPE_GEOHASH, false);

                for (int i = 0; i < 30; i++) {
                    // Build 6-char geohash rotating through the alphabet
                    StringBuilder hash = new StringBuilder(6);
                    for (int j = 0; j < 6; j++) {
                        hash.append(geoAlphabet.charAt((i + j) % 32));
                    }
                    geoCol.addGeoHash(GeoHashes.fromString(hash.toString()), 30);
                    sender.at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_geohash_multi", "count\n30\n");
            assertSql(
                    "SELECT geo FROM ws_geohash_multi ORDER BY ts LIMIT 3",
                    "geo\n012345\n123456\n234567\n"
            );
            assertSql(
                    "SELECT geo FROM ws_geohash_multi ORDER BY ts DESC LIMIT 3",
                    "geo\nxyz012\nwxyz01\nvwxyz0\n"
            );
        });
    }

    /**
     * Tests null GeoHash values interleaved with non-null values.
     * Verifies correct null handling through the full path.
     */
    @Test
    public void testGeoHash_nullable() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_geohash_null (" +
                    "geo GEOHASH(6c), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            String geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz";

            try (QwpWebSocketSender sender = createSender(port)) {
                QwpTableBuffer buf = sender.getTableBuffer("ws_geohash_null");
                QwpTableBuffer.ColumnBuffer geoCol = buf.getOrCreateColumn("geo", TYPE_GEOHASH, true);

                // 30 rows: even-indexed rows get a geohash, odd-indexed rows get null
                for (int i = 0; i < 30; i++) {
                    if (i % 2 == 0) {
                        String hash = String.valueOf(geoAlphabet.charAt(i / 2)).repeat(6);
                        geoCol.addGeoHash(GeoHashes.fromString(hash), 30);
                    } else {
                        geoCol.addNull();
                    }
                    sender.at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_geohash_null", "count\n30\n");
            assertSql(
                    "SELECT count() FROM ws_geohash_null WHERE geo IS NULL",
                    "count\n15\n"
            );
            assertSql(
                    "SELECT geo FROM ws_geohash_null ORDER BY ts LIMIT 4",
                    "geo\n000000\n\n111111\n\n"
            );
        });
    }

    /**
     * Tests GEOHASH(4c) = 20 bits (GEOINT) via the native GeoHash wire protocol.
     */
    @Test
    public void testGeoHash_shortResolution() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_geohash_short (" +
                    "geo GEOHASH(4c), " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            String geoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz";

            try (QwpWebSocketSender sender = createSender(port)) {
                QwpTableBuffer buf = sender.getTableBuffer("ws_geohash_short");
                QwpTableBuffer.ColumnBuffer geoCol = buf.getOrCreateColumn("geo", TYPE_GEOHASH, false);

                for (int i = 0; i < 30; i++) {
                    String hash = String.valueOf(geoAlphabet.charAt(i)).repeat(4);
                    geoCol.addGeoHash(GeoHashes.fromString(hash), 20);
                    sender.at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_geohash_short", "count\n30\n");
            assertSql(
                    "SELECT geo FROM ws_geohash_short ORDER BY ts LIMIT 3",
                    "geo\n0000\n1111\n2222\n"
            );
            assertSql(
                    "SELECT geo FROM ws_geohash_short ORDER BY ts DESC LIMIT 3",
                    "geo\nxxxx\nwwww\nvvvv\n"
            );
        });
    }

    @Test
    public void testGorillaDisabled_dataCorrectness() throws Exception {
        runInContext((port) -> {
            int rowCount = 100;
            long baseTimestamp = 1_704_067_200_000_000L; // 2024-01-01T00:00:00 in micros

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.setGorillaEnabled(false);

                for (int i = 0; i < rowCount; i++) {
                    sender.table("ws_gorilla_disabled")
                            .longColumn("value", i)
                            .at(baseTimestamp + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_gorilla_disabled", "count\n100\n");
            assertSql("SELECT sum(value) FROM ws_gorilla_disabled", "sum\n4950\n");
            assertSql(
                    "SELECT min(timestamp), max(timestamp) FROM ws_gorilla_disabled",
                    """
                            min\tmax
                            2024-01-01T00:00:00.000000Z\t2024-01-01T00:00:00.000099Z
                            """
            );
        });
    }

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
        Assume.assumeTrue("Async mode only (window > 1)", windowSize > 1);

        runInContext((port) -> {
            int inFlightWindowSize = 100;
            int totalRows = 10_000;

            // Create sender with high in-flight window
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null,
                    10,                             // autoFlushRows = 10: batch every 10 rows
                    Integer.MAX_VALUE,              // autoFlushBytes: disabled
                    TimeUnit.HOURS.toNanos(1),      // autoFlushInterval: disabled
                    inFlightWindowSize,
                    null
            )) {
                for (int i = 0; i < totalRows; i++) {
                    sender.table("ws_ack_test")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i * 1_000_000L, ChronoUnit.MICROS);
                }
                // This should succeed - cumulative ACKs handle the batch volume
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_ack_test", "count\n" + totalRows + "\n");
        });
    }

    /**
     * Tests that a type-mismatch error surfaces on flush() in both sync and
     * async modes.
     * <p>
     * Creates a table with a LONG column, then sends a STRING into it via a
     * fresh connection. flush() must throw in both modes because it always
     * waits for all pending ACKs before returning.
     */
    @Test
    public void testImmediateErrorPropagation_typeMismatchOnFlush() throws Exception {
        runInContext((port) -> {
            // First sender: create table with long column
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_error_propagation_test")
                        .longColumn("value", 42)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            // Second sender: fresh connection, no client-side column cache
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_error_propagation_test")
                        .stringColumn("value", "not a number")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue("Error message should indicate server error: " + e.getMessage(),
                        e.getMessage().contains("WRITE_ERROR") ||
                                e.getMessage().contains("Processing failed") ||
                                e.getMessage().contains("Server error"));
            }
        });
    }

    /**
     * Tests the QWP-specific intColumn() method that encodes a native INT wire type.
     * Pre-creates an INT column to verify the client sends the correct type code.
     */
    @Test
    public void testIntColumn_directWrite() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_int_direct (" +
                    "value INT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_int_direct")
                        .intColumn("value", 0)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_int_direct")
                        .intColumn("value", 2_147_483_647)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_int_direct")
                        .intColumn("value", -1)
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_int_direct")
                        .intColumn("value", 123_456_789)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_int_direct", "count\n4\n");
            assertSql(
                    "SELECT value FROM ws_int_direct ORDER BY ts",
                    "value\n0\n2147483647\n-1\n123456789\n"
            );
        });
    }

    @Test
    public void testInterleavedTables() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Interleave rows between tables
                for (int i = 0; i < 20; i++) {
                    sender.table("ws_interleave_a")
                            .symbol("id", "a" + i)
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i * 2, ChronoUnit.MICROS);

                    sender.table("ws_interleave_b")
                            .symbol("id", "b" + i)
                            .longColumn("value", i * 10)
                            .at(1_000_000_000_000L + i * 2 + 1, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_interleave_a", "count\n20\n");
            assertSql("SELECT count() FROM ws_interleave_b", "count\n20\n");
        });
    }

    @Test
    public void testLargeArray() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                double[] largeArray = new double[1000];
                for (int i = 0; i < largeArray.length; i++) {
                    largeArray[i] = i * 0.1;
                }
                sender.table("ws_test_large_array")
                        .doubleArray("values", largeArray)
                        .at(1_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_large_array", "count\n1\n");
        });
    }

    @Test
    public void testLargePayloadPerRow() throws Exception {
        runInContext((port) -> {
            // ~100KB string per row
            String largeString = "x".repeat(100_000);
            // 10K-element array per row (~80KB)
            double[] largeArray = new double[10_000];
            for (int i = 0; i < largeArray.length; i++) {
                largeArray[i] = i * 0.01;
            }

            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 3; i++) {
                    sender.table("ws_test_large_payload")
                            .symbol("id", "row" + i)
                            .stringColumn("big_text", largeString)
                            .doubleArray("big_array", largeArray)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    sender.flush();
                }
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM ws_test_large_payload",
                    "count\n3\n"
            );
            assertSql(
                    "SELECT length(big_text) FROM ws_test_large_payload LIMIT 1",
                    "length\n100000\n"
            );
        }, 1_048_576);
    }

    @Test
    public void testLargeStringColumn() throws Exception {
        runInContext((port) -> {
            String largeString = "x".repeat(1000);
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_large_string")
                        .symbol("id", "row1")
                        .stringColumn("large_data", largeString)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT length(large_data) FROM ws_test_large_string", "length\n1000\n");
        });
    }

    /**
     * Two tables with different schemas flushed in a single message.
     * Verifies that both tables are created with correct schemas and data.
     */
    @Test
    public void testMultiTable_differentSchemas() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_mt_schema_a")
                        .longColumn("id", 1)
                        .stringColumn("name", "alice")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_mt_schema_a")
                        .longColumn("id", 2)
                        .stringColumn("name", "bob")
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("ws_mt_schema_b")
                        .doubleColumn("price", 99.5)
                        .boolColumn("active", true)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            assertSql(
                    "SELECT id, name FROM ws_mt_schema_a ORDER BY id",
                    """
                            id\tname
                            1\talice
                            2\tbob
                            """
            );
            assertSql(
                    "SELECT price, active FROM ws_mt_schema_b",
                    """
                            price\tactive
                            99.5\ttrue
                            """
            );
        });
    }

    /**
     * Interleaved rows across multiple tables in a single flush.
     * Rows for different tables are interleaved to verify the client
     * correctly groups them by table in the multi-table message.
     */
    @Test
    public void testMultiTable_interleavedRows() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 100; i++) {
                    sender.table("ws_mt_interleave_a")
                            .longColumn("val", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    sender.table("ws_mt_interleave_b")
                            .longColumn("val", i * 10)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    sender.table("ws_mt_interleave_c")
                            .longColumn("val", i * 100)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_mt_interleave_a", "count\n100\n");
            assertSql("SELECT count() FROM ws_mt_interleave_b", "count\n100\n");
            assertSql("SELECT count() FROM ws_mt_interleave_c", "count\n100\n");
            assertSql("SELECT sum(val) FROM ws_mt_interleave_a", "sum\n4950\n");
            assertSql("SELECT sum(val) FROM ws_mt_interleave_b", "sum\n49500\n");
            assertSql("SELECT sum(val) FROM ws_mt_interleave_c", "sum\n495000\n");
        });
    }

    /**
     * Large number of rows across multiple tables in a single flush.
     * Exercises multi-table encoding with significant payload sizes.
     */
    @Test
    public void testMultiTable_largeRowCounts() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 1000; i++) {
                    sender.table("ws_mt_large_a")
                            .longColumn("id", i)
                            .doubleColumn("val", i * 1.5)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    sender.table("ws_mt_large_b")
                            .longColumn("id", i)
                            .stringColumn("data", "row_" + i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_mt_large_a", "count\n1000\n");
            assertSql("SELECT count() FROM ws_mt_large_b", "count\n1000\n");
            assertSql("SELECT sum(id) FROM ws_mt_large_a", "sum\n499500\n");
            assertSql("SELECT sum(id) FROM ws_mt_large_b", "sum\n499500\n");
        });
    }

    /**
     * Many tables (10) in a single flush. Exercises the multi-table header
     * with a larger table count.
     */
    @Test
    public void testMultiTable_manyTables() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int t = 0; t < 10; t++) {
                    for (int r = 0; r < 20; r++) {
                        sender.table("ws_mt_many_" + t)
                                .longColumn("val", t * 100 + r)
                                .at(1_000_000_000_000L + r, ChronoUnit.MICROS);
                    }
                }
                sender.flush();
            }

            for (int i = 0; i < 10; i++) {
                drainWalQueue();
            }
            for (int t = 0; t < 10; t++) {
                assertSql(
                        "SELECT count() FROM ws_mt_many_" + t,
                        "count\n20\n"
                );
            }
            // Verify a specific table's data
            assertSql("SELECT min(val) FROM ws_mt_many_3", "min\n300\n");
            assertSql("SELECT max(val) FROM ws_mt_many_3", "max\n319\n");
        });
    }

    /**
     * Multiple flushes with the same tables. The second flush should use
     * schema references since the schema was sent in the first flush.
     */
    @Test
    public void testMultiTable_multipleFlushesWithSchemaRef() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // First flush: full schema sent
                sender.table("ws_mt_schemaref_a")
                        .longColumn("id", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_mt_schemaref_b")
                        .doubleColumn("price", 10.0)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Second flush: schema ref should be used for both tables
                sender.table("ws_mt_schemaref_a")
                        .longColumn("id", 2)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.table("ws_mt_schemaref_b")
                        .doubleColumn("price", 20.0)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();

                // Third flush: add a third table (full schema) alongside existing ones (schema ref)
                sender.table("ws_mt_schemaref_a")
                        .longColumn("id", 3)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.table("ws_mt_schemaref_b")
                        .doubleColumn("price", 30.0)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.table("ws_mt_schemaref_c")
                        .stringColumn("name", "new_table")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_mt_schemaref_a", "count\n3\n");
            assertSql("SELECT sum(id) FROM ws_mt_schemaref_a", "sum\n6\n");
            assertSql("SELECT count() FROM ws_mt_schemaref_b", "count\n3\n");
            assertSql("SELECT sum(price) FROM ws_mt_schemaref_b", "sum\n60.0\n");
            assertSql("SELECT count() FROM ws_mt_schemaref_c", "count\n1\n");
        });
    }

    /**
     * Multiple tables sharing symbols via the global symbol dictionary.
     * All tables use the same symbol values; the delta dict is shared
     * at the message level.
     */
    @Test
    public void testMultiTable_sharedSymbols() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_mt_sym_trades")
                        .symbol("ticker", "AAPL")
                        .longColumn("qty", 100)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_mt_sym_trades")
                        .symbol("ticker", "GOOG")
                        .longColumn("qty", 200)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("ws_mt_sym_quotes")
                        .symbol("ticker", "AAPL")
                        .doubleColumn("bid", 150.0)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_mt_sym_quotes")
                        .symbol("ticker", "MSFT")
                        .doubleColumn("bid", 300.0)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);

                sender.table("ws_mt_sym_meta")
                        .symbol("ticker", "GOOG")
                        .stringColumn("exchange", "NASDAQ")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_mt_sym_trades", "count\n2\n");
            assertSql("SELECT count() FROM ws_mt_sym_quotes", "count\n2\n");
            assertSql("SELECT count() FROM ws_mt_sym_meta", "count\n1\n");
            assertSql(
                    "SELECT ticker, qty FROM ws_mt_sym_trades ORDER BY ticker",
                    """
                            ticker\tqty
                            AAPL\t100
                            GOOG\t200
                            """
            );
            assertSql(
                    "SELECT ticker, bid FROM ws_mt_sym_quotes ORDER BY ticker",
                    """
                            ticker\tbid
                            AAPL\t150.0
                            MSFT\t300.0
                            """
            );
        });
    }

    /**
     * Symbols shared across tables with progressive accumulation over
     * multiple flushes. Each flush adds new symbols to the global dictionary
     * while reusing previously sent ones.
     */
    @Test
    public void testMultiTable_sharedSymbolsProgressiveFlush() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Flush 1: introduce AAPL in both tables
                sender.table("ws_mt_symprog_a")
                        .symbol("sym", "AAPL")
                        .longColumn("v", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_mt_symprog_b")
                        .symbol("sym", "AAPL")
                        .longColumn("v", 10)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Flush 2: reuse AAPL (no delta), add GOOG (delta)
                sender.table("ws_mt_symprog_a")
                        .symbol("sym", "AAPL")
                        .longColumn("v", 2)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.table("ws_mt_symprog_b")
                        .symbol("sym", "GOOG")
                        .longColumn("v", 20)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();

                // Flush 3: reuse AAPL and GOOG, add MSFT
                sender.table("ws_mt_symprog_a")
                        .symbol("sym", "GOOG")
                        .longColumn("v", 3)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.table("ws_mt_symprog_b")
                        .symbol("sym", "MSFT")
                        .longColumn("v", 30)
                        .at(1_000_000_000_002L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_mt_symprog_a", "count\n3\n");
            assertSql("SELECT count() FROM ws_mt_symprog_b", "count\n3\n");
            assertSql(
                    "SELECT sym, v FROM ws_mt_symprog_a ORDER BY v",
                    """
                            sym\tv
                            AAPL\t1
                            AAPL\t2
                            GOOG\t3
                            """
            );
            assertSql(
                    "SELECT sym, v FROM ws_mt_symprog_b ORDER BY v",
                    """
                            sym\tv
                            AAPL\t10
                            GOOG\t20
                            MSFT\t30
                            """
            );
        });
    }

    /**
     * Multiple tables with varied column types in a single flush.
     * Each table uses a different mix of column types.
     */
    @Test
    public void testMultiTable_variedColumnTypes() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_mt_types_longs")
                        .longColumn("a", 1)
                        .longColumn("b", 2)
                        .longColumn("c", 3)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("ws_mt_types_doubles")
                        .doubleColumn("x", 1.1)
                        .doubleColumn("y", 2.2)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("ws_mt_types_strings")
                        .stringColumn("msg", "hello")
                        .stringColumn("tag", "world")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("ws_mt_types_bools")
                        .boolColumn("flag", true)
                        .longColumn("val", 42)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.table("ws_mt_types_mixed")
                        .symbol("sym", "X")
                        .longColumn("num", 99)
                        .doubleColumn("frac", 3.14)
                        .stringColumn("label", "pi")
                        .boolColumn("ok", false)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);

                sender.flush();
            }

            for (int i = 0; i < 5; i++) {
                drainWalQueue();
            }
            assertSql("SELECT a + b + c FROM ws_mt_types_longs", "column\n6\n");
            assertSql("SELECT x, y FROM ws_mt_types_doubles", "x\ty\n1.1\t2.2\n");
            assertSql("SELECT msg, tag FROM ws_mt_types_strings", "msg\ttag\nhello\tworld\n");
            assertSql("SELECT flag, val FROM ws_mt_types_bools", "flag\tval\ntrue\t42\n");
            assertSql(
                    "SELECT sym, num, frac, label, ok FROM ws_mt_types_mixed",
                    """
                            sym\tnum\tfrac\tlabel\tok
                            X\t99\t3.14\tpi\tfalse
                            """
            );
        });
    }

    /**
     * Tests all narrowing paths together in a single table with multiple rows.
     */
    @Test
    public void testNarrowing_AllTypesMultipleRows() throws Exception {
        runInContext((port) -> {
            // Pre-create table with all narrow types
            execute("CREATE TABLE ws_narrow_all (" +
                    "byte_val BYTE, " +
                    "short_val SHORT, " +
                    "int_val INT, " +
                    "float_val FLOAT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 100; i++) {
                    sender.table("ws_narrow_all")
                            .longColumn("byte_val", i % 128)
                            .longColumn("short_val", i * 100)
                            .longColumn("int_val", i * 10_000)
                            .doubleColumn("float_val", i * 1.5)
                            .at(1_704_067_200_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_narrow_all", "count\n100\n");

            // Verify first and last rows
            assertSql(
                    "SELECT byte_val, short_val, int_val, float_val FROM ws_narrow_all ORDER BY ts LIMIT 1",
                    "byte_val\tshort_val\tint_val\tfloat_val\n0\t0\t0\t0.0\n"
            );
            assertSql(
                    "SELECT byte_val, short_val, int_val, float_val FROM ws_narrow_all ORDER BY ts DESC LIMIT 1",
                    "byte_val\tshort_val\tint_val\tfloat_val\n99\t9900\t990000\t148.5\n"
            );
        });
    }

    /**
     * Tests DOUBLE to FLOAT narrowing by pre-creating a table with FLOAT column
     * and sending DOUBLE values over QWP v1.
     */
    @Test
    public void testNarrowing_DoubleToFloat() throws Exception {
        runInContext((port) -> {
            // Pre-create table with FLOAT column
            execute("CREATE TABLE ws_narrow_float (" +
                    "value FLOAT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_narrow_float")
                        .doubleColumn("value", 0.0)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_narrow_float")
                        .doubleColumn("value", 3.14159)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_narrow_float")
                        .doubleColumn("value", -2.71828)
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_narrow_float")
                        .doubleColumn("value", 1000.5)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_narrow_float", "count\n4\n");
            // Note: FLOAT has less precision than DOUBLE, so values are rounded
            assertSql(
                    "SELECT value FROM ws_narrow_float ORDER BY ts",
                    "value\n0.0\n3.14159\n-2.71828\n1000.5\n"
            );
        });
    }

    /**
     * Tests LONG to BYTE narrowing by pre-creating a table with BYTE column
     * and sending LONG values over QWP v1.
     */
    @Test
    public void testNarrowing_LongToByte() throws Exception {
        runInContext((port) -> {
            // Pre-create table with BYTE column
            execute("CREATE TABLE ws_narrow_byte (" +
                    "value BYTE, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_narrow_byte")
                        .longColumn("value", 0)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_narrow_byte")
                        .longColumn("value", 127)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_narrow_byte")
                        .longColumn("value", -128)
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_narrow_byte")
                        .longColumn("value", -1)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_narrow_byte", "count\n4\n");
            assertSql(
                    "SELECT value FROM ws_narrow_byte ORDER BY ts",
                    "value\n0\n127\n-128\n-1\n"
            );
        });
    }

    /**
     * Tests LONG to INT narrowing by pre-creating a table with INT column
     * and sending LONG values over QWP v1.
     */
    @Test
    public void testNarrowing_LongToInt() throws Exception {
        runInContext((port) -> {
            // Pre-create table with INT column
            execute("CREATE TABLE ws_narrow_int (" +
                    "value INT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_narrow_int")
                        .longColumn("value", 0)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_narrow_int")
                        .longColumn("value", 2_147_483_647)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_narrow_int")
                        .longColumn("value", -2_147_483_648) // interpreted as null when INT
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_narrow_int")
                        .longColumn("value", 123_456_789)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_narrow_int", "count\n4\n");
            assertSql(
                    "SELECT value FROM ws_narrow_int ORDER BY ts",
                    "value\n0\n2147483647\nnull\n123456789\n"
            );
        });
    }

    /**
     * Tests LONG to SHORT narrowing by pre-creating a table with SHORT column
     * and sending LONG values over QWP v1.
     */
    @Test
    public void testNarrowing_LongToShort() throws Exception {
        runInContext((port) -> {
            // Pre-create table with SHORT column
            execute("CREATE TABLE ws_narrow_short (" +
                    "value SHORT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_narrow_short")
                        .longColumn("value", 0)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_narrow_short")
                        .longColumn("value", 32_767)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_narrow_short")
                        .longColumn("value", -32_768)
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_narrow_short")
                        .longColumn("value", 1000)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_narrow_short", "count\n4\n");
            assertSql(
                    "SELECT value FROM ws_narrow_short ORDER BY ts",
                    "value\n0\n32767\n-32768\n1000\n"
            );
        });
    }

    @Test
    public void testNegativeNumbers() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_negative")
                        .longColumn("long_val", -12_345L)
                        .longColumn("int_val", -999L)
                        .doubleColumn("double_val", -3.14)
                        .doubleColumn("float_val", -2.71)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_negative", "count\n1\n");
        });
    }

    /**
     * Tests that QWP v1 rejects writes to non-WAL tables.
     * <p>
     * QWP v1 only supports WAL tables. When attempting to write to a non-WAL table
     * (created with BYPASS WAL), the server should return an error.
     */
    @Test
    public void testNonWalTableRejected() throws Exception {
        runInContext((port) -> {
            // Create a non-WAL table via SQL
            execute("CREATE TABLE non_wal_table (" +
                    "tag SYMBOL, " +
                    "value LONG, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY BYPASS WAL");

            // Verify the table exists and is non-WAL
            assertSql(
                    "SELECT walEnabled FROM tables() WHERE table_name = 'non_wal_table'",
                    "walEnabled\nfalse\n"
            );

            // Try to write to the non-WAL table via ILP - should fail
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("non_wal_table")
                        .symbol("tag", "test")
                        .longColumn("value", 42)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
                Assert.fail("Expected LineSenderException when writing to non-WAL table");
            } catch (LineSenderException e) {
                // Expected: server rejects writes to non-WAL tables
                Assert.assertTrue("Error message should indicate table issue: " + e.getMessage(),
                        e.getMessage().contains("WRITE_ERROR"));
            }
        });
    }

    /**
     * Tests schema evolution by adding a new column in the second batch within
     * a single connection. The new column set gets a new schema ID, so the
     * client re-sends the full schema. Rows from batch 1 should have NULL for
     * the extra column.
     */
    @Test
    public void testSchemaEvolution_addColumnMidConnection() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: two columns (sym, val)
                sender.table("ws_schema_evo_add")
                        .symbol("sym", "AAA")
                        .longColumn("val", 10)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_schema_evo_add")
                        .symbol("sym", "BBB")
                        .longColumn("val", 20)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: three columns (sym, val, extra) - schema ID changes
                sender.table("ws_schema_evo_add")
                        .symbol("sym", "CCC")
                        .longColumn("val", 30)
                        .doubleColumn("extra", 3.14)
                        .at(1_000_000_002_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_schema_evo_add", "count\n3\n");
            assertSql(
                    "SELECT sym, val, extra FROM ws_schema_evo_add ORDER BY val",
                    """
                            sym\tval\textra
                            AAA\t10\tnull
                            BBB\t20\tnull
                            CCC\t30\t3.14
                            """
            );
        });
    }

    /**
     * Tests schema evolution by adding three new columns across three successive
     * batches within a single connection. Each batch introduces one more column.
     */
    @Test
    public void testSchemaEvolution_multipleNewColumns() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: columns (sym, val)
                sender.table("ws_schema_evo_multi")
                        .symbol("sym", "A")
                        .longColumn("val", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: add col_x
                sender.table("ws_schema_evo_multi")
                        .symbol("sym", "B")
                        .longColumn("val", 2)
                        .doubleColumn("col_x", 1.1)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 3: add col_y
                sender.table("ws_schema_evo_multi")
                        .symbol("sym", "C")
                        .longColumn("val", 3)
                        .doubleColumn("col_x", 2.2)
                        .stringColumn("col_y", "hello")
                        .at(1_000_000_002_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 4: add col_z
                sender.table("ws_schema_evo_multi")
                        .symbol("sym", "D")
                        .longColumn("val", 4)
                        .doubleColumn("col_x", 3.3)
                        .stringColumn("col_y", "world")
                        .boolColumn("col_z", true)
                        .at(1_000_000_003_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_schema_evo_multi", "count\n4\n");
            assertSql(
                    "SELECT sym, val, col_x, col_y, col_z FROM ws_schema_evo_multi ORDER BY val",
                    """
                            sym\tval\tcol_x\tcol_y\tcol_z
                            A\t1\tnull\t\tfalse
                            B\t2\t1.1\t\tfalse
                            C\t3\t2.2\thello\tfalse
                            D\t4\t3.3\tworld\ttrue
                            """
            );
        });
    }

    /**
     * Tests that sending columns in a different order across batches triggers
     * a schema ID change and the client re-sends the full schema. The server
     * must match columns by name regardless of order.
     */
    @Test
    public void testSchemaEvolution_reorderColumns() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: order is (sym, alpha, beta)
                sender.table("ws_schema_evo_reorder")
                        .symbol("sym", "first")
                        .longColumn("alpha", 10)
                        .doubleColumn("beta", 1.5)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();

                // Batch 2: order is (sym, beta, alpha) - reversed data columns
                sender.table("ws_schema_evo_reorder")
                        .symbol("sym", "second")
                        .doubleColumn("beta", 2.5)
                        .longColumn("alpha", 20)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_schema_evo_reorder", "count\n2\n");
            assertSql(
                    "SELECT sym, alpha, beta FROM ws_schema_evo_reorder ORDER BY alpha",
                    """
                            sym\talpha\tbeta
                            first\t10\t1.5
                            second\t20\t2.5
                            """
            );
        });
    }

    /**
     * Opens two successive connections to the same server. Each connection
     * resets the client's maxSentSchemaId, so the second sender must
     * re-send the full schema (not a reference) on its first batch. This
     * verifies that the reset-on-reconnect logic works end-to-end.
     */
    @Test
    public void testSchemaReference_newConnectionResendsFull() throws Exception {
        runInContext((port) -> {
            // Connection 1: send two batches (batch 1 = full schema, batch 2 = reference)
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int batch = 0; batch < 2; batch++) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("ws_schema_ref_reconn")
                                .symbol("src", "conn1")
                                .longColumn("seq", batch * 5 + i)
                                .at(1_000_000_000_000L + batch * 100 + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
            }

            drainWalQueue();

            // Connection 2: new sender must resend full schema, then schema ref
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int batch = 0; batch < 2; batch++) {
                    for (int i = 0; i < 5; i++) {
                        sender.table("ws_schema_ref_reconn")
                                .symbol("src", "conn2")
                                .longColumn("seq", 10 + batch * 5 + i)
                                .at(1_000_000_000_100L + batch * 100 + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
            }

            // Wait for WAL to apply connection 2's data
            drainWalQueue();
            assertSql(
                    "SELECT count() FROM ws_schema_ref_reconn",
                    "count\n20\n"
            );
            assertSql(
                    "SELECT count() FROM ws_schema_ref_reconn WHERE src = 'conn1'",
                    "count\n10\n"
            );
            assertSql(
                    "SELECT count() FROM ws_schema_ref_reconn WHERE src = 'conn2'",
                    "count\n10\n"
            );
        });
    }

    /**
     * Sends 5 batches with the same schema on a single connection. Batch 1 sends
     * the full schema; batches 2-5 implicitly use schema reference mode (varint
     * schemaId only) because the schema ID is already below maxSentSchemaId
     * after the first successful ACK.
     */
    @Test
    public void testSchemaReference_registryHitAfterMultipleBatches() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 10; i++) {
                        sender.table("ws_schema_ref_hit")
                                .symbol("tag", "batch" + batch)
                                .longColumn("value", batch * 10 + i)
                                .at(1_000_000_000_000L + batch * 100 + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM ws_schema_ref_hit",
                    "count\n50\n"
            );
            // Verify all 5 batch tags landed
            assertSql(
                    "SELECT count_distinct(tag) FROM ws_schema_ref_hit",
                    "count_distinct\n5\n"
            );
        });
    }

    /**
     * Tests the QWP-specific shortColumn() method that encodes a native SHORT wire type.
     * Pre-creates a SHORT column to verify the client sends the correct type code.
     */
    @Test
    public void testShortColumn_directWrite() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ws_short_direct (" +
                    "value SHORT, " +
                    "ts TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(ts) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_short_direct")
                        .shortColumn("value", (short) 0)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);
                sender.table("ws_short_direct")
                        .shortColumn("value", (short) 32_767)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);
                sender.table("ws_short_direct")
                        .shortColumn("value", (short) -32_768)
                        .at(1_704_067_200_000_002L, ChronoUnit.MICROS);
                sender.table("ws_short_direct")
                        .shortColumn("value", (short) -1)
                        .at(1_704_067_200_000_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_short_direct", "count\n4\n");
            assertSql(
                    "SELECT value FROM ws_short_direct ORDER BY ts",
                    "value\n0\n32767\n-32768\n-1\n"
            );
        });
    }

    @Test
    public void testSpecialCharactersInString() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_special_str")
                        .symbol("id", "row1")
                        .stringColumn("special", "hello\tworld\nnewline")
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_special_str", "count\n1\n");
        });
    }

    @Test
    public void testSpecialCharactersInSymbol() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_special_sym")
                        .symbol("special", "hello-world_123")
                        .longColumn("value", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql(
                    "SELECT special FROM ws_test_special_sym",
                    "special\nhello-world_123\n"
            );
        });
    }
    // These tests exercise the server-side symbol ID cache optimization
    // which maps clientSymbolId → tableSymbolId to bypass string lookups.

    /**
     * Tests ingestion into a pre-created STAC-like quotes table with narrow column types:
     * SYMBOL, CHAR, FLOAT, SHORT, BOOLEAN.
     * This exercises the columnar write path for all these types simultaneously.
     */
    @Test
    public void testStacQuotesSchema() throws Exception {
        runInContext((port) -> {
            // Pre-create STAC quotes table with narrow types
            execute("CREATE TABLE ws_stac_q (" +
                    "s SYMBOL, " +
                    "x CHAR, " +
                    "b FLOAT, " +
                    "a FLOAT, " +
                    "v SHORT, " +
                    "w SHORT, " +
                    "m BOOLEAN, " +
                    "T TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(T) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_stac_q")
                        .symbol("s", "AAPL")
                        .stringColumn("x", "N")
                        .doubleColumn("b", 150.25)
                        .doubleColumn("a", 150.50)
                        .longColumn("v", 100)
                        .longColumn("w", 200)
                        .boolColumn("m", true)
                        .at(1_704_067_200_000_000L, ChronoUnit.MICROS);

                sender.table("ws_stac_q")
                        .symbol("s", "MSFT")
                        .stringColumn("x", "Q")
                        .doubleColumn("b", 380.10)
                        .doubleColumn("a", 380.30)
                        .longColumn("v", 50)
                        .longColumn("w", 75)
                        .boolColumn("m", false)
                        .at(1_704_067_200_000_001L, ChronoUnit.MICROS);

                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_stac_q", "count\n2\n");
            assertSql(
                    "SELECT s, x, b, a, v, w, m FROM ws_stac_q ORDER BY T",
                    """
                            s\tx\tb\ta\tv\tw\tm
                            AAPL\tN\t150.25\t150.5\t100\t200\ttrue
                            MSFT\tQ\t380.1\t380.3\t50\t75\tfalse
                            """
            );
        });
    }

    @Test
    public void testSymbolCache_fastPath_cacheInvalidationOnWalApply() throws Exception {
        // Tests that cache is properly invalidated when watermark changes.
        // The cache uses checkAndInvalidate(watermark) to detect changes.
        Assume.assumeTrue("Sync mode only (window=1) - requires same connection", windowSize == 1);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Phase 1: Insert and commit sym_a, sym_b
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_a")
                        .longColumn("val", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_b")
                        .longColumn("val", 2)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();

                drainWalQueue();

                // Phase 2: Populate cache for sym_a, sym_b
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_a")
                        .longColumn("val", 3)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_b")
                        .longColumn("val", 4)
                        .at(1_000_000_001_001L, ChronoUnit.MICROS);
                sender.flush();

                // Phase 3: Add sym_c (new symbol)
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_c")  // NEW
                        .longColumn("val", 5)
                        .at(1_000_000_002_000L, ChronoUnit.MICROS);
                sender.flush();

                // Apply WAL - sym_c becomes committed, watermark changes
                drainWalQueue();

                // Phase 4: After WAL apply, watermark changed
                // Cache should be invalidated, but sym_a and sym_b are still in table
                // This round repopulates cache with new watermark
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_a")  // Cache invalidated, repopulate
                        .longColumn("val", 6)
                        .at(1_000_000_003_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_b")  // Cache invalidated, repopulate
                        .longColumn("val", 7)
                        .at(1_000_000_003_001L, ChronoUnit.MICROS);
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_c")  // Now committed, can be cached
                        .longColumn("val", 8)
                        .at(1_000_000_003_002L, ChronoUnit.MICROS);
                sender.flush();

                // Phase 5: All three should now hit cache
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_a")  // CACHE HIT
                        .longColumn("val", 9)
                        .at(1_000_000_004_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_b")  // CACHE HIT
                        .longColumn("val", 10)
                        .at(1_000_000_004_001L, ChronoUnit.MICROS);
                sender.table("ws_cache_invalidate")
                        .symbol("tag", "sym_c")  // CACHE HIT
                        .longColumn("val", 11)
                        .at(1_000_000_004_002L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_cache_invalidate", "count\n11\n");
            assertSql("SELECT count(distinct tag) FROM ws_cache_invalidate", "count_distinct\n3\n");
        });
    }

    @Test
    public void testSymbolCache_fastPath_highVolumeReuse() throws Exception {
        // High-volume test: many rows reusing the same small set of symbols.
        // After warmup, the vast majority should be cache hits.
        Assume.assumeTrue("Sync mode only (window=1) - requires same connection", windowSize == 1);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                String[] levels = {"DEBUG", "INFO", "WARN", "ERROR"};  // 4 symbols
                String[] sources = {"app", "db", "cache"};  // 3 symbols

                // Round 1: Initial batch to create symbols
                for (int i = 0; i < 12; i++) {  // 4*3 = 12 combinations
                    sender.table("ws_cache_high_volume")
                            .symbol("level", levels[i % levels.length])
                            .symbol("source", sources[i % sources.length])
                            .longColumn("seq", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                drainWalQueue();

                // Round 2: Populate caches
                for (int i = 0; i < 12; i++) {
                    sender.table("ws_cache_high_volume")
                            .symbol("level", levels[i % levels.length])
                            .symbol("source", sources[i % sources.length])
                            .longColumn("seq", i + 100)
                            .at(1_000_000_001_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                // Rounds 3-12: 1000 more rows, all should be cache hits
                for (int batch = 0; batch < 10; batch++) {
                    for (int i = 0; i < 100; i++) {
                        int idx = batch * 100 + i;
                        sender.table("ws_cache_high_volume")
                                .symbol("level", levels[idx % levels.length])  // CACHE HIT
                                .symbol("source", sources[idx % sources.length])  // CACHE HIT
                                .longColumn("seq", idx + 1000)
                                .at(1_000_000_002_000L + idx, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
            }

            drainWalQueue();
            // 12 + 12 + 1000 = 1024 rows
            assertSql("SELECT count() FROM ws_cache_high_volume", "count\n1024\n");
            assertSql("SELECT count(distinct level) FROM ws_cache_high_volume", "count_distinct\n4\n");
            assertSql("SELECT count(distinct source) FROM ws_cache_high_volume", "count_distinct\n3\n");
        });
    }

    @Test
    public void testSymbolCache_fastPath_manyRoundsNoWalApplyBetween() throws Exception {
        // Tests cache behavior when WAL is applied once, then many rounds of symbol reuse.
        // After initial WAL apply:
        // - Round 2: cache miss, populates cache
        // - Rounds 3-10: all cache HITS (fast path)
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                String[] symbols = {"alpha", "beta", "gamma"};

                // Round 1: New symbols
                for (int i = 0; i < 6; i++) {
                    sender.table("ws_cache_many_rounds")
                            .symbol("tag", symbols[i % symbols.length])
                            .longColumn("round", 1)
                            .longColumn("idx", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                drainWalQueue();

                // Rounds 2-10: All reuse the same symbols
                // Round 2 populates cache, Rounds 3-10 hit cache
                for (int round = 2; round <= 10; round++) {
                    for (int i = 0; i < 6; i++) {
                        sender.table("ws_cache_many_rounds")
                                .symbol("tag", symbols[i % symbols.length])
                                .longColumn("round", round)
                                .longColumn("idx", i)
                                .at(1_000_000_000_000L + (round * 1000) + i, ChronoUnit.MICROS);
                    }
                    sender.flush();
                }
            }

            drainWalQueue();
            // 6 rows * 10 rounds = 60 rows
            assertSql("SELECT count() FROM ws_cache_many_rounds", "count\n60\n");
            assertSql("SELECT count(distinct tag) FROM ws_cache_many_rounds", "count_distinct\n3\n");
            // Verify all rounds are present
            assertSql("SELECT count(distinct round) FROM ws_cache_many_rounds", "count_distinct\n10\n");
        });
    }
    // These tests exercise various interleavings of server-side commits,
    // WAL apply jobs, and sender batches.

    @Test
    public void testSymbolCache_fastPath_mixedNewAndCached() throws Exception {
        // Tests interleaving of new symbols (cache miss) and existing symbols (cache hit).
        // After WAL apply and cache warmup:
        // - Existing symbols should hit cache (fast path)
        // - New symbols should miss cache (slow path via putSym)
        Assume.assumeTrue("Sync mode only (window=1) - requires same connection", windowSize == 1);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Round 1: Initial symbols
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_a")
                        .longColumn("val", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_b")
                        .longColumn("val", 2)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();

                drainWalQueue();

                // Round 2: Populate cache for existing symbols
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_a")  // Cache miss -> populate
                        .longColumn("val", 3)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_b")  // Cache miss -> populate
                        .longColumn("val", 4)
                        .at(1_000_000_001_001L, ChronoUnit.MICROS);
                sender.flush();

                drainWalQueue();

                // Round 3: Mix of cached (fast path) and new (slow path)
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_a")  // CACHE HIT (fast path)
                        .longColumn("val", 5)
                        .at(1_000_000_002_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "new_c")  // NEW - cache miss, putSym
                        .longColumn("val", 6)
                        .at(1_000_000_002_001L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_b")  // CACHE HIT (fast path)
                        .longColumn("val", 7)
                        .at(1_000_000_002_002L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "new_d")  // NEW - cache miss, putSym
                        .longColumn("val", 8)
                        .at(1_000_000_002_003L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_a")  // CACHE HIT (fast path)
                        .longColumn("val", 9)
                        .at(1_000_000_002_004L, ChronoUnit.MICROS);
                sender.flush();

                // Round 4: All symbols now exist, but new_c and new_d not yet in cache
                // After WAL apply, they'll be committed
                drainWalQueue();

                // Round 5: new_c, new_d should now be cacheable
                sender.table("ws_cache_mixed")
                        .symbol("type", "new_c")  // Cache miss -> populate (now committed)
                        .longColumn("val", 10)
                        .at(1_000_000_003_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "new_d")  // Cache miss -> populate (now committed)
                        .longColumn("val", 11)
                        .at(1_000_000_003_001L, ChronoUnit.MICROS);
                sender.flush();

                // Round 6: All four symbols should now hit cache
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_a")  // CACHE HIT
                        .longColumn("val", 12)
                        .at(1_000_000_004_000L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "existing_b")  // CACHE HIT
                        .longColumn("val", 13)
                        .at(1_000_000_004_001L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "new_c")  // CACHE HIT
                        .longColumn("val", 14)
                        .at(1_000_000_004_002L, ChronoUnit.MICROS);
                sender.table("ws_cache_mixed")
                        .symbol("type", "new_d")  // CACHE HIT
                        .longColumn("val", 15)
                        .at(1_000_000_004_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_cache_mixed", "count\n15\n");
            assertSql("SELECT count(distinct type) FROM ws_cache_mixed", "count_distinct\n4\n");
        });
    }

    @Test
    public void testSymbolCache_fastPath_multipleColumnsIndependentCaches() throws Exception {
        // Tests that each symbol column has its own independent cache.
        // Cache for column A should not affect cache for column B.
        Assume.assumeTrue("Sync mode only (window=1) - requires same connection", windowSize == 1);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Round 1: Different symbols in each column
                for (int i = 0; i < 5; i++) {
                    sender.table("ws_cache_multi_col")
                            .symbol("col_a", "a_val_" + (i % 2))  // 2 distinct
                            .symbol("col_b", "b_val_" + (i % 3))  // 3 distinct
                            .symbol("col_c", "c_val_" + (i % 4))  // 4 distinct (but only 4 rows so actually max 4)
                            .longColumn("seq", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                drainWalQueue();

                // Round 2: Populate caches (cache miss -> SymbolMapReader)
                for (int i = 0; i < 5; i++) {
                    sender.table("ws_cache_multi_col")
                            .symbol("col_a", "a_val_" + (i % 2))
                            .symbol("col_b", "b_val_" + (i % 3))
                            .symbol("col_c", "c_val_" + (i % 4))
                            .longColumn("seq", i + 10)
                            .at(1_000_000_001_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                // Round 3: All caches should hit (fast path for all 3 columns)
                for (int i = 0; i < 5; i++) {
                    sender.table("ws_cache_multi_col")
                            .symbol("col_a", "a_val_" + (i % 2))  // CACHE HIT
                            .symbol("col_b", "b_val_" + (i % 3))  // CACHE HIT
                            .symbol("col_c", "c_val_" + (i % 4))  // CACHE HIT
                            .longColumn("seq", i + 20)
                            .at(1_000_000_002_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();

                // Round 4: Add new symbol to col_a, reuse others
                // col_a cache miss for new value, col_b and col_c still hit
                sender.table("ws_cache_multi_col")
                        .symbol("col_a", "a_val_NEW")  // NEW - cache miss
                        .symbol("col_b", "b_val_0")  // CACHE HIT
                        .symbol("col_c", "c_val_0")  // CACHE HIT
                        .longColumn("seq", 100)
                        .at(1_000_000_003_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_cache_multi_col", "count\n16\n");
            assertSql("SELECT count(distinct col_a) FROM ws_cache_multi_col", "count_distinct\n3\n");
            assertSql("SELECT count(distinct col_b) FROM ws_cache_multi_col", "count_distinct\n3\n");
            assertSql("SELECT count(distinct col_c) FROM ws_cache_multi_col", "count_distinct\n4\n");
        });
    }

    @Test
    public void testSymbolCache_fastPath_sameConnectionThreeRounds() throws Exception {
        // This test exercises the cache fast path by:
        // Round 1: Insert new symbols (cache miss, putSym - symbols are local/uncommitted)
        // WAL apply: Symbols become committed
        // Round 2: Reuse symbols (cache miss, SymbolMapReader finds them, cache populated)
        // Round 3: Reuse symbols again (CACHE HIT - fast path via putSymIndex)
        //
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Round 1: Insert new symbols
                // These go through putSym() since they're new (not in committed table)
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "device_" + (i % 3))  // 3 distinct values
                            .symbol("status", i % 2 == 0 ? "online" : "offline")  // 2 distinct values
                            .longColumn("seq", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                // Apply WAL - symbols become committed to table
                drainWalQueue();

                // Round 2: Reuse the SAME symbols
                // Cache miss -> SymbolMapReader.keyOf() finds committed symbols -> cache populated
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "device_" + (i % 3))  // Same 3 values
                            .symbol("status", i % 2 == 0 ? "online" : "offline")  // Same 2 values
                            .longColumn("seq", i + 100)
                            .at(1_000_000_001_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                // Apply WAL again
                drainWalQueue();

                // Round 3: Reuse the SAME symbols AGAIN
                // NOW the cache should be populated from Round 2
                // This should hit the FAST PATH: cachedTableId != NO_ENTRY && cachedTableId < watermark
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "device_" + (i % 3))  // Cache HIT!
                            .symbol("status", i % 2 == 0 ? "online" : "offline")  // Cache HIT!
                            .longColumn("seq", i + 200)
                            .at(1_000_000_002_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                // Round 4: Even more reuse to maximize cache hits
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "device_" + (i % 3))  // Cache HIT!
                            .symbol("status", i % 2 == 0 ? "online" : "offline")  // Cache HIT!
                            .longColumn("seq", i + 300)
                            .at(1_000_000_003_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_cache_fast_path", "count\n40\n");
            assertSql("SELECT count(distinct device) FROM ws_cache_fast_path", "count_distinct\n3\n");
            assertSql("SELECT count(distinct status) FROM ws_cache_fast_path", "count_distinct\n2\n");
        });
    }

    @Test
    public void testSymbolCache_fastPath_sameConnectionThreeRounds_singleSymbol() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "foo")
                            .longColumn("seq", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                drainWalQueue();

                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "foo")
                            .longColumn("seq", i + 100)
                            .at(1_000_000_001_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                drainWalQueue();

                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "foo")
                            .longColumn("seq", i + 200)
                            .at(1_000_000_002_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                // Round 4: Even more reuse to maximize cache hits
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_fast_path")
                            .symbol("device", "foo")
                            .longColumn("seq", i + 300)
                            .at(1_000_000_003_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_cache_fast_path", "count\n40\n");
            assertSql("SELECT count(distinct device) FROM ws_cache_fast_path", "count_distinct\n1\n");
        });
    }

    @Test
    public void testSymbolCache_manyRepeatedSymbols() throws Exception {
        // This test sends many rows with repeated symbols to exercise the cache.
        // The cache should provide a performance benefit by avoiding string lookups
        // for repeated symbols.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                String[] regions = {"us-east", "us-west", "eu-west", "eu-central", "asia-pacific"};
                String[] hosts = {"host1", "host2", "host3", "host4", "host5",
                        "host6", "host7", "host8", "host9", "host10"};

                // Send 1000 rows with repeated symbol values
                for (int i = 0; i < 1000; i++) {
                    sender.table("ws_symbol_cache_test")
                            .symbol("region", regions[i % regions.length])
                            .symbol("host", hosts[i % hosts.length])
                            .longColumn("cpu_usage", i % 100)
                            .doubleColumn("memory_free", 1024.0 + (i % 512))
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);

                    // Flush every 100 rows
                    if ((i + 1) % 100 == 0) {
                        sender.flush();
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_symbol_cache_test", "count\n1000\n");
            assertSql("SELECT count(distinct region) FROM ws_symbol_cache_test", "count_distinct\n5\n");
            assertSql("SELECT count(distinct host) FROM ws_symbol_cache_test", "count_distinct\n10\n");
        });
    }

    @Test
    public void testSymbolCache_multipleColumnsWithRepeatedSymbols() throws Exception {
        // Tests caching with multiple symbol columns in the same table.
        // Each column should have its own independent cache.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                String[] devices = {"sensor_a", "sensor_b", "sensor_c"};
                String[] locations = {"floor1", "floor2"};
                String[] types = {"temperature", "humidity", "pressure"};

                for (int i = 0; i < 100; i++) {
                    sender.table("ws_multi_symbol_cols")
                            .symbol("device", devices[i % devices.length])
                            .symbol("location", locations[i % locations.length])
                            .symbol("measurement_type", types[i % types.length])
                            .doubleColumn("value", 20.0 + (i % 30))
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_multi_symbol_cols", "count\n100\n");
            assertSql("SELECT count(distinct device) FROM ws_multi_symbol_cols", "count_distinct\n3\n");
            assertSql("SELECT count(distinct location) FROM ws_multi_symbol_cols", "count_distinct\n2\n");
            assertSql("SELECT count(distinct measurement_type) FROM ws_multi_symbol_cols", "count_distinct\n3\n");
        });
    }

    @Test
    public void testSymbolCache_multipleTablesIndependentSymbols() throws Exception {
        // Tests that symbol caches are independent per table.
        // The same symbol value in different tables should be cached separately.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Send to table1 with symbols
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_table1")
                            .symbol("status", i % 2 == 0 ? "active" : "inactive")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }

                // Send to table2 with same symbol column name but different values
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_cache_table2")
                            .symbol("status", i % 3 == 0 ? "running" : i % 3 == 1 ? "stopped" : "pending")
                            .longColumn("value", i * 10)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }

                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();

            // Verify table1
            assertSql("SELECT count() FROM ws_cache_table1", "count\n10\n");
            assertSql("SELECT count(distinct status) FROM ws_cache_table1", "count_distinct\n2\n");

            // Verify table2
            assertSql("SELECT count() FROM ws_cache_table2", "count\n10\n");
            assertSql("SELECT count(distinct status) FROM ws_cache_table2", "count_distinct\n3\n");
        });
    }

    @Test
    public void testSymbolCache_reconnect_clearsCache() throws Exception {
        // Tests that disconnecting and reconnecting clears the symbol cache.
        // The new connection should still work correctly with fresh symbols.
        Assume.assumeTrue("Sync mode only (window=1) - reconnection behavior differs", windowSize == 1);
        runInContext((port) -> {
            // First connection
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_reconnect_test")
                            .symbol("tag", "conn1_val" + (i % 3))
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            // Wait for first batch to be processed
            drainWalQueue();

            // Second connection with different symbols
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_reconnect_test")
                            .symbol("tag", "conn2_val" + (i % 3))
                            .longColumn("value", i + 100)
                            .at(1_000_000_001_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_reconnect_test", "count\n20\n");
            // Should have 6 distinct tags: conn1_val0, conn1_val1, conn1_val2, conn2_val0, conn2_val1, conn2_val2
            assertSql("SELECT count(distinct tag) FROM ws_reconnect_test", "count_distinct\n6\n");
        });
    }

    @Test
    public void testSymbolDeduplication() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Send many rows with same symbol values to test deduplication
                for (int i = 0; i < 100; i++) {
                    sender.table("ws_test_sym_dedup")
                            .symbol("region", i % 3 == 0 ? "us" : i % 3 == 1 ? "eu" : "asia")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_sym_dedup", "count\n100\n");
            assertSql("SELECT count(distinct region) FROM ws_test_sym_dedup", "count_distinct\n3\n");
        });
    }

    @Test
    public void testSymbol_alternatingTablesRapidFlush() throws Exception {
        // Rapidly alternate between two tables with different symbol sets.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 50; i++) {
                    // Alternate tables on each row
                    if (i % 2 == 0) {
                        sender.table("ws_alt_table_a")
                                .symbol("color", i % 6 < 3 ? "red" : "blue")
                                .longColumn("n", i)
                                .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                    } else {
                        sender.table("ws_alt_table_b")
                                .symbol("size", i % 6 < 2 ? "small" : i % 6 < 4 ? "medium" : "large")
                                .longColumn("n", i)
                                .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                    }

                    // Flush every 5 rows
                    if ((i + 1) % 5 == 0) {
                        sender.flush();
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            drainWalQueue();

            assertSql("SELECT count() FROM ws_alt_table_a", "count\n25\n");
            assertSql("SELECT count(distinct color) FROM ws_alt_table_a", "count_distinct\n2\n");
            assertSql("SELECT count() FROM ws_alt_table_b", "count\n25\n");
            assertSql("SELECT count(distinct size) FROM ws_alt_table_b", "count_distinct\n3\n");
        });
    }

    @Test
    public void testSymbol_connectionDropAndReconnectMidBatch() throws Exception {
        // Tests behavior when connection drops and reconnects in the middle of data ingestion.
        Assume.assumeTrue("Sync mode only (window=1) - connection behavior differs", windowSize == 1);
        runInContext((port) -> {
            // Connection 1: Send partial data
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_drop_reconnect")
                            .symbol("source", "conn1")
                            .symbol("type", i % 2 == 0 ? "typeA" : "typeB")
                            .longColumn("seq", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }
            // Connection 1 closed

            drainWalQueue();

            // Connection 2: Different symbols for "source", reuse "type" values
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_drop_reconnect")
                            .symbol("source", "conn2")
                            .symbol("type", i % 3 == 0 ? "typeA" : i % 3 == 1 ? "typeB" : "typeC")
                            .longColumn("seq", i + 100)
                            .at(1_000_000_001_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();

            // Connection 3: Mix of all previous symbols plus new
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_drop_reconnect")
                            .symbol("source", i % 3 == 0 ? "conn1" : i % 3 == 1 ? "conn2" : "conn3")
                            .symbol("type", i % 4 == 0 ? "typeA" : i % 4 == 1 ? "typeB" : i % 4 == 2 ? "typeC" : "typeD")
                            .longColumn("seq", i + 200)
                            .at(1_000_000_002_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_drop_reconnect", "count\n30\n");
            assertSql("SELECT count(distinct source) FROM ws_drop_reconnect", "count_distinct\n3\n");
            assertSql("SELECT count(distinct type) FROM ws_drop_reconnect", "count_distinct\n4\n");
        });
    }
    // These tests specifically exercise the cache hit (fast path) in writeSymbolWithCache().
    // The fast path requires:
    // 1. Symbols committed to table (WAL applied)
    // 2. Cache populated via SymbolMapReader lookup (on first reuse after commit)
    // 3. Same symbols used again on SAME connection (cache hit)

    @Test
    public void testSymbol_flushBetweenEachRow() throws Exception {
        // Tests symbol handling when each row is flushed separately.
        // This creates maximum interleaving of commits.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                String[] symbols = {"alpha", "beta", "gamma", "delta"};
                for (int i = 0; i < 20; i++) {
                    sender.table("ws_flush_each_row")
                            .symbol("tag", symbols[i % symbols.length])
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                    sender.flush();  // Flush after every single row
                }
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_flush_each_row", "count\n20\n");
            assertSql("SELECT count(distinct tag) FROM ws_flush_each_row", "count_distinct\n4\n");
        });
    }

    @Test
    public void testSymbol_highCardinalityColumn() throws Exception {
        // Tests a high-cardinality symbol column (many unique values).
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // 500 unique user_id values, 5 action values
                for (int i = 0; i < 500; i++) {
                    sender.table("ws_high_cardinality")
                            .symbol("user_id", "user_" + i)  // High cardinality
                            .symbol("action", "action_" + (i % 5))  // Low cardinality
                            .longColumn("timestamp_ms", System.currentTimeMillis())
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);

                    if ((i + 1) % 50 == 0) {
                        sender.flush();
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_high_cardinality", "count\n500\n");
            assertSql("SELECT count(distinct user_id) FROM ws_high_cardinality", "count_distinct\n500\n");
            assertSql("SELECT count(distinct action) FROM ws_high_cardinality", "count_distinct\n5\n");
        });
    }

    @Test
    public void testSymbol_interleavedTablesWithWalApply() throws Exception {
        // Tests interleaved writes to multiple tables with WAL apply between batches.
        Assume.assumeTrue("Sync mode only (window=1) - timing-dependent", windowSize == 1);
        runInContext((port) -> {
            // Round 1: Write to both tables
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 5; i++) {
                    sender.table("ws_interleave_t1")
                            .symbol("sym", "t1_v" + (i % 2))
                            .longColumn("val", i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    sender.table("ws_interleave_t2")
                            .symbol("sym", "t2_v" + (i % 3))
                            .longColumn("val", i * 10)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();

            // Round 2: More interleaved writes with some symbol reuse
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 5; i++) {
                    // Reuse t1_v0, t1_v1, add t1_v2
                    sender.table("ws_interleave_t1")
                            .symbol("sym", "t1_v" + (i % 3))
                            .longColumn("val", i + 100)
                            .at(1_000_000_001_000L + i, ChronoUnit.MICROS);
                    // Reuse t2_v0, t2_v1, t2_v2, add t2_v3
                    sender.table("ws_interleave_t2")
                            .symbol("sym", "t2_v" + (i % 4))
                            .longColumn("val", i * 10 + 100)
                            .at(1_000_000_001_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();

            assertSql("SELECT count() FROM ws_interleave_t1", "count\n10\n");
            assertSql("SELECT count(distinct sym) FROM ws_interleave_t1", "count_distinct\n3\n");
            assertSql("SELECT count() FROM ws_interleave_t2", "count\n10\n");
            assertSql("SELECT count(distinct sym) FROM ws_interleave_t2", "count_distinct\n4\n");
        });
    }

    @Test
    public void testSymbol_manySymbolsWithPeriodicWalApply() throws Exception {
        // Tests a large number of distinct symbols with periodic WAL apply.
        // This exercises symbol table growth and cache behavior.
        Assume.assumeTrue("Sync mode only (window=1) - timing-dependent", windowSize == 1);
        runInContext((port) -> {
            int totalSymbols = 100;
            int batchSize = 10;

            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < totalSymbols; i++) {
                    sender.table("ws_many_symbols_wal")
                            .symbol("unique_tag", "tag_" + i)
                            .symbol("group", "group_" + (i % 10))
                            .longColumn("seq", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);

                    // Flush every batchSize rows
                    if ((i + 1) % batchSize == 0) {
                        sender.flush();

                        // Apply WAL periodically
                        if ((i + 1) % (batchSize * 3) == 0) {
                            drainWalQueue();
                        }
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_many_symbols_wal", "count\n100\n");
            assertSql("SELECT count(distinct unique_tag) FROM ws_many_symbols_wal", "count_distinct\n100\n");
            assertSql("SELECT count(distinct \"group\") FROM ws_many_symbols_wal", "count_distinct\n10\n");
        });
    }

    @Test
    public void testSymbol_newSymbolsAfterWalApply() throws Exception {
        // Tests that new symbols can be added after WAL apply has committed previous symbols.
        Assume.assumeTrue("Sync mode only (window=1) - timing-dependent", windowSize == 1);
        runInContext((port) -> {
            // Phase 1: Initial symbols
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_a")
                        .longColumn("id", 1)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_b")
                        .longColumn("id", 2)
                        .at(1_000_000_000_001L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            // Phase 2: Mix of existing and new symbols
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_a")  // existing
                        .longColumn("id", 3)
                        .at(1_000_000_001_000L, ChronoUnit.MICROS);
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_c")  // NEW
                        .longColumn("id", 4)
                        .at(1_000_000_001_001L, ChronoUnit.MICROS);
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_b")  // existing
                        .longColumn("id", 5)
                        .at(1_000_000_001_002L, ChronoUnit.MICROS);
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_d")  // NEW
                        .longColumn("id", 6)
                        .at(1_000_000_001_003L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            // Phase 3: Even more new symbols
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_e")  // NEW
                        .longColumn("id", 7)
                        .at(1_000_000_002_000L, ChronoUnit.MICROS);
                sender.table("ws_new_after_wal")
                        .symbol("category", "cat_a")  // existing from phase 1
                        .longColumn("id", 8)
                        .at(1_000_000_002_001L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_new_after_wal", "count\n8\n");
            assertSql("SELECT count(distinct category) FROM ws_new_after_wal", "count_distinct\n5\n");
            // Verify all categories exist
            assertSql(
                    "SELECT category FROM ws_new_after_wal ORDER BY category",
                    "category\ncat_a\ncat_a\ncat_a\ncat_b\ncat_b\ncat_c\ncat_d\ncat_e\n"
            );
        });
    }

    @Test
    public void testSymbol_nullSymbolsInterleaved() throws Exception {
        // Tests interleaving of null and non-null symbol values.
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 30; i++) {
                    Sender row = sender.table("ws_null_interleave")
                            .longColumn("id", i);

                    // Alternate between null and non-null symbols
                    if (i % 3 == 0) {
                        row.symbol("optional", null);
                    } else {
                        row.symbol("optional", "val_" + (i % 5));
                    }

                    row.at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);

                    if ((i + 1) % 10 == 0) {
                        sender.flush();
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_null_interleave", "count\n30\n");
            // 10 nulls (i % 3 == 0), 20 non-nulls with values val_1, val_2, val_3, val_4 (not val_0 since those are null rows)
            assertSql("SELECT count() FROM ws_null_interleave WHERE optional IS NULL", "count\n10\n");
            assertSql("SELECT count() FROM ws_null_interleave WHERE optional IS NOT NULL", "count\n20\n");
        });
    }

    @Test
    public void testSymbol_rapidFlushWithWalApply() throws Exception {
        // Stress test: rapid small flushes with periodic WAL apply.
        Assume.assumeTrue("Sync mode only (window=1) - timing-dependent", windowSize == 1);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                String[] envs = {"prod", "staging", "dev", "test"};
                String[] services = {"api", "web", "worker", "scheduler", "cache"};

                for (int batch = 0; batch < 10; batch++) {
                    // Small batch of rows
                    for (int i = 0; i < 5; i++) {
                        int idx = batch * 5 + i;
                        sender.table("ws_rapid_flush")
                                .symbol("env", envs[idx % envs.length])
                                .symbol("service", services[idx % services.length])
                                .doubleColumn("latency", 10.0 + (idx % 100))
                                .at(1_000_000_000_000L + idx * 1000L, ChronoUnit.MICROS);
                    }
                    sender.flush();

                    // Drain WAL every 3 batches
                    if ((batch + 1) % 3 == 0) {
                        drainWalQueue();
                        drainWalQueue();
                    }
                }
            }

            drainWalQueue();
            drainWalQueue();
            assertSql("SELECT count() FROM ws_rapid_flush", "count\n50\n");
            assertSql("SELECT count(distinct env) FROM ws_rapid_flush", "count_distinct\n4\n");
            assertSql("SELECT count(distinct service) FROM ws_rapid_flush", "count_distinct\n5\n");
        });
    }

    @Test
    public void testSymbol_sameConnectionMultipleBatchesWithWalApply() throws Exception {
        // Tests multiple batches on the same connection with WAL apply between them.
        // This is the most realistic scenario for long-lived connections.
        Assume.assumeTrue("Sync mode only (window=1) - timing-dependent", windowSize == 1);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_same_conn_wal")
                            .symbol("status", i % 2 == 0 ? "ok" : "error")
                            .longColumn("code", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                drainWalQueue();

                // Batch 2 - reuse symbols on same connection after WAL apply
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_same_conn_wal")
                            .symbol("status", i % 3 == 0 ? "ok" : i % 3 == 1 ? "error" : "warning")
                            .longColumn("code", i + 100)
                            .at(1_000_000_001_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();

                // Apply WAL again
                drainWalQueue();

                // Batch 3 - more symbols
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_same_conn_wal")
                            .symbol("status", i % 4 == 0 ? "ok" : i % 4 == 1 ? "error" : i % 4 == 2 ? "warning" : "critical")
                            .longColumn("code", i + 200)
                            .at(1_000_000_002_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_same_conn_wal", "count\n30\n");
            assertSql("SELECT count(distinct status) FROM ws_same_conn_wal", "count_distinct\n4\n");
        });
    }

    @Test
    public void testSymbol_walApplyBetweenBatches() throws Exception {
        // Tests symbol handling when WAL is applied between sender batches.
        // This exercises the watermark change detection in the cache.
        Assume.assumeTrue("Sync mode only (window=1) - timing-dependent", windowSize == 1);
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 1: Send some symbols
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_wal_apply_test")
                            .symbol("region", i % 2 == 0 ? "east" : "west")
                            .longColumn("value", i)
                            .at(1_000_000_000_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();

            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 2: Reuse some symbols, add new ones
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_wal_apply_test")
                            .symbol("region", i % 3 == 0 ? "east" : i % 3 == 1 ? "west" : "central")
                            .longColumn("value", i + 100)
                            .at(1_000_000_001_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();

            try (QwpWebSocketSender sender = createSender(port)) {
                // Batch 3: More symbols
                for (int i = 0; i < 10; i++) {
                    sender.table("ws_wal_apply_test")
                            .symbol("region", i % 4 == 0 ? "north" : i % 4 == 1 ? "south" : i % 4 == 2 ? "east" : "west")
                            .longColumn("value", i + 200)
                            .at(1_000_000_002_000L + i * 1000L, ChronoUnit.MICROS);
                }
                sender.flush();
            }
            drainWalQueue();

            assertSql("SELECT count() FROM ws_wal_apply_test", "count\n30\n");
            // east, west, central, north, south = 5 distinct
            assertSql("SELECT count(distinct region) FROM ws_wal_apply_test", "count_distinct\n5\n");
        });
    }

    /**
     * Tests that TIMESTAMP (micros) data sent to a TIMESTAMP_NANO column is correctly
     * converted by multiplying by 1000.
     */
    @Test
    public void testTimestampMicrosToNanosConversion() throws Exception {
        runInContext((port) -> {
            // Create table with TIMESTAMP_NANO column for non-designated timestamp
            execute("CREATE TABLE ws_ts_convert_nano (" +
                    "tag SYMBOL, " +
                    "ts_field TIMESTAMP_NS, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            // Send microsecond timestamp to nanos column
            long tsMicros = 1_704_067_200_000_000L;  // 2024-01-01 00:00:00 in micros

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_ts_convert_nano")
                        .symbol("tag", "test")
                        .timestampColumn("ts_field", tsMicros, ChronoUnit.MICROS)  // Send as micros
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            // Verify the timestamp was correctly converted to nanos
            assertSql(
                    "SELECT ts_field FROM ws_ts_convert_nano",
                    """
                            ts_field
                            2024-01-01T00:00:00.000000000Z
                            """
            );
        });
    }

    /**
     * Tests that TIMESTAMP_NANOS data sent to a TIMESTAMP (micros) column is correctly
     * converted by dividing by 1000.
     * <p>
     * This test verifies the fix for a bug where the columnar path did not apply
     * precision conversion, resulting in values being 1000x too large.
     */
    @Test
    public void testTimestampNanosToMicrosConversion() throws Exception {
        runInContext((port) -> {
            // Create table with TIMESTAMP (micros) column for non-designated timestamp
            execute("CREATE TABLE ws_ts_convert (" +
                    "tag SYMBOL, " +
                    "ts_field TIMESTAMP, " +  // micros precision
                    "timestamp TIMESTAMP NOT NULL" +   // designated timestamp
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            // Send nanosecond timestamp to micros column
            // 1704067200000000000 nanos = 1704067200000000 micros = 2024-01-01 00:00:00 UTC
            long tsNanos = 1_704_067_200_000_000_000L;

            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_ts_convert")
                        .symbol("tag", "test")
                        .timestampColumn("ts_field", tsNanos, ChronoUnit.NANOS)  // Send as nanos
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();

            // Verify the timestamp was correctly converted to micros
            // If bug exists: value will be 1704067200000000000 (nanos, ~year 55970)
            // If fixed: value will be 1704067200000000 (micros, 2024-01-01)
            assertSql(
                    "SELECT ts_field FROM ws_ts_convert",
                    """
                            ts_field
                            2024-01-01T00:00:00.000000Z
                            """
            );
        });
    }

    @Test
    public void testTimestampOnlyRows() throws Exception {
        runInContext((port) -> {
            execute("CREATE TABLE ts_only_ws (ts TIMESTAMP NOT NULL, val LONG) TIMESTAMP(ts) PARTITION BY HOUR WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                // Row with user-supplied timestamp, no other columns
                sender.table("ts_only_ws").at(1_000_000L, ChronoUnit.MICROS);
                // Row with server-assigned timestamp, no other columns
                sender.table("ts_only_ws").atNow();
                sender.flush();
            }

            drainWalQueue();

            // The user-supplied row carries epoch+1s, which is the earliest in the table;
            // both rows have null for the val column since no value was provided
            assertSql(
                    "SELECT ts, val FROM ts_only_ws ORDER BY ts LIMIT 1",
                    """
                            ts\tval
                            1970-01-01T00:00:01.000000Z\tnull
                            """
            );
            assertSql(
                    "SELECT count(), count() - count(val) AS null_count FROM ts_only_ws",
                    """
                            count\tnull_count
                            2\t2
                            """
            );
        });
    }

    @Test
    public void testVarcharLargeValue() throws Exception {
        runInContext((port) -> {
            // Pre-create table with VARCHAR column
            execute("CREATE TABLE ws_varchar_large_test (" +
                    "tag SYMBOL, " +
                    "v VARCHAR, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 30; i++) {
                    int len = 500 + i * 300;
                    StringBuilder sb = new StringBuilder(len);
                    for (int j = 0; j < len; j++) {
                        sb.append((char) ('a' + (j % 26)));
                    }
                    sender.table("ws_varchar_large_test")
                            .symbol("tag", "r" + i)
                            .stringColumn("v", sb.toString())
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                    if ((i + 1) % 5 == 0) {
                        sender.flush();
                    }
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_varchar_large_test", "count\n30\n");
            // First row: len=500, last row: len=500+29*300=9200
            assertSql(
                    "SELECT length(v) FROM ws_varchar_large_test ORDER BY timestamp LIMIT 1",
                    "length\n500\n"
            );
            assertSql(
                    "SELECT length(v) FROM ws_varchar_large_test ORDER BY timestamp DESC LIMIT 1",
                    "length\n9200\n"
            );
            // Verify first row starts with 'abcde'
            assertSql(
                    "SELECT left(v, 5) FROM ws_varchar_large_test ORDER BY timestamp LIMIT 1",
                    "left\nabcde\n"
            );
        });
    }

    @Test
    public void testVarcharMultipleRows() throws Exception {
        runInContext((port) -> {
            // Pre-create table with VARCHAR column
            execute("CREATE TABLE ws_varchar_multi_test (" +
                    "tag SYMBOL, " +
                    "v VARCHAR, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 30; i++) {
                    sender.table("ws_varchar_multi_test")
                            .symbol("tag", "r" + i)
                            .stringColumn("v", "row-" + i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM ws_varchar_multi_test",
                    "count\n30\n"
            );
            assertSql(
                    "SELECT v FROM ws_varchar_multi_test ORDER BY timestamp LIMIT 3",
                    "v\nrow-0\nrow-1\nrow-2\n"
            );
            assertSql(
                    "SELECT v FROM ws_varchar_multi_test ORDER BY timestamp DESC LIMIT 3",
                    "v\nrow-29\nrow-28\nrow-27\n"
            );
        });
    }

    @Test
    public void testVarcharUnicode() throws Exception {
        runInContext((port) -> {
            // Pre-create table with VARCHAR column
            execute("CREATE TABLE ws_varchar_unicode_test (" +
                    "tag SYMBOL, " +
                    "v VARCHAR, " +
                    "timestamp TIMESTAMP NOT NULL" +
                    ") TIMESTAMP(timestamp) PARTITION BY DAY WAL");

            // 10 distinct unicode templates, each used 3 times for 30 total rows
            String[] unicodeValues = {
                    "こんにちは世界",
                    "😀🚀🌍",
                    "abc-éèê-üöä-АБВ",
                    "你好世界",
                    "안녕하세요",
                    "مرحبا",
                    "שלום",
                    "สวัสดี",
                    "ÀÁÂÃÄÅ",
                    "☃❤★♫☂"
            };
            String[] tags = {
                    "cjk", "emoji", "mixed", "chinese", "korean",
                    "arabic", "hebrew", "thai", "latin", "symbol"
            };

            try (QwpWebSocketSender sender = createSender(port)) {
                for (int i = 0; i < 30; i++) {
                    int idx = i % 10;
                    sender.table("ws_varchar_unicode_test")
                            .symbol("tag", tags[idx] + "_" + (i / 10))
                            .stringColumn("v", unicodeValues[idx] + "-" + i)
                            .at(1_000_000_000_000L + i, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql(
                    "SELECT count() FROM ws_varchar_unicode_test",
                    "count\n30\n"
            );
            assertSql(
                    "SELECT v FROM ws_varchar_unicode_test WHERE tag = 'cjk_0'",
                    "v\nこんにちは世界-0\n"
            );
            assertSql(
                    "SELECT v FROM ws_varchar_unicode_test WHERE tag = 'emoji_1'",
                    "v\n😀🚀🌍-11\n"
            );
            assertSql(
                    "SELECT v FROM ws_varchar_unicode_test WHERE tag = 'mixed_2'",
                    "v\nabc-éèê-üöä-АБВ-22\n"
            );
        });
    }

    @Test
    public void testWideTable100Columns() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                for (int row = 0; row < 10; row++) {
                    Sender rowBuilder = sender.table("ws_wide_table");
                    for (int col = 0; col < 100; col++) {
                        rowBuilder.longColumn("col" + col, row * 100 + col);
                    }
                    rowBuilder.at(1_000_000_000_000L + row, ChronoUnit.MICROS);
                }
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_wide_table", "count\n10\n");
        });
    }

    @Test
    public void testZeroValues() throws Exception {
        runInContext((port) -> {
            try (QwpWebSocketSender sender = createSender(port)) {
                sender.table("ws_test_zeros")
                        .longColumn("long_val", 0L)
                        .longColumn("int_val", 0L)
                        .doubleColumn("double_val", 0.0)
                        .doubleColumn("float_val", 0.0)
                        .at(1_000_000_000_000L, ChronoUnit.MICROS);
                sender.flush();
            }

            drainWalQueue();
            assertSql("SELECT count() FROM ws_test_zeros", "count\n1\n");
        });
    }

    /**
     * Creates a sender with the appropriate window size.
     * Window=1 gives sync behavior, window>1 gives async behavior.
     */
    private QwpWebSocketSender createSender(int port) {
        return QwpWebSocketSender.connect("localhost", port, null,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_ROWS,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_BYTES,
                QwpWebSocketSender.DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                windowSize,
                null);
    }
}
