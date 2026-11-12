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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * The reads of a parquet day whose column of a type without a NULL sentinel (BOOLEAN, BYTE, SHORT, CHAR) has a column
 * top, after a lazy ALTER COLUMN TYPE to a type with a NULL: the parquet table {@code pt} must hold the rows of its
 * native twin {@code tw} under every way a query decodes the day - a LIMIT window that starts inside a top, a filter
 * that decodes only its matching rows, every page encoding and both page versions, and a file without statistics,
 * whose chunks count no NULL. See {@link ParquetColumnTopConversionTest} for the conversions themselves.
 */
public class ParquetColumnTopReadTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";

    @Test
    public void testColumnEncodings() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        // pages of a few rows, so a column top spans pages and a page starts inside one
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_DATA_PAGE_SIZE, 64);
        for (String version : new String[]{"1", "2"}) {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_VERSION, version);
            for (String encoding : new String[]{"PLAIN", "RLE_DICTIONARY", "DELTA_BINARY_PACKED"}) {
                for (String[] conversion : new String[][]{{"BYTE", "DOUBLE"}, {"SHORT", "VARCHAR"}, {"CHAR", "STRING"}}) {
                    final String sourceType = conversion[0];
                    final String targetType = conversion[1];
                    assertMemoryLeak(() -> {
                        createSplitTwins(sourceType, "CHAR".equals(sourceType) ? "'q'" : "7");
                        twins("ALTER TABLE %s ALTER COLUMN b SET PARQUET(" + encoding + ")");
                        convertToParquet();
                        twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
                        assertTwins("v" + version + ' ' + encoding + ' ' + sourceType + " -> " + targetType);
                        dropTwins();
                    });
                }
            }
        }
    }

    @Test
    public void testPartitionDecoderReportsColumnTopRows() throws Exception {
        // The _pm decoder reports the column-top rows of a chunk wherever they sit. Row group 8 of the day holds the
        // two stored values at the end of the first folder, then 98 rows of the top of the second folder: its leading
        // top is empty, as its first row holds a value. A window that starts inside the top leads with it.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSplitTwins("BYTE", "7");
            convertToParquet();
            try (
                    TableReader reader = getReader("pt");
                    RowGroupBuffers buffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    DirectIntList columns = new DirectIntList(2, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)
            ) {
                reader.openPartition(0);
                final ParquetPartitionDecoder decoder = reader.getAndInitParquetPartitionDecoder(0);
                columns.add(decoder.metadata().getColumnIndex("b"));
                columns.add(ColumnType.BYTE);
                Assert.assertEquals(100, decoder.decodeRowGroup(buffers, columns, 8, 0, 100));
                Assert.assertEquals(1, buffers.getChunkTopRowCount(0));
                final long topRows = buffers.getChunkTopRowsPtr(0);
                Assert.assertEquals(2, Unsafe.getLong(topRows));
                Assert.assertEquals(100, Unsafe.getLong(topRows + Long.BYTES));
                Assert.assertEquals(0, buffers.getChunkLeadingTopRows(0));
                Assert.assertFalse(RowGroupBuffers.isTopRow(topRows, 1, 1));
                Assert.assertTrue(RowGroupBuffers.isTopRow(topRows, 1, 2));
                Assert.assertTrue(RowGroupBuffers.isTopRow(topRows, 1, 99));
                Assert.assertFalse(RowGroupBuffers.isTopRow(topRows, 1, 100));

                // the decode returns the rows of the pages it reads, the buffer holds the window
                decoder.decodeRowGroup(buffers, columns, 8, 50, 100);
                Assert.assertEquals(50, buffers.getChunkDataSize(0));
                Assert.assertEquals(50, buffers.getChunkLeadingTopRows(0));
            }
        });
    }

    @Test
    public void testConvertToNativeKeepsOnlyTheLeadingColumnTop() throws Exception {
        // One row group holds the whole day: the stored values of the first folder, then the top of the second.
        // A native column holds a column top only ahead of its first value, so CONVERT TO NATIVE stores the 0 or
        // false that the later top decodes to, as a native squash of the folders does, and ALTER reads it as such.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2_000);
        for (String sourceType : new String[]{"BYTE", "SHORT", "BOOLEAN"}) {
            assertMemoryLeak(() -> {
                createSplitTwins(sourceType, "BOOLEAN".equals(sourceType) ? "true" : "7");
                convertToParquet();
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + DAY + "'");
                execute("ALTER TABLE tw SQUASH PARTITIONS");
                drainWalQueue();
                assertNotSuspended();
                assertQuery("SELECT count() FROM table_partitions('tw') WHERE name LIKE '" + DAY + "%'")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n1\n");
                assertTwins(sourceType);
                twins("ALTER TABLE %s ALTER COLUMN b TYPE INT");
                assertTwins(sourceType + " -> INT");
                assertQuery("SELECT count() FROM pt WHERE b IS NULL AND ts < '2024-01-02'")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n0\n");
                dropTwins();
            });
        }
    }

    @Test
    public void testCoveringIndexIncludesConvertedColumn() throws Exception {
        // A covering index built over the parquet day decodes b as VARCHAR or STRING into its sidecar. The day is the
        // split day of createSplitTwins(), with a symbol s: row group 8 holds stored values ahead of top rows.
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        for (String[] conversion : new String[][]{{"BYTE", "VARCHAR"}, {"BOOLEAN", "STRING"}}) {
            final String sourceType = conversion[0];
            final String targetType = conversion[1];
            assertMemoryLeak(() -> {
                for (String table : new String[]{"pt", "tw"}) {
                    execute("CREATE TABLE " + table + " (v LONG, s SYMBOL, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
                    execute("INSERT INTO " + table + " SELECT x, 'k' || (x % 3), '2024-01-01'::TIMESTAMP + x * 60_000_000L FROM long_sequence(1_000)");
                }
                drainWalQueue();
                twins("INSERT INTO %s (v, s, ts) VALUES (99, 'k0', '2024-01-02')");
                twins("INSERT INTO %s (v, s, ts) VALUES (500_000, 'k1', '2024-01-01T13:20:30')");
                twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
                final String value = "BOOLEAN".equals(sourceType) ? "true" : "7";
                twins("INSERT INTO %s (v, s, ts, b) VALUES (1_000_000, 'k1', '2024-01-01T00:00:30', " + value + "), (1_000_001, 'k1', '2024-01-01T13:19:30', " + value + ")");
                assertQuery("SELECT numRows FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                        .noRandomAccess()
                        .returns("numRows\n802\n201\n");
                convertToParquet();
                twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
                twins("ALTER TABLE %s ALTER COLUMN s ADD INDEX TYPE POSTING INCLUDE (v, b)");
                final String query = "SELECT v, b FROM %s WHERE s = 'k1' AND ts < '2024-01-02'";
                assertTwins(sourceType + " -> " + targetType, query);
                // the top rows of the second folder: 500_000 and the 67 of v 801..1000 with v % 3 = 1
                assertQuery("SELECT count() FROM pt WHERE s = 'k1' AND b IS NULL AND ts < '2024-01-02'")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n68\n");
                assertQuery(query.replace("%s", "pt"))
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("CoveringIndex on: s")
                        .returns(sqlResult(query.replace("%s", "tw")));
                dropTwins();
            });
        }
    }

    @Test
    public void testFilteredReads() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        for (String[] conversion : new String[][]{{"BYTE", "DOUBLE"}, {"BYTE", "VARCHAR"}, {"BOOLEAN", "INT"}, {"BOOLEAN", "STRING"}, {"SHORT", "DECIMAL(18,3)"}}) {
            final String sourceType = conversion[0];
            final String targetType = conversion[1];
            assertMemoryLeak(() -> {
                createSplitTwins(sourceType, "BOOLEAN".equals(sourceType) ? "true" : "7");
                convertToParquet();
                twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
                // a filter on another column decodes b of the matching rows only, and a filter on b all of it
                for (String query : new String[]{
                        "SELECT v, ts, b FROM %s WHERE v % 7 = 0 ORDER BY ts, v",
                        "SELECT v, ts, b FROM %s WHERE v > 790 AND v < 1_000_002 ORDER BY ts, v",
                        "SELECT v, ts, b FROM %s WHERE b IS NULL ORDER BY ts, v",
                        "SELECT v, ts, b FROM %s WHERE b IS NOT NULL ORDER BY ts, v",
                        "SELECT b, count() FROM %s WHERE v % 3 = 0 GROUP BY b ORDER BY b",
                        "SELECT count(), count(b) FROM %s WHERE v % 2 = 0",
                        // the sorts compare the rows of the filter through a second record
                        "SELECT v, ts, b FROM %s WHERE v % 5 = 0 ORDER BY b, v",
                        "SELECT v, ts, b FROM %s WHERE v % 5 = 0 ORDER BY b, v LIMIT 30",
                        "SELECT v, ts, b FROM %s WHERE v % 5 = 0 ORDER BY b DESC, v DESC LIMIT -30",
                        "SELECT y.v, y.b FROM %s x JOIN (SELECT v, b FROM %s WHERE v % 5 = 0) y ON (v) ORDER BY y.v"
                }) {
                    assertTwins(sourceType + " -> " + targetType, query);
                }
                dropTwins();
            });
        }
    }

    @Test
    public void testLimitWindowStartsInsideColumnTop() throws Exception {
        // A LIMIT decodes only the rows of a row group from its first one: the window of row group 9 starts at its row
        // 30, inside the top of its first 50 rows, and its first 20 rows read NULL.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        for (String[] conversion : new String[][]{{"BYTE", "VARCHAR"}, {"BOOLEAN", "STRING"}, {"SHORT", "DOUBLE"}, {"CHAR", "VARCHAR"}}) {
            final String sourceType = conversion[0];
            final String targetType = conversion[1];
            assertMemoryLeak(() -> {
                createSingleFolderTwins(sourceType);
                convertToParquet();
                twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
                assertTwins(sourceType + " -> " + targetType, "SELECT v, ts, b FROM %s LIMIT 930, 960");
                assertQuery("SELECT count() FROM (SELECT b FROM pt LIMIT 930, 960) WHERE b IS NULL")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n20\n");
                dropTwins();
            });
        }
    }

    @Test
    public void testSentinelSourceConvertedToNoSentinelTypeKeepsColumnTop() throws Exception {
        // INT -> SHORT reads the column top of the INT column as 0, a value of SHORT. CONVERT TO NATIVE keeps it as the
        // native column top of the SHORT column, as native ALTER does, so ALTER back to INT reads it as NULL again. Only
        // the column top the parquet file records tells the top from the NULLs and values of INT.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        for (String[] conversion : new String[][]{{"INT", "SHORT"}, {"LONG", "BYTE"}, {"DOUBLE", "BOOLEAN"}}) {
            final String sourceType = conversion[0];
            final String targetType = conversion[1];
            assertMemoryLeak(() -> {
                createTwins("SELECT x, '2024-01-01'::TIMESTAMP + x * 1_000_000L FROM long_sequence(950)");
                twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
                twins("INSERT INTO %s SELECT x + 950, '2024-01-01T01:00:00'::TIMESTAMP + x * 1_000_000L, (CASE WHEN x % 4 = 0 THEN NULL ELSE x % 3 END)::" + sourceType + " FROM long_sequence(50)");
                twins("INSERT INTO %s (v, ts) VALUES (99, '2024-01-02')");
                convertToParquet();
                twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
                assertTwins(sourceType + " -> " + targetType);
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + DAY + "'");
                drainWalQueue();
                assertNotSuspended();
                assertTwins(sourceType + " -> " + targetType + ", native");
                twins("ALTER TABLE %s ALTER COLUMN b TYPE " + sourceType);
                assertTwins(sourceType + " -> " + targetType + " -> " + sourceType);
                assertQuery("SELECT count() FROM pt WHERE b IS NULL AND v <= 950")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n950\n");
                dropTwins();
            });
        }
    }

    @Test
    public void testSentinelSourceConvertedToVarSizeType() throws Exception {
        // INT has a NULL of its own: a conversion to VARCHAR or STRING reads NULL from it, through no column-top rows.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        for (String targetType : new String[]{"VARCHAR", "STRING"}) {
            assertMemoryLeak(() -> {
                createSplitTwins("INT", "7");
                convertToParquet();
                twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
                assertTwins("INT -> " + targetType);
                assertTwins("INT -> " + targetType, "SELECT v, ts, b FROM %s WHERE v % 5 = 0 ORDER BY b, v");
                // a late row between the folders rewrites row group 8
                twins("INSERT INTO %s (v, ts, b) VALUES (1_000_002, '2024-01-01T13:20:10', '9')");
                assertTwins("INT -> " + targetType + ", late row");
                execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + DAY + "'");
                drainWalQueue();
                assertNotSuspended();
                assertTwins("INT -> " + targetType + ", native");
                dropTwins();
            });
        }
    }

    @Test
    public void testStatisticsDisabled() throws Exception {
        // Without statistics the _pm counts no NULL of a chunk: only the definition levels tell the top rows.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        for (String[] conversion : new String[][]{{"BYTE", "DOUBLE"}, {"SHORT", "VARCHAR"}, {"BOOLEAN", "LONG"}}) {
            final String sourceType = conversion[0];
            final String targetType = conversion[1];
            assertMemoryLeak(() -> {
                createSingleFolderTwins(sourceType);
                convertToParquet();
                twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
                assertTwins(sourceType + " -> " + targetType);
                assertQuery("SELECT count() FROM pt WHERE b IS NULL")
                        .noRandomAccess()
                        .expectSize()
                        .returns("count\n950\n");
                dropTwins();
            });
        }
    }

    private static void assertNotSuspended() {
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("tw")));
    }

    private static void assertTwins(String context) throws Exception {
        assertTwins(context, "SELECT v, ts, b FROM %s ORDER BY ts, v");
    }

    private static void assertTwins(String context, String queryTemplate) throws Exception {
        try {
            assertSqlCursors(queryTemplate.replace("%s", "tw"), queryTemplate.replace("%s", "pt"));
        } catch (AssertionError e) {
            throw new AssertionError(context + ": " + queryTemplate + ": " + e.getMessage(), e);
        }
    }

    private static void convertToParquet() throws Exception {
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        drainWalQueue();
        assertNotSuspended();
    }

    // 950 rows before ADD COLUMN b, then 50 rows that store b, 0 or false among them
    private static void createSingleFolderTwins(String sourceType) throws Exception {
        createTwins("SELECT x, '2024-01-01'::TIMESTAMP + x * 1_000_000L FROM long_sequence(950)");
        twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
        final String values = switch (sourceType) {
            case "BOOLEAN" -> "x % 3 = 0";
            case "CHAR" -> "CASE x % 2 WHEN 0 THEN 'a' ELSE 'Z' END::CHAR";
            default -> "(x % 3)::" + sourceType;
        };
        twins("INSERT INTO %s SELECT x + 950, '2024-01-01T01:00:00'::TIMESTAMP + x * 1_000_000L, " + values + " FROM long_sequence(50)");
        twins("INSERT INTO %s (v, ts) VALUES (99, '2024-01-02')");
    }

    private static void createTwins(String rows) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO " + table + " " + rows);
        }
        drainWalQueue();
    }

    private static void dropTwins() throws Exception {
        execute("DROP TABLE pt");
        execute("DROP TABLE tw");
        drainWalQueue();
    }

    private static void twins(String sqlTemplate) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute(sqlTemplate.replace("%s", table));
        }
        drainWalQueue();
        assertNotSuspended();
    }

    // a day of two folders, see ParquetColumnTopConversionTest.testSplitPartition(): b holds values in the first, of
    // `value` and 0 or false, and a column top in the second
    private void createSplitTwins(String sourceType, String value) throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4096);
        createTwins("SELECT x, '2024-01-01'::TIMESTAMP + x * 60_000_000L FROM long_sequence(1_000)");
        twins("INSERT INTO %s (v, ts) VALUES (99, '2024-01-02')");
        twins("INSERT INTO %s (v, ts) VALUES (500_000, '2024-01-01T13:20:30')");
        twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
        twins("INSERT INTO %s (v, ts, b) VALUES (1_000_000, '2024-01-01T00:00:30', " + value + "), (1_000_001, '2024-01-01T13:19:30', " + value + ")");
        assertQuery("SELECT numRows FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                .noRandomAccess()
                .returns("numRows\n802\n201\n");
    }

    private String sqlResult(String query) throws Exception {
        printSql(query);
        return sink.toString();
    }
}
