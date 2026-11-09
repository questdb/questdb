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
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.cairo.PartitionCompactionScanJob;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * BOOLEAN, BYTE, SHORT and CHAR have no NULL of their own: a row of such a column reads NULL only in a column top,
 * and native ALTER COLUMN TYPE to a type with a NULL reads the top rows as NULL and every stored value, 0 and false
 * included, as itself. A parquet file holds the top rows as definition-level-0 rows, wherever they sit: CONVERT TO
 * PARQUET of a split or a composite partition writes the folders, or the pieces, of a row group one after another,
 * each with its own top. The parquet table {@code pt} takes the conversion, its native twin {@code tw} the same SQL
 * without it, and the two must hold the same rows after the ALTER, which reads the parquet day lazily, and after
 * CONVERT TO NATIVE, which writes it back to native, a reopen, a compaction rebuild of the parquet file and a late
 * row that rewrites a row group.
 * <p>
 * The tables are WAL tables: only they hold composite partitions, and a non-WAL table squashes a split day when it
 * moves on to the next day and cannot convert its last day, so it never converts a split day. Partition 2024-01-01
 * converts with row groups of 100 rows; 2024-01-02 holds one row, so 2024-01-01 is not the last partition.
 */
@RunWith(Parameterized.class)
public class ParquetColumnTopConversionTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";
    private static final String DAY_ROWS = "ts < '2024-01-02'";
    private final String sourceType;
    private final String targetType;

    public ParquetColumnTopConversionTest(String sourceType, String targetType) {
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        final String[] numericTargets = {"INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP", "TIMESTAMP_NS", "VARCHAR", "STRING", "SYMBOL"};
        for (String target : numericTargets) {
            params.add(new Object[]{"BOOLEAN", target});
        }
        params.add(new Object[]{"BOOLEAN", "BYTE"});
        params.add(new Object[]{"BOOLEAN", "SHORT"});
        for (String source : new String[]{"BYTE", "SHORT"}) {
            for (String target : numericTargets) {
                params.add(new Object[]{source, target});
            }
            params.add(new Object[]{source, "BOOLEAN"});
            params.add(new Object[]{source, "BYTE".equals(source) ? "SHORT" : "BYTE"});
            // every width: DECIMAL8, 16, 32, 64, 128 and 256
            for (String target : new String[]{"DECIMAL(2,0)", "DECIMAL(4,1)", "DECIMAL(9,2)", "DECIMAL(18,3)", "DECIMAL(38,4)", "DECIMAL(76,5)"}) {
                params.add(new Object[]{source, target});
            }
        }
        for (String target : new String[]{"VARCHAR", "STRING", "SYMBOL"}) {
            params.add(new Object[]{"CHAR", target});
        }
        return params;
    }

    @Test
    public void testCompositePartition() throws Exception {
        // The ALTER to SYMBOL converts the day to native first, and a native column holds no top after its
        // first value: the top of a later piece reads as the 0 or false it decodes to, as a native fold of
        // the pieces writes it.
        Assume.assumeFalse("SYMBOL".equals(targetType));
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, true);
        // pieces of 128 rows make a piece of the late row worth more than a rewrite of the day
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 64);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            // rows v 1..1000, two every 100ms but v 1 alone at midnight
            createTwins(" DEDUP UPSERT KEYS(ts, v)", "SELECT x, '2024-01-01'::TIMESTAMP + (x / 2) * 100_000L FROM long_sequence(1_000)");
            twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
            twins("INSERT INTO %s (v, ts) VALUES (99, '2024-01-02')");
            // merge-append writes the late row as a piece of its own, which the conversion puts between v 1 and v 2
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_000, '2024-01-01T00:00:00.050000Z', " + storedValue() + ")");
            convertToParquet();
            // row group 0: v 1, the late row and v 2..99, which the top of the base piece covers
            assertRowGroup(0, 100, 99);

            // the top of the base piece covers its 1000 rows
            twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
            assertTwins(1_000);
            rebuildParquet();
            assertTwins(1_000);
            // a late row into row group 0 rewrites it
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_002, '2024-01-01T00:00:00.150000Z', " + lateTargetValue() + ")");
            assertTwins(1_000);
            assertConvertToNativeAndReopen(1_000);
        });
    }

    @Test
    public void testSingleFolder() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSingleFolderTwins();
            convertToParquet();
            // row groups 0..8 lie inside the top, row group 9 holds 50 top rows, then the stored values
            assertRowGroup(8, 100, 100);
            assertRowGroup(9, 100, 50);

            twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
            final int nullRows = 950 + overflowRows();
            assertTwins(nullRows);
            assertConvertToNativeAndReopen(nullRows);
        });
    }

    @Test
    public void testSingleFolderConvertedToNativeBeforeAlter() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            createSingleFolderTwins();
            convertToParquet();
            // CONVERT TO NATIVE keeps the top as the native column top, which a later native ALTER reads as NULL
            convertToNative();
            assertTwins(sourceType, 950);
            twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
            final int nullRows = 950 + overflowRows();
            assertTwins(nullRows);
            engine.releaseAllReaders();
            engine.releaseAllWriters();
            assertTwins(nullRows);
        });
    }

    @Test
    public void testSplitPartition() throws Exception {
        // see testCompositePartition
        Assume.assumeFalse("SYMBOL".equals(targetType));
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            // rows v 1..1000, one a minute from 00:01
            createTwins("", "SELECT x, '2024-01-01'::TIMESTAMP + x * 60_000_000L FROM long_sequence(1_000)");
            twins("INSERT INTO %s (v, ts) VALUES (99, '2024-01-02')");
            // a late row after v 800 splits the day, as the prefix is more than twice the rest: the second folder
            // holds the late row and v 801..1000
            twins("INSERT INTO %s (v, ts) VALUES (500_000, '2024-01-01T13:20:30')");
            twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
            // late rows into the first folder rewrite it: b holds two stored values and the stored 0 or false of
            // its other rows there, and stays a top in the second folder
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_000, '2024-01-01T00:00:30', " + storedValue() + "), (1_000_001, '2024-01-01T13:19:30', " + lateValue() + ")");
            assertQuery("SELECT numRows FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                    .noRandomAccess()
                    .returns("numRows\n802\n201\n");
            convertToParquet();
            // row group 8: two stored values of the first folder, then 98 rows of the second folder's top
            assertRowGroup(8, 100, 98);

            // the 201 top rows of the second folder, and for CHAR the stored (char) 0 of v 1..800 too, a NULL
            final int nullRows = "CHAR".equals(sourceType) ? 1001 : 201;
            twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
            assertTwins(nullRows);
            rebuildParquet();
            assertTwins(nullRows);
            // a late row between the folders rewrites row group 8
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_002, '2024-01-01T13:20:10', " + lateTargetValue() + ")");
            assertTwins(nullRows);
            assertConvertToNativeAndReopen(nullRows);
        });
    }

    private static void assertNotSuspended() {
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("pt")));
        Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("tw")));
    }

    private static boolean isParquetPartitionCompact(TableToken token) {
        try (TableReader reader = engine.getReader(token)) {
            reader.openPartition(0);
            return PartitionCompactionScanJob.isParquetPartitionCompact(
                    reader.getTxFile(),
                    0,
                    reader.getMetadata(),
                    reader.getColumnVersionReader(),
                    reader.getAndInitParquetPartitionDecoder(0).metadata()
            );
        }
    }

    private static void twins(String sqlTemplate) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute(sqlTemplate.replace("%s", table));
        }
        drainWalQueue();
        assertNotSuspended();
    }

    private void assertConvertToNativeAndReopen(int nullRows) throws Exception {
        convertToNative();
        assertTwins(nullRows);
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertTwins(nullRows);
    }

    // row group `rowGroup` of the parquet day holds `size` rows, `nullCount` of them NULL in b, in the source type
    private void assertRowGroup(int rowGroup, long size, long nullCount) {
        try (TableReader reader = engine.getReader("pt")) {
            Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(0));
            reader.openPartition(0);
            final ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
            final int columnIndex = meta.getColumnIndex("b");
            Assert.assertEquals(sourceType, ColumnType.nameOf(meta.getColumnType(columnIndex)));
            Assert.assertEquals(size, meta.getRowGroupSize(rowGroup));
            Assert.assertTrue(meta.hasChunkNullCount(rowGroup, columnIndex));
            Assert.assertEquals(nullCount, meta.getChunkNullCount(rowGroup, columnIndex));
        }
    }

    private void assertTwins(int nullRows) throws Exception {
        assertTwins(targetType, nullRows);
    }

    // pt holds the rows of tw, and `nullRows` rows of b of type `type` on the day read NULL when the type has a NULL:
    // the rows of its column tops
    private void assertTwins(String type, int nullRows) throws Exception {
        assertSqlCursors("SELECT v, ts, b FROM tw ORDER BY ts, v", "SELECT v, ts, b FROM pt ORDER BY ts, v");
        final int expectedNullRows = hasNull(type) ? nullRows : 0;
        for (String table : new String[]{"pt", "tw"}) {
            assertQuery("SELECT count() FROM " + table + " WHERE b IS NULL AND " + DAY_ROWS)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + expectedNullRows + "\n");
        }
    }

    private void convertToNative() throws Exception {
        execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + DAY + "'");
        drainWalQueue();
        assertNotSuspended();
        assertQuery("SELECT isParquet FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                .noRandomAccess()
                .returns("isParquet\nfalse\n");
    }

    private void convertToParquet() throws Exception {
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        drainWalQueue();
        assertNotSuspended();
        assertQuery("SELECT isParquet FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                .noRandomAccess()
                .returns("isParquet\ntrue\n");
    }

    // 950 rows before ADD COLUMN b, then 50 rows that store b: 0 or false among them, and the extremes of the type
    private void createSingleFolderTwins() throws Exception {
        createTwins("", "SELECT x, '2024-01-01'::TIMESTAMP + x * 1_000_000L FROM long_sequence(950)");
        twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
        twins("INSERT INTO %s SELECT x + 950, '2024-01-01T01:00:00'::TIMESTAMP + x * 1_000_000L, " + storedValues() + " FROM long_sequence(50)");
        twins("INSERT INTO %s (v, ts) VALUES (99, '2024-01-02')");
    }

    private void createTwins(String dedup, String rows) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL" + dedup);
            execute("INSERT INTO " + table + " " + rows);
        }
        drainWalQueue();
    }

    // whether `type` reads a column-top row as NULL
    private static boolean hasNull(String type) {
        return switch (type) {
            case "BOOLEAN", "BYTE", "SHORT" -> false;
            default -> true;
        };
    }

    // a value of the target type, a late row stores after the ALTER
    private String lateTargetValue() {
        return lateValue() + "::" + sourceType + "::" + targetType;
    }

    // a stored value of the source type, other than 0 or false
    private String lateValue() {
        return switch (sourceType) {
            case "BOOLEAN" -> "true";
            case "CHAR" -> "'q'";
            default -> "7";
        };
    }

    // the rows of the stored values of createSingleFolderTwins() the target type cannot hold: a DECIMAL whose
    // precision is too small for the extremes of the source type, 10 rows each, reads them as NULL
    private int overflowRows() {
        if (!targetType.startsWith("DECIMAL(")) {
            return 0;
        }
        final String[] precisionScale = targetType.substring("DECIMAL(".length(), targetType.length() - 1).split(",");
        final BigDecimal limit = BigDecimal.TEN.pow(Integer.parseInt(precisionScale[0]));
        final int scale = Integer.parseInt(precisionScale[1]);
        final long[] extremes = "BYTE".equals(sourceType) ? new long[]{127, -128} : new long[]{32_767, -32_768};
        int rows = 0;
        for (long extreme : extremes) {
            if (BigDecimal.valueOf(extreme).movePointRight(scale).abs().compareTo(limit) >= 0) {
                rows += 10;
            }
        }
        return rows;
    }

    // the compaction sweep rebuilds the parquet file of the day, whose schema the ALTER left stale
    private void rebuildParquet() throws Exception {
        final TableToken token = engine.verifyTableName("pt");
        Assert.assertFalse("the ALTER leaves the parquet schema stale", isParquetPartitionCompact(token));
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_CHECK_INTERVAL, -1);
        try (PartitionCompactionScanJob job = new PartitionCompactionScanJob(engine)) {
            Assert.assertTrue(engine.getPartitionCompactionRequestQueue().tryRequest(token, MicrosFormatUtils.parseTimestamp(DAY + "T00:00:00.000000Z")));
            Assert.assertTrue("the request must dispatch the rebuild", job.run());
        }
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        drainWalQueue();
        assertNotSuspended();
        Assert.assertTrue("the rebuilt partition must be compact", isParquetPartitionCompact(token));
    }

    // a stored value of the source type: 0 or false
    private String storedValue() {
        return switch (sourceType) {
            case "BOOLEAN" -> "false";
            case "CHAR" -> "'a'";
            default -> "0";
        };
    }

    // the values of b of the rows x of long_sequence(), with 0 or false among them, and the extremes of the type
    private String storedValues() {
        return switch (sourceType) {
            case "BOOLEAN" -> "x % 3 = 0";
            case "BYTE" -> "CASE x % 5 WHEN 0 THEN 0 WHEN 1 THEN 127 WHEN 2 THEN -128 WHEN 3 THEN -1 ELSE 5 END::BYTE";
            case "SHORT" ->
                    "CASE x % 5 WHEN 0 THEN 0 WHEN 1 THEN 32_767 WHEN 2 THEN -32_768 WHEN 3 THEN -1 ELSE 5 END::SHORT";
            case "CHAR" -> "CASE x % 3 WHEN 0 THEN 'a' WHEN 1 THEN 'Z' ELSE '7' END::CHAR";
            default -> throw new AssertionError(sourceType);
        };
    }
}
