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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * BYTE and SHORT have no NULL of their own: a row of such a column reads NULL only in a column top, and native
 * ALTER COLUMN TYPE to INT, LONG or TIMESTAMP reads the top rows as NULL and every stored value, 0 included, as
 * itself. CONVERT TO PARQUET of a split or a composite partition writes the folders, or the pieces, of a row group
 * one after another, each with its own top, so a row group can hold stored values ahead of top rows: its NULLs are
 * not its leading rows, and its null count covers rows that hold values. The parquet table {@code pt} takes such a
 * conversion, its native twin {@code tw} the same SQL without it. The two must hold the same rows after the lazy
 * ALTER, after a compaction rebuild of the parquet file, after a late row that rewrites the row group, after
 * CONVERT TO NATIVE and after a reopen.
 * <p>
 * The tables are WAL tables: only they hold composite partitions, and a non-WAL table squashes a split day when it
 * moves on to the next day and cannot convert its last day, so it never converts a split day. Partition 2024-01-01
 * converts with row groups of 100 rows; 2024-01-02 holds one row, so 2024-01-01 is not the last partition.
 */
@RunWith(Parameterized.class)
public class ParquetFolderColumnTopWidenTest extends AbstractCairoTest {
    private static final String DAY = "2024-01-01";
    private static final String DAY_ROWS = "ts < '2024-01-02'";
    private final String sourceType;
    private final String targetType;

    public ParquetFolderColumnTopWidenTest(String sourceType, String targetType) {
        this.sourceType = sourceType;
        this.targetType = targetType;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> data() {
        final List<Object[]> params = new ArrayList<>();
        for (String sourceType : new String[]{"BYTE", "SHORT"}) {
            for (String targetType : new String[]{"INT", "LONG", "TIMESTAMP", "TIMESTAMP_NS"}) {
                params.add(new Object[]{sourceType, targetType});
            }
        }
        return params;
    }

    @Test
    public void testCompositePartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, true);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        assertMemoryLeak(() -> {
            // rows v 1..200_000, two every 100ms but v 1 alone at midnight
            createTwins(" DEDUP UPSERT KEYS(ts, v)", "SELECT x, '2024-01-01'::TIMESTAMP + (x / 2) * 100_000L FROM long_sequence(200_000)");
            twins("ALTER TABLE %s ADD COLUMN b " + sourceType);
            twins("INSERT INTO %s (v, ts) VALUES (99, '2024-01-02')");
            // merge-append writes the late row as a piece of its own, which the conversion puts between v 1 and v 2
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_000, '2024-01-01T00:00:00.050000Z', 5)");
            convertToParquet();
            // row group 0: v 1, the late row and v 2..99, which the top of the base piece covers
            assertRowGroup(0, 100, 99);

            final String keys = "1, 2, 99, 200_000, 1_000_000";
            final String keyRows = rows(1, null, 2, null, 99, null, 200_000, null, 1_000_000, 5);
            twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
            assertTwins(keys, keyRows, 200_000);
            rebuildParquet();
            assertTwins(keys, keyRows, 200_000);

            // a late row into row group 0 rewrites it
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_002, '2024-01-01T00:00:00.150000Z', 9)");
            final String lateKeys = keys + ", 1_000_002";
            final String lateRows = rows(1, null, 2, null, 99, null, 200_000, null, 1_000_000, 5, 1_000_002, 9);
            assertTwins(lateKeys, lateRows, 200_000);
            assertConvertToNativeAndReopen(lateKeys, lateRows, 200_000);
        });
    }

    @Test
    public void testSplitPartition() throws Exception {
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
            // late rows into the first folder rewrite it: b holds 5, 7 and the stored 0 of its other rows there, and
            // stays a top in the second folder
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_000, '2024-01-01T00:00:30', 5), (1_000_001, '2024-01-01T13:19:30', 7)");
            assertQuery("SELECT numRows FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                    .noRandomAccess()
                    .returns("numRows\n802\n201\n");
            convertToParquet();
            // row group 8: the 7 and v 800 of the first folder, then 98 rows of the second folder's top
            assertRowGroup(8, 100, 98);

            final String keys = "1, 799, 800, 801, 1_000, 500_000, 1_000_000, 1_000_001";
            final String keyRows = rows(1, 0, 799, 0, 800, 0, 801, null, 1_000, null, 500_000, null, 1_000_000, 5, 1_000_001, 7);
            twins("ALTER TABLE %s ALTER COLUMN b TYPE " + targetType);
            assertTwins(keys, keyRows, 201);
            rebuildParquet();
            assertTwins(keys, keyRows, 201);

            // a late row between the folders rewrites row group 8
            twins("INSERT INTO %s (v, ts, b) VALUES (1_000_002, '2024-01-01T13:20:10', 9)");
            final String lateKeys = keys + ", 1_000_002";
            final String lateRows = rows(1, 0, 799, 0, 800, 0, 801, null, 1_000, null, 500_000, null, 1_000_000, 5, 1_000_001, 7, 1_000_002, 9);
            assertTwins(lateKeys, lateRows, 201);
            assertConvertToNativeAndReopen(lateKeys, lateRows, 201);
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
            execute(String.format(sqlTemplate, table));
        }
        drainWalQueue();
        assertNotSuspended();
    }

    private void assertConvertToNativeAndReopen(String keys, String keyRows, int nullCount) throws Exception {
        execute("ALTER TABLE pt CONVERT PARTITION TO NATIVE LIST '" + DAY + "'");
        drainWalQueue();
        assertNotSuspended();
        assertQuery("SELECT isParquet FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                .noRandomAccess()
                .returns("isParquet\nfalse\n");
        assertTwins(keys, keyRows, nullCount);
        engine.releaseAllReaders();
        engine.releaseAllWriters();
        assertTwins(keys, keyRows, nullCount);
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

    // pt holds the rows of tw, `keyRows` at the rows `keys` name, and `nullCount` NULLs in b on the parquet day
    private void assertTwins(String keys, String keyRows, int nullCount) throws Exception {
        assertSqlCursors("SELECT v, ts, b FROM tw ORDER BY ts, v", "SELECT v, ts, b FROM pt ORDER BY ts, v");
        for (String table : new String[]{"pt", "tw"}) {
            assertQuery("SELECT v, b FROM " + table + " WHERE v IN (" + keys + ") AND " + DAY_ROWS + " ORDER BY v")
                    .returns("v\tb\n" + keyRows);
            assertQuery("SELECT count() FROM " + table + " WHERE b IS NULL AND " + DAY_ROWS)
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n" + nullCount + "\n");
        }
    }

    private void convertToParquet() throws Exception {
        execute("ALTER TABLE pt CONVERT PARTITION TO PARQUET LIST '" + DAY + "'");
        drainWalQueue();
        assertNotSuspended();
        assertQuery("SELECT isParquet FROM table_partitions('pt') WHERE name LIKE '" + DAY + "%'")
                .noRandomAccess()
                .returns("isParquet\ntrue\n");
    }

    private void createTwins(String dedup, String rows) throws Exception {
        for (String table : new String[]{"pt", "tw"}) {
            execute("CREATE TABLE " + table + " (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL" + dedup);
            execute("INSERT INTO " + table + " " + rows);
        }
        drainWalQueue();
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
        try (TableReader reader = engine.getReader(token)) {
            Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormat(0));
            reader.openPartition(0);
            final ParquetMetaFileReader meta = reader.getAndInitParquetPartitionDecoder(0).metadata();
            Assert.assertEquals(targetType, ColumnType.nameOf(meta.getColumnType(meta.getColumnIndex("b"))));
        }
    }

    // the text of b in the target type: NULL, or the stored value widened, which TIMESTAMP reads as micros or nanos
    private String render(Integer value) {
        return switch (targetType) {
            case "TIMESTAMP" -> value == null ? "" : String.format("1970-01-01T00:00:00.%06dZ", value);
            case "TIMESTAMP_NS" -> value == null ? "" : String.format("1970-01-01T00:00:00.%09dZ", value);
            default -> value == null ? "null" : Integer.toString(value);
        };
    }

    // the rows of (v, b) pairs, b null for a NULL
    private String rows(Object... pairs) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < pairs.length; i += 2) {
            sb.append(pairs[i]).append('\t').append(render((Integer) pairs[i + 1])).append('\n');
        }
        return sb.toString();
    }
}
