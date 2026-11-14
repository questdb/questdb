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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.cairo.TableReaderTest.assertOpenPartitionCount;

public class TableReaderReloadTest extends AbstractCairoTest {

    @Test
    public void testReloadAfterDropBitmapIndexedColumnBeforePostingIndexedColumn() throws Exception {
        testReloadAfterDropIndexedColumnBeforeIndexedColumn("BITMAP", "POSTING");
    }

    @Test
    public void testReloadAfterDropPostingIndexedColumnBeforeBitmapIndexedColumn() throws Exception {
        testReloadAfterDropIndexedColumnBeforeIndexedColumn("POSTING", "BITMAP");
    }

    @Test
    public void testReloadFreesIndexReadersOfReplacedColumnInClosedPartition() throws Exception {
        // A new index type makes the column a new instance in the same slot: the old readers can't serve it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SYMBOL INDEX TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T12:00:00', 'x'), ('2024-01-02T12:00:00', 'x')");
            try (TableReader reader = newOffPoolReader(configuration, "t")) {
                final int a = reader.getMetadata().getColumnIndex("a");
                reader.openPartition(0);
                final IndexReader bwd = reader.getIndexReader(0, a, IndexReader.DIR_BACKWARD);
                final IndexReader fwd = reader.getIndexReader(0, a, IndexReader.DIR_FORWARD);
                reader.closePartitionByIndex(0);

                // ADD INDEX compiles against a pooled reader, which shares the scoreboard id of this off-pool
                // reader: release the txn as the pool does when a query returns its reader
                reader.goPassive();
                execute("ALTER TABLE t ALTER COLUMN a DROP INDEX");
                execute("ALTER TABLE t ALTER COLUMN a ADD INDEX TYPE BITMAP");
                reader.goActive();

                Assert.assertEquals(a, reader.getMetadata().getColumnIndex("a"));
                Assert.assertNull(reader.getIndexReaderIfExists(0, a, IndexReader.DIR_BACKWARD));
                Assert.assertNull(reader.getIndexReaderIfExists(0, a, IndexReader.DIR_FORWARD));

                reader.openPartition(0);
                Assert.assertNotSame(bwd, reader.getIndexReader(0, a, IndexReader.DIR_BACKWARD));
                Assert.assertNotSame(fwd, reader.getIndexReader(0, a, IndexReader.DIR_FORWARD));
                Assert.assertEquals("0", indexRowIds(reader, 0, a, IndexReader.DIR_BACKWARD));
                Assert.assertEquals("0", indexRowIds(reader, 0, a, IndexReader.DIR_FORWARD));
            }
        });
    }

    @Test
    public void testReloadMovesIndexReadersWithTheirColumnsInClosedPartition() throws Exception {
        // closePartitionByIndex() leaves the partition's index readers in their slots for reuse
        testReloadMovesIndexReadersWithTheirColumns(false);
    }

    @Test
    public void testReloadMovesIndexReadersWithTheirColumnsInRewrittenPartition() throws Exception {
        // the reload that sees the column move also finds the partition rewritten, and closes it
        testReloadMovesIndexReadersWithTheirColumns(true);
    }

    @Test
    public void testReloadReplacesStandInIndexReaderOncePartitionHasColumn() throws Exception {
        // b comes after the data, so the first partition lacks it and its index lookups go through stand-in readers
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SYMBOL INDEX TYPE POSTING) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T12:00:00', 'x'), ('2024-01-02T12:00:00', 'x')");
            execute("ALTER TABLE t ADD COLUMN b SYMBOL INDEX TYPE BITMAP");
            try (TableReader reader = newOffPoolReader(configuration, "t")) {
                final int b = reader.getMetadata().getColumnIndex("b");
                reader.openPartition(0);
                final IndexReader standInBwd = reader.getIndexReader(0, b, IndexReader.DIR_BACKWARD);
                final IndexReader standInFwd = reader.getIndexReader(0, b, IndexReader.DIR_FORWARD);
                // every row of the partition reads NULL in b, through the same stand-ins on each lookup
                Assert.assertEquals("0", indexRowIds(reader, 0, b, IndexReader.DIR_BACKWARD, null));
                Assert.assertEquals("0", indexRowIds(reader, 0, b, IndexReader.DIR_FORWARD, null));
                Assert.assertSame(standInBwd, reader.getIndexReader(0, b, IndexReader.DIR_BACKWARD));
                Assert.assertSame(standInFwd, reader.getIndexReader(0, b, IndexReader.DIR_FORWARD));

                // the row before the partition's only row rewrites the partition, which has b from then on
                execute("INSERT INTO t VALUES ('2024-01-01T06:00:00', 'y', 'x')");
                Assert.assertTrue(reader.reload());
                reader.openPartition(0);

                Assert.assertNotSame(standInBwd, reader.getIndexReader(0, b, IndexReader.DIR_BACKWARD));
                Assert.assertNotSame(standInFwd, reader.getIndexReader(0, b, IndexReader.DIR_FORWARD));
                Assert.assertEquals("0", indexRowIds(reader, 0, b, IndexReader.DIR_BACKWARD));
                Assert.assertEquals("0", indexRowIds(reader, 0, b, IndexReader.DIR_FORWARD));
                Assert.assertEquals("1", indexRowIds(reader, 0, b, IndexReader.DIR_BACKWARD, null));
                Assert.assertEquals("1", indexRowIds(reader, 0, b, IndexReader.DIR_FORWARD, null));
            }
        });
    }

    @Test
    public void testReloadTruncateByDay() {
        testReloadAfterTruncate(PartitionBy.DAY, 3000000000L, false);
    }

    @Test
    public void testReloadTruncateByMonth() {
        testReloadAfterTruncate(PartitionBy.MONTH, 50000000000L, false);
    }

    @Test
    public void testReloadTruncateByNone() {
        testReloadAfterTruncate(PartitionBy.NONE, 1000000, false);
    }

    @Test
    public void testReloadTruncateByWeek() {
        testReloadAfterTruncate(PartitionBy.WEEK, 7 * 3000000000L, false);
    }

    @Test
    public void testReloadTruncateByYear() {
        testReloadAfterTruncate(PartitionBy.YEAR, 365 * 50000000000L, false);
    }

    @Test
    public void testReloadTruncateKeepSymbolTables() {
        testReloadAfterTruncate(PartitionBy.DAY, 3000000000L, true);
    }

    @Test
    public void testTruncateInsertReloadDay() {
        testTruncateInsertReload(PartitionBy.DAY, 3000000000L, false);
    }

    @Test
    public void testTruncateInsertReloadKeepSymbolTables() {
        testTruncateInsertReload(PartitionBy.DAY, 3000000000L, true);
    }

    @Test
    public void testTruncateInsertReloadMonth() {
        testTruncateInsertReload(PartitionBy.MONTH, 50000000000L, false);
    }

    @Test
    public void testTruncateInsertReloadNone() {
        testTruncateInsertReload(PartitionBy.NONE, 1000000L, false);
    }

    @Test
    public void testTruncateInsertReloadWeek() {
        testTruncateInsertReload(PartitionBy.WEEK, 7 * 3000000000L, false);
    }

    @Test
    public void testTruncateInsertReloadYear() {
        testTruncateInsertReload(PartitionBy.YEAR, 365 * 50000000000L, false);
    }

    private static String indexRowIds(TableReader reader, int partitionIndex, int columnIndex, int direction) {
        return indexRowIds(reader, partitionIndex, columnIndex, direction, "x");
    }

    // the row ids of the partition that the column's index finds for the symbol, null for NULL
    private static String indexRowIds(TableReader reader, int partitionIndex, int columnIndex, int direction, String symbol) {
        final int key = TableUtils.toIndexKey(reader.getSymbolMapReader(columnIndex).keyOf(symbol));
        final StringSink rowIds = new StringSink();
        // bounded by the partition's last row: a stand-in reader answers NULL with every row id up to the bound
        final long maxRowId = reader.getPartitionRowCount(partitionIndex) - 1;
        final RowCursor cursor = reader.getIndexReader(partitionIndex, columnIndex, direction).getCursor(key, 0, maxRowId);
        try {
            while (cursor.hasNext()) {
                if (rowIds.length() > 0) {
                    rowIds.put(' ');
                }
                rowIds.put(cursor.next());
            }
        } finally {
            Misc.free(cursor);
        }
        return rowIds.toString();
    }

    private void assertIndexScans(String column) throws Exception {
        final String expected = "ts\t" + column + "\n"
                + "2024-01-01T00:00:00.000000Z\tx\n"
                + "2024-01-02T00:00:00.000000Z\tx\n";
        assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'x'")
                .noLeakCheck()
                .timestamp("ts")
                .withPlanContaining("Index forward scan on: " + column)
                .returns(expected);
        final String expectedDesc = "ts\t" + column + "\n"
                + "2024-01-02T00:00:00.000000Z\tx\n"
                + "2024-01-01T00:00:00.000000Z\tx\n";
        assertQuery("SELECT ts, " + column + " FROM t WHERE " + column + " = 'x' ORDER BY ts DESC")
                .noLeakCheck()
                .timestampDesc("ts")
                .withPlanContaining("Index backward scan on: " + column)
                .returns(expectedDesc);
    }

    private void assertTable(Rnd rnd, long buffer, RecordCursor cursor, Record record) {
        while (cursor.hasNext()) {
            Assert.assertEquals(rnd.nextInt(), record.getInt(0));
            Assert.assertEquals(rnd.nextShort(), record.getShort(1));
            Assert.assertEquals(rnd.nextByte(), record.getByte(2));
            Assert.assertEquals(rnd.nextDouble(), record.getDouble(3), 0.00001);
            Assert.assertEquals(rnd.nextFloat(), record.getFloat(4), 0.00001);
            Assert.assertEquals(rnd.nextLong(), record.getLong(5));
            TestUtils.assertEquals(rnd.nextChars(3), record.getStrA(6));
            TestUtils.assertEquals(rnd.nextChars(2), record.getSymA(7));
            Assert.assertEquals(rnd.nextBoolean(), record.getBool(8));

            rnd.nextChars(buffer, 1024 / 2);
            Assert.assertEquals(rnd.nextLong(), record.getDate(10));
        }
    }

    private void populateTable(Rnd rnd, long buffer, long timestamp, long increment, TableWriter writer) {
        for (int i = 0; i < 100; i++) {
            TableWriter.Row row = writer.newRow(timestamp);
            row.putInt(0, rnd.nextInt());
            row.putShort(1, rnd.nextShort());
            row.putByte(2, rnd.nextByte());
            row.putDouble(3, rnd.nextDouble());
            row.putFloat(4, rnd.nextFloat());
            row.putLong(5, rnd.nextLong());
            row.putStr(6, rnd.nextChars(3));
            row.putSym(7, rnd.nextChars(2));
            row.putBool(8, rnd.nextBoolean());
            rnd.nextChars(buffer, 1024 / 2);
            row.putBin(9, buffer, 1024);
            row.putDate(10, rnd.nextLong());
            row.append();
            timestamp += increment;
        }
        writer.commit();
    }

    private void testReloadAfterDropIndexedColumnBeforeIndexedColumn(String droppedIndexType, String keptIndexType) throws Exception {
        // Dropping the first column moves the second one into its slot of the pooled reader.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, a SYMBOL INDEX TYPE " + droppedIndexType
                    + ", b SYMBOL INDEX TYPE " + keptIndexType + ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T00:00:00', 'x', 'x'), ('2024-01-02T00:00:00', 'x', 'x')");
            assertIndexScans("a");
            assertIndexScans("b");
            execute("ALTER TABLE t DROP COLUMN a");
            assertIndexScans("b");
        });
    }

    private void testReloadAfterTruncate(int partitionBy, long increment, boolean keepSymbolTables) {
        if (Os.isWindows()) {
            return;
        }
        final Rnd rnd = new Rnd();
        final int bufferSize = 1024;
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, partitionBy);
        model.timestamp();
        AbstractCairoTest.create(model);

        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
            try (TableReader reader = newOffPoolReader(configuration, "all")) {
                Assert.assertFalse(reader.reload());
            }

            populateTable(rnd, buffer, timestamp, increment, writer);
            rnd.reset();

            try (
                    TableReader reader = newOffPoolReader(configuration, "all");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                final Record record = cursor.getRecord();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);

                if (keepSymbolTables) {
                    writer.truncateSoft();
                } else {
                    writer.truncate();
                }
                Assert.assertTrue(reader.reload());
                assertOpenPartitionCount(reader);
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());

                rnd.reset();
                populateTable(rnd, buffer, timestamp, increment, writer);
                Assert.assertTrue(reader.reload());
                assertOpenPartitionCount(reader);

                rnd.reset();
                cursor.toTop();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);
            }
        }
    }

    private void testReloadMovesIndexReadersWithTheirColumns(boolean isRewritten) throws Exception {
        // Dropping c moves a and b one slot left, b into a's slot, and both stay the same column instances: their
        // readers in the partition the reader has closed follow them and serve them again once it reopens.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (ts TIMESTAMP, c INT, a SYMBOL INDEX TYPE POSTING, b SYMBOL INDEX TYPE BITMAP) " +
                    "TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("INSERT INTO t VALUES ('2024-01-01T12:00:00', 1, 'x', 'x'), ('2024-01-02T12:00:00', 2, 'x', 'x')");
            try (TableReader reader = newOffPoolReader(configuration, "t")) {
                final int a = reader.getMetadata().getColumnIndex("a");
                final int b = reader.getMetadata().getColumnIndex("b");
                reader.openPartition(0);
                final IndexReader aBwd = reader.getIndexReader(0, a, IndexReader.DIR_BACKWARD);
                final IndexReader aFwd = reader.getIndexReader(0, a, IndexReader.DIR_FORWARD);
                final IndexReader bBwd = reader.getIndexReader(0, b, IndexReader.DIR_BACKWARD);
                final IndexReader bFwd = reader.getIndexReader(0, b, IndexReader.DIR_FORWARD);
                final String expectedRowIds;
                if (isRewritten) {
                    // the row before 'x' merges into a new version of the partition, which shifts 'x' to row 1
                    execute("INSERT INTO t VALUES ('2024-01-01T06:00:00', 3, 'y', 'y')");
                    expectedRowIds = "1";
                } else {
                    reader.closePartitionByIndex(0);
                    expectedRowIds = "0";
                }

                execute("ALTER TABLE t DROP COLUMN c");
                Assert.assertTrue(reader.reload());

                Assert.assertEquals(a - 1, reader.getMetadata().getColumnIndex("a"));
                Assert.assertEquals(b - 1, reader.getMetadata().getColumnIndex("b"));
                Assert.assertSame(aBwd, reader.getIndexReaderIfExists(0, a - 1, IndexReader.DIR_BACKWARD));
                Assert.assertSame(aFwd, reader.getIndexReaderIfExists(0, a - 1, IndexReader.DIR_FORWARD));
                Assert.assertSame(bBwd, reader.getIndexReaderIfExists(0, b - 1, IndexReader.DIR_BACKWARD));
                Assert.assertSame(bFwd, reader.getIndexReaderIfExists(0, b - 1, IndexReader.DIR_FORWARD));

                reader.openPartition(0);
                Assert.assertSame(aBwd, reader.getIndexReader(0, a - 1, IndexReader.DIR_BACKWARD));
                Assert.assertSame(aFwd, reader.getIndexReader(0, a - 1, IndexReader.DIR_FORWARD));
                Assert.assertSame(bBwd, reader.getIndexReader(0, b - 1, IndexReader.DIR_BACKWARD));
                Assert.assertSame(bFwd, reader.getIndexReader(0, b - 1, IndexReader.DIR_FORWARD));
                Assert.assertEquals(expectedRowIds, indexRowIds(reader, 0, a - 1, IndexReader.DIR_BACKWARD));
                Assert.assertEquals(expectedRowIds, indexRowIds(reader, 0, a - 1, IndexReader.DIR_FORWARD));
                Assert.assertEquals(expectedRowIds, indexRowIds(reader, 0, b - 1, IndexReader.DIR_BACKWARD));
                Assert.assertEquals(expectedRowIds, indexRowIds(reader, 0, b - 1, IndexReader.DIR_FORWARD));
            }
        });
    }

    private void testTruncateInsertReload(int partitionBy, long increment, boolean keepSymbolTables) {
        if (Os.isWindows()) {
            return;
        }

        final Rnd rnd = new Rnd();
        final int bufferSize = 1024;
        int timestampType = TestUtils.generateRandom(LOG).nextBoolean() ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
        TimestampDriver timestampDriver = ColumnType.getTimestampDriver(timestampType);
        increment = timestampDriver.fromMicros(increment);
        long buffer = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        TableModel model = CreateTableTestUtils.getAllTypesModel(configuration, partitionBy);
        model.timestamp(timestampType);
        AbstractCairoTest.create(model);

        long timestamp = 0;
        try (TableWriter writer = newOffPoolWriter(configuration, "all")) {
            try (TableReader reader = newOffPoolReader(configuration, "all")) {
                Assert.assertFalse(reader.reload());
            }

            populateTable(rnd, buffer, timestamp, increment, writer);
            rnd.reset();

            try (
                    TableReader reader = newOffPoolReader(configuration, "all");
                    TestTableReaderRecordCursor cursor = new TestTableReaderRecordCursor().of(reader)
            ) {
                final Record record = cursor.getRecord();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);

                if (keepSymbolTables) {
                    writer.truncateSoft();
                } else {
                    writer.truncate();
                }

                // Write different data
                rnd.reset(123, 123);
                populateTable(rnd, buffer, timestamp, increment / 2, writer);
                Assert.assertTrue(reader.reload());
                assertOpenPartitionCount(reader);

                // Assert the data is what was written the second time
                rnd.reset(123, 123);
                cursor.toTop();
                assertTable(rnd, buffer, cursor, record);
                assertOpenPartitionCount(reader);
            }
        }
    }
}
