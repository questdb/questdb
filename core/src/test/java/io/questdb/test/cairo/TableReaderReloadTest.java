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

package io.questdb.test.cairo;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.CreateTableTestUtils;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.cairo.TableReaderTest.assertOpenPartitionCount;

public class TableReaderReloadTest extends AbstractCairoTest {

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
