/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.Record;
import com.questdb.cairo.sql.RecordCursor;
import com.questdb.std.Rnd;
import com.questdb.std.Unsafe;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderReloadTest extends AbstractCairoTest {

    @Test
    public void testReloadTruncateByDay() {
        testReloadAfterTruncate(PartitionBy.DAY, 3000000000L);
    }

    @Test
    public void testReloadTruncateByMonth() {
        testReloadAfterTruncate(PartitionBy.MONTH, 50000000000L);
    }

    @Test
    public void testReloadTruncateByNone() {
        testReloadAfterTruncate(PartitionBy.NONE, 1000000);
    }

    @Test
    public void testReloadTruncateByYear() {
        testReloadAfterTruncate(PartitionBy.YEAR, 365 * 50000000000L);
    }

    private void assertTable(Rnd rnd, long buffer, RecordCursor cursor, Record record) {
        while (cursor.hasNext()) {
            Assert.assertEquals(rnd.nextInt(), record.getInt(0));
            Assert.assertEquals(rnd.nextShort(), record.getShort(1));
            Assert.assertEquals(rnd.nextByte(), record.getByte(2));
            Assert.assertEquals(rnd.nextDouble2(), record.getDouble(3), 0.00001);
            Assert.assertEquals(rnd.nextFloat2(), record.getFloat(4), 0.00001);
            Assert.assertEquals(rnd.nextLong(), record.getLong(5));
            TestUtils.assertEquals(rnd.nextChars(3), record.getStr(6));
            TestUtils.assertEquals(rnd.nextChars(2), record.getSym(7));
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
            row.putDouble(3, rnd.nextDouble2());
            row.putFloat(4, rnd.nextFloat2());
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

    private void testReloadAfterTruncate(int partitionBy, long increment) {
        final Rnd rnd = new Rnd();
        final int bufferSize = 1024;
        long buffer = Unsafe.malloc(bufferSize);
        try (TableModel model = CairoTestUtils.getAllTypesModel(configuration, partitionBy)) {
            model.timestamp();
            CairoTestUtils.create(model);
        }

        long timestamp = 0;
        try (TableWriter writer = new TableWriter(configuration, "all")) {

            try (TableReader reader = new TableReader(configuration, "all")) {
                Assert.assertFalse(reader.reload());
            }

            populateTable(rnd, buffer, timestamp, increment, writer);
            rnd.reset();

            try (TableReader reader = new TableReader(configuration, "all")) {
                RecordCursor cursor = reader.getCursor();
                final Record record = cursor.getRecord();
                assertTable(rnd, buffer, cursor, record);
                writer.truncate();
                Assert.assertTrue(reader.reload());
                cursor = reader.getCursor();
                Assert.assertFalse(cursor.hasNext());

                rnd.reset();
                populateTable(rnd, buffer, timestamp, increment, writer);
                Assert.assertTrue(reader.reload());

                rnd.reset();
                cursor = reader.getCursor();
                assertTable(rnd, buffer, cursor, record);
            }
        }
    }
}
