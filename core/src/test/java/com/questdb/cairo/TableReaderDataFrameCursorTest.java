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

import com.questdb.cairo.sql.DataFrame;
import com.questdb.common.ColumnType;
import com.questdb.common.PartitionBy;
import com.questdb.common.SymbolTable;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderDataFrameCursorTest extends AbstractCairoTest {

    @Test
    public void testRollbackSymbolIndexByDay() throws Exception {
        testSymbolIndexReadAfterRollback(PartitionBy.DAY, 1000000 * 60 * 5, 3);
    }

    @Test
    public void testRollbackSymbolIndexByMonth() throws Exception {
        testSymbolIndexReadAfterRollback(PartitionBy.MONTH, 1000000 * 60 * 5 * 24L, 2);
    }

    @Test
    public void testRollbackSymbolIndexByNone() throws Exception {
        testSymbolIndexReadAfterRollback(PartitionBy.NONE, 1000000 * 60 * 5, 0);
    }

    @Test
    public void testRollbackSymbolIndexByYear() throws Exception {
        testSymbolIndexReadAfterRollback(PartitionBy.YEAR, 1000000 * 60 * 5 * 24L * 10L, 2);
    }

    @Test
    public void testSymbolIndexReadByDay() throws Exception {
        testSymbolIndexRead(PartitionBy.DAY, 1000000 * 60 * 5, 3);
    }

    @Test
    public void testSymbolIndexReadByMonth() throws Exception {
        testSymbolIndexRead(PartitionBy.MONTH, 1000000 * 60 * 5 * 24L, 2);
    }

    @Test
    public void testSymbolIndexReadByNone() throws Exception {
        testSymbolIndexRead(PartitionBy.NONE, 1000000 * 60 * 5, 0);
    }

    @Test
    public void testSymbolIndexReadByYear() throws Exception {
        testSymbolIndexRead(PartitionBy.YEAR, 1000000 * 60 * 5 * 24L * 10L, 2);
    }

    private void assertIndexRowsMatchSymbol(TableReaderDataFrameCursor cursor, TableReaderRecord record) {
        // SymbolTable is table at table scope, so it will be the same for every
        // data frame here. Get its instance outside of data frame loop.
        SymbolTable symbolTable = cursor.getSymbolTable(0);

        while (cursor.hasNext()) {
            DataFrame frame = cursor.next();
            record.jumpTo(frame.getPartitionIndex(), frame.getRowLo());
            final long limit = frame.getRowHi();

            // BitmapIndex is always at data frame scope, each table can have more than one.
            // we have to get BitmapIndexReader instance once for each frame.
            BitmapIndexReader indexReader = frame.getBitmapIndexReader(0);

            // because out Symbol column 0 is indexed, frame has to have index.
            Assert.assertNotNull(indexReader);

            int keyCount = indexReader.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                BitmapIndexCursor ic = indexReader.getCursor(i, limit * 4);
                CharSequence expected = symbolTable.value(i);
                while (ic.hasNext()) {
                    long offset = ic.next();
                    record.jumpTo(frame.getPartitionIndex(), offset / 4);
                    TestUtils.assertEquals(expected, record.getSym(0));
                }
            }
        }
    }

    private void assertSymbolFoundInIndex(TableReaderDataFrameCursor cursor, TableReaderRecord record, int M) {
        // SymbolTable is table at table scope, so it will be the same for every
        // data frame here. Get its instance outside of data frame loop.
        SymbolTable symbolTable = cursor.getSymbolTable(0);

        long count = 0;
        while (cursor.hasNext()) {
            DataFrame frame = cursor.next();
            record.jumpTo(frame.getPartitionIndex(), frame.getRowLo());
            final long limit = frame.getRowHi();

            // BitmapIndex is always at data frame scope, each table can have more than one.
            // we have to get BitmapIndexReader instance once for each frame.
            BitmapIndexReader indexReader = frame.getBitmapIndexReader(0);

            // because out Symbol column 0 is indexed, frame has to have index.
            Assert.assertNotNull(indexReader);

            // Iterate data frame and advance record by incrementing "recordIndex"
            while (record.getRecordIndex() < limit) {
                CharSequence sym = record.getSym(0);

                // Get index cursor for each symbol in data frame
                BitmapIndexCursor ic = indexReader.getCursor(symbolTable.getQuick(sym), limit * 4);

                // Assert that index cursor contains offset of current row
                boolean offsetFound = false;
                long target = record.getRecordIndex() * 4;
                while (ic.hasNext()) {
                    if (ic.next() == target) {
                        offsetFound = true;
                        break;
                    }
                }
                Assert.assertTrue(offsetFound);
                record.incrementRecordIndex();
                count++;
            }
        }
        // assert that we read entire table
        Assert.assertEquals(M, count);
    }

    private long populateTable(TableWriter writer, String[] symbols, Rnd rnd, long ts, long increment, int M, int N) {
        long timestamp = ts;
        for (int i = 0; i < M; i++) {
            TableWriter.Row row = writer.newRow(timestamp += increment);
            row.putSym(0, symbols[rnd.nextPositiveInt() % N]);
            row.append();
        }
        return timestamp;
    }

    private void testSymbolIndexRead(int partitionBy, long increment, int expectedPartitionMin) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.SYMBOL).indexed(true, N / 4).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String symbols[] = new String[N];
            final int M = 1000;

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x")) {
                populateTable(writer, symbols, rnd, timestamp, increment, M, N);
                writer.commit();
            }

            // check that each symbol in table exists in index as well
            // and current row is collection of index rows
            try (TableReader reader = new TableReader(configuration, "x")) {

                // TableRecord will help us read the table. We need to position this record using
                // "recordIndex" and "columnBase".
                TableReaderRecord record = new TableReaderRecord(reader);

                Assert.assertTrue(reader.getPartitionCount() > expectedPartitionMin);

                // Open data frame cursor. This one will frame table as collection of
                // partitions, each partition is a frame.
                TableReaderDataFrameCursor cursor = new TableReaderDataFrameCursor();
                cursor.of(reader, 0, reader.getPartitionCount());
                assertSymbolFoundInIndex(cursor, record, M);
                cursor.toTop();
                assertSymbolFoundInIndex(cursor, record, M);
                cursor.toTop();
                assertIndexRowsMatchSymbol(cursor, record);
            }
        });
    }

    private void testSymbolIndexReadAfterRollback(int partitionBy, long increment, int expectedPartitionMin) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.SYMBOL).indexed(true, N / 4).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String symbols[] = new String[N];
            final int M = 1000;

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            // prepare the data, make sure rollback does the job
            long timestamp = 0;

            try (TableWriter writer = new TableWriter(configuration, "x")) {
                timestamp = populateTable(writer, symbols, rnd, timestamp, increment, M, N);
                writer.commit();
                timestamp = populateTable(writer, symbols, rnd, timestamp, increment, M, N);
                writer.rollback();
                populateTable(writer, symbols, rnd, timestamp, increment, M, N);
                writer.commit();
            }

            // check that each symbol in table exists in index as well
            // and current row is collection of index rows
            try (TableReader reader = new TableReader(configuration, "x")) {

                // TableRecord will help us read the table. We need to position this record using
                // "recordIndex" and "columnBase".
                TableReaderRecord record = new TableReaderRecord(reader);

                Assert.assertTrue(reader.getPartitionCount() > expectedPartitionMin);

                // Open data frame cursor. This one will frame table as collection of
                // partitions, each partition is a frame.
                TableReaderDataFrameCursor cursor = new TableReaderDataFrameCursor();
                cursor.of(reader, 0, reader.getPartitionCount());
                assertSymbolFoundInIndex(cursor, record, M * 2);
                cursor.toTop();
                assertSymbolFoundInIndex(cursor, record, M * 2);
                cursor.toTop();
                assertIndexRowsMatchSymbol(cursor, record);
            }
        });
    }
}