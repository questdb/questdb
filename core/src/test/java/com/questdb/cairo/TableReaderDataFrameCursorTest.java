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
import com.questdb.std.Chars;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableReaderDataFrameCursorTest extends AbstractCairoTest {

    @Test
    public void testRemoveFirstColByDay() throws Exception {
        testRemoveFirstColumn(PartitionBy.DAY, 1000000 * 60 * 5, 3);
    }

    @Test
    public void testRemoveFirstColByMonth() throws Exception {
        testRemoveFirstColumn(PartitionBy.MONTH, 1000000 * 60 * 5 * 24L, 2);
    }

    @Test
    public void testRemoveFirstColByNone() throws Exception {
        testRemoveFirstColumn(PartitionBy.NONE, 1000000 * 60 * 5, 0);
    }

    @Test
    public void testRemoveFirstColByYear() throws Exception {
        testRemoveFirstColumn(PartitionBy.YEAR, 1000000 * 60 * 5 * 24L * 10L, 2);
    }

    @Test
    public void testRemoveLastColByDay() throws Exception {
        testRemoveLastColumn(PartitionBy.DAY, 1000000 * 60 * 5, 3);
    }

    @Test
    public void testRemoveLastColByMonth() throws Exception {
        testRemoveLastColumn(PartitionBy.MONTH, 1000000 * 60 * 5 * 24L, 2);
    }

    @Test
    public void testRemoveLastColByNone() throws Exception {
        testRemoveFirstColumn(PartitionBy.NONE, 1000000 * 60 * 5, 0);
    }

    @Test
    public void testRemoveLastColByYear() throws Exception {
        testRemoveLastColumn(PartitionBy.YEAR, 1000000 * 60 * 5 * 24L * 10L, 2);
    }

    @Test
    public void testRemoveMidColByDay() throws Exception {
        testRemoveMidColumn(PartitionBy.DAY, 1000000 * 60 * 5, 3);
    }

    @Test
    public void testRemoveMidColByMonth() throws Exception {
        testRemoveMidColumn(PartitionBy.MONTH, 1000000 * 60 * 5 * 24L, 2);
    }

    @Test
    public void testRemoveMidColByNone() throws Exception {
        testRemoveMidColumn(PartitionBy.NONE, 1000000 * 60 * 5, 0);
    }

    @Test
    public void testRemoveMidColByYear() throws Exception {
        testRemoveMidColumn(PartitionBy.YEAR, 1000000 * 60 * 5 * 24L * 10L, 2);
    }

    //

    @Test
    public void testReplaceIndexedWithUnindexedByByDay() throws Exception {
        testReplaceIndexedColWithUnindexed(PartitionBy.DAY, 1000000 * 60 * 5, 3);
    }

    @Test
    public void testReplaceIndexedWithUnindexedByByNone() throws Exception {
        testReplaceIndexedColWithUnindexed(PartitionBy.NONE, 1000000 * 60 * 5, 0);
    }

    @Test
    public void testReplaceIndexedWithUnindexedByByYear() throws Exception {
        testReplaceIndexedColWithUnindexed(PartitionBy.YEAR, 1000000 * 60 * 5 * 24L * 10L, 2);
    }

    @Test
    public void testReplaceIndexedWithUnindexedByMonth() throws Exception {
        testReplaceIndexedColWithUnindexed(PartitionBy.MONTH, 1000000 * 60 * 5 * 24L, 2);
    }

    ///

    @Test
    public void testReplaceUnindexedWithIndexedByByDay() throws Exception {
        testReplaceUnindexedColWithIndexed(PartitionBy.DAY, 1000000 * 60 * 5, 3);
    }

    ///

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

    private void assertIndexRowsMatchSymbol(TableReaderDataFrameCursor cursor, TableReaderRecord record, int columnIndex) {
        // SymbolTable is table at table scope, so it will be the same for every
        // data frame here. Get its instance outside of data frame loop.
        SymbolTable symbolTable = cursor.getSymbolTable(columnIndex);

        while (cursor.hasNext()) {
            DataFrame frame = cursor.next();
            record.jumpTo(frame.getPartitionIndex(), frame.getRowLo());
            final long limit = frame.getRowHi();

            // BitmapIndex is always at data frame scope, each table can have more than one.
            // we have to get BitmapIndexReader instance once for each frame.
            BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex);

            // because out Symbol column 0 is indexed, frame has to have index.
            Assert.assertNotNull(indexReader);

            int keyCount = indexReader.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                BitmapIndexCursor ic = indexReader.getCursor(i, limit - 1);
                CharSequence expected = symbolTable.value(i - 1);
                while (ic.hasNext()) {
                    long row = ic.next();
                    record.jumpTo(frame.getPartitionIndex(), row);
                    TestUtils.assertEquals(expected, record.getSym(columnIndex));
                }
            }
        }
    }

    private void assertNoIndex(TableReaderDataFrameCursor cursor, int columnIndex) {
        while (cursor.hasNext()) {
            DataFrame frame = cursor.next();
            try {
                frame.getBitmapIndexReader(columnIndex);
                Assert.fail();
            } catch (CairoException e) {
                Assert.assertTrue(Chars.contains(e.getMessage(), "Not indexed"));
            }
        }
    }

    private void assertSymbolFoundInIndex(TableReaderDataFrameCursor cursor, TableReaderRecord record, int columnIndex, int M) {
        // SymbolTable is table at table scope, so it will be the same for every
        // data frame here. Get its instance outside of data frame loop.
        SymbolTable symbolTable = cursor.getSymbolTable(columnIndex);

        long count = 0;
        while (cursor.hasNext()) {
            DataFrame frame = cursor.next();
            record.jumpTo(frame.getPartitionIndex(), frame.getRowLo());
            final long limit = frame.getRowHi();

            // BitmapIndex is always at data frame scope, each table can have more than one.
            // we have to get BitmapIndexReader instance once for each frame.
            BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex);

            // because out Symbol column 0 is indexed, frame has to have index.
            Assert.assertNotNull(indexReader);

            // Iterate data frame and advance record by incrementing "recordIndex"
            while (record.getRecordIndex() < limit) {
                CharSequence sym = record.getSym(columnIndex);

                // Assert that index cursor contains offset of current row
                boolean offsetFound = false;
                long target = record.getRecordIndex();

                // Get index cursor for each symbol in data frame
                BitmapIndexCursor ic = indexReader.getCursor(symbolTable.getQuick(sym) + 1, limit - 1);

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

    private void testRemoveFirstColumn(int partitionBy, long increment, int expectedPartitionMin) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
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
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(3, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();

                try (TableReader reader = new TableReader(configuration, "x")) {
                    TableReaderRecord record = new TableReaderRecord(reader);

                    Assert.assertTrue(reader.getPartitionCount() > expectedPartitionMin);

                    TableReaderDataFrameCursor cursor = new TableReaderDataFrameCursor();

                    // assert baseline
                    cursor.of(reader);
                    assertSymbolFoundInIndex(cursor, record, 1, M);
                    cursor.toTop();
                    assertSymbolFoundInIndex(cursor, record, 3, M);

                    writer.removeColumn("a");

                    // Indexes should shift left for both writer and reader
                    // To make sure writer is ok we add more rows
                    for (int i = 0; i < M; i++) {
                        TableWriter.Row row = writer.newRow(timestamp += increment);
                        row.putSym(0, symbols[rnd.nextPositiveInt() % N]);
                        row.putInt(1, rnd.nextInt());
                        row.putSym(2, symbols[rnd.nextPositiveInt() % N]);
                        row.append();
                    }
                    writer.commit();

                    cursor.reload();
                    assertSymbolFoundInIndex(cursor, record, 0, M * 2);
                    cursor.toTop();
                    assertSymbolFoundInIndex(cursor, record, 2, M * 2);
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 0);
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 2);
                }
            }
        });
    }

    private void testRemoveLastColumn(int partitionBy, long increment, int expectedPartitionMin) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
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
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(3, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();

                try (TableReader reader = new TableReader(configuration, "x")) {
                    TableReaderRecord record = new TableReaderRecord(reader);

                    Assert.assertTrue(reader.getPartitionCount() > expectedPartitionMin);

                    TableReaderDataFrameCursor cursor = new TableReaderDataFrameCursor();

                    // assert baseline
                    cursor.of(reader);
                    assertSymbolFoundInIndex(cursor, record, 1, M);
                    cursor.toTop();
                    assertSymbolFoundInIndex(cursor, record, 3, M);

                    writer.removeColumn("c");

                    // Indexes should shift left for both writer and reader
                    // To make sure writer is ok we add more rows
                    for (int i = 0; i < M; i++) {
                        TableWriter.Row row = writer.newRow(timestamp += increment);
                        row.putStr(0, rnd.nextChars(20));
                        row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                        row.putInt(2, rnd.nextInt());
                        row.append();
                    }
                    writer.commit();

                    cursor.reload();
                    assertSymbolFoundInIndex(cursor, record, 1, M * 2);
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 1);
                }
            }
        });
    }

    private void testRemoveMidColumn(int partitionBy, long increment, int expectedPartitionMin) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
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
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(3, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();

                try (TableReader reader = new TableReader(configuration, "x")) {
                    TableReaderRecord record = new TableReaderRecord(reader);

                    Assert.assertTrue(reader.getPartitionCount() > expectedPartitionMin);

                    TableReaderDataFrameCursor cursor = new TableReaderDataFrameCursor();

                    // assert baseline
                    cursor.of(reader);
                    assertSymbolFoundInIndex(cursor, record, 1, M);
                    cursor.toTop();
                    assertSymbolFoundInIndex(cursor, record, 3, M);

                    writer.removeColumn("i");

                    // Indexes should shift left for both writer and reader
                    // To make sure writer is ok we add more rows
                    for (int i = 0; i < M; i++) {
                        TableWriter.Row row = writer.newRow(timestamp += increment);
                        row.putStr(0, rnd.nextChars(20));
                        row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                        row.putSym(2, symbols[rnd.nextPositiveInt() % N]);
                        row.append();
                    }
                    writer.commit();

                    cursor.reload();
                    assertSymbolFoundInIndex(cursor, record, 1, M * 2);
                    cursor.toTop();
                    assertSymbolFoundInIndex(cursor, record, 2, M * 2);
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 1);
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 2);
                }
            }
        });
    }

    private void testReplaceIndexedColWithUnindexed(int partitionBy, long increment, int expectedPartitionMin) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int M = 1000;
            final int N = 100;

            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    timestamp().
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4)
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String symbols[] = new String[N];

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x")) {
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(4, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();

                try (TableReader reader = new TableReader(configuration, "x")) {

                    final TableReaderDataFrameCursor cursor = new TableReaderDataFrameCursor();
                    final TableReaderRecord record = new TableReaderRecord(reader);

                    Assert.assertTrue(reader.getPartitionCount() > expectedPartitionMin);


                    cursor.of(reader);
                    assertSymbolFoundInIndex(cursor, record, 1, M);
                    cursor.toTop();
                    assertSymbolFoundInIndex(cursor, record, 4, M);

                    writer.removeColumn("c");
                    writer.addColumn("c", ColumnType.SYMBOL);

                    for (int i = 0; i < M; i++) {
                        TableWriter.Row row = writer.newRow(timestamp += increment);
                        row.putStr(0, rnd.nextChars(20));
                        row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                        row.putInt(2, rnd.nextInt());
                        row.putSym(4, symbols[rnd.nextPositiveInt() % N]);
                        row.append();
                    }
                    writer.commit();

                    Assert.assertTrue(reader.reload());
                    cursor.reload();
                    assertSymbolFoundInIndex(cursor, record, 1, M * 2);
                    cursor.toTop();
                    assertNoIndex(cursor, 4);
                }
            }
        });
    }

    private void testReplaceUnindexedColWithIndexed(int partitionBy, long increment, int expectedPartitionMin) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int M = 1000;
            final int N = 100;

            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    timestamp().
                    col("c", ColumnType.SYMBOL)
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String symbols[] = new String[N];

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x")) {
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(4, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();

                try (TableReader reader = new TableReader(configuration, "x")) {

                    final TableReaderDataFrameCursor cursor = new TableReaderDataFrameCursor();
                    final TableReaderRecord record = new TableReaderRecord(reader);

                    Assert.assertTrue(reader.getPartitionCount() > expectedPartitionMin);


                    cursor.of(reader);
                    assertSymbolFoundInIndex(cursor, record, 1, M);
                    cursor.toTop();
                    assertNoIndex(cursor, 4);

                    writer.removeColumn("c");
                    writer.addColumn("c", ColumnType.SYMBOL, N, true, true, 8);

                    for (int i = 0; i < M; i++) {
                        TableWriter.Row row = writer.newRow(timestamp += increment);
                        row.putStr(0, rnd.nextChars(20));
                        row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                        row.putInt(2, rnd.nextInt());
                        row.putSym(4, symbols[rnd.nextPositiveInt() % N]);
                        row.append();
                    }
                    writer.commit();

                    Assert.assertTrue(reader.reload());
                    cursor.reload();
                    assertSymbolFoundInIndex(cursor, record, 1, M * 2);
                    cursor.toTop();
                    assertSymbolFoundInIndex(cursor, record, 4, M * 2);
                }
            }
        });
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
                cursor.of(reader);
                assertSymbolFoundInIndex(cursor, record, 0, M);
                cursor.toTop();
                assertSymbolFoundInIndex(cursor, record, 0, M);
                cursor.toTop();
                assertIndexRowsMatchSymbol(cursor, record, 0);
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
                cursor.of(reader);
                assertSymbolFoundInIndex(cursor, record, 0, M * 2);
                cursor.toTop();
                assertSymbolFoundInIndex(cursor, record, 0, M * 2);
                cursor.toTop();
                assertIndexRowsMatchSymbol(cursor, record, 0);
            }
        });
    }
}