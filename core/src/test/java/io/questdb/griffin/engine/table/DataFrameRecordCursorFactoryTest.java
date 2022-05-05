/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectCharSequence;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_DESC;

public class DataFrameRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testFactory() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int N = 100;
            // separate two symbol columns with primitive. It will make problems apparent if index does not shift correctly
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final String[] symbols = new String[N];
            final int M = 1000;
            final long increment = 1000000 * 60L * 4;

            for (int i = 0; i < N; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            rnd.reset();

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x", metrics)) {
                for (int i = 0; i < M; i++) {
                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % N]);
                    row.putInt(2, rnd.nextInt());
                    row.putSym(3, symbols[rnd.nextPositiveInt() % N]);
                    row.append();
                }
                writer.commit();
            }

            try (CairoEngine engine = new CairoEngine(configuration)) {
                String value = symbols[N - 10];
                int columnIndex;
                int symbolKey;
                RecordMetadata metadata;
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION)) {
                    columnIndex = reader.getMetadata().getColumnIndexQuiet("b");
                    symbolKey = reader.getSymbolMapReader(columnIndex).keyOf(value);
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }
                SymbolIndexRowCursorFactory symbolIndexRowCursorFactory = new SymbolIndexRowCursorFactory(columnIndex, symbolKey, true, BitmapIndexReader.DIR_FORWARD, null);
                FullFwdDataFrameCursorFactory dataFrameFactory = new FullFwdDataFrameCursorFactory(engine, "x", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);

                // entity index
                final IntList columnIndexes = new IntList();
                final IntList columnSizes = new IntList();
                populateColumnTypes(metadata, columnIndexes, columnSizes);
                DataFrameRecordCursorFactory factory = new DataFrameRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameFactory,
                        symbolIndexRowCursorFactory,
                        false,
                        null,
                        false,
                        columnIndexes,
                        columnSizes,
                        true
                );
                SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllCairoSecurityContext.INSTANCE, null, null, -1, null);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Record record = cursor.getRecord();
                    while (cursor.hasNext()) {
                        TestUtils.assertEquals(value, record.getSym(1));
                    }
                }
            }
        });
    }

    @Test
    public void testPageFrameCursorNoColTops() throws Exception {
        // pageFrameMaxSize < rowCount
        testFwdPageFrameCursor(64, 8, -1);
        testFwdPageFrameCursor(65, 8, -1);
        // pageFrameMaxSize == rowCount
        testFwdPageFrameCursor(64, 64, -1);
        // pageFrameMaxSize > rowCount
        testFwdPageFrameCursor(63, 64, -1);
    }

    @Test
    public void testPageFrameCursorWithColTops() throws Exception {
        // pageFrameMaxSize < rowCount
        testFwdPageFrameCursor(64, 8, 3);
        testFwdPageFrameCursor(64, 8, 8);
        testFwdPageFrameCursor(65, 8, 11);
        // pageFrameMaxSize == rowCount
        testFwdPageFrameCursor(64, 64, 32);
        // pageFrameMaxSize > rowCount
        testFwdPageFrameCursor(63, 64, 61);
    }

    @Test
    public void testPageFrameBwdCursorNoColTops() throws Exception {
        // pageFrameMaxSize < rowCount
        testBwdPageFrameCursor(64, 8, -1);
        testBwdPageFrameCursor(65, 8, -1);
        // pageFrameMaxSize == rowCount
        testBwdPageFrameCursor(64, 64, -1);
        // pageFrameMaxSize > rowCount
        testBwdPageFrameCursor(63, 64, -1);
    }

    @Test
    public void testPageFrameBwdCursorWithColTops() throws Exception {
        // pageFrameMaxSize < rowCount
        testBwdPageFrameCursor(64, 8, 3);
        testBwdPageFrameCursor(64, 8, 8);
        testBwdPageFrameCursor(65, 8, 11);
        // pageFrameMaxSize == rowCount
        testBwdPageFrameCursor(64, 64, 32);
        // pageFrameMaxSize > rowCount
        testBwdPageFrameCursor(63, 64, 61);
    }

    private void populateColumnTypes(RecordMetadata metadata, IntList columnIndexes, IntList columnSizes) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
            columnSizes.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
        }
    }

    private void testFwdPageFrameCursor(int rowCount, int maxSize, int startTopAt) throws Exception {
        pageFrameMaxRows = maxSize;

        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.HOUR).
                    col("i", ColumnType.INT).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final long increment = 1000000 * 60L * 4;

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x", metrics)) {
                int iIndex = writer.getColumnIndex("i");
                int jIndex = -1;
                int sIndex = -1;
                for (int i = 0; i < rowCount; i++) {
                    if (i == startTopAt) {
                        writer.addColumn("j", ColumnType.LONG);
                        jIndex = writer.getColumnIndex("j");
                        writer.addColumn("s", ColumnType.STRING);
                        sIndex = writer.getColumnIndex("s");
                    }

                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    row.putInt(iIndex, rnd.nextInt());
                    if (startTopAt > 0 && i >= startTopAt) {
                        row.putLong(jIndex, rnd.nextLong());
                        row.putStr(sIndex, rnd.nextChars(32));
                    }
                    row.append();
                }
                writer.commit();
            }

            try (CairoEngine engine = new CairoEngine(configuration)) {
                RecordMetadata metadata;
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION)) {
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }

                final IntList columnIndexes = new IntList();
                final IntList columnSizes = new IntList();
                populateColumnTypes(metadata, columnIndexes, columnSizes);

                FullFwdDataFrameCursorFactory dataFrameFactory = new FullFwdDataFrameCursorFactory(engine, "x", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
                DataFrameRowCursorFactory rowCursorFactory = new DataFrameRowCursorFactory(); // stub RowCursorFactory
                DataFrameRecordCursorFactory factory = new DataFrameRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameFactory,
                        rowCursorFactory,
                        false,
                        null,
                        true,
                        columnIndexes,
                        columnSizes,
                        true
                );

                SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        null,
                        null,
                        -1,
                        null
                );

                Assert.assertTrue(factory.supportPageFrameCursor());

                rnd.reset();
                long ts = 0;
                int rowIndex = 0;
                final DirectCharSequence dcs = new DirectCharSequence();
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {

                        long len = frame.getPartitionHi() - frame.getPartitionLo();
                        Assert.assertTrue(len > 0);
                        Assert.assertTrue(len <= maxSize);

                        long intColAddr = frame.getPageAddress(0);
                        long tsColAddr = frame.getPageAddress(1);
                        long longColAddr = frame.getPageAddress(2);
                        long iStrColAddr = frame.getIndexPageAddress(3);
                        long dStrColAddr = frame.getPageAddress(3);

                        for (long i = 0; i < len; i++, rowIndex++) {
                            Assert.assertEquals(rnd.nextInt(), Unsafe.getUnsafe().getInt(intColAddr + i * 4L));
                            Assert.assertEquals(ts += increment, Unsafe.getUnsafe().getLong(tsColAddr + i * 8L));

                            if (startTopAt > 0 && rowIndex >= startTopAt) {
                                Assert.assertEquals(rnd.nextLong(), Unsafe.getUnsafe().getLong(longColAddr + i * 8L));
                                final long strOffset = Unsafe.getUnsafe().getLong(iStrColAddr + i * 8);
                                dcs.of(dStrColAddr + strOffset + 4, dStrColAddr + Unsafe.getUnsafe().getLong(iStrColAddr + i * 8 + 8));
                                TestUtils.assertEquals(rnd.nextChars(32), dcs);
                            }
                        }
                    }
                    Assert.assertEquals(rowCount, rowIndex);
                }
            }
        });
    }

    private void testBwdPageFrameCursor(int rowCount, int maxSize, int startTopAt) throws Exception {
        pageFrameMaxRows = maxSize;

        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.HOUR).
                    col("i", ColumnType.INT).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            final long increment = 1000000 * 60L * 4;

            // memoize Rnd output to be able to iterate it in backwards direction
            int[] rndInts = new int[rowCount];
            long[] rndLongs = new long[rowCount];
            CharSequence[] rndStrs = new CharSequence[rowCount];

            // prepare the data, writing rows in the backward direction
            long timestamp = 0;
            try (TableWriter writer = new TableWriter(configuration, "x", metrics)) {
                int iIndex = writer.getColumnIndex("i");
                int jIndex = -1;
                int sIndex = -1;
                for (int i = 0; i < rowCount; i++) {
                    if (i == startTopAt) {
                        writer.addColumn("j", ColumnType.LONG);
                        jIndex = writer.getColumnIndex("j");
                        writer.addColumn("s", ColumnType.STRING);
                        sIndex = writer.getColumnIndex("s");
                    }

                    TableWriter.Row row = writer.newRow(timestamp += increment);
                    rndInts[i] = rnd.nextInt();
                    row.putInt(iIndex, rndInts[i]);
                    if (startTopAt > 0 && i >= startTopAt) {
                        rndLongs[i] = rnd.nextLong();
                        row.putLong(jIndex, rndLongs[i]);
                        rndStrs[i] = rnd.nextChars(32).toString();
                        row.putStr(sIndex, rndStrs[i]);
                    }
                    row.append();
                }
                writer.commit();
            }

            try (CairoEngine engine = new CairoEngine(configuration)) {
                RecordMetadata metadata;
                try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION)) {
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }

                final IntList columnIndexes = new IntList();
                final IntList columnSizes = new IntList();
                populateColumnTypes(metadata, columnIndexes, columnSizes);

                FullFwdDataFrameCursorFactory dataFrameFactory = new FullFwdDataFrameCursorFactory(engine, "x", TableUtils.ANY_TABLE_ID, TableUtils.ANY_TABLE_VERSION);
                DataFrameRowCursorFactory rowCursorFactory = new DataFrameRowCursorFactory(); // stub RowCursorFactory
                DataFrameRecordCursorFactory factory = new DataFrameRecordCursorFactory(
                        configuration,
                        metadata,
                        dataFrameFactory,
                        rowCursorFactory,
                        false,
                        null,
                        true,
                        columnIndexes,
                        columnSizes,
                        true
                );

                SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(
                        AllowAllCairoSecurityContext.INSTANCE,
                        null,
                        null,
                        -1,
                        null
                );

                Assert.assertTrue(factory.supportPageFrameCursor());

                long ts = (rowCount + 1) * increment;
                int rowIndex = rowCount - 1;
                final DirectCharSequence dcs = new DirectCharSequence();
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_DESC)) {
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {

                        long len = frame.getPartitionHi() - frame.getPartitionLo();
                        Assert.assertTrue(len > 0);
                        Assert.assertTrue(len <= maxSize);

                        long intColAddr = frame.getPageAddress(0);
                        long tsColAddr = frame.getPageAddress(1);
                        long longColAddr = frame.getPageAddress(2);
                        long iStrColAddr = frame.getIndexPageAddress(3);
                        long dStrColAddr = frame.getPageAddress(3);

                        for (long i = len - 1; i > -1; i--) {
                            Assert.assertEquals(rndInts[rowIndex], Unsafe.getUnsafe().getInt(intColAddr + i * 4L));
                            Assert.assertEquals(ts -= increment, Unsafe.getUnsafe().getLong(tsColAddr + i * 8L));

                            if (startTopAt > 0 && rowIndex >= startTopAt) {
                                Assert.assertEquals(rndLongs[rowIndex], Unsafe.getUnsafe().getLong(longColAddr + i * 8L));
                                final long strOffset = Unsafe.getUnsafe().getLong(iStrColAddr + i * 8);
                                dcs.of(dStrColAddr + strOffset + 4, dStrColAddr + Unsafe.getUnsafe().getLong(iStrColAddr + i * 8 + 8));
                                TestUtils.assertEquals(rndStrs[rowIndex], dcs);
                            }
                            rowIndex--;
                        }
                    }
                    Assert.assertEquals(-1, rowIndex);
                }
            }
        });
    }
}