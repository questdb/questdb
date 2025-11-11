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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.PageFrameRowCursorFactory;
import io.questdb.griffin.engine.table.SymbolIndexRowCursorFactory;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

public class PageFrameRecordCursorImplFactoryTest extends AbstractCairoTest {

    @Test
    public void testFactory_FullPartitionFrameCursorFactory() throws Exception {
        assertMemoryLeak(() -> {
            final int N = 100;
            // Separate two symbol columns with primitive. It will make problems apparent if the index does not shift correctly
            TableToken tableToken;
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, N / 4).
                    col("i", ColumnType.INT).
                    col("c", ColumnType.SYMBOL).indexed(true, N / 4).
                    timestamp();
            tableToken = AbstractCairoTest.create(model);

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
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
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
                GenericRecordMetadata metadata;
                try (TableReader reader = engine.getReader("x")) {
                    columnIndex = reader.getMetadata().getColumnIndexQuiet("b");
                    symbolKey = reader.getSymbolMapReader(columnIndex).keyOf(value);
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }
                SymbolIndexRowCursorFactory symbolIndexRowCursorFactory = new SymbolIndexRowCursorFactory(
                        columnIndex,
                        symbolKey,
                        true,
                        BitmapIndexReader.DIR_FORWARD,
                        null
                );
                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC)) {
                    // entity index
                    final IntList columnIndexes = new IntList();
                    final IntList columnSizes = new IntList();
                    populateColumnTypes(metadata, columnIndexes, columnSizes);
                    PageFrameRecordCursorFactory factory = new PageFrameRecordCursorFactory(
                            configuration,
                            metadata,
                            frameFactory,
                            symbolIndexRowCursorFactory,
                            false,
                            null,
                            false,
                            columnIndexes,
                            columnSizes,
                            true,
                            false
                    );
                    try (
                            SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                    ) {
                        Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            TestUtils.assertEquals(value, record.getSymA(1));
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory1() throws Exception {
        // many partitions
        // num of rows is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final int skip = 0;
        final long expectedNumOfRows = 10000;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory1_skip() throws Exception {
        // many partitions
        // num of rows is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final int skip = 10;
        final long expectedNumOfRows = 10000;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory2() throws Exception {
        // many partitions
        // num of rows is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 1000000L * 60 * 60;
        final int skip = 0;
        final long expectedNumOfRows = 600;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory2_skip() throws Exception {
        // many partitions
        // num of rows is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 1000000L * 60 * 60;
        final int skip = 10;
        final long expectedNumOfRows = 600;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory3() throws Exception {
        // single partition
        // num of rows is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 100L;
        final int skip = 0;
        final long expectedNumOfRows = 10000;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory3_skip() throws Exception {
        // single partition
        // num of rows is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 100L;
        final int skip = 10;
        final long expectedNumOfRows = 10000;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory4() throws Exception {
        // single partition
        // num of rows is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 100L;
        final int skip = 0;
        final long expectedNumOfRows = 600;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory4_skip() throws Exception {
        // single partition
        // num of rows is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 100L;
        final int skip = 10;
        final long expectedNumOfRows = 600;

        testFactory_FullPartitionFrameCursorFactory(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter1() throws Exception {
        // many partitions
        // num of rows is greater than page frame max rows
        final int numOfRows = 10100;
        final long increment = 1000000L * 60 * 60;
        final int skip = 0;
        final long expectedNumOfRows = 1010;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter1_skip() throws Exception {
        // many partitions
        // num of rows is greater than page frame max rows
        final int numOfRows = 10100;
        final long increment = 1000000L * 60 * 60;
        final int skip = 20;
        final long expectedNumOfRows = 1010;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter2() throws Exception {
        // many partitions
        // num of rows is less than page frame max rows
        final int numOfRows = 9100;
        final long increment = 1000000L * 60 * 60;
        final int skip = 0;
        final long expectedNumOfRows = 910;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter2_skip() throws Exception {
        // many partitions
        // num of rows is less than page frame max rows
        final int numOfRows = 9100;
        final long increment = 1000000L * 60 * 60;
        final int skip = 20;
        final long expectedNumOfRows = 910;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter3() throws Exception {
        // single partition
        // num of rows is greater than page frame max rows
        final int numOfRows = 10100;
        final long increment = 10L;
        final int skip = 0;
        final long expectedNumOfRows = 1010;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter3_skip() throws Exception {
        // single partition
        // num of rows is greater than page frame max rows
        final int numOfRows = 10100;
        final long increment = 10L;
        final int skip = 15;
        final long expectedNumOfRows = 1010;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter4() throws Exception {
        // single partition
        // num of rows is less than page frame max rows
        final int numOfRows = 9100;
        final long increment = 10L;
        final int skip = 0;
        final long expectedNumOfRows = 910;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter4_skip() throws Exception {
        // single partition
        // num of rows is less than page frame max rows
        final int numOfRows = 9100;
        final long increment = 10L;
        final int skip = 15;
        final long expectedNumOfRows = 910;

        testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(numOfRows, increment, skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory1() throws Exception {
        // many partitions
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 0;
        final long expectedNumOfRows = 24 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory1_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalLength = 1000000L * 60 * 60 * 12;
        final int skip = 0;
        final long expectedNumOfRows = 3 * (12 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                0L, intervalLength,
                DAY, DAY + intervalLength,
                2 * DAY, 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory1_nonZeroStart_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalLo = 1000000L * 60 * 60 * 4;
        final long intervalLength = 1000000L * 60 * 60 * 8;
        final int skip = 0;
        final long expectedNumOfRows = 3 * (8 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                intervalLo, intervalLo + intervalLength,
                intervalLo + DAY, intervalLo + DAY + intervalLength,
                intervalLo + 2 * DAY, intervalLo + 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory1_skip() throws Exception {
        // many partitions
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 10;
        final long expectedNumOfRows = 24 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory1_skip_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalLength = 1000000L * 60 * 60 * 12;
        final int skip = 8;
        final long expectedNumOfRows = 3 * (12 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                0L, intervalLength,
                DAY, DAY + intervalLength,
                2 * DAY, 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory1_skip_nonZeroStart_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalLo = 1000000L * 60 * 60 * 4;
        final long intervalLength = 1000000L * 60 * 60 * 8;
        final int skip = 6;
        final long expectedNumOfRows = 3 * (8 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                intervalLo, intervalLo + intervalLength,
                intervalLo + DAY, intervalLo + DAY + intervalLength,
                intervalLo + 2 * DAY, intervalLo + 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory2() throws Exception {
        // many partitions
        // interval is 3 partitions
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 0;
        final long expectedNumOfRows = 24 * 3 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory2_nonZeroStart() throws Exception {
        // many partitions
        // interval is 3 partitions
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long start = 1000000L * 60 * 60 * 8;
        final long intervalHi = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 0;
        final long expectedNumOfRows = -8 + 24 * 3 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{start, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory2_skip() throws Exception {
        // many partitions
        // interval is 3 partitions
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 10;
        final long expectedNumOfRows = 24 * 3 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory2_skip_nonZeroStart() throws Exception {
        // many partitions
        // interval is 3 partitions
        // size is less than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long start = 1000000L * 60 * 60 * 8;
        final long intervalHi = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 1;
        final long expectedNumOfRows = -8 + 24 * 3 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{start, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory3() throws Exception {
        // many partitions
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 0;
        final long expectedNumOfRows = 1440 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory3_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60;
        final long intervalLength = 1000000L * 60 * 60 * 18;
        final int skip = 0;
        final long expectedNumOfRows = 3 * (1080 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                0L, intervalLength,
                DAY, DAY + intervalLength,
                2 * DAY, 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory3_nonZeroStart_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60;
        final long intervalLo = 1000000L * 60 * 60 * 4;
        final long intervalLength = 1000000L * 60 * 60 * 18;
        final int skip = 0;
        final long expectedNumOfRows = 3 * (1080 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                intervalLo, intervalLo + intervalLength,
                intervalLo + DAY, intervalLo + DAY + intervalLength,
                intervalLo + 2 * DAY, intervalLo + 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory3_skip() throws Exception {
        // many partitions
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 10;
        final long expectedNumOfRows = 1440 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory3_skip_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60;
        final long intervalLength = 1000000L * 60 * 60 * 18;
        final int skip = 100;
        final long expectedNumOfRows = 3 * (1080 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                0L, intervalLength,
                DAY, DAY + intervalLength,
                2 * DAY, 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory3_skip_nonZeroStart_multipleIntervals() throws Exception {
        // many partitions
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1000000L * 60;
        final long intervalLo = 1000000L * 60 * 60 * 2;
        final long intervalLength = 1000000L * 60 * 60 * 18;
        final int skip = 100;
        final long expectedNumOfRows = 3 * (1080 + 1);

        final long DAY = 1000000L * 60 * 60 * 24;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{
                intervalLo, intervalLo + intervalLength,
                intervalLo + DAY, intervalLo + DAY + intervalLength,
                intervalLo + 2 * DAY, intervalLo + 2 * DAY + intervalLength
        }), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory4() throws Exception {
        // single partition
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1L;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 0;
        final long expectedNumOfRows = 10000;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory4_skip() throws Exception {
        // single partition
        // interval is the first partition
        // size is greater than page frame max rows
        final int numOfRows = 10000;
        final long increment = 1L;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 10;
        final long expectedNumOfRows = 10000;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory5() throws Exception {
        // single partition
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 10L;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 0;
        final long expectedNumOfRows = 600;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory5_skip() throws Exception {
        // single partition
        // interval is the first partition
        // size is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 10L;
        final long intervalHi = 1000000L * 60 * 60 * 24;
        final int skip = 10;
        final long expectedNumOfRows = 600;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory6() throws Exception {
        // single partition
        // interval does not include full partition
        // size is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 100L;
        final long intervalHi = 1000L * 30;
        final int skip = 0;
        final long expectedNumOfRows = 300 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory6_skip() throws Exception {
        // single partition
        // interval does not include full partition
        // size is less than page frame max rows
        final int numOfRows = 600;
        final long increment = 100L;
        final long intervalHi = 1000L * 30;
        final int skip = 10;
        final long expectedNumOfRows = 300 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{0L, intervalHi}), skip, expectedNumOfRows);
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

    private void populateColumnTypes(RecordMetadata metadata, IntList columnIndexes, IntList columnSizes) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
            columnSizes.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
        }
    }

    private void testBwdPageFrameCursor(int rowCount, int maxSize, int startTopAt) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, maxSize);

        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.HOUR).
                    col("i", ColumnType.INT).
                    timestamp();
            TableToken tableToken = AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();
            final long increment = 1000000 * 60L * 4;

            // memoize Rnd output to be able to iterate it in backwards direction
            int[] rndInts = new int[rowCount];
            long[] rndLongs = new long[rowCount];
            CharSequence[] rndStrs = new CharSequence[rowCount];

            // prepare the data, writing rows in the backward direction
            long timestamp = 0;
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
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
                GenericRecordMetadata metadata;
                try (TableReader reader = engine.getReader("x")) {
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }

                final IntList columnIndexes = new IntList();
                final IntList columnSizes = new IntList();
                populateColumnTypes(metadata, columnIndexes, columnSizes);

                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC)) {
                    PageFrameRowCursorFactory rowCursorFactory = new PageFrameRowCursorFactory(ORDER_ASC); // stub RowCursorFactory
                    try (PageFrameRecordCursorFactory factory = new PageFrameRecordCursorFactory(
                            configuration,
                            metadata,
                            frameFactory,
                            rowCursorFactory,
                            false,
                            null,
                            true,
                            columnIndexes,
                            columnSizes,
                            true,
                            false
                    )) {

                        Assert.assertTrue(factory.supportsPageFrameCursor());

                        long ts = (rowCount + 1) * increment;
                        int rowIndex = rowCount - 1;
                        final DirectString dcs = new DirectString();
                        try (
                                SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
                                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_DESC)
                        ) {
                            PageFrame frame;
                            while ((frame = cursor.next()) != null) {

                                long len = frame.getPartitionHi() - frame.getPartitionLo();
                                Assert.assertTrue(len > 0);
                                Assert.assertTrue(len <= maxSize);

                                long intColAddr = frame.getPageAddress(0);
                                long tsColAddr = frame.getPageAddress(1);
                                long longColAddr = frame.getPageAddress(2);
                                long iStrColAddr = frame.getAuxPageAddress(3);
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
                }
            }
        });
        // This method can be called multiple times during the test. Remove created tables.
        tearDown();
        setUp();
    }

    private void testFactory_FullPartitionFrameCursorFactory(long increment, int skip, long expectedNumOfRows, int order) throws SqlException {
        try (CairoEngine engine = new CairoEngine(configuration)) {
            GenericRecordMetadata metadata;
            TableToken tableToken;
            try (TableReader reader = engine.getReader("x")) {
                metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                tableToken = reader.getTableToken();
            }
            final RowCursorFactory rowFactory = new PageFrameRowCursorFactory(order);
            try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, order)) {
                // entity index
                final IntList columnIndexes = new IntList();
                final IntList columnSizes = new IntList();
                populateColumnTypes(metadata, columnIndexes, columnSizes);
                PageFrameRecordCursorFactory factory = new PageFrameRecordCursorFactory(
                        configuration,
                        metadata,
                        frameFactory,
                        rowFactory,
                        false,
                        null,
                        false,
                        columnIndexes,
                        columnSizes,
                        true,
                        false
                );
                try (
                        SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
                        RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                ) {
                    RecordCursor.Counter counter = new RecordCursor.Counter();

                    if (skip > 0) {
                        Record record = cursor.getRecord();
                        while (counter.get() < skip && cursor.hasNext()) {
                            Assert.assertEquals((order == ORDER_ASC ? counter.get() : expectedNumOfRows - counter.get() - 1) * increment, record.getTimestamp(3));
                            counter.inc();
                        }
                    }

                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(expectedNumOfRows, counter.get());
                }
            }
        }
    }

    private void testFactory_FullPartitionFrameCursorFactory(int numOfRows, long increment, int skip, long expectedNumOfRows) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, "10");
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, "1000");

        assertMemoryLeak(() -> {
            final int numOfSymbols = 100;
            final TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).
                    col("i", ColumnType.INT).
                    timestamp();
            AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();

            final String[] symbols = new String[numOfSymbols];
            for (int i = 0; i < numOfSymbols; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            rnd.reset();

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < numOfRows; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % numOfSymbols]);
                    row.putInt(2, rnd.nextInt());
                    row.append();

                    timestamp += increment;
                }
                writer.commit();
            }

            testFactory_FullPartitionFrameCursorFactory(increment, skip, expectedNumOfRows, ORDER_ASC);
            testFactory_FullPartitionFrameCursorFactory(increment, skip, expectedNumOfRows, ORDER_DESC);
        });
    }

    private void testFactory_FullPartitionFrameCursorFactory_SymbolIndexFilter(int numOfRows, long increment, int skip, long expectedNumOfRows) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, "10");
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, "1000");

        assertMemoryLeak(() -> {
            final int numOfSymbols = 10;
            final TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).indexed(true, numOfSymbols).
                    col("i", ColumnType.INT).
                    timestamp();
            final TableToken tableToken = AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();

            final String[] symbols = new String[numOfSymbols];
            for (int i = 0; i < numOfSymbols; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            rnd.reset();

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < numOfRows; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[i % numOfSymbols]);
                    row.putInt(2, rnd.nextInt());
                    row.append();

                    timestamp += increment;
                }
                writer.commit();
            }

            try (CairoEngine engine = new CairoEngine(configuration)) {
                String value = symbols[0];
                int columnIndex;
                int symbolKey;
                GenericRecordMetadata metadata;
                try (TableReader reader = engine.getReader("x")) {
                    columnIndex = reader.getMetadata().getColumnIndexQuiet("b");
                    symbolKey = reader.getSymbolMapReader(columnIndex).keyOf(value);
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }
                SymbolIndexRowCursorFactory symbolIndexRowCursorFactory = new SymbolIndexRowCursorFactory(
                        columnIndex,
                        symbolKey,
                        true,
                        BitmapIndexReader.DIR_FORWARD,
                        null
                );
                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC)) {
                    // entity index
                    final IntList columnIndexes = new IntList();
                    final IntList columnSizes = new IntList();
                    populateColumnTypes(metadata, columnIndexes, columnSizes);
                    PageFrameRecordCursorFactory factory = new PageFrameRecordCursorFactory(
                            configuration,
                            metadata,
                            frameFactory,
                            symbolIndexRowCursorFactory,
                            false,
                            null,
                            false,
                            columnIndexes,
                            columnSizes,
                            true,
                            false
                    );
                    try (
                            SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                    ) {
                        RecordCursor.Counter counter = new RecordCursor.Counter();

                        if (skip > 0) {
                            Record record = cursor.getRecord();
                            while (counter.get() < skip && cursor.hasNext()) {
                                Assert.assertEquals(counter.get() * numOfSymbols * increment, record.getTimestamp(3));
                                counter.inc();
                            }
                        }

                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(expectedNumOfRows, counter.get());
                    }
                }
            }
        });
    }

    private void testFactory_IntervalPartitionFrameCursorFactory(int numOfRows, long increment, LongList intervals, int skip, long expectedNumOfRows) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, "10");
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, "1000");

        assertMemoryLeak(() -> {
            final TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.STRING).
                    col("b", ColumnType.SYMBOL).
                    col("i", ColumnType.INT).
                    timestamp();
            final TableToken tableToken = AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();

            final int numOfSymbols = 100;
            final String[] symbols = new String[numOfSymbols];
            for (int i = 0; i < numOfSymbols; i++) {
                symbols[i] = rnd.nextChars(8).toString();
            }

            rnd.reset();

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < numOfRows; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putStr(0, rnd.nextChars(20));
                    row.putSym(1, symbols[rnd.nextPositiveInt() % numOfSymbols]);
                    row.putInt(2, rnd.nextInt());
                    row.append();

                    timestamp += increment;
                }
                writer.commit();
            }

            try (CairoEngine engine = new CairoEngine(configuration)) {
                GenericRecordMetadata metadata;
                int timestampType;
                int timestampIndex;
                try (TableReader reader = engine.getReader("x")) {
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                    timestampType = reader.getMetadata().getTimestampType();
                    timestampIndex = reader.getMetadata().getTimestampIndex();
                }

                final RuntimeIntervalModel intervalModel = new RuntimeIntervalModel(
                        ColumnType.getTimestampDriver(timestampType),
                        PartitionBy.DAY,
                        intervals,
                        new ObjList<>()
                );
                final RowCursorFactory rowFactory = new PageFrameRowCursorFactory(ORDER_ASC);
                try (IntervalPartitionFrameCursorFactory frameFactory = new IntervalPartitionFrameCursorFactory(
                        tableToken, TableUtils.ANY_TABLE_VERSION, intervalModel, timestampIndex, metadata, ORDER_ASC
                )) {
                    final IntList columnIndexes = new IntList();
                    final IntList columnSizes = new IntList();
                    populateColumnTypes(metadata, columnIndexes, columnSizes);
                    PageFrameRecordCursorFactory factory = new PageFrameRecordCursorFactory(
                            configuration,
                            metadata,
                            frameFactory,
                            rowFactory,
                            false,
                            null,
                            false,
                            columnIndexes,
                            columnSizes,
                            true,
                            false
                    );
                    try (
                            SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
                            RecordCursor cursor = factory.getCursor(sqlExecutionContext)
                    ) {
                        RecordCursor.Counter counter = new RecordCursor.Counter();

                        if (skip > 0) {
                            Record record = cursor.getRecord();
                            while (counter.get() < skip && cursor.hasNext()) {
                                Assert.assertEquals(intervals.getQuick(0) + counter.get() * increment, record.getTimestamp(3));
                                counter.inc();
                            }
                        }

                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(expectedNumOfRows, counter.get());
                    }
                }
            }
        });
    }

    private void testFwdPageFrameCursor(int rowCount, int maxSize, int startTopAt) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, maxSize);

        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.HOUR).
                    col("i", ColumnType.INT).
                    timestamp();
            TableToken tt = AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();
            final long increment = 1000000 * 60L * 4;

            // prepare the data
            long timestamp = 0;
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
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
                GenericRecordMetadata metadata;
                try (TableReader reader = engine.getReader("x")) {
                    metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
                }

                final IntList columnIndexes = new IntList();
                final IntList columnSizes = new IntList();
                populateColumnTypes(metadata, columnIndexes, columnSizes);

                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tt, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC)) {
                    PageFrameRowCursorFactory rowCursorFactory = new PageFrameRowCursorFactory(ORDER_ASC); // stub RowCursorFactory
                    try (PageFrameRecordCursorFactory factory = new PageFrameRecordCursorFactory(
                            configuration,
                            metadata,
                            frameFactory,
                            rowCursorFactory,
                            false,
                            null,
                            true,
                            columnIndexes,
                            columnSizes,
                            true,
                            false
                    )) {

                        Assert.assertTrue(factory.supportsPageFrameCursor());

                        rnd.reset();
                        long ts = 0;
                        int rowIndex = 0;
                        final DirectString dcs = new DirectString();
                        try (
                                SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine);
                                PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)
                        ) {
                            PageFrame frame;
                            while ((frame = cursor.next()) != null) {

                                long len = frame.getPartitionHi() - frame.getPartitionLo();
                                Assert.assertTrue(len > 0);
                                Assert.assertTrue(len <= maxSize);

                                long intColAddr = frame.getPageAddress(0);
                                long tsColAddr = frame.getPageAddress(1);
                                long longColAddr = frame.getPageAddress(2);
                                long iStrColAddr = frame.getAuxPageAddress(3);
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
                }
            }
        });
        tearDown();
        setUp();
    }
}
