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

package io.questdb.test.griffin.engine.table;

import io.questdb.PropertyKey;
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
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.BwdTableReaderPageFrameCursor;
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
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.TableModel;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

public class PageFrameRecordCursorImplFactoryTest extends AbstractCairoTest {
    // One partition of 100 rows whose first two predate the ADD COLUMN. An ordinary walk cuts that
    // partition into two frames at the column top; a skip walk cuts it at the skip target instead.
    // Both cuts fit inside the default page frame size, so this needs no frame-size override.
    private static final String COLUMN_TOP_TABLE_ADD_COLUMN = "ALTER TABLE t ADD COLUMN pad INT";
    private static final String COLUMN_TOP_TABLE_DDL =
            "CREATE TABLE t (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL";
    private static final String COLUMN_TOP_TABLE_DML_ABOVE_TOP = """
            INSERT INTO t
            SELECT (x + 2)::INT, timestamp_sequence('2024-01-01T00:00:02', 1_000_000L), (x + 2)::INT
            FROM long_sequence(98)
            """;
    private static final String COLUMN_TOP_TABLE_DML_BELOW_TOP = """
            INSERT INTO t VALUES
            (1, '2024-01-01T00:00:00.000000Z'),
            (2, '2024-01-01T00:00:01.000000Z')
            """;
    // Three partitions of 5, 5 and 3 rows: a skip of 4 lands inside the first, which leaves the
    // frames of the two that follow numbered one lower than an ordinary walk numbers them.
    private static final String SKIP_WALK_TABLE_DDL =
            "CREATE TABLE bids (i INT, rating STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY";
    private static final String SKIP_WALK_TABLE_DML = """
            INSERT INTO bids VALUES
            (1, 'GOOD', '2000-01-01T00:00:00.000000Z'),
            (2, 'GOOD', '2000-01-01T00:00:01.000000Z'),
            (3, 'SCAM', '2000-01-01T00:00:02.000000Z'),
            (4, 'SCAM', '2000-01-01T00:00:03.000000Z'),
            (5, 'EXCELLENT', '2000-01-01T00:00:04.000000Z'),
            (6, 'SCAM', '2000-01-02T00:00:00.000000Z'),
            (7, 'GOOD', '2000-01-02T00:00:01.000000Z'),
            (8, 'GOOD', '2000-01-02T00:00:02.000000Z'),
            (9, 'GOOD', '2000-01-02T00:00:03.000000Z'),
            (10, 'GOOD', '2000-01-02T00:00:04.000000Z'),
            (11, 'SCAM', '2000-01-03T00:00:00.000000Z'),
            (12, 'UNKNOWN', '2000-01-03T00:00:01.000000Z'),
            (13, 'GOOD', '2000-01-03T00:00:02.000000Z')
            """;

    @Override
    public void setUp() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, TestUtils.randomSymbolIndexTypeName(rnd));
        super.setUp();
    }

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
                RowCursorFactory symbolIndexRowCursorFactory = new SymbolIndexRowCursorFactory(
                        columnIndex,
                        symbolKey,
                        IndexReader.DIR_FORWARD,
                        null
                );
                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC, null, 0, false)) {
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
    public void testFactory_IntervalPartitionFrameCursorFactory_openStart() throws Exception {
        // many partitions
        // open lower bound (ts < X), interval spans multiple partitions.
        // The backward interval cursor's calculateSize() once stopped after the
        // first (newest) partition for this shape and undercounted by one.
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 0;
        final long expectedNumOfRows = 24 * 3 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{Long.MIN_VALUE, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory_openStart_skip() throws Exception {
        // many partitions
        // open lower bound (ts < X), interval spans multiple partitions, with a
        // partial iteration before calculateSize() to exercise the mid-scan path.
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalHi = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 10;
        final long expectedNumOfRows = 24 * 3 + 1;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{Long.MIN_VALUE, intervalHi}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory_openEnd() throws Exception {
        // many partitions
        // open upper bound (ts > X), interval spans most partitions. Forward and
        // backward calculateSize() must both walk every partition in the range.
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalLo = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 0;
        final long expectedNumOfRows = numOfRows - 24 * 3;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{intervalLo, Long.MAX_VALUE}), skip, expectedNumOfRows);
    }

    @Test
    public void testFactory_IntervalPartitionFrameCursorFactory_openEnd_skip() throws Exception {
        // many partitions
        // open upper bound (ts > X) with a partial iteration before calculateSize().
        final int numOfRows = 10000;
        final long increment = 1000000L * 60 * 60;
        final long intervalLo = 1000000L * 60 * 60 * 24 * 3;
        final int skip = 10;
        final long expectedNumOfRows = numOfRows - 24 * 3;

        testFactory_IntervalPartitionFrameCursorFactory(numOfRows, increment, new LongList(new long[]{intervalLo, Long.MAX_VALUE}), skip, expectedNumOfRows);
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
        testBwdPageFrameCursor(64, 4, 8, -1);
        testBwdPageFrameCursor(65, 4, 8, -1);
        // pageFrameMaxSize == rowCount
        testBwdPageFrameCursor(64, 32, 64, -1);
        // pageFrameMaxSize > rowCount
        testBwdPageFrameCursor(63, 32, 64, -1);
    }

    @Test
    public void testPageFrameBwdCursorWithColTops() throws Exception {
        // pageFrameMaxSize < rowCount
        testBwdPageFrameCursor(64, 8, 8, 3);
        testBwdPageFrameCursor(64, 8, 8, 8);
        testBwdPageFrameCursor(65, 8, 8, 11);
        // pageFrameMaxSize == rowCount
        testBwdPageFrameCursor(64, 64, 64, 32);
        // pageFrameMaxSize > rowCount
        testBwdPageFrameCursor(63, 64, 64, 61);
    }

    @Test
    public void testPageFrameCursorNoColTops() throws Exception {
        // pageFrameMaxSize < rowCount
        testFwdPageFrameCursor(64, 4, 8, -1);
        testFwdPageFrameCursor(65, 4, 8, -1);
        // pageFrameMaxSize == rowCount
        testFwdPageFrameCursor(64, 32, 64, -1);
        // pageFrameMaxSize > rowCount
        testFwdPageFrameCursor(63, 32, 64, -1);
    }

    @Test
    public void testPageFrameCursorWithColTops() throws Exception {
        // pageFrameMaxSize < rowCount
        testFwdPageFrameCursor(64, 8, 8, 3);
        testFwdPageFrameCursor(64, 8, 8, 8);
        testFwdPageFrameCursor(65, 8, 8, 11);
        // pageFrameMaxSize == rowCount
        testFwdPageFrameCursor(64, 64, 64, 32);
        // pageFrameMaxSize > rowCount
        testFwdPageFrameCursor(63, 64, 64, 61);
    }

    @Test
    public void testPageFrameMemoryRecordOfNullDropsSymbolTableSource() {
        class TestPageFrameMemoryRecord extends PageFrameMemoryRecord {
            private boolean hasSymbolTableSource() {
                return symbolTableSource != null;
            }
        }

        final TestPageFrameMemoryRecord record = new TestPageFrameMemoryRecord();
        try {
            record.of(new SymbolTableSource() {
                @Override
                public SymbolTable getSymbolTable(int columnIndex) {
                    return null;
                }

                @Override
                public SymbolTable newSymbolTable(int columnIndex) {
                    return null;
                }
            });
            Assert.assertTrue(record.hasSymbolTableSource());
            record.of(null);
            Assert.assertFalse(record.hasSymbolTableSource());
        } finally {
            record.close();
        }
    }

    /**
     * A skip walk cuts page frames at the skip target, an ordinary walk cuts them at the partition,
     * column-top and page-frame-size boundaries, and the two number the frames they produce
     * differently. The address cache is keyed by that number and survives toTop(), so a skip walk
     * that runs over the frames of an earlier ordinary walk used to read their addresses and page
     * limits with its own row counts - reading a STRING past the end of its aux page, or returning
     * another partition's rows.
     * <p>
     * The cursor-level tests below drive the cursor directly rather than through assertQuery(): the
     * defect needs a specific call sequence on ONE cursor (walk, then toTop, then a skip of a chosen
     * size), which a query's result alone cannot pin down - QueryAssertion picks its skip sizes at
     * random. The one query-level test, testNestedLimitRereadsTheSameRows(), goes through
     * assertQuery().returns() instead: a nested LIMIT issues that call sequence on its own, so a
     * query's result does pin that one down.
     */
    @Test
    public void testSkipRowsAfterFullWalkReadsTail() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(13, walk(cursor, null));
                cursor.toTop();
                skipRows(cursor, 4);
                assertRemainingRows(cursor, """
                        5|EXCELLENT
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * The same defect through the other cut: a skip target that covers a whole partition collapses
     * the two frames a column top splits that partition into.
     */
    @Test
    public void testSkipRowsOverColumnTopAfterFullWalkReadsTail() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE bids (i INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO bids VALUES
                    (1, '2000-01-01T00:00:00.000000Z'),
                    (2, '2000-01-01T00:00:01.000000Z'),
                    (3, '2000-01-01T00:00:02.000000Z')
                    """);
            execute("ALTER TABLE bids ADD COLUMN rating STRING");
            execute("""
                    INSERT INTO bids VALUES
                    (4, '2000-01-01T00:00:03.000000Z', 'SCAM'),
                    (5, '2000-01-01T00:00:04.000000Z', 'EXCELLENT'),
                    (6, '2000-01-02T00:00:00.000000Z', 'SCAM'),
                    (7, '2000-01-02T00:00:01.000000Z', 'GOOD'),
                    (8, '2000-01-02T00:00:02.000000Z', 'GOOD'),
                    (9, '2000-01-02T00:00:03.000000Z', 'GOOD'),
                    (10, '2000-01-02T00:00:04.000000Z', 'GOOD'),
                    (11, '2000-01-03T00:00:00.000000Z', 'SCAM'),
                    (12, '2000-01-03T00:00:01.000000Z', 'UNKNOWN'),
                    (13, '2000-01-03T00:00:02.000000Z', 'GOOD')
                    """);
            try (
                    RecordCursorFactory factory = engine.select("SELECT i, rating FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(13, walk(cursor, null));
                cursor.toTop();
                skipRows(cursor, 5);
                assertRemainingRows(cursor, """
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * The reverse order: an ordinary walk must not run over the frames a skip walk cached either.
     * A skipped frame carries no addresses at all, so reading its rows returns NULLs rather than
     * throwing.
     */
    @Test
    public void testFullWalkAfterSkipRowsReadsEveryRow() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                skipRows(cursor, 4);
                Assert.assertEquals(9, walk(cursor, null));
                cursor.toTop();
                assertRemainingRows(cursor, """
                        1|GOOD
                        2|GOOD
                        3|SCAM
                        4|SCAM
                        5|EXCELLENT
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * A layout can be cut by more than one skip: a from-top skip shapes its leading frames and a
     * later mid-walk skip appends its own, differently cut ones. The cursor must not then reuse that
     * layout for a walk that repeats only the leading skip - past the landing it cuts frames where an
     * ordinary walk cuts them, over ordinals the second skip filled with its own frames.
     */
    @Test
    public void testSkipRowsRepeatedAfterMidWalkSkipReadsTail() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                skipRows(cursor, 4);
                Assert.assertTrue("the skip of 4 left no rows behind", cursor.hasNext());
                skipRows(cursor, 2);
                cursor.toTop();
                skipRows(cursor, 4);
                assertRemainingRows(cursor, """
                        5|EXCELLENT
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * The same two cuts as the test above, but with the mid-walk skip asking for exactly as many rows
     * as the leading one. Only the ordinal the skip was issued at then tells the two cuts apart: the
     * mid-walk skip records its own, non-zero ordinal, which is what stops the last skip below -
     * issued from the top, so at ordinal 0 - from reusing a layout whose frames past the landing are
     * the second skip's, not an ordinary walk's.
     */
    @Test
    public void testSkipRowsRepeatedAfterEqualMidWalkSkipReadsTail() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                skipRows(cursor, 4);
                Assert.assertTrue("the skip of 4 left no rows behind", cursor.hasNext());
                skipRows(cursor, 4);
                cursor.toTop();
                skipRows(cursor, 4);
                assertRemainingRows(cursor, """
                        5|EXCELLENT
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * A skip issued past the first frame of a walk cannot drop the cache: this walk already handed
     * out frame ordinal 0, and its live row cursor still resolves rows against frameCount - 1. So a
     * mid-walk skip that finds frames of an earlier walk ahead of it has to skip row by row, which
     * leaves the numbering alone. Cutting frames instead renumbers from zero underneath the live row
     * cursor, and the skip of 2 below - which that row cursor absorbs whole - then leaves the next
     * hasNext() resolving frame ordinal -1.
     */
    @Test
    public void testSkipRowsMidWalkAfterFullWalkReadsTail() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(13, walk(cursor, null));
                cursor.toTop();
                Assert.assertTrue("the table is not empty, so the first frame must yield a row", cursor.hasNext());
                skipRows(cursor, 2);
                assertRemainingRows(cursor, """
                        4|SCAM
                        5|EXCELLENT
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * The query shape that drives the sequence above: the inner LIMIT issues its skip of 4 from the
     * top of the page frame cursor and the outer one immediately issues a mid-walk skip of 2 through
     * it (LimitRecordCursor.skipRows -> ensureReadyToConsume -> toTop -> base.skipRows, then
     * base.skipRows again). Reading the result twice - which the assertion battery does through
     * toTop() - re-issues the same leading skip over the layout the second skip cut.
     */
    @Test
    public void testNestedLimitRereadsTheSameRows() throws Exception {
        assertQuery("SELECT * FROM (SELECT * FROM bids LIMIT 4, 100) LIMIT 2, 5")
                .ddl(SKIP_WALK_TABLE_DDL, SKIP_WALK_TABLE_DML)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        i\trating\tts
                        7\tGOOD\t2000-01-02T00:00:01.000000Z
                        8\tGOOD\t2000-01-02T00:00:02.000000Z
                        9\tGOOD\t2000-01-02T00:00:03.000000Z
                        """);
    }

    /**
     * The shape a user runs into. A CROSS JOIN sizes its slave, rewinds it, and only then reads it
     * ({@code CrossJoinRecordCursorFactory#getCursor}); the interval scan reports an unknown size, so the
     * slave's LIMIT sizes itself by running {@code PageFrameRecordCursorImpl.skipRows()}. That sizing walk
     * cuts frames at the skip target, the read after the rewind cuts them at the column top, and both
     * number their frames from zero - so the read used to bind the sizing walk's addressless skip frame
     * and report nulls for rows the LIMIT window does hold.
     */
    @Test
    public void testCrossJoinLimitRereadsSkippedSlave() throws Exception {
        assertQuery("""
                SELECT m.x, s.i, s.pad
                FROM long_sequence(2) m
                CROSS JOIN (SELECT i, pad FROM t WHERE ts IN '2024-01-01' LIMIT 10) s
                LIMIT 2,8""")
                .ddl(COLUMN_TOP_TABLE_DDL, COLUMN_TOP_TABLE_DML_BELOW_TOP, COLUMN_TOP_TABLE_ADD_COLUMN, COLUMN_TOP_TABLE_DML_ABOVE_TOP)
                .noRandomAccess()
                .withPlanContaining("Interval forward scan on: t")
                .returns("""
                        x\ti\tpad
                        1\t3\t3
                        1\t4\t4
                        1\t5\t5
                        1\t6\t6
                        1\t7\t7
                        1\t8\t8
                        """);
    }

    /**
     * The same rewind, but with a skip that runs past the column top instead of landing before it: a
     * tail LIMIT skips every row but the last ten, which collapses the frames of the whole leading part
     * of the partition into skeletons. The read after the rewind then needs every one of those ordinals
     * back.
     */
    @Test
    public void testCrossJoinTailLimitRereadsSkippedSlave() throws Exception {
        assertQuery("""
                SELECT m.x, s.i, s.pad
                FROM long_sequence(2) m
                CROSS JOIN (SELECT i, pad FROM t WHERE ts IN '2024-01-01' LIMIT -10) s
                LIMIT 2,8""")
                .ddl(COLUMN_TOP_TABLE_DDL, COLUMN_TOP_TABLE_DML_BELOW_TOP, COLUMN_TOP_TABLE_ADD_COLUMN, COLUMN_TOP_TABLE_DML_ABOVE_TOP)
                .noRandomAccess()
                .withPlanContaining("Interval forward scan on: t")
                .returns("""
                        x\ti\tpad
                        1\t93\t93
                        1\t94\t94
                        1\t95\t95
                        1\t96\t96
                        1\t97\t97
                        1\t98\t98
                        """);
    }

    /**
     * A skip walk whose rows an order-by then reaches again by row id. The sort walks the LIMIT once,
     * keeps a row id per row, and reads the rows back through {@code recordAt()}; a row id names a frame
     * by its ordinal, so dropping the skip walk's frames must not strand the ids the sort is holding.
     * The assertion battery re-reads the result, which drives that sequence twice.
     */
    @Test
    public void testOrderByOverSkippedLimitReadsByRowId() throws Exception {
        assertQuery("SELECT * FROM (SELECT i, pad FROM t WHERE ts IN '2024-01-01' LIMIT 2,12) ORDER BY i DESC")
                .ddl(COLUMN_TOP_TABLE_DDL, COLUMN_TOP_TABLE_DML_BELOW_TOP, COLUMN_TOP_TABLE_ADD_COLUMN, COLUMN_TOP_TABLE_DML_ABOVE_TOP)
                .withPlanContaining("Encode sort light", "Interval forward scan on: t")
                .returns("""
                        i\tpad
                        12\t12
                        11\t11
                        10\t10
                        9\t9
                        8\t8
                        7\t7
                        6\t6
                        5\t5
                        4\t4
                        3\t3
                        """);
    }

    /**
     * The same rewind with a PARQUET frame in the cache. A skip target that covers a whole partition
     * collapses it whatever its format, so the landing frame of the skip walk takes the ordinal the
     * ordinary walk gave the parquet partition - and a parquet ordinal carries a decoder and a row group
     * rather than page addresses, so reusing it reads the wrong partition entirely.
     */
    @Test
    public void testSkipRowsAfterFullWalkReadsTailOverParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            // 2000-01-03 is the active partition, which leaves the middle one convertible.
            execute("ALTER TABLE bids CONVERT PARTITION TO PARQUET LIST '2000-01-02'");
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertEquals(13, walk(cursor, null));
                cursor.toTop();
                skipRows(cursor, 4);
                assertRemainingRows(cursor, """
                        5|EXCELLENT
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * A skip walk, a rewind, then a skip to a DIFFERENT target. The second walk cuts its frames
     * somewhere else than the cached ones are cut, so it must drop them rather than run over them.
     */
    @Test
    public void testSkipRowsAfterRewindWithDifferentTargetReadsTail() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                skipRows(cursor, 4);
                Assert.assertEquals(9, walk(cursor, null));
                cursor.toTop();
                skipRows(cursor, 7);
                assertRemainingRows(cursor, """
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * A skip walk, a rewind, then the SAME skip again - the one case where the cached frames do describe
     * the walk that is about to run, so the cursor keeps them. This is the shape a CROSS JOIN re-scans its
     * slave with, so it is also what stops the fix from re-pricing every frame on each master row.
     */
    @Test
    public void testSkipRowsRepeatedAfterRewindReadsTail() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                skipRows(cursor, 4);
                Assert.assertEquals(9, walk(cursor, null));
                cursor.toTop();
                skipRows(cursor, 4);
                assertRemainingRows(cursor, """
                        5|EXCELLENT
                        6|SCAM
                        7|GOOD
                        8|GOOD
                        9|GOOD
                        10|GOOD
                        11|SCAM
                        12|UNKNOWN
                        13|GOOD
                        """);
            }
        });
    }

    /**
     * The same rewind through {@link BwdTableReaderPageFrameCursor}, whose skip walk collapses frames
     * the way the forward one's does. Driven directly so the skip size is pinned down: the planner turns
     * every ORDER BY ts DESC + LIMIT shape this test could use into a Top K or a sort, so SQL alone
     * cannot reach the backward skip walk. The factory assertion holds the test to a frame scan, and the
     * descending row order - which a forward scan cannot produce - holds it to the backward one.
     */
    @Test
    public void testSkipRowsAfterFullWalkReadsTailDescending() throws Exception {
        assertMemoryLeak(() -> {
            createSkipWalkTable();
            try (
                    RecordCursorFactory factory = engine.select("SELECT * FROM bids ORDER BY ts DESC", sqlExecutionContext);
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(
                        "ORDER BY ts DESC no longer scans page frames, so this test stopped covering the backward cursor",
                        factory.getBaseFactory() instanceof PageFrameRecordCursorFactory
                );
                Assert.assertEquals(13, walk(cursor, null));
                cursor.toTop();
                skipRows(cursor, 4);
                assertRemainingRows(cursor, """
                        9|GOOD
                        8|GOOD
                        7|GOOD
                        6|SCAM
                        5|EXCELLENT
                        4|SCAM
                        3|SCAM
                        2|GOOD
                        1|GOOD
                        """);
            }
        });
    }

    private static void assertRemainingRows(RecordCursor cursor, String expected) {
        final StringSink actual = new StringSink();
        walk(cursor, actual);
        TestUtils.assertEquals(expected, actual);
    }

    private static void skipRows(RecordCursor cursor, long rowCount) {
        final RecordCursor.Counter counter = new RecordCursor.Counter();
        counter.set(rowCount);
        cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
        Assert.assertEquals("skipRows() left rows unskipped", 0, counter.get());
    }

    private static int walk(RecordCursor cursor, @Nullable StringSink sink) {
        final Record record = cursor.getRecord();
        int count = 0;
        while (cursor.hasNext()) {
            count++;
            if (sink != null) {
                sink.put(record.getInt(0)).put('|').put(record.getStrA(1)).put('\n');
            }
        }
        return count;
    }

    private void createSkipWalkTable() throws SqlException {
        execute(SKIP_WALK_TABLE_DDL);
        execute(SKIP_WALK_TABLE_DML);
    }

    private void populateColumnTypes(RecordMetadata metadata, IntList columnIndexes, IntList columnSizes) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            columnIndexes.add(i);
            columnSizes.add(Numbers.msb(ColumnType.sizeOf(metadata.getColumnType(i))));
        }
    }

    private void testBwdPageFrameCursor(int rowCount, int minSize, int maxSize, int startTopAt) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, minSize);
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
                        writer.addColumn("j", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);
                        jIndex = writer.getColumnIndex("j");
                        writer.addColumn("s", ColumnType.STRING, AllowAllSecurityContext.INSTANCE);
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

                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC, null, 0, false)) {
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
                                Assert.assertTrue(len <= maxSize + minSize);

                                long intColAddr = frame.getPageAddress(0);
                                long tsColAddr = frame.getPageAddress(1);
                                long longColAddr = frame.getPageAddress(2);
                                long iStrColAddr = frame.getAuxPageAddress(3);
                                long dStrColAddr = frame.getPageAddress(3);

                                for (long i = len - 1; i > -1; i--) {
                                    Assert.assertEquals(rndInts[rowIndex], Unsafe.getInt(intColAddr + i * 4L));
                                    Assert.assertEquals(ts -= increment, Unsafe.getLong(tsColAddr + i * 8L));

                                    if (startTopAt > 0 && rowIndex >= startTopAt) {
                                        Assert.assertEquals(rndLongs[rowIndex], Unsafe.getLong(longColAddr + i * 8L));
                                        final long strOffset = Unsafe.getLong(iStrColAddr + i * 8);
                                        dcs.of(dStrColAddr + strOffset + 4, dStrColAddr + Unsafe.getLong(iStrColAddr + i * 8 + 8));
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
            try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, order, null, 0, false)) {
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
                RowCursorFactory symbolIndexRowCursorFactory = new SymbolIndexRowCursorFactory(
                        columnIndex,
                        symbolKey,
                        IndexReader.DIR_FORWARD,
                        null
                );
                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tableToken, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC, null, 0, false)) {
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

                // Exercise both scan directions. calculateSize() must agree with the
                // materialized row count regardless of order; the backward interval
                // cursor in particular once stopped after the first partition.
                assertIntervalCalculateSize(engine, tableToken, metadata, timestampType, timestampIndex, intervals, increment, skip, expectedNumOfRows, ORDER_ASC);
                assertIntervalCalculateSize(engine, tableToken, metadata, timestampType, timestampIndex, intervals, increment, skip, expectedNumOfRows, ORDER_DESC);
            }
        });
    }

    private void assertIntervalCalculateSize(
            CairoEngine engine,
            TableToken tableToken,
            GenericRecordMetadata metadata,
            int timestampType,
            int timestampIndex,
            LongList intervals,
            long increment,
            int skip,
            long expectedNumOfRows,
            int order
    ) throws SqlException {
        final RuntimeIntervalModel intervalModel = new RuntimeIntervalModel(
                ColumnType.getTimestampDriver(timestampType),
                PartitionBy.DAY,
                intervals,
                new ObjList<>()
        );
        final RowCursorFactory rowFactory = new PageFrameRowCursorFactory(order);
        try (IntervalPartitionFrameCursorFactory frameFactory = new IntervalPartitionFrameCursorFactory(
                tableToken, TableUtils.ANY_TABLE_VERSION, intervalModel, timestampIndex, metadata, order, null, 0, false
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
                        // Per-row timestamp check only holds for a forward scan with a
                        // closed lower bound; an open lower bound (Long.MIN_VALUE) starts
                        // at the first data row, not at the interval bound.
                        if (order == ORDER_ASC && intervals.getQuick(0) >= 0) {
                            Assert.assertEquals(intervals.getQuick(0) + counter.get() * increment, record.getTimestamp(3));
                        }
                        counter.inc();
                    }
                }

                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.assertEquals(expectedNumOfRows, counter.get());
            }
        }
    }

    private void testFwdPageFrameCursor(int rowCount, int minSize, int maxSize, int startTopAt) throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, minSize);
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
                        writer.addColumn("j", ColumnType.LONG, AllowAllSecurityContext.INSTANCE);
                        jIndex = writer.getColumnIndex("j");
                        writer.addColumn("s", ColumnType.STRING, AllowAllSecurityContext.INSTANCE);
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

                try (FullPartitionFrameCursorFactory frameFactory = new FullPartitionFrameCursorFactory(tt, TableUtils.ANY_TABLE_VERSION, metadata, ORDER_ASC, null, 0, false)) {
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
                                Assert.assertTrue(len <= maxSize + minSize);

                                long intColAddr = frame.getPageAddress(0);
                                long tsColAddr = frame.getPageAddress(1);
                                long longColAddr = frame.getPageAddress(2);
                                long iStrColAddr = frame.getAuxPageAddress(3);
                                long dStrColAddr = frame.getPageAddress(3);

                                for (long i = 0; i < len; i++, rowIndex++) {
                                    Assert.assertEquals(rnd.nextInt(), Unsafe.getInt(intColAddr + i * 4L));
                                    Assert.assertEquals(ts += increment, Unsafe.getLong(tsColAddr + i * 8L));

                                    if (startTopAt > 0 && rowIndex >= startTopAt) {
                                        Assert.assertEquals(rnd.nextLong(), Unsafe.getLong(longColAddr + i * 8L));
                                        final long strOffset = Unsafe.getLong(iStrColAddr + i * 8);
                                        dcs.of(dStrColAddr + strOffset + 4, dStrColAddr + Unsafe.getLong(iStrColAddr + i * 8 + 8));
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
