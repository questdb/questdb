/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class IntervalFwdDataFrameCursorTest extends AbstractCairoTest {
    private static final LongList intervals = new LongList();

    @Test
    public void testAllIntervalsAfterTableByDay() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-01T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:01.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-08T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.DAY, increment, N, "", 0);
    }

    @Test
    public void testAllIntervalsAfterTableByNone() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-01T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:01.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-08T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.NONE, increment, N, "", 0);
    }

    @Test
    public void testAllIntervalsBeforeTableByDay() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-01T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:01.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-08T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.DAY, increment, N, "", 0);
    }

    @Test
    public void testAllIntervalsBeforeTableByNone() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-01T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T00:00:01.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-08T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.NONE, increment, N, "", 0);
    }

    @Test
    public void testByNone() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        intervals.clear();
        // exact date match
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T18:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T20:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:30:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:35:00.000Z"));

        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-05T12:30:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-05T14:35:00.000Z"));

        final String expected = "1980-01-02T18:00:00.000000Z\n" +
                "1980-01-02T20:00:00.000000Z\n" +
                "1983-01-05T14:00:00.000000Z\n";

        testIntervals(PartitionBy.NONE, increment, N, expected, 3);
    }

    @Test
    public void testClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).
                    col("a", ColumnType.INT).
                    col("b", ColumnType.INT).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            TableReader reader = new TableReader(configuration, "x");
            IntervalFwdDataFrameCursor cursor = new IntervalFwdDataFrameCursor(new RuntimeIntervalModel(intervals), reader.getMetadata().getTimestampIndex());
            cursor.of(reader, null);
            cursor.close();
            Assert.assertFalse(reader.isOpen());
            cursor.close();
            Assert.assertFalse(reader.isOpen());
        });
    }

    @Test
    public void testDuplicateTimestamps() throws Exception {
        int N = 10;

        intervals.clear();
        // Assume testIntervals will insert rows 3 years after 1980-01-01,
        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-01T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-02T00:00:00.000Z"));

        sink.clear();
        sink.repeat( "1983-01-01T00:00:00.000000Z\n", N);

        testIntervals(PartitionBy.NONE, 0, N, sink, N);
    }

    @Test
    public void testExactMatch() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        intervals.clear();
        // exact date match
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:00:00.000Z"));
        // this one falls through cracks
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:30:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:35:00.000Z"));

        final String expected = "1980-01-02T22:00:00.000000Z\n";

        testIntervals(PartitionBy.DAY, increment, N, expected, 1);
    }

    @Test
    public void testFallsBelow() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        intervals.clear();
        // exact date match
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T18:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T20:00:00.000Z"));

        // interval falls below active partition
        // previous interval must not be on the edge of partition
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:30:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:35:00.000Z"));

        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-05T12:30:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-05T14:35:00.000Z"));

        final String expected = "1980-01-02T18:00:00.000000Z\n" +
                "1980-01-02T20:00:00.000000Z\n" +
                "1983-01-05T14:00:00.000000Z\n";

        testIntervals(PartitionBy.DAY, increment, N, expected, 3);
    }

    @Test
    public void testIntervalCursorNoTimestamp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.SYMBOL).indexed(true, 4).
                    col("b", ColumnType.SYMBOL).indexed(true, 4)
            ) {
                CairoTestUtils.create(model);
            }
        });
    }

    @Test
    public void testIntervalSpansPartitions() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-03T23:59:59.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-04T15:00:00.002Z"));

        final String expected = "1983-01-04T00:00:00.000000Z\n" +
                "1983-01-04T02:00:00.000000Z\n" +
                "1983-01-04T04:00:00.000000Z\n" +
                "1983-01-04T06:00:00.000000Z\n" +
                "1983-01-04T08:00:00.000000Z\n" +
                "1983-01-04T10:00:00.000000Z\n" +
                "1983-01-04T12:00:00.000000Z\n" +
                "1983-01-04T14:00:00.000000Z\n";

        testIntervals(PartitionBy.DAY, increment, N, expected, 8);
    }

    @Test
    public void testNegativeReloadByDay() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T01:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T16:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T21:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-03T11:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-03T14:00:00.000Z"));

        final String expected1 = "1980-01-02T02:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T22:00:00.000000Z\n" +
                "1980-01-03T12:00:00.000000Z\n" +
                "1980-01-03T14:00:00.000000Z\n";

        testReload(PartitionBy.DAY, increment, intervals, N, expected1, null);
    }

    @Test
    public void testPartitionCull() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T01:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T16:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T21:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-03T11:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-03T14:00:00.000Z"));

        final String expected = "1980-01-02T02:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T22:00:00.000000Z\n" +
                "1980-01-03T12:00:00.000000Z\n" +
                "1980-01-03T14:00:00.000000Z\n";

        testIntervals(PartitionBy.DAY, increment, N, expected, 11);
    }

    @Test
    public void testPositiveReloadByDay() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T01:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T16:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T21:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-02T22:00:00.000Z"));
        //
        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-05T11:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1983-01-05T14:00:00.000Z"));

        final String expected1 = "1980-01-02T02:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T22:00:00.000000Z\n";

        final String expected2 = expected1 +
                "1983-01-05T12:00:00.000000Z\n" +
                "1983-01-05T14:00:00.000000Z\n";

        testReload(PartitionBy.DAY, increment, intervals, N, expected1, expected2);
    }

    public void testReload(int partitionBy, long increment, LongList intervals, int rowCount, CharSequence expected1, CharSequence expected2) throws Exception {
        assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.SYMBOL).indexed(true, 4).
                    col("b", ColumnType.SYMBOL).indexed(true, 4).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            long timestamp = TimestampFormatUtils.parseTimestamp("1980-01-01T00:00:00.000Z");

            final int timestampIndex;
            try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, "x")) {
                timestampIndex = reader.getMetadata().getTimestampIndex();
            }
            final TableReaderRecord record = new TableReaderRecord();
            final IntervalFwdDataFrameCursorFactory factory = new IntervalFwdDataFrameCursorFactory(engine, "x", -1, 0, new RuntimeIntervalModel(intervals), timestampIndex);
            try (DataFrameCursor cursor = factory.getCursor(AllowAllSqlSecurityContext.INSTANCE)) {

                // assert that there is nothing to start with
                record.of(cursor.getTableReader());

                assertEquals("", record, cursor);

                try (TableWriter writer = new TableWriter(configuration, "x")) {
                    for (int i = 0; i < rowCount; i++) {
                        TableWriter.Row row = writer.newRow(timestamp);
                        row.putSym(0, rnd.nextChars(4));
                        row.putSym(1, rnd.nextChars(4));
                        row.append();
                        timestamp += increment;
                    }
                    writer.commit();

                    Assert.assertTrue(cursor.reload());
                    assertEquals(expected1, record, cursor);

                    timestamp = Timestamps.addYear(timestamp, 3);

                    for (int i = 0; i < rowCount; i++) {
                        TableWriter.Row row = writer.newRow(timestamp);
                        row.putSym(0, rnd.nextChars(4));
                        row.putSym(1, rnd.nextChars(4));
                        row.append();
                        timestamp += increment;
                    }
                    writer.commit();

                    Assert.assertTrue(cursor.reload());
                    if (expected2 != null) {
                        assertEquals(expected2, record, cursor);
                    } else {
                        assertEquals(expected1, record, cursor);
                    }

                    Assert.assertFalse(cursor.reload());
                }
            }

            try (TableWriter writer = engine.getWriter(AllowAllCairoSecurityContext.INSTANCE, "x", "testing")) {
                writer.removeColumn("a");
            }

            try {
                factory.getCursor(AllowAllSqlSecurityContext.INSTANCE);
                Assert.fail();
            } catch (ReaderOutOfDateException ignored) {
            }
        });
    }

    @Test
    public void testSingleIntervalWholeTable() throws Exception {
        // day partition
        // two hour interval between timestamps
        long increment = 1000000L * 3600 * 2;
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(TimestampFormatUtils.parseTimestamp("1980-01-01T00:00:00.000Z"));
        intervals.add(TimestampFormatUtils.parseTimestamp("1984-01-06T00:00:00.000Z"));

        final String expected = "1980-01-01T00:00:00.000000Z\n" +
                "1980-01-01T02:00:00.000000Z\n" +
                "1980-01-01T04:00:00.000000Z\n" +
                "1980-01-01T06:00:00.000000Z\n" +
                "1980-01-01T08:00:00.000000Z\n" +
                "1980-01-01T10:00:00.000000Z\n" +
                "1980-01-01T12:00:00.000000Z\n" +
                "1980-01-01T14:00:00.000000Z\n" +
                "1980-01-01T16:00:00.000000Z\n" +
                "1980-01-01T18:00:00.000000Z\n" +
                "1980-01-01T20:00:00.000000Z\n" +
                "1980-01-01T22:00:00.000000Z\n" +
                "1980-01-02T00:00:00.000000Z\n" +
                "1980-01-02T02:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T18:00:00.000000Z\n" +
                "1980-01-02T20:00:00.000000Z\n" +
                "1980-01-02T22:00:00.000000Z\n" +
                "1980-01-03T00:00:00.000000Z\n" +
                "1980-01-03T02:00:00.000000Z\n" +
                "1980-01-03T04:00:00.000000Z\n" +
                "1980-01-03T06:00:00.000000Z\n" +
                "1980-01-03T08:00:00.000000Z\n" +
                "1980-01-03T10:00:00.000000Z\n" +
                "1980-01-03T12:00:00.000000Z\n" +
                "1980-01-03T14:00:00.000000Z\n" +
                "1980-01-03T16:00:00.000000Z\n" +
                "1980-01-03T18:00:00.000000Z\n" +
                "1980-01-03T20:00:00.000000Z\n" +
                "1980-01-03T22:00:00.000000Z\n" +
                "1983-01-04T00:00:00.000000Z\n" +
                "1983-01-04T02:00:00.000000Z\n" +
                "1983-01-04T04:00:00.000000Z\n" +
                "1983-01-04T06:00:00.000000Z\n" +
                "1983-01-04T08:00:00.000000Z\n" +
                "1983-01-04T10:00:00.000000Z\n" +
                "1983-01-04T12:00:00.000000Z\n" +
                "1983-01-04T14:00:00.000000Z\n" +
                "1983-01-04T16:00:00.000000Z\n" +
                "1983-01-04T18:00:00.000000Z\n" +
                "1983-01-04T20:00:00.000000Z\n" +
                "1983-01-04T22:00:00.000000Z\n" +
                "1983-01-05T00:00:00.000000Z\n" +
                "1983-01-05T02:00:00.000000Z\n" +
                "1983-01-05T04:00:00.000000Z\n" +
                "1983-01-05T06:00:00.000000Z\n" +
                "1983-01-05T08:00:00.000000Z\n" +
                "1983-01-05T10:00:00.000000Z\n" +
                "1983-01-05T12:00:00.000000Z\n" +
                "1983-01-05T14:00:00.000000Z\n" +
                "1983-01-05T16:00:00.000000Z\n" +
                "1983-01-05T18:00:00.000000Z\n" +
                "1983-01-05T20:00:00.000000Z\n" +
                "1983-01-05T22:00:00.000000Z\n" +
                "1983-01-06T00:00:00.000000Z\n" +
                "1983-01-06T02:00:00.000000Z\n" +
                "1983-01-06T04:00:00.000000Z\n" +
                "1983-01-06T06:00:00.000000Z\n" +
                "1983-01-06T08:00:00.000000Z\n" +
                "1983-01-06T10:00:00.000000Z\n" +
                "1983-01-06T12:00:00.000000Z\n" +
                "1983-01-06T14:00:00.000000Z\n" +
                "1983-01-06T16:00:00.000000Z\n" +
                "1983-01-06T18:00:00.000000Z\n" +
                "1983-01-06T20:00:00.000000Z\n" +
                "1983-01-06T22:00:00.000000Z\n";

        testIntervals(PartitionBy.DAY, increment, N, expected, 72);
    }

    private static void assertIndexRowsMatchSymbol(DataFrameCursor cursor, TableReaderRecord record, int columnIndex, long expectedCount) {
        // SymbolTable is table at table scope, so it will be the same for every
        // data frame here. Get its instance outside of data frame loop.
        StaticSymbolTable symbolTable = cursor.getSymbolTable(columnIndex);

        long rowCount = 0;
        DataFrame frame;
        while ((frame = cursor.next()) != null) {
            record.jumpTo(frame.getPartitionIndex(), frame.getRowLo());
            final long limit = frame.getRowHi();
            final long low = frame.getRowLo();

            // BitmapIndex is always at data frame scope, each table can have more than one.
            // we have to get BitmapIndexReader instance once for each frame.
            BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);

            // because out Symbol column 0 is indexed, frame has to have index.
            Assert.assertNotNull(indexReader);

            int keyCount = indexReader.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                RowCursor ic = indexReader.getCursor(true, i, low, limit - 1);
                CharSequence expected = symbolTable.valueOf(i - 1);
                while (ic.hasNext()) {
                    long row = ic.next();
                    record.setRecordIndex(row);
                    TestUtils.assertEquals(expected, record.getSym(columnIndex));
                    rowCount++;
                }
            }
        }
        Assert.assertEquals(expectedCount, rowCount);
    }

    private void assertEquals(CharSequence expected, TableReaderRecord record, DataFrameCursor cursor) {
        sink.clear();
        collectTimestamps(cursor, record);
        TestUtils.assertEquals(expected, sink);
    }

    private void collectTimestamps(DataFrameCursor cursor, TableReaderRecord record) {
        int timestampIndex = cursor.getTableReader().getMetadata().getTimestampIndex();
        DataFrame frame;
        while ((frame = cursor.next()) != null) {
            record.jumpTo(frame.getPartitionIndex(), frame.getRowLo());
            long limit = frame.getRowHi();
            long recordIndex;
            while ((recordIndex = record.getRecordIndex()) < limit) {
                sink.putISODate(record.getDate(timestampIndex)).put('\n');
                record.setRecordIndex(recordIndex + 1);
            }
        }
    }

    private void testIntervals(int partitionBy, long increment, int rowCount, CharSequence expected, long expectedCount) throws Exception {
        TestUtils.assertMemoryLeak(() -> {

            try (TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.SYMBOL).indexed(true, 4).
                    col("b", ColumnType.SYMBOL).indexed(true, 4).
                    timestamp()
            ) {
                CairoTestUtils.create(model);
            }

            final Rnd rnd = new Rnd();
            long timestamp = TimestampFormatUtils.parseTimestamp("1980-01-01T00:00:00.000Z");
            try (TableWriter writer = new TableWriter(configuration, "x")) {
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putSym(0, rnd.nextChars(4));
                    row.putSym(1, rnd.nextChars(4));
                    row.append();
                    timestamp += increment;
                }
                writer.commit();

                timestamp = Timestamps.addYear(timestamp, 3);

                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putSym(0, rnd.nextChars(4));
                    row.putSym(1, rnd.nextChars(4));
                    row.append();
                    timestamp += increment;
                }
                writer.commit();

            }

            try (TableReader reader = new TableReader(configuration, "x")) {
                final TableReaderRecord record = new TableReaderRecord();
                IntervalFwdDataFrameCursor cursor = new IntervalFwdDataFrameCursor(
                        new RuntimeIntervalModel(IntervalFwdDataFrameCursorTest.intervals),
                        reader.getMetadata().getTimestampIndex());
                cursor.of(reader, null);
                record.of(reader);

                assertEquals(expected, record, cursor);

                if (expected.length() > 0) {
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 0, expectedCount);
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 1, expectedCount);
                }

                cursor.toTop();
                assertEquals(expected, record, cursor);
            }
        });
    }

}
