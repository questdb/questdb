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

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IntervalBwdPartitionFrameCursor;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.PartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.DirectIntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.cutlass.text.SqlExecutionContextStub;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;

@RunWith(Parameterized.class)
public class IntervalBwdPartitionFrameCursorTest extends AbstractCairoTest {
    private final boolean convertToParquet;
    private final LongList intervals = new LongList();
    private final TestTimestampType timestampType;

    public IntervalBwdPartitionFrameCursorTest(boolean convertToParquet, TestTimestampType timestampType) {
        this.convertToParquet = convertToParquet;
        this.timestampType = timestampType;
    }

    @Parameterized.Parameters(name = "parquet={0},ts={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {true, TestTimestampType.MICRO},
                {true, TestTimestampType.NANO},
                {false, TestTimestampType.MICRO},
                {false, TestTimestampType.NANO},
        });
    }

    @Test
    public void testAllIntervalsAfterTableByDay() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        // single interval spanning all the table
        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-01T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:01.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-08T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.DAY, increment, N, "", 0);
    }

    @Test
    public void testAllIntervalsAfterTableByNone() throws Exception {
        Assume.assumeFalse(convertToParquet);

        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-01T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:01.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-08T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.NONE, increment, N, "", 0);
    }

    @Test
    public void testAllIntervalsBeforeTableByDay() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        // single interval spanning all of the table
        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-01T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:01.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-08T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.DAY, increment, N, "", 0);
    }

    @Test
    public void testAllIntervalsBeforeTableByNone() throws Exception {
        Assume.assumeFalse(convertToParquet);

        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        // single interval spanning all the table
        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-01T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T00:00:01.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-06T14:00:01.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-08T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1979-01-09T00:00:00.000Z"));

        testIntervals(PartitionBy.NONE, increment, N, "", 0);
    }

    @Test
    public void testByNone() throws Exception {
        Assume.assumeFalse(convertToParquet);

        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        intervals.clear();
        // exact date match
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T18:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T20:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:30:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:35:00.000Z"));

        intervals.add(timestampType.getDriver().parseFloorLiteral("1983-01-05T12:30:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1983-01-05T14:35:00.000Z"));

        final String expected = replaceTimestampSuffix1("1983-01-05T14:00:00.000000Z\n" +
                "1980-01-02T20:00:00.000000Z\n" +
                "1980-01-02T18:00:00.000000Z\n", timestampType.getTypeName());

        testIntervals(PartitionBy.NONE, increment, N, expected, 3);
    }

    @Test
    public void testClose() throws Exception {
        Assume.assumeFalse(convertToParquet);
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.NONE).
                    col("a", ColumnType.INT).
                    col("b", ColumnType.INT).
                    timestamp();
            AbstractCairoTest.create(model);

            TableReader reader = newOffPoolReader(configuration, "x");
            IntervalBwdPartitionFrameCursor cursor = new IntervalBwdPartitionFrameCursor(
                    new RuntimeIntervalModel(
                            ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()),
                            reader.getPartitionedBy(),
                            intervals
                    ),
                    reader.getMetadata().getTimestampIndex()
            );
            cursor.of(reader, null);
            cursor.close();
            Assert.assertFalse(reader.isOpen());
            cursor.close();
            Assert.assertFalse(reader.isOpen());
        });
    }

    @Test
    public void testExactMatch() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        intervals.clear();
        // exact date match
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:00:00.000Z"));
        // this one falls through cracks
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:30:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:35:00.000Z"));

        final String expected = replaceTimestampSuffix1("1980-01-02T22:00:00.000000Z\n", timestampType.getTypeName());

        testIntervals(PartitionBy.DAY, increment, N, expected, 1);
    }

    @Test
    public void testFallsBelow() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        intervals.clear();
        // exact date match
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T18:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T20:00:00.000Z"));

        // interval falls below active partition
        // the previous interval must not be on the edge of partition
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:30:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:35:00.000Z"));

        intervals.add(timestampType.getDriver().parseFloorLiteral("1983-01-05T12:30:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1983-01-05T14:35:00.000Z"));

        final String expected = replaceTimestampSuffix1("1983-01-05T14:00:00.000000Z\n" +
                "1980-01-02T20:00:00.000000Z\n" +
                "1980-01-02T18:00:00.000000Z\n", timestampType.getTypeName());

        testIntervals(PartitionBy.DAY, increment, N, expected, 3);
    }

    @Test
    public void testIntervalCursorNoTimestamp() throws Exception {
        Assume.assumeFalse(convertToParquet && ColumnType.isTimestampNano(timestampType.getTimestampType()));
        TestUtils.assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.SYMBOL).indexed(true, 4).
                    col("b", ColumnType.SYMBOL).indexed(true, 4);
            AbstractCairoTest.create(model);
        });
    }

    @Test
    public void testNegativeReloadByDay() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T01:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T16:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T21:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-03T11:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-03T14:00:00.000Z"));

        final String expected = replaceTimestampSuffix1("1980-01-03T14:00:00.000000Z\n" +
                "1980-01-03T12:00:00.000000Z\n" +
                "1980-01-02T22:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T02:00:00.000000Z\n", timestampType.getTypeName());

        testReload(increment, intervals, N, expected, null);
    }

    @Test
    public void testPartitionCull() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        // single interval spanning all the table
        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T01:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T16:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T21:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-03T11:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-03T14:00:00.000Z"));

        final String expected = replaceTimestampSuffix1("1980-01-03T14:00:00.000000Z\n" +
                "1980-01-03T12:00:00.000000Z\n" +
                "1980-01-02T22:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T02:00:00.000000Z\n", timestampType.getTypeName());

        testIntervals(PartitionBy.DAY, increment, N, expected, 11);
    }

    @Test
    public void testPositiveReloadByDay() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T01:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T16:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T21:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-02T22:00:00.000Z"));
        //
        intervals.add(timestampType.getDriver().parseFloorLiteral("1983-01-05T11:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1983-01-05T14:00:00.000Z"));

        final String expected1 = replaceTimestampSuffix1("1980-01-02T22:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T02:00:00.000000Z\n", timestampType.getTypeName());

        final String expected2 = replaceTimestampSuffix1("1983-01-05T14:00:00.000000Z\n" +
                "1983-01-05T12:00:00.000000Z\n", timestampType.getTypeName()) + expected1;

        testReload(PartitionBy.DAY, increment, intervals, N, expected1, expected2);
    }

    public void testReload(
            int partitionBy,
            long increment,
            LongList intervals,
            int rowCount,
            CharSequence expected1,
            CharSequence expected2
    ) throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken;
            TableModel model = new TableModel(configuration, "x", partitionBy).
                    col("a", ColumnType.SYMBOL).indexed(true, 4).
                    col("b", ColumnType.SYMBOL).indexed(true, 4).
                    timestamp(timestampType.getTimestampType());
            tableToken = AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();
            long timestamp = timestampType.getDriver().parseFloorLiteral("1980-01-01T00:00:00.000Z");

            GenericRecordMetadata metadata;
            final int timestampIndex;

            final SqlExecutionContext executionContext = new SqlExecutionContextStub(engine);

            try (TableReader reader = engine.getReader(tableToken)) {
                timestampIndex = reader.getMetadata().getTimestampIndex();
                metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
            }
            final TestTableReaderRecord record = new TestTableReaderRecord();
            try (
                    final IntervalPartitionFrameCursorFactory factory = new IntervalPartitionFrameCursorFactory(
                            tableToken,
                            0,
                            new RuntimeIntervalModel(
                                    ColumnType.getTimestampDriver(metadata.getTimestampType()),
                                    partitionBy,
                                    intervals
                            ),
                            timestampIndex,
                            metadata,
                            ORDER_DESC
                    );
                    final PartitionFrameCursor cursor = factory.getCursor(executionContext, ORDER_DESC)
            ) {
                // assert that there is nothing to start with
                record.of(cursor.getTableReader());

                assertEqualTimestamps("", record, cursor);

                try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                    for (int i = 0; i < rowCount; i++) {
                        TableWriter.Row row = writer.newRow(timestamp);
                        row.putSym(0, rnd.nextChars(4));
                        row.putSym(1, rnd.nextChars(4));
                        row.append();
                        timestamp += increment;
                    }
                    writer.commit();

                    Assert.assertTrue(cursor.reload());
                    assertEqualTimestamps(expected1, record, cursor);

                    timestamp = timestampType.getDriver().addYears(timestamp, 3);

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
                        assertEqualTimestamps(expected2, record, cursor);
                    } else {
                        assertEqualTimestamps(expected1, record, cursor);
                    }

                    Assert.assertFalse(cursor.reload());
                }

                if (convertToParquet) {
                    execute("alter table x convert partition to parquet where timestamp >= 0;");
                }

                try (TableWriter writer = engine.getWriter(tableToken, "testing")) {
                    writer.removeColumn("b");
                }

                try {
                    factory.getCursor(executionContext, ORDER_DESC);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }
        });
    }

    @Test
    public void testSingleIntervalWholeTable() throws Exception {
        // day partition
        // two-hour interval between timestamps
        long increment = timestampType.getDriver().fromHours(2);
        // 3 days
        int N = 36;

        // single interval spanning all the table
        intervals.clear();
        intervals.add(timestampType.getDriver().parseFloorLiteral("1980-01-01T00:00:00.000Z"));
        intervals.add(timestampType.getDriver().parseFloorLiteral("1984-01-06T00:00:00.000Z"));

        final String expected = replaceTimestampSuffix1("1983-01-06T22:00:00.000000Z\n" +
                "1983-01-06T20:00:00.000000Z\n" +
                "1983-01-06T18:00:00.000000Z\n" +
                "1983-01-06T16:00:00.000000Z\n" +
                "1983-01-06T14:00:00.000000Z\n" +
                "1983-01-06T12:00:00.000000Z\n" +
                "1983-01-06T10:00:00.000000Z\n" +
                "1983-01-06T08:00:00.000000Z\n" +
                "1983-01-06T06:00:00.000000Z\n" +
                "1983-01-06T04:00:00.000000Z\n" +
                "1983-01-06T02:00:00.000000Z\n" +
                "1983-01-06T00:00:00.000000Z\n" +
                "1983-01-05T22:00:00.000000Z\n" +
                "1983-01-05T20:00:00.000000Z\n" +
                "1983-01-05T18:00:00.000000Z\n" +
                "1983-01-05T16:00:00.000000Z\n" +
                "1983-01-05T14:00:00.000000Z\n" +
                "1983-01-05T12:00:00.000000Z\n" +
                "1983-01-05T10:00:00.000000Z\n" +
                "1983-01-05T08:00:00.000000Z\n" +
                "1983-01-05T06:00:00.000000Z\n" +
                "1983-01-05T04:00:00.000000Z\n" +
                "1983-01-05T02:00:00.000000Z\n" +
                "1983-01-05T00:00:00.000000Z\n" +
                "1983-01-04T22:00:00.000000Z\n" +
                "1983-01-04T20:00:00.000000Z\n" +
                "1983-01-04T18:00:00.000000Z\n" +
                "1983-01-04T16:00:00.000000Z\n" +
                "1983-01-04T14:00:00.000000Z\n" +
                "1983-01-04T12:00:00.000000Z\n" +
                "1983-01-04T10:00:00.000000Z\n" +
                "1983-01-04T08:00:00.000000Z\n" +
                "1983-01-04T06:00:00.000000Z\n" +
                "1983-01-04T04:00:00.000000Z\n" +
                "1983-01-04T02:00:00.000000Z\n" +
                "1983-01-04T00:00:00.000000Z\n" +
                "1980-01-03T22:00:00.000000Z\n" +
                "1980-01-03T20:00:00.000000Z\n" +
                "1980-01-03T18:00:00.000000Z\n" +
                "1980-01-03T16:00:00.000000Z\n" +
                "1980-01-03T14:00:00.000000Z\n" +
                "1980-01-03T12:00:00.000000Z\n" +
                "1980-01-03T10:00:00.000000Z\n" +
                "1980-01-03T08:00:00.000000Z\n" +
                "1980-01-03T06:00:00.000000Z\n" +
                "1980-01-03T04:00:00.000000Z\n" +
                "1980-01-03T02:00:00.000000Z\n" +
                "1980-01-03T00:00:00.000000Z\n" +
                "1980-01-02T22:00:00.000000Z\n" +
                "1980-01-02T20:00:00.000000Z\n" +
                "1980-01-02T18:00:00.000000Z\n" +
                "1980-01-02T16:00:00.000000Z\n" +
                "1980-01-02T14:00:00.000000Z\n" +
                "1980-01-02T12:00:00.000000Z\n" +
                "1980-01-02T10:00:00.000000Z\n" +
                "1980-01-02T08:00:00.000000Z\n" +
                "1980-01-02T06:00:00.000000Z\n" +
                "1980-01-02T04:00:00.000000Z\n" +
                "1980-01-02T02:00:00.000000Z\n" +
                "1980-01-02T00:00:00.000000Z\n" +
                "1980-01-01T22:00:00.000000Z\n" +
                "1980-01-01T20:00:00.000000Z\n" +
                "1980-01-01T18:00:00.000000Z\n" +
                "1980-01-01T16:00:00.000000Z\n" +
                "1980-01-01T14:00:00.000000Z\n" +
                "1980-01-01T12:00:00.000000Z\n" +
                "1980-01-01T10:00:00.000000Z\n" +
                "1980-01-01T08:00:00.000000Z\n" +
                "1980-01-01T06:00:00.000000Z\n" +
                "1980-01-01T04:00:00.000000Z\n" +
                "1980-01-01T02:00:00.000000Z\n" +
                "1980-01-01T00:00:00.000000Z\n", timestampType.getTypeName());

        testIntervals(PartitionBy.DAY, increment, N, expected, 72);
    }

    private static void assertIndexRowsMatchSymbol(PartitionFrameCursor cursor, TestTableReaderRecord record, int columnIndex, long expectedCount) {
        // SymbolTable is a table at table scope, so it will be the same for every
        // partition frame here. Get its instance outside of partition frame loop.
        StaticSymbolTable symbolTable = record.getReader().getSymbolTable(columnIndex);

        boolean allNative = true;
        long rowCount = 0;
        PartitionFrame frame;
        while ((frame = cursor.next()) != null) {
            // TODO(puzpuzpuz): port the subsequent checks to parquet
            if (frame.getPartitionFormat() != PartitionFormat.NATIVE) {
                allNative = false;
                continue;
            }

            record.jumpTo(frame.getPartitionIndex(), frame.getRowLo());
            final long limit = frame.getRowHi();
            final long low = frame.getRowLo();

            // BitmapIndex is always at partition frame scope, each table can have more than one.
            // we have to get BitmapIndexReader instance once for each frame.
            BitmapIndexReader indexReader = record.getReader().getBitmapIndexReader(frame.getPartitionIndex(), columnIndex, BitmapIndexReader.DIR_BACKWARD);

            // because out Symbol column 0 is indexed, frame has to have index.
            Assert.assertNotNull(indexReader);

            int keyCount = indexReader.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                RowCursor ic = indexReader.getCursor(true, i, low, limit - 1);
                CharSequence expected = symbolTable.valueOf((int) (i - low - 1));
                while (ic.hasNext()) {
                    long row = ic.next();
                    record.setRecordIndex(row);
                    TestUtils.assertEquals(expected, record.getSymA(columnIndex));
                    rowCount++;
                }
            }
        }
        if (allNative) {
            Assert.assertEquals(expectedCount, rowCount);
        }
    }

    private void assertEqualTimestamps(CharSequence expected, TestTableReaderRecord record, PartitionFrameCursor cursor) {
        sink.clear();
        collectTimestamps(cursor, record);
        TestUtils.assertEquals(expected, sink);
    }

    private void collectTimestamps(PartitionFrameCursor cursor, TestTableReaderRecord record) {
        try (
                RowGroupBuffers parquetBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                DirectIntList parquetColumns = new DirectIntList(2, MemoryTag.NATIVE_DEFAULT)
        ) {
            int timestampIndex = cursor.getTableReader().getMetadata().getTimestampIndex();
            parquetColumns.add(timestampIndex);
            parquetColumns.add(timestampType.getTimestampType());

            PartitionFrame frame;
            while ((frame = cursor.next()) != null) {
                if (frame.getPartitionFormat() == PartitionFormat.NATIVE) {
                    record.jumpTo(frame.getPartitionIndex(), frame.getRowHi() - 1);
                    long limit = frame.getRowLo() - 1;
                    long recordIndex;
                    while ((recordIndex = record.getRecordIndex()) > limit) {
                        sink.putISODate(timestampType.getDriver(), record.getDate(timestampIndex)).put('\n');
                        record.setRecordIndex(recordIndex - 1);
                    }
                    continue;
                }

                Assert.assertEquals(PartitionFormat.PARQUET, frame.getPartitionFormat());
                PartitionDecoder parquetDecoder = frame.getParquetDecoder();
                Assert.assertNotNull(parquetDecoder);
                PartitionDecoder.Metadata parquetMetadata = parquetDecoder.metadata();
                for (int i = 0, n = parquetMetadata.rowGroupCount(); i < n; i++) {
                    int size = parquetMetadata.rowGroupSize(i);
                    parquetDecoder.decodeRowGroup(parquetBuffers, parquetColumns, i, 0, size);
                    long addr = parquetBuffers.getChunkDataPtr(0);
                    for (long r = frame.getRowHi() - 1; r > frame.getRowLo() - 1; r--) {
                        sink.putISODate(timestampType.getDriver(), Unsafe.getUnsafe().getLong(addr + r * Long.BYTES)).put('\n');
                    }
                }
            }
        }
    }

    private void testIntervals(int partitionBy, long increment, int rowCount, CharSequence expected, long expectedCount) throws Exception {
        assertMemoryLeak(() -> {
            TableModel model = new TableModel(configuration, "x", partitionBy)
                    .col("a", ColumnType.SYMBOL).indexed(true, 4)
                    .col("b", ColumnType.SYMBOL).indexed(true, 4)
                    .timestamp(timestampType.getTimestampType());
            AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();
            long timestamp = timestampType.getDriver().parseFloorLiteral("1980-01-01T00:00:00.000Z");
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putSym(0, rnd.nextChars(4));
                    row.putSym(1, rnd.nextChars(4));
                    row.append();
                    timestamp += increment;
                }
                writer.commit();

                timestamp = timestampType.getDriver().addYears(timestamp, 3);

                for (int i = 0; i < rowCount; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putSym(0, rnd.nextChars(4));
                    row.putSym(1, rnd.nextChars(4));
                    row.append();
                    timestamp += increment;
                }
                writer.commit();
            }

            if (convertToParquet) {
                execute("alter table x convert partition to parquet where timestamp >= 0;");
            }

            try (
                    TableReader reader = newOffPoolReader(configuration, "x");
                    IntervalBwdPartitionFrameCursor cursor = new IntervalBwdPartitionFrameCursor(
                            new RuntimeIntervalModel(
                                    ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()),
                                    reader.getPartitionedBy(),
                                    intervals
                            ),
                            reader.getMetadata().getTimestampIndex()
                    )
            ) {
                final TestTableReaderRecord record = new TestTableReaderRecord();
                cursor.of(reader, null);
                record.of(reader);

                assertEqualTimestamps(expected, record, cursor);

                if (expected.length() > 0) {
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 0, expectedCount);
                    cursor.toTop();
                    assertIndexRowsMatchSymbol(cursor, record, 1, expectedCount);
                }

                cursor.toTop();
                assertEqualTimestamps(expected, record, cursor);
            }
        });
    }

    private void testReload(
            long increment,
            LongList intervals,
            int rowCount,
            CharSequence expected1,
            CharSequence expected2
    ) throws Exception {
        assertMemoryLeak(() -> {
            TableToken tableToken;
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY).
                    col("a", ColumnType.SYMBOL).indexed(true, 4).
                    col("b", ColumnType.SYMBOL).indexed(true, 4).
                    timestamp(timestampType.getTimestampType());
            tableToken = AbstractCairoTest.create(model);

            final Rnd rnd = new Rnd();
            long timestamp = timestampType.getDriver().parseFloorLiteral("1980-01-01T00:00:00.000Z");

            GenericRecordMetadata metadata;
            final int timestampIndex;

            final SqlExecutionContext executionContext = new SqlExecutionContextStub(engine);

            try (TableReader reader = engine.getReader(tableToken)) {
                timestampIndex = reader.getMetadata().getTimestampIndex();
                metadata = GenericRecordMetadata.copyOf(reader.getMetadata());
            }
            final TestTableReaderRecord record = new TestTableReaderRecord();
            try (
                    final IntervalPartitionFrameCursorFactory factory = new IntervalPartitionFrameCursorFactory(
                            tableToken,
                            0,
                            new RuntimeIntervalModel(
                                    ColumnType.getTimestampDriver(metadata.getTimestampType()),
                                    PartitionBy.DAY,
                                    intervals
                            ),
                            timestampIndex,
                            metadata,
                            ORDER_DESC
                    );
                    final PartitionFrameCursor cursor = factory.getCursor(executionContext, ORDER_DESC)
            ) {
                // assert that there is nothing to start with
                record.of(cursor.getTableReader());

                assertEqualTimestamps("", record, cursor);

                try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                    for (int i = 0; i < rowCount; i++) {
                        TableWriter.Row row = writer.newRow(timestamp);
                        row.putSym(0, rnd.nextChars(4));
                        row.putSym(1, rnd.nextChars(4));
                        row.append();
                        timestamp += increment;
                    }
                    writer.commit();

                    Assert.assertTrue(cursor.reload());
                    assertEqualTimestamps(expected1, record, cursor);

                    timestamp = timestampType.getDriver().addYears(timestamp, 3);

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
                        assertEqualTimestamps(expected2, record, cursor);
                    } else {
                        assertEqualTimestamps(expected1, record, cursor);
                    }

                    Assert.assertFalse(cursor.reload());
                }

                if (convertToParquet) {
                    execute("alter table x convert partition to parquet where timestamp >= 0;");
                }

                try (TableWriter writer = engine.getWriter(tableToken, "testing")) {
                    writer.removeColumn("b");
                }

                try {
                    factory.getCursor(executionContext, ORDER_DESC);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }
        });
    }
}
