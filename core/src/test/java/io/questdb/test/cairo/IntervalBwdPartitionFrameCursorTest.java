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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoConfigurationWrapper;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IntervalBwdPartitionFrameCursor;
import io.questdb.cairo.IntervalPartitionFrameCursorFactory;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.cairo.ParquetMetaFileReader;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.griffin.model.RuntimeIntervalModel;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
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

    @Override
    public void setUp() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        setProperty(PropertyKey.CAIRO_DEFAULT_SYMBOL_INDEX_TYPE, TestUtils.randomSymbolIndexTypeName(rnd));
        super.setUp();
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
    public void testBoundedIntervalSearch() throws Exception {
        assertMemoryLeak(() -> {
            final int rowCount = 12;
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, rowCount);
            AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.DAY)
                    .timestamp(timestampType.getTimestampType()));
            final long start = timestampType.getDriver().parseFloorLiteral("2020-01-01T00:00");
            final long hour = timestampType.getDriver().fromHours(1);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int i = 0; i < rowCount; i++) {
                    writer.newRow(start + (i / 2) * hour).append();
                }
                writer.newRow(start + timestampType.getDriver().fromDays(1)).append();
                writer.commit();
            }
            if (convertToParquet) {
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            }
            intervals.add(start + hour, start + hour);
            intervals.add(start + 3 * hour, start + 3 * hour);
            intervals.add(start + 4 * hour, start + 4 * hour);

            final IntList decodedRowHi = new IntList();
            final CairoConfiguration trackingConfiguration = new CairoConfigurationWrapper(configuration) {
                @Override
                public ParquetPartitionDecoder newParquetPartitionDecoder() {
                    return new ParquetPartitionDecoder() {
                        @Override
                        public int decodeRowGroup(RowGroupBuffers buffers, DirectIntList columns, int rowGroup, int rowLo, int rowHi) {
                            decodedRowHi.add(rowHi);
                            return super.decodeRowGroup(buffers, columns, rowGroup, rowLo, rowHi);
                        }
                    };
                }
            };
            try (
                    TableReader reader = newOffPoolReader(configuration, "x");
                    IntervalBwdPartitionFrameCursor cursor = new IntervalBwdPartitionFrameCursor(
                            trackingConfiguration,
                            new RuntimeIntervalModel(timestampType.getDriver(), reader.getPartitionedBy(), intervals),
                            reader.getMetadata().getTimestampIndex()
                    )
            ) {
                cursor.of(reader, null);
                for (int pass = 0; pass < 2; pass++) {
                    assertFrameRows(cursor.next(), 8, 10);
                    decodedRowHi.clear();
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(counter);
                    Assert.assertEquals(4, counter.get());
                    assertDecodeBound(decodedRowHi, 8);

                    // Calculating the remaining size must not advance the iteration's bound.
                    decodedRowHi.clear();
                    assertFrameRows(cursor.next(), 6, 8);
                    assertDecodeBound(decodedRowHi, 8);
                    decodedRowHi.clear();
                    assertFrameRows(cursor.next(), 2, 4);
                    Assert.assertNull(cursor.next());
                    assertDecodeBound(decodedRowHi, 6);
                    cursor.toTop();
                }
            }
        });
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

        final String expected = replaceTimestampSuffix1("""
                1983-01-05T14:00:00.000000Z
                1980-01-02T20:00:00.000000Z
                1980-01-02T18:00:00.000000Z
                """, timestampType.getTypeName());

        testIntervals(PartitionBy.NONE, increment, N, expected, 3);
    }

    @Test
    public void testCalculateSizeUnboundedLowIntervalAcrossPartitions() throws Exception {
        // Regression for an off-by-one (and early-exit) in calculateSize() on an unbounded-low
        // interval ("ts < X", intervalLo == Long.MIN_VALUE). The frame size is hi - lo with lo the
        // exclusive lower index, which must be -1 when the interval reaches below row 0; a previous
        // ": 0" dropped row 0 and -- because that also routes to "skip interval" instead of "skip
        // partition" -- stopped after the first matching partition, so calculateSize() diverged from
        // what next() iterates. Three day-partitions with the cut inside the middle one exercise both
        // the partially-cut partition and a wholly-included lower one; with convertToParquet the cut
        // lands inside a parquet partition.
        assertMemoryLeak(() -> {
            final long increment = timestampType.getDriver().fromHours(1);
            TableModel model = new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("a", ColumnType.INT)
                    .timestamp(timestampType.getTimestampType());
            AbstractCairoTest.create(model);

            long timestamp = timestampType.getDriver().parseFloorLiteral("2024-01-01T00:00:00.000Z");
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                // 72 hourly rows across three day-partitions (2024-01-01..03).
                for (int i = 0; i < 72; i++) {
                    TableWriter.Row row = writer.newRow(timestamp);
                    row.putInt(0, i);
                    row.append();
                    timestamp += increment;
                }
                writer.commit();
            }
            if (convertToParquet) {
                // Converts the two non-active partitions; 2024-01-03 stays native, so the cut below
                // lands inside parquet 2024-01-02.
                execute("alter table x convert partition to parquet where timestamp >= 0;");
            }

            // "ts < 2024-01-02T05:00:00" -> [Long.MIN_VALUE, 2024-01-02T04:00:00] (edges inclusive).
            // Matches all 24 rows of 2024-01-01 plus 00:00..04:00 of 2024-01-02 = 29 rows.
            intervals.clear();
            intervals.add(Long.MIN_VALUE);
            intervals.add(timestampType.getDriver().parseFloorLiteral("2024-01-02T04:00:00.000Z"));

            try (
                    TableReader reader = newOffPoolReader(configuration, "x");
                    IntervalBwdPartitionFrameCursor cursor = new IntervalBwdPartitionFrameCursor(
                            configuration,
                            new RuntimeIntervalModel(
                                    ColumnType.getTimestampDriver(reader.getMetadata().getTimestampType()),
                                    reader.getPartitionedBy(),
                                    intervals
                            ),
                            reader.getMetadata().getTimestampIndex()
                    )
            ) {
                cursor.of(reader, null);

                // Ground truth: what next() actually iterates (the path the bug did not touch).
                long iterated = 0;
                PartitionFrame frame;
                while ((frame = cursor.next()) != null) {
                    iterated += frame.getRowHi() - frame.getRowLo();
                }
                Assert.assertEquals(29L, iterated);

                // calculateSize() must agree with the iteration. The pre-fix ": 0" returned 4 here
                // (off by one on the cut partition, then stopping before the wholly-included one).
                cursor.toTop();
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(counter);
                Assert.assertEquals(iterated, counter.get());
            }
        });
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
                    configuration,
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

        final String expected = replaceTimestampSuffix1("""
                1983-01-05T14:00:00.000000Z
                1980-01-02T20:00:00.000000Z
                1980-01-02T18:00:00.000000Z
                """, timestampType.getTypeName());

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
    public void testIntervalSpansPartitions() throws Exception {
        assertMemoryLeak(() -> {
            AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.DAY)
                    .col("v", ColumnType.INT)
                    .timestamp(timestampType.getTimestampType()));
            execute("""
                    INSERT INTO x VALUES
                    (1, '2020-01-01T21:00'),
                    (2, '2020-01-02T10:00'),
                    (3, '2020-01-02T11:00'),
                    (4, '2020-01-03T00:00')
                    """);
            if (convertToParquet) {
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE timestamp >= 0");
            }
            assertQuery("SELECT v FROM x WHERE timestamp IN '2020-01-01T20:00;9h' ORDER BY timestamp DESC")
                    .noLeakCheck()
                    .withPlanContaining("Interval backward scan")
                    .returns("v\n1\n");
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

        final String expected = replaceTimestampSuffix1("""
                1980-01-03T14:00:00.000000Z
                1980-01-03T12:00:00.000000Z
                1980-01-02T22:00:00.000000Z
                1980-01-02T16:00:00.000000Z
                1980-01-02T14:00:00.000000Z
                1980-01-02T12:00:00.000000Z
                1980-01-02T10:00:00.000000Z
                1980-01-02T08:00:00.000000Z
                1980-01-02T06:00:00.000000Z
                1980-01-02T04:00:00.000000Z
                1980-01-02T02:00:00.000000Z
                """, timestampType.getTypeName());

        testReload(increment, intervals, N, expected);
    }

    @Test
    public void testOverlappingIntervals() throws Exception {
        // Each row must stay in one frame when the interval list is unsorted or overlaps. On
        // 2020-01-01 two unsorted intervals share 02:00, which three rows hold across a row group
        // boundary. On 2020-01-02 the earlier interval ends inside the row group that starts at the
        // later interval's first row.
        assertMemoryLeak(() -> {
            node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 3);
            AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.DAY)
                    .timestamp(timestampType.getTimestampType()));
            final long start = timestampType.getDriver().parseFloorLiteral("2020-01-01T00:00");
            final long hour = timestampType.getDriver().fromHours(1);
            final long day = timestampType.getDriver().fromDays(1);
            try (TableWriter writer = newOffPoolWriter(configuration, "x")) {
                for (int h : new int[]{0, 1, 2, 2, 2, 3, 4, 5}) {
                    writer.newRow(start + h * hour).append();
                }
                for (int h = 0; h < 6; h++) {
                    writer.newRow(start + day + h * hour).append();
                }
                writer.newRow(start + 2 * day).append();
                writer.commit();
            }
            if (convertToParquet) {
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE timestamp >= 0");
            }
            intervals.add(start + 2 * hour, start + 4 * hour + hour / 2);
            intervals.add(start + hour, start + 2 * hour);
            intervals.add(start + day + hour, start + day + 4 * hour + hour / 2);
            intervals.add(start + day + 3 * hour, start + day + 6 * hour);

            try (
                    TableReader reader = newOffPoolReader(configuration, "x");
                    IntervalBwdPartitionFrameCursor cursor = new IntervalBwdPartitionFrameCursor(
                            configuration,
                            new RuntimeIntervalModel(timestampType.getDriver(), reader.getPartitionedBy(), intervals),
                            reader.getMetadata().getTimestampIndex()
                    )
            ) {
                cursor.of(reader, null);
                for (int pass = 0; pass < 2; pass++) {
                    // 2020-01-02 01:00..05:00 and 2020-01-01 01:00..04:00, each row once
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(counter);
                    Assert.assertEquals(11, counter.get());
                    assertFrameRows(cursor.next(), 1, 6);
                    assertFrameRows(cursor.next(), 1, 7);
                    Assert.assertNull(cursor.next());
                    cursor.toTop();
                }
            }
        });
    }

    @Test
    public void testOverlappingTickIntervals() throws Exception {
        // The spring-forward gap makes the '...@Europe/London;50m;1h;4' intervals overlap in UTC.
        // Each row must appear once in both scan directions and in count().
        assertMemoryLeak(() -> {
            AbstractCairoTest.create(new TableModel(configuration, "x", PartitionBy.DAY)
                    .timestamp(timestampType.getTimestampType()));
            execute("""
                    INSERT INTO x VALUES
                    ('2024-03-31T00:40'),
                    ('2024-03-31T01:10'),
                    ('2024-03-31T01:35'),
                    ('2024-03-31T01:40'),
                    ('2024-04-01T12:00')
                    """);
            if (convertToParquet) {
                execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE timestamp >= 0");
            }
            final String where = " FROM x WHERE timestamp IN '2024-03-31T00:30@Europe/London;50m;1h;4'";
            assertQuery("SELECT timestamp" + where)
                    .noLeakCheck()
                    .timestamp("timestamp")
                    .returns(replaceTimestampSuffix1("""
                            timestamp
                            2024-03-31T00:40:00.000000Z
                            2024-03-31T01:10:00.000000Z
                            2024-03-31T01:35:00.000000Z
                            2024-03-31T01:40:00.000000Z
                            """, timestampType.getTypeName()));
            assertQuery("SELECT timestamp" + where + " ORDER BY timestamp DESC")
                    .noLeakCheck()
                    .timestampDesc("timestamp")
                    .returns(replaceTimestampSuffix1("""
                            timestamp
                            2024-03-31T01:40:00.000000Z
                            2024-03-31T01:35:00.000000Z
                            2024-03-31T01:10:00.000000Z
                            2024-03-31T00:40:00.000000Z
                            """, timestampType.getTypeName()));
            assertQuery("SELECT count()" + where)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("count\n4\n");
        });
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

        final String expected = replaceTimestampSuffix1("""
                1980-01-03T14:00:00.000000Z
                1980-01-03T12:00:00.000000Z
                1980-01-02T22:00:00.000000Z
                1980-01-02T16:00:00.000000Z
                1980-01-02T14:00:00.000000Z
                1980-01-02T12:00:00.000000Z
                1980-01-02T10:00:00.000000Z
                1980-01-02T08:00:00.000000Z
                1980-01-02T06:00:00.000000Z
                1980-01-02T04:00:00.000000Z
                1980-01-02T02:00:00.000000Z
                """, timestampType.getTypeName());

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

        final String expected1 = replaceTimestampSuffix1("""
                1980-01-02T22:00:00.000000Z
                1980-01-02T16:00:00.000000Z
                1980-01-02T14:00:00.000000Z
                1980-01-02T12:00:00.000000Z
                1980-01-02T10:00:00.000000Z
                1980-01-02T08:00:00.000000Z
                1980-01-02T06:00:00.000000Z
                1980-01-02T04:00:00.000000Z
                1980-01-02T02:00:00.000000Z
                """, timestampType.getTypeName());

        final String expected2 = replaceTimestampSuffix1("""
                1983-01-05T14:00:00.000000Z
                1983-01-05T12:00:00.000000Z
                """, timestampType.getTypeName()) + expected1;

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
            IntList columnIndexes = new IntList();
            columnIndexes.add(0); // a
            columnIndexes.add(1); // b
            columnIndexes.add(2); // timestamp
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
                            ORDER_DESC,
                            null,
                            0,
                            false
                    );
                    final PartitionFrameCursor cursor = factory.getCursor(executionContext, columnIndexes, ORDER_DESC)
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
                    factory.getCursor(executionContext, new IntList(), ORDER_DESC);
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

        final String expected = replaceTimestampSuffix1("""
                1983-01-06T22:00:00.000000Z
                1983-01-06T20:00:00.000000Z
                1983-01-06T18:00:00.000000Z
                1983-01-06T16:00:00.000000Z
                1983-01-06T14:00:00.000000Z
                1983-01-06T12:00:00.000000Z
                1983-01-06T10:00:00.000000Z
                1983-01-06T08:00:00.000000Z
                1983-01-06T06:00:00.000000Z
                1983-01-06T04:00:00.000000Z
                1983-01-06T02:00:00.000000Z
                1983-01-06T00:00:00.000000Z
                1983-01-05T22:00:00.000000Z
                1983-01-05T20:00:00.000000Z
                1983-01-05T18:00:00.000000Z
                1983-01-05T16:00:00.000000Z
                1983-01-05T14:00:00.000000Z
                1983-01-05T12:00:00.000000Z
                1983-01-05T10:00:00.000000Z
                1983-01-05T08:00:00.000000Z
                1983-01-05T06:00:00.000000Z
                1983-01-05T04:00:00.000000Z
                1983-01-05T02:00:00.000000Z
                1983-01-05T00:00:00.000000Z
                1983-01-04T22:00:00.000000Z
                1983-01-04T20:00:00.000000Z
                1983-01-04T18:00:00.000000Z
                1983-01-04T16:00:00.000000Z
                1983-01-04T14:00:00.000000Z
                1983-01-04T12:00:00.000000Z
                1983-01-04T10:00:00.000000Z
                1983-01-04T08:00:00.000000Z
                1983-01-04T06:00:00.000000Z
                1983-01-04T04:00:00.000000Z
                1983-01-04T02:00:00.000000Z
                1983-01-04T00:00:00.000000Z
                1980-01-03T22:00:00.000000Z
                1980-01-03T20:00:00.000000Z
                1980-01-03T18:00:00.000000Z
                1980-01-03T16:00:00.000000Z
                1980-01-03T14:00:00.000000Z
                1980-01-03T12:00:00.000000Z
                1980-01-03T10:00:00.000000Z
                1980-01-03T08:00:00.000000Z
                1980-01-03T06:00:00.000000Z
                1980-01-03T04:00:00.000000Z
                1980-01-03T02:00:00.000000Z
                1980-01-03T00:00:00.000000Z
                1980-01-02T22:00:00.000000Z
                1980-01-02T20:00:00.000000Z
                1980-01-02T18:00:00.000000Z
                1980-01-02T16:00:00.000000Z
                1980-01-02T14:00:00.000000Z
                1980-01-02T12:00:00.000000Z
                1980-01-02T10:00:00.000000Z
                1980-01-02T08:00:00.000000Z
                1980-01-02T06:00:00.000000Z
                1980-01-02T04:00:00.000000Z
                1980-01-02T02:00:00.000000Z
                1980-01-02T00:00:00.000000Z
                1980-01-01T22:00:00.000000Z
                1980-01-01T20:00:00.000000Z
                1980-01-01T18:00:00.000000Z
                1980-01-01T16:00:00.000000Z
                1980-01-01T14:00:00.000000Z
                1980-01-01T12:00:00.000000Z
                1980-01-01T10:00:00.000000Z
                1980-01-01T08:00:00.000000Z
                1980-01-01T06:00:00.000000Z
                1980-01-01T04:00:00.000000Z
                1980-01-01T02:00:00.000000Z
                1980-01-01T00:00:00.000000Z
                """, timestampType.getTypeName());

        testIntervals(PartitionBy.DAY, increment, N, expected, 72);
    }

    private static void assertFrameRows(PartitionFrame frame, long lo, long hi) {
        Assert.assertNotNull(frame);
        Assert.assertEquals(lo, frame.getRowLo());
        Assert.assertEquals(hi, frame.getRowHi());
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
            IndexReader indexReader = record.getReader().getIndexReader(frame.getPartitionIndex(), columnIndex, IndexReader.DIR_BACKWARD);

            // because out Symbol column 0 is indexed, frame has to have index.
            Assert.assertNotNull(indexReader);

            int keyCount = indexReader.getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                RowCursor ic = indexReader.getCursor(i, low, limit - 1);
                CharSequence expected = symbolTable.valueOf((int) (i - low - 1));
                while (ic.hasNext()) {
                    long row = ic.next();
                    record.setRecordIndex(row);
                    TestUtils.assertEquals(expected, record.getSymA(columnIndex));
                    rowCount++;
                }
                Misc.free(ic);
            }
        }
        if (allNative) {
            Assert.assertEquals(expectedCount, rowCount);
        }
    }

    private void assertDecodeBound(IntList decodedRowHi, int rowHi) {
        if (convertToParquet) {
            Assert.assertTrue("the interval search must decode timestamps", decodedRowHi.size() > 0);
            for (int i = 0; i < decodedRowHi.size(); i++) {
                Assert.assertTrue("decoded rows from a previously searched interval", decodedRowHi.getQuick(i) <= rowHi);
            }
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
                ParquetPartitionDecoder parquetDecoder = frame.getParquetMetaDecoder();
                Assert.assertNotNull(parquetDecoder);
                ParquetMetaFileReader parquetMetadata = parquetDecoder.metadata();
                for (int i = 0, n = parquetMetadata.getRowGroupCount(); i < n; i++) {
                    int size = (int) parquetMetadata.getRowGroupSize(i);
                    parquetDecoder.decodeRowGroup(parquetBuffers, parquetColumns, i, 0, size);
                    long addr = parquetBuffers.getChunkDataPtr(0);
                    for (long r = frame.getRowHi() - 1; r > frame.getRowLo() - 1; r--) {
                        sink.putISODate(timestampType.getDriver(), Unsafe.getLong(addr + r * Long.BYTES)).put('\n');
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
                            configuration,
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

                if (!expected.isEmpty()) {
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
            CharSequence expected1
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
            IntList columnIndexes = new IntList();
            columnIndexes.add(0); // a
            columnIndexes.add(1); // b
            columnIndexes.add(2); // timestamp
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
                            ORDER_DESC,
                            null,
                            0,
                            false
                    );
                    final PartitionFrameCursor cursor = factory.getCursor(executionContext, columnIndexes, ORDER_DESC)
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
                    assertEqualTimestamps(expected1, record, cursor);

                    Assert.assertFalse(cursor.reload());
                }

                if (convertToParquet) {
                    execute("alter table x convert partition to parquet where timestamp >= 0;");
                }

                try (TableWriter writer = engine.getWriter(tableToken, "testing")) {
                    writer.removeColumn("b");
                }

                try {
                    factory.getCursor(executionContext, new IntList(), ORDER_DESC);
                    Assert.fail();
                } catch (TableReferenceOutOfDateException ignored) {
                }
            }
        });
    }
}
