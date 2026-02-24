/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.table.ConcurrentTimeFrameCursor;
import io.questdb.griffin.engine.table.TablePageFrameCursor;
import io.questdb.mp.WorkerPool;
import io.questdb.std.DirectIntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Rows;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimeFrameCursorTest extends AbstractCairoTest {

    @Test
    public void testEmptyTable() throws Exception {
        testBothCursors(
                "CREATE TABLE x (" +
                        " i INT," +
                        " t TIMESTAMP" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    Assert.assertFalse(cursor.next());
                    Assert.assertFalse(cursor.prev());
                }
        );
    }

    @Test
    public void testNavigateBackward() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(100000000, 100000000) t" +
                            " FROM long_sequence(" + nRows + ")" +
                            ") TIMESTAMP (t) PARTITION BY DAY",
                    cursor -> {
                        TestUtils.printSql(engine, sqlExecutionContext, "x order by t desc", sink);
                        final StringSink actualSink = new StringSink();
                        actualSink.put("a\tb\tt\n");

                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                        }
                        // backward scan
                        while (cursor.prev()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowHi() - 1; row >= frame.getRowLo(); row--) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        TestUtils.assertEquals(sink, actualSink);
                    }
            );
        }
    }

    @Test
    public void testNavigateBothDirections() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(100000000, 100000000) t" +
                            " FROM long_sequence(" + nRows + ")" +
                            ") TIMESTAMP (t) PARTITION BY DAY",
                    cursor -> {
                        TestUtils.printSql(engine, sqlExecutionContext, "x union all (x order by t desc)", sink);
                        final StringSink actualSink = new StringSink();
                        actualSink.put("a\tb\tt\n");

                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        // forward scan
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        // backward scan
                        while (cursor.prev()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowHi() - 1; row >= frame.getRowLo(); row--) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        TestUtils.assertEquals(sink, actualSink);
                    }
            );
        }
    }

    @Test
    public void testNavigateForward() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " rnd_int() a," +
                            " rnd_str() b," +
                            " timestamp_sequence(100000000, 100000000) t" +
                            " FROM long_sequence(" + nRows + ")" +
                            ") TIMESTAMP (t) PARTITION BY DAY",
                    cursor -> {
                        TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
                        final StringSink actualSink = new StringSink();
                        actualSink.put("a\tb\tt\n");

                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        // forward scan
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                                cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        TestUtils.assertEquals(sink, actualSink);
                    }
            );
        }
    }

    @Test
    public void testNavigateForwardNoPartitionBy() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " rnd_long() a," +
                        " rnd_boolean() b," +
                        " timestamp_sequence(0, 100000000) t" +
                        " FROM long_sequence(1000)" +
                        ") TIMESTAMP (t)",
                cursor -> {
                    TestUtils.printSql(engine, sqlExecutionContext, "x", sink);
                    final StringSink actualSink = new StringSink();
                    actualSink.put("a\tb\tt\n");

                    Record record = cursor.getRecord();
                    TimeFrame frame = cursor.getTimeFrame();
                    // forward scan
                    while (cursor.next()) {
                        Assert.assertEquals(-1, frame.getRowLo());
                        Assert.assertEquals(-1, frame.getRowHi());
                        cursor.open();
                        for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                            cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                            actualSink.put(record.getLong(0));
                            actualSink.put('\t');
                            actualSink.put(record.getBool(1));
                            actualSink.put('\t');
                            MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                            actualSink.put('\n');
                        }
                    }
                    TestUtils.assertEquals(sink, actualSink);
                }
        );
    }

    @Test
    public void testNotSupportedForNonPartitionedTable() throws Exception {
        record TestCase(String table, String ddl, String query) {
        }

        final TestCase[] testCases = new TestCase[]{
                new TestCase(
                        "non_partitioned_t",
                        "create table non_partitioned_t as (select x from long_sequence(10))",
                        "non_partitioned_t"
                ),
                new TestCase(
                        "filtered_t",
                        "create table filtered_t (i int, ts timestamp) timestamp(ts) partition by day",
                        "filtered_t where i > 0"
                ),
                new TestCase(
                        "desc_ordered_t",
                        "create table desc_ordered_t (ts timestamp) timestamp(ts) partition by day",
                        "desc_ordered_t order by ts desc"
                )
        };

        for (TestCase tc : testCases) {
            assertMemoryLeak(() -> {
                execute(tc.ddl);
                try (RecordCursorFactory factory = select(tc.query)) {
                    Assert.assertFalse("time frame cursor shouldn't be supported for " + tc.table, factory.supportsTimeFrameCursor());
                }
            });
        }
    }

    @Test
    public void testSeekEstimate() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " timestamp_sequence('2020-01-01', 3_600_000_000) t" +
                        " FROM long_sequence(72)" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Collect frame estimate boundaries
                    long[] estimateHi = new long[3];
                    for (int i = 0; i < 3; i++) {
                        Assert.assertTrue(cursor.next());
                        Assert.assertEquals(i, frame.getFrameIndex());
                        estimateHi[i] = frame.getTimestampEstimateHi();
                    }
                    Assert.assertFalse(cursor.next());

                    // Seek before all frames: positions at -1
                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    // next() after seek should return first frame
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Seek after all frames: positions at last frame
                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(2, frame.getFrameIndex());
                    // next() should return false
                    Assert.assertFalse(cursor.next());

                    // Seek to ceiling of first partition: should find frame 0
                    cursor.seekEstimate(estimateHi[0]);
                    Assert.assertEquals(0, frame.getFrameIndex());
                    // next() should return frame 1
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(1, frame.getFrameIndex());

                    // Seek to just before ceiling of first partition: no frame qualifies
                    cursor.seekEstimate(estimateHi[0] - 1);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    // next() should return frame 0
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Seek to ceiling of second partition: should find frame 1
                    cursor.seekEstimate(estimateHi[1]);
                    Assert.assertEquals(1, frame.getFrameIndex());
                    // next() should return frame 2
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(2, frame.getFrameIndex());
                }
        );
    }

    @Test
    public void testSeekEstimateEmptyTable() throws Exception {
        testBothCursors(
                "CREATE TABLE x (" +
                        " i INT," +
                        " t TIMESTAMP" +
                        ") TIMESTAMP (t) PARTITION BY DAY",
                cursor -> {
                    TimeFrame frame = cursor.getTimeFrame();

                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());

                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());
                }
        );
    }

    @Test
    public void testSeekEstimateNoPartition() throws Exception {
        testBothCursors(
                "CREATE TABLE x AS (SELECT" +
                        " timestamp_sequence(0, 100_000_000) t" +
                        " FROM long_sequence(100)" +
                        ") TIMESTAMP (t)",
                cursor -> {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Single partition with ceiling = Long.MAX_VALUE
                    // Any real timestamp is less than Long.MAX_VALUE, so seek positions at -1
                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Long.MAX_VALUE matches the ceiling, so seek finds frame 0
                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(0, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());
                }
        );
    }

    @Test
    public void testSeekEstimateSplitPartitions() throws Exception {
        executeWithPool((engine, compiler, executionContext) -> {
            // Create a table spanning 3 calendar-day partitions (Feb 3-5), then insert
            // OOO data into the last partition to trigger a partition split.
            execute(
                    compiler,
                    "CREATE TABLE x AS (" +
                            "  SELECT timestamp_sequence('2020-02-03T13', 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(60 * 24 * 2 + 300)" +
                            ") TIMESTAMP (ts) PARTITION BY DAY",
                    executionContext
            );
            execute(
                    compiler,
                    "INSERT INTO x" +
                            "  SELECT timestamp_sequence('2020-02-05T17:01', 60 * 1_000_000L) ts" +
                            "  FROM long_sequence(50)",
                    executionContext
            );

            try (RecordCursorFactory factory = engine.select("x", executionContext)) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(executionContext)) {
                    TimeFrame frame = cursor.getTimeFrame();

                    // Enumerate all frames, collecting their estimate ceilings.
                    LongList ceilings = new LongList();
                    boolean hasSplit = false;
                    while (cursor.next()) {
                        long hi = frame.getTimestampEstimateHi();
                        if (ceilings.size() > 0 && ceilings.getLast() == hi) {
                            hasSplit = true;
                        }
                        ceilings.add(hi);
                    }
                    int frameCount = ceilings.size();
                    Assert.assertTrue("expected at least one split partition", hasSplit);
                    Assert.assertTrue("expected more frames than partitions", frameCount > 3);

                    // Seek before all frames
                    cursor.seekEstimate(0);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());

                    // Seek after all frames
                    cursor.seekEstimate(Long.MAX_VALUE);
                    Assert.assertEquals(frameCount - 1, frame.getFrameIndex());
                    Assert.assertFalse(cursor.next());

                    // For split partitions, multiple frames share the same ceiling.
                    // seekEstimate should return the LAST frame with that ceiling.
                    for (int i = 0; i < frameCount; i++) {
                        // Find the last frame with the same ceiling as frame i
                        int lastWithSameCeiling = i;
                        while (lastWithSameCeiling + 1 < frameCount
                                && ceilings.getQuick(lastWithSameCeiling + 1) == ceilings.getQuick(i)) {
                            lastWithSameCeiling++;
                        }

                        cursor.seekEstimate(ceilings.getQuick(i));
                        Assert.assertEquals(
                                "seekEstimate(" + ceilings.getQuick(i) + ") should find last frame with ceiling",
                                lastWithSameCeiling,
                                frame.getFrameIndex()
                        );

                        // next() should advance to the frame after the split group
                        if (lastWithSameCeiling + 1 < frameCount) {
                            Assert.assertTrue(cursor.next());
                            Assert.assertEquals(lastWithSameCeiling + 1, frame.getFrameIndex());
                        } else {
                            Assert.assertFalse(cursor.next());
                        }
                    }

                    // Seek to just before the first ceiling: no frame qualifies
                    cursor.seekEstimate(ceilings.getQuick(0) - 1);
                    Assert.assertEquals(-1, frame.getFrameIndex());
                    Assert.assertTrue(cursor.next());
                    Assert.assertEquals(0, frame.getFrameIndex());
                }
            }
        });
    }

    @Test
    public void testTimeFrameBoundaries() throws Exception {
        final int[] partitionBys = new int[]{
                PartitionBy.NONE,
                PartitionBy.HOUR,
                PartitionBy.DAY,
                PartitionBy.WEEK,
                PartitionBy.MONTH,
                PartitionBy.YEAR,
        };

        for (int partitionBy : partitionBys) {
            testBothCursors(
                    "CREATE TABLE x AS (SELECT" +
                            " timestamp_sequence(100000000, " + 365 * Micros.DAY_MICROS + ") t" +
                            " FROM long_sequence(3)" +
                            ") TIMESTAMP (t) PARTITION BY " + PartitionBy.toString(partitionBy),
                    cursor -> {
                        Record record = cursor.getRecord();
                        TimeFrame frame = cursor.getTimeFrame();
                        // forward scan
                        while (cursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            cursor.open();
                            cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowLo()));
                            long tsLo = record.getTimestamp(0);

                            TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(ColumnType.TIMESTAMP, partitionBy);
                            TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(ColumnType.TIMESTAMP, partitionBy);

                            long expectedEstimateTsLo = floorMethod != null ? floorMethod.floor(tsLo) : 0;
                            long expectedEstimateTsHi = ceilMethod != null ? ceilMethod.ceil(tsLo) : Long.MAX_VALUE;

                            Assert.assertEquals(expectedEstimateTsLo, frame.getTimestampEstimateLo());
                            Assert.assertEquals(expectedEstimateTsHi, frame.getTimestampEstimateHi());

                            Assert.assertEquals(tsLo, frame.getTimestampLo());
                            cursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowHi() - 1));
                            long tsHi = record.getTimestamp(0);
                            Assert.assertEquals(tsHi + 1, frame.getTimestampHi());
                        }
                    }
            );
        }
    }

    @Test
    public void testTimeFrameBoundariesSplitPartitions() throws Exception {
        executeWithPool((engine, compiler, executionContext) -> {
            // produce split partition
            execute(
                    compiler,
                    "create table x as (" +
                            "  select timestamp_sequence('2020-02-03T13', 60*1000000L) ts " +
                            "  from long_sequence(60*24*2+300)" +
                            ") timestamp (ts) partition by DAY",
                    executionContext
            );
            execute(
                    compiler,
                    "create table z as (" +
                            "  select timestamp_sequence('2020-02-05T17:01', 60*1000000L) ts " +
                            "  from long_sequence(50)" +
                            ")",
                    executionContext
            );
            execute(
                    compiler,
                    "create table y as (select * from x union all select * from z)",
                    executionContext
            );
            execute(
                    compiler,
                    "insert into x select * from z",
                    executionContext
            );

            try (RecordCursorFactory factory = engine.select("x", executionContext)) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameCursor timeFrameCursor = factory.getTimeFrameCursor(executionContext)) {
                    Record record = timeFrameCursor.getRecord();
                    TimeFrame frame = timeFrameCursor.getTimeFrame();
                    long prevSplitTimestampHi = -1;
                    // forward scan
                    while (timeFrameCursor.next()) {
                        Assert.assertEquals(-1, frame.getRowLo());
                        Assert.assertEquals(-1, frame.getRowHi());
                        timeFrameCursor.open();
                        timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowLo()));
                        long tsLo = record.getTimestamp(0);

                        TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(ColumnType.TIMESTAMP, PartitionBy.DAY);
                        TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(ColumnType.TIMESTAMP, PartitionBy.DAY);

                        long expectedEstimateTsLo = floorMethod != null ? floorMethod.floor(tsLo) : 0;
                        long expectedEstimateTsHi = ceilMethod != null ? ceilMethod.ceil(tsLo) : Long.MAX_VALUE;

                        Assert.assertTrue(frame.getTimestampEstimateLo() >= expectedEstimateTsLo);
                        if (prevSplitTimestampHi != -1) {
                            // this must be split partition
                            Assert.assertEquals(prevSplitTimestampHi, frame.getTimestampEstimateLo());
                        }
                        Assert.assertTrue(frame.getTimestampEstimateHi() <= expectedEstimateTsHi);
                        if (frame.getTimestampEstimateHi() < expectedEstimateTsHi) {
                            // this must be split partition
                            prevSplitTimestampHi = frame.getTimestampEstimateHi();
                        } else {
                            prevSplitTimestampHi = -1;
                        }

                        Assert.assertEquals(tsLo, frame.getTimestampLo());
                        timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowHi() - 1));
                        long tsHi = record.getTimestamp(0);
                        Assert.assertEquals(tsHi + 1, frame.getTimestampHi());
                    }
                }
            }
        });
    }

    private static void executeWithPool(CustomisableRunnable runnable) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CairoConfiguration configuration = new DefaultTestCairoConfiguration(root) {
                @Override
                public long getPartitionO3SplitMinSize() {
                    return 1000;
                }
            };
            WorkerPool pool = new WorkerPool(() -> 2);
            TestUtils.execute(pool, runnable, configuration, LOG);
        });
    }

    private void testBothCursors(String ddl, TimeFrameCursorAssertion assertion) throws Exception {
        assertMemoryLeak(() -> {
            execute(ddl);
            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                // Test TimeFrameCursorImpl
                try (TimeFrameCursor cursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    assertion.test(cursor);
                }
                // Test ConcurrentTimeFrameCursorImpl
                testWithConcurrentCursor(factory, assertion);
            }
            execute("DROP TABLE x");
        });
    }

    private void testWithConcurrentCursor(RecordCursorFactory factory, TimeFrameCursorAssertion assertion) throws Exception {
        // Unwrap QueryProgress to get the base factory with direct access to TablePageFrameCursor
        RecordCursorFactory baseFactory = factory instanceof QueryProgress ? factory.getBaseFactory() : factory;
        ConcurrentTimeFrameCursor cursor = baseFactory.newTimeFrameCursor();
        Assert.assertNotNull(cursor);
        PageFrameAddressCache addressCache = new PageFrameAddressCache();
        DirectIntList partitionIndexes = new DirectIntList(64, MemoryTag.NATIVE_DEFAULT, true);
        try {
            TablePageFrameCursor pageFrameCursor = (TablePageFrameCursor) baseFactory.getPageFrameCursor(
                    sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC
            );
            RecordMetadata metadata = baseFactory.getMetadata();
            addressCache.of(metadata, pageFrameCursor.getColumnIndexes(), pageFrameCursor.isExternal());

            LongList rowCounts = new LongList();
            LongList partitionTimestamps = new LongList();
            LongList partitionCeilings = new LongList();

            int frameCount = 0;
            PageFrame frame;
            while ((frame = pageFrameCursor.next()) != null) {
                partitionIndexes.add(frame.getPartitionIndex());
                rowCounts.add(frame.getPartitionHi() - frame.getPartitionLo());
                addressCache.add(frameCount++, frame);
            }

            ConcurrentTimeFrameCursor.populatePartitionTimestamps(pageFrameCursor, partitionTimestamps, partitionCeilings);

            cursor.of(
                    pageFrameCursor,
                    addressCache,
                    partitionIndexes,
                    rowCounts,
                    partitionTimestamps,
                    partitionCeilings,
                    frameCount,
                    metadata.getTimestampIndex()
            );

            assertion.test(cursor);
        } finally {
            Misc.free(cursor);
            Misc.free(partitionIndexes);
            Misc.free(addressCache);
        }
    }

    @FunctionalInterface
    private interface TimeFrameCursorAssertion {
        void test(TimeFrameCursor cursor) throws Exception;
    }
}
