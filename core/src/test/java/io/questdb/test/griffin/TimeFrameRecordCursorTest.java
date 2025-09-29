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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameRecordCursor;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Rows;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.DefaultTestCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TimeFrameRecordCursorTest extends AbstractCairoTest {

    @Test
    public void testEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x (" +
                            " i int," +
                            " t timestamp" +
                            ") timestamp (t) partition by DAY"
            );

            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameRecordCursor timeFrameCursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    Assert.assertFalse(timeFrameCursor.next());
                    Assert.assertFalse(timeFrameCursor.prev());
                }
            }
        });
    }

    @Test
    public void testNavigateBackward() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            assertMemoryLeak(() -> {
                execute(
                        "create table x as (select" +
                                " rnd_int() a," +
                                " rnd_str() b," +
                                " timestamp_sequence(100000000, 100000000) t" +
                                " from long_sequence(" + nRows + ")" +
                                ") timestamp (t) partition by DAY"
                );

                TestUtils.printSql(
                        engine,
                        sqlExecutionContext,
                        "x order by t desc",
                        sink
                );

                final StringSink actualSink = new StringSink();
                // header
                actualSink.put("a\tb\tt\n");

                try (RecordCursorFactory factory = select("x")) {
                    Assert.assertTrue(factory.supportsTimeFrameCursor());
                    try (TimeFrameRecordCursor timeFrameCursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                        Record record = timeFrameCursor.getRecord();
                        TimeFrame frame = timeFrameCursor.getTimeFrame();
                        while (timeFrameCursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                        }
                        // backward scan
                        while (timeFrameCursor.prev()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            timeFrameCursor.open();
                            for (long row = frame.getRowHi() - 1; row >= frame.getRowLo(); row--) {
                                timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
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
                }

                execute("drop table x");
            });
        }
    }

    @Test
    public void testNavigateBothDirections() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            assertMemoryLeak(() -> {
                execute(
                        "create table x as (select" +
                                " rnd_int() a," +
                                " rnd_str() b," +
                                " timestamp_sequence(100000000, 100000000) t" +
                                " from long_sequence(" + nRows + ")" +
                                ") timestamp (t) partition by DAY"
                );

                TestUtils.printSql(
                        engine,
                        sqlExecutionContext,
                        "x union all (x order by t desc)",
                        sink
                );

                final StringSink actualSink = new StringSink();
                // header
                actualSink.put("a\tb\tt\n");

                try (RecordCursorFactory factory = select("x")) {
                    Assert.assertTrue(factory.supportsTimeFrameCursor());
                    try (TimeFrameRecordCursor timeFrameCursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                        Record record = timeFrameCursor.getRecord();
                        TimeFrame frame = timeFrameCursor.getTimeFrame();
                        // forward scan
                        while (timeFrameCursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            timeFrameCursor.open();
                            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                                timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
                                actualSink.put(record.getInt(0));
                                actualSink.put('\t');
                                actualSink.put(record.getStrA(1));
                                actualSink.put('\t');
                                MicrosFormatUtils.appendDateTimeUSec(actualSink, record.getTimestamp(2));
                                actualSink.put('\n');
                            }
                        }
                        // backward scan
                        while (timeFrameCursor.prev()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            timeFrameCursor.open();
                            for (long row = frame.getRowHi() - 1; row >= frame.getRowLo(); row--) {
                                timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
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
                }

                execute("drop table x");
            });
        }
    }

    @Test
    public void testNavigateForward() throws Exception {
        final int nRowsBase = 500;
        final int N = 10;
        for (int i = 0; i < N; i++) {
            final int nRows = nRowsBase * (i + 1);
            assertMemoryLeak(() -> {
                execute(
                        "create table x as (select" +
                                " rnd_int() a," +
                                " rnd_str() b," +
                                " timestamp_sequence(100000000, 100000000) t" +
                                " from long_sequence(" + nRows + ")" +
                                ") timestamp (t) partition by DAY"
                );

                TestUtils.printSql(
                        engine,
                        sqlExecutionContext,
                        "x",
                        sink
                );

                final StringSink actualSink = new StringSink();
                // header
                actualSink.put("a\tb\tt\n");

                try (RecordCursorFactory factory = select("x")) {
                    Assert.assertTrue(factory.supportsTimeFrameCursor());
                    try (TimeFrameRecordCursor timeFrameCursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                        Record record = timeFrameCursor.getRecord();
                        TimeFrame frame = timeFrameCursor.getTimeFrame();
                        // forward scan
                        while (timeFrameCursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            timeFrameCursor.open();
                            for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                                timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
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
                }

                execute("drop table x");
            });
        }
    }

    @Test
    public void testNavigateForwardNoPartitionBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (select" +
                            " rnd_long() a," +
                            " rnd_boolean() b," +
                            " timestamp_sequence(0, 100000000) t" +
                            " from long_sequence(1000)" +
                            ") timestamp (t)"
            );

            TestUtils.printSql(
                    engine,
                    sqlExecutionContext,
                    "x",
                    sink
            );

            final StringSink actualSink = new StringSink();
            // header
            actualSink.put("a\tb\tt\n");

            try (RecordCursorFactory factory = select("x")) {
                Assert.assertTrue(factory.supportsTimeFrameCursor());
                try (TimeFrameRecordCursor timeFrameCursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                    Record record = timeFrameCursor.getRecord();
                    TimeFrame frame = timeFrameCursor.getTimeFrame();
                    // forward scan
                    while (timeFrameCursor.next()) {
                        Assert.assertEquals(-1, frame.getRowLo());
                        Assert.assertEquals(-1, frame.getRowHi());
                        timeFrameCursor.open();
                        for (long row = frame.getRowLo(); row < frame.getRowHi(); row++) {
                            timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), row));
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
            }

            execute("drop table x");
        });
    }

    @Test
    public void testNotSupportedForNonPartitionedTable() throws Exception {
        class TestCase {
            final String ddl;
            final String query;
            final String table;

            TestCase(String table, String ddl, String query) {
                this.table = table;
                this.ddl = ddl;
                this.query = query;
            }
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
                        "time_filtered_t",
                        "create table time_filtered_t (ts timestamp) timestamp(ts) partition by day",
                        "time_filtered_t where ts in '1970-01-13' or ts = '1970-05-14T16:00:02.000000Z'"
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
            assertMemoryLeak(() -> {
                execute(
                        "create table x as (select" +
                                " timestamp_sequence(100000000, " + 365 * Micros.DAY_MICROS + ") t" +
                                " from long_sequence(3)" +
                                ") timestamp (t) partition by " + PartitionBy.toString(partitionBy)
                );

                try (RecordCursorFactory factory = select("x")) {
                    Assert.assertTrue(factory.supportsTimeFrameCursor());
                    try (TimeFrameRecordCursor timeFrameCursor = factory.getTimeFrameCursor(sqlExecutionContext)) {
                        Record record = timeFrameCursor.getRecord();
                        TimeFrame frame = timeFrameCursor.getTimeFrame();
                        // forward scan
                        while (timeFrameCursor.next()) {
                            Assert.assertEquals(-1, frame.getRowLo());
                            Assert.assertEquals(-1, frame.getRowHi());
                            timeFrameCursor.open();
                            timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowLo()));
                            long tsLo = record.getTimestamp(0);

                            TimestampDriver.TimestampFloorMethod floorMethod = PartitionBy.getPartitionFloorMethod(ColumnType.TIMESTAMP, partitionBy);
                            TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(ColumnType.TIMESTAMP, partitionBy);

                            long expectedEstimateTsLo = floorMethod != null ? floorMethod.floor(tsLo) : 0;
                            long expectedEstimateTsHi = ceilMethod != null ? ceilMethod.ceil(tsLo) : Long.MAX_VALUE;

                            Assert.assertEquals(expectedEstimateTsLo, frame.getTimestampEstimateLo());
                            Assert.assertEquals(expectedEstimateTsHi, frame.getTimestampEstimateHi());

                            Assert.assertEquals(tsLo, frame.getTimestampLo());
                            timeFrameCursor.recordAt(record, Rows.toRowID(frame.getFrameIndex(), frame.getRowHi() - 1));
                            long tsHi = record.getTimestamp(0);
                            Assert.assertEquals(tsHi + 1, frame.getTimestampHi());
                        }
                    }
                }

                execute("drop table x");
            });
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
                try (TimeFrameRecordCursor timeFrameCursor = factory.getTimeFrameCursor(executionContext)) {
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
}
