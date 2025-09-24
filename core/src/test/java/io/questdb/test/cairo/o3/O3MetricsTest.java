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

package io.questdb.test.cairo.o3;

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TimestampDriver;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.AbstractCairoTest.replaceTimestampSuffix;

@RunWith(Parameterized.class)
public class O3MetricsTest extends AbstractO3Test {
    private static final long MICROS_IN_HOUR = 3600000000L;
    private static final long MILLENNIUM = 946684800000000L;  // 2020-01-01T00:00:00

    public O3MetricsTest(TestTimestampType timestampType) {
        super(timestampType);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {TestTimestampType.MICRO}, {TestTimestampType.NANO}
        });
    }

    @Test
    public void testAppendOneRow() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 8;

            setupBasicTable(engine, sqlExecutionContext, initRowCount, timestampType.getTypeName());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 13));
                r.putInt(0, 9);
                r.append();
                w.commit();
            }

            engine.print("x", sink, sqlExecutionContext);
            final String expected = replaceTimestampSuffix("i\tts\n" +
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n" +
                    "3\t2000-01-01T07:00:00.000000Z\n" +
                    "4\t2000-01-01T08:00:00.000000Z\n" +
                    "5\t2000-01-01T09:00:00.000000Z\n" +
                    "6\t2000-01-01T10:00:00.000000Z\n" +
                    "7\t2000-01-01T11:00:00.000000Z\n" +
                    "8\t2000-01-01T12:00:00.000000Z\n" +
                    "9\t2000-01-01T13:00:00.000000Z\n", timestampType.getTypeName());  // new row

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getCommittedRows());
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertMiddleEmptyPartition() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 2;
            setupBasicTable(engine, sqlExecutionContext, initRowCount, timestampType.getTypeName());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 2, 0, 0));
                r.putInt(0, 4);
                r.append();

                w.commit();
            }

            {
                engine.print("x", sink, sqlExecutionContext);
                final String expected = replaceTimestampSuffix("i\tts\n" +
                        "1\t2000-01-01T05:00:00.000000Z\n" +
                        "2\t2000-01-01T06:00:00.000000Z\n" +
                        "4\t2000-01-03T00:00:00.000000Z\n", timestampType.getTypeName());  // new row

                TestUtils.assertEquals(expected, sink);
            }

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getCommittedRows());

            // Appended to new partition.
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getPhysicallyWrittenRows());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 1, 0, 0));
                r.putInt(0, 3);
                r.append();

                w.commit();
            }

            {
                engine.print("x", sink, sqlExecutionContext);
                final String expected = replaceTimestampSuffix("i\tts\n" +
                        "1\t2000-01-01T05:00:00.000000Z\n" +
                        "2\t2000-01-01T06:00:00.000000Z\n" +
                        "3\t2000-01-02T00:00:00.000000Z\n" +  // new row
                        "4\t2000-01-03T00:00:00.000000Z\n", timestampType.getTypeName());

                TestUtils.assertEquals(expected, sink);
            }

            Assert.assertEquals(initRowCount + 2, metrics.tableWriterMetrics().getCommittedRows());

            // Created new partition that didn't previously exist in between the other two.
            Assert.assertEquals(initRowCount + 2, metrics.tableWriterMetrics().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertOneRowBefore() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 8;

            setupBasicTable(engine, sqlExecutionContext, initRowCount, timestampType.getTypeName());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 4));
                r.putInt(0, 0);
                r.append();
                w.commit();
            }

            engine.print("x", sink, sqlExecutionContext);
            final String expected = replaceTimestampSuffix("i\tts\n" +
                    "0\t2000-01-01T04:00:00.000000Z\n" +  // new row
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n" +
                    "3\t2000-01-01T07:00:00.000000Z\n" +
                    "4\t2000-01-01T08:00:00.000000Z\n" +
                    "5\t2000-01-01T09:00:00.000000Z\n" +
                    "6\t2000-01-01T10:00:00.000000Z\n" +
                    "7\t2000-01-01T11:00:00.000000Z\n" +
                    "8\t2000-01-01T12:00:00.000000Z\n", timestampType.getTypeName());

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getCommittedRows());

            // There was a single partition which had to be re-written, along with the additional record.
            Assert.assertEquals(initRowCount * 2 + 1, metrics.tableWriterMetrics().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertOneRowMiddle() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 8;

            setupBasicTable(engine, sqlExecutionContext, initRowCount, timestampType.getTypeName());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 8, 30));
                r.putInt(0, 100);
                r.append();
                w.commit();
            }

            engine.print("x", sink, sqlExecutionContext);
            final String expected = replaceTimestampSuffix("i\tts\n" +
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n" +
                    "3\t2000-01-01T07:00:00.000000Z\n" +
                    "4\t2000-01-01T08:00:00.000000Z\n" +
                    "100\t2000-01-01T08:30:00.000000Z\n" +  // new row
                    "5\t2000-01-01T09:00:00.000000Z\n" +
                    "6\t2000-01-01T10:00:00.000000Z\n" +
                    "7\t2000-01-01T11:00:00.000000Z\n" +
                    "8\t2000-01-01T12:00:00.000000Z\n", timestampType.getTypeName());

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getCommittedRows());

            // There was a single partition which had to be re-written, along with the additional record.
            Assert.assertEquals(initRowCount * 2 + 1, metrics.tableWriterMetrics().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertRowsAfterEachPartition() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 24;
            setupBasicTable(engine, sqlExecutionContext, initRowCount, timestampType.getTypeName());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 23, 30));
                r.putInt(0, 101);
                r.append();

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 23, 15));
                r.putInt(0, 102);
                r.append();

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 23, 45));
                r.putInt(0, 103);
                r.append();

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 1, 4, 45));
                r.putInt(0, 201);
                r.append();

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 1, 4, 30));
                r.putInt(0, 202);
                r.append();

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 1, 4, 15));
                r.putInt(0, 203);
                r.append();

                w.commit();
            }

            engine.print("x", sink, sqlExecutionContext);
            final String expected = replaceTimestampSuffix("i\tts\n" +
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n" +
                    "3\t2000-01-01T07:00:00.000000Z\n" +
                    "4\t2000-01-01T08:00:00.000000Z\n" +
                    "5\t2000-01-01T09:00:00.000000Z\n" +
                    "6\t2000-01-01T10:00:00.000000Z\n" +
                    "7\t2000-01-01T11:00:00.000000Z\n" +
                    "8\t2000-01-01T12:00:00.000000Z\n" +
                    "9\t2000-01-01T13:00:00.000000Z\n" +
                    "10\t2000-01-01T14:00:00.000000Z\n" +
                    "11\t2000-01-01T15:00:00.000000Z\n" +
                    "12\t2000-01-01T16:00:00.000000Z\n" +
                    "13\t2000-01-01T17:00:00.000000Z\n" +
                    "14\t2000-01-01T18:00:00.000000Z\n" +
                    "15\t2000-01-01T19:00:00.000000Z\n" +
                    "16\t2000-01-01T20:00:00.000000Z\n" +
                    "17\t2000-01-01T21:00:00.000000Z\n" +
                    "18\t2000-01-01T22:00:00.000000Z\n" +
                    "19\t2000-01-01T23:00:00.000000Z\n" +
                    "102\t2000-01-01T23:15:00.000000Z\n" +  // new row
                    "101\t2000-01-01T23:30:00.000000Z\n" +  // new row
                    "103\t2000-01-01T23:45:00.000000Z\n" +  // new row
                    "20\t2000-01-02T00:00:00.000000Z\n" +
                    "21\t2000-01-02T01:00:00.000000Z\n" +
                    "22\t2000-01-02T02:00:00.000000Z\n" +
                    "23\t2000-01-02T03:00:00.000000Z\n" +
                    "24\t2000-01-02T04:00:00.000000Z\n" +
                    "203\t2000-01-02T04:15:00.000000Z\n" +  // new row
                    "202\t2000-01-02T04:30:00.000000Z\n" +  // new row
                    "201\t2000-01-02T04:45:00.000000Z\n", timestampType.getTypeName());  // new row

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 6, metrics.tableWriterMetrics().getCommittedRows());

            // No partitions had to be re-written: New records appended at the end of each.
            Assert.assertEquals(initRowCount + 6, metrics.tableWriterMetrics().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertRowsBeforePartition() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 2;
            setupBasicTable(engine, sqlExecutionContext, initRowCount, timestampType.getTypeName());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), -1));
                r.putInt(0, -1);
                r.append();

                w.commit();
            }

            engine.print("x", sink, sqlExecutionContext);
            final String expected = replaceTimestampSuffix("i\tts\n" +
                    "-1\t1999-12-31T23:00:00.000000Z\n" +  // new row
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n", timestampType.getTypeName());

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getCommittedRows());

            // Appended to earlier partition.
            Assert.assertEquals(initRowCount + 1, metrics.tableWriterMetrics().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testWithO3MaxLag() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 2;
            setupBasicTable(engine, sqlExecutionContext, initRowCount, timestampType.getTypeName());

            Metrics metrics = engine.getMetrics();

            long rowCount = initRowCount;
            Assert.assertEquals(rowCount, metrics.tableWriterMetrics().getPhysicallyWrittenRows());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 0));
                r.putInt(0, 100);
                r.append();
                ++rowCount;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 7));
                r.putInt(0, 200);
                r.append();
                ++rowCount;

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 8));
                r.putInt(0, 300);
                r.append();
                ++rowCount;

                w.ic();

                r = w.newRow(millenniumTimestamp(timestampType.getTimestampType(), 7, 45));
                r.putInt(0, 400);
                r.append();
                ++rowCount;

                w.ic();

                w.commit();
                Assert.assertEquals(rowCount, metrics.tableWriterMetrics().getCommittedRows());
                Assert.assertEquals(rowCount + 2, metrics.tableWriterMetrics().getPhysicallyWrittenRows());

            }
        });
    }

    private static long millenniumTimestamp(int timestampType, int hours) {
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        return driver.fromMicros(MILLENNIUM) + driver.fromHours(hours);
    }

    private static long millenniumTimestamp(int timestampType, int hours, int minutes) {
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        return driver.fromMicros(MILLENNIUM) +
                driver.fromHours(hours) +
                driver.fromMinutes(minutes);
    }

    private static long millenniumTimestamp(int timestampType, int days, int hours, int minutes) {
        TimestampDriver driver = ColumnType.getTimestampDriver(timestampType);
        return driver.fromMicros(MILLENNIUM) +
                driver.fromDays(days) +
                driver.fromHours(hours) +
                driver.fromMinutes(minutes);
    }

    /**
     * Set up a daily-partitioned table with hourly entries.
     * A new partition every 24 `rowCount` rows.
     */
    private static void setupBasicTable(
            CairoEngine engine,
            SqlExecutionContext sqlExecutionContext,
            long rowCount,
            String timestampType
    ) throws SqlException {
        final String createTableSql = String.format(
                "create table x as (" +
                        "  select" +
                        "    cast(x as int) i," +
                        "    (to_timestamp('2000-01', 'yyyy-MM') + ((x + 4) * %d))::" + timestampType + " ts" +
                        "  from" +
                        "    long_sequence(%d)" +
                        ") timestamp(ts) partition by DAY",
                MICROS_IN_HOUR,
                rowCount);
        engine.execute(createTableSql, sqlExecutionContext);

        Metrics metrics = engine.getMetrics();
        Assert.assertEquals(rowCount, metrics.tableWriterMetrics().getCommittedRows());
        Assert.assertEquals(rowCount, metrics.tableWriterMetrics().getPhysicallyWrittenRows());
    }
}
