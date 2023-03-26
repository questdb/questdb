/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class O3MetricsTest extends AbstractO3Test {
    private static final long MICROS_IN_DAY = 86400000000L;
    private static final long MICROS_IN_HOUR = 3600000000L;
    private static final long MICROS_IN_MINUTE = 60000000L;
    private static final long MILLENNIUM = 946684800000000L;  // 2020-01-01T00:00:00

    @Test
    public void testAppendOneRow() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 8;

            setupBasicTable(engine, compiler, sqlExecutionContext, initRowCount);

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(13));
                r.putInt(0, 9);
                r.append();
                w.commit();
            }

            printSqlResult(compiler, sqlExecutionContext, "x");
            final String expected = "i\tts\n" +
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n" +
                    "3\t2000-01-01T07:00:00.000000Z\n" +
                    "4\t2000-01-01T08:00:00.000000Z\n" +
                    "5\t2000-01-01T09:00:00.000000Z\n" +
                    "6\t2000-01-01T10:00:00.000000Z\n" +
                    "7\t2000-01-01T11:00:00.000000Z\n" +
                    "8\t2000-01-01T12:00:00.000000Z\n" +
                    "9\t2000-01-01T13:00:00.000000Z\n";  // new row

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getCommittedRows());
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertMiddleEmptyPartition() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 2;
            setupBasicTable(engine, compiler, sqlExecutionContext, initRowCount);

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(2, 0, 0));
                r.putInt(0, 4);
                r.append();

                w.commit();
            }

            {
                printSqlResult(compiler, sqlExecutionContext, "x");
                final String expected = "i\tts\n" +
                        "1\t2000-01-01T05:00:00.000000Z\n" +
                        "2\t2000-01-01T06:00:00.000000Z\n" +
                        "4\t2000-01-03T00:00:00.000000Z\n";  // new row

                TestUtils.assertEquals(expected, sink);
            }

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getCommittedRows());

            // Appended to new partition.
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getPhysicallyWrittenRows());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(1, 0, 0));
                r.putInt(0, 3);
                r.append();

                w.commit();
            }

            {
                printSqlResult(compiler, sqlExecutionContext, "x");
                final String expected = "i\tts\n" +
                        "1\t2000-01-01T05:00:00.000000Z\n" +
                        "2\t2000-01-01T06:00:00.000000Z\n" +
                        "3\t2000-01-02T00:00:00.000000Z\n" +  // new row
                        "4\t2000-01-03T00:00:00.000000Z\n";

                TestUtils.assertEquals(expected, sink);
            }

            Assert.assertEquals(initRowCount + 2, metrics.tableWriter().getCommittedRows());

            // Created new partition that didn't previously exist in between the other two.
            Assert.assertEquals(initRowCount + 2, metrics.tableWriter().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertOneRowBefore() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 8;

            setupBasicTable(engine, compiler, sqlExecutionContext, initRowCount);

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(4));
                r.putInt(0, 0);
                r.append();
                w.commit();
            }

            printSqlResult(compiler, sqlExecutionContext, "x");
            final String expected = "i\tts\n" +
                    "0\t2000-01-01T04:00:00.000000Z\n" +  // new row
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n" +
                    "3\t2000-01-01T07:00:00.000000Z\n" +
                    "4\t2000-01-01T08:00:00.000000Z\n" +
                    "5\t2000-01-01T09:00:00.000000Z\n" +
                    "6\t2000-01-01T10:00:00.000000Z\n" +
                    "7\t2000-01-01T11:00:00.000000Z\n" +
                    "8\t2000-01-01T12:00:00.000000Z\n";

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getCommittedRows());

            // There was a single partition which had to be re-written, along with the additional record.
            Assert.assertEquals(initRowCount * 2 + 1, metrics.tableWriter().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertOneRowMiddle() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 8;

            setupBasicTable(engine, compiler, sqlExecutionContext, initRowCount);

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(8, 30));
                r.putInt(0, 100);
                r.append();
                w.commit();
            }

            printSqlResult(compiler, sqlExecutionContext, "x");
            final String expected = "i\tts\n" +
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n" +
                    "3\t2000-01-01T07:00:00.000000Z\n" +
                    "4\t2000-01-01T08:00:00.000000Z\n" +
                    "100\t2000-01-01T08:30:00.000000Z\n" +  // new row
                    "5\t2000-01-01T09:00:00.000000Z\n" +
                    "6\t2000-01-01T10:00:00.000000Z\n" +
                    "7\t2000-01-01T11:00:00.000000Z\n" +
                    "8\t2000-01-01T12:00:00.000000Z\n";

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getCommittedRows());

            // There was a single partition which had to be re-written, along with the additional record.
            Assert.assertEquals(initRowCount * 2 + 1, metrics.tableWriter().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertRowsAfterEachPartition() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 24;
            setupBasicTable(engine, compiler, sqlExecutionContext, initRowCount);

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(23, 30));
                r.putInt(0, 101);
                r.append();

                r = w.newRow(millenniumTimestamp(23, 15));
                r.putInt(0, 102);
                r.append();

                r = w.newRow(millenniumTimestamp(23, 45));
                r.putInt(0, 103);
                r.append();

                r = w.newRow(millenniumTimestamp(1, 4, 45));
                r.putInt(0, 201);
                r.append();

                r = w.newRow(millenniumTimestamp(1, 4, 30));
                r.putInt(0, 202);
                r.append();

                r = w.newRow(millenniumTimestamp(1, 4, 15));
                r.putInt(0, 203);
                r.append();

                w.commit();
            }

            printSqlResult(compiler, sqlExecutionContext, "x");
            final String expected = "i\tts\n" +
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
                    "201\t2000-01-02T04:45:00.000000Z\n";  // new row

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 6, metrics.tableWriter().getCommittedRows());

            // No partitions had to be re-written: New records appended at the end of each.
            Assert.assertEquals(initRowCount + 6, metrics.tableWriter().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testInsertRowsBeforePartition() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 2;
            setupBasicTable(engine, compiler, sqlExecutionContext, initRowCount);

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r;

                r = w.newRow(millenniumTimestamp(-1));
                r.putInt(0, -1);
                r.append();

                w.commit();
            }

            printSqlResult(compiler, sqlExecutionContext, "x");
            final String expected = "i\tts\n" +
                    "-1\t1999-12-31T23:00:00.000000Z\n" +  // new row
                    "1\t2000-01-01T05:00:00.000000Z\n" +
                    "2\t2000-01-01T06:00:00.000000Z\n";

            TestUtils.assertEquals(expected, sink);

            Metrics metrics = engine.getMetrics();
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getCommittedRows());

            // Appended to earlier partition.
            Assert.assertEquals(initRowCount + 1, metrics.tableWriter().getPhysicallyWrittenRows());
        });
    }

    @Test
    public void testWithO3MaxLag() throws Exception {
        executeVanillaWithMetrics((engine, compiler, sqlExecutionContext) -> {
            final long initRowCount = 2;
            setupBasicTable(engine, compiler, sqlExecutionContext, initRowCount);

            Metrics metrics = engine.getMetrics();

            long rowCount = initRowCount;
            Assert.assertEquals(rowCount, metrics.tableWriter().getPhysicallyWrittenRows());

            try (TableWriter w = TestUtils.getWriter(engine, "x")) {
                TableWriter.Row r = w.newRow(millenniumTimestamp(0));
                r.putInt(0, 100);
                r.append();
                ++rowCount;

                r = w.newRow(millenniumTimestamp(7));
                r.putInt(0, 200);
                r.append();
                ++rowCount;

                r = w.newRow(millenniumTimestamp(8));
                r.putInt(0, 300);
                r.append();
                ++rowCount;

                w.ic();

                r = w.newRow(millenniumTimestamp(7, 45));
                r.putInt(0, 400);
                r.append();
                ++rowCount;

                w.ic();

                w.commit();
                Assert.assertEquals(rowCount, metrics.tableWriter().getCommittedRows());
                Assert.assertEquals(rowCount + 2, metrics.tableWriter().getPhysicallyWrittenRows());

            }
        });
    }

    private static long millenniumTimestamp(int hours) {
        return MILLENNIUM + (hours * MICROS_IN_HOUR);
    }

    private static long millenniumTimestamp(int hours, int minutes) {
        return MILLENNIUM +
                (hours * MICROS_IN_HOUR) +
                (minutes * MICROS_IN_MINUTE);
    }

    private static long millenniumTimestamp(int days, int hours, int minutes) {
        return MILLENNIUM +
                (days * MICROS_IN_DAY) +
                (hours * MICROS_IN_HOUR) +
                (minutes * MICROS_IN_MINUTE);
    }

    /**
     * Set up a daily-partitioned table with hourly entries.
     * A new partition every 24 `rowCount` rows.
     */
    private static void setupBasicTable(
            CairoEngine engine,
            SqlCompiler compiler,
            SqlExecutionContext sqlExecutionContext,
            long rowCount
    ) throws SqlException {
        final String createTableSql = String.format(
                "create table x as (" +
                        "  select" +
                        "    cast(x as int) i," +
                        "    to_timestamp('2000-01', 'yyyy-MM') + ((x + 4) * %d) ts" +
                        "  from" +
                        "    long_sequence(%d)" +
                        ") timestamp(ts) partition by DAY",
                MICROS_IN_HOUR,
                rowCount);
        compiler.compile(createTableSql, sqlExecutionContext);

        Metrics metrics = engine.getMetrics();
        Assert.assertEquals(rowCount, metrics.tableWriter().getCommittedRows());
        Assert.assertEquals(rowCount, metrics.tableWriter().getPhysicallyWrittenRows());
    }
}
