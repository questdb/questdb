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

package io.questdb.griffin;

import io.questdb.cairo.TableWriter;
import io.questdb.griffin.model.IntervalUtils;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class WriteApplyLogTest extends AbstractGriffinTest {
    private static final Log LOG = LogFactory.getLog(WriteApplyLogTest.class);

    @Test
    public void testApplyInOrder100k() throws Exception {
        testApplyInOrder(100_000);
    }

    @Test
    public void testApplyOutOfOrder100k() throws Exception {
        testApplyOutOfOrder(100_000);
    }

    @SuppressWarnings("SameParameterValue")
    private void compareTables(String expected, String actual) throws SqlException {
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expected, actual, LOG);
    }

    private String generateRandomOrderTableSelect(long tsIncrement, int count, String startTime, long tsStartSequence) throws SqlException {
        long tableId = System.nanoTime();
        compile(
                "create table wal_" + tableId + " as (" +
                        generateTableSelect(tsIncrement, count, startTime, tsStartSequence) +
                        ")",
                sqlExecutionContext
        );
        return "select to_long128(rn - 1 + " + tsStartSequence + "L, ts1) ts," +
                "ts1," +
                "i," +
                "timestamp," +
                "b," +
                "c," +
                "d," +
                "e," +
                "f," +
                "j," +
                "l," +
                "l256" +
                " from (select *, row_number() over() rn from (wal_" + tableId + " order by j))";
    }

    @NotNull
    private String generateTableSelect(long tsIncrement, int count1, String startTime, long tsStartSequence) {
        return "select" +
                " to_long128(x - 1 +  " + tsStartSequence + "L, timestamp_sequence('" + startTime + "', " + tsIncrement + "L) ) ts," +
                " timestamp_sequence('" + startTime + "', " + tsIncrement + "L) ts1," +
                " cast(x as int) i," +
                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp," +
                " rnd_boolean() b," +
                " rnd_str('ABC', 'CDE', null, 'XYZ') c," +
                " rnd_double(2) d," +
                " rnd_float(2) e," +
                " rnd_short(10,1024) f," +
                " rnd_long() j," +
                " rnd_byte(2,50) l," +
                " rnd_long256() l256" +
                " from long_sequence(" + count1 + ")";
    }

    @SuppressWarnings("SameParameterValue")
    private void testApplyInOrder(int pointsMultiplier) throws Exception {
        assertMemoryLeak(() -> {
            long tsIncrement = 100 * 60 * 1_000_000L / pointsMultiplier;
            int count1 = 5 * pointsMultiplier;
            int count2 = 7 * pointsMultiplier;
            String startTime1 = "1970-01-06T18:56:03";
            compile(
                    "create table wal_all as (" +
                            generateTableSelect(tsIncrement, count1, startTime1, 0L) +
                            ")",
                    sqlExecutionContext
            );

            String startTime2 = "1970-01-07T00:32:31";
            compile(
                    "insert into wal_all\n" +
                            generateTableSelect(tsIncrement, count2, startTime2, count1),
                    sqlExecutionContext
            );

            // Create talbe to compare to without Long128 column
            compile("create table wal_clean as (select * from wal_all)");
            compile("alter table wal_clean drop column ts");
            compile("alter table wal_clean rename column ts1 to ts");
            compile("create table x as (select * from wal_clean where 1 != 1) timestamp(ts) partition by DAY");

            try (
                    TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "test");
                    Path walPath = new Path()
            ) {
                walPath.of(configuration.getRoot()).concat("wal_all").concat("default");
                long timestampLo = IntervalUtils.parseFloorPartialTimestamp(startTime1);
                long timestampHi = timestampLo + count1 * tsIncrement;

                LOG.info().$("=== Applying WAL transaction ===").$();
                applyWal(writer, walPath, 0, count1, true, timestampLo, timestampHi);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "wal_clean limit " + count1, "x", LOG);

                // Apply second WAL segment
                LOG.info().$("=== Applying WAL transaction 2 ===").$();
                long timestampLo2 = IntervalUtils.parseFloorPartialTimestamp(startTime2);
                long timestampHi2 = timestampLo2 + count2 * tsIncrement;

                applyWal(writer, walPath, count1, count1 + count2, true, timestampLo2, timestampHi2);

                compareTables("select * from wal_clean order by ts", "x");
            }
        });
    }

    @SuppressWarnings("SameParameterValue")
    private void testApplyOutOfOrder(int pointsMultiplier) throws Exception {
        assertMemoryLeak(() -> {
            long tsIncrement = 100 * 60 * 1_000_000L / pointsMultiplier;
            int count1 = 5 * pointsMultiplier;
            int count2 = 7 * pointsMultiplier;
            String startTime1 = "1970-01-06T18:56:03";
            compile(
                    "create table wal_all as (" +
                            generateRandomOrderTableSelect(tsIncrement, count1, startTime1, 0L) +
                            ")",
                    sqlExecutionContext
            );

            String startTime2 = "1970-01-07T00:32:31";
            compile(
                    "insert into wal_all\n" +
                            generateRandomOrderTableSelect(tsIncrement, count2, startTime2, count1),
                    sqlExecutionContext
            );

            // Create talbe to compare to without Long128 column
            compile("create table wal_clean as (select * from wal_all)");
            compile("alter table wal_clean drop column ts");
            compile("alter table wal_clean rename column ts1 to ts");
            compile("create table x as (select * from wal_clean where 1 != 1) timestamp(ts) partition by DAY");

            try (
                    TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "x", "test");
                    Path walPath = new Path()
            ) {
                walPath.of(configuration.getRoot()).concat("wal_all").concat("default");
                long timestampLo = IntervalUtils.parseFloorPartialTimestamp(startTime1);
                long timestampHi = timestampLo + count1 * tsIncrement;

                LOG.info().$("=== Applying WAL transaction ===").$();
                applyWal(writer, walPath, 0, count1, false, timestampLo, timestampHi);

                compareTables("select * from (wal_clean limit " + count1 + ") order by ts", "x");
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, "select * from (wal_clean limit " + count1 + ") order by ts", "x", LOG);

                // Apply second WAL segment
                LOG.info().$("=== Applying WAL transaction 2 ===").$();
                long timestampLo2 = IntervalUtils.parseFloorPartialTimestamp(startTime2);
                long timestampHi2 = timestampLo2 + count2 * tsIncrement;

                applyWal(writer, walPath, count1, count1 + count2, false, timestampLo2, timestampHi2);

                compareTables("select * from wal_clean order by ts", "x");
            }
        });
    }

    private void applyWal(TableWriter writer, Path walPath, int rowLo, int count1, boolean inOrder, long timestampLo, long timestampHi) {
        writer.processWalCommit(walPath, -1L, inOrder, rowLo, count1, timestampLo, timestampHi, null);
    }
}
