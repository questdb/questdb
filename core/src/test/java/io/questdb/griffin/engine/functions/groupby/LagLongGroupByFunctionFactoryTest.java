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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.DateFormat;
import io.questdb.std.datetime.microtime.TimestampFormatCompiler;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class LagLongGroupByFunctionFactoryTest extends AbstractGriffinTest {

    private static final String CREATE_TABLE_STMT = "create table tab as (" +
            "select " +
            "  timestamp_sequence(\n" +
            "            to_timestamp('2021-07-05T22:37:00', 'yyyy-MM-ddTHH:mm:ss'),\n" +
            "            780000000) as ts, " +
            "  rnd_str('aaa', 'bbb', 'ccc', 'ddd', null) as name, " +
            "  rnd_long(0, 20, 1) as value " +
            "from long_sequence(20)" +
            ") timestamp(ts) partition by DAY";

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testBase() throws Exception {
        // to showcase the input data for subsequent tests
        assertQuery(
                "ts\tname\tvalue\n" +
                        "2021-07-05T22:37:00.000000Z\taaa\t3\n" +
                        "2021-07-05T22:50:00.000000Z\tbbb\tNaN\n" +
                        "2021-07-05T23:03:00.000000Z\t\t12\n" +
                        "2021-07-05T23:16:00.000000Z\tbbb\t8\n" +
                        "2021-07-05T23:29:00.000000Z\tccc\t18\n" +

                        "2021-07-05T23:42:00.000000Z\tbbb\t11\n" +
                        "2021-07-05T23:55:00.000000Z\tccc\t5\n" +
                        "2021-07-06T00:08:00.000000Z\tccc\tNaN\n" +
                        "2021-07-06T00:21:00.000000Z\t\t14\n" +

                        "2021-07-06T00:34:00.000000Z\taaa\t4\n" +
                        "2021-07-06T00:47:00.000000Z\taaa\t4\n" +
                        "2021-07-06T01:00:00.000000Z\tbbb\t8\n" +
                        "2021-07-06T01:13:00.000000Z\tddd\t17\n" +
                        "2021-07-06T01:26:00.000000Z\tddd\t4\n" +

                        "2021-07-06T01:39:00.000000Z\taaa\t17\n" +
                        "2021-07-06T01:52:00.000000Z\taaa\tNaN\n" +
                        "2021-07-06T02:05:00.000000Z\t\tNaN\n" +
                        "2021-07-06T02:18:00.000000Z\tbbb\t20\n" +
                        "2021-07-06T02:31:00.000000Z\tccc\t15\n" +

                        "2021-07-06T02:44:00.000000Z\tccc\t2\n",
                "select * from tab",
                CREATE_TABLE_STMT,
                "ts",
                true,
                true,
                true
        );
    }

    @Test
    public void testLagSampleBy1h() throws Exception {
        assertQuery(
                "lag\n" +
                        "8\n" +
                        "14\n" +
                        "17\n" +
                        "20\n" +
                        "NaN\n",
                "select lag(value, 1) from tab sample by 1h",
                CREATE_TABLE_STMT,
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLag2SampleBy1h() throws Exception {
        assertQuery(
                "lag\n" +
                        "12\n" +
                        "NaN\n" +
                        "8\n" +
                        "NaN\n" +
                        "NaN\n",
                "select lag(value, 2, null) from tab sample by 1h",
                CREATE_TABLE_STMT,
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLag4SampleBy1h() throws Exception {
        assertQuery(
                "lag\n" +
                        "3\n" +
                        "11\n" +
                        "NaN\n" +
                        "17\n" +
                        "NaN\n",
                "select lag(value, 4) from tab sample by 1h",
                CREATE_TABLE_STMT,
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLag6SampleBy1h() throws Exception {
        assertQuery(
                "lag\n" +
                        "78\n" +
                        "78\n" +
                        "78\n" +
                        "78\n" +
                        "78\n",
                "select lag(value, 6, 78) from tab sample by 1h",
                CREATE_TABLE_STMT,
                null,
                false,
                true,
                true,
                true
        );
    }


    @Test
    public void testLagSampleBy1d() throws Exception {
        assertQuery(
                "lag\n" +
                        "15\n",
                "select lag(value) from tab sample by 1d",
                CREATE_TABLE_STMT,
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLagSampleBy20m() throws Exception {
        assertQuery(
                "lag\n" +
                        "3\n" +
                        "12\n" +
                        "NaN\n" +
                        "11\n" +
                        "NaN\n" +
                        "14\n" +
                        "NaN\n" +
                        "8\n" +
                        "NaN\n" +
                        "17\n" +
                        "NaN\n" +
                        "20\n" +
                        "NaN\n",
                "select lag(value, 1) from tab sample by 20m",
                CREATE_TABLE_STMT,
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLagSampleBy100ms() throws Exception {
        assertQuery(
                "lag\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n",
                "select lag(value, 1, 0) from tab sample by 100T",
                CREATE_TABLE_STMT,
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testLagEmptyTable() throws Exception {
        assertQuery(
                "lag\n",
                "select lag(value, 6, 78) from tab sample by 1d",
                "create table tab (ts timestamp, name string, value long) timestamp(ts) partition by DAY",
                null,
                false,
                true,
                true,
                true
        );
    }

    @Test
    public void testExcessiveArgs() throws Exception {
        assertFailure("select lag(value, 1, 0, 2) from tab sample by 100T",
                CREATE_TABLE_STMT, 7, "excessive arguments, up to three are expected");
    }

    @Test
    public void testOffsetZero() throws Exception {
        assertFailure("select lag(value, 0) from tab",
                CREATE_TABLE_STMT, 7, "offset must be greater than 0");
    }

    private static final DateFormat TS_PATTERN = new TimestampFormatCompiler().compile("yyyy-MM-ddTHH:mm:ss.Sz");


    private static void appendUserCaseRow(TableWriter writer, int id, long count, long ts) {
        TableWriter.Row row = writer.newRow();
        row.putInt(0, id);
        row.putLong(1, count);
        row.putTimestamp(2, ts);
        row.append();
    }

    @Test
    public void testUserCase1() throws SqlException, NumericException {
        // id	count	ts
        // 0	10	2021-09-09T12:43:45.000000Z
        // 0	11	2021-09-09T12:43:46.000000Z
        // 0	12	2021-09-09T12:43:47.000000Z
        // 0	16	2021-09-09T12:43:48.000000Z

        // 0	17	2021-09-09T12:43:49.000000Z
        // 1	20	2021-09-09T12:43:50.000000Z
        // 1	21	2021-09-09T12:43:51.000000Z
        // 1	21	2021-09-09T12:43:52.000000Z

        // 1	22	2021-09-09T12:43:53.000000Z
        compiler.compile("create table tank(id int, count long, ts timestamp)", sqlExecutionContext);
        try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tank", "testing")) {
            long ts = 1631191425000000L;
            appendUserCaseRow(writer, 0, 10, ts);
            appendUserCaseRow(writer, 0, 11, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 0, 12, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 0, 16, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 0, 17, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 20, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 21, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 21, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 22, ts + Timestamps.SECOND_MICROS);
            writer.commit();
        }
        CharSequence selectQuery = "SELECT ts, last(count) - first(count) FROM tank timestamp(ts) sample by 4s";
        try (RecordCursorFactory factory = compiler.compile(selectQuery, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                sink.clear();
                TestUtils.assertCursor("ts\tcolumn\n" +
                                "2021-09-09T12:43:45.000000Z\t6\n" +
                                "2021-09-09T12:43:49.000000Z\t4\n" +
                                "2021-09-09T12:43:53.000000Z\t0\n",
                        cursor,
                        factory.getMetadata(),
                        true,
                        sink);
            }
        }

    }

    @Test
    public void testUserCase2() throws SqlException, NumericException {
        // id	count	ts
        // 0	10	2021-09-09T12:43:45.000000Z
        // 0	11	2021-09-09T12:43:46.000000Z
        // 0	12	2021-09-09T12:43:47.000000Z
        // 0	16	2021-09-09T12:43:48.000000Z

        // 0	17	2021-09-09T12:43:49.000000Z

        // 1	20	2021-09-09T12:43:50.000000Z
        // 1	21	2021-09-09T12:43:51.000000Z
        // 1	21	2021-09-09T12:43:52.000000Z

        // 1	22	2021-09-09T12:43:53.000000Z
        compiler.compile("create table tank(id int, count long, ts timestamp)", sqlExecutionContext);
        try (TableWriter writer = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), "tank", "testing")) {
            long ts = 1631191425000000L;
            appendUserCaseRow(writer, 0, 10, ts);
            appendUserCaseRow(writer, 0, 11, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 0, 12, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 0, 16, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 0, 17, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 20, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 21, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 21, ts +=Timestamps.SECOND_MICROS);
            appendUserCaseRow(writer, 1, 22, ts + Timestamps.SECOND_MICROS);
            writer.commit();
        }
        CharSequence selectQuery = "SELECT ts, id, lag(count, 2, 0) FROM tank timestamp(ts) sample by 4s";
        try (RecordCursorFactory factory = compiler.compile(selectQuery, sqlExecutionContext).getRecordCursorFactory()) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                sink.clear();
                TestUtils.assertCursor("ts\tid\tlag\n" +
                                "2021-09-09T12:43:45.000000Z\t0\t11\n" +
                                "2021-09-09T12:43:49.000000Z\t0\t0\n" +
                                "2021-09-09T12:43:49.000000Z\t1\t20\n" +
                                "2021-09-09T12:43:53.000000Z\t1\t0\n",
                        cursor,
                        factory.getMetadata(),
                        true,
                        sink);
            }
        }
    }
}
