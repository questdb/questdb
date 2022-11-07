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

import io.questdb.cairo.SqlJitMode;
import org.junit.Test;

/**
 * Test order by asc and desc with filter(s)/where clause
 */
public class OrderByWithFilterTest extends AbstractGriffinTest {

    static final int ORDER_ASC = 0;
    static final int ORDER_DESC = 1;

    @Test
    public void testOrderByDescInOverClause() throws Exception {
        String expected = "ts\ttemp\n" +
                "1970-04-23T22:00:00.000000Z\t99.9780\n" +
                "1971-02-02T02:00:00.000000Z\t98.3369\n";
        String direction = "desc";

        assertOrderByInOverClause(expected, direction);
    }

    @Test
    public void testOrderByAscInOverClause() throws Exception {
        String expected = "ts\ttemp\n" +
                "1970-05-23T02:00:00.000000Z\t0.0049\n" +
                "1971-02-21T16:00:00.000000Z\t0.3032\n";
        String direction = "asc";

        assertOrderByInOverClause(expected, direction);
    }

    private void assertOrderByInOverClause(String expected, String direction) throws Exception {
        assertQuery(expected,
                "select ts, temp from \n" +
                        "( \n" +
                        "  Select temp, ts, \n" +
                        "         row_number() over (partition by timestamp_floor('y', ts) order by temp " + direction + ")  rid \n" +
                        "  from weather \n" +
                        ") inq \n" +
                        "where rid = 1 \n" +
                        "order by ts",
                "create table weather as " +
                        "(select cast(x*36000000000 as timestamp) ts, \n" +
                        "  rnd_float(0)*100 temp from long_sequence(1000));", null);
    }

    @Test
    public void testOrderByAscWithByteFilter() throws Exception {
        testOrderByWithFilter("byte", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithCharFilter() throws Exception {
        testOrderByWithFilter("char", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithDoubleFilter() throws Exception {
        testOrderByWithFilter("double", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithFloatFilter() throws Exception {
        testOrderByWithFilter("float", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithIntFilter() throws Exception {
        testOrderByWithFilter("int", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithLongFilter() throws Exception {
        testOrderByWithFilter("long", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithShortFilter() throws Exception {
        testOrderByWithFilter("short", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithStringFilter() throws Exception {
        testOrderByWithFilter("string", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithSymbolFilter() throws Exception {
        testOrderByWithFilter("symbol", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithTimestampFilter() throws Exception {
        testOrderByWithFilter("timestamp", ORDER_ASC);
    }

    @Test//triggers DeferredSingleSymbolFilterDataFrameRecordCursorFactory
    public void testOrderByDescSelectByIndexedSymbolColumn() throws Exception {
        runQueries("CREATE TABLE trips(l long,s symbol index capacity 10, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, case when x <= 5 then 'ABC' when x <= 7 then 'DEF' else 'GHI' end," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");

        assertQuery("l\ts\tts\n" +
                        "7\tDEF\t2022-01-09T22:40:00.000000Z\n" +
                        "6\tDEF\t2022-01-08T18:53:20.000000Z\n",
                "select l, s, ts from trips where s = 'DEF' order by ts desc",
                null, null, true, false, false);
    }

    @Test
    public void testOrderByDescWithByteFilter() throws Exception {
        testOrderByWithFilter("byte", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithCharFilter() throws Exception {
        testOrderByWithFilter("char", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithDataFrameRecordCursorFactory() throws Exception {
        runQueries("CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
        //A0, A1, A2, A0, A1, A2, A0, A1, A2, A0
        assertQuery("l\ts\tts\n" +
                        "8\tA2\t2022-01-11T02:26:40.000000Z\n" +
                        "5\tA2\t2022-01-07T15:06:40.000000Z\n" +
                        "2\tA2\t2022-01-04T03:46:40.000000Z\n",
                "select l, s, ts from trips where s = 'A2' and test_match() order by ts desc",
                null, null, true, false, false);
    }

    @Test
    public void testOrderByDescWithDoubleFilter() throws Exception {
        testOrderByWithFilter("double", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithFilterOnExcludedValuesRecordCursorFactory() throws Exception {
        runQueries("CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
        assertQuery("l\ts\tts\n" +
                        "9\tA0\t2022-01-12T06:13:20.000000Z\n" +
                        "8\tA2\t2022-01-11T02:26:40.000000Z\n" +
                        "6\tA0\t2022-01-08T18:53:20.000000Z\n" +
                        "5\tA2\t2022-01-07T15:06:40.000000Z\n" +
                        "3\tA0\t2022-01-05T07:33:20.000000Z\n" +
                        "2\tA2\t2022-01-04T03:46:40.000000Z\n",
                "select l, s, ts from trips where s != 'A1' and test_match() order by ts desc",
                null, null, true, false, false);
    }

    @Test
    public void testOrderByDescWithFilterOnSubQueryRecordCursorFactory() throws Exception {
        runQueries("CREATE TABLE trips(l long,s symbol index capacity 10, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, case when x<=3 then 'ABC' when x>6 and x <= 9 then 'DEF' else 'GHI' end," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");

        assertQuery("l\ts\tts\n" +
                        "9\tDEF\t2022-01-12T06:13:20.000000Z\n" +
                        "8\tDEF\t2022-01-11T02:26:40.000000Z\n" +
                        "7\tDEF\t2022-01-09T22:40:00.000000Z\n" +
                        "3\tABC\t2022-01-05T07:33:20.000000Z\n" +
                        "2\tABC\t2022-01-04T03:46:40.000000Z\n" +
                        "1\tABC\t2022-01-03T00:00:00.000000Z\n",
                "select l, s, ts from trips where s in (select 'DEF' union all select 'ABC' ) and length(s) = 3 order by ts desc",
                null, null, true, false, false);
    }

    @Test
    public void testOrderByDescWithFilterOnValuesRecordCursorFactory() throws Exception {
        runQueries("CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
        //A0, A1, A2, A0, A1, A2, A0, A1, A2, A0
        assertQuery("l\ts\tts\n" +
                        "9\tA0\t2022-01-12T06:13:20.000000Z\n" +
                        "8\tA2\t2022-01-11T02:26:40.000000Z\n" +
                        "6\tA0\t2022-01-08T18:53:20.000000Z\n" +
                        "5\tA2\t2022-01-07T15:06:40.000000Z\n" +
                        "3\tA0\t2022-01-05T07:33:20.000000Z\n" +
                        "2\tA2\t2022-01-04T03:46:40.000000Z\n",
                "select l, s, ts from trips where s in ('A2', 'A0') and length(s) = 2 order by ts desc",
                null, null, true, false, false);
    }

    @Test
    public void testOrderByDescWithFloatFilter() throws Exception {
        testOrderByWithFilter("float", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithIntFilter() throws Exception {
        testOrderByWithFilter("int", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithLongFilter() throws Exception {
        testOrderByWithFilter("long", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithMultipleNotEqualsSymbolConditions() throws Exception {
        runQueries("CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");
        assertQuery("l\ts\tts\n" +
                        "8\tA2\t2022-01-11T02:26:40.000000Z\n" +
                        "5\tA2\t2022-01-07T15:06:40.000000Z\n" +
                        "2\tA2\t2022-01-04T03:46:40.000000Z\n",
                "select l, s, ts from trips where s != 'A1' and s != 'A0' and test_match() order by ts desc",
                null, null, true, false, false);
    }

    @Test
    public void testOrderByDescWithShortFilter() throws Exception {
        testOrderByWithFilter("short", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithStringFilter() throws Exception {
        testOrderByWithFilter("string", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithSymbolFilter() throws Exception {
        testOrderByWithFilter("symbol", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithTimestampFilter() throws Exception {
        testOrderByWithFilter("timestamp", ORDER_DESC);
    }

    @Test
    public void testOrderByTimestampWithColumnTops() throws Exception {

        runQueries("CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-01T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 10000000000) " +
                        "  from long_sequence(10);",
                "alter table trips add col1 int",
                "alter table trips add col2 string",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-02T12:00:00', 'yyyy-MM-ddTHH:mm:ss'), 10000000000), " +
                        "  x * 10, " +
                        "  cast(x * 100 as string) " +
                        "  from long_sequence(10)");

        assertQuery("l\tts\tcol1\tcol2\n" +
                        "10\t2022-01-03T13:00:00.000000Z\t100\t1000\n" +
                        "9\t2022-01-03T10:13:20.000000Z\t90\t900\n" +
                        "8\t2022-01-03T07:26:40.000000Z\t80\t800\n" +
                        "10\t2022-01-02T01:00:00.000000Z\tNaN\t\n",
                "select l as l, ts, col1, col2 from trips where l > 7 order by ts desc limit 4",
                null, "ts###DESC", true, false, true);

        assertQuery("l\tts\tcol1\tcol2\n" +
                        "1010\t2022-01-03T13:00:00.000000Z\t100\t1000\n" +
                        "1009\t2022-01-03T10:13:20.000000Z\t90\t900\n" +
                        "1008\t2022-01-03T07:26:40.000000Z\t80\t800\n" +
                        "1010\t2022-01-02T01:00:00.000000Z\tNaN\t\n",
                "select l + 1000 as l, ts, col1, col2 from trips where l > 7 order by ts desc limit 4",
                null, "ts###DESC", true, false, true);

        assertQuery("l\tts\tcol1\tcol2\n" +
                        "9\t2022-01-01T22:13:20.000000Z\tNaN\t\n" +
                        "10\t2022-01-02T01:00:00.000000Z\tNaN\t\n" +
                        "9\t2022-01-03T10:13:20.000000Z\t90\t900\n" +
                        "10\t2022-01-03T13:00:00.000000Z\t100\t1000\n",
                "select l, ts, col1, col2 from trips where l > 8 order by ts",
                null, "ts", true, false, false);

        assertQuery("l\tts\tcol1\tcol2\n" +
                        "4\t2022-01-02T20:20:00.000000Z\t40\t400\n" +
                        "10\t2022-01-02T01:00:00.000000Z\tNaN\t\n" +
                        "9\t2022-01-01T22:13:20.000000Z\tNaN\t\n" +
                        "8\t2022-01-01T19:26:40.000000Z\tNaN\t\n" +
                        "7\t2022-01-01T16:40:00.000000Z\tNaN\t\n",
                "select l, ts, col1, col2 from trips where ts between '2022-01-01T14' and '2022-01-02T23' and l > 3 order by ts desc limit 5",
                null, null, true, false, true);
    }

    @Test
    public void testOrderByTimestampWithComplexJITFilter() throws Exception {
        runQueries("CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(1000);");

        assertQuery("l\tts\n" +
                        "5\t2022-01-07T15:06:40.000000Z\n",
                "select l, ts from trips " +
                        "where l <=5 and ts < to_timestamp('2022-01-08T00:00:00', 'yyyy-MM-ddTHH:mm:ss') " +
                        "order by ts desc limit 1",
                null, null, true, false, true);
    }

    @Test
    public void testOrderByTimestampWithJITAndIntervalFilters() throws Exception {

        runQueries("CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence('2022-01-03', 50000000000) " +
                        "  from long_sequence(10);");

        assertQuery("l\tts\n" +
                        "3\t2022-01-04T03:46:40.000000Z\n" +
                        "2\t2022-01-03T13:53:20.000000Z\n" +
                        "1\t2022-01-03T00:00:00.000000Z\n",
                "select l, ts from trips " +
                        "where l <=5 and ts < '2022-01-04T04' " +
                        "order by ts desc",
                null, null, true, false, false);
    }

    @Test
    public void testOrderByTimestampWithJitFilterAndLimitDesc() throws Exception {

        runQueries("CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");

        assertQuery("l\tts\n" +
                        "5\t2022-01-07T15:06:40.000000Z\n" +
                        "4\t2022-01-06T11:20:00.000000Z\n" +
                        "3\t2022-01-05T07:33:20.000000Z\n" +
                        "2\t2022-01-04T03:46:40.000000Z\n" +
                        "1\t2022-01-03T00:00:00.000000Z\n",
                "select l, ts from trips where l <=5 order by ts desc limit 5",
                null,
                "ts###DESC",
                true,
                false,
                true
        );
    }

    @Test
    public void testOrderByTimestampWithJitFilterAndLimitAsc() throws Exception {

        runQueries("CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");

        assertQuery("l\tts\n" +
                        "1\t2022-01-03T00:00:00.000000Z\n" +
                        "2\t2022-01-04T03:46:40.000000Z\n" +
                        "3\t2022-01-05T07:33:20.000000Z\n" +
                        "4\t2022-01-06T11:20:00.000000Z\n" +
                        "5\t2022-01-07T15:06:40.000000Z\n",
                "select l, ts from trips where l <=5 order by ts asc limit 5",
                null, "ts###ASC", true, false, true);
    }

    @Test
    public void testOrderByTimestampWithNonJitFilter() throws Exception {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);

        runQueries("CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");

        assertQuery("l\tts\n" +
                        "5\t2022-01-07T15:06:40.000000Z\n" +
                        "4\t2022-01-06T11:20:00.000000Z\n" +
                        "3\t2022-01-05T07:33:20.000000Z\n" +
                        "2\t2022-01-04T03:46:40.000000Z\n" +
                        "1\t2022-01-03T00:00:00.000000Z\n",
                "select l, ts from trips where l <=5 order by ts desc limit 5",
                null, "ts###DESC", true, false, true);
    }

    @Test
    public void testOrderByTimestampWithJitFilterDesc() throws Exception {
        testOrderByTimestampWithFilterDesc();
    }

    @Test
    public void testOrderByTimestampWithNonJitFilterDesc() throws Exception {
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
        testOrderByTimestampWithFilterDesc();
    }

    private void testOrderByTimestampWithFilterDesc() throws Exception {
        runQueries("CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);");

        assertQuery("l\tts\n" +
                        "5\t2022-01-07T15:06:40.000000Z\n" +
                        "4\t2022-01-06T11:20:00.000000Z\n" +
                        "3\t2022-01-05T07:33:20.000000Z\n" +
                        "2\t2022-01-04T03:46:40.000000Z\n" +
                        "1\t2022-01-03T00:00:00.000000Z\n",
                "select l, ts from trips where l <= 5 order by ts desc",
                null, "ts###DESC", true, false, false);
    }

    private void runQueries(String... queries) throws Exception {
        assertMemoryLeak(() -> {
            for (String query : queries) {
                compiler.compile(query, sqlExecutionContext).execute(null).await();
            }
        });
    }

    private void testOrderByWithFilter(String type, int order) throws Exception {

        String function;
        if ("double".equals(type) || "float".equals(type)) {
            function = "4+rnd_#TYPE#(50)*100";
        } else if ("short".equals(type) || "byte".equals(type)) {
            function = "rnd_#TYPE#(4,100)";
        } else if ("char".equals(type)) {
            function = "cast(rnd_byte(4,100) as char)";
        } else if ("symbol".equals(type)) {
            function = "cast('' || rnd_int(4,100,50) as symbol)";
        } else if ("string".equals(type)) {
            function = "'' || rnd_int(4,100,50)";
        } else {
            function = "rnd_#TYPE#(4,100,50)";
        }

        runQueries("CREATE TABLE test(x #TYPE#, ts TIMESTAMP) timestamp(ts) partition by month;".replace("#TYPE#", type),
                //should create 3+ partitions with randomly ordered x values
                ("insert into test " +
                        "select #FUNC#,\n" +
                        "    timestamp_sequence('2022-01-01'::timestamp, 100000000000)\n" +
                        "from long_sequence(100)\n" +
                        "union all \n" +
                        "select cast(1 as #TYPE#), rnd_timestamp('2022-01-01'::timestamp, '2022-01-03'::timestamp + 33*100000000000 , 0)\n" +
                        "union all  " +
                        "select cast(2 as #TYPE#), rnd_timestamp('2022-01-01'::timestamp + 34*100000000000, '2022-01-03'::timestamp + 66*100000000000 , 0)\n" +
                        "union all " +
                        "select cast(3 as #TYPE#), rnd_timestamp('2022-01-01'::timestamp + 67*100000000000, '2022-01-03'::timestamp + 100*100000000000 , 0)\n")
                        .replace("#FUNC#", function)
                        .replace("#TYPE#", type));
        //add new column and create more partitions to trigger jit col tops case
        assertMemoryLeak(() -> compile("alter table test add column y double;"));
        runQueries(("insert into test select #FUNC#, timestamp_sequence('2022-01-01'::timestamp + 100*100000000000, 100000000000), rnd_double() " +
                "from long_sequence(100) ")
                .replace("#FUNC#", function)
                .replace("#TYPE#", type));

        String expectedResult;
        switch (type) {
            case "float":
                expectedResult = order == ORDER_ASC ? "x\n1.0000\n2.0000\n3.0000\n" : "x\n3.0000\n2.0000\n1.0000\n";
                break;
            case "double":
                expectedResult = order == ORDER_ASC ? "x\n1.0\n2.0\n3.0\n" : "x\n3.0\n2.0\n1.0\n";
                break;
            case "timestamp":
                expectedResult = order == ORDER_ASC ? "x\n1970-01-01T00:00:00.000001Z\n1970-01-01T00:00:00.000002Z\n1970-01-01T00:00:00.000003Z\n" :
                        "x\n1970-01-01T00:00:00.000003Z\n1970-01-01T00:00:00.000002Z\n1970-01-01T00:00:00.000001Z\n";
                break;
            case "char":
                expectedResult = order == ORDER_ASC ? "x\n\u0001\n\u0002\n\u0003\n" : "x\n\u0003\n\u0002\n\u0001\n";
                break;
            default:
                expectedResult = order == ORDER_ASC ? "x\n1\n2\n3\n" : "x\n3\n2\n1\n";
                break;
        }

        if ("string".equals(type) || "symbol".equals(type)) {
            assertQuery(expectedResult,
                    ("select x from test where x in ('1', '2', '3') and y = NaN order by ts " + (order == ORDER_ASC ? "asc" : "desc")).replace("#TYPE#", type),
                    null, null, true, false, false);
        } else if ("char".equals(type)) {
            assertQuery(expectedResult,
                    ("select x from test where x in (cast(1 as char), cast(2 as char), cast(3 as char)) and y = NaN order by ts " + (order == ORDER_ASC ? "asc" : "desc")).replace("#TYPE#", type),
                    null, null, true, false, false);
        } else {
            assertQuery(expectedResult,
                    ("select x from test where x <= 3 and y = NaN order by ts " + (order == ORDER_ASC ? "asc" : "desc")).replace("#TYPE#", type),
                    null, null, true, false, false);
        }
    }
}
