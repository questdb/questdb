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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AsOfJoinTest extends AbstractGriffinTest {

    @Before
    public void resetJoinType() {
        compiler.setFullFatJoins(false);
    }

    @Test
    public void testAsOfJoinAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "CREATE TABLE fx_rate (" +
                            "    ts TIMESTAMP, " +
                            "    code SYMBOL CAPACITY 128 NOCACHE, " +
                            "    rate INT" +
                            ") timestamp(ts)",
                    sqlExecutionContext
            );
            executeInsert("INSERT INTO fx_rate values ('2022-10-05T04:00:00.000000Z', '1001', 10);");

            compiler.compile(
                    "CREATE TABLE trades (" +
                            "    ts TIMESTAMP, " +
                            "    price INT, " +
                            "    qty INT, " +
                            "    flag INT, " +
                            "    fx_rate_code SYMBOL CAPACITY 128 NOCACHE" +
                            ") timestamp(ts);",
                    sqlExecutionContext
            );
            executeInsert("INSERT INTO trades values ('2022-10-05T08:15:00.000000Z', 100, 500, 0, '1001');");
            executeInsert("INSERT INTO trades values ('2022-10-05T08:16:00.000000Z', 100, 500, 1, '1001');");
            executeInsert("INSERT INTO trades values ('2022-10-05T08:16:00.000000Z', 100, 500, 2, '1001');");

            String query =
                    "SELECT\n" +
                            "  SUM(CASE WHEN t.flag = 0 THEN 0.9 * (t.price * f.rate) ELSE 0.0 END)," +
                            "  SUM(CASE WHEN t.flag = 1 THEN 0.7 * (t.price * f.rate) ELSE 0.0 END)," +
                            "  SUM(CASE WHEN t.flag = 2 THEN 0.2 * (t.price * f.rate) ELSE 0.0 END)" +
                            "FROM  " +
                            "  trades t " +
                            "ASOF JOIN fx_rate f on f.code = t.fx_rate_code";

            String expected = "SUM\tSUM1\tSUM2\n" +
                    "900.0\t700.0\t200.0\n";

            printSqlResult(expected, query, null, false, true);
        });
    }

    @Test
    public void testAsOfJoinCombinedWithInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table t1 as (select x as id, cast(x as timestamp) ts from long_sequence(5)) timestamp(ts) partition by day;",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table t2 as (select x as id, cast(x as timestamp) ts from long_sequence(5)) timestamp(ts) partition by day;",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table t3 (id long, ts timestamp) timestamp(ts) partition by day;",
                    sqlExecutionContext
            );

            final String query = "SELECT *\n" +
                    "FROM (\n" +
                    "  (t1 INNER JOIN t2 ON id) \n" +
                    "  ASOF JOIN t3 ON id\n" +
                    ");";
            final String expected = "id\tts\tid1\tts1\tid2\tts2\n" +
                    "1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\tNaN\t\n" +
                    "2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\tNaN\t\n" +
                    "3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\tNaN\t\n" +
                    "4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\tNaN\t\n" +
                    "5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\tNaN\t\n";
            printSqlResult(expected, query, "ts", false, false);
        });
    }

    @Test
    public void testAsOfJoinDynamicTimestamp() throws Exception {
        compiler.compile(
                "create table positions2 as (" +
                        "select x, cast(x * 1000000L as TIMESTAMP) time from long_sequence(10)" +
                        ") timestamp(time)", sqlExecutionContext);

        assertSql("select t1.time1 + 1 as time, t1.x, t2.x, t1.x - t2.x\n" +
                        "from \n" +
                        "(\n" +
                        "    (\n" +
                        "        select time - 1 as time1, x\n" +
                        "        from positions2\n" +
                        "    )\n" +
                        "    timestamp(time1)\n" +
                        ") t1\n" +
                        "asof join positions2 t2",
                "time\tx\tx1\tcolumn\n" +
                        "1970-01-01T00:00:01.000000Z\t1\tNaN\tNaN\n" +
                        "1970-01-01T00:00:02.000000Z\t2\t1\t1\n" +
                        "1970-01-01T00:00:03.000000Z\t3\t2\t1\n" +
                        "1970-01-01T00:00:04.000000Z\t4\t3\t1\n" +
                        "1970-01-01T00:00:05.000000Z\t5\t4\t1\n" +
                        "1970-01-01T00:00:06.000000Z\t6\t5\t1\n" +
                        "1970-01-01T00:00:07.000000Z\t7\t6\t1\n" +
                        "1970-01-01T00:00:08.000000Z\t8\t7\t1\n" +
                        "1970-01-01T00:00:09.000000Z\t9\t8\t1\n" +
                        "1970-01-01T00:00:10.000000Z\t10\t9\t1\n");
    }

    @Test
    public void testAsOfJoinForSelectWithTimestamps() throws Exception {
        final String expected = "tag\thi\tlo\tts\tts1\n" +
                "AA\t315515118\t315515118\t1970-01-03T00:00:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                "BB\t-727724771\t-727724771\t1970-01-03T00:06:00.000000Z\t1970-01-03T00:06:00.000000Z\n" +
                "CC\t-948263339\t-948263339\t1970-01-03T00:12:00.000000Z\t1970-01-03T00:12:00.000000Z\n" +
                "CC\t592859671\t592859671\t1970-01-03T00:18:00.000000Z\t1970-01-03T00:18:00.000000Z\n" +
                "AA\t-847531048\t-847531048\t1970-01-03T00:24:00.000000Z\t1970-01-03T00:24:00.000000Z\n" +
                "BB\t-2041844972\t-2041844972\t1970-01-03T00:30:00.000000Z\t1970-01-03T00:30:00.000000Z\n" +
                "BB\t-1575378703\t-1575378703\t1970-01-03T00:36:00.000000Z\t1970-01-03T00:36:00.000000Z\n" +
                "BB\t1545253512\t1545253512\t1970-01-03T00:42:00.000000Z\t1970-01-03T00:42:00.000000Z\n" +
                "AA\t1573662097\t1573662097\t1970-01-03T00:48:00.000000Z\t1970-01-03T00:48:00.000000Z\n" +
                "AA\t339631474\t339631474\t1970-01-03T00:54:00.000000Z\t1970-01-03T00:54:00.000000Z\n";

        assertQuery13(
                "tag\thi\tlo\tts\tts1\n",
                "select a.tag, a.seq hi, b.seq lo,  a.ts, b.ts from tab a asof join tab b on (tag)",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false,
                true
        );
    }

    @Test
    public void testAsOfJoinForSelectWithoutTimestamp() throws Exception {
        final String expected = "tag\thi\tlo\n" +
                "AA\t315515118\t315515118\n" +
                "BB\t-727724771\t-727724771\n" +
                "CC\t-948263339\t-948263339\n" +
                "CC\t592859671\t592859671\n" +
                "AA\t-847531048\t-847531048\n" +
                "BB\t-2041844972\t-2041844972\n" +
                "BB\t-1575378703\t-1575378703\n" +
                "BB\t1545253512\t1545253512\n" +
                "AA\t1573662097\t1573662097\n" +
                "AA\t339631474\t339631474\n";

        assertQuery13(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false,
                true
        );
    }

    @Test
    public void testAsOfJoinForSelectWithoutTimestampAndWithWhereStatementAsOuter() throws Exception {
        final String expected = "hi\tlo\n" +
                "2\t1\n" +
                "3\t2\n" +
                "4\t3\n" +
                "5\t4\n" +
                "6\t5\n" +
                "7\t6\n" +
                "8\t7\n" +
                "9\t8\n" +
                "10\t9\n" +
                "11\t10\n" +
                "12\t11\n" +
                "13\t12\n" +
                "14\t13\n" +
                "15\t14\n" +
                "16\t15\n" +
                "17\t16\n" +
                "18\t17\n" +
                "19\t18\n" +
                "20\t19\n" +
                "21\t20\n" +
                "22\t21\n" +
                "23\t22\n" +
                "24\t23\n" +
                "25\t24\n" +
                "26\t25\n" +
                "27\t26\n" +
                "28\t27\n" +
                "29\t28\n" +
                "30\t29\n";
        assertQuery(
                "hi\tlo\n",
                "(select a.seq hi, b.seq lo from test a lt join test b) where lo != NaN",
                "create table test(seq long, ts timestamp) timestamp(ts)",
                null,
                "insert into test select x, cast(x+10 as timestamp) from (select x, rnd_double() rnd from long_sequence(30)) where rnd<0.9999)",
                expected,
                false
        );
    }

    @Test
    public void testAsOfJoinForSelectWithoutTimestampAndWithWhereStatementV2() throws Exception {
        final String expected = "tag\thi\tlo\n";
        assertQuery(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag) where b.seq < a.seq",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false
        );
    }

    @Test
    public void testAsOfJoinNoAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            // ASKS
            compiler.compile(
                    "create table asks(ask int, ts timestamp) timestamp(ts) partition by none",
                    sqlExecutionContext
            );
            executeInsert("insert into asks values(100, 0)");
            executeInsert("insert into asks values(101, 2);");
            executeInsert("insert into asks values(102, 4);");

            // BIDS
            compiler.compile(
                    "create table bids(bid int, ts timestamp) timestamp(ts) partition by none",
                    sqlExecutionContext
            );
            executeInsert("insert into bids values(101, 1);");
            executeInsert("insert into bids values(102, 3);");
            executeInsert("insert into bids values(103, 5);");

            String query =
                    "SELECT \n" +
                            "    b.timebid timebid,\n" +
                            "    a.timeask timeask, \n" +
                            "    b.b b, \n" +
                            "    a.a a\n" +
                            "FROM (select b.bid b, b.ts timebid from bids b) b \n" +
                            "    ASOF JOIN\n" +
                            "(select a.ask a, a.ts timeask from asks a) a\n" +
                            "WHERE (b.timebid != a.timeask);";

            String expected = "timebid\ttimeask\tb\ta\n" +
                    "1970-01-01T00:00:00.000001Z\t1970-01-01T00:00:00.000000Z\t101\t100\n" +
                    "1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000002Z\t102\t101\n" +
                    "1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000004Z\t103\t102\n";

            printSqlResult(expected, query, "timebid", false, false);
        });
    }

    @Test
    public void testAsOfJoinOnEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table t1 as (select x as id, cast(x as timestamp) ts from long_sequence(5)) timestamp(ts) partition by day;",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table t2 (id long, ts timestamp) timestamp(ts) partition by day;",
                    sqlExecutionContext
            );

            final String query = "SELECT * FROM t1 \n" +
                    "ASOF JOIN t2 ON id;";
            final String expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:00.000001Z\tNaN\t\n" +
                    "2\t1970-01-01T00:00:00.000002Z\tNaN\t\n" +
                    "3\t1970-01-01T00:00:00.000003Z\tNaN\t\n" +
                    "4\t1970-01-01T00:00:00.000004Z\tNaN\t\n" +
                    "5\t1970-01-01T00:00:00.000005Z\tNaN\t\n";
            printSqlResult(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testExplicitTimestampIsNotNecessaryWhenAsofJoiningExplicitlyOrderedTables() throws Exception {
        testExplicitTimestampIsNotNecessaryWhenJoining("asof join", "ts");
    }

    @Test
    public void testExplicitTimestampIsNotNecessaryWhenLtJoiningExplicitlyOrderedTables() throws Exception {
        testExplicitTimestampIsNotNecessaryWhenJoining("lt join", "ts");
    }

    @Test
    public void testExplicitTimestampIsNotNecessaryWhenSpliceJoiningExplicitlyOrderedTables() throws Exception {
        testExplicitTimestampIsNotNecessaryWhenJoining("splice join", null);
    }

    @Test
    public void testFullAsOfJoinDoesNotConvertSymbolKeyToString() throws Exception {
        testFullJoinDoesNotConvertSymbolKeyToString("asof join");
    }

    @Test
    public void testFullLtJoinDoesNotConvertSymbolKeyToString() throws Exception {
        testFullJoinDoesNotConvertSymbolKeyToString("lt join");
    }

    @Test
    public void testIssue2976() throws Exception {
        assertMemoryLeak(() -> {
            compiler.setFullFatJoins(true);
            compile("CREATE TABLE 'tests' (\n" +
                    "  Ticker SYMBOL capacity 256 CACHE,\n" +
                    "  ts timestamp\n" +
                    ") timestamp (ts) PARTITION BY MONTH");
            compile("INSERT INTO tests VALUES " +
                    "('AAPL', '2000')," +
                    "('AAPL', '2001')," +
                    "('AAPL', '2002')," +
                    "('AAPL', '2003')," +
                    "('AAPL', '2004')," +
                    "('AAPL', '2005')"
            );

            String query = "SELECT * " +
                    "FROM tests t0 " +
                    "LT JOIN (" +
                    "   SELECT * " +
                    "   FROM tests t1 " +
                    "   LT JOIN (" +
                    "       SELECT * " +
                    "       FROM tests t2 " +
                    "       LT JOIN (" +
                    "           SELECT * FROM tests t3" +
                    "       ) ON (Ticker)" +
                    "   ) ON (Ticker)" +
                    ") ON (Ticker)";
            String expected = "Ticker\tts\tTicker1\tts1\tTicker11\tts11\tTicker111\tts111\n" +
                    "AAPL\t2000-01-01T00:00:00.000000Z\t\t\t\t\t\t\n" +
                    "AAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\t\t\t\t\n" +
                    "AAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\t\t\n" +
                    "AAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\n" +
                    "AAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\n" +
                    "AAPL\t2005-01-01T00:00:00.000000Z\tAAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\n";
            assertQuery(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testLtJoin() throws Exception {
        final String expected = "tag\thi\tlo\tts\tts1\n" +
                "AA\t315515118\tNaN\t1970-01-03T00:00:00.000000Z\t\n" +
                "BB\t-727724771\tNaN\t1970-01-03T00:06:00.000000Z\t\n" +
                "CC\t-948263339\tNaN\t1970-01-03T00:12:00.000000Z\t\n" +
                "CC\t592859671\t-948263339\t1970-01-03T00:18:00.000000Z\t1970-01-03T00:12:00.000000Z\n" +
                "AA\t-847531048\t315515118\t1970-01-03T00:24:00.000000Z\t1970-01-03T00:00:00.000000Z\n" +
                "BB\t-2041844972\t-727724771\t1970-01-03T00:30:00.000000Z\t1970-01-03T00:06:00.000000Z\n" +
                "BB\t-1575378703\t-2041844972\t1970-01-03T00:36:00.000000Z\t1970-01-03T00:30:00.000000Z\n" +
                "BB\t1545253512\t-1575378703\t1970-01-03T00:42:00.000000Z\t1970-01-03T00:36:00.000000Z\n" +
                "AA\t1573662097\t-847531048\t1970-01-03T00:48:00.000000Z\t1970-01-03T00:24:00.000000Z\n" +
                "AA\t339631474\t1573662097\t1970-01-03T00:54:00.000000Z\t1970-01-03T00:48:00.000000Z\n";

        assertQuery13(
                "tag\thi\tlo\tts\tts1\n",
                "select a.tag, a.seq hi, b.seq lo , a.ts, b.ts from tab a lt join tab b on (tag)",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false,
                true
        );
    }

    @Test
    public void testLtJoin2TablesKeyed() throws Exception {
        assertMemoryLeak(() -> {
            //tabY
            compiler.compile("create table tabY (tag symbol, x long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            executeInsert("insert into tabY values ('A', 1, 10000)");
            executeInsert("insert into tabY values ('A', 2, 20000)");
            executeInsert("insert into tabY values ('A', 3, 30000)");
            executeInsert("insert into tabY values ('B', 1, 30000)");
            executeInsert("insert into tabY values ('B', 2, 40000)");
            executeInsert("insert into tabY values ('B', 3, 50000)");
            //tabZ
            compiler.compile("create table tabZ (tag symbol, x long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            executeInsert("insert into tabZ values ('B', 1, 10000)");
            executeInsert("insert into tabZ values ('B', 2, 20000)");
            executeInsert("insert into tabZ values ('B', 3, 30000)");
            executeInsert("insert into tabZ values ('A', 3, 30000)");
            executeInsert("insert into tabZ values ('A', 6, 40000)");
            executeInsert("insert into tabZ values ('A', 7, 50000)");
            //check tables
            String ex = "tag\tx\tts\n" +
                    "A\t1\t1970-01-01T00:00:00.010000Z\n" +
                    "A\t2\t1970-01-01T00:00:00.020000Z\n" +
                    "A\t3\t1970-01-01T00:00:00.030000Z\n" +
                    "B\t1\t1970-01-01T00:00:00.030000Z\n" +
                    "B\t2\t1970-01-01T00:00:00.040000Z\n" +
                    "B\t3\t1970-01-01T00:00:00.050000Z\n";
            printSqlResult(ex, "tabY", "ts", true, true);
            ex = "tag\tx\tts\n" +
                    "B\t1\t1970-01-01T00:00:00.010000Z\n" +
                    "B\t2\t1970-01-01T00:00:00.020000Z\n" +
                    "B\t3\t1970-01-01T00:00:00.030000Z\n" +
                    "A\t3\t1970-01-01T00:00:00.030000Z\n" +
                    "A\t6\t1970-01-01T00:00:00.040000Z\n" +
                    "A\t7\t1970-01-01T00:00:00.050000Z\n";
            printSqlResult(ex, "tabZ", "ts", true, true);
            // test
            ex = "tag\thi\tlo\n" +
                    "A\t1\tNaN\n" +
                    "A\t2\tNaN\n" +
                    "A\t3\tNaN\n" +
                    "B\t1\t2\n" +
                    "B\t2\t3\n" +
                    "B\t3\t3\n";
            String query = "select a.tag, a.x hi, b.x lo from tabY a lt join tabZ b on (tag) ";
            printSqlResult(ex, query, null, false, true);
        });
    }

    @Test
    public void testLtJoinForEqTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile("create table tank(ts timestamp, SequenceNumber int) timestamp(ts)", sqlExecutionContext);
            executeInsert("insert into tank values('2021-07-26T02:36:02.566000Z',1)");
            executeInsert("insert into tank values('2021-07-26T02:36:03.094000Z',2)");
            executeInsert("insert into tank values('2021-07-26T02:36:03.097000Z',3)");
            executeInsert("insert into tank values('2021-07-26T02:36:03.097000Z',4)");
            executeInsert("insert into tank values('2021-07-26T02:36:03.097000Z',5)");
            executeInsert("insert into tank values('2021-07-26T02:36:03.097000Z',6)");
            executeInsert("insert into tank values('2021-07-26T02:36:03.098000Z',7)");
            executeInsert("insert into tank values('2021-07-26T02:36:03.098000Z',8)");

            String expected = "ts\tSequenceNumber\tSequenceNumber1\tcolumn\n" +
                    "2021-07-26T02:36:02.566000Z\t1\tNaN\tNaN\n" +
                    "2021-07-26T02:36:03.094000Z\t2\t1\t1\n" +
                    "2021-07-26T02:36:03.097000Z\t3\t2\t1\n" +
                    "2021-07-26T02:36:03.097000Z\t4\t2\t2\n" +
                    "2021-07-26T02:36:03.097000Z\t5\t2\t3\n" +
                    "2021-07-26T02:36:03.097000Z\t6\t2\t4\n" +
                    "2021-07-26T02:36:03.098000Z\t7\t6\t1\n" +
                    "2021-07-26T02:36:03.098000Z\t8\t6\t2\n";
            String query = "select w1.ts ts, w1.SequenceNumber, w2.SequenceNumber, w1.SequenceNumber - w2.SequenceNumber from tank w1 lt join tank w2";
            printSqlResult(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testLtJoinForSelectWithoutTimestampAndWithWhereStatement() throws Exception {
        final String expected = "hi\tlo\n" +
                "18116\t18114\n" +
                "48689\t48687\n" +
                "57275\t57273\n" +
                "63855\t63853\n" +
                "72763\t72761\n" +
                "87011\t87009\n" +
                "87113\t87111\n" +
                "91369\t91367\n";
        assertQuery(
                "hi\tlo\n",
                "(select a.seq hi, b.seq lo from test a lt join test b) where hi > lo + 1",
                "create table test(seq long, ts timestamp) timestamp(ts)",
                null,
                "insert into test select x, cast(x+10 as timestamp) from (select x, rnd_double() rnd from long_sequence(100000)) where rnd<0.9999",
                expected,
                false
        );
    }

    @Test
    public void testLtJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join y on y.sym2 = x.sym";

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t22.463\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                    "2\tgoogl\t29.92\t0.423\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z\n" +
                    "3\tmsft\t65.086\t0.456\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:32:00.000000Z\n" +
                    "4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                    "5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "6\tibm\t76.11\t0.9540000000000001\t2018-01-01T01:12:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "7\tmsft\t55.992000000000004\t0.545\t2018-01-01T01:24:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                    "8\tibm\t23.905\t0.9540000000000001\t2018-01-01T01:36:00.000000Z\t2018-01-01T00:56:00.000000Z\n" +
                    "9\tgoogl\t67.786\t0.198\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:00:00.000000Z\n" +
                    "10\tgoogl\t38.54\t0.198\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:00:00.000000Z\n";
            try {
                compiler.setFullFatJoins(true);

                compiler.compile(
                        "create table x as (" +
                                "select" +
                                " cast(x as int) i," +
                                " rnd_symbol('msft','ibm', 'googl') sym," +
                                " round(rnd_double(0)*100, 3) amt," +
                                " to_timestamp('2018-01', 'yyyy-MM') + x * 720000000 timestamp" +
                                " from long_sequence(10)" +
                                ") timestamp (timestamp)",
                        sqlExecutionContext
                );

                compiler.compile(
                        "create table y as (" +
                                "select cast(x as int) i," +
                                " rnd_symbol('msft','ibm', 'googl') sym2," +
                                " round(rnd_double(0), 3) price," +
                                " to_timestamp('2018-01', 'yyyy-MM') + x * 120000000 timestamp" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)",
                        sqlExecutionContext
                );

                assertQueryAndCache(expected, query, "timestamp", true);

                compiler.compile(
                        "insert into x select * from (" +
                                "select" +
                                " cast(x + 10 as int) i," +
                                " rnd_symbol('msft','ibm', 'googl') sym," +
                                " round(rnd_double(0)*100, 3) amt," +
                                " to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp" +
                                " from long_sequence(10)" +
                                ") timestamp(timestamp)",
                        sqlExecutionContext
                );

                compiler.compile(
                        "insert into y select * from (" +
                                "select" +
                                " cast(x + 30 as int) i," +
                                " rnd_symbol('msft','ibm', 'googl') sym2," +
                                " round(rnd_double(0), 3) price," +
                                " to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp" +
                                " from long_sequence(30)" +
                                ") timestamp(timestamp)",
                        sqlExecutionContext
                );

                assertQuery("i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                                "1\tmsft\t22.463\tNaN\t2018-01-01T00:12:00.000000Z\t\n" +
                                "2\tgoogl\t29.92\t0.423\t2018-01-01T00:24:00.000000Z\t2018-01-01T00:16:00.000000Z\n" +
                                "3\tmsft\t65.086\t0.456\t2018-01-01T00:36:00.000000Z\t2018-01-01T00:32:00.000000Z\n" +
                                "4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000Z\t2018-01-01T00:34:00.000000Z\n" +
                                "5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000Z\t2018-01-01T00:46:00.000000Z\n" +
                                "6\tibm\t76.11\t0.427\t2018-01-01T01:12:00.000000Z\t2018-01-01T01:10:00.000000Z\n" +
                                "7\tmsft\t55.992000000000004\t0.226\t2018-01-01T01:24:00.000000Z\t2018-01-01T01:16:00.000000Z\n" +
                                "8\tibm\t23.905\t0.029\t2018-01-01T01:36:00.000000Z\t2018-01-01T01:34:00.000000Z\n" +
                                "9\tgoogl\t67.786\t0.076\t2018-01-01T01:48:00.000000Z\t2018-01-01T01:46:00.000000Z\n" +
                                "10\tgoogl\t38.54\t0.339\t2018-01-01T02:00:00.000000Z\t2018-01-01T01:58:00.000000Z\n" +
                                "11\tmsft\t68.069\t0.051000000000000004\t2018-01-01T02:12:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                                "12\tmsft\t24.008\t0.051000000000000004\t2018-01-01T02:24:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                                "13\tgoogl\t94.559\t0.6900000000000001\t2018-01-01T02:36:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                                "14\tibm\t62.474000000000004\t0.068\t2018-01-01T02:48:00.000000Z\t2018-01-01T01:40:00.000000Z\n" +
                                "15\tmsft\t39.017\t0.051000000000000004\t2018-01-01T03:00:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                                "16\tgoogl\t10.643\t0.6900000000000001\t2018-01-01T03:12:00.000000Z\t2018-01-01T02:00:00.000000Z\n" +
                                "17\tmsft\t7.246\t0.051000000000000004\t2018-01-01T03:24:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                                "18\tmsft\t36.798\t0.051000000000000004\t2018-01-01T03:36:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                                "19\tmsft\t66.98\t0.051000000000000004\t2018-01-01T03:48:00.000000Z\t2018-01-01T01:50:00.000000Z\n" +
                                "20\tgoogl\t26.369\t0.6900000000000001\t2018-01-01T04:00:00.000000Z\t2018-01-01T02:00:00.000000Z\n",
                        query,
                        "timestamp",
                        false,
                        true


                );
            } finally {
                compiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testLtJoinNoAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            // ASKS
            compiler.compile(
                    "create table asks(ask int, ts timestamp) timestamp(ts) partition by none",
                    sqlExecutionContext
            );
            executeInsert("insert into asks values(100, 0)");
            executeInsert("insert into asks values(101, 3);");
            executeInsert("insert into asks values(102, 4);");

            // BIDS
            compiler.compile(
                    "create table bids(bid int, ts timestamp) timestamp(ts) partition by none",
                    sqlExecutionContext
            );
            executeInsert("insert into bids values(101, 0);");
            executeInsert("insert into bids values(102, 3);");
            executeInsert("insert into bids values(103, 5);");

            String query =
                    "SELECT \n" +
                            "    b.timebid timebid,\n" +
                            "    a.timeask timeask, \n" +
                            "    b.b b, \n" +
                            "    a.a a\n" +
                            "FROM (select b.bid b, b.ts timebid from bids b) b \n" +
                            "    LT JOIN\n" +
                            "(select a.ask a, a.ts timeask from asks a) a\n" +
                            "WHERE (b.timebid != a.timeask);";

            String expected = "timebid\ttimeask\tb\ta\n" +
                    "1970-01-01T00:00:00.000000Z\t\t101\tNaN\n" +
                    "1970-01-01T00:00:00.000003Z\t1970-01-01T00:00:00.000000Z\t102\t100\n" +
                    "1970-01-01T00:00:00.000005Z\t1970-01-01T00:00:00.000004Z\t103\t102\n";

            printSqlResult(expected, query, "timebid", false, false);
        });
    }

    //select a.seq hi, b.seq lo from tab a lt join b where hi > lo + 1
    @Test
    public void testLtJoinNoTimestamp() throws Exception {
        final String expected = "tag\thi\tlo\n" +
                "AA\t315515118\tNaN\n" +
                "BB\t-727724771\tNaN\n" +
                "CC\t-948263339\tNaN\n" +
                "CC\t592859671\t-948263339\n" +
                "AA\t-847531048\t315515118\n" +
                "BB\t-2041844972\t-727724771\n" +
                "BB\t-1575378703\t-2041844972\n" +
                "BB\t1545253512\t-1575378703\n" +
                "AA\t1573662097\t-847531048\n" +
                "AA\t339631474\t1573662097\n";

        assertQuery13(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a lt join tab b on (tag)",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false,
                true
        );
    }

    @Test
    public void testLtJoinOnCompositeSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            compiler.setFullFatJoins(true);
            // stock and exchange are composite keys
            // rating is also a symbol, but not used in a join key
            compile("CREATE TABLE bids (stock SYMBOL, exchange SYMBOL, ts TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            compile("CREATE TABLE asks (stock SYMBOL, exchange SYMBOL, ts TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");

            compile("INSERT INTO bids VALUES " +
                    "('AAPL', 'NASDAQ', '2000-01-01T00:00:00.000000Z', 1, 'GOOD')," +
                    "('AAPL', 'NASDAQ', '2001-01-01T00:00:00.000000Z', 2, 'GOOD')," +
                    "('AAPL', 'NASDAQ', '2002-01-01T00:00:00.000000Z', 3, 'SCAM')," +
                    "('AAPL', 'LSE', '2000-01-01T00:00:00.000000Z', 4, 'SCAM')," +
                    "('AAPL', 'LSE', '2001-01-01T00:00:00.000000Z', 5, 'EXCELLENT')," +
                    "('AAPL', 'LSE', '2002-01-01T00:00:00.000000Z', 6, 'SCAM')," +
                    "('MSFT', 'NASDAQ', '2000-01-01T00:00:00.000000Z', 7, 'GOOD')," +
                    "('MSFT', 'NASDAQ', '2001-01-01T00:00:00.000000Z', 8, 'GOOD')," +
                    "('MSFT', 'NASDAQ', '2002-01-01T00:00:00.000000Z', 9, 'SCAM')," +
                    "('MSFT', 'LSE', '2000-01-01T00:00:00.000000Z', 10, 'UNKNOWN')," +
                    "('MSFT', 'LSE', '2001-01-01T00:00:00.000000Z', 11, 'GOOD')"
            );

            compile("INSERT INTO asks VALUES " +
                    "('AAPL', 'NASDAQ', '2000-01-01T00:00:00.000000Z', 1, 'GOOD')," +
                    "('AAPL', 'NASDAQ', '2001-01-01T00:00:00.000000Z', 2, 'EXCELLENT')," +
                    "('AAPL', 'NASDAQ', '2002-01-01T00:00:00.000000Z', 3, 'EXCELLENT')," +
                    "('AAPL', 'LSE', '2000-01-01T00:00:00.000000Z', 4, 'EXCELLENT')," +
                    "('AAPL', 'LSE', '2001-01-01T00:00:00.000000Z', 5, 'EXCELLENT')," +
                    "('AAPL', 'LSE', '2002-01-01T00:00:00.000000Z', 6, 'SCAM')," +
                    "('MSFT', 'NASDAQ', '2000-01-01T00:00:00.000000Z', 7, 'EXCELLENT')," +
                    "('MSFT', 'NASDAQ', '2001-01-01T00:00:00.000000Z', 8, 'GOOD')," +
                    "('MSFT', 'NASDAQ', '2002-01-01T00:00:00.000000Z', 9, 'EXCELLENT')," +
                    "('MSFT', 'LSE', '2000-01-01T00:00:00.000000Z', 10, 'GOOD')," +
                    "('MSFT', 'LSE', '2001-01-01T00:00:00.000000Z', 11, 'SCAM')"
            );

            String query = "SELECT * FROM bids LT JOIN asks ON (stock, exchange)";
            String expected = "stock\texchange\tts\ti\trating\tstock1\texchange1\tts1\ti1\trating1\n" +
                    "AAPL\tNASDAQ\t2000-01-01T00:00:00.000000Z\t1\tGOOD\t\t\t\tNaN\t\n" +
                    "AAPL\tLSE\t2000-01-01T00:00:00.000000Z\t4\tSCAM\t\t\t\tNaN\t\n" +
                    "MSFT\tNASDAQ\t2000-01-01T00:00:00.000000Z\t7\tGOOD\t\t\t\tNaN\t\n" +
                    "MSFT\tLSE\t2000-01-01T00:00:00.000000Z\t10\tUNKNOWN\t\t\t\tNaN\t\n" +
                    "AAPL\tNASDAQ\t2001-01-01T00:00:00.000000Z\t2\tGOOD\tAAPL\tNASDAQ\t2000-01-01T00:00:00.000000Z\t1\tGOOD\n" +
                    "AAPL\tLSE\t2001-01-01T00:00:00.000000Z\t5\tEXCELLENT\tAAPL\tLSE\t2000-01-01T00:00:00.000000Z\t4\tEXCELLENT\n" +
                    "MSFT\tNASDAQ\t2001-01-01T00:00:00.000000Z\t8\tGOOD\tMSFT\tNASDAQ\t2000-01-01T00:00:00.000000Z\t7\tEXCELLENT\n" +
                    "MSFT\tLSE\t2001-01-01T00:00:00.000000Z\t11\tGOOD\tMSFT\tLSE\t2000-01-01T00:00:00.000000Z\t10\tGOOD\n" +
                    "AAPL\tLSE\t2002-01-01T00:00:00.000000Z\t6\tSCAM\tAAPL\tLSE\t2001-01-01T00:00:00.000000Z\t5\tEXCELLENT\n" +
                    "MSFT\tNASDAQ\t2002-01-01T00:00:00.000000Z\t9\tSCAM\tMSFT\tNASDAQ\t2001-01-01T00:00:00.000000Z\t8\tGOOD\n" +
                    "AAPL\tNASDAQ\t2002-01-01T00:00:00.000000Z\t3\tSCAM\tAAPL\tNASDAQ\t2001-01-01T00:00:00.000000Z\t2\tEXCELLENT\n";
            assertQuery(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testLtJoinOnEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            compiler.compile(
                    "create table t1 as (select x as id, cast(x as timestamp) ts from long_sequence(5)) timestamp(ts) partition by day;",
                    sqlExecutionContext
            );
            compiler.compile(
                    "create table t2 (id long, ts timestamp) timestamp(ts) partition by day;",
                    sqlExecutionContext
            );

            final String query = "SELECT * FROM t1 \n" +
                    "LT JOIN t2 ON id;";
            final String expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:00.000001Z\tNaN\t\n" +
                    "2\t1970-01-01T00:00:00.000002Z\tNaN\t\n" +
                    "3\t1970-01-01T00:00:00.000003Z\tNaN\t\n" +
                    "4\t1970-01-01T00:00:00.000004Z\tNaN\t\n" +
                    "5\t1970-01-01T00:00:00.000005Z\tNaN\t\n";
            printSqlResult(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testLtJoinOnRandomlyGeneratedColumn() throws Exception {
        final String expected = "tag\thi\tlo\n" +
                "CC\t592859671\t-948263339\n" +
                "BB\t-1575378703\t-2041844972\n" +
                "BB\t1545253512\t-1575378703\n" +
                "AA\t1573662097\t1545253512\n";

        assertQuery(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a lt join tab b where a.seq > b.seq + 1",
                "create table tab (\n" +
                        "    tag symbol index,\n" +
                        "    seq int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                        "        rnd_int() seq, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                false
        );
    }

    @Test
    public void testLtJoinOnSymbolWithSyntheticMasterSymbol() throws Exception {
        assertMemoryLeak(() -> {
            compiler.setFullFatJoins(true);

            // create a master table - without a symbol column
            compile("create table taba as (select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 10000000000000L) as ts from long_sequence(5)) timestamp(ts)");

            // create a slave table - with a symbol column, with timestamps 1 microsecond before master timestamps
            compile("create table tabb as (select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss') - 1, 10000000000000L) as ts, rnd_symbol('A', 'B', 'C') as sym from long_sequence(5)) timestamp(ts)");

            // use a CTE to amend the master table with a synthetic symbol column
            String query = "with s as (\n" +
                    "  select cast (s as symbol) synthetic_sym, ts\n" +
                    "  from (\n" +
                    "      SELECT\n" +
                    "        CASE\n" +
                    "          WHEN ts % 3 = 0 THEN 'A'\n" +
                    "          WHEN ts % 3 = 1 THEN 'B'\n" +
                    "          ELSE 'C'\n" +
                    "        END as s, *\n" +
                    "      FROM taba\n" +
                    "    )\n" +
                    "  )\n" +
                    "select * from s\n" +
                    "lt join tabb on (s.synthetic_sym = tabb.sym);";
            String expected = "synthetic_sym\tts\tts1\tsym\n" +
                    "A\t2019-10-17T00:00:00.000000Z\t2019-10-16T23:59:59.999999Z\tA\n" +
                    "B\t2020-02-09T17:46:40.000000Z\t\t\n" +
                    "C\t2020-06-04T11:33:20.000000Z\t\t\n" +
                    "A\t2020-09-28T05:20:00.000000Z\t2020-02-09T17:46:39.999999Z\tA\n" +
                    "B\t2021-01-21T23:06:40.000000Z\t2020-06-04T11:33:19.999999Z\tB\n";
            assertQuery(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testLtJoinOnSymbolsDifferentIDs() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table x (s symbol, xi int, xts timestamp) timestamp(xts)");
            compile("create table y (s symbol, yi int, yts timestamp) timestamp(yts)");
            compile("insert into x values ('a', 0, '2000')");
            compile("insert into x values ('b', 1, '2001')");
            compile("insert into x values ('c', 2, '2001')");

            compile("insert into y values ('c', 0, '1990')");
            compile("insert into y values ('d', 1, '1991')");
            compile("insert into y values ('a', 2, '1992')");
            compile("insert into y values ('a', 3, '1993')");

            String query = "select * from x LT JOIN y on (s)";
            String expected = "s\txi\txts\ts1\tyi\tyts\n" +
                    "a\t0\t2000-01-01T00:00:00.000000Z\ta\t3\t1993-01-01T00:00:00.000000Z\n" +
                    "b\t1\t2001-01-01T00:00:00.000000Z\t\tNaN\t\n" +
                    "c\t2\t2001-01-01T00:00:00.000000Z\tc\t0\t1990-01-01T00:00:00.000000Z\n";

            assertQuery(expected, query, "xts", false, true);
        });
    }

    @Test
    public void testLtJoinOneTableKeyed() throws Exception {
        assertMemoryLeak(() -> {
            //tabY
            compiler.compile("create table tabY (tag symbol, x long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            executeInsert("insert into tabY values ('A', 1, 10000)");
            executeInsert("insert into tabY values ('A', 2, 20000)");
            executeInsert("insert into tabY values ('A', 3, 30000)");
            executeInsert("insert into tabY values ('B', 1, 30000)");
            executeInsert("insert into tabY values ('B', 2, 40000)");
            executeInsert("insert into tabY values ('B', 3, 50000)");
            //check tables
            String ex = "tag\tx\tts\n" +
                    "A\t1\t1970-01-01T00:00:00.010000Z\n" +
                    "A\t2\t1970-01-01T00:00:00.020000Z\n" +
                    "A\t3\t1970-01-01T00:00:00.030000Z\n" +
                    "B\t1\t1970-01-01T00:00:00.030000Z\n" +
                    "B\t2\t1970-01-01T00:00:00.040000Z\n" +
                    "B\t3\t1970-01-01T00:00:00.050000Z\n";
            printSqlResult(ex, "tabY", "ts", true, true);
            // test
            ex = "tag\thi\tlo\n" +
                    "A\t1\tNaN\n" +
                    "A\t2\t1\n" +
                    "A\t3\t2\n" +
                    "B\t1\tNaN\n" +
                    "B\t2\t1\n" +
                    "B\t3\t2\n";
            String query = "select a.tag, a.x hi, b.x lo from tabY a lt join tabY b on (tag) ";
            printSqlResult(ex, query, null, false, true);
        });
    }

    @Test
    public void testLtJoinOneTableKeyedV2() throws Exception {
        assertMemoryLeak(() -> {
            //tabY
            compiler.compile("create table tabY (tag symbol, x long, ts timestamp) timestamp(ts)", sqlExecutionContext);
            executeInsert("insert into tabY values ('A', 1, 10000)");
            executeInsert("insert into tabY values ('A', 2, 20000)");
            executeInsert("insert into tabY values ('A', 3, 30000)");
            executeInsert("insert into tabY values ('B', 1, 40000)");
            executeInsert("insert into tabY values ('B', 2, 50000)");
            executeInsert("insert into tabY values ('B', 3, 60000)");
            //check tables
            String ex = "tag\tx\tts\n" +
                    "A\t1\t1970-01-01T00:00:00.010000Z\n" +
                    "A\t2\t1970-01-01T00:00:00.020000Z\n" +
                    "A\t3\t1970-01-01T00:00:00.030000Z\n" +
                    "B\t1\t1970-01-01T00:00:00.040000Z\n" +
                    "B\t2\t1970-01-01T00:00:00.050000Z\n" +
                    "B\t3\t1970-01-01T00:00:00.060000Z\n";
            printSqlResult(ex, "tabY", "ts", true, true);
            // test
            ex = "tag\thi\tlo\n" +
                    "A\t1\tNaN\n" +
                    "A\t2\t1\n" +
                    "A\t3\t2\n" +
                    "B\t1\tNaN\n" +
                    "B\t2\t1\n" +
                    "B\t3\t2\n";
            String query = "select a.tag, a.x hi, b.x lo from tabY a lt join tabY b on (tag) ";
            printSqlResult(ex, query, null, false, true);
        });
    }

    @Test
    public void testLtJoinSequenceGap() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            compiler.compile("create table tab as " +
                    "(" +
                    "select " +
                    "rnd_symbol('AA', 'BB') tag," +
                    " x, " +
                    " timestamp_sequence(0, 10000) ts" +
                    " from" +
                    " long_sequence(20)" +
                    ") timestamp(ts) partition by DAY", sqlExecutionContext);
            //insert
            executeInsert("insert into tab values ('CC', 24, 210000)");
            executeInsert("insert into tab values ('CC', 25, 220000)");
            String ex = "tag\tx\tts\n" +
                    "AA\t1\t1970-01-01T00:00:00.000000Z\n" +
                    "AA\t2\t1970-01-01T00:00:00.010000Z\n" +
                    "BB\t3\t1970-01-01T00:00:00.020000Z\n" +
                    "BB\t4\t1970-01-01T00:00:00.030000Z\n" +
                    "BB\t5\t1970-01-01T00:00:00.040000Z\n" +
                    "BB\t6\t1970-01-01T00:00:00.050000Z\n" +
                    "AA\t7\t1970-01-01T00:00:00.060000Z\n" +
                    "BB\t8\t1970-01-01T00:00:00.070000Z\n" +
                    "AA\t9\t1970-01-01T00:00:00.080000Z\n" +
                    "AA\t10\t1970-01-01T00:00:00.090000Z\n" +
                    "AA\t11\t1970-01-01T00:00:00.100000Z\n" +
                    "AA\t12\t1970-01-01T00:00:00.110000Z\n" +
                    "AA\t13\t1970-01-01T00:00:00.120000Z\n" +
                    "BB\t14\t1970-01-01T00:00:00.130000Z\n" +
                    "BB\t15\t1970-01-01T00:00:00.140000Z\n" +
                    "AA\t16\t1970-01-01T00:00:00.150000Z\n" +
                    "AA\t17\t1970-01-01T00:00:00.160000Z\n" +
                    "BB\t18\t1970-01-01T00:00:00.170000Z\n" +
                    "BB\t19\t1970-01-01T00:00:00.180000Z\n" +
                    "AA\t20\t1970-01-01T00:00:00.190000Z\n" +
                    "CC\t24\t1970-01-01T00:00:00.210000Z\n" +
                    "CC\t25\t1970-01-01T00:00:00.220000Z\n";
            String query = "tab";
            printSqlResult(ex, query, "ts", true, true);
            // test
            ex = "tag\thi\tlo\n" +
                    "CC\t24\t20\n";
            query = "select a.tag, a.x hi, b.x lo " +
                    "from tab a " +
                    "lt join tab b " +
                    "where a.x > b.x + 1";
            printSqlResult(ex, query, null, false, false);
        });
    }

    @Test
    public void testLtJoinSequenceGapOnKey() throws Exception {
        assertMemoryLeak(() -> {
            //create table
            compiler.compile("create table tab as " +
                    "(" +
                    "select " +
                    "rnd_symbol('AA', 'BB') tag," +
                    " x, " +
                    " timestamp_sequence(0, 10000) ts" +
                    " from" +
                    " long_sequence(20)" +
                    ") timestamp(ts) partition by DAY", sqlExecutionContext);
            //insert
            executeInsert("insert into tab values ('CC', 24, 210000)");
            executeInsert("insert into tab values ('CC', 25, 220000)");
            String ex = "tag\tx\tts\n" +
                    "AA\t1\t1970-01-01T00:00:00.000000Z\n" +
                    "AA\t2\t1970-01-01T00:00:00.010000Z\n" +
                    "BB\t3\t1970-01-01T00:00:00.020000Z\n" +
                    "BB\t4\t1970-01-01T00:00:00.030000Z\n" +
                    "BB\t5\t1970-01-01T00:00:00.040000Z\n" +
                    "BB\t6\t1970-01-01T00:00:00.050000Z\n" +
                    "AA\t7\t1970-01-01T00:00:00.060000Z\n" +
                    "BB\t8\t1970-01-01T00:00:00.070000Z\n" +
                    "AA\t9\t1970-01-01T00:00:00.080000Z\n" +
                    "AA\t10\t1970-01-01T00:00:00.090000Z\n" +
                    "AA\t11\t1970-01-01T00:00:00.100000Z\n" +
                    "AA\t12\t1970-01-01T00:00:00.110000Z\n" +
                    "AA\t13\t1970-01-01T00:00:00.120000Z\n" +
                    "BB\t14\t1970-01-01T00:00:00.130000Z\n" +
                    "BB\t15\t1970-01-01T00:00:00.140000Z\n" +
                    "AA\t16\t1970-01-01T00:00:00.150000Z\n" +
                    "AA\t17\t1970-01-01T00:00:00.160000Z\n" +
                    "BB\t18\t1970-01-01T00:00:00.170000Z\n" +
                    "BB\t19\t1970-01-01T00:00:00.180000Z\n" +
                    "AA\t20\t1970-01-01T00:00:00.190000Z\n" +
                    "CC\t24\t1970-01-01T00:00:00.210000Z\n" +
                    "CC\t25\t1970-01-01T00:00:00.220000Z\n";
            String query = "tab";
            printSqlResult(ex, query, "ts", true, true);
            // test
            ex = "tag\thi\tlo\n" +
                    "AA\t7\t2\n" +
                    "BB\t8\t6\n" +
                    "AA\t9\t7\n" +
                    "BB\t14\t8\n" +
                    "AA\t16\t13\n" +
                    "BB\t18\t15\n" +
                    "AA\t20\t17\n";
            query = "select a.tag, a.x hi, b.x lo from tab a lt join tab b on (tag)  where a.x > b.x + 1";
            printSqlResult(ex, query, null, false, false);
        });
    }

    @Test
    public void testNestedASOF_keySymbol() throws Exception {
        assertMemoryLeak(() -> {
            compiler.setFullFatJoins(true);
            compile("CREATE TABLE 'tests' (\n" +
                    "  Ticker SYMBOL capacity 256 CACHE,\n" +
                    "  ts timestamp\n" +
                    ") timestamp (ts) PARTITION BY MONTH");
            compile("insert into tests VALUES " +
                    "('AAPL', '2000')," +
                    "('AAPL', '2001')," +
                    "('AAPL', '2002')," +
                    "('AAPL', '2003')," +
                    "('AAPL', '2004')," +
                    "('AAPL', '2005')"
            );
            compile("insert into tests VALUES " +
                    "('QSTDB', '2003')," +
                    "('QSTDB', '2004')," +
                    "('QSTDB', '2005')," +
                    "('QSTDB', '2006')," +
                    "('QSTDB', '2007')," +
                    "('QSTDB', '2008')"
            );

            String query = "SELECT * " +
                    "FROM tests t0 " +
                    "ASOF JOIN (" +
                    "   SELECT * " +
                    "   FROM tests t1" +
                    "   ASOF JOIN (" +
                    "       SELECT * " +
                    "       FROM tests t2" +
                    "       ASOF JOIN (" +
                    "           SELECT * FROM tests t3" +
                    "       ) on (Ticker)" +
                    "   ) ON (Ticker)" +
                    ") ON (Ticker)";
            String expected = "Ticker\tts\tTicker1\tts1\tTicker11\tts11\tTicker111\tts111\n" +
                    "AAPL\t2000-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\n" +
                    "AAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\n" +
                    "AAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\n" +
                    "AAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2003-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\n" +
                    "AAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2004-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\n" +
                    "AAPL\t2005-01-01T00:00:00.000000Z\tAAPL\t2005-01-01T00:00:00.000000Z\tAAPL\t2005-01-01T00:00:00.000000Z\tAAPL\t2005-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2006-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2007-01-01T00:00:00.000000Z\tQSTDB\t2007-01-01T00:00:00.000000Z\tQSTDB\t2007-01-01T00:00:00.000000Z\tQSTDB\t2007-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2008-01-01T00:00:00.000000Z\tQSTDB\t2008-01-01T00:00:00.000000Z\tQSTDB\t2008-01-01T00:00:00.000000Z\tQSTDB\t2008-01-01T00:00:00.000000Z\n";
            assertQuery(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testNestedLT_keySymbol() throws Exception {
        assertMemoryLeak(() -> {
            compiler.setFullFatJoins(true);
            compile("CREATE TABLE 'tests' (\n" +
                    "  Ticker SYMBOL capacity 256 CACHE,\n" +
                    "  ts timestamp\n" +
                    ") timestamp (ts) PARTITION BY MONTH");
            compile("insert into tests VALUES " +
                    "('AAPL', '2000')," +
                    "('AAPL', '2001')," +
                    "('AAPL', '2002')," +
                    "('AAPL', '2003')," +
                    "('AAPL', '2004')," +
                    "('AAPL', '2005')"
            );
            compile("insert into tests VALUES " +
                    "('QSTDB', '2003')," +
                    "('QSTDB', '2004')," +
                    "('QSTDB', '2005')," +
                    "('QSTDB', '2006')," +
                    "('QSTDB', '2007')," +
                    "('QSTDB', '2008')"
            );

            String query = "SELECT * " +
                    "FROM tests t0 " +
                    "LT JOIN (" +
                    "   SELECT * " +
                    "   FROM tests t1 " +
                    "   LT JOIN (" +
                    "       SELECT * " +
                    "       FROM tests t2 " +
                    "       LT JOIN (" +
                    "           SELECT * FROM tests t3" +
                    "       ) on (Ticker)" +
                    "   ) ON (Ticker)" +
                    ") ON (Ticker)";
            String expected = "Ticker\tts\tTicker1\tts1\tTicker11\tts11\tTicker111\tts111\n" +
                    "AAPL\t2000-01-01T00:00:00.000000Z\t\t\t\t\t\t\n" +
                    "AAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\t\t\t\t\n" +
                    "AAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\t\t\n" +
                    "AAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2003-01-01T00:00:00.000000Z\t\t\t\t\t\t\n" +
                    "AAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\t\t\t\t\n" +
                    "AAPL\t2005-01-01T00:00:00.000000Z\tAAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\t\t\n" +
                    "QSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2007-01-01T00:00:00.000000Z\tQSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\n" +
                    "QSTDB\t2008-01-01T00:00:00.000000Z\tQSTDB\t2007-01-01T00:00:00.000000Z\tQSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\n";
            assertQuery(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testNestedLT_keySymbol_moreColumns() throws Exception {
        assertMemoryLeak(() -> {
            compiler.setFullFatJoins(true);
            compile("CREATE TABLE 'tests' (\n" +
                    "  UnusedTag SYMBOL,\n" + // just filler to make the joining a bit more interesting
                    "  Ticker SYMBOL capacity 256 CACHE,\n" +
                    "  ts timestamp,\n" +
                    "  price int\n" +
                    ") timestamp (ts) PARTITION BY MONTH");
            compile("insert into tests VALUES " +
                    "('Whatever', 'AAPL', '2000', 0)," +
                    "('Whatever', 'AAPL', '2001', 1)," +
                    "('Whatever', 'AAPL', '2002', 2)," +
                    "('Whatever', 'AAPL', '2003', 3)," +
                    "('Whatever', 'AAPL', '2004', 4)," +
                    "('Whatever', 'AAPL', '2005', 5)"
            );
            compile("insert into tests VALUES " +
                    "('Whatever', 'QSTDB', '2003', 6)," +
                    "('Whatever', 'QSTDB', '2004', 7)," +
                    "('Whatever', 'QSTDB', '2005', 8)," +
                    "('Whatever', 'QSTDB', '2006', 9)," +
                    "('Whatever', 'QSTDB', '2007', 10)," +
                    "('Whatever', 'QSTDB', '2008', 11)"
            );

            String query = "SELECT t2unused, Ticker AS t0ticker, ts AS t0ts, t1ticker, t1ts, t2ticker, t2ts, t3ticker, t3ts \n" +
                    "FROM tests \n" +
                    "LT JOIN (\n" +
                    "    SELECT t2unused, Ticker AS t1ticker, UnusedTag AS t1unused, ts AS t1ts, t3unused, t2ticker, t2ts, t3ticker, t3ts \n" +
                    "    FROM tests \n" +
                    "    LT JOIN (\n" +
                    "        SELECT UnusedTag AS t2unused, Ticker AS t2ticker, t3unused, ts AS t2ts, t3ticker, t3ts \n" +
                    "        FROM tests \n" +
                    "        LT JOIN (\n" +
                    "            SELECT UnusedTag AS t3unused, Ticker AS t3ticker, ts AS t3ts FROM tests\n" +
                    "        ) t3 ON (ticker = t3.t3ticker)\n" +
                    "    ) t2 ON (Ticker = t2ticker)\n" +
                    ") t1 ON (Ticker = t1ticker)";

            String expected = "t2unused\tt0ticker\tt0ts\tt1ticker\tt1ts\tt2ticker\tt2ts\tt3ticker\tt3ts\n" +
                    "\tAAPL\t2000-01-01T00:00:00.000000Z\t\t\t\t\t\t\n" +
                    "\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\t\t\t\t\n" +
                    "Whatever\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\t\t\n" +
                    "Whatever\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\tAAPL\t2000-01-01T00:00:00.000000Z\n" +
                    "\tQSTDB\t2003-01-01T00:00:00.000000Z\t\t\t\t\t\t\n" +
                    "Whatever\tAAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\tAAPL\t2001-01-01T00:00:00.000000Z\n" +
                    "\tQSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\t\t\t\t\n" +
                    "Whatever\tAAPL\t2005-01-01T00:00:00.000000Z\tAAPL\t2004-01-01T00:00:00.000000Z\tAAPL\t2003-01-01T00:00:00.000000Z\tAAPL\t2002-01-01T00:00:00.000000Z\n" +
                    "Whatever\tQSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\t\t\n" +
                    "Whatever\tQSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\tQSTDB\t2003-01-01T00:00:00.000000Z\n" +
                    "Whatever\tQSTDB\t2007-01-01T00:00:00.000000Z\tQSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\tQSTDB\t2004-01-01T00:00:00.000000Z\n" +
                    "Whatever\tQSTDB\t2008-01-01T00:00:00.000000Z\tQSTDB\t2007-01-01T00:00:00.000000Z\tQSTDB\t2006-01-01T00:00:00.000000Z\tQSTDB\t2005-01-01T00:00:00.000000Z\n";
            assertQuery(expected, query, "t0ts", false, true);
        });
    }

    private void testExplicitTimestampIsNotNecessaryWhenJoining(String joinType, String timestamp) throws Exception {
        assertQuery("ts\ty\tts1\ty1\n",
                "select * from " +
                        "(select * from (select * from x where y = 10 order by ts desc limit 20) order by ts ) a " +
                        joinType +
                        "(select * from x order by ts limit 5) b",
                "create table x (ts timestamp, y int) timestamp(ts)",
                timestamp, false);
    }

    private void testFullJoinDoesNotConvertSymbolKeyToString(String joinType) throws Exception {
        assertMemoryLeak(() -> {
            compiler.setFullFatJoins(true);
            compile("create table tab_a (sym_a symbol, ts_a timestamp, s_a string) timestamp(ts_a) partition by DAY");
            compile("create table tab_b (sym_b symbol, ts_b timestamp, s_B string) timestamp(ts_b) partition by DAY");

            compile("insert into tab_a values " +
                    "('ABC', '2022-01-01T00:00:00.000000Z', 'foo')"
            );
            compile("insert into tab_b values " +
                    "('DCE', '2021-01-01T00:00:00.000000Z', 'bar')," + // first INSERT a row with DCE to make sure symbol table for tab_b differs from tab_a
                    "('ABC', '2021-01-01T00:00:00.000000Z', 'bar')"
            );

            String query = "select sym_a, sym_b from tab_a a " + joinType + " tab_b b on sym_a = sym_b";
            try (RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    io.questdb.cairo.sql.Record record = cursor.getRecord();
                    RecordMetadata metadata = factory.getMetadata();
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(ColumnType.SYMBOL, metadata.getColumnType(0));
                    Assert.assertEquals(ColumnType.SYMBOL, metadata.getColumnType(1));
                    CharSequence sym0 = record.getSym(0);
                    CharSequence sym1 = record.getSym(1);
                    TestUtils.assertEquals("ABC", sym0);
                    TestUtils.assertEquals("ABC", sym1);
                    Assert.assertFalse(cursor.hasNext());
                }
            }
        });
    }
}
