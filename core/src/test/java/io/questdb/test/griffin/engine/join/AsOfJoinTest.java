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

package io.questdb.test.griffin.engine.join;

import io.questdb.PropertyKey;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.jit.JitUtil;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.TestTimestampType;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class AsOfJoinTest extends AbstractCairoTest {
    private final TestTimestampType leftTableTimestampType;
    private final TestTimestampType rightTableTimestampType;

    public AsOfJoinTest() {
        Rnd rnd = TestUtils.generateRandom(LOG);
        this.leftTableTimestampType = TestUtils.getTimestampType(rnd);
        this.rightTableTimestampType = TestUtils.getTimestampType(rnd);
    }

    @Test
    public void testAsOfJoinAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("""
                            CREATE TABLE fx_rate (
                                ts #TIMESTAMP,
                                code SYMBOL CAPACITY 128 NOCACHE,
                                rate INT
                            ) timestamp(ts)
                            """,
                    leftTableTimestampType.getTypeName()
            );
            execute("INSERT INTO fx_rate values ('2022-10-05T04:00:00.000000Z', '1001', 10);");

            executeWithRewriteTimestamp("""
                            CREATE TABLE trades (
                                ts #TIMESTAMP,
                                price INT,
                                qty INT,
                                flag INT,
                                fx_rate_code SYMBOL CAPACITY 128 NOCACHE
                            ) timestamp(ts);
                            """,
                    rightTableTimestampType.getTypeName()
            );
            execute("INSERT INTO trades values ('2022-10-05T08:15:00.000000Z', 100, 500, 0, '1001');");
            execute("INSERT INTO trades values ('2022-10-05T08:16:00.000000Z', 100, 500, 1, '1001');");
            execute("INSERT INTO trades values ('2022-10-05T08:16:00.000000Z', 100, 500, 2, '1001');");

            String query = """
                    SELECT
                      SUM(CASE WHEN t.flag = 0 THEN 0.9 * (t.price * f.rate) ELSE 0.0 END),
                      SUM(CASE WHEN t.flag = 1 THEN 0.7 * (t.price * f.rate) ELSE 0.0 END),
                      SUM(CASE WHEN t.flag = 2 THEN 0.2 * (t.price * f.rate) ELSE 0.0 END)
                    FROM
                      trades t
                    ASOF JOIN fx_rate f on f.code = t.fx_rate_code
                    """;

            String expected = """
                    SUM\tSUM1\tSUM2
                    900.0\t700.0\t200.0
                    """;

            printSqlResult(expected, query, null, false, true);
        });
    }

    @Test
    public void testAsOfJoinCombinedWithInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table t1 as (select x as id, cast(x as #TIMESTAMP) ts from long_sequence(5)) timestamp(ts) partition by day;", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("create table t2 as (select x as id, cast(x as #TIMESTAMP) ts from long_sequence(5)) timestamp(ts) partition by day;", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("create table t3 (id long, ts #TIMESTAMP) timestamp(ts) partition by day;", rightTableTimestampType.getTypeName());

            final String query = """
                    SELECT *
                    FROM (
                      (t1 INNER JOIN t2 ON id)\s
                      ASOF JOIN t3 ON id
                    );""";
            final String expected = replaceTimestampSuffix("""
                    id\tts\tid1\tts1\tid2\tts2
                    1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\tnull\t
                    2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\tnull\t
                    3\t1970-01-01T00:00:00.000003Z\t3\t1970-01-01T00:00:00.000003Z\tnull\t
                    4\t1970-01-01T00:00:00.000004Z\t4\t1970-01-01T00:00:00.000004Z\tnull\t
                    5\t1970-01-01T00:00:00.000005Z\t5\t1970-01-01T00:00:00.000005Z\tnull\t
                    """, leftTableTimestampType.getTypeName());
            printSqlResult(expected, query, "ts", false, false);
        });
    }

    @Test
    public void testAsOfJoinDynamicTimestamp() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table positions2 as (" +
                            "select x, cast(x * 1000000L" + (leftTableTimestampType == TestTimestampType.NANO ? "*1000L" : "") + " as #TIMESTAMP) time from long_sequence(10)" +
                            ") timestamp(time)", leftTableTimestampType.getTypeName());

            assertSql(
                    replaceTimestampSuffix("""
                                    time\tx\tx1\tcolumn
                                    1970-01-01T00:00:01.000000Z\t1\tnull\tnull
                                    1970-01-01T00:00:02.000000Z\t2\t1\t1
                                    1970-01-01T00:00:03.000000Z\t3\t2\t1
                                    1970-01-01T00:00:04.000000Z\t4\t3\t1
                                    1970-01-01T00:00:05.000000Z\t5\t4\t1
                                    1970-01-01T00:00:06.000000Z\t6\t5\t1
                                    1970-01-01T00:00:07.000000Z\t7\t6\t1
                                    1970-01-01T00:00:08.000000Z\t8\t7\t1
                                    1970-01-01T00:00:09.000000Z\t9\t8\t1
                                    1970-01-01T00:00:10.000000Z\t10\t9\t1
                                    """,
                            leftTableTimestampType.getTypeName()),
                    """
                            select t1.time1 + 1 as time, t1.x, t2.x, t1.x - t2.x
                            from\s
                            (
                                (
                                    select time - 1 as time1, x
                                    from positions2
                                )
                                timestamp(time1)
                            ) t1
                            asof join positions2 t2"""
            );
        });
    }

    @Test
    public void testAsOfJoinForSelectWithTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());

            final String expected = "tag\thi\tlo\tts\tts1\n" +
                    "AA\t315515118\t315515118\t1970-01-03T00:00:00.000000" + leftSuffix + "\t1970-01-03T00:00:00.000000" + leftSuffix + "\n" +
                    "BB\t-727724771\t-727724771\t1970-01-03T00:06:00.000000" + leftSuffix + "\t1970-01-03T00:06:00.000000" + leftSuffix + "\n" +
                    "CC\t-948263339\t-948263339\t1970-01-03T00:12:00.000000" + leftSuffix + "\t1970-01-03T00:12:00.000000" + leftSuffix + "\n" +
                    "CC\t592859671\t592859671\t1970-01-03T00:18:00.000000" + leftSuffix + "\t1970-01-03T00:18:00.000000" + leftSuffix + "\n" +
                    "AA\t-847531048\t-847531048\t1970-01-03T00:24:00.000000" + leftSuffix + "\t1970-01-03T00:24:00.000000" + leftSuffix + "\n" +
                    "BB\t-2041844972\t-2041844972\t1970-01-03T00:30:00.000000" + leftSuffix + "\t1970-01-03T00:30:00.000000" + leftSuffix + "\n" +
                    "BB\t-1575378703\t-1575378703\t1970-01-03T00:36:00.000000" + leftSuffix + "\t1970-01-03T00:36:00.000000" + leftSuffix + "\n" +
                    "BB\t1545253512\t1545253512\t1970-01-03T00:42:00.000000" + leftSuffix + "\t1970-01-03T00:42:00.000000" + leftSuffix + "\n" +
                    "AA\t1573662097\t1573662097\t1970-01-03T00:48:00.000000" + leftSuffix + "\t1970-01-03T00:48:00.000000" + leftSuffix + "\n" +
                    "AA\t339631474\t339631474\t1970-01-03T00:54:00.000000" + leftSuffix + "\t1970-01-03T00:54:00.000000" + leftSuffix + "\n";

            assertQuery(
                    "tag\thi\tlo\tts\tts1\n",
                    "select a.tag, a.seq hi, b.seq lo,  a.ts, b.ts from tab a asof join tab b on (tag)",
                    "create table tab (\n" +
                            "    tag symbol index,\n" +
                            "    seq int,\n" +
                            "    ts " + leftTableTimestampType.getTypeName() + "\n" +
                            ") timestamp(ts) partition by DAY",
                    "ts",
                    "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag, \n" +
                            "        rnd_int() seq, \n" +
                            "        timestamp_sequence(172800000000, 360000000)::" + leftTableTimestampType.getTypeName() + " ts \n" +
                            "    from long_sequence(10)) timestamp (ts)",
                    expected,
                    false,
                    true,
                    false
            );
        });
    }

    @Test
    public void testAsOfJoinForSelectWithoutTimestamp() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        final String expected = """
                tag\thi\tlo
                AA\t315515118\t315515118
                BB\t-727724771\t-727724771
                CC\t-948263339\t-948263339
                CC\t592859671\t592859671
                AA\t-847531048\t-847531048
                BB\t-2041844972\t-2041844972
                BB\t-1575378703\t-1575378703
                BB\t1545253512\t1545253512
                AA\t1573662097\t1573662097
                AA\t339631474\t339631474
                """;

        executeWithRewriteTimestamp(
                """
                        create table tab (
                            tag symbol index,
                            seq int,
                            ts #TIMESTAMP
                        ) timestamp(ts) partition by DAY""",
                leftTableTimestampType.getTypeName());

        assertQuery("tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)",
                null,
                false,
                true
        );
        execute(
                """
                        insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag,\s
                                rnd_int() seq,\s
                                timestamp_sequence(172800000000, 360000000)::timestamp ts\s
                            from long_sequence(10)) timestamp (ts)"""

        );
        assertQuery(expected,
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)",
                null,
                false,
                true
        );
    }

    @Test
    public void testAsOfJoinForSelectWithoutTimestampAndWithWhereStatementAsOuter() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        final String expected = """
                hi\tlo
                2\t1
                3\t2
                4\t3
                5\t4
                6\t5
                7\t6
                8\t7
                9\t8
                10\t9
                11\t10
                12\t11
                13\t12
                14\t13
                15\t14
                16\t15
                17\t16
                18\t17
                19\t18
                20\t19
                21\t20
                22\t21
                23\t22
                24\t23
                25\t24
                26\t25
                27\t26
                28\t27
                29\t28
                30\t29
                """;

        executeWithRewriteTimestamp(
                "create table test(seq long, ts #TIMESTAMP) timestamp(ts)",
                leftTableTimestampType.getTypeName());

        assertQuery("hi\tlo\n",
                "(select a.seq hi, b.seq lo from test a lt join test b) where lo != null",
                null,
                false,
                false
        );
        executeWithRewriteTimestamp(
                "insert into test select x, cast(x+10 as #TIMESTAMP) from (select x, rnd_double() rnd from long_sequence(30)) where rnd<0.9999",
                leftTableTimestampType.getTypeName()

        );
        assertQuery(expected,
                "(select a.seq hi, b.seq lo from test a lt join test b) where lo != null",
                null,
                false,
                false
        );
    }

    @Test
    public void testAsOfJoinForSelectWithoutTimestampAndWithWhereStatementV2() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        final String expected = "tag\thi\tlo\n";

        executeWithRewriteTimestamp(
                """
                        create table tab (
                            tag symbol index,
                            seq int,
                            ts #TIMESTAMP
                        ) timestamp(ts) partition by DAY""",
                leftTableTimestampType.getTypeName());

        assertQuery("tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag) where b.seq < a.seq",
                null,
                false,
                false
        );
        execute(
                """
                        insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag,\s
                                rnd_int() seq,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)"""

        );
        assertQuery(expected,
                "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag) where b.seq < a.seq",
                null,
                false,
                false
        );
    }

    @Test
    public void testAsOfJoinHighCardinalityKeysAndTolerance() throws Exception {
        // this tests set low threshold for evacuation of full fat ASOF join map
        // and compares that Fast and FullFat results are the same

        setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_EVACUATION_THRESHOLD, "10");
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE master (vch VARCHAR, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE slave (vch VARCHAR, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            execute("""
                    INSERT INTO master SELECT
                    rnd_int()::varchar as vch,
                    (timestamp_sequence(0, 1000000) + x * 1000000)::timestamp as ts
                    FROM long_sequence(1_000)
                    """
            );

            execute("""
                    INSERT INTO slave SELECT
                    rnd_int()::varchar as vch,
                    (timestamp_sequence(0, 1000000) + x * 1000000)::timestamp as ts
                    FROM long_sequence(1_000)
                    """
            );

            String query = "SELECT * FROM master ASOF JOIN slave y ON(vch) TOLERANCE 1s";
            printSql("EXPLAIN " + query, true);
            TestUtils.assertNotContains(sink, "AsOf Join Fast Scan");
            printSql(query, true);
            String fullFatResult = sink.toString();

            printSql("EXPLAIN " + query, false);
            TestUtils.assertContains(sink, "AsOf Join Fast Scan");
            printSql(query, false);
            String lightResult = sink.toString();
            TestUtils.assertEquals(fullFatResult, lightResult);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolBasic() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('GOOGL', 100.0, '2024-01-01T10:01:00.000000Z')");
            execute("INSERT INTO trades VALUES ('MSFT', 200.0, '2024-01-01T10:02:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 151.0, '2024-01-01T10:03:00.000000Z')");

            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T09:59:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('GOOGL', 99.5, 100.5, '2024-01-01T10:00:30.000000Z')");
            execute("INSERT INTO quotes VALUES ('MSFT', 199.5, 200.5, '2024-01-01T10:01:30.000000Z')");
            execute("INSERT INTO quotes VALUES ('AAPL', 150.5, 151.5, '2024-01-01T10:02:30.000000Z')");

            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\t149.5\t150.5
                    GOOGL\t100.0\t99.5\t100.5
                    MSFT\t200.0\t199.5\t200.5
                    AAPL\t151.0\t150.5\t151.5
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol";
            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolCrossFrameMatch() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Trade on day 2
            execute("INSERT INTO trades VALUES ('AAPL', 155.0, '2024-01-02T10:00:00.000000Z')");

            // Quote only on day 1 (should match as it's the latest before the trade)
            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T15:00:00.000000Z')");

            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t155.0\t149.5\t150.5
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol";
            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolEmptyRightTable() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('GOOGL', 100.0, '2024-01-01T10:01:00.000000Z')");

            // No quotes - all joins should return nulls
            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\tnull\tnull
                    GOOGL\t100.0\tnull\tnull
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol";
            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolFutureTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");

            // Quote timestamp is after trade - should not match
            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T10:01:00.000000Z')");

            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\tnull\tnull
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol";
            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolMultipleFrames() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Insert trades spanning multiple days
            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 155.0, '2024-01-02T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 160.0, '2024-01-03T10:00:00.000000Z')");

            // Insert quotes in different partitions
            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T09:00:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('AAPL', 154.5, 155.5, '2024-01-02T09:00:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('AAPL', 159.5, 160.5, '2024-01-03T09:00:00.000000Z')");

            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\t149.5\t150.5
                    AAPL\t155.0\t154.5\t155.5
                    AAPL\t160.0\t159.5\t160.5
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol";
            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolMultipleSymbols() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Mixed trades for multiple symbols
            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('GOOGL', 100.0, '2024-01-01T10:00:30.000000Z')");
            execute("INSERT INTO trades VALUES ('MSFT', 200.0, '2024-01-01T10:01:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 151.0, '2024-01-01T10:02:00.000000Z')");
            execute("INSERT INTO trades VALUES ('GOOGL', 101.0, '2024-01-01T10:03:00.000000Z')");

            // Quotes for all symbols at various times
            execute("INSERT INTO quotes VALUES ('AAPL', 149.0, 150.0, '2024-01-01T09:30:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('GOOGL', 99.0, 100.0, '2024-01-01T09:45:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('MSFT', 199.0, 200.0, '2024-01-01T09:50:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('AAPL', 150.5, 151.5, '2024-01-01T10:01:30.000000Z')");
            execute("INSERT INTO quotes VALUES ('GOOGL', 100.5, 101.5, '2024-01-01T10:02:30.000000Z')");

            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\t149.0\t150.0
                    GOOGL\t100.0\t99.0\t100.0
                    MSFT\t200.0\t199.0\t200.0
                    AAPL\t151.0\t150.5\t151.5
                    GOOGL\t101.0\t100.5\t101.5
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol";
            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolNoMatch() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('TSLA', 250.0, '2024-01-01T10:01:00.000000Z')"); // No matching symbol in quotes

            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T09:59:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('GOOGL', 99.5, 100.5, '2024-01-01T10:00:30.000000Z')");

            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\t149.5\t150.5
                    TSLA\t250.0\tnull\tnull
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol";
            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolTolerance() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Trades at 10:00 and 10:05
            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 152.0, '2024-01-01T10:05:00.000000Z')");

            // Quotes: one old (09:50) and one recent (10:04:50)
            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T09:50:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('AAPL', 151.5, 152.5, '2024-01-01T10:04:50.000000Z')");

            // With 1-minute tolerance: first trade should have no match (quote is 10 mins old)
            String expected1 = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\tnull\tnull
                    AAPL\t152.0\t151.5\t152.5
                    """;

            String queryBody1 = "t.symbol, t.price, q.bid, q.ask FROM trades t " +
                    "ASOF JOIN quotes q ON t.symbol = q.symbol " +
                    "TOLERANCE 1m";

            assertAlgoAndResult(queryBody1, "", "Fast", expected1);
            assertAlgoAndResult(queryBody1, "asof_index_search(t q)", "Indexed", expected1);
            assertAlgoAndResult(queryBody1, "asof_memoized(t q)", "Memoized", expected1);

            // With 15-minute tolerance: both trades should match
            String expected2 = """
                    symbol\tprice\tbid\task
                    AAPL\t150.0\t149.5\t150.5
                    AAPL\t152.0\t151.5\t152.5
                    """;

            String queryBody2 = "t.symbol, t.price, q.bid, q.ask FROM trades t " +
                    "ASOF JOIN quotes q ON t.symbol = q.symbol " +
                    "TOLERANCE 15m";
            assertAlgoAndResult(queryBody2, "", "Fast", expected2);
            assertAlgoAndResult(queryBody2, "asof_index_search(t q)", "Indexed", expected2);
            assertAlgoAndResult(queryBody2, "asof_memoized(t q)", "Memoized", expected2);
        });
    }

    @Test
    public void testAsOfJoinIndexedSymbolWithBetween() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL INDEX,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Trades over several days
            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 155.0, '2024-01-02T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 160.0, '2024-01-03T10:00:00.000000Z')");

            // Quotes over several days
            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T09:00:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('AAPL', 154.5, 155.5, '2024-01-02T09:00:00.000000Z')");
            execute("INSERT INTO quotes VALUES ('AAPL', 159.5, 160.5, '2024-01-03T09:00:00.000000Z')");

            // Use BETWEEN to limit time range
            String expected = """
                    symbol\tprice\tbid\task
                    AAPL\t155.0\t154.5\t155.5
                    """;

            String queryBody = "t.symbol, t.price, q.bid, q.ask FROM trades t " +
                    "ASOF JOIN (SELECT * FROM quotes WHERE ts BETWEEN '2024-01-02T00:00:00.000000Z' AND '2024-01-02T23:59:59.999999Z') q " +
                    "ON t.symbol = q.symbol " +
                    "WHERE t.ts BETWEEN '2024-01-02T00:00:00.000000Z' AND '2024-01-02T23:59:59.999999Z'";

            assertAlgoAndResult(queryBody, "", "Fast", expected);
            assertAlgoAndResult(queryBody, "asof_index_search(t q)", "Indexed", expected);
            assertAlgoAndResult(queryBody, "asof_memoized(t q)", "Memoized", expected);
        });
    }

    @Test
    public void testAsOfJoinLinearSearchHint() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table orders as (\n" +
                            "  select \n" +
                            "    concat('sym_', rnd_int(0, 10, 0))::symbol as order_symbol,\n" +
                            "    rnd_double() price,\n" +
                            "    rnd_double() volume,\n" +
                            "    ('2025'::timestamp + x * 200_000_000L + rnd_int(0, 10_000, 0))::" + leftTableTimestampType.getTypeName() + " as ts,\n" +
                            "  from long_sequence(5)\n" +
                            ") timestamp(ts) partition by day;\n",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp("create table market_data as (\n" +
                            "  select \n" +
                            "    concat('sym_', rnd_int(0, 10, 0))::symbol as market_data_symbol,\n" +
                            "    rnd_double() bid,\n" +
                            "    rnd_double() ask,\n" +
                            "    ('2025'::timestamp + x * 100_000L + rnd_int(0, 10_000, 0))::" + rightTableTimestampType.getTypeName() + " as ts,\n" +
                            "  from long_sequence(10_000)\n" +
                            ") timestamp(ts) partition by day;",
                    rightTableTimestampType.getTypeName()
            );

            String queryBody = """
                    * from (
                      select orders.ts, bid, md.market_data_symbol, orders.order_symbol, md.md_ts as order_ts, price from oRdERS
                      asof join (
                        select ts as md_ts, market_Data_symbol, bid from market_data
                        where market_data_symbol = 'sym_1'\s
                      ) MD \s
                      where orders.ts > '2025-01-01T00:00:00.000000000Z'\s
                      and bid > price
                    );""";
            String queryWithoutHint = "select " + queryBody;
            String queryWithLinearHint = "select /*+ asof_linear(orders md) */ " + queryBody;

            // plan with the linear search hint should NOT use the FAST ASOF
            assertQueryNoLeakCheck("QUERY PLAN\n" +
                            "SelectedRecord\n" +
                            "    Filter filter: oRdERS.price<MD.bid\n" +
                            "        AsOf Join\n" +
                            "            PageFrame\n" +
                            "                Row forward scan\n" +
                            "                Interval forward scan on: orders\n" +
                            (leftTableTimestampType == TestTimestampType.MICRO ?
                                    "                  intervals: [(\"2025-01-01T00:00:00.000001Z\",\"MAX\")]\n" :
                                    "                  intervals: [(\"2025-01-01T00:00:00.000000001Z\",\"MAX\")]\n") +
                            "            SelectedRecord\n" +
                            "                Async " + (JitUtil.isJitSupported() ? "JIT " : "") + "Filter workers: 1\n" +
                            "                  filter: market_Data_symbol='sym_1'\n" +
                            "                    PageFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: market_data\n",
                    "EXPLAIN " + queryWithLinearHint, null, false, true);

            String expectedPlan = "QUERY PLAN\n" +
                    "SelectedRecord\n" +
                    "    Filter filter: oRdERS.price<MD.bid\n" +
                    "        Filtered AsOf Join Fast Scan\n" +
                    "          filter: market_Data_symbol='sym_1'\n" +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Interval forward scan on: orders\n" +
                    (leftTableTimestampType == TestTimestampType.MICRO ?
                            "                  intervals: [(\"2025-01-01T00:00:00.000001Z\",\"MAX\")]\n" :
                            "                  intervals: [(\"2025-01-01T00:00:00.000000001Z\",\"MAX\")]\n") +
                    "            PageFrame\n" +
                    "                Row forward scan\n" +
                    "                Frame forward scan on: market_data\n";
            // query without Linear hint should use the fast asof join
            assertQueryNoLeakCheck(expectedPlan,
                    "EXPLAIN " + queryWithoutHint, null, false, true);

            // both queries must return the same result
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            String expectedResult = "ts\tbid\tmarket_data_symbol\torder_symbol\torder_ts\tprice\n" +
                    "2025-01-01T00:03:20.003570" + leftSuffix + "\t0.18646912884414946\tsym_1\tsym_4\t2025-01-01T00:03:19.407091" + rightSuffix + "\t0.08486964232560668\n" +
                    "2025-01-01T00:06:40.006304" + leftSuffix + "\t0.9130994629783138\tsym_1\tsym_2\t2025-01-01T00:06:37.303610" + rightSuffix + "\t0.8423410920883345\n" +
                    "2025-01-01T00:13:20.002056" + leftSuffix + "\t0.24872951622414008\tsym_1\tsym_4\t2025-01-01T00:13:19.909382" + rightSuffix + "\t0.0367581207471136\n" +
                    "2025-01-01T00:16:40.009947" + leftSuffix + "\t0.5071618579762882\tsym_1\tsym_6\t2025-01-01T00:16:39.800653" + rightSuffix + "\t0.3100545983862456\n";

            assertQueryNoLeakCheck(expectedResult, queryWithLinearHint, "ts", false, false);
            assertQueryNoLeakCheck(expectedResult, queryWithoutHint, "ts", false, false);
        });
    }

    @Test
    public void testAsOfJoinNoAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            // ASKS
            executeWithRewriteTimestamp(
                    "create table asks(ask int, ts #TIMESTAMP) timestamp(ts) partition by none",
                    leftTableTimestampType.getTypeName()
            );
            execute("insert into asks values(100, 0)");
            execute("insert into asks values(101, 2::timestamp);");
            execute("insert into asks values(102, 4::timestamp);");

            // BIDS
            executeWithRewriteTimestamp(
                    "create table bids(bid int, ts #TIMESTAMP) timestamp(ts) partition by none",
                    rightTableTimestampType.getTypeName()
            );
            execute("insert into bids values(101, 1::timestamp);");
            execute("insert into bids values(102, 3::timestamp);");
            execute("insert into bids values(103, 5::timestamp);");

            String query =
                    """
                            SELECT\s
                                b.timebid timebid,
                                a.timeask timeask,\s
                                b.b b,\s
                                a.a a
                            FROM (select b.bid b, b.ts timebid from bids b) b\s
                                ASOF JOIN
                            (select a.ask a, a.ts timeask from asks a) a
                            WHERE (b.timebid != a.timeask);""";

            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            String expected = "timebid\ttimeask\tb\ta\n" +
                    "1970-01-01T00:00:00.000001" + rightSuffix + "\t1970-01-01T00:00:00.000000" + leftSuffix + "\t101\t100\n" +
                    "1970-01-01T00:00:00.000003" + rightSuffix + "\t1970-01-01T00:00:00.000002" + leftSuffix + "\t102\t101\n" +
                    "1970-01-01T00:00:00.000005" + rightSuffix + "\t1970-01-01T00:00:00.000004" + leftSuffix + "\t103\t102\n";

            printSqlResult(expected, query, "timebid", false, false);
        });
    }

    @Test
    public void testAsOfJoinOnEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table t1 as (select x as id, cast(x as #TIMESTAMP) ts from long_sequence(5)) timestamp(ts) partition by day;", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("create table t2 (id long, ts  #TIMESTAMP) timestamp(ts) partition by day;", rightTableTimestampType.getTypeName());

            final String query = "SELECT * FROM t1 \n" +
                    "ASOF JOIN t2 ON id;";
            final String expected = """
                    id\tts\tid1\tts1
                    1\t1970-01-01T00:00:00.000001Z\tnull\t
                    2\t1970-01-01T00:00:00.000002Z\tnull\t
                    3\t1970-01-01T00:00:00.000003Z\tnull\t
                    4\t1970-01-01T00:00:00.000004Z\tnull\t
                    5\t1970-01-01T00:00:00.000005Z\tnull\t
                    """;
            printSqlResult(replaceTimestampSuffix(expected, leftTableTimestampType.getTypeName()), query, "ts", false, true);
        });
    }

    @Test
    public void testAsOfJoinOnNullSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            final String expected = """
                    tag\thi\tlo
                    AA\t315515118\t315515118
                    BB\t-727724771\t-727724771
                    \t-948263339\t-948263339
                    \t592859671\t592859671
                    AA\t-847531048\t-847531048
                    BB\t-2041844972\t-2041844972
                    BB\t-1575378703\t-1575378703
                    BB\t1545253512\t1545253512
                    AA\t1573662097\t1573662097
                    AA\t339631474\t339631474
                    """;
            executeWithRewriteTimestamp(
                    """
                            create table tab (
                                tag symbol index,
                                seq int,
                                ts timestamp
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );
            assertQuery(
                    "tag\thi\tlo\n",
                    "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)",
                    null,
                    false,
                    true
            );
            execute("""
                    insert into tab select * from (select rnd_symbol('AA', 'BB', null) tag,\s
                            rnd_int() seq,\s
                            timestamp_sequence(172800000000, 360000000) ts\s
                        from long_sequence(10)) timestamp (ts)"""
            );
            assertQuery(
                    expected,
                    "select a.tag, a.seq hi, b.seq lo from tab a asof join tab b on (tag)",
                    null,
                    false,
                    true
            );

            execute("create table tab2 as (select * from tab where tag is not null)");
            assertQueryNoLeakCheck("""
                            tag\thi\tlo
                            AA\t315515118\t315515118
                            BB\t-727724771\t-727724771
                            \t-948263339\tnull
                            \t592859671\tnull
                            AA\t-847531048\t-847531048
                            BB\t-2041844972\t-2041844972
                            BB\t-1575378703\t-1575378703
                            BB\t1545253512\t1545253512
                            AA\t1573662097\t1573662097
                            AA\t339631474\t339631474
                            """,
                    "select a.tag, a.seq hi, b.seq lo from tab a asof join tab2 b on (tag)", null, null, false, true);
        });
    }

    @Test
    public void testAsOfJoinOnTripleSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                executeWithRewriteTimestamp(
                        "CREATE TABLE bids (stock SYMBOL, exchange SYMBOL, market SYMBOL, ts #TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY",
                        leftTableTimestampType.getTypeName()
                );
                executeWithRewriteTimestamp(
                        "CREATE TABLE asks (stock SYMBOL, exchange SYMBOL, market SYMBOL, ts #TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY",
                        rightTableTimestampType.getTypeName()
                );

                execute("INSERT INTO bids VALUES " +
                        "('AAPL', 'NASDAQ', 'US', '2000-01-01T00:00:00.000000Z', 1, 'GOOD')," +
                        "('AAPL', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 2, 'GOOD')," +
                        "('AAPL', 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 3, 'SCAM')," +
                        "('AAPL', 'NASDAQ', 'EU', '2000-01-01T00:00:00.000000Z', 4, 'SCAM')," +
                        "('AAPL', 'NASDAQ', 'EU', '2001-01-01T00:00:00.000000Z', 5, 'EXCELLENT')," +
                        "('AAPL', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 6, 'SCAM')," +
                        "('AAPL', 'LSE', 'UK', '2001-01-01T00:00:00.000000Z', 7, 'GOOD')," +
                        "('AAPL', 'LSE', 'UK', '2002-01-01T00:00:00.000000Z', 8, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2000-01-01T00:00:00.000000Z', 9, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 10, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 11, 'SCAM')," +
                        "('MSFT', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 12, 'UNKNOWN')," +
                        "('MSFT', 'LSE', 'UK', '2001-01-01T00:00:00.000000Z', 13, 'GOOD')"
                );

                execute("INSERT INTO asks VALUES " +
                        "('AAPL', 'NASDAQ', 'US', '2000-01-01T00:00:00.000000Z', 1, 'GOOD')," +
                        "('AAPL', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 2, 'EXCELLENT')," +
                        "('AAPL', 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 3, 'EXCELLENT')," +
                        "('AAPL', 'NASDAQ', 'EU', '2000-01-01T00:00:00.000000Z', 4, 'EXCELLENT')," +
                        "('AAPL', 'NASDAQ', 'EU', '2001-01-01T00:00:00.000000Z', 5, 'EXCELLENT')," +
                        "('AAPL', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 6, 'SCAM')," +
                        "('AAPL', 'LSE', 'UK', '2001-01-01T00:00:00.000000Z', 7, 'EXCELLENT')," +
                        "('AAPL', 'LSE', 'UK', '2002-01-01T00:00:00.000000Z', 8, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2000-01-01T00:00:00.000000Z', 9, 'EXCELLENT')," +
                        "('MSFT', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 10, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 11, 'EXCELLENT')," +
                        "('MSFT', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 12, 'GOOD')," +
                        "('MSFT', 'LSE', 'UK', '2001-01-01T00:00:00.000000Z', 13, 'SCAM')"
                );

                String query = "SELECT * FROM bids ASOF JOIN asks ON (stock, exchange, market)";
                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());
                String expected = "stock\texchange\tmarket\tts\ti\trating\tstock1\texchange1\tmarket1\tts1\ti1\trating1\n" +
                        "AAPL\tNASDAQ\tUS\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tGOOD\tAAPL\tNASDAQ\tUS\t2000-01-01T00:00:00.000000" + rightSuffix + "\t1\tGOOD\n" +
                        "AAPL\tNASDAQ\tEU\t2000-01-01T00:00:00.000000" + leftSuffix + "\t4\tSCAM\tAAPL\tNASDAQ\tEU\t2000-01-01T00:00:00.000000" + rightSuffix + "\t4\tEXCELLENT\n" +
                        "AAPL\tLSE\tUK\t2000-01-01T00:00:00.000000" + leftSuffix + "\t6\tSCAM\tAAPL\tLSE\tUK\t2000-01-01T00:00:00.000000" + rightSuffix + "\t6\tSCAM\n" +
                        "MSFT\tNASDAQ\tUS\t2000-01-01T00:00:00.000000" + leftSuffix + "\t9\tGOOD\tMSFT\tNASDAQ\tUS\t2000-01-01T00:00:00.000000" + rightSuffix + "\t9\tEXCELLENT\n" +
                        "MSFT\tLSE\tUK\t2000-01-01T00:00:00.000000" + leftSuffix + "\t12\tUNKNOWN\tMSFT\tLSE\tUK\t2000-01-01T00:00:00.000000" + rightSuffix + "\t12\tGOOD\n" +
                        "AAPL\tNASDAQ\tUS\t2001-01-01T00:00:00.000000" + leftSuffix + "\t2\tGOOD\tAAPL\tNASDAQ\tUS\t2001-01-01T00:00:00.000000" + rightSuffix + "\t2\tEXCELLENT\n" +
                        "AAPL\tNASDAQ\tEU\t2001-01-01T00:00:00.000000" + leftSuffix + "\t5\tEXCELLENT\tAAPL\tNASDAQ\tEU\t2001-01-01T00:00:00.000000" + rightSuffix + "\t5\tEXCELLENT\n" +
                        "AAPL\tLSE\tUK\t2001-01-01T00:00:00.000000" + leftSuffix + "\t7\tGOOD\tAAPL\tLSE\tUK\t2001-01-01T00:00:00.000000" + rightSuffix + "\t7\tEXCELLENT\n" +
                        "MSFT\tNASDAQ\tUS\t2001-01-01T00:00:00.000000" + leftSuffix + "\t10\tGOOD\tMSFT\tNASDAQ\tUS\t2001-01-01T00:00:00.000000" + rightSuffix + "\t10\tGOOD\n" +
                        "MSFT\tLSE\tUK\t2001-01-01T00:00:00.000000" + leftSuffix + "\t13\tGOOD\tMSFT\tLSE\tUK\t2001-01-01T00:00:00.000000" + rightSuffix + "\t13\tSCAM\n" +
                        "AAPL\tLSE\tUK\t2002-01-01T00:00:00.000000" + leftSuffix + "\t8\tGOOD\tAAPL\tLSE\tUK\t2002-01-01T00:00:00.000000" + rightSuffix + "\t8\tGOOD\n" +
                        "MSFT\tNASDAQ\tUS\t2002-01-01T00:00:00.000000" + leftSuffix + "\t11\tSCAM\tMSFT\tNASDAQ\tUS\t2002-01-01T00:00:00.000000" + rightSuffix + "\t11\tEXCELLENT\n" +
                        "AAPL\tNASDAQ\tUS\t2002-01-01T00:00:00.000000" + leftSuffix + "\t3\tSCAM\tAAPL\tNASDAQ\tUS\t2002-01-01T00:00:00.000000" + rightSuffix + "\t3\tEXCELLENT\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testAsOfJoinOnTripleSymbolKeyLastKeyMissing() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                executeWithRewriteTimestamp(
                        "CREATE TABLE bids (stock SYMBOL, exchange SYMBOL, market SYMBOL, ts #TIMESTAMP, i INT, rating STRING) TIMESTAMP(ts) PARTITION BY DAY",
                        leftTableTimestampType.getTypeName()
                );
                executeWithRewriteTimestamp(
                        "CREATE TABLE asks (stock SYMBOL, exchange SYMBOL, market SYMBOL, ts #TIMESTAMP, i INT, rating STRING) TIMESTAMP(ts) PARTITION BY DAY",
                        rightTableTimestampType.getTypeName()
                );

                execute("INSERT INTO bids VALUES " +
                        "('AAPL', 'NASDAQ', 'ASIA', '2000-01-01T00:00:00.000000Z', 1, 'GOOD')," +
                        "('AAPL', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 2, 'GOOD')," +
                        "(null, 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 3, 'SCAM')," +
                        "('AAPL', 'NASDAQ', 'EU', '2000-01-01T00:00:00.000000Z', 4, 'SCAM')," +
                        "('AAPL', 'NASDAQ', 'EU', '2001-01-01T00:00:00.000000Z', 5, 'EXCELLENT')," +
                        "('AAPL', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 6, 'SCAM')," +
                        "('AAPL', 'LSE', 'UK', '2001-01-01T00:00:00.000000Z', 7, 'GOOD')," +
                        "('AAPL', 'LSE', 'UK', '2002-01-01T00:00:00.000000Z', 8, 'GOOD')," +
                        "('MSFT', 'FRA', 'US', '2000-01-01T00:00:00.000000Z', 9, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 10, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 11, 'SCAM')," +
                        "('QDB', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 12, 'UNKNOWN')," +
                        "('MSFT', 'LSE', null, '2001-01-01T00:00:00.000000Z', 13, 'GOOD')"
                );

                execute("INSERT INTO asks VALUES " +
                        "('AAPL', 'NASDAQ', 'US', '2000-01-01T00:00:00.000000Z', 1, 'GOOD')," +
                        "('AAPL', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 2, 'EXCELLENT')," +
                        "('AAPL', 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 3, 'EXCELLENT')," +
                        "('AAPL', 'NASDAQ', 'EU', '2000-01-01T00:00:00.000000Z', 4, 'EXCELLENT')," +
                        "('AAPL', 'NASDAQ', 'EU', '2001-01-01T00:00:00.000000Z', 5, 'EXCELLENT')," +
                        "('AAPL', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 6, 'SCAM')," +
                        "('AAPL', 'LSE', 'UK', '2001-01-01T00:00:00.000000Z', 7, 'EXCELLENT')," +
                        "('AAPL', 'LSE', 'UK', '2002-01-01T00:00:00.000000Z', 8, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2000-01-01T00:00:00.000000Z', 9, 'EXCELLENT')," +
                        "('MSFT', 'NASDAQ', 'US', '2001-01-01T00:00:00.000000Z', 10, 'GOOD')," +
                        "('MSFT', 'NASDAQ', 'US', '2002-01-01T00:00:00.000000Z', 11, 'EXCELLENT')," +
                        "('MSFT', 'LSE', 'UK', '2000-01-01T00:00:00.000000Z', 12, 'GOOD')," +
                        "('MSFT', 'LSE', 'UK', '2001-01-01T00:00:00.000000Z', 13, 'SCAM')"
                );

                String query = "SELECT * FROM bids ASOF JOIN asks ON (stock, rating, exchange, market)";
                String expected = "stock\texchange\tmarket\tts\ti\trating\tstock1\texchange1\tmarket1\tts1\ti1\trating1\n" +
                        "AAPL\tNASDAQ\tASIA\t2000-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t1\tGOOD\t\t\t\t\tnull\t\n" +
                        "AAPL\tNASDAQ\tEU\t2000-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t4\tSCAM\t\t\t\t\tnull\t\n" +
                        "AAPL\tLSE\tUK\t2000-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t6\tSCAM\tAAPL\tLSE\tUK\t2000-01-01T00:00:00.000000" + getTimestampSuffix(rightTableTimestampType.getTypeName()) + "\t6\tSCAM\n" +
                        "MSFT\tFRA\tUS\t2000-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t9\tGOOD\t\t\t\t\tnull\t\n" +
                        "QDB\tLSE\tUK\t2000-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t12\tUNKNOWN\t\t\t\t\tnull\t\n" +
                        "AAPL\tNASDAQ\tUS\t2001-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t2\tGOOD\tAAPL\tNASDAQ\tUS\t2000-01-01T00:00:00.000000" + getTimestampSuffix(rightTableTimestampType.getTypeName()) + "\t1\tGOOD\n" +
                        "AAPL\tNASDAQ\tEU\t2001-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t5\tEXCELLENT\tAAPL\tNASDAQ\tEU\t2001-01-01T00:00:00.000000" + getTimestampSuffix(rightTableTimestampType.getTypeName()) + "\t5\tEXCELLENT\n" +
                        "AAPL\tLSE\tUK\t2001-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t7\tGOOD\t\t\t\t\tnull\t\n" +
                        "MSFT\tNASDAQ\tUS\t2001-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t10\tGOOD\tMSFT\tNASDAQ\tUS\t2001-01-01T00:00:00.000000" + getTimestampSuffix(rightTableTimestampType.getTypeName()) + "\t10\tGOOD\n" +
                        "MSFT\tLSE\t\t2001-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t13\tGOOD\t\t\t\t\tnull\t\n" +
                        "AAPL\tLSE\tUK\t2002-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t8\tGOOD\tAAPL\tLSE\tUK\t2002-01-01T00:00:00.000000" + getTimestampSuffix(rightTableTimestampType.getTypeName()) + "\t8\tGOOD\n" +
                        "MSFT\tNASDAQ\tUS\t2002-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t11\tSCAM\t\t\t\t\tnull\t\n" +
                        "\tNASDAQ\tUS\t2002-01-01T00:00:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\t3\tSCAM\t\t\t\t\tnull\t\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testAsOfJoinTolerance() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't1' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't2' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    rightTableTimestampType.getTypeName());
            execute("insert into t1 select x as id, (x + x*1_000_000)::timestamp ts from long_sequence(10)");
            execute("insert into t2 select x as id, (x)::timestamp ts from long_sequence(5)");

            // keyed join and slave supports timeframe -> plan should use AsOfJoinFastRecordCursorFactory
            String query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 2s;";
            // sanity check: uses AsOfJoinFastRecordCursorFactory
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "AsOf Join Fast Scan");
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());
            String expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t1\t1970-01-01T00:00:00.000001" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\t2\t1970-01-01T00:00:00.000002" + rightSuffix + "\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            // keyed join and slave has a stealable filter -> should use FilteredAsOfJoinFastRecordCursorFactory
            query = "SELECT * FROM t1 ASOF JOIN (select * from t2 where t2.id != 1000) ON id TOLERANCE 2s;";
            // sanity check: uses FilteredAsOfJoinFastRecordCursorFactory
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "Filtered AsOf Join Fast Scan");
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            assertQueryFullFatNoLeakCheck(expected, query, "ts", false, true, true);


            // non-keyed join and slave supports timeframe -> should use AsOfJoinNoKeyFastRecordCursorFactory
            query = "SELECT * FROM t1 ASOF JOIN t2 TOLERANCE 2s;";

            expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t5\t1970-01-01T00:00:00.000005" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\t5\t1970-01-01T00:00:00.000005" + rightSuffix + "\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "AsOf Join Fast Scan");
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            // non-keyed join, slave has a filter, no hint -> should also use FilteredAsOfJoinNoKeyFastRecordCursorFactory
            query = "SELECT * FROM t1 ASOF JOIN (select * from t2 where t2.id != 1000) t2 TOLERANCE 2s;";
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "Filtered AsOf Join Fast Scan");
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            // non-keyed join, slave has a filter, linear hint -> should also use AsOfJoinNoKeyRecordCursorFactory
            query = "SELECT /*+ asof_linear(t1 t2) */ * FROM t1 ASOF JOIN (select * from t2 where t2.id != 1000) t2 TOLERANCE 2s;";
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "AsOf Join");
            TestUtils.assertNotContains(sink, "Filtered");
            TestUtils.assertNotContains(sink, "Memoized");
        });
    }

    @Test
    public void testAsOfJoinToleranceNegative() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table t1 as (select x as id, (x + x*1_000_000)::#TIMESTAMP ts from long_sequence(10)) timestamp(ts) partition by day;", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("create table t2 as (select x as id, (x)::#TIMESTAMP ts from long_sequence(5)) timestamp(ts) partition by day;", rightTableTimestampType.getTypeName());

            String query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE -2s;";
            assertExceptionNoLeakCheck(query, 49, "ASOF JOIN TOLERANCE must be positive");

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 0s;";
            assertExceptionNoLeakCheck(query, 46, "zero is not a valid tolerance value");

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 0;";
            assertExceptionNoLeakCheck(query, 46, "zero is not a valid tolerance value");

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1Q;";
            assertExceptionNoLeakCheck(query, 46, "unsupported TOLERANCE unit [unit=Q]");
        });
    }

    @Test
    public void testAsOfJoinToleranceSupportedUnits() throws Exception {
        assertMemoryLeak(() -> {

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't1' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't2' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    rightTableTimestampType.getTypeName());
            execute("insert into t1 select x as id, (x + x*1_000_000)::timestamp ts from long_sequence(10)");
            execute("insert into t2 select x as id, (x)::timestamp ts from long_sequence(5)");
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            String expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t1\t1970-01-01T00:00:00.000001" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\tnull\t\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";

            String query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1000000U;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1000000000n;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1000T;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1s;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1n;";
            assertQueryNoLeakCheck("id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\tnull\t\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\tnull\t\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n", query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1m;";
            expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t1\t1970-01-01T00:00:00.000001" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\t2\t1970-01-01T00:00:00.000002" + rightSuffix + "\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\t3\t1970-01-01T00:00:00.000003" + rightSuffix + "\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\t4\t1970-01-01T00:00:00.000004" + rightSuffix + "\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\t5\t1970-01-01T00:00:00.000005" + rightSuffix + "\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1h;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1d;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1w;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);
        });
    }

    @Test
    public void testAsOfJoinToleranceSupportedUnitsWithDifferentTimestampTypes() throws Exception {
        assertMemoryLeak(() -> {

            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't1' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't2' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    rightTableTimestampType.getTypeName());
            execute("insert into t1 select x as id, (x + x*1_000_000)::timestamp ts from long_sequence(10)");
            execute("insert into t2 select x as id, (x)::timestamp ts from long_sequence(5)");
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            String expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t1\t1970-01-01T00:00:00.000001" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\tnull\t\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";

            String query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1000000U;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1000000000n;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1000T;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1s;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1n;";
            assertQueryNoLeakCheck("id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\tnull\t\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\tnull\t\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n", query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1m;";
            expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t1\t1970-01-01T00:00:00.000001" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\t2\t1970-01-01T00:00:00.000002" + rightSuffix + "\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\t3\t1970-01-01T00:00:00.000003" + rightSuffix + "\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\t4\t1970-01-01T00:00:00.000004" + rightSuffix + "\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\t5\t1970-01-01T00:00:00.000005" + rightSuffix + "\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1h;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1d;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            query = "SELECT * FROM t1 ASOF JOIN t2 ON id TOLERANCE 1w;";
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);
        });
    }

    @Test
    public void testAsOfJoinWithIndexSearchHint() throws Exception {
        assertMemoryLeak(() -> {
            // Create tables with symbol index
            executeWithRewriteTimestamp(
                    """
                            create table orders as (
                              select\s
                                rnd_symbol('A', 'B', 'C') as sym,
                                cast(x as #TIMESTAMP) as ts
                              from long_sequence(100)
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            create table trades as (
                              select\s
                                rnd_symbol('A', 'B', 'C') as sym,
                                cast(x + 10 as #TIMESTAMP) as ts
                              from long_sequence(50)
                            ), index(sym) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Query with asof_index_search hint
            String query = "SELECT /*+ asof_index_search(orders trades) */ * FROM orders " +
                    "ASOF JOIN trades ON (sym)";

            // Verify the query plan shows indexed search
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "AsOf Join Indexed");

            // Execute and verify existence of results
            printSql(query);
            Assert.assertFalse(sink.isEmpty());
        });
    }

    @Test
    public void testAsOfJoinWithLinearSearchHint() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            create table t1 as (
                              select\s
                                rnd_symbol('A', 'B', 'C') as sym,
                                cast(x as #TIMESTAMP) as ts
                              from long_sequence(100)
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            create table t2 as (
                              select\s
                                rnd_symbol('A', 'B', 'C') as sym,
                                cast(x as #TIMESTAMP) as ts
                              from long_sequence(100)
                            ), index(sym) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Query with asof_linear hint (forces linear search)
            String query = "SELECT /*+ asof_linear(t1 t2) */ * FROM t1 " +
                    "ASOF JOIN t2 ON (sym)";

            // Verify the query plan does NOT show Fast Scan
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "AsOf Join");
            TestUtils.assertNotContains(sink, "Memoized");
            TestUtils.assertNotContains(sink, "Indexed");

            // Execute and verify results
            printSql(query);
            Assert.assertFalse(sink.isEmpty());
        });
    }

    @Test
    public void testAsOfJoinWithMultipleHintsCombination() throws Exception {
        assertMemoryLeak(() -> {
            // Create test tables with symbols
            executeWithRewriteTimestamp(
                    """
                            create table events as (
                              select\s
                                rnd_symbol('TYPE1', 'TYPE2', 'TYPE3') as event_type,
                                cast(x as #TIMESTAMP) as ts
                              from long_sequence(200)
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            create table responses as (
                              select\s
                                rnd_symbol('TYPE1', 'TYPE2', 'TYPE3') as event_type,
                                cast(x + 10 as #TIMESTAMP) as ts
                              from long_sequence(100)
                            ), index(event_type) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Test that linear search hint takes precedence over index search
            String query = "SELECT /*+ asof_linear(events responses) asof_index_search(events responses) */ * " +
                    "FROM events ASOF JOIN responses ON (event_type)";

            printSql("EXPLAIN " + query);
            // Linear search should take precedence
            TestUtils.assertContains(sink, "AsOf Join");
            TestUtils.assertNotContains(sink, "Indexed");
            TestUtils.assertNotContains(sink, "Memoized");
        });
    }

    @Test
    public void testCursorToTop() throws Exception {
        assertMemoryLeak(() -> {
            // This test verifies that toTop() properly clears the memoization state in AsOfJoinMemoizedRecordCursor.
            // The CROSS JOIN forces the ASOF JOIN cursor to be reset via toTop() for each row in the multiplier table.
            // After toTop(), the memoization cache (rememberedSymbols map and scanned range tracking) must be cleared
            // so that the second iteration produces identical results to the first iteration.

            // Create left table for ASOF JOIN
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE trades (
                                symbol SYMBOL,
                                price DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY
                            """,
                    leftTableTimestampType.getTypeName()
            );
            // Insert trades with repeating symbols and interleaved timestamps
            // This will test that memoization correctly caches and reuses previous matches
            execute("INSERT INTO trades VALUES ('AAPL', 150.0, '2024-01-01T10:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('GOOGL', 100.0, '2024-01-01T11:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('MSFT', 200.0, '2024-01-01T12:00:00.000000Z')");
            execute("INSERT INTO trades VALUES ('AAPL', 151.0, '2024-01-01T13:00:00.000000Z')");  // AAPL again - should reuse memoized data
            execute("INSERT INTO trades VALUES ('GOOGL', 101.0, '2024-01-01T14:00:00.000000Z')"); // GOOGL again
            execute("INSERT INTO trades VALUES ('TSLA', 250.0, '2024-01-01T15:00:00.000000Z')"); // New symbol
            execute("INSERT INTO trades VALUES ('AAPL', 152.0, '2024-01-01T16:00:00.000000Z')");  // AAPL third time
            execute("INSERT INTO trades VALUES ('MSFT', 201.0, '2024-01-01T17:00:00.000000Z')"); // MSFT again

            // Create right table for ASOF JOIN
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE quotes (
                                symbol SYMBOL,
                                bid DOUBLE,
                                ask DOUBLE,
                                ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY
                            """,
                    rightTableTimestampType.getTypeName()
            );
            // Insert quotes with interleaved timestamps relative to trades
            // This creates a complex pattern where memoization needs to track multiple symbols
            execute("INSERT INTO quotes VALUES ('AAPL', 149.5, 150.5, '2024-01-01T09:00:00.000000Z')");  // Before first AAPL trade
            execute("INSERT INTO quotes VALUES ('GOOGL', 99.5, 100.5, '2024-01-01T10:30:00.000000Z')"); // Between AAPL and GOOGL trades
            execute("INSERT INTO quotes VALUES ('MSFT', 199.5, 200.5, '2024-01-01T11:30:00.000000Z')"); // Between GOOGL and MSFT trades
            execute("INSERT INTO quotes VALUES ('AAPL', 150.5, 151.5, '2024-01-01T12:30:00.000000Z')");  // Between MSFT and 2nd AAPL trade
            execute("INSERT INTO quotes VALUES ('GOOGL', 100.5, 101.5, '2024-01-01T13:30:00.000000Z')"); // Between 2nd AAPL and 2nd GOOGL
            execute("INSERT INTO quotes VALUES ('TSLA', 249.5, 250.5, '2024-01-01T14:30:00.000000Z')"); // Between 2nd GOOGL and TSLA trade
            execute("INSERT INTO quotes VALUES ('AAPL', 151.5, 152.5, '2024-01-01T15:30:00.000000Z')");  // Between TSLA and 3rd AAPL
            execute("INSERT INTO quotes VALUES ('MSFT', 200.5, 201.5, '2024-01-01T16:30:00.000000Z')"); // Between 3rd AAPL and 2nd MSFT

            // Create a small table to CROSS JOIN with - this will cause toTop() to be called
            execute("CREATE TABLE multiplier (id INT)");
            execute("INSERT INTO multiplier VALUES (1)");
            execute("INSERT INTO multiplier VALUES (2)");

            // CROSS JOIN will iterate through the ASOF JOIN result multiple times,
            // calling toTop() on the ASOF JOIN cursor for each row in the multiplier table
            String query = """
                    SELECT /*+ ASOF_MEMOIZED(t q) */ m.id, asof_result.* FROM multiplier m
                    CROSS JOIN (
                      SELECT t.symbol, t.price, q.bid, q.ask
                      FROM trades t
                      ASOF JOIN quotes q ON t.symbol = q.symbol
                    ) asof_result
                    """;

            // Expected results: Each trade matches with the most recent quote at or before its timestamp
            // The pattern repeats twice (once for each multiplier row), testing that toTop() properly clears state
            String expected = """
                    id\tsymbol\tprice\tbid\task
                    1\tAAPL\t150.0\t149.5\t150.5
                    1\tGOOGL\t100.0\t99.5\t100.5
                    1\tMSFT\t200.0\t199.5\t200.5
                    1\tAAPL\t151.0\t150.5\t151.5
                    1\tGOOGL\t101.0\t100.5\t101.5
                    1\tTSLA\t250.0\t249.5\t250.5
                    1\tAAPL\t152.0\t151.5\t152.5
                    1\tMSFT\t201.0\t200.5\t201.5
                    2\tAAPL\t150.0\t149.5\t150.5
                    2\tGOOGL\t100.0\t99.5\t100.5
                    2\tMSFT\t200.0\t199.5\t200.5
                    2\tAAPL\t151.0\t150.5\t151.5
                    2\tGOOGL\t101.0\t100.5\t101.5
                    2\tTSLA\t250.0\t249.5\t250.5
                    2\tAAPL\t152.0\t151.5\t152.5
                    2\tMSFT\t201.0\t200.5\t201.5
                    """;

            assertQueryNoLeakCheck(expected, query, null, false, true);

            // Verify the plan uses Memoized scan
            sink.clear();
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "AsOf Join Memoized Scan");
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
    public void testImplicitTimestampPropagationWontCauseAmbiguity() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    "create table t1 (x int, ts #TIMESTAMP) timestamp(ts) partition by day",
                    leftTableTimestampType.getTypeName()
            );

            execute("insert into t1 values (1, '2022-10-05T08:15:00.000000Z')");
            execute("insert into t1 values (2, '2022-10-05T08:17:00.000000Z')");
            execute("insert into t1 values (3, '2022-10-05T08:21:00.000000Z')");

            executeWithRewriteTimestamp(
                    "create table t2 (x int, ts #TIMESTAMP) timestamp(ts) partition by day",
                    rightTableTimestampType.getTypeName()
            );
            execute("insert into t2 values (4, '2022-10-05T08:18:00.000000Z')");
            execute("insert into t2 values (5, '2022-10-05T08:19:00.000000Z')");
            execute("insert into t2 values (6, '2023-10-05T09:00:00.000000Z')");

            assertQuery("ts\n" +
                            "2022-10-05T08:15:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\n" +
                            "2022-10-05T08:17:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\n" +
                            "2022-10-05T08:21:00.000000" + getTimestampSuffix(leftTableTimestampType.getTypeName()) + "\n",
                    "select ts from t1 asof join (select x from t2)",
                    null, "ts", false, true);
        });
    }

    @Test
    public void testInterleaved1() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:17:00.000000Z', 1, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:21:00.000000Z', 2, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:21:00.000000Z', 2, 'b');");
            execute("INSERT INTO t1 values ('2022-10-10T01:01:00.000000Z', 3, 'a');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2022-10-05T08:18:00.000000Z', 4, 'a');");
            execute("INSERT INTO t2 values ('2022-10-05T08:19:00.000000Z', 5, 'a');");
            execute("INSERT INTO t2 values ('2023-10-05T09:00:00.000000Z', 6, 'a');");
            execute("INSERT INTO t2 values ('2023-10-06T01:00:00.000000Z', 7, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testInterleaved2() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2000-02-07T22:00:00.000000Z', 1, 'a');");
            execute("INSERT INTO t1 values ('2000-02-08T06:00:00.000000Z', 2, 'a');");
            execute("INSERT INTO t1 values ('2000-02-08T19:00:00.000000Z', 3, 'a');");
            execute("INSERT INTO t1 values ('2000-02-08T19:00:00.000000Z', 3, 'b');");
            execute("INSERT INTO t1 values ('2000-02-09T16:00:00.000000Z', 4, 'a');");
            execute("INSERT INTO t1 values ('2000-02-09T16:00:00.000000Z', 5, 'a');");
            execute("INSERT INTO t1 values ('2000-02-10T06:00:00.000000Z', 6, 'a');");
            execute("INSERT INTO t1 values ('2000-02-10T06:00:00.000000Z', 6, 'b');");
            execute("INSERT INTO t1 values ('2000-02-10T19:00:00.000000Z', 7, 'a');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2000-02-07T14:00:00.000000Z', 8, 'a');");
            execute("INSERT INTO t2 values ('2000-02-08T02:00:00.000000Z', 9, 'a');");
            execute("INSERT INTO t2 values ('2000-02-08T02:00:00.000000Z', 10, 'a');");
            execute("INSERT INTO t2 values ('2000-02-08T02:00:00.000000Z', 10, 'c');");
            execute("INSERT INTO t2 values ('2000-02-08T21:00:00.000000Z', 11, 'a');");
            execute("INSERT INTO t2 values ('2000-02-09T15:00:00.000000Z', 12, 'a');");
            execute("INSERT INTO t2 values ('2000-02-09T20:00:00.000000Z', 13, 'a');");
            execute("INSERT INTO t2 values ('2000-02-09T20:00:00.000000Z', 13, 'c');");
            execute("INSERT INTO t2 values ('2000-02-10T16:00:00.000000Z', 14, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testIssue2976() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                executeWithRewriteTimestamp("""
                        CREATE TABLE 'tests' (
                          Ticker SYMBOL capacity 256 CACHE,
                          ts #TIMESTAMP
                        ) timestamp (ts) PARTITION BY MONTH""", leftTableTimestampType.getTypeName());
                execute("INSERT INTO tests VALUES " +
                        "('AAPL', '2000')," +
                        "('AAPL', '2001')," +
                        "('AAPL', '2002')," +
                        "('AAPL', '2003')," +
                        "('AAPL', '2004')," +
                        "('AAPL', '2005')"
                );

                String query = """
                        SELECT *
                        FROM tests t0
                        LT JOIN (
                          SELECT *
                          FROM tests t1
                          LT JOIN (
                              SELECT *
                              FROM tests t2
                              LT JOIN (
                                  SELECT * FROM tests t3
                              ) ON (Ticker)
                          ) ON (Ticker)
                        ) ON (Ticker)
                        """;
                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                String expected = "Ticker\tts\tTicker1\tts1\tTicker11\tts11\tTicker111\tts111\n" +
                        "AAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\t\t\n" +
                        "AAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\n" +
                        "AAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                        "AAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "AAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "AAPL\t2005-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testJoinOnSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x (sym SYMBOL, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE y (sym SYMBOL, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            execute(
                    "INSERT INTO x VALUES " +
                            "('1', '2000-01-01T00:00:00.000000Z')," +
                            "('3', '2000-01-01T00:00:01.000000Z')," +
                            "('1', '2000-01-01T00:00:02.000000Z')," +
                            "('2', '2000-01-01T00:00:03.000000Z')," +
                            "('4', '2000-01-01T00:00:04.000000Z')"
            );
            execute(
                    "INSERT INTO y VALUES " +
                            "('2', '2000-01-01T00:00:00.000000Z')," +
                            "('4', '2000-01-01T00:00:01.000000Z')," +
                            "('1', '2000-01-01T00:00:02.000000Z')," +
                            "('2', '2000-01-01T00:00:03.000000Z')," +
                            "('3', '2000-01-01T00:00:04.000000Z')"
            );

            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            // ASOF JOIN
            String query = "SELECT * FROM (select sym, ts from x) x " +
                    "ASOF JOIN (select sym, ts from y) y ON(sym)";
            String expected = "sym\tts\tsym1\tts1\n" +
                    "1\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                    "3\t2000-01-01T00:00:01.000000" + leftSuffix + "\t\t\n" +
                    "1\t2000-01-01T00:00:02.000000" + leftSuffix + "\t1\t2000-01-01T00:00:02.000000" + rightSuffix + "\n" +
                    "2\t2000-01-01T00:00:03.000000" + leftSuffix + "\t2\t2000-01-01T00:00:03.000000" + rightSuffix + "\n" +
                    "4\t2000-01-01T00:00:04.000000" + leftSuffix + "\t4\t2000-01-01T00:00:01.000000" + rightSuffix + "\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // LT JOIN
            query = "SELECT * FROM (select sym, ts from x) x " +
                    "LT JOIN (select sym, ts from y) y ON(sym)";
            expected = "sym\tts\tsym1\tts1\n" +
                    "1\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                    "3\t2000-01-01T00:00:01.000000" + leftSuffix + "\t\t\n" +
                    "1\t2000-01-01T00:00:02.000000" + leftSuffix + "\t\t\n" +
                    "2\t2000-01-01T00:00:03.000000" + leftSuffix + "\t2\t2000-01-01T00:00:00.000000" + rightSuffix + "\n" +
                    "4\t2000-01-01T00:00:04.000000" + leftSuffix + "\t4\t2000-01-01T00:00:01.000000" + rightSuffix + "\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // SPLICE JOIN
            query = "SELECT * FROM (select sym, ts from x) x " +
                    "SPLICE JOIN (select sym, ts from y) y ON(sym)";
            expected = "sym\tts\tsym1\tts1\n" +
                    "1\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                    "3\t2000-01-01T00:00:01.000000" + leftSuffix + "\t\t\n" +
                    "1\t2000-01-01T00:00:02.000000" + leftSuffix + "\t1\t2000-01-01T00:00:02.000000" + rightSuffix + "\n" +
                    "2\t2000-01-01T00:00:03.000000" + leftSuffix + "\t2\t2000-01-01T00:00:03.000000" + rightSuffix + "\n" +
                    "4\t2000-01-01T00:00:04.000000" + leftSuffix + "\t\t\n";
            assertQueryNoLeakCheck(expected, query, null, false, false);
        });
    }

    @Test
    public void testJoinStringOnSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x (sym STRING, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE y (sym SYMBOL INDEX, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            execute(
                    "INSERT INTO x VALUES " +
                            "('1', '2000-01-01T00:00:00.000000Z')," +
                            "('3', '2000-01-01T00:00:01.000000Z')," +
                            "('1', '2000-01-01T00:00:02.000000Z')," +
                            "('2', '2000-01-01T00:00:03.000000Z')," +
                            "('4', '2000-01-01T00:00:04.000000Z')," +
                            "(null, '2000-01-01T00:00:03.000000Z')," +
                            "('не-ASCII', '2000-01-01T00:00:03.000000Z')"
            );
            execute(
                    "INSERT INTO y VALUES " +
                            "('2', '2000-01-01T00:00:00.000000Z')," +
                            "('4', '2000-01-01T00:00:01.000000Z')," +
                            "('1', '2000-01-01T00:00:02.000000Z')," +
                            "('2', '2000-01-01T00:00:03.000000Z')," +
                            "('3', '2000-01-01T00:00:04.000000Z')"
            );

            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            // LT JOIN
            String query = "SELECT * FROM x LT JOIN y ON(sym)";
            String expected = "sym\tts\tsym1\tts1\n" +
                    "1\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                    "3\t2000-01-01T00:00:01.000000" + leftSuffix + "\t\t\n" +
                    "1\t2000-01-01T00:00:02.000000" + leftSuffix + "\t\t\n" +
                    "\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "не-ASCII\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "2\t2000-01-01T00:00:03.000000" + leftSuffix + "\t2\t2000-01-01T00:00:00.000000" + rightSuffix + "\n" +
                    "4\t2000-01-01T00:00:04.000000" + leftSuffix + "\t4\t2000-01-01T00:00:01.000000" + rightSuffix + "\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // ASOF JOIN
            String queryBody = "* FROM x ASOF JOIN y ON(sym)";
            query = "SELECT " + queryBody;
            expected = "sym\tts\tsym1\tts1\n" +
                    "1\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                    "3\t2000-01-01T00:00:01.000000" + leftSuffix + "\t\t\n" +
                    "1\t2000-01-01T00:00:02.000000" + leftSuffix + "\t1\t2000-01-01T00:00:02.000000" + rightSuffix + "\n" +
                    "\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "не-ASCII\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "2\t2000-01-01T00:00:03.000000" + leftSuffix + "\t2\t2000-01-01T00:00:03.000000" + rightSuffix + "\n" +
                    "4\t2000-01-01T00:00:04.000000" + leftSuffix + "\t4\t2000-01-01T00:00:01.000000" + rightSuffix + "\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            String hintedQuery = "SELECT /*+ asof_index_search(x y) */ " + queryBody;
            printSql("EXPLAIN " + hintedQuery);
            TestUtils.assertContains(sink, "AsOf Join Indexed");
            assertSql(expected, hintedQuery);
            assertQueryNoLeakCheck(expected, hintedQuery, "ts", false, true);
        });
    }

    @Test
    public void testJoinVarcharOnSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x (sym VARCHAR, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE y (sym SYMBOL INDEX, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            execute(
                    "INSERT INTO x VALUES " +
                            "('😊', '2000-01-01T00:00:00.000000Z')," +
                            "('3', '2000-01-01T00:00:01.000000Z')," +
                            "('😊', '2000-01-01T00:00:02.000000Z')," +
                            "('2', '2000-01-01T00:00:03.000000Z')," +
                            "('4', '2000-01-01T00:00:04.000000Z')," +
                            "(null, '2000-01-01T00:00:03.000000Z')," +
                            "('не-ASCII', '2000-01-01T00:00:03.000000Z')"
            );
            execute(
                    "INSERT INTO y VALUES " +
                            "('2', '2000-01-01T00:00:00.000000Z')," +
                            "('4', '2000-01-01T00:00:01.000000Z')," +
                            "('😊', '2000-01-01T00:00:02.000000Z')," +
                            "('2', '2000-01-01T00:00:03.000000Z')," +
                            "('3', '2000-01-01T00:00:04.000000Z')"
            );

            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            // LT JOIN
            String query = "SELECT * FROM x LT JOIN y ON(sym)";
            String expected = "sym\tts\tsym1\tts1\n" +
                    "😊\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                    "3\t2000-01-01T00:00:01.000000" + leftSuffix + "\t\t\n" +
                    "😊\t2000-01-01T00:00:02.000000" + leftSuffix + "\t\t\n" +
                    "\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "не-ASCII\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "2\t2000-01-01T00:00:03.000000" + leftSuffix + "\t2\t2000-01-01T00:00:00.000000" + rightSuffix + "\n" +
                    "4\t2000-01-01T00:00:04.000000" + leftSuffix + "\t4\t2000-01-01T00:00:01.000000" + rightSuffix + "\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // ASOF JOIN
            String queryBody = "* FROM x ASOF JOIN y ON(sym)";
            query = "SELECT " + queryBody;
            expected = "sym\tts\tsym1\tts1\n" +
                    "😊\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                    "3\t2000-01-01T00:00:01.000000" + leftSuffix + "\t\t\n" +
                    "😊\t2000-01-01T00:00:02.000000" + leftSuffix + "\t😊\t2000-01-01T00:00:02.000000" + rightSuffix + "\n" +
                    "\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "не-ASCII\t2000-01-01T00:00:03.000000" + leftSuffix + "\t\t\n" +
                    "2\t2000-01-01T00:00:03.000000" + leftSuffix + "\t2\t2000-01-01T00:00:03.000000" + rightSuffix + "\n" +
                    "4\t2000-01-01T00:00:04.000000" + leftSuffix + "\t4\t2000-01-01T00:00:01.000000" + rightSuffix + "\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            String hintedQuery = "SELECT /*+ asof_index_search(x y) */ " + queryBody;
            printSql("EXPLAIN " + hintedQuery);
            TestUtils.assertContains(sink, "AsOf Join Indexed");
            assertSql(expected, hintedQuery);
            assertQueryNoLeakCheck(expected, hintedQuery, "ts", false, true);
        });
    }

    @Test
    public void testLtJoin2TablesKeyed() throws Exception {
        assertMemoryLeak(() -> {
            //tabY
            executeWithRewriteTimestamp("create table tabY (tag symbol, x long, ts #TIMESTAMP) timestamp(ts)", leftTableTimestampType.getTypeName());
            execute("insert into tabY values ('A', 1, 10000::timestamp)");
            execute("insert into tabY values ('A', 2, 20000::timestamp)");
            execute("insert into tabY values ('A', 3, 30000::timestamp)");
            execute("insert into tabY values ('B', 1, 30000::timestamp)");
            execute("insert into tabY values ('B', 2, 40000::timestamp)");
            execute("insert into tabY values ('B', 3, 50000::timestamp)");
            //tabZ
            executeWithRewriteTimestamp("create table tabZ (tag symbol, x long, ts #TIMESTAMP) timestamp(ts)", rightTableTimestampType.getTypeName());
            execute("insert into tabZ values ('B', 1, 10000::timestamp)");
            execute("insert into tabZ values ('B', 2, 20000::timestamp)");
            execute("insert into tabZ values ('B', 3, 30000::timestamp)");
            execute("insert into tabZ values ('A', 3, 30000::timestamp)");
            execute("insert into tabZ values ('A', 6, 40000::timestamp)");
            execute("insert into tabZ values ('A', 7, 50000::timestamp)");
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            //check tables
            String ex = "tag\tx\tts\n" +
                    "A\t1\t1970-01-01T00:00:00.010000" + leftSuffix + "\n" +
                    "A\t2\t1970-01-01T00:00:00.020000" + leftSuffix + "\n" +
                    "A\t3\t1970-01-01T00:00:00.030000" + leftSuffix + "\n" +
                    "B\t1\t1970-01-01T00:00:00.030000" + leftSuffix + "\n" +
                    "B\t2\t1970-01-01T00:00:00.040000" + leftSuffix + "\n" +
                    "B\t3\t1970-01-01T00:00:00.050000" + leftSuffix + "\n";
            printSqlResult(ex, "tabY", "ts", true, true);
            ex = "tag\tx\tts\n" +
                    "B\t1\t1970-01-01T00:00:00.010000" + rightSuffix + "\n" +
                    "B\t2\t1970-01-01T00:00:00.020000" + rightSuffix + "\n" +
                    "B\t3\t1970-01-01T00:00:00.030000" + rightSuffix + "\n" +
                    "A\t3\t1970-01-01T00:00:00.030000" + rightSuffix + "\n" +
                    "A\t6\t1970-01-01T00:00:00.040000" + rightSuffix + "\n" +
                    "A\t7\t1970-01-01T00:00:00.050000" + rightSuffix + "\n";
            printSqlResult(ex, "tabZ", "ts", true, true);
            // test
            ex = """
                    tag\thi\tlo
                    A\t1\tnull
                    A\t2\tnull
                    A\t3\tnull
                    B\t1\t2
                    B\t2\t3
                    B\t3\t3
                    """;
            String query = "select a.tag, a.x hi, b.x lo from tabY a lt join tabZ b on (tag) ";
            printSqlResult(ex, query, null, false, true);
        });
    }

    @Test
    public void testLtJoinForEqTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            executeWithRewriteTimestamp("create table tank(ts #TIMESTAMP, SequenceNumber int) timestamp(ts)", leftTableTimestampType.getTypeName());
            execute("insert into tank values('2021-07-26T02:36:02.566000Z',1)");
            execute("insert into tank values('2021-07-26T02:36:03.094000Z',2)");
            execute("insert into tank values('2021-07-26T02:36:03.097000Z',3)");
            execute("insert into tank values('2021-07-26T02:36:03.097000Z',4)");
            execute("insert into tank values('2021-07-26T02:36:03.097000Z',5)");
            execute("insert into tank values('2021-07-26T02:36:03.097000Z',6)");
            execute("insert into tank values('2021-07-26T02:36:03.098000Z',7)");
            execute("insert into tank values('2021-07-26T02:36:03.098000Z',8)");

            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());

            String expected = "ts\tSequenceNumber\tSequenceNumber1\tcolumn\n" +
                    "2021-07-26T02:36:02.566000" + leftSuffix + "\t1\tnull\tnull\n" +
                    "2021-07-26T02:36:03.094000" + leftSuffix + "\t2\t1\t1\n" +
                    "2021-07-26T02:36:03.097000" + leftSuffix + "\t3\t2\t1\n" +
                    "2021-07-26T02:36:03.097000" + leftSuffix + "\t4\t2\t2\n" +
                    "2021-07-26T02:36:03.097000" + leftSuffix + "\t5\t2\t3\n" +
                    "2021-07-26T02:36:03.097000" + leftSuffix + "\t6\t2\t4\n" +
                    "2021-07-26T02:36:03.098000" + leftSuffix + "\t7\t6\t1\n" +
                    "2021-07-26T02:36:03.098000" + leftSuffix + "\t8\t6\t2\n";
            String query = "select w1.ts ts, w1.SequenceNumber, w2.SequenceNumber, w1.SequenceNumber - w2.SequenceNumber from tank w1 lt join tank w2";
            printSqlResult(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testLtJoinForSelectWithoutTimestampAndWithWhereStatement() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        final String expected = """
                hi\tlo
                18116\t18114
                48689\t48687
                57275\t57273
                63855\t63853
                72763\t72761
                87011\t87009
                87113\t87111
                91369\t91367
                """;
        executeWithRewriteTimestamp("create table test(seq long, ts #TIMESTAMP) timestamp(ts)", leftTableTimestampType.getTypeName());
        assertQuery(
                "hi\tlo\n",
                "(select a.seq hi, b.seq lo from test a lt join test b) where hi > lo + 1",
                null,
                false
        );
        execute("insert into test select x, cast(x+10 as timestamp) from (select x, rnd_double() rnd from long_sequence(100000)) where rnd<0.9999");

        assertQuery(
                expected,
                "(select a.seq hi, b.seq lo from test a lt join test b) where hi > lo + 1",
                null,
                false
        );
    }

    @Test
    public void testLtJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            final String query = "select x.i, x.sym, x.amt, price, x.timestamp, y.timestamp from x lt join y on y.sym2 = x.sym";

            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            final String expected = "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                    "1\tmsft\t22.463\tnull\t2018-01-01T00:12:00.000000" + leftSuffix + "\t\n" +
                    "2\tgoogl\t29.92\t0.423\t2018-01-01T00:24:00.000000" + leftSuffix + "\t2018-01-01T00:16:00.000000" + rightSuffix + "\n" +
                    "3\tmsft\t65.086\t0.456\t2018-01-01T00:36:00.000000" + leftSuffix + "\t2018-01-01T00:32:00.000000" + rightSuffix + "\n" +
                    "4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000" + leftSuffix + "\t2018-01-01T00:34:00.000000" + rightSuffix + "\n" +
                    "5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000" + leftSuffix + "\t2018-01-01T00:46:00.000000" + rightSuffix + "\n" +
                    "6\tibm\t76.11\t0.9540000000000001\t2018-01-01T01:12:00.000000" + leftSuffix + "\t2018-01-01T00:56:00.000000" + rightSuffix + "\n" +
                    "7\tmsft\t55.992000000000004\t0.545\t2018-01-01T01:24:00.000000" + leftSuffix + "\t2018-01-01T00:46:00.000000" + rightSuffix + "\n" +
                    "8\tibm\t23.905\t0.9540000000000001\t2018-01-01T01:36:00.000000" + leftSuffix + "\t2018-01-01T00:56:00.000000" + rightSuffix + "\n" +
                    "9\tgoogl\t67.786\t0.198\t2018-01-01T01:48:00.000000" + leftSuffix + "\t2018-01-01T01:00:00.000000" + rightSuffix + "\n" +
                    "10\tgoogl\t38.54\t0.198\t2018-01-01T02:00:00.000000" + leftSuffix + "\t2018-01-01T01:00:00.000000" + rightSuffix + "\n";
            executeWithRewriteTimestamp("""
                    CREATE TABLE 'x' (\s
                    \ti INT,
                    \tsym SYMBOL CAPACITY 128 CACHE,
                    \tamt DOUBLE,
                    \ttimestamp #TIMESTAMP
                    ) timestamp(timestamp)""", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("""
                    CREATE TABLE 'y' (\s
                    \ti INT,
                    \tsym2 SYMBOL CAPACITY 128 CACHE,
                    \tprice DOUBLE,
                    \ttimestamp #TIMESTAMP
                    ) timestamp(timestamp)""", rightTableTimestampType.getTypeName());

            execute("""
                    insert into x
                    select
                     cast(x as int),
                     rnd_symbol('msft','ibm', 'googl'),
                     round(rnd_double(0)*100, 3),
                     to_timestamp('2018-01', 'yyyy-MM') + x * 720000000
                     from long_sequence(10)
                    """
            );

            execute("""
                    insert into y
                    select cast(x as int),
                     rnd_symbol('msft','ibm', 'googl'),
                     round(rnd_double(0), 3),
                     to_timestamp('2018-01', 'yyyy-MM') + x * 120000000
                     from long_sequence(30)
                    """
            );
            assertQueryAndCacheFullFat(expected, query, "timestamp", false, true);

            execute("""
                    insert into x select * from (
                    select
                     cast(x + 10 as int) i,
                     rnd_symbol('msft','ibm', 'googl') sym,
                     round(rnd_double(0)*100, 3) amt,
                     to_timestamp('2018-01', 'yyyy-MM') + (x + 10) * 720000000 timestamp
                     from long_sequence(10)
                    ) timestamp(timestamp)
                    """
            );

            execute("""
                    insert into y select * from (
                    select
                     cast(x + 30 as int) i,
                     rnd_symbol('msft','ibm', 'googl') sym2,
                     round(rnd_double(0), 3) price,
                     to_timestamp('2018-01', 'yyyy-MM') + (x + 30) * 120000000 timestamp
                     from long_sequence(30)
                    ) timestamp(timestamp)
                    """
            );

            assertQueryFullFatNoLeakCheck(
                    "i\tsym\tamt\tprice\ttimestamp\ttimestamp1\n" +
                            "1\tmsft\t22.463\tnull\t2018-01-01T00:12:00.000000" + leftSuffix + "\t\n" +
                            "2\tgoogl\t29.92\t0.423\t2018-01-01T00:24:00.000000" + leftSuffix + "\t2018-01-01T00:16:00.000000" + rightSuffix + "\n" +
                            "3\tmsft\t65.086\t0.456\t2018-01-01T00:36:00.000000" + leftSuffix + "\t2018-01-01T00:32:00.000000" + rightSuffix + "\n" +
                            "4\tibm\t98.563\t0.405\t2018-01-01T00:48:00.000000" + leftSuffix + "\t2018-01-01T00:34:00.000000" + rightSuffix + "\n" +
                            "5\tmsft\t50.938\t0.545\t2018-01-01T01:00:00.000000" + leftSuffix + "\t2018-01-01T00:46:00.000000" + rightSuffix + "\n" +
                            "6\tibm\t76.11\t0.427\t2018-01-01T01:12:00.000000" + leftSuffix + "\t2018-01-01T01:10:00.000000" + rightSuffix + "\n" +
                            "7\tmsft\t55.992000000000004\t0.226\t2018-01-01T01:24:00.000000" + leftSuffix + "\t2018-01-01T01:16:00.000000" + rightSuffix + "\n" +
                            "8\tibm\t23.905\t0.029\t2018-01-01T01:36:00.000000" + leftSuffix + "\t2018-01-01T01:34:00.000000" + rightSuffix + "\n" +
                            "9\tgoogl\t67.786\t0.076\t2018-01-01T01:48:00.000000" + leftSuffix + "\t2018-01-01T01:46:00.000000" + rightSuffix + "\n" +
                            "10\tgoogl\t38.54\t0.339\t2018-01-01T02:00:00.000000" + leftSuffix + "\t2018-01-01T01:58:00.000000" + rightSuffix + "\n" +
                            "11\tmsft\t68.069\t0.051000000000000004\t2018-01-01T02:12:00.000000" + leftSuffix + "\t2018-01-01T01:50:00.000000" + rightSuffix + "\n" +
                            "12\tmsft\t24.008\t0.051000000000000004\t2018-01-01T02:24:00.000000" + leftSuffix + "\t2018-01-01T01:50:00.000000" + rightSuffix + "\n" +
                            "13\tgoogl\t94.559\t0.6900000000000001\t2018-01-01T02:36:00.000000" + leftSuffix + "\t2018-01-01T02:00:00.000000" + rightSuffix + "\n" +
                            "14\tibm\t62.474000000000004\t0.068\t2018-01-01T02:48:00.000000" + leftSuffix + "\t2018-01-01T01:40:00.000000" + rightSuffix + "\n" +
                            "15\tmsft\t39.017\t0.051000000000000004\t2018-01-01T03:00:00.000000" + leftSuffix + "\t2018-01-01T01:50:00.000000" + rightSuffix + "\n" +
                            "16\tgoogl\t10.643\t0.6900000000000001\t2018-01-01T03:12:00.000000" + leftSuffix + "\t2018-01-01T02:00:00.000000" + rightSuffix + "\n" +
                            "17\tmsft\t7.246\t0.051000000000000004\t2018-01-01T03:24:00.000000" + leftSuffix + "\t2018-01-01T01:50:00.000000" + rightSuffix + "\n" +
                            "18\tmsft\t36.798\t0.051000000000000004\t2018-01-01T03:36:00.000000" + leftSuffix + "\t2018-01-01T01:50:00.000000" + rightSuffix + "\n" +
                            "19\tmsft\t66.98\t0.051000000000000004\t2018-01-01T03:48:00.000000" + leftSuffix + "\t2018-01-01T01:50:00.000000" + rightSuffix + "\n" +
                            "20\tgoogl\t26.369\t0.6900000000000001\t2018-01-01T04:00:00.000000" + leftSuffix + "\t2018-01-01T02:00:00.000000" + rightSuffix + "\n",
                    query,
                    "timestamp",
                    false,
                    true,
                    true
            );
        });
    }

    @Test
    public void testLtJoinHighCardinalityKeysAndTolerance() throws Exception {
        // this tests set low threshold for evacuation of full fat ASOF join map
        // and compares that Fast and FullFat results are the same

        setProperty(PropertyKey.CAIRO_SQL_ASOF_JOIN_EVACUATION_THRESHOLD, "10");
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE master (vch VARCHAR, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE slave (vch VARCHAR, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

            execute("""
                    INSERT INTO master SELECT
                    rnd_int()::varchar as vch,
                    timestamp_sequence(0, 1000000) + x * 1500000 as ts
                    FROM long_sequence(1_000)
                    """
            );

            execute("""
                    INSERT INTO slave SELECT
                    rnd_int()::varchar as vch,
                    timestamp_sequence(0, 1000000) + x * 1000000 as ts
                    FROM long_sequence(1_000)
                    """
            );

            String query = "SELECT * FROM master LT JOIN slave y ON(vch) TOLERANCE 1s";
            printSql("EXPLAIN " + query, true);
            TestUtils.assertNotContains(sink, "Lt Join Light");
            printSql(query, true);
            String fullFatResult = sink.toString();

            printSql("EXPLAIN " + query, false);
            TestUtils.assertContains(sink, "Lt Join Light");
            printSql(query, false);
            String lightResult = sink.toString();
            TestUtils.assertEquals(fullFatResult, lightResult);
        });
    }

    @Test
    public void testLtJoinKeyed() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
        final String expected = "tag\thi\tlo\tts\tts1\n" +
                "AA\t315515118\tnull\t1970-01-03T00:00:00.000000" + leftSuffix + "\t\n" +
                "BB\t-727724771\tnull\t1970-01-03T00:06:00.000000" + leftSuffix + "\t\n" +
                "CC\t-948263339\tnull\t1970-01-03T00:12:00.000000" + leftSuffix + "\t\n" +
                "CC\t592859671\t-948263339\t1970-01-03T00:18:00.000000" + leftSuffix + "\t1970-01-03T00:12:00.000000" + leftSuffix + "\n" +
                "AA\t-847531048\t315515118\t1970-01-03T00:24:00.000000" + leftSuffix + "\t1970-01-03T00:00:00.000000" + leftSuffix + "\n" +
                "BB\t-2041844972\t-727724771\t1970-01-03T00:30:00.000000" + leftSuffix + "\t1970-01-03T00:06:00.000000" + leftSuffix + "\n" +
                "BB\t-1575378703\t-2041844972\t1970-01-03T00:36:00.000000" + leftSuffix + "\t1970-01-03T00:30:00.000000" + leftSuffix + "\n" +
                "BB\t1545253512\t-1575378703\t1970-01-03T00:42:00.000000" + leftSuffix + "\t1970-01-03T00:36:00.000000" + leftSuffix + "\n" +
                "AA\t1573662097\t-847531048\t1970-01-03T00:48:00.000000" + leftSuffix + "\t1970-01-03T00:24:00.000000" + leftSuffix + "\n" +
                "AA\t339631474\t1573662097\t1970-01-03T00:54:00.000000" + leftSuffix + "\t1970-01-03T00:48:00.000000" + leftSuffix + "\n";
        executeWithRewriteTimestamp(
                """
                        create table tab (
                            tag symbol index,
                            seq int,
                            ts #TIMESTAMP
                        ) timestamp(ts) partition by DAY""",
                leftTableTimestampType.getTypeName()
        );
        assertQuery(
                "tag\thi\tlo\tts\tts1\n",
                "select a.tag, a.seq hi, b.seq lo , a.ts, b.ts from tab a lt join tab b on (tag)",
                "ts",
                false,
                true
        );
        execute(
                """
                        insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag,\s
                                rnd_int() seq,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)"""
        );
        assertQuery(
                expected,
                "select a.tag, a.seq hi, b.seq lo , a.ts, b.ts from tab a lt join tab b on (tag)",
                "ts",
                false,
                true
        );
    }

    @Test
    public void testLtJoinNoAliasDuplication() throws Exception {
        assertMemoryLeak(() -> {
            // ASKS
            executeWithRewriteTimestamp("create table asks(ask int, ts #TIMESTAMP) timestamp(ts) partition by none", leftTableTimestampType.getTypeName());
            execute("insert into asks values(100, 0)");
            execute("insert into asks values(101, 3::timestamp);");
            execute("insert into asks values(102, 4::timestamp);");

            // BIDS
            executeWithRewriteTimestamp("create table bids(bid int, ts #TIMESTAMP) timestamp(ts) partition by none", rightTableTimestampType.getTypeName());
            execute("insert into bids values(101, 0);");
            execute("insert into bids values(102, 3::timestamp);");
            execute("insert into bids values(103, 5::timestamp);");

            String query =
                    """
                            SELECT\s
                                b.timebid timebid,
                                a.timeask timeask,\s
                                b.b b,\s
                                a.a a
                            FROM (select b.bid b, b.ts timebid from bids b) b\s
                                LT JOIN
                            (select a.ask a, a.ts timeask from asks a) a
                            WHERE (b.timebid != a.timeask);""";

            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

            String expected = "timebid\ttimeask\tb\ta\n" +
                    "1970-01-01T00:00:00.000000" + rightSuffix + "\t\t101\tnull\n" +
                    "1970-01-01T00:00:00.000003" + rightSuffix + "\t1970-01-01T00:00:00.000000" + leftSuffix + "\t102\t100\n" +
                    "1970-01-01T00:00:00.000005" + rightSuffix + "\t1970-01-01T00:00:00.000004" + leftSuffix + "\t103\t102\n";

            printSqlResult(expected, query, "timebid", false, false);
        });
    }

    // select a.seq hi, b.seq lo from tab a lt join b where hi > lo + 1
    @Test
    public void testLtJoinNoTimestamp() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        final String expected = """
                tag\thi\tlo
                AA\t315515118\tnull
                BB\t-727724771\tnull
                CC\t-948263339\tnull
                CC\t592859671\t-948263339
                AA\t-847531048\t315515118
                BB\t-2041844972\t-727724771
                BB\t-1575378703\t-2041844972
                BB\t1545253512\t-1575378703
                AA\t1573662097\t-847531048
                AA\t339631474\t1573662097
                """;
        executeWithRewriteTimestamp(
                """
                        create table tab (
                            tag symbol index,
                            seq int,
                            ts #TIMESTAMP
                        ) timestamp(ts) partition by DAY""",
                leftTableTimestampType.getTypeName()
        );
        assertQuery(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a lt join tab b on (tag)",
                null,
                false,
                true
        );
        execute("""
                insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag,\s
                        rnd_int() seq,\s
                        timestamp_sequence(172800000000, 360000000) ts\s
                    from long_sequence(10)) timestamp (ts)"""
        );
        assertQuery(
                expected,
                "select a.tag, a.seq hi, b.seq lo from tab a lt join tab b on (tag)",
                null,
                false,
                true
        );
    }

    @Test
    public void testLtJoinNonKeyed() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                executeWithRewriteTimestamp("CREATE TABLE bids (stock SYMBOL, exchange SYMBOL, ts #TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
                executeWithRewriteTimestamp("CREATE TABLE asks (stock SYMBOL, exchange SYMBOL, ts #TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

                execute("INSERT INTO bids VALUES " +
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

                execute("INSERT INTO asks VALUES " +
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

                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());

                String query = "SELECT * FROM bids LT JOIN asks";
                String expected = "stock\texchange\tts\ti\trating\tstock1\texchange1\tts1\ti1\trating1\n" +
                        "AAPL\tNASDAQ\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tGOOD\t\t\t\tnull\t\n" +
                        "AAPL\tLSE\t2000-01-01T00:00:00.000000" + leftSuffix + "\t4\tSCAM\t\t\t\tnull\t\n" +
                        "MSFT\tNASDAQ\t2000-01-01T00:00:00.000000" + leftSuffix + "\t7\tGOOD\t\t\t\tnull\t\n" +
                        "MSFT\tLSE\t2000-01-01T00:00:00.000000" + leftSuffix + "\t10\tUNKNOWN\t\t\t\tnull\t\n" +
                        "AAPL\tNASDAQ\t2001-01-01T00:00:00.000000" + leftSuffix + "\t2\tGOOD\tMSFT\tLSE\t2000-01-01T00:00:00.000000" + rightSuffix + "\t10\tGOOD\n" +
                        "AAPL\tLSE\t2001-01-01T00:00:00.000000" + leftSuffix + "\t5\tEXCELLENT\tMSFT\tLSE\t2000-01-01T00:00:00.000000" + rightSuffix + "\t10\tGOOD\n" +
                        "MSFT\tNASDAQ\t2001-01-01T00:00:00.000000" + leftSuffix + "\t8\tGOOD\tMSFT\tLSE\t2000-01-01T00:00:00.000000" + rightSuffix + "\t10\tGOOD\n" +
                        "MSFT\tLSE\t2001-01-01T00:00:00.000000" + leftSuffix + "\t11\tGOOD\tMSFT\tLSE\t2000-01-01T00:00:00.000000" + rightSuffix + "\t10\tGOOD\n" +
                        "AAPL\tLSE\t2002-01-01T00:00:00.000000" + leftSuffix + "\t6\tSCAM\tMSFT\tLSE\t2001-01-01T00:00:00.000000" + rightSuffix + "\t11\tSCAM\n" +
                        "MSFT\tNASDAQ\t2002-01-01T00:00:00.000000" + leftSuffix + "\t9\tSCAM\tMSFT\tLSE\t2001-01-01T00:00:00.000000" + rightSuffix + "\t11\tSCAM\n" +
                        "AAPL\tNASDAQ\t2002-01-01T00:00:00.000000" + leftSuffix + "\t3\tSCAM\tMSFT\tLSE\t2001-01-01T00:00:00.000000" + rightSuffix + "\t11\tSCAM\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testLtJoinOnCompositeSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());
                compiler.setFullFatJoins(true);
                // stock and exchange are composite keys
                // rating is also a symbol, but not used in a join key
                executeWithRewriteTimestamp("CREATE TABLE bids (stock SYMBOL, exchange SYMBOL, ts #TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
                executeWithRewriteTimestamp("CREATE TABLE asks (stock SYMBOL, exchange SYMBOL, ts #TIMESTAMP, i INT, rating SYMBOL) TIMESTAMP(ts) PARTITION BY DAY", rightTableTimestampType.getTypeName());

                execute("INSERT INTO bids VALUES " +
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

                execute("INSERT INTO asks VALUES " +
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
                        "AAPL\tNASDAQ\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tGOOD\t\t\t\tnull\t\n" +
                        "AAPL\tLSE\t2000-01-01T00:00:00.000000" + leftSuffix + "\t4\tSCAM\t\t\t\tnull\t\n" +
                        "MSFT\tNASDAQ\t2000-01-01T00:00:00.000000" + leftSuffix + "\t7\tGOOD\t\t\t\tnull\t\n" +
                        "MSFT\tLSE\t2000-01-01T00:00:00.000000" + leftSuffix + "\t10\tUNKNOWN\t\t\t\tnull\t\n" +
                        "AAPL\tNASDAQ\t2001-01-01T00:00:00.000000" + leftSuffix + "\t2\tGOOD\tAAPL\tNASDAQ\t2000-01-01T00:00:00.000000" + rightSuffix + "\t1\tGOOD\n" +
                        "AAPL\tLSE\t2001-01-01T00:00:00.000000" + leftSuffix + "\t5\tEXCELLENT\tAAPL\tLSE\t2000-01-01T00:00:00.000000" + rightSuffix + "\t4\tEXCELLENT\n" +
                        "MSFT\tNASDAQ\t2001-01-01T00:00:00.000000" + leftSuffix + "\t8\tGOOD\tMSFT\tNASDAQ\t2000-01-01T00:00:00.000000" + rightSuffix + "\t7\tEXCELLENT\n" +
                        "MSFT\tLSE\t2001-01-01T00:00:00.000000" + leftSuffix + "\t11\tGOOD\tMSFT\tLSE\t2000-01-01T00:00:00.000000" + rightSuffix + "\t10\tGOOD\n" +
                        "AAPL\tLSE\t2002-01-01T00:00:00.000000" + leftSuffix + "\t6\tSCAM\tAAPL\tLSE\t2001-01-01T00:00:00.000000" + rightSuffix + "\t5\tEXCELLENT\n" +
                        "MSFT\tNASDAQ\t2002-01-01T00:00:00.000000" + leftSuffix + "\t9\tSCAM\tMSFT\tNASDAQ\t2001-01-01T00:00:00.000000" + rightSuffix + "\t8\tGOOD\n" +
                        "AAPL\tNASDAQ\t2002-01-01T00:00:00.000000" + leftSuffix + "\t3\tSCAM\tAAPL\tNASDAQ\t2001-01-01T00:00:00.000000" + rightSuffix + "\t2\tEXCELLENT\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testLtJoinOnEmptyTable() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("create table t1 as (select x as id, cast(x as #TIMESTAMP) ts from long_sequence(5)) timestamp(ts) partition by day;", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("create table t2 (id long, ts #TIMESTAMP) timestamp(ts) partition by day;", rightTableTimestampType.getTypeName());

            final String query = "SELECT * FROM t1 LT JOIN t2 ON id;";
            final String expected = replaceTimestampSuffix("""
                    id\tts\tid1\tts1
                    1\t1970-01-01T00:00:00.000001Z\tnull\t
                    2\t1970-01-01T00:00:00.000002Z\tnull\t
                    3\t1970-01-01T00:00:00.000003Z\tnull\t
                    4\t1970-01-01T00:00:00.000004Z\tnull\t
                    5\t1970-01-01T00:00:00.000005Z\tnull\t
                    """, leftTableTimestampType.getTypeName());
            printSqlResult(expected, query, "ts", false, true);
        });
    }

    @Test
    public void testLtJoinOnRandomlyGeneratedColumn() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        final String expected = """
                tag\thi\tlo
                CC\t592859671\t-948263339
                BB\t-1575378703\t-2041844972
                BB\t1545253512\t-1575378703
                AA\t1573662097\t1545253512
                """;
        executeWithRewriteTimestamp(
                """
                        create table tab (
                            tag symbol index,
                            seq int,
                            ts #TIMESTAMP
                        ) timestamp(ts) partition by DAY""",
                leftTableTimestampType.getTypeName()
        );
        assertQuery(
                "tag\thi\tlo\n",
                "select a.tag, a.seq hi, b.seq lo from tab a lt join tab b where a.seq > b.seq + 1",
                null,
                false
        );
        execute("""
                insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') tag,\s
                        rnd_int() seq,\s
                        timestamp_sequence(172800000000, 360000000) ts\s
                    from long_sequence(10)) timestamp (ts)"""
        );
        assertQuery(
                expected,
                "select a.tag, a.seq hi, b.seq lo from tab a lt join tab b where a.seq > b.seq + 1",
                null,
                false
        );
    }

    @Test
    public void testLtJoinOnSymbolWithSyntheticMasterSymbol() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());
                compiler.setFullFatJoins(true);
                executeWithRewriteTimestamp("""
                                CREATE TABLE 'taba' (\s
                                ts #TIMESTAMP
                                ) timestamp(ts)""",
                        leftTableTimestampType.getTypeName()
                );

                executeWithRewriteTimestamp("""
                                CREATE TABLE 'tabb' (\s
                                ts #TIMESTAMP,
                                sym SYMBOL CAPACITY 128 CACHE
                                ) timestamp(ts)""",
                        rightTableTimestampType.getTypeName()
                );

                // create a master table - without a symbol column
                execute("insert into taba select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 10000000000000L) as ts from long_sequence(5)");

                // create a slave table - with a symbol column, with timestamps 1 microsecond before master timestamps
                execute("insert into tabb select timestamp_sequence(to_timestamp('2019-10-17T00:00:00', 'yyyy-MM-ddTHH:mm:ss') - 1, 10000000000000L) as ts, rnd_symbol('A', 'B', 'C') as sym from long_sequence(5)");
                // use a CTE to amend the master table with a synthetic symbol column
                String query = """
                        with s as (
                          select cast (s as symbol) synthetic_sym, ts
                          from (
                              SELECT
                                CASE
                                  WHEN ts % 3 = 0 THEN 'A'
                                  WHEN ts % 3 = 1 THEN 'B'
                                  ELSE 'C'
                                END as s, *
                              FROM taba
                            )
                          )
                        select * from s
                        lt join tabb on (s.synthetic_sym = tabb.sym);""";
                String expected = "synthetic_sym\tts\tts1\tsym\n" +
                        "A\t2019-10-17T00:00:00.000000" + leftSuffix + "\t2019-10-16T23:59:59.999999" + rightSuffix + "\tA\n" +
                        "B\t2020-02-09T17:46:40.000000" + leftSuffix + "\t\t\n" +
                        "C\t2020-06-04T11:33:20.000000" + leftSuffix + "\t\t\n" +
                        "A\t2020-09-28T05:20:00.000000" + leftSuffix + "\t2020-02-09T17:46:39.999999" + rightSuffix + "\tA\n" +
                        "B\t2021-01-21T23:06:40.000000" + leftSuffix + "\t2020-06-04T11:33:19.999999" + rightSuffix + "\tB\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testLtJoinOnSymbolsDifferentIDs() throws Exception {
        assertMemoryLeak(() -> {
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("create table x (s symbol, xi int, xts #TIMESTAMP) timestamp(xts)", leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("create table y (s symbol, yi int, yts #TIMESTAMP) timestamp(yts)", rightTableTimestampType.getTypeName());
            execute("insert into x values ('a', 0, '2000')");
            execute("insert into x values ('b', 1, '2001')");
            execute("insert into x values ('c', 2, '2001')");

            execute("insert into y values ('c', 0, '1990')");
            execute("insert into y values ('d', 1, '1991')");
            execute("insert into y values ('a', 2, '1992')");
            execute("insert into y values ('a', 3, '1993')");

            String query = "select * from x LT JOIN y on (s)";
            String expected = "s\txi\txts\ts1\tyi\tyts\n" +
                    "a\t0\t2000-01-01T00:00:00.000000" + leftSuffix + "\ta\t3\t1993-01-01T00:00:00.000000" + rightSuffix + "\n" +
                    "b\t1\t2001-01-01T00:00:00.000000" + leftSuffix + "\t\tnull\t\n" +
                    "c\t2\t2001-01-01T00:00:00.000000" + leftSuffix + "\tc\t0\t1990-01-01T00:00:00.000000" + rightSuffix + "\n";

            assertQueryNoLeakCheck(expected, query, "xts", false, true);
        });
    }

    @Test
    public void testLtJoinOneTableKeyed() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            // tabY
            executeWithRewriteTimestamp("create table tabY (tag symbol, x long, ts #TIMESTAMP) timestamp(ts)", leftTableTimestampType.getTypeName());
            execute("insert into tabY values ('A', 1, 10000::timestamp)");
            execute("insert into tabY values ('A', 2, 20000::timestamp)");
            execute("insert into tabY values ('A', 3, 30000::timestamp)");
            execute("insert into tabY values ('B', 1, 30000::timestamp)");
            execute("insert into tabY values ('B', 2, 40000::timestamp)");
            execute("insert into tabY values ('B', 3, 50000::timestamp)");
            // check tables
            String ex = "tag\tx\tts\n" +
                    "A\t1\t1970-01-01T00:00:00.010000" + leftSuffix + "\n" +
                    "A\t2\t1970-01-01T00:00:00.020000" + leftSuffix + "\n" +
                    "A\t3\t1970-01-01T00:00:00.030000" + leftSuffix + "\n" +
                    "B\t1\t1970-01-01T00:00:00.030000" + leftSuffix + "\n" +
                    "B\t2\t1970-01-01T00:00:00.040000" + leftSuffix + "\n" +
                    "B\t3\t1970-01-01T00:00:00.050000" + leftSuffix + "\n";
            printSqlResult(ex, "tabY", "ts", true, true);
            // test
            ex = """
                    tag\thi\tlo
                    A\t1\tnull
                    A\t2\t1
                    A\t3\t2
                    B\t1\tnull
                    B\t2\t1
                    B\t3\t2
                    """;
            String query = "select a.tag, a.x hi, b.x lo from tabY a lt join tabY b on (tag) ";
            printSqlResult(ex, query, null, false, true);
        });
    }

    @Test
    public void testLtJoinOneTableKeyedV2() throws Exception {
        assertMemoryLeak(() -> {
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            // tabY
            executeWithRewriteTimestamp("create table tabY (tag symbol, x long, ts #TIMESTAMP) timestamp(ts)", leftTableTimestampType.getTypeName());
            execute("insert into tabY values ('A', 1, 10000::timestamp)");
            execute("insert into tabY values ('A', 2, 20000::timestamp)");
            execute("insert into tabY values ('A', 3, 30000::timestamp)");
            execute("insert into tabY values ('B', 1, 40000::timestamp)");
            execute("insert into tabY values ('B', 2, 50000::timestamp)");
            execute("insert into tabY values ('B', 3, 60000::timestamp)");
            // check tables
            String ex = "tag\tx\tts\n" +
                    "A\t1\t1970-01-01T00:00:00.010000" + leftSuffix + "\n" +
                    "A\t2\t1970-01-01T00:00:00.020000" + leftSuffix + "\n" +
                    "A\t3\t1970-01-01T00:00:00.030000" + leftSuffix + "\n" +
                    "B\t1\t1970-01-01T00:00:00.040000" + leftSuffix + "\n" +
                    "B\t2\t1970-01-01T00:00:00.050000" + leftSuffix + "\n" +
                    "B\t3\t1970-01-01T00:00:00.060000" + leftSuffix + "\n";
            printSqlResult(ex, "tabY", "ts", true, true);
            // test
            ex = """
                    tag\thi\tlo
                    A\t1\tnull
                    A\t2\t1
                    A\t3\t2
                    B\t1\tnull
                    B\t2\t1
                    B\t3\t2
                    """;
            String query = "select a.tag, a.x hi, b.x lo from tabY a lt join tabY b on (tag) ";
            printSqlResult(ex, query, null, false, true);
        });
    }

    @Test
    public void testLtJoinSequenceGap() throws Exception {
        assertMemoryLeak(() -> {
            Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
            // create table
            execute("create table tab as " +
                    "(" +
                    "select " +
                    "rnd_symbol('AA', 'BB') tag," +
                    " x, " +
                    " timestamp_sequence(0, 10000)::" + leftTableTimestampType.getTypeName() + " ts" +
                    " from" +
                    " long_sequence(20)" +
                    ") timestamp(ts) partition by DAY");
            // insert
            execute("insert into tab values ('CC', 24, 210000::timestamp)");
            execute("insert into tab values ('CC', 25, 220000::timestamp)");
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String ex = "tag\tx\tts\n" +
                    "AA\t1\t1970-01-01T00:00:00.000000" + leftSuffix + "\n" +
                    "AA\t2\t1970-01-01T00:00:00.010000" + leftSuffix + "\n" +
                    "BB\t3\t1970-01-01T00:00:00.020000" + leftSuffix + "\n" +
                    "BB\t4\t1970-01-01T00:00:00.030000" + leftSuffix + "\n" +
                    "BB\t5\t1970-01-01T00:00:00.040000" + leftSuffix + "\n" +
                    "BB\t6\t1970-01-01T00:00:00.050000" + leftSuffix + "\n" +
                    "AA\t7\t1970-01-01T00:00:00.060000" + leftSuffix + "\n" +
                    "BB\t8\t1970-01-01T00:00:00.070000" + leftSuffix + "\n" +
                    "AA\t9\t1970-01-01T00:00:00.080000" + leftSuffix + "\n" +
                    "AA\t10\t1970-01-01T00:00:00.090000" + leftSuffix + "\n" +
                    "AA\t11\t1970-01-01T00:00:00.100000" + leftSuffix + "\n" +
                    "AA\t12\t1970-01-01T00:00:00.110000" + leftSuffix + "\n" +
                    "AA\t13\t1970-01-01T00:00:00.120000" + leftSuffix + "\n" +
                    "BB\t14\t1970-01-01T00:00:00.130000" + leftSuffix + "\n" +
                    "BB\t15\t1970-01-01T00:00:00.140000" + leftSuffix + "\n" +
                    "AA\t16\t1970-01-01T00:00:00.150000" + leftSuffix + "\n" +
                    "AA\t17\t1970-01-01T00:00:00.160000" + leftSuffix + "\n" +
                    "BB\t18\t1970-01-01T00:00:00.170000" + leftSuffix + "\n" +
                    "BB\t19\t1970-01-01T00:00:00.180000" + leftSuffix + "\n" +
                    "AA\t20\t1970-01-01T00:00:00.190000" + leftSuffix + "\n" +
                    "CC\t24\t1970-01-01T00:00:00.210000" + leftSuffix + "\n" +
                    "CC\t25\t1970-01-01T00:00:00.220000" + leftSuffix + "\n";
            String query = "tab";
            printSqlResult(ex, query, "ts", true, true);
            // test
            ex = """
                    tag\thi\tlo
                    CC\t24\t20
                    """;
            query = "select a.tag, a.x hi, b.x lo " +
                    "from tab a " +
                    "lt join tab b " +
                    "where a.x > b.x + 1";
            printSqlResult(ex, query, null, false, false);
        });
    }

    @Test
    public void testLtJoinSequenceGapOnKey() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            // create table
            execute("create table tab as " +
                    "(" +
                    "select " +
                    "rnd_symbol('AA', 'BB') tag," +
                    " x, " +
                    " timestamp_sequence(0, 10000)::" + leftTableTimestampType.getTypeName() + " ts" +
                    " from" +
                    " long_sequence(20)" +
                    ") timestamp(ts) partition by DAY");
            // insert
            execute("insert into tab values ('CC', 24, 210000::timestamp)");
            execute("insert into tab values ('CC', 25, 220000::timestamp)");
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String ex = "tag\tx\tts\n" +
                    "AA\t1\t1970-01-01T00:00:00.000000" + leftSuffix + "\n" +
                    "AA\t2\t1970-01-01T00:00:00.010000" + leftSuffix + "\n" +
                    "BB\t3\t1970-01-01T00:00:00.020000" + leftSuffix + "\n" +
                    "BB\t4\t1970-01-01T00:00:00.030000" + leftSuffix + "\n" +
                    "BB\t5\t1970-01-01T00:00:00.040000" + leftSuffix + "\n" +
                    "BB\t6\t1970-01-01T00:00:00.050000" + leftSuffix + "\n" +
                    "AA\t7\t1970-01-01T00:00:00.060000" + leftSuffix + "\n" +
                    "BB\t8\t1970-01-01T00:00:00.070000" + leftSuffix + "\n" +
                    "AA\t9\t1970-01-01T00:00:00.080000" + leftSuffix + "\n" +
                    "AA\t10\t1970-01-01T00:00:00.090000" + leftSuffix + "\n" +
                    "AA\t11\t1970-01-01T00:00:00.100000" + leftSuffix + "\n" +
                    "AA\t12\t1970-01-01T00:00:00.110000" + leftSuffix + "\n" +
                    "AA\t13\t1970-01-01T00:00:00.120000" + leftSuffix + "\n" +
                    "BB\t14\t1970-01-01T00:00:00.130000" + leftSuffix + "\n" +
                    "BB\t15\t1970-01-01T00:00:00.140000" + leftSuffix + "\n" +
                    "AA\t16\t1970-01-01T00:00:00.150000" + leftSuffix + "\n" +
                    "AA\t17\t1970-01-01T00:00:00.160000" + leftSuffix + "\n" +
                    "BB\t18\t1970-01-01T00:00:00.170000" + leftSuffix + "\n" +
                    "BB\t19\t1970-01-01T00:00:00.180000" + leftSuffix + "\n" +
                    "AA\t20\t1970-01-01T00:00:00.190000" + leftSuffix + "\n" +
                    "CC\t24\t1970-01-01T00:00:00.210000" + leftSuffix + "\n" +
                    "CC\t25\t1970-01-01T00:00:00.220000" + leftSuffix + "\n";
            String query = "tab";
            printSqlResult(ex, query, "ts", true, true);
            // test
            ex = """
                    tag\thi\tlo
                    AA\t7\t2
                    BB\t8\t6
                    AA\t9\t7
                    BB\t14\t8
                    AA\t16\t13
                    BB\t18\t15
                    AA\t20\t17
                    """;
            query = "select a.tag, a.x hi, b.x lo from tab a lt join tab b on (tag)  where a.x > b.x + 1";
            printSqlResult(ex, query, null, false, false);
        });
    }

    @Test
    public void testLtJoinTolerance() throws Exception {
        assertMemoryLeak(() -> {
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            String rightSuffix = getTimestampSuffix(rightTableTimestampType.getTypeName());
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't1' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp(
                    """
                            CREATE TABLE 't2' (\s
                            id LONG,
                            ts #TIMESTAMP
                            ) timestamp(ts) PARTITION BY DAY""",
                    rightTableTimestampType.getTypeName());

            execute("insert into t1 select x as id, (x + x*1_000_000)::timestamp ts from long_sequence(10)");
            execute("insert into t2 select x as id, (x)::timestamp ts from long_sequence(5)");


            // keyed join and slave has no timeframe support -> should use Lt Join Light
            String expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t1\t1970-01-01T00:00:00.000001" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\t2\t1970-01-01T00:00:00.000002" + rightSuffix + "\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";
            String query = "SELECT * FROM t1 LT JOIN (select * from t2 where t2.id != 1000) ON id TOLERANCE 2s;";
            // sanity check: uses Lt Join Light
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "Lt Join Light");
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);
            assertQueryFullFatNoLeakCheck(expected, query, "ts", false, true, true);


            // non-keyed join and slave supports timeframe -> should use Lt Join Fast Scan
            query = "SELECT * FROM t1 LT JOIN t2 TOLERANCE 2s;";
            expected = "id\tts\tid1\tts1\n" +
                    "1\t1970-01-01T00:00:01.000001" + leftSuffix + "\t5\t1970-01-01T00:00:00.000005" + rightSuffix + "\n" +
                    "2\t1970-01-01T00:00:02.000002" + leftSuffix + "\t5\t1970-01-01T00:00:00.000005" + rightSuffix + "\n" +
                    "3\t1970-01-01T00:00:03.000003" + leftSuffix + "\tnull\t\n" +
                    "4\t1970-01-01T00:00:04.000004" + leftSuffix + "\tnull\t\n" +
                    "5\t1970-01-01T00:00:05.000005" + leftSuffix + "\tnull\t\n" +
                    "6\t1970-01-01T00:00:06.000006" + leftSuffix + "\tnull\t\n" +
                    "7\t1970-01-01T00:00:07.000007" + leftSuffix + "\tnull\t\n" +
                    "8\t1970-01-01T00:00:08.000008" + leftSuffix + "\tnull\t\n" +
                    "9\t1970-01-01T00:00:09.000009" + leftSuffix + "\tnull\t\n" +
                    "10\t1970-01-01T00:00:10.000010" + leftSuffix + "\tnull\t\n";
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "Lt Join Fast Scan");
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            // non-keyed join, slave supports timeframe but avoid BINARY_SEARCH hint -> should use Lt Join (full fat)
            query = "SELECT /*+ avoid_lt_binary_search(t1 t2) */ * FROM t1 LT JOIN t2 TOLERANCE 2s;";
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "Lt Join");
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);

            // non-keyed join, slave has a filter -> should also use Lt Join
            query = "SELECT * FROM t1 LT JOIN (select * from t2 where t2.id != 1000) t2 TOLERANCE 2s;";
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "Lt Join");
            assertQueryNoLeakCheck(expected, query, null, "ts", false, true);
        });
    }

    @Test
    public void testLtJoinWithAsofLinearSearchHint() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp(
                    """
                            create table t1 as (
                              select\s
                                rnd_symbol('A', 'B') as sym,
                                cast(x as #TIMESTAMP) as ts
                              from long_sequence(100)
                            ) timestamp(ts) partition by DAY""",
                    leftTableTimestampType.getTypeName()
            );

            executeWithRewriteTimestamp(
                    """
                            create table t2 as (
                              select\s
                                rnd_symbol('A', 'B') as sym,
                                cast(x as #TIMESTAMP) as ts
                              from long_sequence(100)
                            ), index(sym) timestamp(ts) partition by DAY""",
                    rightTableTimestampType.getTypeName()
            );

            // Query with asof_linear hint (forces linear search)
            String query = "SELECT /*+ asof_linear(t1 t2) */ * FROM t1 " +
                    "LT JOIN t2 ON (sym)";

            // Verify the query plan does NOT show Fast scan
            printSql("EXPLAIN " + query);
            TestUtils.assertContains(sink, "Lt Join");
            TestUtils.assertNotContains(sink, "Fast");

            // Execute and verify results
            printSql(query);
            Assert.assertFalse(sink.isEmpty());
        });
    }

    @Test
    public void testNestedASOF_keySymbol() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                executeWithRewriteTimestamp("""
                                CREATE TABLE 'tests' (
                                  Ticker SYMBOL capacity 256 CACHE,
                                  ts #TIMESTAMP
                                ) timestamp (ts) PARTITION BY MONTH""",
                        leftTableTimestampType.getTypeName());
                execute("insert into tests VALUES " +
                        "('AAPL', '2000')," +
                        "('AAPL', '2001')," +
                        "('AAPL', '2002')," +
                        "('AAPL', '2003')," +
                        "('AAPL', '2004')," +
                        "('AAPL', '2005')"
                );
                execute("insert into tests VALUES " +
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
                        "AAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "AAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "AAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "AAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "AAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "AAPL\t2005-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2005-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2005-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2005-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2008-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2008-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2008-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2008-01-01T00:00:00.000000" + leftSuffix + "\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testNestedLT_keySymbol() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                executeWithRewriteTimestamp("""
                                CREATE TABLE 'tests' (
                                  Ticker SYMBOL capacity 256 CACHE,
                                  ts #TIMESTAMP
                                ) timestamp (ts) PARTITION BY MONTH""",
                        leftTableTimestampType.getTypeName());
                execute("insert into tests VALUES " +
                        "('AAPL', '2000')," +
                        "('AAPL', '2001')," +
                        "('AAPL', '2002')," +
                        "('AAPL', '2003')," +
                        "('AAPL', '2004')," +
                        "('AAPL', '2005')"
                );
                execute("insert into tests VALUES " +
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
                        "AAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\t\t\n" +
                        "AAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\n" +
                        "AAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                        "AAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\t\t\n" +
                        "AAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\n" +
                        "AAPL\t2005-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                        "QSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "QSTDB\t2008-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\n";
                assertQueryNoLeakCheck(compiler, expected, query, "ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testNestedLT_keySymbol_moreColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
                String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
                executeWithRewriteTimestamp(
                        "CREATE TABLE 'tests' (\n" +
                                "  UnusedTag SYMBOL,\n" + // just filler to make the joining a bit more interesting
                                "  Ticker SYMBOL capacity 256 CACHE,\n" +
                                "  ts #TIMESTAMP,\n" +
                                "  price int\n" +
                                ") timestamp (ts) PARTITION BY MONTH",
                        leftTableTimestampType.getTypeName());
                execute("insert into tests VALUES " +
                        "('Whatever', 'AAPL', '2000', 0)," +
                        "('Whatever', 'AAPL', '2001', 1)," +
                        "('Whatever', 'AAPL', '2002', 2)," +
                        "('Whatever', 'AAPL', '2003', 3)," +
                        "('Whatever', 'AAPL', '2004', 4)," +
                        "('Whatever', 'AAPL', '2005', 5)"
                );
                execute("insert into tests VALUES " +
                        "('Whatever', 'QSTDB', '2003', 6)," +
                        "('Whatever', 'QSTDB', '2004', 7)," +
                        "('Whatever', 'QSTDB', '2005', 8)," +
                        "('Whatever', 'QSTDB', '2006', 9)," +
                        "('Whatever', 'QSTDB', '2007', 10)," +
                        "('Whatever', 'QSTDB', '2008', 11)"
                );

                String query = """
                        SELECT t2unused, Ticker AS t0ticker, ts AS t0ts, t1ticker, t1ts, t2ticker, t2ts, t3ticker, t3ts\s
                        FROM tests\s
                        LT JOIN (
                            SELECT t2unused, Ticker AS t1ticker, UnusedTag AS t1unused, ts AS t1ts, t3unused, t2ticker, t2ts, t3ticker, t3ts\s
                            FROM tests\s
                            LT JOIN (
                                SELECT UnusedTag AS t2unused, Ticker AS t2ticker, t3unused, ts AS t2ts, t3ticker, t3ts\s
                                FROM tests\s
                                LT JOIN (
                                    SELECT UnusedTag AS t3unused, Ticker AS t3ticker, ts AS t3ts FROM tests
                                ) t3 ON (ticker = t3.t3ticker)
                            ) t2 ON (Ticker = t2ticker)
                        ) t1 ON (Ticker = t1ticker)""";

                String expected = "t2unused\tt0ticker\tt0ts\tt1ticker\tt1ts\tt2ticker\tt2ts\tt3ticker\tt3ts\n" +
                        "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\t\t\n" +
                        "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\n" +
                        "Whatever\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                        "Whatever\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2000-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\t\t\n" +
                        "Whatever\tAAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2001-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\t\t\t\t\n" +
                        "Whatever\tAAPL\t2005-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2004-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2003-01-01T00:00:00.000000" + leftSuffix + "\tAAPL\t2002-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "Whatever\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\t\t\n" +
                        "Whatever\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2003-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "Whatever\tQSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2004-01-01T00:00:00.000000" + leftSuffix + "\n" +
                        "Whatever\tQSTDB\t2008-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2007-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2006-01-01T00:00:00.000000" + leftSuffix + "\tQSTDB\t2005-01-01T00:00:00.000000" + leftSuffix + "\n";

                assertQueryNoLeakCheck(compiler, expected, query, "t0ts", false, sqlExecutionContext, true);
            }
        });
    }

    @Test
    public void testRightHandAfter() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:17:00.000000Z', 2, 'b');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'a');");
            execute("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'b');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandBefore() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:30.000000Z', 1, 'b');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'a');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2021-10-01T00:00:00.000000Z', 3, 'a');");
            execute("INSERT INTO t2 values ('2021-10-03T01:00:00.000000Z', 4, 'a');");
            execute("INSERT INTO t2 values ('2021-10-03T01:00:00.000000Z', 4, 'b');");
            execute("INSERT INTO t2 values ('2021-10-05T04:00:00.000000Z', 5, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 1, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 2, 'a');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 1, 'a');");
            execute("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 2, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandEmpty() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 2, 'a');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2023-10-05T04:00:00.000000Z', 3, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandPartitionBoundary() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2022-10-05T00:00:00.000000Z', 0, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T00:00:00.000000Z', 0, 'b');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2022-10-04T23:59:59.999999Z', 1, 'a');");
            execute("INSERT INTO t2 values ('2022-10-04T23:59:59.999999Z', 1, 'b');");
            execute("INSERT INTO t2 values ('2022-10-05T00:00:00.000000Z', 2, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testRightHandSame() throws Exception {
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t1 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t1 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            execute("INSERT INTO t1 values ('2022-10-05T08:16:00.000000Z', 1, 'b');");
            execute("INSERT INTO t1 values ('2022-10-07T08:16:00.000000Z', 2, 'a');");

            executeWithRewriteTimestamp("CREATE TABLE t2 (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", rightTableTimestampType.getTypeName());
            execute("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'a');");
            execute("INSERT INTO t2 values ('2022-10-05T08:15:00.000000Z', 0, 'c');");
            execute("INSERT INTO t2 values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            execute("INSERT INTO t2 values ('2022-10-07T08:16:00.000000Z', 2, 'a');");

            assertResultSetsMatch("t1", "t2");
        });
    }

    @Test
    public void testSelfJoin() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE t (ts #TIMESTAMP, i INT, s SYMBOL) timestamp(ts) partition by day bypass wal", leftTableTimestampType.getTypeName());
            execute("INSERT INTO t values ('2022-10-05T00:00:00.000000Z', 0, 'a');");
            execute("INSERT INTO t values ('2022-10-05T08:16:00.000000Z', 1, 'a');");
            execute("INSERT INTO t values ('2022-10-05T08:16:00.000000Z', 3, 'a');");
            execute("INSERT INTO t values ('2022-10-05T23:59:59.999999Z', 4, 'a');");
            execute("INSERT INTO t values ('2022-10-05T23:59:59.999999Z', 4, 'b');");
            execute("INSERT INTO t values ('2022-10-06T00:00:00.000000Z', 5, 'a');");
            execute("INSERT INTO t values ('2022-10-06T00:01:00.000000Z', 6, 'a');");
            execute("INSERT INTO t values ('2022-10-06T00:01:00.000000Z', 6, 'c');");
            execute("INSERT INTO t values ('2022-10-06T00:02:00.000000Z', 7, 'a');");

            assertResultSetsMatch("t as t1", "t as t2");
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey1() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE trades (pair SYMBOL, ts #TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());

            execute("""
                    INSERT INTO trades VALUES
                    ('BTC-USD', '2000-01-01T00:00:00.000000Z', 1),
                    ('BTC-USD', '2001-01-01T00:00:01.000000Z', 2),
                    ('BTC-USD', '2002-01-01T00:00:03.000000Z', 3),
                    ('ETH-USD', '2001-01-01T00:00:00.000000Z', 4),
                    ('ETH-USD', '2001-01-01T00:00:01.000000Z', 5),
                    ('ETH-USD', '2001-01-01T00:00:03.000000Z', 6)
                    """
            );

            // ASOF JOIN
            String query = "SELECT * FROM trades t1 ASOF JOIN trades t2 ON (pair)";
            String expected = "pair\tts\tprice\tpair1\tts1\tprice1\n" +
                    "BTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tBTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\n" +
                    "ETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\tETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\n" +
                    "BTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\tBTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\n" +
                    "ETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\tETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\n" +
                    "ETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\tETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\n" +
                    "BTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\tBTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // LT JOIN
            query = "SELECT * FROM trades t1 LT JOIN trades t2 ON (pair)";
            expected = "pair\tts\tprice\tpair1\tts1\tprice1\n" +
                    "BTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\t\t\tnull\n" +
                    "ETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\t\t\tnull\n" +
                    "BTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\tBTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\n" +
                    "ETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\tETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\n" +
                    "ETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\tETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\n" +
                    "BTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\tBTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // SPLICE JOIN
            query = "SELECT * FROM trades t1 SPLICE JOIN trades t2 ON (pair)";
            expected = "pair\tts\tprice\tpair1\tts1\tprice1\n" +
                    "BTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tBTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\n" +
                    "ETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\tETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\n" +
                    "BTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\tBTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\n" +
                    "ETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\tETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\n" +
                    "ETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\tETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\n" +
                    "BTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\tBTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\n";
            assertQueryNoLeakCheck(expected, query, null, false, false);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey2() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            String leftSuffix = getTimestampSuffix(leftTableTimestampType.getTypeName());
            executeWithRewriteTimestamp("CREATE TABLE trades (pair SYMBOL, ts #TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());

            execute("""
                    INSERT INTO trades VALUES
                    ('BTC-USD', '2000-01-01T00:00:00.000000Z', 1),
                    ('BTC-USD', '2001-01-01T00:00:01.000000Z', 2),
                    ('BTC-USD', '2002-01-01T00:00:03.000000Z', 3),
                    ('ETH-USD', '2001-01-01T00:00:00.000000Z', 4),
                    ('ETH-USD', '2001-01-01T00:00:01.000000Z', 5),
                    ('ETH-USD', '2001-01-01T00:00:03.000000Z', 6)
                    """
            );

            // ASOF JOIN
            String query = "SELECT * FROM (select pair p1, ts, price from trades) t1 " +
                    "ASOF JOIN (select ts, price, pair p2 from trades) t2 ON t1.p1 = t2.p2";
            String expected = "p1\tts\tprice\tts1\tprice1\tp2\n" +
                    "BTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tBTC-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\tETH-USD\n" +
                    "BTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\tBTC-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\tETH-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\tETH-USD\n" +
                    "BTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\tBTC-USD\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // LT JOIN
            query = "SELECT * FROM (select pair p1, ts, price from trades) t1 " +
                    "LT JOIN (select ts, price, pair p2 from trades) t2 ON t1.p1 = t2.p2";
            expected = "p1\tts\tprice\tts1\tprice1\tp2\n" +
                    "BTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\t\tnull\t\n" +
                    "ETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\t\tnull\t\n" +
                    "BTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tBTC-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\tETH-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\tETH-USD\n" +
                    "BTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\tBTC-USD\n";
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // SPLICE JOIN
            query = "SELECT * FROM (select pair p1, ts, price from trades) t1 " +
                    "SPLICE JOIN (select ts, price, pair p2 from trades) t2 ON t1.p1 = t2.p2";
            expected = "p1\tts\tprice\tts1\tprice1\tp2\n" +
                    "BTC-USD\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\t2000-01-01T00:00:00.000000" + leftSuffix + "\t1\tBTC-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\t2001-01-01T00:00:00.000000" + leftSuffix + "\t4\tETH-USD\n" +
                    "BTC-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\t2001-01-01T00:00:01.000000" + leftSuffix + "\t2\tBTC-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\t2001-01-01T00:00:01.000000" + leftSuffix + "\t5\tETH-USD\n" +
                    "ETH-USD\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\t2001-01-01T00:00:03.000000" + leftSuffix + "\t6\tETH-USD\n" +
                    "BTC-USD\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\t2002-01-01T00:00:03.000000" + leftSuffix + "\t3\tBTC-USD\n";
            assertQueryNoLeakCheck(expected, query, null, false, false);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey3() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (pair SYMBOL, side SYMBOL, ts #TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());
            execute("""
                    INSERT INTO trades VALUES
                    ('BTC-USD', 'sell', '2000-01-01T00:00:00.000000Z', 1),
                    ('BTC-USD', 'buy', '2001-01-01T00:00:01.000000Z', 2),
                    ('BTC-USD', 'sell', '2002-01-01T00:00:03.000000Z', 3),
                    ('ETH-USD', 'sell', '2001-01-01T00:00:00.000000Z', 4),
                    ('ETH-USD', 'buy', '2001-01-01T00:00:01.000000Z', 5),
                    ('ETH-USD', 'sell', '2001-01-01T00:00:03.000000Z', 6)
                    """
            );

            // ASOF JOIN
            String query = "SELECT * FROM trades t1 ASOF JOIN trades t2 ON(pair, side)";
            String expected = replaceTimestampSuffix("""
                    pair\tside\tts\tprice\tpair1\tside1\tts1\tprice1
                    BTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1
                    ETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4\tETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4
                    BTC-USD\tbuy\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\tbuy\t2001-01-01T00:00:01.000000Z\t2
                    ETH-USD\tbuy\t2001-01-01T00:00:01.000000Z\t5\tETH-USD\tbuy\t2001-01-01T00:00:01.000000Z\t5
                    ETH-USD\tsell\t2001-01-01T00:00:03.000000Z\t6\tETH-USD\tsell\t2001-01-01T00:00:03.000000Z\t6
                    BTC-USD\tsell\t2002-01-01T00:00:03.000000Z\t3\tBTC-USD\tsell\t2002-01-01T00:00:03.000000Z\t3
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // LT JOIN
            query = "SELECT * FROM trades t1 LT JOIN trades t2 ON(pair, side)";
            expected = replaceTimestampSuffix("""
                    pair\tside\tts\tprice\tpair1\tside1\tts1\tprice1
                    BTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1\t\t\t\tnull
                    ETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4\t\t\t\tnull
                    BTC-USD\tbuy\t2001-01-01T00:00:01.000000Z\t2\t\t\t\tnull
                    ETH-USD\tbuy\t2001-01-01T00:00:01.000000Z\t5\t\t\t\tnull
                    ETH-USD\tsell\t2001-01-01T00:00:03.000000Z\t6\tETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4
                    BTC-USD\tsell\t2002-01-01T00:00:03.000000Z\t3\tBTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // SPLICE JOIN
            query = "SELECT * FROM trades t1 SPLICE JOIN trades t2 ON(pair, side)";
            expected = replaceTimestampSuffix("""
                    pair\tside\tts\tprice\tpair1\tside1\tts1\tprice1
                    BTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\tsell\t2000-01-01T00:00:00.000000Z\t1
                    ETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4\tETH-USD\tsell\t2001-01-01T00:00:00.000000Z\t4
                    BTC-USD\tbuy\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\tbuy\t2001-01-01T00:00:01.000000Z\t2
                    ETH-USD\tbuy\t2001-01-01T00:00:01.000000Z\t5\tETH-USD\tbuy\t2001-01-01T00:00:01.000000Z\t5
                    ETH-USD\tsell\t2001-01-01T00:00:03.000000Z\t6\tETH-USD\tsell\t2001-01-01T00:00:03.000000Z\t6
                    BTC-USD\tsell\t2002-01-01T00:00:03.000000Z\t3\tBTC-USD\tsell\t2002-01-01T00:00:03.000000Z\t3
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, null, false, false);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey4() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE x (sym1 SYMBOL, sym2 SYMBOL, ts #TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());

            execute("""
                    INSERT INTO x VALUES
                    ('1', '2', '2000-01-01T00:00:00.000000Z'),
                    ('3', '4', '2000-01-01T00:00:01.000000Z'),
                    ('1', '1', '2000-01-01T00:00:02.000000Z'),
                    ('2', '2', '2000-01-01T00:00:03.000000Z'),
                    ('4', '3', '2000-01-01T00:00:04.000000Z')
                    """
            );

            // ASOF JOIN
            String query = "SELECT * FROM (select sym1 s, ts from x) x1 " +
                    "ASOF JOIN (select sym2 s, ts from x) x2 ON(s)";
            String expected = replaceTimestampSuffix("""
                    s\tts\ts1\tts1
                    1\t2000-01-01T00:00:00.000000Z\t\t
                    3\t2000-01-01T00:00:01.000000Z\t\t
                    1\t2000-01-01T00:00:02.000000Z\t1\t2000-01-01T00:00:02.000000Z
                    2\t2000-01-01T00:00:03.000000Z\t2\t2000-01-01T00:00:03.000000Z
                    4\t2000-01-01T00:00:04.000000Z\t4\t2000-01-01T00:00:01.000000Z
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // LT JOIN
            query = "SELECT * FROM (select sym1 s, ts from x) x1 " +
                    "LT JOIN (select sym2 s, ts from x) x2 ON(s)";
            expected = replaceTimestampSuffix("""
                    s\tts\ts1\tts1
                    1\t2000-01-01T00:00:00.000000Z\t\t
                    3\t2000-01-01T00:00:01.000000Z\t\t
                    1\t2000-01-01T00:00:02.000000Z\t\t
                    2\t2000-01-01T00:00:03.000000Z\t2\t2000-01-01T00:00:00.000000Z
                    4\t2000-01-01T00:00:04.000000Z\t4\t2000-01-01T00:00:01.000000Z
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, "ts", false, true);

            // SPLICE JOIN
            query = "SELECT * FROM (select sym1 s, ts from x) x1 " +
                    "SPLICE JOIN (select sym2 s, ts from x) x2 ON(s)";
            expected = replaceTimestampSuffix("""
                    s\tts\ts1\tts1
                    1\t2000-01-01T00:00:00.000000Z\t\t
                    3\t2000-01-01T00:00:01.000000Z\t\t
                    1\t2000-01-01T00:00:02.000000Z\t1\t2000-01-01T00:00:02.000000Z
                    2\t2000-01-01T00:00:03.000000Z\t2\t2000-01-01T00:00:03.000000Z
                    4\t2000-01-01T00:00:04.000000Z\t\t
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, null, false, false);
        });
    }

    @Test
    public void testSelfJoinOnSymbolKey5() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (pair SYMBOL, ts #TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY DAY", leftTableTimestampType.getTypeName());

            execute("""
                    INSERT INTO trades VALUES
                    ('BTC-USD', '2000-01-01T00:00:00.000000Z', 1),
                    ('BTC-USD', '2001-01-01T00:00:01.000000Z', 2),
                    ('BTC-USD', '2002-01-01T00:00:03.000000Z', 3),
                    ('ETH-USD', '2001-01-01T00:00:00.000000Z', 4),
                    ('ETH-USD', '2001-01-01T00:00:01.000000Z', 5),
                    ('ETH-USD', '2001-01-01T00:00:03.000000Z', 6)
                    """
            );

            // ASOF JOIN
            String query = "SELECT * FROM (select * from trades where pair = 'BTC-USD') t1 " +
                    "ASOF JOIN (select * from trades where pair = 'BTC-USD') t2 ON(pair)";
            String expected = replaceTimestampSuffix("""
                    pair\tts\tprice\tpair1\tts1\tprice1
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\t2000-01-01T00:00:00.000000Z\t1
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\t2001-01-01T00:00:01.000000Z\t2
                    BTC-USD\t2002-01-01T00:00:03.000000Z\t3\tBTC-USD\t2002-01-01T00:00:03.000000Z\t3
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, "ts", false, false);

            // LT JOIN
            query = "SELECT * FROM (select * from trades where pair = 'BTC-USD') t1 " +
                    "LT JOIN (select * from trades where pair = 'BTC-USD') t2 ON(pair)";
            expected = replaceTimestampSuffix("""
                    pair\tts\tprice\tpair1\tts1\tprice1
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\t\t\tnull
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\t2000-01-01T00:00:00.000000Z\t1
                    BTC-USD\t2002-01-01T00:00:03.000000Z\t3\tBTC-USD\t2001-01-01T00:00:01.000000Z\t2
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, "ts", false, false);

            // SPLICE JOIN
            query = "SELECT * FROM (select * from trades where pair = 'BTC-USD') t1 " +
                    "SPLICE JOIN (select * from trades where pair = 'BTC-USD') t2 ON(pair)";
            expected = replaceTimestampSuffix("""
                    pair\tts\tprice\tpair1\tts1\tprice1
                    BTC-USD\t2000-01-01T00:00:00.000000Z\t1\tBTC-USD\t2000-01-01T00:00:00.000000Z\t1
                    BTC-USD\t2001-01-01T00:00:01.000000Z\t2\tBTC-USD\t2001-01-01T00:00:01.000000Z\t2
                    BTC-USD\t2002-01-01T00:00:03.000000Z\t3\tBTC-USD\t2002-01-01T00:00:03.000000Z\t3
                    """, leftTableTimestampType.getTypeName());
            assertQueryNoLeakCheck(expected, query, null, false, false);
        });
    }

    @Test
    public void testSingleSymbolAsOf() throws Exception {
        assertMemoryLeak(
                (TestUtils.LeakProneCode) () -> {
                    executeWithRewriteTimestamp(
                            """
                                    create table t1 as (
                                    SELECT
                                        rnd_symbol_zipf(1_000, 2.0) AS symbol,
                                        rnd_symbol('buy', 'sell') as side,
                                        rnd_double() * 20 + 10 AS price,
                                        rnd_double() * 20 + 10 AS amount,
                                        generate_series as timestamp
                                      FROM generate_series('2025-01-01'::#TIMESTAMP, '2025-01-02', '172898983u')
                                      ) timestamp(timestamp) partition by day
                                    """,
                            leftTableTimestampType.getTypeName()
                    );

                    executeWithRewriteTimestamp(
                            """
                                    create table t2 as (
                                    SELECT
                                          '2024-12-31T23'::#TIMESTAMP + (60*x) + rnd_long(-20, 20, 0) as ts,
                                          rnd_symbol_zipf(1_000, 2.0) sym,
                                          rnd_double() * 10.0 + 5.0 bid,
                                          rnd_double() * 10.0 + 5.0 ask
                                          FROM long_sequence(10_000)
                                    ) timestamp(ts) partition by day
                                    """,
                            rightTableTimestampType.getTypeName()
                    );

                    var asofSQL = """
                                    SELECT /*+ ASOF_LINEAR(t p) */ avg(bid)\s
                                    FROM t1 t\s
                                    ASOF JOIN t2 p on (t.symbol=p.sym);
                            """;

                    assertQueryNoLeakCheck(
                            """
                                    avg
                                    10.82018197104726
                                    """,
                            asofSQL,
                            null,
                            false,
                            true
                    );

                    assertSql(
                            """
                                    QUERY PLAN
                                    GroupBy vectorized: false
                                      values: [avg(bid)]
                                        SelectedRecord
                                            AsOf Join Single Symbol
                                              condition: p.sym=t.symbol
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t2
                                    """,
                            "explain " + asofSQL

                    );

                    // ensure algo is not triggered without hint
                    assertSql(
                            """
                                    QUERY PLAN
                                    GroupBy vectorized: false
                                      values: [avg(bid)]
                                        SelectedRecord
                                            AsOf Join Fast Scan
                                              condition: p.sym=t.symbol
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t1
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: t2
                                    """,
                            """
                                    EXPLAIN SELECT avg(bid)\s
                                    FROM t1 t\s
                                    ASOF JOIN t2 p on (t.symbol=p.sym);
                                    """
                    );
                }
        );
    }

    @Test
    public void testSingleSymbolAsOfWithDynamicSlaveSymbolThrows() throws Exception {
        assertMemoryLeak(
                (TestUtils.LeakProneCode) () -> {
                    executeWithRewriteTimestamp(
                            """
                                    create table dyn_master as (
                                    select
                                        rnd_symbol('A', 'B', 'C') as sym,
                                        rnd_double() as val,
                                        generate_series as timestamp
                                    from generate_series('2025-01-01'::#TIMESTAMP, '2025-01-01T00:30', '300000000u')
                                    ) timestamp(timestamp) partition by day
                                    """,
                            leftTableTimestampType.getTypeName()
                    );

                    executeWithRewriteTimestamp(
                            """
                                    create table dyn_slave_src as (
                                    select
                                        cast(sym as string) as sym_str,
                                        cast(timestamp as #TIMESTAMP) as ts
                                    from dyn_master
                                    ) timestamp(ts) partition by day
                                    """,
                            rightTableTimestampType.getTypeName()
                    );

                    final String sql = """
                            SELECT /*+ ASOF_LINEAR(m s) */m.sym, ts
                            FROM dyn_master m
                            ASOF JOIN (
                                SELECT cast(sym_str as symbol) AS sym, ts
                                FROM dyn_slave_src
                            ) s ON m.sym = s.sym
                            """;

                    assertQueryNoLeakCheck(
                            replaceTimestampSuffix1("""
                                            sym	ts
                                            A	2025-01-01T00:00:00.000000Z
                                            C	2025-01-01T00:05:00.000000Z
                                            C	2025-01-01T00:10:00.000000Z
                                            B	2025-01-01T00:15:00.000000Z
                                            B	2025-01-01T00:20:00.000000Z
                                            A	2025-01-01T00:25:00.000000Z
                                            A	2025-01-01T00:30:00.000000Z
                                            """,
                                    rightTableTimestampType.getTypeName()
                            ),
                            sql,
                            null,
                            false,
                            true
                    );

                    assertSql(
                            """
                                    QUERY PLAN
                                    SelectedRecord
                                        AsOf Join Light
                                          condition: s.sym=m.sym
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: dyn_master
                                            VirtualRecord
                                              functions: [ts,sym_str::symbol]
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: dyn_slave_src
                                    """,
                            "explain " + sql
                    );
                }
        );
    }

    @Test
    public void testWithIntrisifiedTimestampFilter() throws Exception {
        Assume.assumeTrue(rightTableTimestampType == TestTimestampType.MICRO);
        assertMemoryLeak(() -> {
            executeWithRewriteTimestamp("CREATE TABLE trades (pair SYMBOL, ts #TIMESTAMP, price INT) TIMESTAMP(ts) PARTITION BY YEAR", leftTableTimestampType.getTypeName());

            execute(
                    "INSERT INTO trades VALUES " +
                            "('BTC-USD', '2000-01-01T00:00:00.000000Z', 1)," +
                            "('BTC-USD', '2000-02-01T00:00:00.000000Z', 2)," +
                            "('BTC-USD', '2000-03-01T00:00:00.000000Z', 3)," +
                            "('BTC-USD', '2000-04-01T00:00:00.000000Z', 4)," +
                            "('BTC-USD', '2000-05-01T00:00:00.000000Z', 5)," +
                            "('BTC-USD', '2000-06-01T00:00:00.000000Z', 6)"
            );

            assertQuery(replaceTimestampSuffix("""
                            pair\tts\tprice\tpair1\tts1\tprice1
                            BTC-USD\t2000-01-01T00:00:00.000000Z\t1\t\t\tnull
                            BTC-USD\t2000-02-01T00:00:00.000000Z\t2\t\t\tnull
                            BTC-USD\t2000-03-01T00:00:00.000000Z\t3\tBTC-USD\t2000-03-01T00:00:00.000000Z\t3
                            BTC-USD\t2000-04-01T00:00:00.000000Z\t4\tBTC-USD\t2000-03-01T00:00:00.000000Z\t3
                            BTC-USD\t2000-05-01T00:00:00.000000Z\t5\tBTC-USD\t2000-03-01T00:00:00.000000Z\t3
                            BTC-USD\t2000-06-01T00:00:00.000000Z\t6\tBTC-USD\t2000-03-01T00:00:00.000000Z\t3
                            """, leftTableTimestampType.getTypeName()),
                    """
                            select * from trades
                            asof join (
                              select * from trades
                              where ts in '2000-03'
                            ) t;""",
                    null,
                    "ts",
                    null,
                    null,
                    false,
                    true,
                    false
            );
        });
    }

    private void assertAlgoAndResult(String queryBody, String hint, String expectedAlgo, String expectedResult) throws SqlException {
        String hintedQuery;
        if (!hint.isEmpty()) {
            hintedQuery = "SELECT /*+ " + hint + "*/ " + queryBody;
        } else {
            hintedQuery = "SELECT " + queryBody;
        }
        printSql("EXPLAIN " + hintedQuery);
        TestUtils.assertContains(sink, "AsOf Join " + expectedAlgo);
        if (hint.contains("asof_driveby_cache(t q)")) {
            TestUtils.assertContains(sink, "driveByCache: true");
        }
        assertSql(expectedResult, hintedQuery);
    }

    private void assertResultSetsMatch(String leftTable, String rightTable) throws Exception {
        final StringSink expectedSink = new StringSink();
        // equivalent of the below query, but uses slow factory
        printSql("select * from " + leftTable + " asof join (" + rightTable + " where i >= 0) on s", expectedSink);

        final StringSink actualSink = new StringSink();
        printSql("select * from " + leftTable + " asof join " + rightTable + " on s", actualSink);

        TestUtils.assertEquals(expectedSink, actualSink);
    }

    private void testExplicitTimestampIsNotNecessaryWhenJoining(String joinType, String timestamp) throws Exception {
        assertQuery(
                "ts\ty\tts1\ty1\n",
                "select * from " +
                        "(select * from (select * from x where y = 10 order by ts desc limit 20) order by ts ) a " +
                        joinType +
                        "(select * from x order by ts limit 5) b",
                "create table x (ts timestamp, y int) timestamp(ts)",
                timestamp,
                false
        );
    }

    private void testFullJoinDoesNotConvertSymbolKeyToString(String joinType) throws Exception {
        assertMemoryLeak(() -> {
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                executeWithRewriteTimestamp("create table tab_a (sym_a symbol, ts_a #TIMESTAMP, s_a string) timestamp(ts_a) partition by DAY", leftTableTimestampType.getTypeName());
                executeWithRewriteTimestamp("create table tab_b (sym_b symbol, ts_b #TIMESTAMP, s_B string) timestamp(ts_b) partition by DAY", rightTableTimestampType.getTypeName());

                execute("insert into tab_a values " +
                        "('ABC', '2022-01-01T00:00:00.000000Z', 'foo')"
                );
                execute("insert into tab_b values " +
                        "('DCE', '2021-01-01T00:00:00.000000Z', 'bar')," + // first INSERT a row with DCE to make sure symbol table for tab_b differs from tab_a
                        "('ABC', '2021-01-01T00:00:00.000000Z', 'bar')"
                );

                String query = "select sym_a, sym_b from tab_a a " + joinType + " tab_b b on sym_a = sym_b";
                try (RecordCursorFactory factory = select(query)) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Record record = cursor.getRecord();
                        RecordMetadata metadata = factory.getMetadata();
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(ColumnType.SYMBOL, metadata.getColumnType(0));
                        Assert.assertEquals(ColumnType.SYMBOL, metadata.getColumnType(1));
                        CharSequence sym0 = record.getSymA(0);
                        CharSequence sym1 = record.getSymA(1);
                        TestUtils.assertEquals("ABC", sym0);
                        TestUtils.assertEquals("ABC", sym1);
                        Assert.assertFalse(cursor.hasNext());
                    }
                }
            }
        });
    }
}
