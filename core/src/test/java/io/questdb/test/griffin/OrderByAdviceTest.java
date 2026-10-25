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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.idx.BitmapIndexFwdReader;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.griffin.engine.table.AbstractPageFrameRecordCursor;
import io.questdb.griffin.engine.table.PageFrameRecordCursorImpl;
import io.questdb.griffin.engine.table.SortedSymbolIndexRowCursorFactory;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.test.griffin.PlanShape.assertPlanned;

public class OrderByAdviceTest extends AbstractCairoTest {

    @Test
    public void testCreateDesignatedTimestampFromMultipleOrderBy() throws Exception {
        assertQuery("select * from x order by t, a")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " timestamp_sequence(0, 1000) t" +
                        " from long_sequence(9)" +
                        ")")
                .timestamp("t")
                .expectSize()
                .returns("""
                        a\tt
                        1\t1970-01-01T00:00:00.000000Z
                        2\t1970-01-01T00:00:00.001000Z
                        3\t1970-01-01T00:00:00.002000Z
                        4\t1970-01-01T00:00:00.003000Z
                        5\t1970-01-01T00:00:00.004000Z
                        6\t1970-01-01T00:00:00.005000Z
                        7\t1970-01-01T00:00:00.006000Z
                        8\t1970-01-01T00:00:00.007000Z
                        9\t1970-01-01T00:00:00.008000Z
                        """);
    }

    @Test
    public void testDistinctWithOrderBy() throws Exception {
        assertQuery("select distinct b from x order by b desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("b\n2\n1\n0\n");
    }

    @Test
    public void testDistinctWithOrderByAnotherColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "create table x as (" +
                            "select" +
                            " x a," +
                            " x % 3 b" +
                            " from long_sequence(9)" +
                            ")"
            );

            assertQuery("select distinct b from x order by a desc;")
                    .fails(34, "ORDER BY expressions must appear in select list.");
        });
    }

    @Test
    public void testDistinctWithOrderByIndex() throws Exception {
        assertQuery("select distinct b from x order by 1 desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("b\n2\n1\n0\n");
    }

    @Test
    public void testExpressionSearchOrderByAlias() throws Exception {
        final String expected = """
                sym\tspread
                HBC\t-1912873112
                HBC\t-1707758909
                DXR\t-1021839040
                HBC\t-850582456
                ABB\t4171981
                ABB\t74196247
                ABB\t417348950
                ABB\t1191199593
                ABB\t1233285715
                DXR\t1275864035
                """;

        assertQuery("select sym, ask-bid spread from x where ts IN '1970-01-03' order by spread")
                .ddl("""
                        create table x (
                            sym symbol index,
                            bid int,
                            ask int,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_int() bid,\s
                                rnd_int() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)""")
                .returns("sym\tspread\n", expected);
    }

    @Test
    public void testFunctionSearchOrderByAlias() throws Exception {
        final String expected = """
                sym\tmaxp
                DXR\t0.97613283653158
                ABB\t0.9809851788419132
                HBC\t0.9940353811420282
                """;

        assertQuery("select sym , max(price) maxp from x where ts IN '1970-01-04' order by maxp")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .expectSize()
                .returns("sym\tmaxp\n", expected);
    }

    @Test
    public void testFunctionSearchOrderByAlias2() throws Exception {
        final String expected = """
                sym\tmaxp
                DXR\t0.008134052047644613
                HBC\t0.008427132543617488
                ABB\t0.008444033230580739
                """;

        assertQuery("select sym, min(price) maxp from x where ts in '1970-01-04' order by maxp")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .expectSize()
                .returns("sym\tmaxp\n", expected);
    }

    @Test
    public void testInputOrderAggregatesReadIndexKeysInAnyOrder() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedKeyTable();
            assertIndexOrder(ScanPlan.IndexOrder.TABLE_PER_FRAME, "SELECT sym, arg_max(v, ts) FROM t WHERE sym IN ('a', 'b') GROUP BY sym");
            assertIndexOrder(ScanPlan.IndexOrder.TABLE_PER_FRAME, "SELECT sym, string_agg(v::STRING, ',') FROM t WHERE sym IN ('a', 'b') GROUP BY sym");
            assertIndexOrder(ScanPlan.IndexOrder.KEY_PER_FRAME, "SELECT arg_max(v, ts) FROM t WHERE sym IN ('a', 'b')");
            assertQuery("SELECT arg_max(v, ts) FROM t WHERE sym IN ('a', 'b')")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              values: [arg_max(v,ts)]
                                FilterOnValues symbolOrder: desc
                                    Cursor-order scan
                                        Index forward scan on: sym deferred: true
                                          filter: sym='b'
                                        Index forward scan on: sym deferred: true
                                          filter: sym='a'
                                    Frame forward scan on: t
                            """);
            assertIndexOrder(ScanPlan.IndexOrder.KEY_PER_FRAME, "SELECT mode(v) FROM t WHERE sym IN ('a', 'b')");
            assertQuery("SELECT mode(v) FROM t WHERE sym IN ('a', 'b')")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              values: [mode(v)]
                                FilterOnValues symbolOrder: desc
                                    Cursor-order scan
                                        Index forward scan on: sym deferred: true
                                          filter: sym='b'
                                        Index forward scan on: sym deferred: true
                                          filter: sym='a'
                                    Frame forward scan on: t
                            """);
            assertIndexOrder(ScanPlan.IndexOrder.KEY_PER_FRAME, "SELECT string_agg(v::STRING, ',') FROM t WHERE sym IN ('a', 'b')");
            assertQuery("SELECT string_agg(v::STRING, ',') FROM t WHERE sym IN ('a', 'b')")
                    .noLeakCheck()
                    .assertsPlan("""
                            GroupBy vectorized: false
                              values: [string_agg(v::string,,)]
                                FilterOnValues symbolOrder: desc
                                    Cursor-order scan
                                        Index forward scan on: sym deferred: true
                                          filter: sym='b'
                                        Index forward scan on: sym deferred: true
                                          filter: sym='a'
                                    Frame forward scan on: t
                            """);
        });
    }

    @Test
    public void testIntervalSearchOrderByTimestampDescAndColumnLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT x::DOUBLE v, (x / 3 * 1_000_000)::TIMESTAMP ts FROM long_sequence(9)) TIMESTAMP(ts) PARTITION BY DAY");
            assertQuery("SELECT ts, v FROM t WHERE ts IN '1970-01-01' ORDER BY ts DESC, v LIMIT 5")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .withPlan("""
                            Encode sort light lo: 5 partiallySorted: true
                              keys: [ts desc, v]
                                PageFrame
                                    Row backward scan
                                    Interval backward scan on: t
                                      intervals: [("1970-01-01T00:00:00.000000Z","1970-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            ts\tv
                            1970-01-01T00:00:03.000000Z\t9.0
                            1970-01-01T00:00:02.000000Z\t6.0
                            1970-01-01T00:00:02.000000Z\t7.0
                            1970-01-01T00:00:02.000000Z\t8.0
                            1970-01-01T00:00:01.000000Z\t3.0
                            """);
        });
    }

    @Test
    public void testNoKeyGroupBy() throws Exception {
        assertQuery("select sum(price) / count() from x where price > 0")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .noRandomAccess()
                .expectSize()
                .returns("column\nnull\n", """
                        column
                        0.48510032025339733
                        """);
    }

    @Test
    public void testOrderBy2Columns() throws Exception {
        final String expected = """
                k\tprice\tts
                ABB\t0.4217768841969397\t1970-01-03T00:48:00.000000Z
                ABB\t0.7611029514995744\t1970-01-03T00:42:00.000000Z
                ABB\t0.3491070363730514\t1970-01-03T00:36:00.000000Z
                ABB\t0.22452340856088226\t1970-01-03T00:30:00.000000Z
                ABB\t0.8043224099968393\t1970-01-03T00:00:00.000000Z
                DXR\t0.0843832076262595\t1970-01-03T00:12:00.000000Z
                DXR\t0.08486964232560668\t1970-01-03T00:06:00.000000Z
                HBC\t0.0367581207471136\t1970-01-03T00:54:00.000000Z
                HBC\t0.7905675319675964\t1970-01-03T00:24:00.000000Z
                HBC\t0.6508594025855301\t1970-01-03T00:18:00.000000Z
                """;

        assertQuery("select sym k, price, ts from x order by k, ts desc")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)""")
                .expectSize()
                .returns("k\tprice\tts\n", expected);
    }

    @Test
    public void testOrderByAggregateKeepsOrdering() throws Exception {
        execute("""
                create table trades (
                    sym symbol,
                    price double,
                    ts timestamp
                ) timestamp(ts) partition by DAY
                """);
        execute("""
                insert into trades values
                ('AAPL', 100.0, cast('2023-01-01T09:00:00.000000Z' as TIMESTAMP)),
                ('AAPL', 101.0, cast('2023-01-01T09:01:00.000000Z' as TIMESTAMP)),
                ('AAPL', 102.0, cast('2023-01-01T09:02:00.000000Z' as TIMESTAMP)),
                ('MSFT', 200.0, cast('2023-01-01T09:03:00.000000Z' as TIMESTAMP)),
                ('MSFT', 201.0, cast('2023-01-01T09:04:00.000000Z' as TIMESTAMP)),
                ('GOOGL', 300.0, cast('2023-01-01T09:05:00.000000Z' as TIMESTAMP)),
                ('GOOGL', 301.0, cast('2023-01-01T09:06:00.000000Z' as TIMESTAMP)),
                ('AAPL', 103.0, cast('2023-01-01T09:07:00.000000Z' as TIMESTAMP)),
                ('MSFT', 202.0, cast('2023-01-01T09:08:00.000000Z' as TIMESTAMP)),
                ('GOOGL', 302.0, cast('2023-01-01T09:09:00.000000Z' as TIMESTAMP))
                """);

        execute("""
                create table prices (
                    sym symbol,
                    price double,
                    ts timestamp
                ) timestamp(ts) partition by DAY
                """);
        execute("""
                insert into prices values
                ('AAPL', 99.5, cast('2023-01-01T08:59:00.000000Z' as TIMESTAMP)),
                ('AAPL', 100.5, cast('2023-01-01T09:00:00.000000Z' as TIMESTAMP)),
                ('AAPL', 101.5, cast('2023-01-01T09:01:00.000000Z' as TIMESTAMP)),
                ('MSFT', 199.5, cast('2023-01-01T09:02:00.000000Z' as TIMESTAMP)),
                ('MSFT', 200.5, cast('2023-01-01T09:03:00.000000Z' as TIMESTAMP)),
                ('GOOGL', 299.5, cast('2023-01-01T09:04:00.000000Z' as TIMESTAMP)),
                ('GOOGL', 300.5, cast('2023-01-01T09:05:00.000000Z' as TIMESTAMP)),
                ('AAPL', 102.5, cast('2023-01-01T09:06:00.000000Z' as TIMESTAMP)),
                ('MSFT', 201.5, cast('2023-01-01T09:07:00.000000Z' as TIMESTAMP)),
                ('GOOGL', 301.5, cast('2023-01-01T09:08:00.000000Z' as TIMESTAMP))
                """);

        assertQuery("""
                select sym, price, ts, first(concat(price0, '000')) f
                from (
                  select t.*, p.price as price0
                  from (trades where ts > '2023-01-01T09:00:00Z' limit 1, 4) t
                  left join prices p
                  on (t.sym = p.sym) and p.ts >= dateadd('m', -1, t.ts) AND p.ts <= dateadd('m', 1, t.ts)
                  order by t.ts, p.ts
                )
                order by ts
                """)
                .ddl(null)
                .timestamp("ts")
                .expectSize()
                .returns("""
                        sym	price	ts	f
                        AAPL	102.0	2023-01-01T09:02:00.000000Z	101.5000
                        MSFT	200.0	2023-01-01T09:03:00.000000Z	199.5000
                        MSFT	201.0	2023-01-01T09:04:00.000000Z	200.5000
                        """);
    }

    @Test
    public void testOrderByGeoByte() throws Exception {
        assertQuery("select * from pos order by geo desc")
                .ddl("CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(5b) " +
                        ")")
                .mutateWith("insert into pos  values ( 1,##00001), ( 2,##00010), ( 3, ##00011 ), ( 16, ##10000 )")
                .expectSize()
                .returns("id\tgeo\n", """
                        id\tgeo
                        16\th
                        3\t3
                        2\t2
                        1\t1
                        """);
    }

    @Test
    public void testOrderByGeoInt1() throws Exception {
        assertQuery("select * from pos order by geo desc")
                .ddl("CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(20b) " +
                        ")")
                .mutateWith("insert into pos  values ( 1,##00000000000000000001), ( 2,##00000000000000000010), ( 3, ##00000000000000000011 ), ( 16, ##00000000000000010000 )")
                .expectSize()
                .returns("id\tgeo\n", """
                        id\tgeo
                        16\t000h
                        3\t0003
                        2\t0002
                        1\t0001
                        """);
    }

    @Test
    public void testOrderByGeoInt2() throws Exception {
        assertQuery("select * from pos order by geo desc")
                .ddl("CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(17b) " +
                        ")")
                .mutateWith("insert into pos  values ( 1,##00000000000000001), ( 2,##00000000000000010), ( 3, ##00000000000000011 ), ( 16, ##00000000000010000 )")
                .expectSize()
                .returns("id\tgeo\n", """
                        id\tgeo
                        16\t00000000000010000
                        3\t00000000000000011
                        2\t00000000000000010
                        1\t00000000000000001
                        """);
    }

    @Test
    public void testOrderByGeoLong() throws Exception {
        assertQuery("select * from pos order by geo desc")
                .ddl("CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(35b) " +
                        ")")
                .mutateWith("insert into pos  values ( 1,##00000000000000000000000000000000001), ( 2,##00000000000000000000000000000000010), " +
                        "( 3, ##00000000000000000000000000000000011 ), ( 16, ##00000000000000000000000000000010000 )")
                .expectSize()
                .returns("id\tgeo\n", """
                        id\tgeo
                        16\t000000h
                        3\t0000003
                        2\t0000002
                        1\t0000001
                        """);
    }

    @Test
    public void testOrderByGeoShort() throws Exception {
        assertQuery("select * from pos order by geo desc")
                .ddl("CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(10b) " +
                        ")")
                .mutateWith("insert into pos  values ( 1,##0000000001), ( 2,##0000000010), ( 3, ##0000000011 ), ( 16, ##0000010000 )")
                .expectSize()
                .returns("id\tgeo\n", """
                        id\tgeo
                        16\t0h
                        3\t03
                        2\t02
                        1\t01
                        """);
    }

    @Test
    public void testOrderByMultipleColumns() throws Exception {
        final String expected = """
                sym\tprice\tts
                AA\t-847531048\t1970-01-03T00:24:00.000000Z
                AA\t315515118\t1970-01-03T00:00:00.000000Z
                AA\t339631474\t1970-01-03T00:54:00.000000Z
                AA\t1573662097\t1970-01-03T00:48:00.000000Z
                BB\t-2041844972\t1970-01-03T00:30:00.000000Z
                BB\t-1575378703\t1970-01-03T00:36:00.000000Z
                BB\t-727724771\t1970-01-03T00:06:00.000000Z
                BB\t1545253512\t1970-01-03T00:42:00.000000Z
                """;

        assertQuery("select * from tab where sym in ('AA', 'BB') order by sym, price")
                .ddl("""
                        create table tab (
                            sym symbol index,
                            price int,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') sym,\s
                                rnd_int() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)""")
                .returns("sym\tprice\tts\n", expected);
    }

    @Test
    public void testOrderByPriority_columnAliases() throws Exception {
        assertQuery("select a as aliased from x order by aliased asc, aliased desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("aliased\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    @Test
    public void testOrderByPriority_columnDoubleAliased() throws Exception {
        assertQuery("select a as a1, a as a2 from x order by a1 asc, a2 desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("""
                        a1\ta2
                        1\t1
                        2\t2
                        3\t3
                        4\t4
                        5\t5
                        6\t6
                        7\t7
                        8\t8
                        9\t9
                        """);
    }

    @Test
    public void testOrderByPriority_columnLiteralsMixedWithPositions() throws Exception {
        assertQuery("select a from x order by a asc, 1 desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    @Test
    public void testOrderByPriority_columnNameLiteral() throws Exception {
        assertQuery("select a from x order by a asc, a desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    @Test
    public void testOrderByPriority_columnNameLiteral_differentCases() throws Exception {
        assertQuery("select a from x order by a asc, A desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    @Test
    public void testOrderByPriority_columnPositions() throws Exception {
        assertQuery("select a from x order by 1 asc, 1 desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    @Test
    public void testOrderByPriority_columnQuoted() throws Exception {
        assertQuery("select a from x order by \"a\" asc, \"a\" desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n");
    }

    @Test
    public void testOrderByPriority_columnsInterleaved() throws Exception {
        assertQuery("select * from x order by b asc, a desc, b desc;")
                .ddl("create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("""
                        a\tb
                        9\t0
                        6\t0
                        3\t0
                        7\t1
                        4\t1
                        1\t1
                        8\t2
                        5\t2
                        2\t2
                        """);
    }

    @Test
    public void testOrderByTwoGeoHashes1() throws Exception {
        assertQuery("select * from pos order by geo1 asc, geo3 desc")
                .ddl("CREATE TABLE pos (" +
                        "  id int," +
                        "  geo1 GEOHASH(1c)," +
                        "  geo3 GEOHASH(3c)" +
                        ")")
                .mutateWith("insert into pos  values ( 1, #1, #001), ( 2, #1, #002), ( 4, #4, #004) ")
                .expectSize()
                .returns("id\tgeo1\tgeo3\n", """
                        id\tgeo1\tgeo3
                        2\t1\t002
                        1\t1\t001
                        4\t4\t004
                        """);
    }

    @Test
    public void testOrderByTwoGeoHashes2() throws Exception {
        assertQuery("select * from pos order by geo1 asc, geo3 desc")
                .ddl("CREATE TABLE pos (" +
                        "  id int," +
                        "  geo1 GEOHASH(7b)," +
                        "  geo3 GEOHASH(15b)" +
                        ")")
                .mutateWith("insert into pos  values ( 1, ##0000001, ##000000000000001), " +
                        "( 2, ##1000000, ##100000000000000), ( 3, ##0000001, ##100000000000000) ")
                .expectSize()
                .returns("id\tgeo1\tgeo3\n", """
                        id\tgeo1\tgeo3
                        3\t0000001\th00
                        1\t0000001\t001
                        2\t1000000\th00
                        """);
    }

    @Test
    public void testOrderSensitiveAggregatesKeepSubQueryOrder() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t AS (
                        SELECT x id, x % 2 g, x % 3 m, (x % 3)::STRING::SYMBOL msym, x::STRING s, x::STRING::SYMBOL sym,
                            ((x * 7) % 5)::DOUBLE lat, 0.0 lon, 1 k, x::TIMESTAMP ts
                        FROM long_sequence(12)
                    ) TIMESTAMP(ts)""");
            assertQuery("""
                    SELECT g, string_agg(s, ',') sa, string_agg(s::VARCHAR, ',') va, string_distinct_agg(s, ',') sd,
                        string_distinct_agg(sym, ',') yd, string_distinct_agg(s::VARCHAR, ',') vd
                    FROM (SELECT * FROM t ORDER BY id DESC)
                    GROUP BY g
                    ORDER BY g""")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tsa\tva\tsd\tyd\tvd
                            0\t12,10,8,6,4,2\t12,10,8,6,4,2\t12,10,8,6,4,2\t12,10,8,6,4,2\t12,10,8,6,4,2
                            1\t11,9,7,5,3,1\t11,9,7,5,3,1\t11,9,7,5,3,1\t11,9,7,5,3,1\t11,9,7,5,3,1
                            """);
            assertQuery("""
                    SELECT g, mode(m) ml, mode(m::DOUBLE) md, mode(msym) mk, arg_max(id, k) amax, arg_min(id, k) amin,
                        isOrdered(id) o
                    FROM (SELECT * FROM t ORDER BY id DESC)
                    GROUP BY g
                    ORDER BY g""")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tml\tmd\tmk\tamax\tamin\to
                            0\t0\t0.0\t1\t12\t12\tfalse
                            1\t0\t0.0\t2\t11\t11\tfalse
                            """);
            assertQuery("SELECT g, haversine_dist_deg(lat, lon, ts) dist FROM (SELECT * FROM t ORDER BY lat) GROUP BY g ORDER BY g")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            g\tdist
                            0\t444.7858501372019
                            1\t444.7858501372019
                            """);
            assertQuery("SELECT g, twap(m, ts) FROM (SELECT * FROM t ORDER BY ts DESC) GROUP BY g ORDER BY g")
                    .noLeakCheck()
                    .fails(10, "twap() requires the base query to provide ascending designated timestamp order");
            assertQuery("SELECT g, sparkline(m) FROM (SELECT * FROM t ORDER BY ts DESC) GROUP BY g ORDER BY g")
                    .noLeakCheck()
                    .fails(10, "sparkline() requires the base query to provide ascending designated timestamp order");
        });
    }

    @Test
    public void testSelectWithInClauseAndOrderByTimestampDesc() throws Exception {
        final String expected = """
                sym\tbid\task\tts
                BB\t-85170055\t-1792928964\t1970-01-03T00:54:00.000000Z
                AA\t-1849627000\t-1432278050\t1970-01-03T00:48:00.000000Z
                AA\t-1532328444\t-1458132197\t1970-01-03T00:42:00.000000Z
                AA\t339631474\t1530831067\t1970-01-03T00:36:00.000000Z
                AA\t1569490116\t1573662097\t1970-01-03T00:30:00.000000Z
                BB\t-1575378703\t806715481\t1970-01-03T00:24:00.000000Z
                BB\t-1191262516\t-2041844972\t1970-01-03T00:18:00.000000Z
                AA\t315515118\t1548800833\t1970-01-03T00:00:00.000000Z
                """;

        assertQuery("select * from x where sym in ('AA', 'BB' ) order by ts desc")
                .ddl("""
                        create table x (
                            sym symbol index,
                            bid int,
                            ask int,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym,\s
                                rnd_int() bid,\s
                                rnd_int() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)""")
                .timestampDesc("ts")
                .returns("sym\tbid\task\tts\n", expected);
    }

    @Test
    public void testSelectWithOrderByTimestampDesc() throws Exception {
        final String expected = """
                sym\tbid\task\tts
                BB\t-85170055\t-1792928964\t1970-01-03T00:54:00.000000Z
                AA\t-1849627000\t-1432278050\t1970-01-03T00:48:00.000000Z
                AA\t-1532328444\t-1458132197\t1970-01-03T00:42:00.000000Z
                AA\t339631474\t1530831067\t1970-01-03T00:36:00.000000Z
                AA\t1569490116\t1573662097\t1970-01-03T00:30:00.000000Z
                BB\t-1575378703\t806715481\t1970-01-03T00:24:00.000000Z
                BB\t-1191262516\t-2041844972\t1970-01-03T00:18:00.000000Z
                CC\t592859671\t1868723706\t1970-01-03T00:12:00.000000Z
                CC\t73575701\t-948263339\t1970-01-03T00:06:00.000000Z
                AA\t315515118\t1548800833\t1970-01-03T00:00:00.000000Z
                """;

        assertQuery("select * from x order by ts desc")
                .ddl("""
                        create table x (
                            sym symbol index,
                            bid int,
                            ask int,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym,\s
                                rnd_int() bid,\s
                                rnd_int() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)""")
                .timestampDesc("ts")
                .expectSize()
                .returns("sym\tbid\task\tts\n", expected);
    }

    @Test
    public void testSingleSymbolSearchOrderByAliasAndTimestamp() throws Exception {
        final String expected = """
                k\tprice\tts
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                """;

        assertQuery("select sym k, price, ts from x where sym = 'HBC' and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("k\tprice\tts\n", expected);
    }

    @Test
    public void testSingleSymbolSearchOrderByAliasAndTimestampDesc() throws Exception {
        final String expected = """
                k\tprice\tts
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                """;

        assertQuery("select sym k, price, ts from x where sym = 'HBC' and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' and 2 > 1 order by k, ts desc")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("k\tprice\tts\n", expected);
    }

    @Test
    public void testSingleSymbolSearchOrderByAliasAndTimestampDescEmpty() throws Exception {
        TestMatchFunctionFactory.clear();
        assertMemoryLeak(() -> {
            assertQuery("select sym k, price, ts from x where 1 = 2 and sym = 'HBC' and test_match() and ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts desc")
                    .noLeakCheck()
                    .ddl("""
                            create table x (
                                sym symbol index,
                                price double,
                                ts timestamp
                            ) timestamp(ts) partition by DAY""")
                    .expectSize()
                    .returns("k\tprice\tts\n");
            Assert.assertTrue(TestMatchFunctionFactory.isClosed());
        });
    }

    @Test
    public void testSingleSymbolSearchOrderByTimestampAndColumnNegativeLimit() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedTable();
            assertQuery("SELECT ts, id FROM t WHERE s = 'S1' ORDER BY ts, id LIMIT -4")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                Encode sort light
                                  keys: [ts, id]
                                    SelectedRecord
                                        Encode sort light lo: 4 partiallySorted: true
                                          keys: [ts desc, id desc]
                                            DeferredSingleSymbolFilterPageFrame
                                                Index backward scan on: s deferred: true
                                                  filter: s='S1'
                                                Frame backward scan on: t
                            """)
                    .returns("""
                            ts\tid
                            2024-01-01T02:40:00.000000Z\t17
                            2024-01-01T03:00:00.000000Z\t19
                            2024-01-01T03:20:00.000000Z\t21
                            2024-01-01T03:40:00.000000Z\t23
                            """);
        });
    }

    @Test
    public void testSingleSymbolSearchOrderByTimestampDescAndColumnLimit() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedTable();
            assertQuery("SELECT ts, id FROM t WHERE s = 'S1' ORDER BY ts DESC, id LIMIT 4")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .withPlan("""
                            SelectedRecord
                                Encode sort light lo: 4 partiallySorted: true
                                  keys: [ts desc, id]
                                    SelectedRecord
                                        DeferredSingleSymbolFilterPageFrame
                                            Index backward scan on: s deferred: true
                                              filter: s='S1'
                                            Frame backward scan on: t
                            """)
                    .returns("""
                            ts\tid
                            2024-01-01T03:40:00.000000Z\t23
                            2024-01-01T03:20:00.000000Z\t21
                            2024-01-01T03:00:00.000000Z\t19
                            2024-01-01T02:40:00.000000Z\t17
                            """);
            assertQuery("SELECT * FROM (SELECT ts t2, id, v FROM t WHERE s = 'S1' AND ts BETWEEN '2024-01-01T01:00:00' AND '2024-01-01T03:00:00') ORDER BY t2 DESC, v DESC LIMIT 2")
                    .noLeakCheck()
                    .timestampDesc("t2")
                    .expectSize()
                    .returns("""
                            t2\tid\tv
                            2024-01-01T03:00:00.000000Z\t19\t19.0
                            2024-01-01T02:40:00.000000Z\t17\t17.0
                            """);
        });
    }

    @Test
    public void testSingleSymbolSearchOrderByTimestampDescAndComputedColumnLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p AS (SELECT x id, 'a'::SYMBOL s, timestamp_sequence(0, 1_000_000L) ts FROM long_sequence(50)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE p ALTER COLUMN s ADD INDEX");
            assertQuery("SELECT id + 3 val, ts FROM p WHERE s = 'a' ORDER BY ts DESC, id DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("""
                            val\tts
                            53\t1970-01-01T00:00:49.000000Z
                            52\t1970-01-01T00:00:48.000000Z
                            51\t1970-01-01T00:00:47.000000Z
                            """);
        });
    }

    @Test
    public void testSkipDesignatedTimestampFromMultipleOrderBy() throws Exception {
        assertQuery("select * from x order by a, t")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_int() a," +
                        " timestamp_sequence(0, 1000) t" +
                        " from long_sequence(9)" +
                        ")")
                .expectSize()
                .returns("""
                        a\tt
                        -1148479920\t1970-01-01T00:00:00.000000Z
                        -948263339\t1970-01-01T00:00:00.005000Z
                        -727724771\t1970-01-01T00:00:00.003000Z
                        73575701\t1970-01-01T00:00:00.004000Z
                        315515118\t1970-01-01T00:00:00.001000Z
                        592859671\t1970-01-01T00:00:00.007000Z
                        1326447242\t1970-01-01T00:00:00.006000Z
                        1548800833\t1970-01-01T00:00:00.002000Z
                        1868723706\t1970-01-01T00:00:00.008000Z
                        """);
    }

    @Test
    public void testSortOverParquetReadsRowGroupsAfterEviction() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 1_000);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT (CASE WHEN x % 7 = 0 THEN NULL WHEN x % 10 < 6 THEN 'a' WHEN x % 10 < 8 THEN 'b' ELSE 'c' END)::SYMBOL sym,"
                    + " x::TIMESTAMP ts FROM long_sequence(20_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO t (ts) VALUES ('1975-01-01')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET WHERE ts < '1975-01-01'");
            final String query = "SELECT ts, sym FROM (SELECT ts, sym::STRING sym FROM t WHERE sym IN ('a', 'b')"
                    + " AND (ts BETWEEN 100 AND 2_000 OR ts BETWEEN 5_000 AND 9_000)) ORDER BY sym, ts LIMIT -1";
            // budgets below the twelve decoded row groups the scan reads evict row groups the sort then reads
            for (long budget : new long[]{256 * 1024 * 1024, 16_384, 32_768, 65_536, 131_072}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, budget);
                assertQuery(query)
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                ts	sym
                                1970-01-01T00:00:00.008997Z	b
                                """);
            }
        });
    }

    @Test
    public void testSortedSymbolIndexAcrossPageFrames() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTable("k", "DAY");
            sqlExecutionContext.changePageFrameSizes(1, 4);
            assertQuery("SELECT s, id FROM k WHERE ts IN '2024-01-01' ORDER BY s")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                SelectedRecord
                                    SortedSymbolIndex
                                        Index forward scan on: s
                                          symbolOrder: asc
                                        Interval forward scan on: k
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	id
                            S0	3
                            S0	6
                            S0	9
                            S0	12
                            S0	15
                            S0	18
                            S0	21
                            S0	24
                            S1	1
                            S1	4
                            S1	7
                            S1	10
                            S1	13
                            S1	16
                            S1	19
                            S1	22
                            S2	2
                            S2	5
                            S2	8
                            S2	11
                            S2	14
                            S2	17
                            S2	20
                            S2	23
                            """);
            assertQuery("SELECT s, id FROM k WHERE ts IN '2024-01-01' ORDER BY s DESC, ts DESC LIMIT 6, 10")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                Limit left: 6 right: 10 skip-rows-max: 6 take-rows-max: 4
                                    SortedSymbolIndex
                                        Index backward scan on: s
                                          symbolOrder: desc
                                        Interval backward scan on: k
                                          intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            s	id
                            S2	5
                            S2	2
                            S1	22
                            S1	19
                            """);
        });
    }

    @Test
    public void testSortedSymbolIndexFindsLateKeyRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n AS (SELECT (CASE WHEN x = 100_000 THEN NULL ELSE 'a' END)::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(100_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c AS (SELECT x::TIMESTAMP ts FROM long_sequence(50_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE c ADD COLUMN sym SYMBOL INDEX");
            execute("INSERT INTO c SELECT (50_000 + x)::TIMESTAMP, 'a' FROM long_sequence(50_000)");
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            // the only NULL row is the last row
            String query = "SELECT ts, sym FROM n WHERE ts >= 0 AND ts < 100_001 ORDER BY sym, ts LIMIT 2";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("symbolOrder: asc")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.100000Z\t
                            1970-01-01T00:00:00.000001Z	a
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM n WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts DESC LIMIT -2";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000001Z	a
                            1970-01-01T00:00:00.100000Z\t
                            """);
            // the column top holds NULL rows in the first half
            query = "SELECT ts, sym FROM c WHERE ts >= 0 AND ts < 100_001 ORDER BY sym, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000001Z\t
                            """);
            assertReadFrames(query, 1);
            query = "SELECT ts, sym FROM c WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	a
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM c WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 49_999, 50_001";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.100000Z	a
                            1970-01-01T00:00:00.000001Z\t
                            """);
            query = "SELECT ts, sym FROM c WHERE ts >= 0 AND ts < 100_001 ORDER BY sym, ts DESC LIMIT -1";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	a
                            """);

            // a single page frame holds the scan of n, and one page frame each side of the column top that of c
            sqlExecutionContext.changePageFrameSizes(1_000_000, 1_000_000);
            query = "SELECT ts, sym FROM n WHERE ts >= 0 AND ts < 100_001 ORDER BY sym, ts LIMIT 2";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.100000Z\t
                            1970-01-01T00:00:00.000001Z	a
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM c WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	a
                            """);
            assertReadFrames(query, 2);
        });
    }

    @Test
    public void testSortedSymbolIndexOverParquetDecodesEachRowGroupOnce() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetKeyTable();
            final String query = "SELECT * FROM k WHERE ts IN '2024-01-01' ORDER BY s";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlan("""
                            SortedSymbolIndex
                                Index forward scan on: s
                                  symbolOrder: asc
                                Interval forward scan on: k
                                  intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            id	s	ts
                            3	S0	2024-01-01T00:00:02.000000Z
                            6	S0	2024-01-01T00:00:05.000000Z
                            9	S0	2024-01-01T00:00:08.000000Z
                            12	S0	2024-01-01T00:00:11.000000Z
                            15	S0	2024-01-01T00:00:14.000000Z
                            18	S0	2024-01-01T00:00:17.000000Z
                            21	S0	2024-01-01T00:00:20.000000Z
                            24	S0	2024-01-01T00:00:23.000000Z
                            1	S1	2024-01-01T00:00:00.000000Z
                            4	S1	2024-01-01T00:00:03.000000Z
                            7	S1	2024-01-01T00:00:06.000000Z
                            10	S1	2024-01-01T00:00:09.000000Z
                            13	S1	2024-01-01T00:00:12.000000Z
                            16	S1	2024-01-01T00:00:15.000000Z
                            19	S1	2024-01-01T00:00:18.000000Z
                            22	S1	2024-01-01T00:00:21.000000Z
                            2	S2	2024-01-01T00:00:01.000000Z
                            5	S2	2024-01-01T00:00:04.000000Z
                            8	S2	2024-01-01T00:00:07.000000Z
                            11	S2	2024-01-01T00:00:10.000000Z
                            14	S2	2024-01-01T00:00:13.000000Z
                            17	S2	2024-01-01T00:00:16.000000Z
                            20	S2	2024-01-01T00:00:19.000000Z
                            23	S2	2024-01-01T00:00:22.000000Z
                            """);
            // the passes of the three keys and the NULL key decode each of the six row groups once
            assertKeyOrderedDecodes(query, null, ParquetDecodeHint.CYCLIC, 6);
            assertKeyOrderedDecodes(query, ParquetDecodeHint.SCATTERED, ParquetDecodeHint.SCATTERED, 6);
        });
    }

    @Test
    public void testSortedSymbolIndexOverParquetDoesNotRevisitEarlierIntervals() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
        assertMemoryLeak(() -> {
            final int intervalCount = 32;
            execute("CREATE TABLE k AS (SELECT x id, (CASE WHEN (x - 1) % 16 < 4 THEN 'A' ELSE 'B' END)::SYMBOL s, "
                    + "timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(" + intervalCount * 16 + ")), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES (513, 'absent', '2024-01-02')");
            execute("ALTER TABLE k CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final int[] frameVisits = {0};
            final SqlExecutionCircuitBreaker countingBreaker = new AtomicBooleanCircuitBreaker(engine) {
                @Override
                public void statefulThrowExceptionIfTrippedTimeThrottledOrYield() {
                    frameVisits[0]++;
                    super.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                }
            };
            for (boolean isFiltered : new boolean[]{true, false}) {
                for (boolean isSymbolDescending : new boolean[]{false, true}) {
                    for (boolean isTimestampDescending : new boolean[]{false, true}) {
                        final String sql = "SELECT id, s FROM k WHERE "
                                + (isFiltered ? "s IN ('A', 'B') AND " : "")
                                + "ts IN '2024-01-01T00:00:00;8s;16s;" + intervalCount + "' ORDER BY s"
                                + (isSymbolDescending ? " DESC" : "") + ", ts" + (isTimestampDescending ? " DESC" : "");
                        final StringSink expected = new StringSink();
                        expected.put("id\ts\n");
                        for (int i = 0; i < 2; i++) {
                            final int key = isSymbolDescending ? 1 - i : i;
                            for (int j = 0; j < intervalCount; j++) {
                                final int interval = isTimestampDescending ? intervalCount - j - 1 : j;
                                for (int row = 0; row < 4; row++) {
                                    expected.put(interval * 16 + key * 4 + (isTimestampDescending ? 4 - row : row + 1))
                                            .put('\t').put(key == 0 ? 'A' : 'B').put('\n');
                                }
                            }
                        }
                        assertQuery(sql)
                                .noLeakCheck()
                                .withPlanContaining(isFiltered ? "FilterOnValues symbolOrder:" : "SortedSymbolIndex")
                                .returns(expected);
                        try (RecordCursorFactory factory = select(sql)) {
                            final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
                            ((SqlExecutionContextImpl) sqlExecutionContext).with(countingBreaker);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                for (int pass = 0; pass < 2; pass++) {
                                    cursor.toTop();
                                    frameVisits[0] = 0;
                                    int rows = 0;
                                    while (cursor.hasNext()) {
                                        rows++;
                                    }
                                    Assert.assertEquals(intervalCount * 8, rows);
                                    Assert.assertTrue("frame visits: " + frameVisits[0], frameVisits[0] < intervalCount * 16);
                                }
                            } finally {
                                ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSortedSymbolIndexOverParquetSkipsEmptyRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k AS (SELECT x id, (CASE WHEN x <= 4 THEN null ELSE 'S' || ((x - 1) / 4) END)::SYMBOL s, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(24)), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES (25, 'absent', '2024-01-02')");
            execute("ALTER TABLE k CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final String query = "SELECT * FROM k WHERE ts IN '2024-01-01' ORDER BY s, ts";
            node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 2 * decodedRowGroupBytes(query));
            for (boolean isFiltered : new boolean[]{false, true}) {
                for (boolean isSymbolDescending : new boolean[]{false, true}) {
                    for (boolean isTimestampDescending : new boolean[]{false, true}) {
                        final String sql = " FROM k WHERE "
                                + (isFiltered ? "s IN ('S1', 'S4') AND " : "")
                                + "ts IN '2024-01-01' ORDER BY s"
                                + (isSymbolDescending ? " DESC" : "")
                                + ", ts" + (isTimestampDescending ? " DESC" : "");
                        final StringSink expected = new StringSink();
                        expected.put("id\ts\n");
                        for (int i = 0; i < 6; i++) {
                            final int key = isSymbolDescending ? 5 - i : i;
                            if (isFiltered && key != 1 && key != 4) {
                                continue;
                            }
                            for (int j = 0; j < 4; j++) {
                                expected.put(key * 4 + (isTimestampDescending ? 4 - j : j + 1)).put('\t');
                                if (key > 0) {
                                    expected.put('S').put(key);
                                }
                                expected.put('\n');
                            }
                        }
                        assertQuery("SELECT id, s" + sql)
                                .noLeakCheck()
                                .withPlanContaining(isFiltered ? "FilterOnValues" : "SortedSymbolIndex")
                                .returns(expected);
                        // the budget of two of the six row groups holds the rows in one window, a pass that decodes
                        // each row group that holds them once
                        assertKeyOrderedDecodes("SELECT *" + sql, null, ParquetDecodeHint.MONOTONIC, isFiltered ? 2 : 6);
                        assertKeyOrderedDecodes("SELECT *" + sql, ParquetDecodeHint.SCATTERED, ParquetDecodeHint.MONOTONIC, isFiltered ? 2 : 6);
                    }
                }
            }
        });
    }

    @Test
    public void testSortedSymbolIndexOverParquetSkipsCachedFrames() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            final int keyCount = 128;
            execute("CREATE TABLE k AS (SELECT x id, ('S' || (100 + (x - 1) / 4))::SYMBOL s, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(" + keyCount * 4 + ")), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES (513, 'absent', '2024-01-02')");
            execute("ALTER TABLE k CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final int[] frameVisits = {0};
            final SqlExecutionCircuitBreaker countingBreaker = new AtomicBooleanCircuitBreaker(engine) {
                @Override
                public void statefulThrowExceptionIfTrippedTimeThrottledOrYield() {
                    frameVisits[0]++;
                    super.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
                }
            };
            for (boolean isSymbolDescending : new boolean[]{false, true}) {
                for (boolean isTimestampDescending : new boolean[]{false, true}) {
                    final String sql = "SELECT id, s FROM k WHERE ts IN '2024-01-01' ORDER BY s"
                            + (isSymbolDescending ? " DESC" : "") + ", ts" + (isTimestampDescending ? " DESC" : "");
                    final StringSink expected = new StringSink();
                    expected.put("id\ts\n");
                    for (int i = 0; i < keyCount; i++) {
                        final int key = isSymbolDescending ? keyCount - i - 1 : i;
                        for (int j = 0; j < 4; j++) {
                            expected.put(key * 4 + (isTimestampDescending ? 4 - j : j + 1)).put("\tS").put(100 + key).put('\n');
                        }
                    }
                    assertQuery(sql).noLeakCheck().withPlanContaining("SortedSymbolIndex").returns(expected);
                    try (RecordCursorFactory factory = select(sql)) {
                        final SqlExecutionCircuitBreaker originalBreaker = sqlExecutionContext.getCircuitBreaker();
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(countingBreaker);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            for (int pass = 0; pass < 2; pass++) {
                                cursor.toTop();
                                frameVisits[0] = 0;
                                int rows = 0;
                                while (cursor.hasNext()) {
                                    rows++;
                                }
                                Assert.assertEquals(keyCount * 4, rows);
                                // The cursor checks its breaker per visited frame, including undecoded metadata.
                                Assert.assertTrue("frame visits: " + frameVisits[0], frameVisits[0] < keyCount * 12);
                            }
                        } finally {
                            ((SqlExecutionContextImpl) sqlExecutionContext).with(originalBreaker);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSortedSymbolIndexSkipsParquetFramesAcrossIntervals() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE k AS (SELECT x id, (CASE WHEN (x - 1) % 24 < 4 THEN null ELSE 'S' || (((x - 1) % 24) / 4) END)::SYMBOL s, timestamp_sequence('2024-01-01', 3_600_000_000) ts FROM long_sequence(72)), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO k VALUES (73, 'absent', '2024-01-04')");
            execute("ALTER TABLE k CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            execute("ALTER TABLE k CONVERT PARTITION TO PARQUET WHERE ts >= '2024-01-03' AND ts < '2024-01-04'");
            for (boolean isFiltered : new boolean[]{false, true}) {
                for (boolean isSymbolDescending : new boolean[]{false, true}) {
                    for (boolean isTimestampDescending : new boolean[]{false, true}) {
                        final String sql = "SELECT id, s FROM k WHERE "
                                + (isFiltered ? "s IN (null, 'S1', 'S4') AND " : "")
                                + "ts IN '2024-01-01T01:00:00;6h;12h;" + (isFiltered ? 6 : 2) + "' ORDER BY s"
                                + (isSymbolDescending ? " DESC" : "") + ", ts" + (isTimestampDescending ? " DESC" : "");
                        final StringSink expected = new StringSink();
                        expected.put("id\ts\n");
                        for (int i = 0; i < 6; i++) {
                            final int key = isSymbolDescending ? 5 - i : i;
                            if (isFiltered && key != 0 && key != 1 && key != 4) {
                                continue;
                            }
                            final int days = isFiltered ? 3 : 1;
                            for (int day = 0; day < days; day++) {
                                for (int j = 0; j < 4; j++) {
                                    final int hour = key * 4 + (isTimestampDescending ? 3 - j : j);
                                    if (hour % 12 < 1 || hour % 12 > 6) {
                                        continue;
                                    }
                                    expected.put((isTimestampDescending ? days - day - 1 : day) * 24 + hour + 1).put('\t');
                                    if (key > 0) {
                                        expected.put('S').put(key);
                                    }
                                    expected.put('\n');
                                }
                            }
                        }
                        assertQuery(sql)
                                .noLeakCheck()
                                .withPlanContaining(isFiltered ? "FilterOnValues" : "SortedSymbolIndex")
                                .returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testSortedSymbolIndexKeyWithoutRowsReadsOnePageFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 'a'::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(100_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE r AS (SELECT (CASE WHEN x > 50_000 THEN 'b' ELSE 'a' END)::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(100_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            // NULL comes first in the key order and has no row
            String query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("symbolOrder: asc")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000001Z	a
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym, ts DESC LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.100000Z	a
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000001Z	a
                            """);
            assertReadFrames(query, 1);
            query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym, ts LIMIT -2";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.099999Z	a
                            1970-01-01T00:00:00.100000Z	a
                            """);
            // 'a' leads the key order with rows only before the interval
            query = "SELECT ts, sym FROM r WHERE ts >= 50_001 AND ts < 100_001 ORDER BY sym, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	b
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM r WHERE ts >= 50_001 AND ts < 100_001 ORDER BY sym, ts DESC LIMIT 2";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.100000Z	b
                            1970-01-01T00:00:00.099999Z	b
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM r WHERE ts >= 50_001 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT -1";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.100000Z	b
                            """);

            // a single page frame holds the scan, so the keys after the first pass share one
            sqlExecutionContext.changePageFrameSizes(1_000_000, 1_000_000);
            query = "SELECT ts, sym FROM r WHERE ts >= 50_001 AND ts < 100_001 ORDER BY sym, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	b
                            """);
            assertReadFrames(query, 2);
        });
    }

    @Test
    public void testSortedSymbolIndexOverParquetGathersWindowsOverBudget() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetKeyTable();
            final String query = "SELECT * FROM k WHERE ts IN '2024-01-01' ORDER BY s DESC, ts DESC";
            node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 3 * decodedRowGroupBytes(query));
            assertQuery(query)
                    .noLeakCheck()
                    .withPlan("""
                            SortedSymbolIndex
                                Index backward scan on: s
                                  symbolOrder: desc
                                Interval backward scan on: k
                                  intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns("""
                            id	s	ts
                            23	S2	2024-01-01T00:00:22.000000Z
                            20	S2	2024-01-01T00:00:19.000000Z
                            17	S2	2024-01-01T00:00:16.000000Z
                            14	S2	2024-01-01T00:00:13.000000Z
                            11	S2	2024-01-01T00:00:10.000000Z
                            8	S2	2024-01-01T00:00:07.000000Z
                            5	S2	2024-01-01T00:00:04.000000Z
                            2	S2	2024-01-01T00:00:01.000000Z
                            22	S1	2024-01-01T00:00:21.000000Z
                            19	S1	2024-01-01T00:00:18.000000Z
                            16	S1	2024-01-01T00:00:15.000000Z
                            13	S1	2024-01-01T00:00:12.000000Z
                            10	S1	2024-01-01T00:00:09.000000Z
                            7	S1	2024-01-01T00:00:06.000000Z
                            4	S1	2024-01-01T00:00:03.000000Z
                            1	S1	2024-01-01T00:00:00.000000Z
                            24	S0	2024-01-01T00:00:23.000000Z
                            21	S0	2024-01-01T00:00:20.000000Z
                            18	S0	2024-01-01T00:00:17.000000Z
                            15	S0	2024-01-01T00:00:14.000000Z
                            12	S0	2024-01-01T00:00:11.000000Z
                            9	S0	2024-01-01T00:00:08.000000Z
                            6	S0	2024-01-01T00:00:05.000000Z
                            3	S0	2024-01-01T00:00:02.000000Z
                            """);
            // the rows outgrow the budget of three of the six row groups, so the read gathers them in two windows, each
            // a pass that decodes the row groups that hold its rows
            assertKeyOrderedDecodes(query, null, ParquetDecodeHint.MONOTONIC, 12);
            assertKeyOrderedDecodes(query, ParquetDecodeHint.SCATTERED, ParquetDecodeHint.MONOTONIC, 12);
        });
    }

    @Test
    public void testSortedSymbolIndexSmallBackwardSkipCostIsBounded() throws Exception {
        assertMemoryLeak(() -> {
            for (int rowCount : new int[]{20_000, 200_000}) {
                final String table = "t" + rowCount;
                execute("CREATE TABLE " + table + " AS (SELECT x id, (CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END)::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(" + rowCount + ")), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
                final String query = "SELECT id, sym FROM " + table + " WHERE ts > " + rowCount / 2 + " AND ts <= " + rowCount + " ORDER BY sym, ts DESC LIMIT 1,2";
                assertQuery(query)
                        .noLeakCheck()
                        .withPlanContaining("SortedSymbolIndex")
                        .returns("id\tsym\n" + (rowCount - 2) + "\ta\n");
                assertSkipCost(query, 0, 0);
            }
        });
    }

    @Test
    public void testSortedSymbolIndexSmallForwardSkipCostIsBounded() throws Exception {
        assertMemoryLeak(() -> {
            for (int rowCount : new int[]{20_000, 200_000}) {
                final String table = "t" + rowCount;
                execute("CREATE TABLE " + table + " AS (SELECT x id, (CASE WHEN x % 2 = 0 THEN 'a' ELSE 'b' END)::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(" + rowCount + ")), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
                final String query = "SELECT id, sym FROM " + table + " WHERE ts >= 0 AND ts < " + (rowCount / 2 + 1) + " ORDER BY sym, ts LIMIT 1,2";
                assertQuery(query)
                        .noLeakCheck()
                        .withPlanContaining("SortedSymbolIndex")
                        .returns("""
                                id\tsym
                                4\ta
                                """);
                assertSkipCost(query, 0, 0);
            }
        });
    }

    @Test
    public void testSortedSymbolIndexSkipCostFollowsLanding() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 'a'::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(100_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(100, 100);
            String query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("SortedSymbolIndex")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000001Z	a
                            """);
            assertSkipCost(query, 0, 0);
            query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 5, 7";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000006Z	a
                            1970-01-01T00:00:00.000007Z	a
                            """);
            assertSkipCost(query, 0, 0);
            query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 60_050, 60_052";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.060051Z	a
                            1970-01-01T00:00:00.060052Z	a
                            """);
            assertSkipCost(query, 0, 0);
            query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts DESC LIMIT 60_050, 60_052";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.039950Z	a
                            1970-01-01T00:00:00.039949Z	a
                            """);
            assertSkipCost(query, 0, 0);

            execute("CREATE TABLE u AS (SELECT ('a' || (x % 2))::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(100_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            final long tenPostingListTraversals = 10 * 50_000 / configuration.getIndexValueBlockSize();
            query = "SELECT ts, sym FROM u WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 30_060, 30_062";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("SortedSymbolIndex")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.060121Z	a1
                            1970-01-01T00:00:00.060123Z	a1
                            """);
            assertSkipCost(query, tenPostingListTraversals, 0);
            query = "SELECT ts, sym FROM u WHERE ts >= 0 AND ts < 100_001 ORDER BY sym DESC, ts DESC LIMIT 30_060, 30_062";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.039879Z	a1
                            1970-01-01T00:00:00.039877Z	a1
                            """);
            assertSkipCost(query, tenPostingListTraversals, 0);
        });
    }

    @Test
    public void testSortedSymbolIndexSkipCountsKeysOfMergedPass() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE m AS (SELECT ('a' || (x % 3))::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(3_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            // one page frame holds the scan, so a single pass reads every key after the first
            sqlExecutionContext.changePageFrameSizes(1_000_000, 1_000_000);
            String query = "SELECT ts, sym FROM m WHERE ts >= 0 AND ts < 3_001 ORDER BY sym, ts LIMIT 2_500, 2_501";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("symbolOrder: asc")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.001502Z	a2
                            """);
            assertSkipCost(query, 64, 0);
            query = "SELECT ts, sym FROM m WHERE ts >= 0 AND ts < 3_001 ORDER BY sym DESC, ts DESC LIMIT 1_250, 1_251";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.002248Z	a1
                            """);
            assertSkipCost(query, 64, 0);
        });
    }

    @Test
    public void testSortedSymbolIndexSkipCountsOnlyTheInterval() throws Exception {
        assertMemoryLeak(() -> {
            // the interval holds the first 2_000 of 99_999 postings of 'a', and 'b' has its only row after it
            execute("CREATE TABLE p AS (SELECT (CASE WHEN x = 100_000 THEN 'b' ELSE 'a' END)::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(100_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            final long intervalHops = 8 * (2_000 / configuration.getIndexValueBlockSize() + 1);
            // the skip ends where the first page frame does
            String query = "SELECT ts, sym FROM p WHERE ts >= 0 AND ts < 2_001 ORDER BY sym DESC, ts LIMIT 1_000, 1_001";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("symbolOrder: desc")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.001001Z	a
                            """);
            assertSkipCost(query, intervalHops, 0);
            query = "SELECT ts, sym FROM p WHERE ts >= 0 AND ts < 2_001 ORDER BY sym DESC, ts DESC LIMIT 1_000, 1_001";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("Index backward scan")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.001000Z	a
                            """);
            assertSkipCost(query, intervalHops, 0);
            query = "SELECT ts, sym FROM p WHERE ts >= 0 AND ts < 2_001 ORDER BY sym, ts LIMIT 1_000, 1_001";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.001001Z	a
                            """);
            assertSkipCost(query, intervalHops, 0);
            // the skip ends inside the second page frame
            query = "SELECT ts, sym FROM p WHERE ts >= 0 AND ts < 2_001 ORDER BY sym DESC, ts LIMIT 1_500, 1_502";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.001501Z	a
                            1970-01-01T00:00:00.001502Z	a
                            """);
            assertSkipCost(query, intervalHops, 0);
            query = "SELECT ts, sym FROM p WHERE ts >= 0 AND ts < 2_001 ORDER BY sym, ts DESC LIMIT 1_500, 1_502";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000500Z	a
                            1970-01-01T00:00:00.000499Z	a
                            """);
            assertSkipCost(query, intervalHops, 0);
            query = "SELECT ts, sym FROM p WHERE ts >= 0 AND ts < 2_001 ORDER BY sym DESC, ts LIMIT -3";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.001998Z	a
                            1970-01-01T00:00:00.001999Z	a
                            1970-01-01T00:00:00.002000Z	a
                            """);
            assertSkipCost(query, intervalHops, 0);
            query = "SELECT ts, sym FROM p WHERE ts >= 0 AND ts < 2_001 ORDER BY sym DESC, ts DESC LIMIT -1_001, -999";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.001001Z	a
                            1970-01-01T00:00:00.001000Z	a
                            """);
            assertSkipCost(query, intervalHops, 0);
        });
    }

    @Test
    public void testSortedSymbolIndexSkipsCountedPageFrames() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t AS (SELECT 'a'::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(1_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE m AS (SELECT ('a' || (x % 3))::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(1_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE c AS (SELECT x::TIMESTAMP ts FROM long_sequence(500)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE c ADD COLUMN sym SYMBOL INDEX");
            execute("INSERT INTO c SELECT (500 + x)::TIMESTAMP, 'b' FROM long_sequence(500)");
            sqlExecutionContext.changePageFrameSizes(100, 100);
            String query = "SELECT ts, sym FROM t WHERE ts >= 0 AND ts < 10_000_001 ORDER BY sym, ts LIMIT -3";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000998Z	a
                            1970-01-01T00:00:00.000999Z	a
                            1970-01-01T00:00:00.001000Z	a
                            """);
            assertWalksFewRows(query);
            query = "SELECT ts, sym FROM m WHERE ts >= 0 AND ts < 10_000_001 ORDER BY sym, ts LIMIT -3";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000992Z	a2
                            1970-01-01T00:00:00.000995Z	a2
                            1970-01-01T00:00:00.000998Z	a2
                            """);
            assertWalksFewRows(query);
            query = "SELECT ts, sym FROM m WHERE ts >= 0 AND ts < 10_000_001 ORDER BY sym DESC, ts DESC LIMIT -3";
            assertQuery(query)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000009Z	a0
                            1970-01-01T00:00:00.000006Z	a0
                            1970-01-01T00:00:00.000003Z	a0
                            """);
            assertWalksFewRows(query);
            query = "SELECT ts, sym FROM c WHERE ts >= 0 AND ts < 10_000_001 ORDER BY sym, ts LIMIT 498, 503";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.000499Z\t
                            1970-01-01T00:00:00.000500Z\t
                            1970-01-01T00:00:00.000501Z	b
                            1970-01-01T00:00:00.000502Z	b
                            1970-01-01T00:00:00.000503Z	b
                            """);
            assertWalksFewRows(query);
        });
    }

    @Test
    public void testSymbolSearchOrderBy() throws Exception {
        final String expected = """
                sym\tprice\tts
                ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z
                ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z
                ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z
                ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z
                ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z
                ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z
                ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z
                ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z
                ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z
                ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z
                ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z
                ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z
                ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z
                ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z
                ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z
                ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z
                ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z
                ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z
                ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z
                ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z
                ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z
                ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z
                ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z
                ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z
                ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z
                ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z
                ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z
                ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z
                ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z
                ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z
                ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z
                ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z
                ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z
                ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z
                ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z
                ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z
                ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                """;

        assertQuery("x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by sym")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("sym\tprice\tts\n", expected);
    }

    @Test
    public void testSymbolSearchOrderByAlias() throws Exception {
        final String expected = """
                k\tprice\tts
                ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z
                ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z
                ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z
                ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z
                ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z
                ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z
                ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z
                ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z
                ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z
                ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z
                ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z
                ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z
                ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z
                ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z
                ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z
                ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z
                ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z
                ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z
                ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z
                ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z
                ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z
                ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z
                ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z
                ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z
                ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z
                ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z
                ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z
                ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z
                ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z
                ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z
                ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z
                ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z
                ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z
                ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z
                ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z
                ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z
                ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                """;

        assertQuery("select sym k, price, ts from x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("k\tprice\tts\n", expected);
    }

    @Test
    public void testSymbolSearchOrderByAliasAndTimestamp() throws Exception {
        final String expected = """
                k\tprice\tts
                ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z
                ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z
                ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z
                ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z
                ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z
                ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z
                ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z
                ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z
                ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z
                ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z
                ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z
                ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z
                ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z
                ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z
                ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z
                ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z
                ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z
                ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z
                ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z
                ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z
                ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z
                ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z
                ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z
                ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z
                ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z
                ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z
                ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z
                ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z
                ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z
                ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z
                ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z
                ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z
                ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z
                ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z
                ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z
                ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z
                ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                """;

        assertQuery("select sym k, price, ts from x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("k\tprice\tts\n", expected);
    }

    @Test
    public void testSymbolSearchOrderByAliasAndTimestampDesc() throws Exception {
        final String expected = """
                k\tprice\tts
                ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z
                ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z
                ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z
                ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z
                ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z
                ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z
                ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z
                ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z
                ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z
                ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z
                ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z
                ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z
                ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z
                ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z
                ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z
                ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z
                ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z
                ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z
                ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z
                ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z
                ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z
                ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z
                ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z
                ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z
                ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z
                ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z
                ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z
                ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z
                ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z
                ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z
                ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z
                ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z
                ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z
                ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z
                ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z
                ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z
                ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                """;

        assertQuery("select sym k, price, ts from x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts desc")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("k\tprice\tts\n", expected);
    }

    @Test
    public void testSymbolSearchOrderByAliasAndTimestampDescEmpty() throws Exception {
        TestMatchFunctionFactory.clear();
        assertMemoryLeak(() -> {
            assertQuery("select sym k, price, ts from x where sym in ('HBC', 'ABB') and 1 = 3 and test_match() and ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts desc")
                    .noLeakCheck()
                    .ddl("""
                            create table x (
                                sym symbol index,
                                price double,
                                ts timestamp
                            ) timestamp(ts) partition by DAY""")
                    .expectSize()
                    .returns("k\tprice\tts\n");
            Assert.assertTrue(TestMatchFunctionFactory.isClosed());
        });
    }

    @Test
    public void testSymbolSearchOrderByIndex() throws Exception {
        final String expected = """
                sym\tprice\tts
                ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z
                ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z
                ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z
                ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z
                ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z
                ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z
                ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z
                ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z
                ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z
                ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z
                ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z
                ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z
                ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z
                ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z
                ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z
                ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z
                ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z
                ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z
                ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z
                ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z
                ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z
                ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z
                ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z
                ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z
                ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z
                ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z
                ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z
                ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z
                ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z
                ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z
                ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z
                ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z
                ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z
                ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z
                ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z
                ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z
                ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                """;

        assertQuery("x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by 1")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("sym\tprice\tts\n", expected);
    }

    @Test
    public void testSymbolSearchOrderByIndexDesc() throws Exception {
        final String expected = """
                sym\tprice\tts
                HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z
                HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z
                HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z
                HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z
                HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z
                HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z
                HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z
                HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z
                HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z
                HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z
                HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z
                HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z
                HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z
                HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z
                HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z
                HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z
                HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z
                HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z
                HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z
                HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z
                HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z
                HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z
                HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z
                HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z
                HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z
                HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z
                HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z
                HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z
                HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z
                ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z
                ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z
                ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z
                ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z
                ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z
                ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z
                ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z
                ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z
                ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z
                ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z
                ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z
                ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z
                ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z
                ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z
                ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z
                ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z
                ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z
                ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z
                ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z
                ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z
                ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z
                ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z
                ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z
                ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z
                ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z
                ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z
                ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z
                ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z
                ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z
                ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z
                ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z
                ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z
                ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z
                ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z
                ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z
                ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z
                ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z
                """;

        assertQuery("x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by 1 desc")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(1000)) timestamp (ts)""")
                .returns("sym\tprice\tts\n", expected);
    }

    @Test
    public void testSymbolSearchOrderByKeyAcrossIntervals() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTable("k", "DAY");
            assertQuery("SELECT s, id FROM k WHERE s = 'S1' AND (ts IN '2024-01-01T00:00:01;4s' OR ts IN '2024-01-01T00:00:13;4s') ORDER BY s, ts DESC")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                DeferredSingleSymbolFilterPageFrame
                                    Index backward scan on: s deferred: true
                                      filter: s='S1'
                                    Interval backward scan on: k
                                      intervals: [("2024-01-01T00:00:01.000000Z","2024-01-01T00:00:04.999999Z"),("2024-01-01T00:00:13.000000Z","2024-01-01T00:00:16.999999Z")]
                            """)
                    .returns("""
                            s	id
                            S1	16
                            S1	4
                            """);
            assertQuery("SELECT s, id FROM k WHERE s IN ('S0', 'S1') AND (ts IN '2024-01-01T00:00:01;4s' OR ts IN '2024-01-01T00:00:13;4s') ORDER BY s")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                SelectedRecord
                                    FilterOnValues symbolOrder: asc
                                        Cursor-order scan
                                            Index forward scan on: s deferred: true
                                              filter: s='S0'
                                            Index forward scan on: s deferred: true
                                              filter: s='S1'
                                        Interval forward scan on: k
                                          intervals: [("2024-01-01T00:00:01.000000Z","2024-01-01T00:00:04.999999Z"),("2024-01-01T00:00:13.000000Z","2024-01-01T00:00:16.999999Z")]
                            """)
                    .returns("""
                            s	id
                            S0	3
                            S0	15
                            S1	4
                            S1	16
                            """);
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyAcrossPageFrames() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTable("n", "NONE");
            sqlExecutionContext.changePageFrameSizes(1, 4);
            assertQuery("SELECT s, id FROM n WHERE s IN ('S0', 'S1') ORDER BY s")
                    .noLeakCheck()
                    .withPlan("""
                            FilterOnValues symbolOrder: asc
                                Cursor-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s='S0'
                                    Index forward scan on: s deferred: true
                                      filter: s='S1'
                                Frame forward scan on: n
                            """)
                    .returns("""
                            s	id
                            S0	3
                            S0	6
                            S0	9
                            S0	12
                            S0	15
                            S0	18
                            S0	21
                            S0	24
                            S1	1
                            S1	4
                            S1	7
                            S1	10
                            S1	13
                            S1	16
                            S1	19
                            S1	22
                            """);
            assertQuery("SELECT s, id FROM n WHERE s IN ('S0', 'S1') ORDER BY s DESC LIMIT 10")
                    .noLeakCheck()
                    .returns("""
                            s	id
                            S1	1
                            S1	4
                            S1	7
                            S1	10
                            S1	13
                            S1	16
                            S1	19
                            S1	22
                            S0	3
                            S0	6
                            """);
            assertQuery("SELECT s, id FROM n WHERE s IN ('S0', 'S1') ORDER BY s, ts DESC")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                FilterOnValues symbolOrder: asc
                                    Cursor-order scan
                                        Index backward scan on: s deferred: true
                                          filter: s='S0'
                                        Index backward scan on: s deferred: true
                                          filter: s='S1'
                                    Frame backward scan on: n
                            """)
                    .returns("""
                            s	id
                            S0	24
                            S0	21
                            S0	18
                            S0	15
                            S0	12
                            S0	9
                            S0	6
                            S0	3
                            S1	22
                            S1	19
                            S1	16
                            S1	13
                            S1	10
                            S1	7
                            S1	4
                            S1	1
                            """);
            assertQuery("SELECT s, id FROM n WHERE s NOT IN ('S2') ORDER BY s LIMIT 3")
                    .noLeakCheck()
                    .returns("""
                            s	id
                            S0	3
                            S0	6
                            S0	9
                            """);
            assertQuery("SELECT s, id FROM n WHERE s = 'S1' ORDER BY s, ts DESC")
                    .noLeakCheck()
                    .withPlan("""
                            SelectedRecord
                                DeferredSingleSymbolFilterPageFrame
                                    Index backward scan on: s deferred: true
                                      filter: s='S1'
                                    Frame backward scan on: n
                            """)
                    .returns("""
                            s	id
                            S1	22
                            S1	19
                            S1	16
                            S1	13
                            S1	10
                            S1	7
                            S1	4
                            S1	1
                            """);
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersWindowsWhenCountedRowsExceedQueryMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 64);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 4096);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE g AS (
                        SELECT x id, (CASE WHEN x % 13 = 0 THEN NULL ELSE 'k' || (x % 7) END)::SYMBOL s, x * 0.5 v,
                            ('value-' || x)::VARCHAR vc, timestamp_sequence('2024-01-01', 5_000_000) ts
                        FROM long_sequence(34_560)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("ALTER TABLE g CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            // all rows, or the share of the rows of the first row group that NOT IN and its filter keep, take more than
            // half the memory limit, so those reads gather windows; the narrower key lists gather in one window
            assertKeyOrderedReadsMatchSort(
                    "g",
                    new String[]{"ts IN '2024-01-01'"},
                    new String[]{"", "s IN ('k1', 'k3', NULL)", "s NOT IN ('k2') AND v > 0", "s IN ('k1', 'k5') AND id % 3 = 0"},
                    768 * 1024L
            );
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersWindowsWhenRowsOutgrowTheDecodeCache() throws Exception {
        // 300 row groups of a day, more than the decode cache keeps, so the read gathers whatever the budget
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE m AS (
                        SELECT ('k' || (x % 4))::SYMBOL s, rpad('v' || x, 2_000, 'x')::VARCHAR vc, timestamp_sequence('2024-01-01', 60_000_000) ts
                        FROM long_sequence(1_200)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO m VALUES ('k1', 'v', '2024-01-02')");
            execute("ALTER TABLE m CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final String where = " FROM m WHERE ts IN '2024-01-01' AND s IN ('k1', 'k2', 'k3') ORDER BY s, ts";
            final String tail = "s, left(vc, 6) vc, length(vc) len, ts" + where;
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(m) */ " + tail, expected);
            final String expectedRows = expected.toString();
            final StringSink actual = new StringSink();
            // the 900 rows take 1.8 MB, within the budget of a 4 MB decode cache, which the read gathers them in,
            // decoding each row group once, and over that of a 1 MB one, where the read gathers them in two windows
            // and decodes each row group once per window
            final long[] decodeCacheBytes = {4 * 1024 * 1024L, 1024 * 1024L};
            final ParquetDecodeHint[] hints = {ParquetDecodeHint.MONOTONIC, ParquetDecodeHint.MONOTONIC};
            final long[] decodes = {300, 600};
            final long[] usedBytes = new long[decodeCacheBytes.length];
            for (int i = 0; i < decodeCacheBytes.length; i++) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, decodeCacheBytes[i]);
                assertKeyOrderedDecodes("SELECT *" + where, null, hints[i], decodes[i]);
                try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    usedBytes[i] = sqlExecutionContext.getMemoryTracker().getUsed();
                }
            }
            Assert.assertTrue("a window holds part of the VARCHAR values", usedBytes[1] + 500_000 < usedBytes[0]);
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersWindowsWhenNonVarcharValuesOutgrowQueryMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 1024);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE b AS (
                        SELECT ('k' || (x % 4))::SYMBOL s, rpad('v' || x, 400, 'x')::STRING str, rnd_bin(400, 400, 0) bin,
                            rnd_double_array(1, 0, 50) arr, timestamp_sequence('2024-01-01', 60_000_000) ts
                        FROM long_sequence(400)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO b (s, ts) VALUES ('k1', '2024-01-02')");
            execute("ALTER TABLE b CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final String tail = "s, left(str, 6) str, length(str) sl, length(bin) bl, dim_length(arr, 1) al, ts FROM b"
                    + " WHERE ts IN '2024-01-01' AND s IN ('k1', 'k2', 'k3') ORDER BY s, ts";
            final StringSink expected = new StringSink();
            // the STRING, BINARY and DOUBLE[] values of the 300 rows outgrow the memory the query may take
            for (String limit : new String[]{"", " LIMIT -7"}) {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                printSql("SELECT /*+ no_index(b) */ " + tail + limit, expected);
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
                assertQuery("SELECT " + tail + limit)
                        .noLeakCheck()
                        .sizeMayVary()
                        .withPlanContaining("FilterOnValues symbolOrder: asc")
                        .returns(expected);
            }
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersWindowsWhenRowsOutgrowQueryMemoryLimit() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 1024);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE m AS (
                        SELECT ('k' || (x % 4))::SYMBOL s, rpad('v' || x, 2_000, 'x')::VARCHAR vc, timestamp_sequence('2024-01-01', 60_000_000) ts
                        FROM long_sequence(400)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO m VALUES ('k1', 'v', '2024-01-02')");
            execute("ALTER TABLE m CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final String tail = "s, left(vc, 6) vc, length(vc) len, ts FROM m WHERE ts IN '2024-01-01' AND s IN ('k1', 'k2', 'k3') ORDER BY s, ts";
            final StringSink expected = new StringSink();
            // the 300 rows of the read hold 600 KB of VARCHAR, which outgrows the memory the query may take, so the read
            // gathers them in windows
            for (String limit : new String[]{"", " LIMIT 5", " LIMIT -7", " LIMIT 3, 40", " LIMIT -250, -4"}) {
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                printSql("SELECT /*+ no_index(m) */ " + tail + limit, expected);
                node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
                assertQuery("SELECT " + tail + limit)
                        .noLeakCheck()
                        .sizeMayVary()
                        .withPlanContaining("FilterOnValues symbolOrder: asc")
                        .returns(expected);
            }
            // a cached factory reads the windows again with and without the limit
            node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
            printSql("SELECT /*+ no_index(m) */ " + tail, expected);
            final String expectedRows = expected.toString();
            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select("SELECT " + tail)) {
                for (long memoryLimit : new long[]{0, 512 * 1024L, 0, 512 * 1024L}) {
                    node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, memoryLimit);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersWindowsBesideCompositePartition() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 512);
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_MERGE_APPEND_ENABLED, "true");
        node1.setProperty(PropertyKey.CAIRO_O3_PARTITION_SPLIT_MIN_SIZE, 128);
        node1.setProperty(PropertyKey.CAIRO_O3_MID_PARTITION_MAX_SPLITS, 1000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_PIECE_THRESHOLD, 100_000);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_COMPACTION_AVG_ROWS_PIECE_LIM, 1);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE g (id LONG, s SYMBOL INDEX CAPACITY 8, v DOUBLE, vc VARCHAR, ts TIMESTAMP)
                    TIMESTAMP(ts) PARTITION BY DAY WAL""");
            execute("""
                    INSERT INTO g
                    SELECT x, ('k' || (x % 3))::SYMBOL, x * 0.5, ('value-' || x)::VARCHAR, timestamp_sequence('2024-01-01', 1_800_000_000L)
                    FROM long_sequence(144)""");
            drainWalQueue();
            execute("""
                    INSERT INTO g VALUES
                        (1001, 'k2', 1.5, 'late-1', '2024-01-02T03:31:00.000000Z'),
                        (1002, 'k1', 2.5, 'late-2', '2024-01-02T08:31:00.000000Z')""");
            drainWalQueue();
            execute("INSERT INTO g VALUES (1003, 'k0', 3.5, 'late-3', '2024-01-02T15:31:00.000000Z')");
            drainWalQueue();
            execute("ALTER TABLE g CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            drainWalQueue();
            try (TableReader reader = engine.getReader(engine.verifyTableName("g"))) {
                Assert.assertTrue(reader.getTxFile().isPartitionParquet(0));
                Assert.assertTrue(reader.getTxFile().isPartitionComposite(1));
            }
            // the Parquet day gathers windows, the composite day walks its pieces key by key
            assertKeyOrderedReadsMatchSort(
                    "g",
                    new String[]{"ts IN '2024-01-01'", "ts IN '2024-01-02'"},
                    new String[]{"", "s IN ('k1', 'k2')", "s NOT IN ('k0')"}
            );
            assertKeyOrderedDecodes("SELECT * FROM g WHERE ts IN '2024-01-01' ORDER BY s, ts", null, ParquetDecodeHint.MONOTONIC, 36);
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyDenseGatherMatchesSort() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE g AS (
                        SELECT x id, (CASE WHEN x % 17 = 0 THEN NULL ELSE 'k' || (x % 256) END)::SYMBOL s,
                            x * 0.5 v, ('value-' || x)::VARCHAR vc, timestamp_sequence('2024-01-01', 1_000_000) ts
                        FROM long_sequence(2_080)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("INSERT INTO g VALUES (2_081, 'k1', 0.5, 'native', '2024-01-02'), (2_082, NULL, 1.0, NULL, '2024-01-02T00:00:01')");
            execute("ALTER TABLE g CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            final StringSink values = new StringSink();
            values.put("s IN ('k0'");
            final StringSink binds = new StringSink();
            binds.put("s IN ($1");
            bindVariableService.setStr(0, "k0");
            for (int i = 1; i < 128; i++) {
                values.put(", 'k").put(i).put("'");
                binds.put(", $").put(i + 1);
                bindVariableService.setStr(i, "k" + i);
            }
            binds.put(", $129, $130)");
            bindVariableService.setStr(128, "absent");
            bindVariableService.setStr(129, null);
            values.put(", NULL)");
            for (long cacheBytes : new long[]{64 * 1024 * 1024L, 1024}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, cacheBytes);
                for (String predicate : new String[]{"", values.toString(), values + " AND id % 3 = 0", binds.toString(), binds + " AND id % 3 = 0"}) {
                    final String tail = (cacheBytes == 1024 || !predicate.isEmpty() ? "id, s, ts" : "*")
                            + " FROM g WHERE ts IN '2024-01-01'" + (predicate.isEmpty() ? "" : " AND " + predicate) + " ORDER BY s, ts";
                    final StringSink expected = new StringSink();
                    printSql("SELECT /*+ no_index(g) */ " + tail, expected);
                    assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                    PageFrameRecordCursorImpl.testDenseScanFrames.set(0);
                    PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
                    try (RecordCursorFactory factory = select("SELECT " + tail); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        // some windows of the small decode cache hold too few keys for the scan to pay
                        if (cacheBytes > 1024) {
                            Assert.assertTrue("the gather scans page frames densely", PageFrameRecordCursorImpl.testDenseScanFrames.get() > 0);
                        }
                        Assert.assertEquals(predicate.isEmpty(), ((PageFrameRecordCursorImpl) cursor).getRowCursorFactory() instanceof SortedSymbolIndexRowCursorFactory);
                        Assert.assertEquals(ParquetDecodeHint.MONOTONIC, ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeHint());
                    } finally {
                        PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
                    }
                }
                assertKeyOrderedReadsMatchSort(
                        "g",
                        new String[]{"ts IN '2024-01-01'", "ts IN '2024-01-01T00:00:03;10m;20m;2'", ""},
                        new String[]{"", values.toString(), values + " AND id % 3 = 0", binds.toString(), binds + " AND id % 3 = 0"}
                );
                for (String order : new String[]{"s, ts", "s DESC, ts DESC", "s, ts DESC", "s DESC, ts"}) {
                    for (String limit : new String[]{" LIMIT 3, 400", " LIMIT -400, -3"}) {
                        final String tail = "ts, vc, s, id FROM g WHERE ts IN '2024-01-01' ORDER BY " + order + limit;
                        final StringSink expected = new StringSink();
                        printSql("SELECT /*+ no_index(g) */ " + tail, expected);
                        assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                    }
                }
            }
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersRowsOfManyRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            // more row groups than the decode cache keeps, whatever its budget
            execute("""
                    CREATE TABLE g AS (
                        SELECT x id, ('k' || (x % 5))::SYMBOL s, x * 0.5 v, ('value-' || x)::VARCHAR vc, timestamp_sequence('2024-01-01', 60_000_000) ts
                        FROM long_sequence(1_040)
                    ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY""");
            execute("INSERT INTO g VALUES (1_041, 'k1', 0.5, 'last', '2024-01-02')");
            execute("ALTER TABLE g CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            assertKeyOrderedDecodes("SELECT * FROM g WHERE ts IN '2024-01-01' ORDER BY s", null, ParquetDecodeHint.MONOTONIC, 260);
            assertKeyOrderedDecodes("SELECT * FROM g WHERE ts IN '2024-01-01' AND s IN ('k1', 'k3') ORDER BY s DESC, ts DESC", null, ParquetDecodeHint.MONOTONIC, 260);
            assertKeyOrderedReadsMatchSort("g", new String[]{"ts IN '2024-01-01'"}, new String[]{"", "s IN ('k1', 'k3')"});
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersWindowsOfPartitionConvertedAfterCompile() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 512);
        assertMemoryLeak(() -> {
            createGatherTable();
            final String query = "SELECT * FROM g WHERE ts IN '2024-01-02' AND s IN ('k1', 'k3', NULL) ORDER BY s, ts DESC";
            final StringSink expected = new StringSink();
            printSql("SELECT /*+ no_index(g) */ * FROM g WHERE ts IN '2024-01-02' AND s IN ('k1', 'k3', NULL) ORDER BY s, ts DESC", expected);
            final String expectedRows = expected.toString();
            final StringSink actual = new StringSink();
            try (RecordCursorFactory factory = select(query)) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                }
                execute("ALTER TABLE g CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-04'");
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    TestUtils.assertCursor(expectedRows, cursor, factory.getMetadata(), true, actual);
                }
                try (RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                        // read every row
                    }
                    final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
                    Assert.assertEquals(28, pool.getDecodeCount());
                    Assert.assertEquals(ParquetDecodeHint.MONOTONIC, pool.getDecodeHint());
                }
            }
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyGathersWindowsOverDecodeCache() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 512);
        assertMemoryLeak(() -> {
            createGatherTable();
            // two days of twelve row groups each, then native days
            execute("ALTER TABLE g CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-03'");
            // the rows outgrow the budget of the decode cache, so the read gathers them in windows, each a pass that
            // decodes the row groups that hold its rows
            assertKeyOrderedDecodes("SELECT * FROM g WHERE ts IN '2024-01-02' ORDER BY s", null, ParquetDecodeHint.MONOTONIC, 48);
            assertKeyOrderedDecodes("SELECT * FROM g WHERE ts IN '2024-01-02' AND s IN ('k1', 'k3', NULL) ORDER BY s DESC", null, ParquetDecodeHint.MONOTONIC, 28);
            assertKeyOrderedDecodes("SELECT * FROM g WHERE ts IN '2024-01-01' ORDER BY s, ts DESC", ParquetDecodeHint.SCATTERED, ParquetDecodeHint.MONOTONIC, 48);
            assertKeyOrderedDecodes("SELECT * FROM g WHERE ts IN '2024-01-01' AND s IN ('k1', 'k5') AND id % 3 = 0 ORDER BY s", null, ParquetDecodeHint.MONOTONIC, 12);
            // a Parquet day gathers windows, a native day walks them key by key
            assertKeyOrderedReadsMatchSort(
                    "g",
                    new String[]{"ts IN '2024-01-02'", "ts IN '2024-01-03'"},
                    new String[]{"", "s IN ('k1', 'k3', NULL)", "s NOT IN ('k2')", "s IN ('k1', 'k5') AND id % 3 = 0"}
            );
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyOverParquetCountsRowsByIndex() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetKeyTable();
            final String query = "SELECT * FROM k WHERE s IN ('S0', 'S2') AND ts IN '2024-01-01' ORDER BY s";
            // the walk keeps the six row groups decoded, a budget of two row groups gathers the rows instead
            for (long budget : new long[]{256 * 1024 * 1024, 2 * decodedRowGroupBytes(query)}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, budget);
                try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(16, counter.get());
                    Assert.assertEquals(0, ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool().getDecodeCount());
                    cursor.toTop();
                    counter.set(14);
                    cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
                    Assert.assertEquals(0, counter.get());
                }
                assertQuery(query + " LIMIT -3")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id	s	ts
                                17	S2	2024-01-01T00:00:16.000000Z
                                20	S2	2024-01-01T00:00:19.000000Z
                                23	S2	2024-01-01T00:00:22.000000Z
                                """);
            }
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyOverParquetDecodesEachRowGroupOnce() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetKeyTable();
            final String expected = """
                    id	s	ts
                    3	S0	2024-01-01T00:00:02.000000Z
                    6	S0	2024-01-01T00:00:05.000000Z
                    9	S0	2024-01-01T00:00:08.000000Z
                    12	S0	2024-01-01T00:00:11.000000Z
                    15	S0	2024-01-01T00:00:14.000000Z
                    18	S0	2024-01-01T00:00:17.000000Z
                    21	S0	2024-01-01T00:00:20.000000Z
                    24	S0	2024-01-01T00:00:23.000000Z
                    2	S2	2024-01-01T00:00:01.000000Z
                    5	S2	2024-01-01T00:00:04.000000Z
                    8	S2	2024-01-01T00:00:07.000000Z
                    11	S2	2024-01-01T00:00:10.000000Z
                    14	S2	2024-01-01T00:00:13.000000Z
                    17	S2	2024-01-01T00:00:16.000000Z
                    20	S2	2024-01-01T00:00:19.000000Z
                    23	S2	2024-01-01T00:00:22.000000Z
                    """;
            String query = "SELECT * FROM k WHERE s IN ('S0', 'S2') AND ts IN '2024-01-01' ORDER BY s";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlan("""
                            FilterOnValues symbolOrder: asc
                                Cursor-order scan
                                    Index forward scan on: s deferred: true
                                      filter: s='S0'
                                    Index forward scan on: s deferred: true
                                      filter: s='S2'
                                Interval forward scan on: k
                                  intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns(expected);
            assertKeyOrderedDecodes(query, null, ParquetDecodeHint.CYCLIC, 6);

            query = "SELECT * FROM k WHERE s NOT IN ('S1') AND ts IN '2024-01-01' ORDER BY s";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlan("""
                            FilterOnExcludedValues symbolOrder: asc
                              symbolFilter: s not in ['S1']
                                Cursor-order scan
                                    Index forward scan on: s
                                      filter: s=3
                                    Index forward scan on: s
                                      filter: s=2
                                Interval forward scan on: k
                                  intervals: [("2024-01-01T00:00:00.000000Z","2024-01-01T23:59:59.999999Z")]
                            """)
                    .returns(expected);
            assertKeyOrderedDecodes(query, null, ParquetDecodeHint.CYCLIC, 6);

            // a single distinct key reads the row groups in one pass
            bindVariableService.clear();
            bindVariableService.setStr(0, "S0");
            bindVariableService.setStr(1, "S0");
            assertKeyOrderedDecodes("SELECT * FROM k WHERE s IN ($1, $2) AND ts IN '2024-01-01' ORDER BY s", null, ParquetDecodeHint.MONOTONIC, 6);
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyOverParquetLandsSkipInsideRowGroup() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetKeyTable();
            // the size of a negative LIMIT reads every row group before the skip lands inside the last one it reads
            for (long budget : new long[]{256 * 1024 * 1024, 160, 240, 320, 400}) {
                node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, budget);
                assertQuery("SELECT * FROM k WHERE s IN ('S0', 'S1') AND ts IN '2024-01-01' ORDER BY s DESC LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id	s	ts
                                24	S0	2024-01-01T00:00:23.000000Z
                                """);
                assertQuery("SELECT * FROM k WHERE s IN ('S0', 'S1') AND ts IN '2024-01-01' ORDER BY s, ts DESC LIMIT -1")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                id	s	ts
                                1	S1	2024-01-01T00:00:00.000000Z
                                """);
            }
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyOverParquetPositionsRecordAfterSkip() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            createParquetKeyTable();
            final String keyListQuery = "SELECT * FROM k WHERE s IN ('S0', 'S1') AND ts IN '2024-01-01' ORDER BY s DESC";
            final String sortedIndexQuery = "SELECT * FROM k WHERE ts IN '2024-01-01' ORDER BY s DESC";
            // five buffers cycle through the six row groups, so each pass decodes a row group into another buffer
            node1.setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 5 * decodedRowGroupBytes(keyListQuery));
            assertLastRowAfterSkip(keyListQuery, 16, 24);
            assertLastRowAfterSkip(sortedIndexQuery, 24, 24);
        });
    }

    @Test
    public void testSymbolSearchOrderByKeyWithoutRowsReadsOnePageFrame() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE r AS (SELECT (CASE WHEN x > 50_000 THEN 'b' ELSE 'a' END)::SYMBOL sym, x::TIMESTAMP ts FROM long_sequence(100_000)), INDEX(sym) TIMESTAMP(ts) PARTITION BY DAY");
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            // 'a' leads the key order with rows only before the interval
            String query = "SELECT ts, sym FROM r WHERE sym IN ('a', 'b') AND ts >= 50_001 AND ts < 100_001 ORDER BY sym, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("FilterOnValues symbolOrder: asc")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	b
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM r WHERE sym IN ('a', 'b') AND ts >= 50_001 AND ts < 100_001 ORDER BY sym, ts DESC LIMIT 2";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.100000Z	b
                            1970-01-01T00:00:00.099999Z	b
                            """);
            assertReadFrames(query, 2);
            // 'c' is not a symbol of the table
            query = "SELECT ts, sym FROM r WHERE sym IN ('c', 'b') AND ts >= 50_001 AND ts < 100_001 ORDER BY sym DESC, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("FilterOnValues symbolOrder: desc")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	b
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM r WHERE sym NOT IN ('c') AND ts >= 50_001 AND ts < 100_001 ORDER BY sym, ts LIMIT 1";
            assertQuery(query)
                    .noLeakCheck()
                    .withPlanContaining("FilterOnExcludedValues symbolOrder: asc")
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.050001Z	b
                            """);
            assertReadFrames(query, 2);
            query = "SELECT ts, sym FROM r WHERE sym IN ('a', 'b') AND ts >= 50_001 AND ts < 100_001 ORDER BY sym, ts LIMIT 30_000, 30_001";
            assertQuery(query)
                    .noLeakCheck()
                    .returns("""
                            ts	sym
                            1970-01-01T00:00:00.080001Z	b
                            """);
            assertSkipCost(query, Long.MAX_VALUE, 0);
        });
    }

    @Test
    public void testTableOrderAggregatesReadIndexKeysInTableOrder() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedKeyTable();
            final String expected = """
                    f\tl\taa\ttw\tsp
                    1\t5\t[1.0,2.0,4.0,5.0]\t2.25\t▁▂▆█
                    """;
            assertQuery("SELECT first(v) f, last(v) l, array_agg(v::DOUBLE) aa, twap(v, ts) tw, sparkline(v) sp FROM t WHERE sym IN ('a', 'b')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT first(v) f, last(v) l, array_agg(v::DOUBLE) aa, twap(v, ts) tw, sparkline(v) sp FROM t WHERE sym::STRING IN ('a', 'b')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns(expected);
            assertQuery("SELECT twap(v, ts) tw FROM t WHERE sym IN ('a', 'b')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            tw
                            2.25
                            """);
            assertIndexOrder(ScanPlan.IndexOrder.TABLE, "SELECT twap(v, ts) FROM t WHERE sym IN ('a', 'b')");
            assertIndexOrder(ScanPlan.IndexOrder.TABLE, "SELECT sym, first(v) FROM t WHERE sym IN ('a', 'b') GROUP BY sym");
        });
    }

    @Test
    public void testTimestampLessThan() throws Exception {
        final String expected = """
                k\tprice\tts
                ABB\t0.8043224099968393\t1970-01-03T00:00:00.000000Z
                DXR\t0.08486964232560668\t1970-01-03T00:06:00.000000Z
                DXR\t0.0843832076262595\t1970-01-03T00:12:00.000000Z
                HBC\t0.6508594025855301\t1970-01-03T00:18:00.000000Z
                HBC\t0.7905675319675964\t1970-01-03T00:24:00.000000Z
                ABB\t0.22452340856088226\t1970-01-03T00:30:00.000000Z
                ABB\t0.3491070363730514\t1970-01-03T00:36:00.000000Z
                ABB\t0.7611029514995744\t1970-01-03T00:42:00.000000Z
                ABB\t0.4217768841969397\t1970-01-03T00:48:00.000000Z
                HBC\t0.0367581207471136\t1970-01-03T00:54:00.000000Z
                DXR\t0.6276954028373309\t1970-01-03T01:00:00.000000Z
                DXR\t0.6778564558839208\t1970-01-03T01:06:00.000000Z
                DXR\t0.8756771741121929\t1970-01-03T01:12:00.000000Z
                HBC\t0.8799634725391621\t1970-01-03T01:18:00.000000Z
                HBC\t0.5249321062686694\t1970-01-03T01:24:00.000000Z
                DXR\t0.7675673070796104\t1970-01-03T01:30:00.000000Z
                DXR\t0.21583224269349388\t1970-01-03T01:36:00.000000Z
                DXR\t0.15786635599554755\t1970-01-03T01:42:00.000000Z
                HBC\t0.1911234617573182\t1970-01-03T01:48:00.000000Z
                ABB\t0.5793466326862211\t1970-01-03T01:54:00.000000Z
                DXR\t0.9687423276940171\t1970-01-03T02:00:00.000000Z
                DXR\t0.6761934857077543\t1970-01-03T02:06:00.000000Z
                DXR\t0.4882051101858693\t1970-01-03T02:12:00.000000Z
                ABB\t0.42281342727402726\t1970-01-03T02:18:00.000000Z
                HBC\t0.810161274171258\t1970-01-03T02:24:00.000000Z
                DXR\t0.5298405941762054\t1970-01-03T02:30:00.000000Z
                HBC\t0.022965637512889825\t1970-01-03T02:36:00.000000Z
                ABB\t0.7763904674818695\t1970-01-03T02:42:00.000000Z
                DXR\t0.975019885372507\t1970-01-03T02:48:00.000000Z
                HBC\t0.0011075361080621349\t1970-01-03T02:54:00.000000Z
                DXR\t0.7643643144642823\t1970-01-03T03:00:00.000000Z
                DXR\t0.8001121139739173\t1970-01-03T03:06:00.000000Z
                HBC\t0.18769708157331322\t1970-01-03T03:12:00.000000Z
                ABB\t0.16381374773748514\t1970-01-03T03:18:00.000000Z
                ABB\t0.6590341607692226\t1970-01-03T03:24:00.000000Z
                DXR\t0.40455469747939254\t1970-01-03T03:30:00.000000Z
                ABB\t0.8837421918800907\t1970-01-03T03:36:00.000000Z
                HBC\t0.05384400312338511\t1970-01-03T03:42:00.000000Z
                ABB\t0.09750574414434399\t1970-01-03T03:48:00.000000Z
                DXR\t0.9644183832564398\t1970-01-03T03:54:00.000000Z
                DXR\t0.7588175403454873\t1970-01-03T04:00:00.000000Z
                DXR\t0.5778947915182423\t1970-01-03T04:06:00.000000Z
                HBC\t0.9269068519549879\t1970-01-03T04:12:00.000000Z
                DXR\t0.5449155021518948\t1970-01-03T04:18:00.000000Z
                DXR\t0.1202416087573498\t1970-01-03T04:24:00.000000Z
                DXR\t0.9640289041849747\t1970-01-03T04:30:00.000000Z
                ABB\t0.7133910271555843\t1970-01-03T04:36:00.000000Z
                HBC\t0.6551335839796312\t1970-01-03T04:42:00.000000Z
                DXR\t0.4971342426836798\t1970-01-03T04:48:00.000000Z
                DXR\t0.48558682958070665\t1970-01-03T04:54:00.000000Z
                HBC\t0.9047642416961028\t1970-01-03T05:00:00.000000Z
                HBC\t0.03167026265669903\t1970-01-03T05:06:00.000000Z
                DXR\t0.14830552335848957\t1970-01-03T05:12:00.000000Z
                DXR\t0.9441658975532605\t1970-01-03T05:18:00.000000Z
                HBC\t0.3456897991538844\t1970-01-03T05:24:00.000000Z
                DXR\t0.24008362859107102\t1970-01-03T05:30:00.000000Z
                DXR\t0.619291960382302\t1970-01-03T05:36:00.000000Z
                DXR\t0.17833722747266334\t1970-01-03T05:42:00.000000Z
                ABB\t0.2185865835029681\t1970-01-03T05:48:00.000000Z
                ABB\t0.3901731258748704\t1970-01-03T05:54:00.000000Z
                DXR\t0.7056586460237274\t1970-01-03T06:00:00.000000Z
                ABB\t0.8438459563914771\t1970-01-03T06:06:00.000000Z
                HBC\t0.13006100084163252\t1970-01-03T06:12:00.000000Z
                ABB\t0.3679848625908545\t1970-01-03T06:18:00.000000Z
                ABB\t0.06944480046327317\t1970-01-03T06:24:00.000000Z
                DXR\t0.4295631643526773\t1970-01-03T06:30:00.000000Z
                HBC\t0.5893398488053903\t1970-01-03T06:36:00.000000Z
                DXR\t0.5699444693578853\t1970-01-03T06:42:00.000000Z
                ABB\t0.9918093114862231\t1970-01-03T06:48:00.000000Z
                ABB\t0.32424562653969957\t1970-01-03T06:54:00.000000Z
                DXR\t0.8998921791869131\t1970-01-03T07:00:00.000000Z
                HBC\t0.7458169804091256\t1970-01-03T07:06:00.000000Z
                HBC\t0.33746104579374825\t1970-01-03T07:12:00.000000Z
                ABB\t0.18740488620384377\t1970-01-03T07:18:00.000000Z
                HBC\t0.10527282622013212\t1970-01-03T07:24:00.000000Z
                DXR\t0.8291193369353376\t1970-01-03T07:30:00.000000Z
                ABB\t0.32673950830571696\t1970-01-03T07:36:00.000000Z
                DXR\t0.5992548493051852\t1970-01-03T07:42:00.000000Z
                DXR\t0.6455967424250787\t1970-01-03T07:48:00.000000Z
                ABB\t0.6202777455654276\t1970-01-03T07:54:00.000000Z
                HBC\t0.029080850168636263\t1970-01-03T08:00:00.000000Z
                ABB\t0.10459352312331183\t1970-01-03T08:06:00.000000Z
                HBC\t0.5346019596733254\t1970-01-03T08:12:00.000000Z
                ABB\t0.9418719455092096\t1970-01-03T08:18:00.000000Z
                HBC\t0.6341292894843615\t1970-01-03T08:24:00.000000Z
                ABB\t0.7340656260730631\t1970-01-03T08:30:00.000000Z
                HBC\t0.5025890936351257\t1970-01-03T08:36:00.000000Z
                HBC\t0.8952510116133903\t1970-01-03T08:42:00.000000Z
                HBC\t0.48964139862697853\t1970-01-03T08:48:00.000000Z
                ABB\t0.7700798090070919\t1970-01-03T08:54:00.000000Z
                HBC\t0.4416432347777828\t1970-01-03T09:00:00.000000Z
                DXR\t0.05158459929273784\t1970-01-03T09:06:00.000000Z
                HBC\t0.2445295612285482\t1970-01-03T09:12:00.000000Z
                ABB\t0.5466900921405317\t1970-01-03T09:18:00.000000Z
                HBC\t0.5290006415737116\t1970-01-03T09:24:00.000000Z
                HBC\t0.7260468106076399\t1970-01-03T09:30:00.000000Z
                DXR\t0.7229359906306887\t1970-01-03T09:36:00.000000Z
                ABB\t0.4592067757817594\t1970-01-03T09:42:00.000000Z
                HBC\t0.5716129058692643\t1970-01-03T09:48:00.000000Z
                DXR\t0.05094182589333662\t1970-01-03T09:54:00.000000Z
                """;

        assertQuery("select sym k, price, ts from x where ts<'1970-01-04T10:30:00.000Z'")
                .ddl("""
                        create table x (
                            sym symbol index,
                            price double,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym,\s
                                rnd_double() price,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(100)) timestamp (ts)""")
                .timestamp("ts")
                .returns("k\tprice\tts\n", expected);
    }

    @Test
    public void testVirtualColumnCancelsPropagationOfOrderByAdvice() throws Exception {
        final String expected = """
                sym\tspread
                AA\t4171981
                AA\t74196247
                AA\t417348950
                AA\t1191199593
                AA\t1233285715
                BB\t-1912873112
                BB\t-1707758909
                BB\t-850582456
                """;

        assertQuery("select sym, ask-bid spread from x where sym in ('AA', 'BB' ) order by sym, spread")
                .ddl("""
                        create table x (
                            sym symbol index,
                            bid int,
                            ask int,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym,\s
                                rnd_int() bid,\s
                                rnd_int() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)""")
                .returns("sym\tspread\n", expected);
    }

    @Test
    public void testVirtualColumnCancelsPropagationOfOrderByAdviceDesc() throws Exception {
        final String expected = """
                sym\tspread
                AA\t1233285715
                AA\t1191199593
                AA\t417348950
                AA\t74196247
                AA\t4171981
                BB\t-850582456
                BB\t-1707758909
                BB\t-1912873112
                """;

        assertQuery("select sym, ask-bid spread from x where sym in ('AA', 'BB' ) order by sym, spread desc")
                .ddl("""
                        create table x (
                            sym symbol index,
                            bid int,
                            ask int,
                            ts timestamp
                        ) timestamp(ts) partition by DAY""")
                .mutateWith("""
                        insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym,\s
                                rnd_int() bid,\s
                                rnd_int() ask,\s
                                timestamp_sequence(172800000000, 360000000) ts\s
                            from long_sequence(10)) timestamp (ts)""")
                .returns("sym\tspread\n", expected);
    }

    private static void assertIndexOrder(ScanPlan.IndexOrder expected, String sql) throws Exception {
        assertPlanned(engine, sqlExecutionContext, sql, ScanPlan.class, scan -> Assert.assertEquals(expected, scan.getIndexOrder()));
    }

    private static void assertKeyOrderedDecodes(String query, ParquetDecodeHint consumerHint, ParquetDecodeHint expectedHint, long expectedDecodes) throws Exception {
        try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
            if (consumerHint != null) {
                cursor.setParquetDecodeHint(consumerHint);
            }
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
                // read every row
            }
            final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
            Assert.assertEquals(expectedDecodes, pool.getDecodeCount());
            Assert.assertEquals(expectedHint, pool.getDecodeHint());
        }
    }

    private static void assertLastRowAfterSkip(String query, long rowCount, long lastId) throws Exception {
        try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
            // a consumer that revisits rows keeps evicting the least recently used row group
            cursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
            final RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
            Assert.assertEquals(rowCount, counter.get());
            cursor.toTop();
            counter.set(rowCount - 1);
            cursor.skipRows(counter, RecordCursor.UNBOUNDED_ROW_COUNT);
            Assert.assertEquals(0, counter.get());
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(lastId, cursor.getRecord().getLong(0));
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private static void createIndexedKeyTable() throws Exception {
        execute("CREATE TABLE t (sym SYMBOL INDEX, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                    ('a', 1, '2024-01-01T00:00:01.000000Z'),
                    ('b', 2, '2024-01-01T00:00:02.000000Z'),
                    ('c', 3, '2024-01-01T00:00:03.000000Z'),
                    ('b', 4, '2024-01-01T00:00:04.000000Z'),
                    ('a', 5, '2024-01-01T00:00:05.000000Z')""");
    }

    private static void createIndexedTable() throws Exception {
        execute("CREATE TABLE t AS (SELECT x id, ('S' || (x % 2))::SYMBOL s, x * 1.0 v, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(24)), INDEX(s) TIMESTAMP(ts) PARTITION BY HOUR");
    }

    private static void createKeyTable(String name, String partitionBy) throws Exception {
        execute("CREATE TABLE " + name + " AS (SELECT x id, ('S' || (x % 3))::SYMBOL s, timestamp_sequence('2024-01-01', 1_000_000) ts FROM long_sequence(24)), INDEX(s) TIMESTAMP(ts) PARTITION BY " + partitionBy);
    }

    /**
     * Asserts that reads of the table ordered by keys, over the intervals and with the key predicates, return what a
     * sort of the rows the scan reads without the index returns, for both key orders, both timestamp orders and LIMITs
     * from either end.
     */
    private void assertKeyOrderedReadsMatchSort(String table, String[] intervals, String[] predicates) throws Exception {
        assertKeyOrderedReadsMatchSort(table, intervals, predicates, 0);
    }

    private void assertKeyOrderedReadsMatchSort(String table, String[] intervals, String[] predicates, long queryMemoryLimit) throws Exception {
        final StringSink expected = new StringSink();
        for (String interval : intervals) {
            for (String predicate : predicates) {
                final String where = interval.isEmpty()
                        ? (predicate.isEmpty() ? "" : " WHERE " + predicate)
                        : " WHERE " + interval + (predicate.isEmpty() ? "" : " AND " + predicate);
                for (String order : new String[]{"s, ts", "s DESC, ts DESC", "s, ts DESC", "s DESC, ts"}) {
                    for (String limit : new String[]{"", " LIMIT 5", " LIMIT -7", " LIMIT 3, 40", " LIMIT -30, -4"}) {
                        final String tail = "id, s, v, vc, ts FROM " + table + where + " ORDER BY " + order + limit;
                        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                        printSql("SELECT /*+ no_index(" + table + ") */ " + tail, expected);
                        node1.setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, queryMemoryLimit);
                        assertQuery("SELECT " + tail).noLeakCheck().sizeMayVary().inferTimestamp().returns(expected);
                    }
                }
            }
        }
    }

    private static void createGatherTable() throws Exception {
        execute("""
                CREATE TABLE g AS (
                    SELECT x id, (CASE WHEN x % 13 = 0 THEN NULL ELSE 'k' || (x % 7) END)::SYMBOL s, x * 0.5 v,
                        ('value-' || x)::VARCHAR vc, timestamp_sequence('2024-01-01', 900_000_000) ts
                    FROM long_sequence(300)
                ), INDEX(s) TIMESTAMP(ts) PARTITION BY DAY""");
    }

    private void assertReadFrames(String query, long readFrames) throws Exception {
        PageFrameRecordCursorImpl.testReadFrames.set(0);
        PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
        try {
            printSql(query, new StringSink());
        } finally {
            PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
        }
        Assert.assertEquals(readFrames, PageFrameRecordCursorImpl.testReadFrames.get());
    }

    private static void createParquetKeyTable() throws Exception {
        createKeyTable("k", "DAY");
        execute("INSERT INTO k VALUES (25, 'S1', '2024-01-02')");
        execute("ALTER TABLE k CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
    }

    private static long decodedRowGroupBytes(String query) throws Exception {
        try (RecordCursorFactory factory = select(query); RecordCursor cursor = factory.getBaseFactory().getCursor(sqlExecutionContext)) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
                // read every row
            }
            final PageFrameMemoryPool pool = ((AbstractPageFrameRecordCursor) cursor).getFrameMemoryPool();
            return pool.getCachedBytes() / pool.getCachedFrameCount();
        }
    }

    private void assertSkipCost(String query, long maxBlockHops, long walkedRows) throws Exception {
        BitmapIndexFwdReader.resetTestCounters();
        PageFrameRecordCursorImpl.testWalkedRows.set(0);
        BitmapIndexFwdReader.isBlockHopCounterEnabled = true;
        PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
        try {
            printSql(query, new StringSink());
        } finally {
            BitmapIndexFwdReader.isBlockHopCounterEnabled = false;
            PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
        }
        final long blockHops = BitmapIndexFwdReader.testRangeCountBlockHops.get();
        Assert.assertTrue("block hops: " + blockHops, blockHops <= maxBlockHops);
        Assert.assertEquals(walkedRows, PageFrameRecordCursorImpl.testWalkedRows.get());
    }

    private void assertWalksFewRows(String query) throws Exception {
        PageFrameRecordCursorImpl.testWalkedRows.set(0);
        PageFrameRecordCursorImpl.isWalkCounterEnabled = true;
        try {
            printSql(query, new StringSink());
        } finally {
            PageFrameRecordCursorImpl.isWalkCounterEnabled = false;
        }
        Assert.assertTrue("rows walked: " + PageFrameRecordCursorImpl.testWalkedRows.get(), PageFrameRecordCursorImpl.testWalkedRows.get() < 100);
    }
}
