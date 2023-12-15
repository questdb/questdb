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

import io.questdb.griffin.engine.functions.test.TestMatchFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class OrderByAdviceTest extends AbstractCairoTest {

    @Test
    public void testDistinctWithOrderBy() throws Exception {
        assertQuery(
                "b\n2\n1\n0\n",
                "select distinct b from x order by b desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null
        );
    }

    @Test
    public void testDistinctWithOrderByAnotherColumn() throws Exception {
        ddl(
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")"
        );

        assertException(
                "select distinct b from x order by a desc;",
                34,
                "ORDER BY expressions must appear in select list."
        );
    }

    @Test
    public void testDistinctWithOrderByIndex() throws Exception {
        assertQuery(
                "b\n2\n1\n0\n",
                "select distinct b from x order by 1 desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null
        );
    }

    @Test
    public void testExpressionSearchOrderByAlias() throws Exception {
        final String expected = "sym\tspread\n" +
                "HBC\t-1912873112\n" +
                "HBC\t-1707758909\n" +
                "DXR\t-1021839040\n" +
                "HBC\t-850582456\n" +
                "ABB\t4171981\n" +
                "ABB\t74196247\n" +
                "ABB\t417348950\n" +
                "ABB\t1191199593\n" +
                "ABB\t1233285715\n" +
                "DXR\t1275864035\n";

        assertQuery(
                "sym\tspread\n",
                "select sym, ask-bid spread from x where ts IN '1970-01-03' order by spread", "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    bid int,\n" +
                        "    ask int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_int() bid, \n" +
                        "        rnd_int() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true,
                true,
                false
        );
    }

    @Test
    public void testFunctionSearchOrderByAlias() throws Exception {
        final String expected = "sym\tmaxp\n" +
                "DXR\t0.97613283653158\n" +
                "ABB\t0.9809851788419132\n" +
                "HBC\t0.9940353811420282\n";

        assertQuery(
                "sym\tmaxp\n",
                "select sym , max(price) maxp from x where ts IN '1970-01-04' order by maxp",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true,
                true,
                false
        );
    }

    @Test
    public void testFunctionSearchOrderByAlias2() throws Exception {
        final String expected = "sym\tmaxp\n" +
                "DXR\t0.008134052047644613\n" +
                "HBC\t0.008427132543617488\n" +
                "ABB\t0.008444033230580739\n";

        assertQuery(
                "sym\tmaxp\n",
                "select sym, min(price) maxp from x where ts in '1970-01-04' order by maxp",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true,
                true,
                false
        );
    }

    @Test
    public void testNoKeyGroupBy() throws Exception {
        assertQuery(
                "column\nNaN\n",
                "select sum(price) / count() from x where price > 0",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                "column\n" +
                        "0.48510032025339733\n",
                false,
                true,
                false
        );
    }

    @Test
    public void testOrderBy2Columns() throws Exception {
        final String expected = "k\tprice\tts\n" +
                "ABB\t0.4217768841969397\t1970-01-03T00:48:00.000000Z\n" +
                "ABB\t0.7611029514995744\t1970-01-03T00:42:00.000000Z\n" +
                "ABB\t0.3491070363730514\t1970-01-03T00:36:00.000000Z\n" +
                "ABB\t0.22452340856088226\t1970-01-03T00:30:00.000000Z\n" +
                "ABB\t0.8043224099968393\t1970-01-03T00:00:00.000000Z\n" +
                "DXR\t0.0843832076262595\t1970-01-03T00:12:00.000000Z\n" +
                "DXR\t0.08486964232560668\t1970-01-03T00:06:00.000000Z\n" +
                "HBC\t0.0367581207471136\t1970-01-03T00:54:00.000000Z\n" +
                "HBC\t0.7905675319675964\t1970-01-03T00:24:00.000000Z\n" +
                "HBC\t0.6508594025855301\t1970-01-03T00:18:00.000000Z\n";

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x order by k, ts desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true,
                true,
                false
        );
    }

    @Test
    public void testOrderByGeoByte() throws Exception {
        assertQuery(
                "id\tgeo\n",
                "select * from pos order by geo desc",
                "CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(5b) " +
                        ")",
                null,
                "insert into pos  values ( 1,##00001), ( 2,##00010), ( 3, ##00011 ), ( 16, ##10000 )",
                "id\tgeo\n" +
                        "16\th\n" +
                        "3\t3\n" +
                        "2\t2\n" +
                        "1\t1\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testOrderByGeoInt1() throws Exception {
        assertQuery(
                "id\tgeo\n",
                "select * from pos order by geo desc",
                "CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(20b) " +
                        ")",
                null,
                "insert into pos  values ( 1,##00000000000000000001), ( 2,##00000000000000000010), ( 3, ##00000000000000000011 ), ( 16, ##00000000000000010000 )",
                "id\tgeo\n" +
                        "16\t000h\n" +
                        "3\t0003\n" +
                        "2\t0002\n" +
                        "1\t0001\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testOrderByGeoInt2() throws Exception {
        assertQuery(
                "id\tgeo\n",
                "select * from pos order by geo desc",
                "CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(17b) " +
                        ")",
                null,
                "insert into pos  values ( 1,##00000000000000001), ( 2,##00000000000000010), ( 3, ##00000000000000011 ), ( 16, ##00000000000010000 )",
                "id\tgeo\n" +
                        "16\t00000000000010000\n" +
                        "3\t00000000000000011\n" +
                        "2\t00000000000000010\n" +
                        "1\t00000000000000001\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testOrderByGeoLong() throws Exception {
        assertQuery(
                "id\tgeo\n",
                "select * from pos order by geo desc",
                "CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(35b) " +
                        ")",
                null,
                "insert into pos  values ( 1,##00000000000000000000000000000000001), ( 2,##00000000000000000000000000000000010), " +
                        "( 3, ##00000000000000000000000000000000011 ), ( 16, ##00000000000000000000000000000010000 )",
                "id\tgeo\n" +
                        "16\t000000h\n" +
                        "3\t0000003\n" +
                        "2\t0000002\n" +
                        "1\t0000001\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testOrderByGeoShort() throws Exception {
        assertQuery(
                "id\tgeo\n",
                "select * from pos order by geo desc",
                "CREATE TABLE pos (" +
                        "  id int," +
                        "  geo GEOHASH(10b) " +
                        ")",
                null,
                "insert into pos  values ( 1,##0000000001), ( 2,##0000000010), ( 3, ##0000000011 ), ( 16, ##0000010000 )",
                "id\tgeo\n" +
                        "16\t0h\n" +
                        "3\t03\n" +
                        "2\t02\n" +
                        "1\t01\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testOrderByMultipleColumns() throws Exception {
        final String expected = "sym\tprice\tts\n" +
                "AA\t-847531048\t1970-01-03T00:24:00.000000Z\n" +
                "AA\t315515118\t1970-01-03T00:00:00.000000Z\n" +
                "AA\t339631474\t1970-01-03T00:54:00.000000Z\n" +
                "AA\t1573662097\t1970-01-03T00:48:00.000000Z\n" +
                "BB\t-2041844972\t1970-01-03T00:30:00.000000Z\n" +
                "BB\t-1575378703\t1970-01-03T00:36:00.000000Z\n" +
                "BB\t-727724771\t1970-01-03T00:06:00.000000Z\n" +
                "BB\t1545253512\t1970-01-03T00:42:00.000000Z\n";

        assertQuery(
                "sym\tprice\tts\n",
                "select * from tab where sym in ('AA', 'BB') order by sym, price",
                "create table tab (\n" +
                        "    sym symbol index,\n" +
                        "    price int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into tab select * from (select rnd_symbol('AA', 'BB', 'CC') sym, \n" +
                        "        rnd_int() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnAliases() throws Exception {
        assertQuery(
                "aliased\n1\n2\n3\n4\n5\n6\n7\n8\n9\n",
                "select a as aliased from x order by aliased asc, aliased desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnDoubleAliased() throws Exception {
        assertQuery(
                "a1\ta2\n" +
                        "1\t1\n" +
                        "2\t2\n" +
                        "3\t3\n" +
                        "4\t4\n" +
                        "5\t5\n" +
                        "6\t6\n" +
                        "7\t7\n" +
                        "8\t8\n" +
                        "9\t9\n",
                "select a as a1, a as a2 from x order by a1 asc, a2 desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnLiteralsMixedWithPositions() throws Exception {
        assertQuery(
                "a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n",
                "select a from x order by a asc, 1 desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnNameLiteral() throws Exception {
        assertQuery(
                "a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n",
                "select a from x order by a asc, a desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnNameLiteral_differentCases() throws Exception {
        assertQuery(
                "a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n",
                "select a from x order by a asc, A desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnPositions() throws Exception {
        assertQuery(
                "a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n",
                "select a from x order by 1 asc, 1 desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnQuoted() throws Exception {
        assertQuery(
                "a\n1\n2\n3\n4\n5\n6\n7\n8\n9\n",
                "select a from x order by \"a\" asc, \"a\" desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByPriority_columnsInterleaved() throws Exception {
        assertQuery(
                "a\tb\n" +
                        "9\t0\n" +
                        "6\t0\n" +
                        "3\t0\n" +
                        "7\t1\n" +
                        "4\t1\n" +
                        "1\t1\n" +
                        "8\t2\n" +
                        "5\t2\n" +
                        "2\t2\n",
                "select * from x order by b asc, a desc, b desc;",
                "create table x as (" +
                        "select" +
                        " x a," +
                        " x % 3 b" +
                        " from long_sequence(9)" +
                        ")",
                null,
                true,
                true
        );
    }

    @Test
    public void testOrderByTwoGeoHashes1() throws Exception {
        assertQuery(
                "id\tgeo1\tgeo3\n",
                "select * from pos order by geo1 asc, geo3 desc",
                "CREATE TABLE pos (" +
                        "  id int," +
                        "  geo1 GEOHASH(1c)," +
                        "  geo3 GEOHASH(3c)" +
                        ")", null,
                "insert into pos  values ( 1, #1, #001), ( 2, #1, #002), ( 4, #4, #004) ",
                "id\tgeo1\tgeo3\n" +
                        "2\t1\t002\n" +
                        "1\t1\t001\n" +
                        "4\t4\t004\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testOrderByTwoGeoHashes2() throws Exception {
        assertQuery(
                "id\tgeo1\tgeo3\n",
                "select * from pos order by geo1 asc, geo3 desc",
                "CREATE TABLE pos (" +
                        "  id int," +
                        "  geo1 GEOHASH(7b)," +
                        "  geo3 GEOHASH(15b)" +
                        ")", null,
                "insert into pos  values ( 1, ##0000001, ##000000000000001), " +
                        "( 2, ##1000000, ##100000000000000), ( 3, ##0000001, ##100000000000000) ",
                "id\tgeo1\tgeo3\n" +
                        "3\t0000001\th00\n" +
                        "1\t0000001\t001\n" +
                        "2\t1000000\th00\n",
                true,
                true,
                false
        );
    }

    @Test
    public void testSelectWithInClauseAndOrderByTimestampDesc() throws Exception {
        final String expected = "sym\tbid\task\tts\n" +
                "BB\t-85170055\t-1792928964\t1970-01-03T00:54:00.000000Z\n" +
                "AA\t-1849627000\t-1432278050\t1970-01-03T00:48:00.000000Z\n" +
                "AA\t-1532328444\t-1458132197\t1970-01-03T00:42:00.000000Z\n" +
                "AA\t339631474\t1530831067\t1970-01-03T00:36:00.000000Z\n" +
                "AA\t1569490116\t1573662097\t1970-01-03T00:30:00.000000Z\n" +
                "BB\t-1575378703\t806715481\t1970-01-03T00:24:00.000000Z\n" +
                "BB\t-1191262516\t-2041844972\t1970-01-03T00:18:00.000000Z\n" +
                "AA\t315515118\t1548800833\t1970-01-03T00:00:00.000000Z\n";


        assertQuery(
                "sym\tbid\task\tts\n",
                "select * from x where sym in ('AA', 'BB' ) order by ts desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    bid int,\n" +
                        "    ask int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                "ts###DESC",
                "insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym, \n" +
                        "        rnd_int() bid, \n" +
                        "        rnd_int() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true
        );
    }

    @Test
    public void testSelectWithOrderByTimestampDesc() throws Exception {
        final String expected = "sym\tbid\task\tts\n" +
                "BB\t-85170055\t-1792928964\t1970-01-03T00:54:00.000000Z\n" +
                "AA\t-1849627000\t-1432278050\t1970-01-03T00:48:00.000000Z\n" +
                "AA\t-1532328444\t-1458132197\t1970-01-03T00:42:00.000000Z\n" +
                "AA\t339631474\t1530831067\t1970-01-03T00:36:00.000000Z\n" +
                "AA\t1569490116\t1573662097\t1970-01-03T00:30:00.000000Z\n" +
                "BB\t-1575378703\t806715481\t1970-01-03T00:24:00.000000Z\n" +
                "BB\t-1191262516\t-2041844972\t1970-01-03T00:18:00.000000Z\n" +
                "CC\t592859671\t1868723706\t1970-01-03T00:12:00.000000Z\n" +
                "CC\t73575701\t-948263339\t1970-01-03T00:06:00.000000Z\n" +
                "AA\t315515118\t1548800833\t1970-01-03T00:00:00.000000Z\n";

        assertQuery(
                "sym\tbid\task\tts\n",
                "select * from x order by ts desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    bid int,\n" +
                        "    ask int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym, \n" +
                        "        rnd_int() bid, \n" +
                        "        rnd_int() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true,
                true,
                false
        );
    }

    @Test
    public void testSingleSymbolSearchOrderByAliasAndTimestamp() throws Exception {
        final String expected = "k\tprice\tts\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n";

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where sym = 'HBC' and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true

        );
    }

    @Test
    public void testSingleSymbolSearchOrderByAliasAndTimestampDesc() throws Exception {
        final String expected = "k\tprice\tts\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n";

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where sym = 'HBC' and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' and 2 > 1 order by k, ts desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true

        );
    }

    @Test
    public void testSingleSymbolSearchOrderByAliasAndTimestampDescEmpty() throws Exception {
        TestMatchFunctionFactory.clear();

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where 1 = 2 and sym = 'HBC' and test_match() and ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                true,
                true

        );

        Assert.assertTrue(TestMatchFunctionFactory.isClosed());
    }

    @Test
    public void testSymbolSearchOrderBy() throws Exception {
        final String expected = "sym\tprice\tts\n" +
                "ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z\n" +
                "ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z\n" +
                "ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z\n" +
                "ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z\n" +
                "ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z\n" +
                "ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z\n" +
                "ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z\n" +
                "ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z\n" +
                "ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z\n" +
                "ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z\n" +
                "ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z\n" +
                "ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z\n" +
                "ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z\n" +
                "ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z\n" +
                "ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z\n" +
                "ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z\n" +
                "ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z\n" +
                "ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z\n" +
                "ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z\n" +
                "ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z\n" +
                "ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z\n" +
                "ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z\n" +
                "ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z\n" +
                "ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z\n" +
                "ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z\n" +
                "ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z\n" +
                "ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z\n" +
                "ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z\n" +
                "ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z\n" +
                "ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z\n" +
                "ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z\n" +
                "ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z\n" +
                "ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z\n" +
                "ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z\n" +
                "ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z\n" +
                "ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z\n" +
                "ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n";

        assertQuery(
                "sym\tprice\tts\n",
                "x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by sym",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true

        );
    }

    @Test
    public void testSymbolSearchOrderByAlias() throws Exception {
        final String expected = "k\tprice\tts\n" +
                "ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z\n" +
                "ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z\n" +
                "ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z\n" +
                "ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z\n" +
                "ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z\n" +
                "ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z\n" +
                "ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z\n" +
                "ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z\n" +
                "ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z\n" +
                "ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z\n" +
                "ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z\n" +
                "ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z\n" +
                "ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z\n" +
                "ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z\n" +
                "ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z\n" +
                "ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z\n" +
                "ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z\n" +
                "ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z\n" +
                "ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z\n" +
                "ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z\n" +
                "ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z\n" +
                "ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z\n" +
                "ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z\n" +
                "ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z\n" +
                "ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z\n" +
                "ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z\n" +
                "ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z\n" +
                "ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z\n" +
                "ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z\n" +
                "ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z\n" +
                "ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z\n" +
                "ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z\n" +
                "ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z\n" +
                "ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z\n" +
                "ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z\n" +
                "ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z\n" +
                "ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n";

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true

        );
    }

    @Test
    public void testSymbolSearchOrderByAliasAndTimestamp() throws Exception {
        final String expected = "k\tprice\tts\n" +
                "ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z\n" +
                "ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z\n" +
                "ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z\n" +
                "ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z\n" +
                "ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z\n" +
                "ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z\n" +
                "ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z\n" +
                "ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z\n" +
                "ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z\n" +
                "ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z\n" +
                "ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z\n" +
                "ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z\n" +
                "ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z\n" +
                "ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z\n" +
                "ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z\n" +
                "ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z\n" +
                "ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z\n" +
                "ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z\n" +
                "ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z\n" +
                "ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z\n" +
                "ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z\n" +
                "ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z\n" +
                "ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z\n" +
                "ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z\n" +
                "ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z\n" +
                "ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z\n" +
                "ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z\n" +
                "ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z\n" +
                "ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z\n" +
                "ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z\n" +
                "ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z\n" +
                "ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z\n" +
                "ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z\n" +
                "ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z\n" +
                "ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z\n" +
                "ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z\n" +
                "ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n";

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true

        );
    }

    @Test
    public void testSymbolSearchOrderByAliasAndTimestampDesc() throws Exception {
        final String expected = "k\tprice\tts\n" +
                "ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z\n" +
                "ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z\n" +
                "ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z\n" +
                "ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z\n" +
                "ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z\n" +
                "ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z\n" +
                "ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z\n" +
                "ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z\n" +
                "ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z\n" +
                "ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z\n" +
                "ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z\n" +
                "ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z\n" +
                "ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z\n" +
                "ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z\n" +
                "ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z\n" +
                "ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z\n" +
                "ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z\n" +
                "ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z\n" +
                "ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z\n" +
                "ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z\n" +
                "ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z\n" +
                "ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z\n" +
                "ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z\n" +
                "ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z\n" +
                "ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z\n" +
                "ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z\n" +
                "ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z\n" +
                "ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z\n" +
                "ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z\n" +
                "ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z\n" +
                "ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z\n" +
                "ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z\n" +
                "ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z\n" +
                "ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z\n" +
                "ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z\n" +
                "ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z\n" +
                "ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n";

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true
        );
    }

    @Test
    public void testSymbolSearchOrderByAliasAndTimestampDescEmpty() throws Exception {
        TestMatchFunctionFactory.clear();
        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where sym in ('HBC', 'ABB') and 1 = 3 and test_match() and ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by k, ts desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                true,
                true

        );
        Assert.assertTrue(TestMatchFunctionFactory.isClosed());
    }

    @Test
    public void testSymbolSearchOrderByIndex() throws Exception {
        final String expected = "sym\tprice\tts\n" +
                "ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z\n" +
                "ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z\n" +
                "ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z\n" +
                "ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z\n" +
                "ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z\n" +
                "ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z\n" +
                "ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z\n" +
                "ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z\n" +
                "ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z\n" +
                "ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z\n" +
                "ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z\n" +
                "ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z\n" +
                "ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z\n" +
                "ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z\n" +
                "ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z\n" +
                "ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z\n" +
                "ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z\n" +
                "ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z\n" +
                "ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z\n" +
                "ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z\n" +
                "ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z\n" +
                "ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z\n" +
                "ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z\n" +
                "ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z\n" +
                "ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z\n" +
                "ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z\n" +
                "ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z\n" +
                "ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z\n" +
                "ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z\n" +
                "ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z\n" +
                "ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z\n" +
                "ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z\n" +
                "ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z\n" +
                "ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z\n" +
                "ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z\n" +
                "ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z\n" +
                "ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n";

        assertQuery(
                "sym\tprice\tts\n",
                "x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by 1",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true

        );
    }

    @Test
    public void testSymbolSearchOrderByIndexDesc() throws Exception {
        final String expected = "sym\tprice\tts\n" +
                "HBC\t0.1350821238488883\t1970-01-04T00:18:00.000000Z\n" +
                "HBC\t0.3397922134720558\t1970-01-04T00:24:00.000000Z\n" +
                "HBC\t0.365427022047211\t1970-01-04T00:36:00.000000Z\n" +
                "HBC\t0.8486538207666282\t1970-01-04T00:48:00.000000Z\n" +
                "HBC\t0.15121120303896474\t1970-01-04T00:54:00.000000Z\n" +
                "HBC\t0.9370193388878216\t1970-01-04T01:00:00.000000Z\n" +
                "HBC\t0.16064467510169633\t1970-01-04T01:18:00.000000Z\n" +
                "HBC\t0.0846754178136283\t1970-01-04T01:24:00.000000Z\n" +
                "HBC\t0.4039042639581232\t1970-01-04T01:42:00.000000Z\n" +
                "HBC\t0.2103287968720018\t1970-01-04T02:54:00.000000Z\n" +
                "HBC\t0.8148792629172324\t1970-01-04T03:24:00.000000Z\n" +
                "HBC\t0.25131981920574875\t1970-01-04T04:06:00.000000Z\n" +
                "HBC\t0.9694731343686098\t1970-01-04T04:18:00.000000Z\n" +
                "HBC\t0.5371478985442728\t1970-01-04T05:00:00.000000Z\n" +
                "HBC\t0.6852762111021103\t1970-01-04T05:18:00.000000Z\n" +
                "HBC\t0.08039440728458325\t1970-01-04T05:24:00.000000Z\n" +
                "HBC\t0.05890936334115593\t1970-01-04T05:36:00.000000Z\n" +
                "HBC\t0.9750738231283522\t1970-01-04T06:24:00.000000Z\n" +
                "HBC\t0.8277715252854949\t1970-01-04T06:30:00.000000Z\n" +
                "HBC\t0.4758209004780879\t1970-01-04T07:00:00.000000Z\n" +
                "HBC\t0.38392106356809774\t1970-01-04T07:42:00.000000Z\n" +
                "HBC\t0.2753819635358048\t1970-01-04T08:06:00.000000Z\n" +
                "HBC\t0.7553832117277283\t1970-01-04T08:12:00.000000Z\n" +
                "HBC\t0.1572805871525168\t1970-01-04T08:24:00.000000Z\n" +
                "HBC\t0.6367746812001958\t1970-01-04T08:42:00.000000Z\n" +
                "HBC\t0.10287867683029772\t1970-01-04T09:12:00.000000Z\n" +
                "HBC\t0.8756165114231503\t1970-01-04T09:24:00.000000Z\n" +
                "HBC\t0.48422909268940273\t1970-01-04T09:48:00.000000Z\n" +
                "HBC\t0.6504194217741501\t1970-01-04T10:06:00.000000Z\n" +
                "ABB\t0.33046819455237\t1970-01-04T00:00:00.000000Z\n" +
                "ABB\t0.3124458010612313\t1970-01-04T00:12:00.000000Z\n" +
                "ABB\t0.5765797240495835\t1970-01-04T01:30:00.000000Z\n" +
                "ABB\t0.4913342104187668\t1970-01-04T01:36:00.000000Z\n" +
                "ABB\t0.8802810667279274\t1970-01-04T01:54:00.000000Z\n" +
                "ABB\t0.6944149053754287\t1970-01-04T02:00:00.000000Z\n" +
                "ABB\t0.0567238328086237\t1970-01-04T02:18:00.000000Z\n" +
                "ABB\t0.9216728993460965\t1970-01-04T02:30:00.000000Z\n" +
                "ABB\t0.3242526975448907\t1970-01-04T03:00:00.000000Z\n" +
                "ABB\t0.42558021324800144\t1970-01-04T03:06:00.000000Z\n" +
                "ABB\t0.9534844124580377\t1970-01-04T03:18:00.000000Z\n" +
                "ABB\t0.1339704489137793\t1970-01-04T03:36:00.000000Z\n" +
                "ABB\t0.4950615235019964\t1970-01-04T03:42:00.000000Z\n" +
                "ABB\t0.3595576962747611\t1970-01-04T03:54:00.000000Z\n" +
                "ABB\t0.21224614178286005\t1970-01-04T04:12:00.000000Z\n" +
                "ABB\t0.5614062040523734\t1970-01-04T04:30:00.000000Z\n" +
                "ABB\t0.16011053107067486\t1970-01-04T04:54:00.000000Z\n" +
                "ABB\t0.28019218825051395\t1970-01-04T05:06:00.000000Z\n" +
                "ABB\t0.9328540909719272\t1970-01-04T05:12:00.000000Z\n" +
                "ABB\t0.13525597398079747\t1970-01-04T05:48:00.000000Z\n" +
                "ABB\t0.10663485323987387\t1970-01-04T05:54:00.000000Z\n" +
                "ABB\t0.647875746786617\t1970-01-04T06:42:00.000000Z\n" +
                "ABB\t0.8203418140538824\t1970-01-04T07:06:00.000000Z\n" +
                "ABB\t0.22122747948030208\t1970-01-04T07:12:00.000000Z\n" +
                "ABB\t0.48731616038337855\t1970-01-04T07:18:00.000000Z\n" +
                "ABB\t0.05579995341081423\t1970-01-04T07:24:00.000000Z\n" +
                "ABB\t0.2544317267472076\t1970-01-04T07:30:00.000000Z\n" +
                "ABB\t0.23673087740006105\t1970-01-04T07:36:00.000000Z\n" +
                "ABB\t0.6713174919725877\t1970-01-04T07:48:00.000000Z\n" +
                "ABB\t0.6383145056717429\t1970-01-04T08:00:00.000000Z\n" +
                "ABB\t0.12715627282156716\t1970-01-04T08:18:00.000000Z\n" +
                "ABB\t0.22156975706915538\t1970-01-04T08:48:00.000000Z\n" +
                "ABB\t0.43117716480568924\t1970-01-04T08:54:00.000000Z\n" +
                "ABB\t0.5519190966196398\t1970-01-04T09:00:00.000000Z\n" +
                "ABB\t0.5884931033499815\t1970-01-04T09:36:00.000000Z\n" +
                "ABB\t0.23387203820756874\t1970-01-04T10:12:00.000000Z\n" +
                "ABB\t0.858967821197869\t1970-01-04T10:24:00.000000Z\n";

        assertQuery(
                "sym\tprice\tts\n",
                "x where sym in ('HBC', 'ABB') and  ts>='1970-01-04T00:00:00.000Z' and ts< '1970-01-04T10:30:00.000Z' order by 1 desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(1000)) timestamp (ts)",
                expected,
                true
        );
    }

    @Test
    public void testTimestampLessThan() throws Exception {
        final String expected = "k\tprice\tts\n" +
                "ABB\t0.8043224099968393\t1970-01-03T00:00:00.000000Z\n" +
                "DXR\t0.08486964232560668\t1970-01-03T00:06:00.000000Z\n" +
                "DXR\t0.0843832076262595\t1970-01-03T00:12:00.000000Z\n" +
                "HBC\t0.6508594025855301\t1970-01-03T00:18:00.000000Z\n" +
                "HBC\t0.7905675319675964\t1970-01-03T00:24:00.000000Z\n" +
                "ABB\t0.22452340856088226\t1970-01-03T00:30:00.000000Z\n" +
                "ABB\t0.3491070363730514\t1970-01-03T00:36:00.000000Z\n" +
                "ABB\t0.7611029514995744\t1970-01-03T00:42:00.000000Z\n" +
                "ABB\t0.4217768841969397\t1970-01-03T00:48:00.000000Z\n" +
                "HBC\t0.0367581207471136\t1970-01-03T00:54:00.000000Z\n" +
                "DXR\t0.6276954028373309\t1970-01-03T01:00:00.000000Z\n" +
                "DXR\t0.6778564558839208\t1970-01-03T01:06:00.000000Z\n" +
                "DXR\t0.8756771741121929\t1970-01-03T01:12:00.000000Z\n" +
                "HBC\t0.8799634725391621\t1970-01-03T01:18:00.000000Z\n" +
                "HBC\t0.5249321062686694\t1970-01-03T01:24:00.000000Z\n" +
                "DXR\t0.7675673070796104\t1970-01-03T01:30:00.000000Z\n" +
                "DXR\t0.21583224269349388\t1970-01-03T01:36:00.000000Z\n" +
                "DXR\t0.15786635599554755\t1970-01-03T01:42:00.000000Z\n" +
                "HBC\t0.1911234617573182\t1970-01-03T01:48:00.000000Z\n" +
                "ABB\t0.5793466326862211\t1970-01-03T01:54:00.000000Z\n" +
                "DXR\t0.9687423276940171\t1970-01-03T02:00:00.000000Z\n" +
                "DXR\t0.6761934857077543\t1970-01-03T02:06:00.000000Z\n" +
                "DXR\t0.4882051101858693\t1970-01-03T02:12:00.000000Z\n" +
                "ABB\t0.42281342727402726\t1970-01-03T02:18:00.000000Z\n" +
                "HBC\t0.810161274171258\t1970-01-03T02:24:00.000000Z\n" +
                "DXR\t0.5298405941762054\t1970-01-03T02:30:00.000000Z\n" +
                "HBC\t0.022965637512889825\t1970-01-03T02:36:00.000000Z\n" +
                "ABB\t0.7763904674818695\t1970-01-03T02:42:00.000000Z\n" +
                "DXR\t0.975019885372507\t1970-01-03T02:48:00.000000Z\n" +
                "HBC\t0.0011075361080621349\t1970-01-03T02:54:00.000000Z\n" +
                "DXR\t0.7643643144642823\t1970-01-03T03:00:00.000000Z\n" +
                "DXR\t0.8001121139739173\t1970-01-03T03:06:00.000000Z\n" +
                "HBC\t0.18769708157331322\t1970-01-03T03:12:00.000000Z\n" +
                "ABB\t0.16381374773748514\t1970-01-03T03:18:00.000000Z\n" +
                "ABB\t0.6590341607692226\t1970-01-03T03:24:00.000000Z\n" +
                "DXR\t0.40455469747939254\t1970-01-03T03:30:00.000000Z\n" +
                "ABB\t0.8837421918800907\t1970-01-03T03:36:00.000000Z\n" +
                "HBC\t0.05384400312338511\t1970-01-03T03:42:00.000000Z\n" +
                "ABB\t0.09750574414434399\t1970-01-03T03:48:00.000000Z\n" +
                "DXR\t0.9644183832564398\t1970-01-03T03:54:00.000000Z\n" +
                "DXR\t0.7588175403454873\t1970-01-03T04:00:00.000000Z\n" +
                "DXR\t0.5778947915182423\t1970-01-03T04:06:00.000000Z\n" +
                "HBC\t0.9269068519549879\t1970-01-03T04:12:00.000000Z\n" +
                "DXR\t0.5449155021518948\t1970-01-03T04:18:00.000000Z\n" +
                "DXR\t0.1202416087573498\t1970-01-03T04:24:00.000000Z\n" +
                "DXR\t0.9640289041849747\t1970-01-03T04:30:00.000000Z\n" +
                "ABB\t0.7133910271555843\t1970-01-03T04:36:00.000000Z\n" +
                "HBC\t0.6551335839796312\t1970-01-03T04:42:00.000000Z\n" +
                "DXR\t0.4971342426836798\t1970-01-03T04:48:00.000000Z\n" +
                "DXR\t0.48558682958070665\t1970-01-03T04:54:00.000000Z\n" +
                "HBC\t0.9047642416961028\t1970-01-03T05:00:00.000000Z\n" +
                "HBC\t0.03167026265669903\t1970-01-03T05:06:00.000000Z\n" +
                "DXR\t0.14830552335848957\t1970-01-03T05:12:00.000000Z\n" +
                "DXR\t0.9441658975532605\t1970-01-03T05:18:00.000000Z\n" +
                "HBC\t0.3456897991538844\t1970-01-03T05:24:00.000000Z\n" +
                "DXR\t0.24008362859107102\t1970-01-03T05:30:00.000000Z\n" +
                "DXR\t0.619291960382302\t1970-01-03T05:36:00.000000Z\n" +
                "DXR\t0.17833722747266334\t1970-01-03T05:42:00.000000Z\n" +
                "ABB\t0.2185865835029681\t1970-01-03T05:48:00.000000Z\n" +
                "ABB\t0.3901731258748704\t1970-01-03T05:54:00.000000Z\n" +
                "DXR\t0.7056586460237274\t1970-01-03T06:00:00.000000Z\n" +
                "ABB\t0.8438459563914771\t1970-01-03T06:06:00.000000Z\n" +
                "HBC\t0.13006100084163252\t1970-01-03T06:12:00.000000Z\n" +
                "ABB\t0.3679848625908545\t1970-01-03T06:18:00.000000Z\n" +
                "ABB\t0.06944480046327317\t1970-01-03T06:24:00.000000Z\n" +
                "DXR\t0.4295631643526773\t1970-01-03T06:30:00.000000Z\n" +
                "HBC\t0.5893398488053903\t1970-01-03T06:36:00.000000Z\n" +
                "DXR\t0.5699444693578853\t1970-01-03T06:42:00.000000Z\n" +
                "ABB\t0.9918093114862231\t1970-01-03T06:48:00.000000Z\n" +
                "ABB\t0.32424562653969957\t1970-01-03T06:54:00.000000Z\n" +
                "DXR\t0.8998921791869131\t1970-01-03T07:00:00.000000Z\n" +
                "HBC\t0.7458169804091256\t1970-01-03T07:06:00.000000Z\n" +
                "HBC\t0.33746104579374825\t1970-01-03T07:12:00.000000Z\n" +
                "ABB\t0.18740488620384377\t1970-01-03T07:18:00.000000Z\n" +
                "HBC\t0.10527282622013212\t1970-01-03T07:24:00.000000Z\n" +
                "DXR\t0.8291193369353376\t1970-01-03T07:30:00.000000Z\n" +
                "ABB\t0.32673950830571696\t1970-01-03T07:36:00.000000Z\n" +
                "DXR\t0.5992548493051852\t1970-01-03T07:42:00.000000Z\n" +
                "DXR\t0.6455967424250787\t1970-01-03T07:48:00.000000Z\n" +
                "ABB\t0.6202777455654276\t1970-01-03T07:54:00.000000Z\n" +
                "HBC\t0.029080850168636263\t1970-01-03T08:00:00.000000Z\n" +
                "ABB\t0.10459352312331183\t1970-01-03T08:06:00.000000Z\n" +
                "HBC\t0.5346019596733254\t1970-01-03T08:12:00.000000Z\n" +
                "ABB\t0.9418719455092096\t1970-01-03T08:18:00.000000Z\n" +
                "HBC\t0.6341292894843615\t1970-01-03T08:24:00.000000Z\n" +
                "ABB\t0.7340656260730631\t1970-01-03T08:30:00.000000Z\n" +
                "HBC\t0.5025890936351257\t1970-01-03T08:36:00.000000Z\n" +
                "HBC\t0.8952510116133903\t1970-01-03T08:42:00.000000Z\n" +
                "HBC\t0.48964139862697853\t1970-01-03T08:48:00.000000Z\n" +
                "ABB\t0.7700798090070919\t1970-01-03T08:54:00.000000Z\n" +
                "HBC\t0.4416432347777828\t1970-01-03T09:00:00.000000Z\n" +
                "DXR\t0.05158459929273784\t1970-01-03T09:06:00.000000Z\n" +
                "HBC\t0.2445295612285482\t1970-01-03T09:12:00.000000Z\n" +
                "ABB\t0.5466900921405317\t1970-01-03T09:18:00.000000Z\n" +
                "HBC\t0.5290006415737116\t1970-01-03T09:24:00.000000Z\n" +
                "HBC\t0.7260468106076399\t1970-01-03T09:30:00.000000Z\n" +
                "DXR\t0.7229359906306887\t1970-01-03T09:36:00.000000Z\n" +
                "ABB\t0.4592067757817594\t1970-01-03T09:42:00.000000Z\n" +
                "HBC\t0.5716129058692643\t1970-01-03T09:48:00.000000Z\n" +
                "DXR\t0.05094182589333662\t1970-01-03T09:54:00.000000Z\n";

        assertQuery(
                "k\tprice\tts\n",
                "select sym k, price, ts from x where ts<'1970-01-04T10:30:00.000Z'", "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    price double,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                "ts",
                "insert into x select * from (select rnd_symbol('ABB', 'HBC', 'DXR') sym, \n" +
                        "        rnd_double() price, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(100)) timestamp (ts)",
                expected,
                true,
                true,
                false
        );
    }

    @Test
    public void testVirtualColumnCancelsPropagationOfOrderByAdvice() throws Exception {
        final String expected = "sym\tspread\n" +
                "AA\t4171981\n" +
                "AA\t74196247\n" +
                "AA\t417348950\n" +
                "AA\t1191199593\n" +
                "AA\t1233285715\n" +
                "BB\t-1912873112\n" +
                "BB\t-1707758909\n" +
                "BB\t-850582456\n";

        assertQuery(
                "sym\tspread\n",
                "select sym, ask-bid spread from x where sym in ('AA', 'BB' ) order by sym, spread",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    bid int,\n" +
                        "    ask int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym, \n" +
                        "        rnd_int() bid, \n" +
                        "        rnd_int() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true
        );
    }

    @Test
    public void testVirtualColumnCancelsPropagationOfOrderByAdviceDesc() throws Exception {
        final String expected = "sym\tspread\n" +
                "AA\t1233285715\n" +
                "AA\t1191199593\n" +
                "AA\t417348950\n" +
                "AA\t74196247\n" +
                "AA\t4171981\n" +
                "BB\t-850582456\n" +
                "BB\t-1707758909\n" +
                "BB\t-1912873112\n";

        assertQuery(
                "sym\tspread\n",
                "select sym, ask-bid spread from x where sym in ('AA', 'BB' ) order by sym, spread desc",
                "create table x (\n" +
                        "    sym symbol index,\n" +
                        "    bid int,\n" +
                        "    ask int,\n" +
                        "    ts timestamp\n" +
                        ") timestamp(ts) partition by DAY",
                null,
                "insert into x select * from (select rnd_symbol('AA', 'BB', 'CC') sym, \n" +
                        "        rnd_int() bid, \n" +
                        "        rnd_int() ask, \n" +
                        "        timestamp_sequence(172800000000, 360000000) ts \n" +
                        "    from long_sequence(10)) timestamp (ts)",
                expected,
                true
        );
    }
}
