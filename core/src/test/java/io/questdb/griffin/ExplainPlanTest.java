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


package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.analytic.RowNumberFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InCharFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampStrFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToRegClassFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToStrArrayFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.StringToStringArrayFunction;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.conditional.CoalesceFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.engine.functions.date.*;
import io.questdb.griffin.engine.functions.eq.EqIntStrCFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestSumXDoubleGroupByFunctionFactory;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ExplainPlanTest extends AbstractGriffinTest {

    protected final static Log LOG = LogFactory.getLog(ExplainPlanTest.class);

    @Test
    public void test2686LeftJoinDoesntMoveOtherInnerJoinPredicate() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                        "join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and b.age = 10 ",
                "VirtualRecord\n" +
                        "  functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]\n" +
                        "    SelectedRecord\n" +
                        "        Filter filter: a.ts>=dateadd('m',-1,b2.ts)\n" +
                        "            Cross Join\n" +
                        "                Filter filter: b.age=10\n" +
                        "                    Nested Loop Left Join\n" +
                        "                      filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)\n" +
                        "                        DataFrame\n" +
                        "                            Row forward scan\n" +
                        "                            Frame forward scan on: table_1\n" +
                        "                        DataFrame\n" +
                        "                            Row forward scan\n" +
                        "                            Frame forward scan on: table_2\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_2\n"));
    }

    @Test
    public void test2686LeftJoinDoesntMoveOtherLeftJoinPredicate() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                        "left join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and b.age = 10 ",
                "VirtualRecord\n" +
                        "  functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]\n" +
                        "    SelectedRecord\n" +
                        "        Nested Loop Left Join\n" +
                        "          filter: (a.ts>=dateadd('m',-1,b2.ts) and b.age=10)\n" +
                        "            Nested Loop Left Join\n" +
                        "              filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_2\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: table_2\n"));
    }

    @Test
    public void test2686LeftJoinDoesntMoveOtherTwoTableEqJoinPredicate() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                        "join table_2 as b2 on a.ts >= dateadd('m', -1, b2.ts) and a.age = b.age ",
                "VirtualRecord\n" +
                        "  functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]\n" +
                        "    SelectedRecord\n" +
                        "        Filter filter: a.ts>=dateadd('m',-1,b2.ts)\n" +
                        "            Cross Join\n" +
                        "                Filter filter: a.age=b.age\n" +
                        "                    Nested Loop Left Join\n" +
                        "                      filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)\n" +
                        "                        DataFrame\n" +
                        "                            Row forward scan\n" +
                        "                            Frame forward scan on: table_1\n" +
                        "                        DataFrame\n" +
                        "                            Row forward scan\n" +
                        "                            Frame forward scan on: table_2\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_2\n"));
    }

    @Test
    public void test2686LeftJoinDoesntPushJoinPredicateToLeftTable() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) and a.age = 10 ",
                "VirtualRecord\n" +
                        "  functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]\n" +
                        "    SelectedRecord\n" +
                        "        Nested Loop Left Join\n" +
                        "          filter: ((a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts) and a.age=10)\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: table_1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: table_2\n"));
    }

    @Test
    public void test2686LeftJoinDoesntPushJoinPredicateToRightTable() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts \n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) and b.age = 10 ",
                "SelectedRecord\n" +
                        "    Nested Loop Left Join\n" +
                        "      filter: ((a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts) and b.age=10)\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: table_1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: table_2\n"));
    }

    @Test
    public void test2686LeftJoinDoesntPushWherePredicateToRightTable() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)" +
                        "where b.age = 10 ",
                "VirtualRecord\n" +
                        "  functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]\n" +
                        "    SelectedRecord\n" +
                        "        Filter filter: b.age=10\n" +
                        "            Nested Loop Left Join\n" +
                        "              filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_2\n"));
    }

    @Test
    public void test2686LeftJoinPushesWherePredicateToLeftJoinCondition() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts) " +
                        "where a.age * b.age = 10",
                "SelectedRecord\n" +
                        "    Filter filter: a.age*b.age=10\n" +
                        "        Nested Loop Left Join\n" +
                        "          filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: table_1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: table_2\n"));
    }

    @Test
    public void test2686LeftJoinPushesWherePredicateToLeftTable() throws Exception {
        test2686Prepare();
        assertMemoryLeak(() -> assertPlan("select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)" +
                        "where a.age = 10 ",
                "VirtualRecord\n" +
                        "  functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]\n" +
                        "    SelectedRecord\n" +
                        "        Nested Loop Left Join\n" +
                        "          filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)\n" +
                        "            Async JIT Filter\n" +
                        "              filter: age=10\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: table_2\n"));
    }

    @Test
    public void testAnalytic0() throws Exception {
        assertPlan("create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select ts, str,  row_number() over (order by l), row_number() over (partition by l) from t",
                "CachedAnalytic\n" +
                        "  functions: [row_number(),row_number()]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n");
    }

    @Test
    public void testAnalytic1() throws Exception {
        assertPlan("create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select str, ts, l, 10, row_number() over ( partition by l order by ts) from t",
                "CachedAnalytic\n" +
                        "  functions: [row_number()]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [str,ts,l,10]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: t\n");
    }

    @Test
    public void testAnalytic2() throws Exception {
        assertPlan("create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select str, ts, l as l1, ts::long+l as tsum, row_number() over ( partition by l, ts order by str) from t",
                "CachedAnalytic\n" +
                        "  functions: [row_number()]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [str,ts,l1,ts::long+l1]\n" +
                        "        SelectedRecord\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: t\n");
    }

    @Test
    public void testAsOfJoin0() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a asof join b on ts where a.i = b.ts::int",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts::int\n" +
                            "        AsOf Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testAsOfJoin0a() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select ts, ts1, i, i1 from (select * from a asof join b on ts ) where i/10 = i1",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i/10=b.i\n" +
                            "        AsOf Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testAsOfJoin1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a asof join b on ts",
                    "SelectedRecord\n" +
                            "    AsOf Join Light\n" +
                            "      condition: b.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n");
        });
    }

    @Test
    public void testAsOfJoin2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a asof join (select * from b limit 10) on ts",
                    "SelectedRecord\n" +
                            "    AsOf Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testAsOfJoin3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a asof join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                    "SelectedRecord\n" +
                            "    AsOf Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        SelectedRecord\n" +
                            "            Sort light\n" +
                            "              keys: [ts, i]\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: b\n");
        });
    }

    @Test
    public void testAsOfJoin4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * " +
                            "from a " +
                            "asof join b on ts " +
                            "asof join a c on ts",
                    "SelectedRecord\n" +
                            "    AsOf Join Light\n" +
                            "      condition: c.ts=a.ts\n" +
                            "        AsOf Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test // where clause predicate can't be pushed to join clause because asof is and outer join
    public void testAsOfJoin5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * " +
                            "from a " +
                            "asof join b " +
                            "where a.i = b.i",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.i\n" +
                            "        AsOf Join\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testAsOfJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");
            try {
                compiler.setFullFatJoins(true);
                assertPlan("select * " +
                                "from a " +
                                "asof join b on a.i = b.i",
                        "SelectedRecord\n" +
                                "    AsOf Join\n" +
                                "      condition: b.i=a.i\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: a\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: b\n");
            } finally {
                compiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testAsOfJoinNoKey() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a asof join b",
                    "SelectedRecord\n" +
                            "    AsOf Join\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n");
        });
    }

    @Test
    public void testCastFloatToDouble() throws Exception {
        assertPlan("select rnd_float()::double ",
                "VirtualRecord\n" +
                        "  functions: [rnd_float()::double]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testCountOfColumnsVectorized() throws Exception {
        assertPlan("create table x " +
                        "(" +
                        " k int, " +
                        " i int, " +
                        " l long, " +
                        " f float, " +
                        " d double, " +
                        " dat date, " +
                        " ts timestamp " +
                        ")",
                "select k, count(1) c1, " +
                        "count(*) cstar, " +
                        "count(i) ci, " +
                        "count(l) cl, " +
                        "count(d) cd, " +
                        "count(dat) cdat, " +
                        "count(ts) cts " +
                        "from x",
                "GroupBy vectorized: true\n" +
                        "  keys: [k]\n" +
                        "  values: [count(*),count(*),count(i),count(l),count(d),count(dat),count(ts)]\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: x\n"
        );
    }

    @Test
    public void testCrossJoin0() throws Exception {
        assertPlan("create table a ( i int, s1 string, s2 string)",
                "select * from a cross join a b where length(a.s1) = length(b.s2)",
                "SelectedRecord\n" +
                        "    Filter filter: length(a.s1)=length(b.s2)\n" +
                        "        Cross Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testCrossJoin0Output() throws Exception {
        assertQuery("cnt\n9\n",
                "select count(*) cnt from a cross join a b where length(a.s1) = length(b.s2)",
                "create table a as (select x, 's' || x as s1, 's' || (x%3) as s2 from long_sequence(3))", null, false, true, true);
    }

    @Test
    public void testCrossJoin1() throws Exception {
        assertPlan("create table a ( i int)",
                "select * from a cross join a b",
                "SelectedRecord\n" +
                        "    Cross Join\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testCrossJoin2() throws Exception {
        assertPlan("create table a ( i int)",
                "select * from a cross join a b cross join a c",
                "SelectedRecord\n" +
                        "    Cross Join\n" +
                        "        Cross Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testCrossJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {"select * from t t1 cross join t t2 order by t1.ts",
                    "select * from (select * from t order by ts desc) t1 cross join t t2 order by t1.ts"};
            for (String query : queries) {
                assertPlan(query,
                        "SelectedRecord\n" +
                                "    Cross Join\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n");

                assertQuery("x\tts\tx1\tts1\n" +
                        "1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                        "1\t1970-01-01T00:00:00.000001Z\t2\t1970-01-01T00:00:00.000002Z\n" +
                        "2\t1970-01-01T00:00:00.000002Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                        "2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\n", query, "ts", false, true);
            }
        });
    }

    @Test
    public void testCrossJoinWithSort2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " +
                    "((select * from t order by ts desc) limit 10) t1 " +
                    "cross join t t2 " +
                    "order by t1.ts desc";

            assertPlan(query,
                    "SelectedRecord\n" +
                            "    Cross Join\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: t\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testCrossJoinWithSort3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x int, ts timestamp) timestamp(ts)");

            assertPlan("select * from " +
                            "((select * from t order by ts asc) limit 10) t1 " +
                            "cross join t t2 " +
                            "order by t1.ts asc",
                    "SelectedRecord\n" +
                            "    Cross Join\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testCrossJoinWithSort4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x int, ts timestamp) timestamp(ts)");

            assertPlan("select * from " +
                            "((select * from t order by ts asc) limit 10) t1 " +
                            "cross join t t2 " +
                            "order by t1.ts desc",
                    "Sort\n" +
                            "  keys: [ts desc]\n" +
                            "    SelectedRecord\n" +
                            "        Cross Join\n" +
                            "            Limit lo: 10\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: t\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t\n");
        });
    }

    @Test
    public void testCrossJoinWithSort5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x int, ts timestamp) timestamp(ts)");

            assertPlan("select * from " +
                            "((select * from t order by ts asc) limit 10) t1 " +
                            "cross join t t2 " +
                            "order by t1.ts asc",
                    "SelectedRecord\n" +
                            "    Cross Join\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: t\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testDistinctTsWithLimit1() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di order by 1 limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit2() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di order by 1 desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [ts desc]\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit3() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit4() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di limit -10",
                "Limit lo: -10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit5a() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where y = 5 limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter\n" +
                        "              filter: y=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit5b() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where y = 5 limit -10",
                "Limit lo: -10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter\n" +
                        "              filter: y=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit6a() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: abs(y)=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit6b() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit -10",
                "Limit lo: -10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: abs(y)=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctTsWithLimit7() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit 10, 20",
                "Limit lo: 10 hi: 20\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: abs(y)=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit1() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di order by 1 limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x]\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit2() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di order by 1 desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x desc]\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit3() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di limit 10",
                "Limit lo: 10\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit4() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di limit -10",
                "Limit lo: -10\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit5a() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di where y = 5 limit 10",
                "Limit lo: 10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter\n" +
                        "              filter: y=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit5b() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di where y = 5 limit -10",
                "Limit lo: -10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter\n" +
                        "              filter: y=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit6a() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit 10",
                "Limit lo: 10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: abs(y)=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit6b() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit -10",
                "Limit lo: -10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: abs(y)=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testDistinctWithLimit7() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit 10, 20",
                "Limit lo: 10 hi: 20\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: abs(y)=5\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n");
    }

    @Test
    public void testExcept() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a except select * from a",
                "Except\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testExceptAndSort1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts desc",
                    "Except\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testExceptAndSort2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts asc",
                    "Except\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testExceptAndSort3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts asc",
                    "Sort light\n" +
                            "  keys: [ts]\n" +
                            "    Except\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: a\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n");
        });
    }

    @Test
    public void testExceptAndSort4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts desc",
                    "Sort light\n" +
                            "  keys: [ts desc]\n" +
                            "    Except\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n");
        });
    }

    @Test
    public void testExplainCreateTable() throws Exception {
        assertSql("explain create table a ( l long, d double)",
                "QUERY PLAN\n" +
                        "Create table: a\n");
    }

    @Test
    public void testExplainCreateTableAsSelect() throws Exception {
        assertSql("explain create table a as (select x, 1 from long_sequence(10))",
                "QUERY PLAN\n" +
                        "Create table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [x,1]\n" +
                        "        long_sequence count: 10\n");
    }

    @Test
    public void testExplainDeferredSingleSymbolFilterDataFrame() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table tab \n" +
                    "(\n" +
                    "   id symbol index,\n" +
                    "   ts timestamp,\n" +
                    "   val double  \n" +
                    ") timestamp(ts);");
            compile("insert into tab values ( 'XXX', 0::timestamp, 1 );");

            assertPlan("  select\n" +
                            "   ts,\n" +
                            "    id, \n" +
                            "    last(val)\n" +
                            "  from tab\n" +
                            "  where id = 'XXX' \n" +
                            "  sample by 15m ALIGN to CALENDAR\n",
                    "SampleByFirstLast\n" +
                            "  keys: [ts, id]\n" +
                            "  values: [last(val)]\n" +
                            "    DeferredSingleSymbolFilterDataFrame\n" +
                            "        Index forward scan on: id\n" +
                            "          filter: id=1\n" +
                            "        Frame forward scan on: tab\n");

        });
    }

    @Test
    public void testExplainInsert() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( l long, d double)");
            assertSql("explain insert into a values (1, 2.0)",
                    "QUERY PLAN\n" +
                            "Insert into table: a\n");
        });
    }

    @Test
    public void testExplainInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( l long, d double)");
            assertSql("explain insert into a select x, 1 from long_sequence(10)",
                    "QUERY PLAN\n" +
                            "Insert into table: a\n" +
                            "    VirtualRecord\n" +
                            "      functions: [x,1]\n" +
                            "        long_sequence count: 10\n");
        });
    }

    @Test
    public void testExplainPlanWithEOLs1() throws Exception {
        assertPlan("create table a (s string)",
                "select * from a where s = '\b\f\n\r\t\\u0013'",
                "Async Filter\n" +
                        "  filter: s='\\b\\f\\n\\r\\t\\u0013'\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testExplainSelect() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( l long, d double)");
            assertSql("explain select * from a;",
                    "QUERY PLAN\n" +
                            "DataFrame\n" +
                            "    Row forward scan\n" +
                            "    Frame forward scan on: a\n");
        });
    }

    @Test
    public void testExplainSelectWithCte1() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "with b as (select * from a where i = 0)" +
                        "select * from a union all select * from b",
                "Union All\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Async JIT Filter\n" +
                        "      filter: i=0\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testExplainSelectWithCte2() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "with b as (select i from a order by s)" +
                        "select * from a join b on a.i = b.i",
                "SelectedRecord\n" +
                        "    Hash Join Light\n" +
                        "      condition: b.i=a.i\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            SelectedRecord\n" +
                        "                Sort light\n" +
                        "                  keys: [s]\n" +
                        "                    DataFrame\n" +
                        "                        Row forward scan\n" +
                        "                        Frame forward scan on: a\n");
    }

    @Test
    public void testExplainSelectWithCte3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( l long, d double)");
            assertSql("explain with b as (select * from a limit 10) select * from b;",
                    "QUERY PLAN\n" +
                            "Limit lo: 10\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: a\n");
        });
    }

    @Test
    public void testExplainUpdate1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( l long, d double)");
            assertSql("explain update a set l = 1, d=10.1;",
                    "QUERY PLAN\n" +
                            "Update table: a\n" +
                            "    VirtualRecord\n" +
                            "      functions: [1,10.1]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testExplainUpdate2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( l1 long, d1 double)");
            compile("create table b ( l2 long, d2 double)");
            assertSql("explain update a set l1 = 1, d1=d2 from b where l1=l2;",
                    "QUERY PLAN\n" +
                            "Update table: a\n" +
                            "    VirtualRecord\n" +
                            "      functions: [1,d1]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: l2=l1\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: a\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: b\n");
        });
    }

    @Test
    public void testExplainUpdateWithFilter() throws Exception {
        assertPlan("create table a ( l long, d double, ts timestamp) timestamp(ts)",
                "update a set l = 20, d = d+rnd_double() " +
                        "where d < 100.0d and ts > dateadd('d', -1, now());",
                "Update table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [20,d+rnd_double()]\n" +
                        "        Async Filter\n" +
                        "          filter: d<100.0\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Interval forward scan on: a\n" +
                        "                  intervals: [static=[0,9223372036854775807,281481419161601,4294967296] dynamic=[dateadd('d',-1,now())]]\n");
    }

    @Test
    public void testExplainWithJsonFormat1() throws Exception {
        assertQuery("QUERY PLAN\n" +
                "[\n" +
                "  {\n" +
                "    \"Plan\": {\n" +
                "        \"Node Type\": \"Count\",\n" +
                "        \"Plans\": [\n" +
                "        {\n" +
                "            \"Node Type\": \"long_sequence\",\n" +
                "            \"count\":  10\n" +
                "        } ]\n" +
                "    }\n" +
                "  }\n" +
                "]\n", "explain (format json) select count (*) from long_sequence(10)", null, null, false, true, true);
    }

    @Test
    public void testExplainWithJsonFormat2() throws Exception {
        compiler.setFullFatJoins(true);

        String expected = "QUERY PLAN\n" +
                "[\n" +
                "  {\n" +
                "    \"Plan\": {\n" +
                "        \"Node Type\": \"SelectedRecord\",\n" +
                "        \"Plans\": [\n" +
                "        {\n" +
                "            \"Node Type\": \"Filter\",\n" +
                "            \"filter\": \"0<a.l+b.l\",\n" +
                "            \"Plans\": [\n" +
                "            {\n" +
                "                \"Node Type\": \"Hash Join\",\n" +
                "                \"condition\": \"b.l=a.l\",\n" +
                "                \"Plans\": [\n" +
                "                {\n" +
                "                    \"Node Type\": \"DataFrame\",\n" +
                "                    \"Plans\": [\n" +
                "                    {\n" +
                "                        \"Node Type\": \"Row forward scan\"\n" +
                "                    },\n" +
                "                    {\n" +
                "                        \"Node Type\": \"Frame forward scan\",\n" +
                "                        \"on\": \"a\"\n" +
                "                    } ]\n" +
                "                },\n" +
                "                {\n" +
                "                    \"Node Type\": \"Hash\",\n" +
                "                    \"Plans\": [\n" +
                "                    {\n" +
                "                        \"Node Type\": \"Async JIT Filter\",\n" +
                "                        \"limit\": \"4\",\n" +
                "                        \"filter\": \"10<l\",\n" +
                "                        \"workers\": \"1\",\n" +
                "                        \"Plans\": [\n" +
                "                        {\n" +
                "                            \"Node Type\": \"DataFrame\",\n" +
                "                            \"Plans\": [\n" +
                "                            {\n" +
                "                                \"Node Type\": \"Row forward scan\"\n" +
                "                            },\n" +
                "                            {\n" +
                "                                \"Node Type\": \"Frame forward scan\",\n" +
                "                                \"on\": \"a\"\n" +
                "                            } ]\n" +
                "                        } ]\n" +
                "                } ]\n" +
                "            } ]\n" +
                "        } ]\n" +
                "    }\n" +
                "  }\n" +
                "]\n";

        if (!JitUtil.isJitSupported()) {
            expected = expected.replace("JIT ", "");
        }

        try {
            assertQuery(expected, "explain (format json) select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ",
                    "create table a ( l long)", null, false, true, true);
        } finally {
            compiler.setFullFatJoins(false);
        }
    }

    @Test
    public void testExplainWithJsonFormat3() throws Exception {
        assertQuery("QUERY PLAN\n" +
                        "[\n" +
                        "  {\n" +
                        "    \"Plan\": {\n" +
                        "        \"Node Type\": \"GroupBy\",\n" +
                        "        \"vectorized\":  false,\n" +
                        "        \"keys\": \"[d]\",\n" +
                        "        \"values\": \"[max(i)]\",\n" +
                        "        \"Plans\": [\n" +
                        "        {\n" +
                        "            \"Node Type\": \"Union\",\n" +
                        "            \"Plans\": [\n" +
                        "            {\n" +
                        "                \"Node Type\": \"DataFrame\",\n" +
                        "                \"Plans\": [\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Row forward scan\"\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Frame forward scan\",\n" +
                        "                    \"on\": \"a\"\n" +
                        "                } ]\n" +
                        "            },\n" +
                        "            {\n" +
                        "                \"Node Type\": \"DataFrame\",\n" +
                        "                \"Plans\": [\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Row forward scan\"\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Frame forward scan\",\n" +
                        "                    \"on\": \"a\"\n" +
                        "                } ]\n" +
                        "            } ]\n" +
                        "        } ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "]\n", "explain (format json) select d, max(i) from (select * from a union select * from a)",
                "create table a ( i int, d double)", null, false, true, true);
    }

    @Test
    public void testExplainWithJsonFormat4() throws Exception {
        assertCompile("create table taba (a1 int, a2 long)");
        assertCompile("create table tabb (b1 int, b2 long)");
        assertQuery("QUERY PLAN\n" +
                        "[\n" +
                        "  {\n" +
                        "    \"Plan\": {\n" +
                        "        \"Node Type\": \"SelectedRecord\",\n" +
                        "        \"Plans\": [\n" +
                        "        {\n" +
                        "            \"Node Type\": \"Nested Loop Left Join\",\n" +
                        "            \"filter\": \"(taba.a1=tabb.b1 or taba.a2=tabb.b2)\",\n" +
                        "            \"Plans\": [\n" +
                        "            {\n" +
                        "                \"Node Type\": \"DataFrame\",\n" +
                        "                \"Plans\": [\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Row forward scan\"\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Frame forward scan\",\n" +
                        "                    \"on\": \"taba\"\n" +
                        "                } ]\n" +
                        "            },\n" +
                        "            {\n" +
                        "                \"Node Type\": \"DataFrame\",\n" +
                        "                \"Plans\": [\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Row forward scan\"\n" +
                        "                },\n" +
                        "                {\n" +
                        "                    \"Node Type\": \"Frame forward scan\",\n" +
                        "                    \"on\": \"tabb\"\n" +
                        "                } ]\n" +
                        "            } ]\n" +
                        "        } ]\n" +
                        "    }\n" +
                        "  }\n" +
                        "]\n",
                " explain (format json) select * from taba left join tabb on a1=b1  or a2=b2", null, null, false, true, true);
    }

    @Test
    public void testExplainWithQueryInParentheses1() throws SqlException {
        assertPlan("(select 1)",
                "VirtualRecord\n" +
                        "  functions: [1]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testExplainWithQueryInParentheses2() throws Exception {
        assertPlan("create table x ( i int)",
                "(select * from x)",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: x\n");
    }

    @Test
    public void testExplainWithQueryInParentheses3() throws Exception {
        assertPlan("create table x ( i int)",
                "((select * from x))",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: x\n");
    }

    @Test
    public void testExplainWithQueryInParentheses4() throws Exception {
        assertPlan("create table x ( i int)",
                "((x))",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: x\n");
    }

    @Test
    public void testExplainWithQueryInParentheses5() throws Exception {
        assertPlan("CREATE TABLE trades (\n" +
                        "  symbol SYMBOL,\n" +
                        "  side SYMBOL,\n" +
                        "  price DOUBLE,\n" +
                        "  amount DOUBLE,\n" +
                        "  timestamp TIMESTAMP\n" +
                        ") timestamp (timestamp) PARTITION BY DAY",
                "((select last(timestamp) as x, last(price) as btcusd " +
                        "from trades " +
                        "where symbol = 'BTC-USD' " +
                        "and timestamp > dateadd('m', -30, now()) ) " +
                        "timestamp(x))",
                "SelectedRecord\n" +
                        "    GroupBy vectorized: false\n" +
                        "      values: [last(timestamp),last(price)]\n" +
                        "        Async JIT Filter\n" +
                        "          filter: symbol='BTC-USD'\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Interval forward scan on: trades\n" +
                        "                  intervals: [static=[0,9223372036854775807,281481419161601,4294967296] dynamic=[dateadd('m',-30,now())]]\n");
    }

    @Test
    public void testFullFatHashJoin0() throws Exception {
        compiler.setFullFatJoins(true);
        try {
            assertPlan("create table a ( l long)",
                    "select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ",
                    "SelectedRecord\n" +
                            "    Filter filter: 0<a.l+b.l\n" +
                            "        Hash Join\n" +
                            "          condition: b.l=a.l\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            Hash\n" +
                            "                Async JIT Filter\n" +
                            "                  limit: 4\n" +
                            "                  filter: 10<l\n" +
                            "                  workers: 1\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: a\n");
        } finally {
            compiler.setFullFatJoins(false);
        }
    }

    @Test
    public void testFullFatHashJoin1() throws Exception {
        compiler.setFullFatJoins(true);
        try {
            assertPlan("create table a ( l long)",
                    "select * from a join (select l from a limit 40) on l",
                    "SelectedRecord\n" +
                            "    Hash Join\n" +
                            "      condition: _xQdbA1.l=a.l\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Hash\n" +
                            "            Limit lo: 40\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: a\n");
        } finally {
            compiler.setFullFatJoins(false);
        }
    }

    @Test
    public void testFullFatHashJoin2() throws Exception {
        compiler.setFullFatJoins(true);
        try {
            assertPlan("create table a ( l long)",
                    "select * from a left join a a1 on l",
                    "SelectedRecord\n" +
                            "    Hash Outer Join\n" +
                            "      condition: a1.l=a.l\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n");
        } finally {
            compiler.setFullFatJoins(false);
        }
    }

    @Test
    public void testFunctions() throws Exception {
        assertMemoryLeak(() -> {//test table for show_columns
            compile("create table bbb( a int )");
        });

        final StringSink sink = new StringSink();

        IntObjHashMap<Function> constFuncs = new IntObjHashMap<>();
        constFuncs.put(ColumnType.BOOLEAN, BooleanConstant.TRUE);
        constFuncs.put(ColumnType.BYTE, new ByteConstant((byte) 1));
        constFuncs.put(ColumnType.SHORT, new ShortConstant((short) 2));
        constFuncs.put(ColumnType.CHAR, new CharConstant('a'));
        constFuncs.put(ColumnType.INT, new IntConstant(3));
        constFuncs.put(ColumnType.LONG, new LongConstant(4));
        constFuncs.put(ColumnType.DATE, new DateConstant(0));
        constFuncs.put(ColumnType.TIMESTAMP, new TimestampConstant(86400000000L));
        constFuncs.put(ColumnType.FLOAT, new FloatConstant(5f));
        constFuncs.put(ColumnType.DOUBLE, new DoubleConstant(6));
        constFuncs.put(ColumnType.STRING, new StrConstant("bbb"));
        constFuncs.put(ColumnType.SYMBOL, new SymbolConstant("symbol", 0));
        constFuncs.put(ColumnType.LONG256, new Long256Constant(0, 1, 2, 3));
        constFuncs.put(ColumnType.GEOBYTE, new GeoByteConstant((byte) 1, ColumnType.getGeoHashTypeWithBits(5)));
        constFuncs.put(ColumnType.GEOSHORT, new GeoShortConstant((short) 1, ColumnType.getGeoHashTypeWithBits(10)));
        constFuncs.put(ColumnType.GEOINT, new GeoIntConstant(1, ColumnType.getGeoHashTypeWithBits(20)));
        constFuncs.put(ColumnType.GEOLONG, new GeoLongConstant(1, ColumnType.getGeoHashTypeWithBits(35)));
        constFuncs.put(ColumnType.GEOHASH, new GeoShortConstant((short) 1, ColumnType.getGeoHashTypeWithBits(15)));
        constFuncs.put(ColumnType.BINARY, new NullBinConstant());
        constFuncs.put(ColumnType.LONG128, new Long128Constant(0, 1));
        constFuncs.put(ColumnType.UUID, new UuidConstant(0, 1));

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("bbb", ColumnType.INT));
        constFuncs.put(ColumnType.RECORD, new RecordColumn(0, metadata));

        GenericRecordMetadata cursorMetadata = new GenericRecordMetadata();
        cursorMetadata.add(new TableColumnMetadata("s", ColumnType.STRING));
        constFuncs.put(ColumnType.CURSOR, new CursorFunction(new EmptyTableRecordCursorFactory(cursorMetadata) {
            public boolean supportPageFrameCursor() {
                return true;
            }
        }));

        IntObjHashMap<Function> colFuncs = new IntObjHashMap<>();
        colFuncs.put(ColumnType.BOOLEAN, new BooleanColumn(1));
        colFuncs.put(ColumnType.BYTE, new ByteColumn(1));
        colFuncs.put(ColumnType.SHORT, new ShortColumn(2));
        colFuncs.put(ColumnType.CHAR, new CharColumn(1));
        colFuncs.put(ColumnType.INT, new IntColumn(1));
        colFuncs.put(ColumnType.LONG, new LongColumn(1));
        colFuncs.put(ColumnType.DATE, new DateColumn(1));
        colFuncs.put(ColumnType.TIMESTAMP, new TimestampColumn(1));
        colFuncs.put(ColumnType.FLOAT, new FloatColumn(1));
        colFuncs.put(ColumnType.DOUBLE, new DoubleColumn(1));
        colFuncs.put(ColumnType.STRING, new StrColumn(1));
        colFuncs.put(ColumnType.SYMBOL, new SymbolColumn(1, true));
        colFuncs.put(ColumnType.LONG256, new Long256Column(1));
        colFuncs.put(ColumnType.GEOBYTE, new GeoByteColumn(1, ColumnType.getGeoHashTypeWithBits(5)));
        colFuncs.put(ColumnType.GEOSHORT, new GeoShortColumn(1, ColumnType.getGeoHashTypeWithBits(10)));
        colFuncs.put(ColumnType.GEOINT, new GeoIntColumn(1, ColumnType.getGeoHashTypeWithBits(20)));
        colFuncs.put(ColumnType.GEOLONG, new GeoLongColumn(1, ColumnType.getGeoHashTypeWithBits(35)));
        colFuncs.put(ColumnType.GEOHASH, new GeoShortColumn((short) 1, ColumnType.getGeoHashTypeWithBits(15)));
        colFuncs.put(ColumnType.BINARY, new BinColumn(1));
        colFuncs.put(ColumnType.LONG128, new Long128Column(1));
        colFuncs.put(ColumnType.UUID, new UuidColumn(1));

        PlanSink planSink = new TextPlanSink() {
            @Override
            public PlanSink putColumnName(int columnIdx) {
                val("column(").val(columnIdx).val(")");
                return this;
            }
        };

        PlanSink tmpPlanSink = new TextPlanSink() {
            @Override
            public PlanSink putColumnName(int columnIdx) {
                val("column(").val(columnIdx).val(")");
                return this;
            }
        };

        ObjList<Function> args = new ObjList<>();
        IntList argPositions = new IntList();

        FunctionFactoryCache cache = compiler.getFunctionFactoryCache();
        LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = cache.getFactories();
        factories.forEach((key, value) -> {
            for (int i = 0, n = value.size(); i < n; i++) {
                planSink.clear();

                FunctionFactoryDescriptor descriptor = value.get(i);
                FunctionFactory factory = descriptor.getFactory();
                int sigArgCount = descriptor.getSigArgCount();

                sink.clear();
                sink.put(factory.getSignature()).put(" types: ");

                for (int p = 0; p < sigArgCount; p++) {
                    int sigArgTypeMask = descriptor.getArgTypeMask(p);
                    final short sigArgType = FunctionFactoryDescriptor.toType(sigArgTypeMask);
                    boolean isArray = FunctionFactoryDescriptor.isArray(sigArgTypeMask);

                    if (p > 0) {
                        sink.put(',');
                    }
                    sink.put(ColumnType.nameOf(sigArgType));
                    if (isArray) {
                        sink.put("[]");
                    }
                }
                sink.put(" -> ");

                int combinations = 1;

                for (int p = 0; p < sigArgCount; p++) {
                    boolean isConstant = FunctionFactoryDescriptor.isConstant(descriptor.getArgTypeMask(p));
                    if (!isConstant) {
                        combinations *= 2;
                    }
                }

                boolean goodArgsFound = false;
                for (int no = 0; no < combinations; no++) {
                    args.clear();
                    argPositions.clear();
                    planSink.clear();

                    int tempNo = no;

                    try {
                        for (int p = 0; p < sigArgCount; p++) {
                            int sigArgTypeMask = descriptor.getArgTypeMask(p);
                            short sigArgType = FunctionFactoryDescriptor.toType(sigArgTypeMask);
                            boolean isConstant = FunctionFactoryDescriptor.isConstant(sigArgTypeMask);
                            boolean isArray = FunctionFactoryDescriptor.isArray(sigArgTypeMask);
                            boolean useConst = isConstant || (tempNo & 1) == 1 || sigArgType == ColumnType.CURSOR || sigArgType == ColumnType.RECORD;
                            boolean isVarArg = sigArgType == ColumnType.VAR_ARG;

                            if (isVarArg) {
                                if (factory instanceof LongSequenceFunctionFactory) {
                                    sigArgType = ColumnType.LONG;
                                } else if (factory instanceof InCharFunctionFactory) {
                                    sigArgType = ColumnType.CHAR;
                                } else if (factory instanceof InTimestampTimestampFunctionFactory) {
                                    sigArgType = ColumnType.TIMESTAMP;
                                } else {
                                    sigArgType = ColumnType.STRING;
                                }
                            }

                            if (factory instanceof SwitchFunctionFactory) {
                                args.add(new IntConstant(1));
                                args.add(new IntConstant(2));
                                args.add(new StrConstant("a"));
                                args.add(new StrConstant("b"));
                            } else if (factory instanceof CoalesceFunctionFactory) {
                                args.add(new FloatColumn(1));
                                args.add(new FloatColumn(2));
                                args.add(new FloatConstant(12f));
                            } else if (factory instanceof ExtractFromTimestampFunctionFactory && sigArgType == ColumnType.STRING) {
                                args.add(new StrConstant("day"));
                            } else if (factory instanceof TimestampCeilFunctionFactory) {
                                args.add(new StrConstant("d"));
                            } else if (sigArgType == ColumnType.STRING && isArray) {
                                args.add(new StringToStringArrayFunction(0, "{'test'}"));
                            } else if (sigArgType == ColumnType.STRING && factory instanceof InTimestampStrFunctionFactory) {
                                args.add(new StrConstant("2022-12-12"));
                            } else if (factory instanceof ToTimezoneTimestampFunctionFactory && p == 1) {
                                args.add(new StrConstant("CET"));
                            } else if (factory instanceof CastStrToRegClassFunctionFactory && useConst) {
                                args.add(new StrConstant("pg_namespace"));
                            } else if (factory instanceof CastStrToStrArrayFunctionFactory) {
                                args.add(new StrConstant("{'abc'}"));
                            } else if (factory instanceof TestSumXDoubleGroupByFunctionFactory && p == 1) {
                                args.add(new StrConstant("123.456"));
                            } else if (factory instanceof TimestampFloorFunctionFactory && p == 0) {
                                args.add(new StrConstant("d"));
                            } else if (factory instanceof DateTruncFunctionFactory && p == 0) {
                                args.add(new StrConstant("year"));
                            } else if (factory instanceof ToUTCTimestampFunctionFactory && p == 1) {
                                args.add(new StrConstant("CEST"));
                            } else if (factory instanceof TimestampAddFunctionFactory && p == 0) {
                                args.add(new CharConstant('s'));
                            } else if (factory instanceof EqIntStrCFunctionFactory && sigArgType == ColumnType.STRING) {
                                args.add(new StrConstant("1"));
                            } else if (!useConst) {
                                args.add(colFuncs.get(sigArgType));
                            } else {
                                args.add(getConst(constFuncs, sigArgType, p));
                            }

                            if (!isConstant) {
                                tempNo >>= 1;
                            }
                        }

                        argPositions.setAll(args.size(), 0);

                        if (factory instanceof RowNumberFunctionFactory) {
                            sqlExecutionContext.configureAnalyticContext(null, null, null, true, true);
                        }

                        Function function = factory.newInstance(0, args, argPositions, engine.getConfiguration(), sqlExecutionContext);
                        function.toPlan(planSink);
                        goodArgsFound = true;

                        Assert.assertFalse("function " + factory.getSignature() + " should serialize to text properly. current text: " + planSink.getSink(), Chars.contains(planSink.getSink(), "io.questdb"));
                        LOG.info().$(sink).$(planSink.getSink()).$();

                        if (function instanceof NegatableBooleanFunction && !((NegatableBooleanFunction) function).isNegated()) {
                            ((NegatableBooleanFunction) function).setNegated();
                            tmpPlanSink.clear();
                            function.toPlan(tmpPlanSink);

                            if (Chars.equals(planSink.getSink(), tmpPlanSink.getSink())) {
                                throw new AssertionError("Same output generated regardless of negatable flag! Factory: " + factory.getSignature() + " " + function);
                            }

                            Assert.assertFalse("function " + factory.getSignature() + " should serialize to text properly",
                                    Chars.contains(tmpPlanSink.getSink(), "io.questdb"));
                        }
                    } catch (Exception t) {
                        LOG.info().$(t).$();
                    }
                }

                if (!goodArgsFound) {
                    throw new RuntimeException("No good set of values found for " + factory);
                }
            }
        });
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByBoolean() throws Exception {
        assertPlan("create table a ( l long, b boolean)",
                "select b, min(l)  from a group by b",
                "GroupBy vectorized: false\n" +
                        "  keys: [b]\n" +
                        "  values: [min(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByBooleanFunction() throws Exception {
        assertPlan("create table a ( l long, b1 boolean, b2 boolean)",
                "select b1||b2, min(l) from a group by b1||b2",
                "GroupBy vectorized: false\n" +
                        "  keys: [concat]\n" +
                        "  values: [min(l)]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [concat([b1,b2]),l]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByDouble() throws Exception {
        assertPlan("create table a ( l long, d double)",
                "select d, min(l) from a group by d",
                "GroupBy vectorized: false\n" +
                        "  keys: [d]\n" +
                        "  values: [min(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByFloat() throws Exception {
        assertPlan("create table a ( l long, f float)",
                "select f, min(l) from a group by f",
                "GroupBy vectorized: false\n" +
                        "  keys: [f]\n" +
                        "  values: [min(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test //special case
    public void testGroupByHour() throws Exception {
        assertPlan("create table a ( ts timestamp, d double)",
                "select hour(ts), min(d) from a group by hour(ts)",
                "GroupBy vectorized: true\n" +
                        "  keys: [ts]\n" +
                        "  values: [min(d)]\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByInt1() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select min(d), i from a group by i",
                "GroupBy vectorized: true\n" +
                        "  keys: [i]\n" +
                        "  values: [min(d)]\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // repeated int key disables vectorized impl
    public void testGroupByInt2() throws Exception {
        assertPlan("create table a ( i int, d double)", "select i, i, min(d) from a group by i, i",
                "GroupBy vectorized: false\n" +
                        "  keys: [i,i1]\n" +
                        "  values: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByKeyedAliased() throws Exception {
        assertPlan("create table a (s symbol, ts timestamp) timestamp(ts) partition by year;",
                "select s as symbol, count() from a",
                "GroupBy vectorized: true\n" +
                        "  keys: [symbol]\n" +
                        "  values: [count(*)]\n" +
                        "  workers: 1\n" +
                        "    SelectedRecord\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByKeyedNoAlias() throws Exception {
        assertPlan("create table a (s symbol, ts timestamp) timestamp(ts) partition by year;",
                "select s, count() from a",
                "GroupBy vectorized: true\n" +
                        "  keys: [s]\n" +
                        "  values: [count(*)]\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByKeyedOnExcept() throws Exception {
        assertCompile("create table a ( i int, d double)");

        assertPlan("create table b ( j int, e double)",
                "select d, max(i) from (select * from a except select * from b)",
                "GroupBy vectorized: false\n" +
                        "  keys: [d]\n" +
                        "  values: [max(i)]\n" +
                        "    Except\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testGroupByKeyedOnIntersect() throws Exception {
        assertCompile("create table a ( i int, d double)");

        assertPlan("create table b ( j int, e double)",
                "select d, max(i) from (select * from a intersect select * from b)",
                "GroupBy vectorized: false\n" +
                        "  keys: [d]\n" +
                        "  values: [max(i)]\n" +
                        "    Intersect\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testGroupByKeyedOnUnion() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select d, max(i) from (select * from a union select * from a)",
                "GroupBy vectorized: false\n" +
                        "  keys: [d]\n" +
                        "  values: [max(i)]\n" +
                        "    Union\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByKeyedOnUnionAll() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select d, max(i) from (select * from a union all select * from a)",
                "GroupBy vectorized: false\n" +
                        "  keys: [d]\n" +
                        "  values: [max(i)]\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByLong() throws Exception {
        assertPlan("create table a ( l long, d double)",
                "select l, min(d) from a group by l",
                "GroupBy vectorized: false\n" +
                        "  keys: [l]\n" +
                        "  values: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed1() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select min(d) from a",
                "GroupBy vectorized: true\n" +
                        "  values: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed10() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a join a b on i )",
                "GroupBy vectorized: false\n" +
                        "  values: [max(i)]\n" +
                        "    SelectedRecord\n" +
                        "        Hash Join Light\n" +
                        "          condition: b.i=a.i\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            Hash\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed11() throws Exception {
        assertPlan("create table a ( gb geohash(4b), gs geohash(12b), gi geohash(24b), gl geohash(40b))",
                "select first(gb), last(gb), first(gs), last(gs), first(gi), last(gi), first(gl), last(gl) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [first(gb),last(gb),first(gs),last(gs),first(gi),last(gi),first(gl),last(gl)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed2() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select min(d), max(d*d) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [min(d),max(d*d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed3() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select max(d+1) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [max(d+1)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed4() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select count(*), max(i), min(d) from a",
                "GroupBy vectorized: true\n" +
                        "  values: [count(*),max(i),min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // constant values used in aggregates disable vectorization
    public void testGroupByNotKeyed5() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select first(10), last(d), avg(10), min(10), max(10) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [first(10),last(d),avg(10),min(10),max(10)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // group by on filtered data is not vectorized
    public void testGroupByNotKeyed6() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from a where i < 10",
                "GroupBy vectorized: false\n" +
                        "  values: [max(i)]\n" +
                        "    Async JIT Filter\n" +
                        "      filter: i<10\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test // order by is ignored and grouped by - vectorized
    public void testGroupByNotKeyed7() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a order by d)",
                "GroupBy vectorized: true\n" +
                        "  values: [max(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // order by can't be ignored; group by is not vectorized
    public void testGroupByNotKeyed8() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a order by d limit 10)",
                "GroupBy vectorized: false\n" +
                        "  values: [max(i)]\n" +
                        "    Sort light lo: 10\n" +
                        "      keys: [d]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test // TODO: group by could be vectorized for union tables and result merged
    public void testGroupByNotKeyed9() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a union all select * from a)",
                "GroupBy vectorized: false\n" +
                        "  values: [max(i)]\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByWithLimit1() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di group by x order by 1 limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x]\n" +
                        "    GroupBy vectorized: true\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit2() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di group by x order by 1 desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x desc]\n" +
                        "    GroupBy vectorized: true\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit3() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di group by x limit 10",
                "Limit lo: 10\n" +
                        "    GroupBy vectorized: true\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit4() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di group by x limit -10",
                "Limit lo: -10\n" +
                        "    GroupBy vectorized: true\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit5a() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di where y = 5 group by x limit 10",
                "Limit lo: 10\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        Async JIT Filter\n" +
                        "          filter: y=5\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit5b() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di where y = 5 group by x limit -10",
                "Limit lo: -10\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        Async JIT Filter\n" +
                        "          filter: y=5\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit6a() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit 10",
                "Limit lo: 10\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        Async Filter\n" +
                        "          filter: abs(y)=5\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit6b() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit -10",
                "Limit lo: -10\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        Async Filter\n" +
                        "          filter: abs(y)=5\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit7() throws Exception {
        assertPlan("create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit 10, 20",
                "Limit lo: 10 hi: 20\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        Async Filter\n" +
                        "          filter: abs(y)=5\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testGroupByWithLimit8() throws Exception {
        assertPlan("create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select ts, count(*) from di where y=5 group by ts  order by ts desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [ts desc]\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [ts]\n" +
                        "      values: [count(*)]\n" +
                        "        Async JIT Filter\n" +
                        "          filter: y=5\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n");
    }

    @Test
    public void testHashInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s1 string)");
            compile("create table b ( i int, s2 string)");

            assertPlan("select s1, s2 from (select a.s1, b.s2, b.i, a.i  from a join b on i) where i < i1 and s1 = s2",
                    "SelectedRecord\n" +
                            "    Filter filter: (b.i<a.i and a.s1=b.s2)\n" +
                            "        Hash Join Light\n" +
                            "          condition: b.i=a.i\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: b\n");
        });
    }

    @Test
    public void testHashLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int)");
            compile("create table b ( i int)");

            assertPlan("select * from a left join b on i",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b.i=a.i\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testHashLeftJoin1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int)");
            compile("create table b ( i int)");

            assertPlan("select * from a left join b on i where b.i is not null",
                    "SelectedRecord\n" +
                            "    Filter filter: b.i!=null\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: b.i=a.i\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: b\n");
        });
    }

    @Ignore
    //FIXME
    //@Ignore("Fails with 'io.questdb.griffin.SqlException: [17] unexpected token: b'")
    @Test
    public void testImplicitJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i1 int)");
            compile("create table b ( i2 int)");

            assertQuery("", "select * from a , b where a.i1 = b.i2", null, null);

            assertPlan("select * from a , b where a.i1 = b.i2",
                    "SelectedRecord\n" +
                            "    Cross Join\n" +
                            "        Cross Join\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testInUuid() throws Exception {
        assertPlan("create table a (u uuid, ts timestamp) timestamp(ts);",
                "select u, ts from a where u in ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333')",
                "Async Filter\n" +
                        "  filter: u in ['22222222-2222-2222-2222-222222222222','11111111-1111-1111-1111-111111111111','33333333-3333-3333-3333-333333333333']\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testIntersect1() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a intersect select * from a",
                "Intersect\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testIntersect2() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a intersect select * from a where i > 0",
                "Intersect\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        Async JIT Filter\n" +
                        "          filter: 0<i\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testIntersectAndSort1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts desc",
                    "Intersect\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testIntersectAndSort2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts asc",
                    "Intersect\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testIntersectAndSort3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts asc",
                    "Sort light\n" +
                            "  keys: [ts]\n" +
                            "    Intersect\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: a\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n");
        });
    }

    @Test
    public void testIntersectAndSort4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts desc",
                    "Sort light\n" +
                            "  keys: [ts desc]\n" +
                            "    Intersect\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n");
        });
    }

    @Test
    public void testLatestOn0() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from a latest on ts partition by i",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn0a() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from (select * from a where i = 10 union select * from a where i =20) latest on ts partition by i",
                "SelectedRecord\n" +
                        "    LatestBy\n" +
                        "        Union\n" +
                        "            Async JIT Filter\n" +
                        "              filter: i=10\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "            Async JIT Filter\n" +
                        "              filter: i=20\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test
    public void testLatestOn0b() throws Exception {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select ts,i from a where s in ('ABC') and i > 0 latest on ts partition by s",
                "SelectedRecord\n" +
                        "    LatestByValueDeferredFiltered\n" +
                        "      filter: 0<i\n" +
                        "      symbolFilter: s='ABC'\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn0c() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertPlan("select ts,i from a where s in ('a1') and i > 0 latest on ts partition by s",
                    "SelectedRecord\n" +
                            "    LatestByValueFiltered\n" +
                            "        Row backward scan\n" +
                            "          symbolFilter: s=0\n" +
                            "          filter: 0<i\n" +
                            "        Frame backward scan on: a\n");
        });
    }

    @Test
    public void testLatestOn0d() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertPlan("select ts,i from a where s in ('a1') latest on ts partition by s",
                    "SelectedRecord\n" +
                            "    LatestByValueFiltered\n" +
                            "        Row backward scan\n" +
                            "          symbolFilter: s=0\n" +
                            "        Frame backward scan on: a\n");
        });
    }

    @Test
    public void testLatestOn0e() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertPlan("select ts,i, s from a where s in ('a1') and i > 0 latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  filter: 0<i\n" +
                            "  symbolFilter: s=1\n" +
                            "    Frame backward scan on: a\n");
        });
    }

    @Test
    public void testLatestOn1() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by i",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test // TODO: should use index
    public void testLatestOn10() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s = 'S1' or s = 'S2' latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  filter: (s='S1' or s='S2')\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn11() throws Exception {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in ('S1', 'S2') latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  includedSymbols: ['S1','S2']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test // TODO: subquery should just read symbols from map
    public void testLatestOn12() throws Exception {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupBy vectorized: true\n" +
                        "              keys: [s]\n" +
                        "              values: [count(*)]\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Row backward scan on: s\n" +
                        "      filter: length(s)=2\n" +
                        "    Frame backward scan on: a\n");

    }

    @Test // TODO: subquery should just read symbols from map
    public void testLatestOn12a() throws Exception {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupBy vectorized: true\n" +
                        "              keys: [s]\n" +
                        "              values: [count(*)]\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Row backward scan on: s\n" +
                        "    Frame backward scan on: a\n");

    }

    @Test // TODO: subquery should just read symbols from map
    public void testLatestOn13() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupBy vectorized: true\n" +
                        "              keys: [s]\n" +
                        "              values: [count(*)]\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Index backward scan on: s\n" +
                        "      filter: length(s)=2\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test // TODO: subquery should just read symbols from map
    public void testLatestOn13a() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupBy vectorized: true\n" +
                        "              keys: [s]\n" +
                        "              values: [count(*)]\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Index backward scan on: s\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test // TODO: should use one or two indexes
    public void testLatestOn14() throws Exception {
        assertPlan("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' and i > 0 latest on ts partition by s1,s2",
                "LatestByAllSymbolsFiltered\n" +
                        "  filter: ((s1 in [S1,S2] and s2='S3') and 0<i)\n" +
                        "    Row backward scan\n" +
                        "      expectedSymbolsCount: 2\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test // TODO: should use one or two indexes
    public void testLatestOn15() throws Exception {
        assertPlan("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' latest on ts partition by s1,s2",
                "LatestByAllSymbolsFiltered\n" +
                        "  filter: (s1 in [S1,S2] and s2='S3')\n" +
                        "    Row backward scan\n" +
                        "      expectedSymbolsCount: 2\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn16() throws Exception {
        assertPlan("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 = 'S1' and ts > 0::timestamp latest on ts partition by s1,s2",
                "LatestByAllSymbolsFiltered\n" +
                        "  filter: s1='S1'\n" +
                        "    Row backward scan\n" +
                        "      expectedSymbolsCount: 2147483647\n" +
                        "    Interval backward scan on: a\n" +
                        "      intervals: [static=[1,9223372036854775807]\n");
    }

    @Test
    public void testLatestOn1a() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from (select ts, i as i1, i as i2 from a ) where 0 < i1 and i2 < 10 latest on ts partition by i1",
                "LatestBy light order_by_timestamp: true\n" +
                        "    SelectedRecord\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter\n" +
                        "              filter: (0<i and i<10)\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test
    public void testLatestOn1b() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select ts, i as i1, i as i2 from a where 0 < i and i < 10 latest on ts partition by i",
                "SelectedRecord\n" +
                        "    SelectedRecord\n" +
                        "        LatestByAllFiltered\n" +
                        "            Row backward scan\n" +
                        "              filter: (0<i and i<10)\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn2() throws Exception {
        assertPlan("create table a ( i int, d double, ts timestamp) timestamp(ts);",
                "select ts, d from a latest on ts partition by i",
                "SelectedRecord\n" +
                        "    LatestByAllFiltered\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn3() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by s",
                "LatestByAllIndexed\n" +
                        "    Index backward scan on: s parallel: true\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn4() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  = 'S1' latest on ts partition by s",
                "DataFrame\n" +
                        "    Index backward scan on: s deferred: true\n" +
                        "      filter: s='S1'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn5a() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertPlan(
                    "select s, i, ts from a where s  in ('def1', 'def2') latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  symbolFilter: s in ['def1','def2']\n" +
                            "    Frame backward scan on: a\n");
        });
    }

    @Test
    public void testLatestOn5b() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertPlan(
                    "select s, i, ts from a where s  in ('1', 'deferred') latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  symbolFilter: s in [1] or s in ['deferred']\n" +
                            "    Frame backward scan on: a\n");
        });
    }

    @Test
    public void testLatestOn5c() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertPlan(
                    "select s, i, ts from a where s  in ('1', '2') latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  symbolFilter: s in [1,2]\n" +
                            "    Frame backward scan on: a\n");
        });
    }

    @Test
    public void testLatestOn6() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and i > 0 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: 0<i\n" +
                        "  symbolFilter: s in ['S1','S2']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn7() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and length(s)<10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)<10\n" +
                        "  symbolFilter: s in ['S1','S2']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn8() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            assertPlan("select s, i, ts from a where s  in ('s1') latest on ts partition by s",
                    "DataFrame\n" +
                            "    Index backward scan on: s\n" +
                            "      filter: s=1\n" +
                            "    Frame backward scan on: a\n");
        });
    }

    @Test // key outside list of symbols
    public void testLatestOn8a() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            assertPlan("select s, i, ts from a where s  in ('bogus_key') latest on ts partition by s",
                    "DataFrame\n" +
                            "    Index backward scan on: s deferred: true\n" +
                            "      filter: s='bogus_key'\n" +
                            "    Frame backward scan on: a\n");
        });
    }

    @Test // columns in order different to table's
    public void testLatestOn9() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)=10\n" +
                        "  symbolFilter: s='S1'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test // columns in table's order
    public void testLatestOn9a() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, s, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)=10\n" +
                        "  symbolFilter: s='S1'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn9b() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x::int, 'S' || x, x::timestamp from long_sequence(10)");

            assertPlan("select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  filter: length(s)=10\n" +
                            "  symbolFilter: s=1\n" +
                            "    Frame backward scan on: a\n");
        });
    }

    @Test
    public void testLeftJoinWithEquality1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a int)");
            compile("create table tabb (b int)");

            assertPlan("select * from taba left join tabb on a=b",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b=a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test
    public void testLeftJoinWithEquality2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  and a2=b2",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b2=a2 and b1=a1\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test // FIXME: there should be no separate filter
    public void testLeftJoinWithEquality3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  or a2=b2",
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: (taba.a1=tabb.b1 or taba.a2=tabb.b2)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tabb\n");
        });
    }

    @Test // FIXME: join and where clause filters should be separated
    public void testLeftJoinWithEquality4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  or a2=b2 where a1 > b2",
                    "SelectedRecord\n" +
                            "    Filter filter: tabb.b2<taba.a1\n" +
                            "        Nested Loop Left Join\n" +
                            "          filter: (taba.a1=tabb.b1 or taba.a2=tabb.b2)\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: taba\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test // FIXME: ORed predicates should be applied as filter in hash join
    public void testLeftJoinWithEquality5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba " +
                            "left join tabb on a1=b1 and (a2=b2+10 or a2=2*b2)",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b1=a1\n" +
                            "      filter: (taba.a2=tabb.b2+10 or taba.a2=2*tabb.b2)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    // left join conditions aren't transitive because left record + null right is produced if they fail
    // that means select * from a left join b on a.i = b.i and a.i=10 doesn't mean resulting records will have a.i = 10 !
    @Test
    public void testLeftJoinWithEquality6() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");
            compile("create table tabc (c1 int, c2 long)");

            assertPlan("select * from taba " +
                            "left join tabb on a1=b1 and a1=5 " +
                            "join tabc on a1=c1",
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: c1=a1\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: b1=a1\n" +
                            "          filter: taba.a1=5\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: taba\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tabb\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabc\n");
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2)",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b2=a2 and b1=a1\n" +
                            "      filter: abs(taba.a2+1)=abs(tabb.b2)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  and a2=b2 and a2+5 = b2+10",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b2=a2 and b1=a1\n" +
                            "      filter: taba.a2+5=tabb.b2+10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressions3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  and a2=b2 and a2+5 = b2+10 and 1=0",
                    "SelectedRecord\n" +
                            "    Hash Outer Join\n" +
                            "      condition: b2=a2 and b1=a1\n" +
                            "      filter: false\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            Empty table\n");
        });
    }

    // FIXME provably false predicate like x!=x in left join means we can skip join and return left + nulls or join with empty right table
    @Test
    public void testLeftJoinWithEqualityAndExpressions4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  and a2!=a2",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b1=a1\n" +
                            "      filter: taba.a2!=taba.a2\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test // FIXME: a2=a2 run as past of left join or be optimized away !
    public void testLeftJoinWithEqualityAndExpressions5() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  and a2=a2",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b1=a1\n" +
                            "      filter: taba.a2=taba.a2\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test // left join filter must remain intact !
    public void testLeftJoinWithEqualityAndExpressions6() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 string)");
            compile("create table tabb (b1 int, b2 string)");

            assertPlan("select * from taba " +
                            "left join tabb on a1=b1  and a2 ~ 'a.*' and b2 ~ '.*z'",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b1=a1\n" +
                            "      filter: (taba.a2 ~ a.* and tabb.b2 ~ .*z)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n");
        });
    }

    @Test // FIXME:  abs(a2+1) = abs(b2) should be applied as left join filter  !
    public void testLeftJoinWithEqualityAndExpressionsAhdWhere1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba " +
                            "left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2) " +
                            "where a1+10 < b1 - 10",
                    "SelectedRecord\n" +
                            "    Filter filter: taba.a1+10<tabb.b1-10\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: b2=a2 and b1=a1\n" +
                            "          filter: abs(taba.a2+1)=abs(tabb.b2)\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: taba\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tabb\n");
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressionsAhdWhere2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2) where a1=b1",
                    "SelectedRecord\n" +
                            "    Filter filter: taba.a1=tabb.b1\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: b2=a2 and b1=a1\n" +
                            "          filter: abs(taba.a2+1)=abs(tabb.b2)\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: taba\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tabb\n");
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressionsAhdWhere3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on a1=b1 and abs(a2+1) = abs(b2) where a2=b2",
                    "SelectedRecord\n" +
                            "    Filter filter: taba.a2=tabb.b2\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: b1=a1\n" +
                            "          filter: abs(taba.a2+1)=abs(tabb.b2)\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: taba\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tabb\n");
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testLeftJoinWithExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on abs(a2+1) = abs(b2)",
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: abs(taba.a2+1)=abs(tabb.b2)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tabb\n");
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testLeftJoinWithExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table taba (a1 int, a2 long)");
            compile("create table tabb (b1 int, b2 long)");

            assertPlan("select * from taba left join tabb on abs(a2+1) = abs(b2) or a2/2 = b2+1",
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: (abs(taba.a2+1)=abs(tabb.b2) or taba.a2/2=tabb.b2+1)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tabb\n");
        });
    }

    @Test
    public void testLtJoin0() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select ts1, ts2, i1, i2 from (select a.i as i1, a.ts as ts1, b.i as i2, b.ts as ts2 from a lt join b on ts) where ts1::long*i1<ts2::long*i2",
                    "SelectedRecord\n" +
                            "    Filter filter: a.ts::long*a.i<b.ts::long*b.i\n" +
                            "        Lt Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testLtJoin1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a lt join b on ts",
                    "SelectedRecord\n" +
                            "    Lt Join Light\n" +
                            "      condition: b.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n");
        });
    }

    @Test
    public void testLtJoin1a() throws Exception {
        // lt join guarantees that a.ts > b.ts [join cond is not an equality predicate]
        // CONCLUSION: a join b on X can't always be translated to a join b on a.X = b.X
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a lt join b on ts where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Lt Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testLtJoin1b() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a lt join b on ts where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Lt Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testLtJoin1c() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a lt join b where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Lt Join\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testLtJoin2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a lt join (select * from b limit 10) on ts",
                    "SelectedRecord\n" +
                            "    Lt Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testLtJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");
            try {
                compiler.setFullFatJoins(true);
                assertPlan("select * " +
                                "from a " +
                                "Lt Join b on a.i = b.i",
                        "SelectedRecord\n" +
                                "    Lt Join\n" +
                                "      condition: b.i=a.i\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: a\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: b\n");
            } finally {
                compiler.setFullFatJoins(false);
            }
        });
    }

    @Test
    public void testLtOfJoin3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a lt join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                    "SelectedRecord\n" +
                            "    Lt Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        SelectedRecord\n" +
                            "            Sort light\n" +
                            "              keys: [ts, i]\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: b\n");
        });
    }

    @Test
    public void testLtOfJoin4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * " +
                            "from a " +
                            "lt join b on ts " +
                            "lt join a c on ts",
                    "SelectedRecord\n" +
                            "    Lt Join Light\n" +
                            "      condition: c.ts=a.ts\n" +
                            "        Lt Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testMultiExcept() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a except select * from a except select * from a",
                "Except\n" +
                        "    Except\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testMultiIntersect() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a intersect select * from a intersect select * from a",
                "Intersect\n" +
                        "    Intersect\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testMultiUnion() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a union select * from a union select * from a",
                "Union\n" +
                        "    Union\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testMultiUnionAll() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a union all select * from a union all select * from a",
                "Union All\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testNestedLoopLeftJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {"select * from t t1 left join t t2 on t1.x*t2.x>0 order by t1.ts",
                    "select * from (select * from t order by ts desc) t1 left join t t2 on t1.x*t2.x>0 order by t1.ts"};
            for (String query : queries) {
                assertPlan(query,
                        "SelectedRecord\n" +
                                "    Nested Loop Left Join\n" +
                                "      filter: 0<t1.x*t2.x\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n");

                assertQuery("x\tts\tx1\tts1\n" +
                        "1\t1970-01-01T00:00:00.000001Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                        "1\t1970-01-01T00:00:00.000001Z\t2\t1970-01-01T00:00:00.000002Z\n" +
                        "2\t1970-01-01T00:00:00.000002Z\t1\t1970-01-01T00:00:00.000001Z\n" +
                        "2\t1970-01-01T00:00:00.000002Z\t2\t1970-01-01T00:00:00.000002Z\n", query, "ts", false, false);
            }
        });
    }

    @Test
    public void testNestedLoopLeftJoinWithSort2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " +
                    "((select * from t order by ts desc) limit 10) t1 " +
                    "left join t t2 on t1.x*t2.x > 0 " +
                    "order by t1.ts desc";

            assertPlan(query,
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: 0<t1.x*t2.x\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: t\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n");
        });
    }

    @Test
    public void testRewriteAggregateWithAddition() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( x int );");

            assertPlan("SELECT sum(x), sum(x+10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum+COUNT*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");

            assertPlan("SELECT sum(x), sum(10+x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*10+sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testRewriteAggregateWithAdditionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( x double );");

            assertPlan("SELECT sum(x), sum(x+10) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(x+10)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("SELECT sum(x), sum(10+x) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(10+x)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( x int );");

            assertPlan("SELECT sum(x), sum(x*10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");

            assertPlan("SELECT sum(x), sum(10*x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,10*sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationIsDisabledForNonIntegerColumnType() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( x double );");

            assertPlan("SELECT sum(x), sum(x*10) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(x*10)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("SELECT sum(x), sum(10*x) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(10*x)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationIsDisabledForNonIntegerConstantType() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( x double );");

            assertPlan("SELECT sum(x), sum(x*10.0) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(x*10.0)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("SELECT sum(x), sum(10.0*x) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(10.0*x)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testRewriteAggregateWithSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( x int );");

            assertPlan("SELECT sum(x), sum(x-10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum-COUNT*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");

            assertPlan("SELECT sum(x), sum(10-x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*10-sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testRewriteAggregateWithSubtractionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( x double );");

            assertPlan("SELECT sum(x), sum(x-10) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(x-10)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("SELECT sum(x), sum(10-x) FROM tab",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(x),sum(10-x)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");
        });
    }

    @Test
    public void testRewriteAggregates() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE hits\n" +
                    "(\n" +
                    "    EventTime timestamp,\n" +
                    "    ResolutionWidth int,\n" +
                    "    ResolutionHeight int\n" +
                    ") TIMESTAMP(EventTime) PARTITION BY DAY;");

            assertPlan("SELECT sum(resolutIONWidth), count(resolutionwIDTH), SUM(ResolutionWidth), sum(ResolutionWidth) + count(), SUM(ResolutionWidth+1),SUM(ResolutionWidth*2),sUM(ResolutionWidth), count()\n" +
                            "FROM hits",
                    "VirtualRecord\n" +
                            "  functions: [sum,count,sum,sum+count1,sum+count*1,sum*2,sum,count1]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(resolutIONWidth),count(resolutIONWidth),count(*)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n");
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h",
                "SampleBy\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillLinear() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(linear)",
                "SampleBy\n" +
                        "  fill: linear\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillNull() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(null)",
                "SampleBy\n" +
                        "  fill: null\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillPrevKeyed() throws Exception {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, first(i) from a sample by 1h fill(prev)",
                "SampleBy\n" +
                        "  fill: prev\n" +
                        "  keys: [s]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillPrevNotKeyed() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(prev)",
                "SampleByFillPrev\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillValueKeyed() throws Exception {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, first(i) from a sample by 1h fill(1)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  keys: [s]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillValueNotKeyed() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(1)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFirstLast() throws Exception {
        assertPlan("create table a ( l long, s symbol, sym symbol index, i int, ts timestamp) timestamp(ts) partition by day;",
                "select sym, first(i), last(s), first(l) " +
                        "from a " +
                        "where sym in ('S') " +
                        "and   ts > 0::timestamp and ts < 100::timestamp " +
                        "sample by 1h",
                "SampleByFirstLast\n" +
                        "  keys: [sym]\n" +
                        "  values: [first(i), last(s), first(l)]\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: sym deferred: true\n" +
                        "          filter: sym='S'\n" +
                        "        Interval forward scan on: a\n" +
                        "          intervals: [static=[1,99]\n");
    }

    @Test
    public void testSampleByKeyed0() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h",
                "SampleBy\n" +
                        "  keys: [l,i]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed1() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h",
                "SampleBy\n" +
                        "  keys: [l,i]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed2() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1h fill(null)",
                "SampleBy\n" +
                        "  fill: null\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed3() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1d fill(linear)",
                "SampleBy\n" +
                        "  fill: linear\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed4() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(1,2)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i),last(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed5() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(prev,prev)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i),last(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelect0() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: a\n");
    }

    @Test
    public void testSelectConcat() throws Exception {
        assertPlan("select concat('a', 'b', rnd_str('c', 'd', 'e'))",
                "VirtualRecord\n" +
                        "  functions: [concat(['a','b',rnd_str([c,d,e])])]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testSelectCount1() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select count(*) from a",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount10() throws Exception {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select 1 from a limit 1) ",
                "Count\n" +
                        "    Limit lo: 1\n" +
                        "        VirtualRecord\n" +
                        "          functions: [1]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test // TODO: should return count on first table instead
    public void testSelectCount11() throws Exception {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a lt join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Lt Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test // TODO: should return count on first table instead
    public void testSelectCount12() throws Exception {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a asof join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        AsOf Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test // TODO: should return count(first table)*count(second_table) instead
    public void testSelectCount13() throws Exception {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a cross join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Cross Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount14() throws Exception {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)",
                "select * from a where s = 'S1' order by ts desc ",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount2() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select count() from a",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // TODO: this should use Count factory same as queries above
    public void testSelectCount3() throws Exception {
        assertPlan("create table a ( i int, d double)",
                "select count(2) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [count(*)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount4() throws Exception {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from a where s = 'S1'",
                "Count\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount5() throws Exception {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union all select * from a) ",
                "Count\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount6() throws Exception {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union select * from a) ",
                "Count\n" +
                        "    Union\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount7() throws Exception {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select * from a intersect select * from a) ",
                "Count\n" +
                        "    Intersect\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount8() throws Exception {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from a where 1=0 ",
                "Count\n" +
                        "    Empty table\n");
    }

    @Test
    public void testSelectCount9() throws Exception {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from a where 1=1 ",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // TODO: should use symbol list
    public void testSelectCountDistinct1() throws Exception {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select count_distinct(s) from tab",
                "GroupBy vectorized: false\n" +
                        "  values: [count_distinct(s)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: should use symbol list
    public void testSelectCountDistinct2() throws Exception {
        assertPlan("create table tab ( s symbol index, ts timestamp);",
                "select count_distinct(s) from tab",
                "GroupBy vectorized: false\n" +
                        "  values: [count_distinct(s)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectCountDistinct3() throws Exception {
        assertPlan("create table tab ( s string, l long );",
                "select count_distinct(l) from tab",
                "GroupBy vectorized: false\n" +
                        "  values: [count_distinct(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectDesc() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc",
                "DataFrame\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testSelectDesc2() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) ;",
                "select * from a order by ts desc",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectDescMaterialized() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) ;",
                "select * from (select i, ts from a union all select 1, null ) order by ts desc",
                "Sort\n" +
                        "  keys: [ts desc]\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        VirtualRecord\n" +
                        "          functions: [1,null]\n" +
                        "            long_sequence count: 1\n");
    }

    @Test
    public void testSelectDistinct0() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct l, ts from tab",
                "DistinctTimeSeries\n" +
                        "  keys: l,ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Ignore
    @Test // FIXME: somehow only ts gets included, pg returns record type
    public void testSelectDistinct0a() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct (l, ts) from tab",
                "DistinctTimeSeries\n" +
                        "  keys: l,ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectDistinct1() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select distinct(l) from tab",
                "Distinct\n" +
                        "  keys: l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    // TODO: should scan symbols table (note: some symbols from the symbol table may be
    //  not present in the end table due to, say, UPDATE)
    public void testSelectDistinct2() throws Exception {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctKey\n" +
                        "    GroupBy vectorized: true\n" +
                        "      keys: [s]\n" +
                        "      values: [count(*)]\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n");
    }

    @Test // TODO: should scan symbols table
    public void testSelectDistinct3() throws Exception {
        assertPlan("create table tab ( s symbol index, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctKey\n" +
                        "    GroupBy vectorized: true\n" +
                        "      keys: [s]\n" +
                        "      values: [count(*)]\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectDistinct4() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select distinct ts, l  from tab",
                "Distinct\n" +
                        "  keys: ts,l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // there's no interval scan because sysdate is evaluated per-row
    public void testSelectDynamicTsInterval1() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > sysdate()",
                "Async Filter\n" +
                        "  filter: sysdate()<ts\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // there's no interval scan because systimestamp is evaluated per-row
    public void testSelectDynamicTsInterval2() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > systimestamp()",
                "Async Filter\n" +
                        "  filter: systimestamp()<ts\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectDynamicTsInterval3() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[0,9223372036854775807,281481419161601,4294967296] dynamic=[now()]]\n");
    }

    @Test
    public void testSelectDynamicTsInterval4() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > dateadd('d', -1, now()) and ts < now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[NaN,0,844422782648321,4294967296,0,9223372036854775807,281481419161601,4294967296] dynamic=[now(),dateadd('d',-1,now())]]\n");
    }

    @Test
    public void testSelectDynamicTsInterval5() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[0,9223372036854775807,281481419161601,4294967296,1640995200000001,9223372036854775807,2147483649,4294967296] dynamic=[now(),null]]\n");
    }

    @Test
    public void testSelectDynamicTsInterval6() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now() order by ts desc",
                "DataFrame\n" +
                        "    Row backward scan\n" +
                        "    Interval backward scan on: tab\n" +
                        "      intervals: [static=[0,9223372036854775807,281481419161601,4294967296,1640995200000001,9223372036854775807,2147483649,4294967296] dynamic=[now(),null]]\n");
    }

    @Test
    public void testSelectFromAllTables() throws Exception {
        assertPlan("select * from all_tables()",
                "all_tables\n");
    }

    @Test
    public void testSelectFromMemoryMetrics() throws Exception {
        assertPlan("select * from memory_metrics()",
                "memory_metrics\n");
    }

    @Test
    public void testSelectFromReaderPool() throws Exception {
        assertPlan("select * from reader_pool()",
                "reader_pool\n");
    }

    @Test
    public void testSelectFromTableColumns() throws Exception {
        assertPlan("create table tab ( s string, sy symbol, i int, ts timestamp)",
                "select * from table_columns('tab')",
                "show_columns of: tab\n");
    }

    @Test
    public void testSelectFromTableWriterMetrics() throws Exception {
        assertPlan("select * from table_writer_metrics()",
                "table_writer_metrics\n");
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLo() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' order by ts desc limit 1 ",
                "Sort light lo: 1\n" +
                        "  keys: [ts desc]\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols1() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in ('S1', 'S2') order by s desc limit 1",
                "Sort light lo: 1\n" +
                        "  keys: [s desc]\n" +
                        "    FilterOnValues\n" +
                        "        Cursor-order scan\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S1'\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S2'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // TODO: having multiple cursors on the same level isn't very clear
    public void testSelectIndexedSymbols10() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in ('S1', 'S2') limit 1",
                "Limit lo: 1\n" +
                        "    FilterOnValues\n" +
                        "        Table-order scan\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S1'\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S2'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // TODO: having multiple cursors on the same level isn't very clear
    public void testSelectIndexedSymbols11() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( s symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");

            assertPlan("select * from a where s in ('S1', 'S2') and length(s) = 2 limit 1",
                    "Limit lo: 1\n" +
                            "    FilterOnValues\n" +
                            "        Table-order scan\n" +
                            "            Index forward scan on: s\n" +
                            "              filter: s=1 and length(s)=2\n" +
                            "            Index forward scan on: s\n" +
                            "              filter: s=2 and length(s)=2\n" +
                            "        Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSelectIndexedSymbols12() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan("select * from a where s1 in ('S1', 'S2') and s2 in ('S2') limit 1",
                    "Limit lo: 1\n" +
                            "    DataFrame\n" +
                            "        Index forward scan on: s2\n" +
                            "          filter: s2=2 and s1 in [S1,S2]\n" +
                            "        Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSelectIndexedSymbols13() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan("select * from a where s1 in ('S1')  order by ts desc",
                    "Sort light\n" +
                            "  keys: [ts desc]\n" +
                            "    DeferredSingleSymbolFilterDataFrame\n" +
                            "        Index forward scan on: s1\n" +
                            "          filter: s1=1\n" +
                            "        Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSelectIndexedSymbols14() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan("select * from a where s1 = 'S1'  order by ts desc",
                    "Sort light\n" +
                            "  keys: [ts desc]\n" +
                            "    DeferredSingleSymbolFilterDataFrame\n" +
                            "        Index forward scan on: s1\n" +
                            "          filter: s1=1\n" +
                            "        Frame forward scan on: a\n");
        });
    }

    @Test // backward index scan is triggered only if query uses a single partition and orders by key column and ts desc
    public void testSelectIndexedSymbols15() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan("select * from a " +
                            "where s1 = 'S1' " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc",
                    "DeferredSingleSymbolFilterDataFrame\n" +
                            "    Index backward scan on: s1\n" +
                            "      filter: s1=1\n" +
                            "    Interval forward scan on: a\n" +
                            "      intervals: [static=[1,8]\n");
        });
    }

    @Test
    public void testSelectIndexedSymbols16() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan("select * from a " +
                            "where s1 in ('S1', 'S2') " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc",
                    "FilterOnValues\n" +
                            "    Cursor-order scan\n" +
                            "        Index backward scan on: s1\n" +
                            "          filter: s1=1\n" +
                            "        Index backward scan on: s1\n" +
                            "          filter: s1=2\n" +
                            "    Interval forward scan on: a\n" +
                            "      intervals: [static=[1,8]\n");
        });
    }

    @Test // TODO: should use the same plan as above
    public void testSelectIndexedSymbols17() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan("select * from a " +
                            "where (s1 = 'S1' or s1 = 'S2') " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc",
                    "Sort light\n" +
                            "  keys: [s1, ts desc]\n" +
                            "    Async JIT Filter\n" +
                            "      filter: (s1='S1' or s1='S2')\n" +
                            "      workers: 1\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: a\n" +
                            "              intervals: [static=[1,8]\n");
        });
    }

    @Test
    public void testSelectIndexedSymbols1a() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select ts, s from a where s in ('S1', 'S2') and length(s) = 2 order by s desc limit 1",
                "Sort light lo: 1\n" +
                        "  keys: [s desc]\n" +
                        "    FilterOnValues\n" +
                        "        Cursor-order scan\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              symbolFilter: s='S1'\n" +
                        "              filter: length(s)=2\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              symbolFilter: s='S2'\n" +
                        "              filter: length(s)=2\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testSelectIndexedSymbols2() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = $1 or s = $2 order by ts desc limit 1",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: (s=$0::string or s=$1::string)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testSelectIndexedSymbols3() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' or s = 'S2' order by ts desc limit 1",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: (s='S1' or s='S2')\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test // TODO: it would be better to get rid of unnecessary sort and limit factories
    public void testSelectIndexedSymbols4() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' and s = 'S2' order by ts desc limit 1",
                "Limit lo: 1\n" +
                        "    Sort\n" +
                        "      keys: [ts desc]\n" +
                        "        Empty table\n");
    }

    @Test
    public void testSelectIndexedSymbols5() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in (select 'S1' union all select 'S2') order by ts desc limit 1",
                "Sort light lo: 1\n" +
                        "  keys: [ts desc]\n" +
                        "    FilterOnSubQuery\n" +
                        "        Union All\n" +
                        "            VirtualRecord\n" +
                        "              functions: ['S1']\n" +
                        "                long_sequence count: 1\n" +
                        "            VirtualRecord\n" +
                        "              functions: ['S2']\n" +
                        "                long_sequence count: 1\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols5a() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in (select 'S1' union all select 'S2') and length(s) = 2 order by ts desc limit 1",
                "Sort light lo: 1\n" +
                        "  keys: [ts desc]\n" +
                        "    FilterOnSubQuery\n" +
                        "      filter: length(s)=2\n" +
                        "        Union All\n" +
                        "            VirtualRecord\n" +
                        "              functions: ['S1']\n" +
                        "                long_sequence count: 1\n" +
                        "            VirtualRecord\n" +
                        "              functions: ['S2']\n" +
                        "                long_sequence count: 1\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test // TODO: this one should scan index/data file backward and skip sorting
    public void testSelectIndexedSymbols6() throws Exception {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s = 'S1' order by s asc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [s]\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols7() throws Exception {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s != 'S1' and length(s) = 2 order by s ",
                "Sort light\n" +
                        "  keys: [s]\n" +
                        "    FilterOnExcludedValues\n" +
                        "      symbolFilter: s not in ['S1']\n" +
                        "      filter: length(s)=2\n" +
                        "        Cursor-order scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols7b() throws Exception {
        assertPlan("create table a ( ts timestamp, s symbol index) timestamp(ts);",
                "select s from a where s != 'S1' and length(s) = 2 order by s ",
                "Sort light\n" +
                        "  keys: [s]\n" +
                        "    FilterOnExcludedValues\n" +
                        "      symbolFilter: s not in ['S1']\n" +
                        "      filter: length(s)=2\n" +
                        "        Cursor-order scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols8() throws Exception {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s != 'S1' order by s ",
                "Sort light\n" +
                        "  keys: [s]\n" +
                        "    FilterOnExcludedValues\n" +
                        "      symbolFilter: s not in ['S1']\n" +
                        "        Cursor-order scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols9() throws Exception {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by year ;",
                "select * from a where ts >= 0::timestamp and ts < 100::timestamp order by s asc",
                "SortedSymbolIndex\n" +
                        "    Index forward scan on: s\n" +
                        "      symbolOrder: asc\n" +
                        "    Interval forward scan on: a\n" +
                        "      intervals: [static=[0,99]\n");
    }

    @Test
    public void testSelectNoOrderByWithNegativeLimit() throws Exception {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select * from a limit -5",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsAsc() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts asc",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsAscAndDesc() throws Exception {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select * from (select * from a order by ts asc limit 5) order by ts desc",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsDescAndAsc() throws Exception {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select * from (select * from a order by ts desc limit 5) order by ts asc",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsDescLargeNegativeLimit1() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit 9223372036854775806L+3L ",
                "Limit lo: -9223372036854775807L\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsDescLargeNegativeLimit2() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -1000000 ",
                "Limit lo: -1000000\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsDescNegativeLimit() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsWithNegativeLimit() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts)",
                "select * from a order by ts  limit -5",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test
    public void testSelectOrderedAsc() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i asc",
                "Sort light\n" +
                        "  keys: [i]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderedDesc() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i desc",
                "Sort light\n" +
                        "  keys: [i desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderedWithLimitLoHi() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i limit 10, 100",
                "Sort light lo: 10 hi: 100\n" +
                        "  keys: [i]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectRandomBoolean() throws Exception {
        assertPlan("select rnd_boolean()",
                "VirtualRecord\n" +
                        "  functions: [rnd_boolean()]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testSelectStaticTsInterval1() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2020-03-01'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1583020800000001,9223372036854775807]\n");
    }

    @Test
    public void testSelectStaticTsInterval10() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc ",
                "Sort light\n" +
                        "  keys: [l desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test
    public void testSelectStaticTsInterval10a() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc, ts desc ",
                "Sort light\n" +
                        "  keys: [l desc, ts desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test
    public void testSelectStaticTsInterval2() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1583020800000000,1583107199999999]\n");
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval3() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' or ts in '2020-03-10'",
                "Async Filter\n" +
                        "  filter: (ts in [1583020800000000,1583107199999999] or ts in [1583798400000000,1583884799999999])\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // ranges don't overlap so result is empty
    public void testSelectStaticTsInterval4() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' and ts in '2020-03-10'",
                "Empty table\n");
    }

    @Test // only 2020-03-10->2020-03-31 needs to be scanned
    public void testSelectStaticTsInterval5() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03' and ts > '2020-03-10'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1583798400000001,1585699199999999]\n");
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval6() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts > '2020-03-01' and ts < '2020-03-10') or (ts > '2020-04-01' and ts < '2020-04-10') ",
                "Async Filter\n" +
                        "  filter: ((1583020800000000<ts and ts<1583798400000000) or (1585699200000000<ts and ts<1586476800000000))\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval7() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts between '2020-03-01' and '2020-03-10') or (ts between '2020-04-01' and '2020-04-10') ",
                "Async Filter\n" +
                        "  filter: (ts between 1583020800000000 and 1583798400000000 or ts between 1585699200000000 and 1586476800000000)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectStaticTsInterval8() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' ",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test
    public void testSelectStaticTsInterval9() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by ts desc",
                "DataFrame\n" +
                        "    Row backward scan\n" +
                        "    Interval backward scan on: tab\n" +
                        "      intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test
    public void testSelectStaticTsIntervalOnTabWithoutDesignatedTimestamp() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where ts > '2020-03-01'",
                "Async Filter\n" +
                        "  filter: 1583020800000000<ts\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWhereOrderByLimit() throws Exception {
        assertPlan("create table xx ( x long, str string) ",
                "select * from xx where str = 'A' order by str,x limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [str, x]\n" +
                        "    Async Filter\n" +
                        "      filter: str='A'\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: xx\n");
    }

    @Test
    public void testSelectWithJittedFilter1() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 ",
                "Async JIT Filter\n" +
                        "  filter: 100<l\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: this one should use jit
    public void testSelectWithJittedFilter10() throws Exception {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where s in ( 'A', 'B' )",
                "Async Filter\n" +
                        "  filter: s in [A,B]\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: this one should interval scan without filter
    public void testSelectWithJittedFilter11() throws Exception {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-02' )",
                "Async Filter\n" +
                        "  filter: ts in [1577836800000000,1577923200000000]\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter12() throws Exception {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-03' ) and s = 'ABC'",
                "Async Filter\n" +
                        "  filter: (ts in [1577836800000000,1578009600000000] and s='ABC')\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter13() throws Exception {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01' ) and s = 'ABC'",
                "Async Filter\n" +
                        "  filter: (ts in [1577836800000000,1577923199999999] and s='ABC')\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter14() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12 or l = 15 ",
                "Async JIT Filter\n" +
                        "  filter: (l=12 or l=15)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter15() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12.345 ",
                "Async JIT Filter\n" +
                        "  filter: l=12.345\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter16() throws Exception {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = false ",
                "Async JIT Filter\n" +
                        "  filter: b=false\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter17() throws Exception {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where not(b = false or ts = 123) ",
                "Async JIT Filter\n" +
                        "  filter: (b!=false and ts!=123)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter18() throws Exception {
        assertPlan("create table tab ( l1 long, l2 long);",
                "select * from tab where l1 < l2 ",
                "Async JIT Filter\n" +
                        "  filter: l1<l2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter19() throws Exception {
        assertPlan("create table tab ( l1 long, l2 long);",
                "select * from tab where l1 * l2 > 0  ",
                "Async JIT Filter\n" +
                        "  filter: 0<l1*l2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter2() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 ",
                "Async JIT Filter\n" +
                        "  filter: (100<l and l<1000)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter20() throws Exception {
        assertPlan("create table tab ( l1 long, l2 long, l3 long);",
                "select * from tab where l1 * l2 > l3  ",
                "Async JIT Filter\n" +
                        "  filter: l3<l1*l2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter21() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1 ",
                "Async JIT Filter\n" +
                        "  filter: l=$0::long\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter22() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1 + 1 ",
                "Async JIT Filter\n" +
                        "  filter: d=1024.1+1\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter23() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = null ",
                "Async JIT Filter\n" +
                        "  filter: d is null\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24a() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit 1 ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24b() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24b2() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit -1 ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24c() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24d() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit -1 ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24e() throws Exception {
        bindVariableService.setInt("maxRows", -1);

        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit :maxRows ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter25() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter26() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 ",
                "Async JIT Filter\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test // TODO: this one should use jit !
    public void testSelectWithJittedFilter3() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and ts = '2022-01-01' ",
                "Async Filter\n" +
                        "  filter: ((100<l and l<1000) and ts=1640995200000000)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter4() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and l = 20",
                "Async JIT Filter\n" +
                        "  filter: ((100<l and l<1000) and l=20)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter5() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or l = 20",
                "Async JIT Filter\n" +
                        "  filter: ((100<l and l<1000) or l=20)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter6() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or ts = 123",
                "Async JIT Filter\n" +
                        "  filter: ((100<l and l<1000) or ts=123)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: this one should use jit
    public void testSelectWithJittedFilter7() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 or ts > '2021-01-01'",
                "Async Filter\n" +
                        "  filter: ((100<l and l<1000) or 1609459200000000<ts)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter8() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 and ts in '2021-01-01'",
                "Async JIT Filter\n" +
                        "  filter: (100<l and l<1000)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [static=[1609459200000000,1609545599999999]\n");
    }

    @Test // TODO: this one should use jit
    public void testSelectWithJittedFilter9() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l in ( 100, 200 )",
                "Async Filter\n" +
                        "  filter: l in [100,200]\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithLimitLo() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10",
                "Limit lo: 10\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLoHi() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10, 100",
                "Limit lo: 10 hi: 100\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLoHiNegative() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10, -100",
                "Limit lo: -10 hi: -100\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLoNegative() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter1() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::short ",
                "Async Filter\n" +
                        "  filter: l=12::short\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit filter doesn't work with type casts
    public void testSelectWithNonJittedFilter10() throws Exception {
        assertPlan("create table tab ( s short, ts timestamp);",
                "select * from tab where s = 1::short ",
                "Async Filter\n" +
                        "  filter: s=1::short\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: should run with jitted filter just like b = true
    public void testSelectWithNonJittedFilter11() throws Exception {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = true::boolean ",
                "Async Filter\n" +
                        "  filter: b=true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: should run with jitted filter just like l = 1024
    public void testSelectWithNonJittedFilter12() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 1024::long ",
                "Async Filter\n" +
                        "  filter: l=1024::long\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: should run with jitted filter just like d = 1024.1
    public void testSelectWithNonJittedFilter13() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1::double ",
                "Async Filter\n" +
                        "  filter: d=1024.1\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // TODO: should run with jitted filter just like d = null
    public void testSelectWithNonJittedFilter14() throws Exception {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = null::double ",
                "Async Filter\n" +
                        "  filter: d is null\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter15() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l | l) > 0  ",
                "Async Filter\n" +
                        "  filter: 0<l|l\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter16() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l & l) > 0  ",
                "Async Filter\n" +
                        "  filter: 0<l&l\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter17() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0  ",
                "Async Filter\n" +
                        "  filter: 0<l^l\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithNonJittedFilter18() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0 limit -1",
                "Async Filter\n" +
                        "  limit: 1\n" +
                        "  filter: 0<l^l\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithNonJittedFilter19() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong("maxRows", -1);

        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0 limit :maxRows",
                "Async Filter\n" +
                        "  limit: 1\n" +
                        "  filter: 0<l^l\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n");
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter2() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::byte ",
                "Async Filter\n" +
                        "  filter: l=12::byte\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter3() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = '123' ",
                "Async Filter\n" +
                        "  filter: l='123'\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit is not because rnd_long() value is not stable
    public void testSelectWithNonJittedFilter4() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = rnd_long() ",
                "Async Filter\n" +
                        "  filter: l=rnd_long()\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithNonJittedFilter5() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = case when l > 0 then 1 when l = 0 then 0 else -1 end ",
                "Async Filter\n" +
                        "  filter: l=case([0<l,1,l=0,0,-1])\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // interval scan is not used because of type mismatch
    public void testSelectWithNonJittedFilter6() throws Exception {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1::string ",
                "Async Filter\n" +
                        "  filter: l=$0::double::string\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit filter doesn't work for string type
    public void testSelectWithNonJittedFilter7() throws Exception {
        assertPlan("create table tab ( s string, ts timestamp);",
                "select * from tab where s = 'test' ",
                "Async Filter\n" +
                        "  filter: s='test'\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit filter doesn't work for string type
    public void testSelectWithNonJittedFilter8() throws Exception {
        assertPlan("create table tab ( s string, ts timestamp);",
                "select * from tab where s = null ",
                "Async Filter\n" +
                        "  filter: s is null\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test // jit filter doesn't work with type casts
    public void testSelectWithNonJittedFilter9() throws Exception {
        assertPlan("create table tab ( b byte, ts timestamp);",
                "select * from tab where b = 1::byte ",
                "Async Filter\n" +
                        "  filter: b=1::byte\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithNotOperator() throws Exception {
        assertPlan("CREATE TABLE tst ( timestamp TIMESTAMP );",
                "select * from tst where timestamp not between '2021-01-01' and '2021-01-10' ",
                "Async Filter\n" +
                        "  filter: not (timestamp between 1609459200000000 and 1610236800000000)\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tst\n");
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLo() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit 10",
                "Limit lo: 10\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLoNegative1() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLoNegative2() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select i from a order by ts desc limit -10",
                "SelectedRecord\n" +
                        "    Sort light\n" +
                        "      keys: [ts desc]\n" +
                        "        Limit lo: 10\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithOrderByTsLimitLoNegative1() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts limit -10",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test
    public void testSelectWithOrderByTsLimitLoNegative2() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select i from a order by ts limit -10",
                "SelectedRecord\n" +
                        "    Sort light\n" +
                        "      keys: [ts]\n" +
                        "        Limit lo: 10\n" +
                        "            DataFrame\n" +
                        "                Row backward scan\n" +
                        "                Frame backward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder1() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l<i",
                "Async JIT Filter\n" +
                        "  filter: l<i\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder2() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l::short<i",
                "Async Filter\n" +
                        "  filter: l::short<i\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder2a() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select i2, i1, ts1 from " +
                        "(select ts as ts1, l as l1, i as i1, i as i2 " +
                        "from a " +
                        "where l::short<i " +
                        "limit 100) " +
                        "where l1*i2 != 0",
                "SelectedRecord\n" +
                        "    Filter filter: l1*i2!=0\n" +
                        "        SelectedRecord\n" +
                        "            SelectedRecord\n" +
                        "                Async Filter\n" +
                        "                  limit: 100\n" +
                        "                  filter: l::short<i\n" +
                        "                  workers: 1\n" +
                        "                    DataFrame\n" +
                        "                        Row forward scan\n" +
                        "                        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder2b() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select i2, i1, ts1 from " +
                        "(select ts as ts1, l as l1, i as i1, i as i2 " +
                        "from a " +
                        "order by ts, l1 " +
                        "limit 100 ) " +
                        "where i1*i2 != 0",
                "SelectedRecord\n" +
                        "    Filter filter: i1*i2!=0\n" +
                        "        Sort light lo: 100\n" +
                        "          keys: [ts1, l1]\n" +
                        "            SelectedRecord\n" +
                        "                SelectedRecord\n" +
                        "                    DataFrame\n" +
                        "                        Row forward scan\n" +
                        "                        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder3() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select k, max(ts) from ( select ts, l as k, i from a where l::short<i ) where k < 0 ",
                "GroupBy vectorized: false\n" +
                        "  keys: [k]\n" +
                        "  values: [max(ts)]\n" +
                        "    SelectedRecord\n" +
                        "        Async Filter\n" +
                        "          filter: (l::short<i and l<0)\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder4() throws Exception {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select mil, k, minl, mini from " +
                        "( select ts as k, max(i*l) as mil, min(i) as mini, min(l) as minl  " +
                        "from a where l::short<i ) " +
                        "where mil + mini> 1 ",
                "Filter filter: 1<mil+mini\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [k]\n" +
                        "      values: [max(i*l),min(l),min(i)]\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: l::short<i\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test
    public void testSortAscLimitAndSortAgain1a() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts asc limit 10) order by ts asc",
                    "Limit lo: 10\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain1b() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts desc, l desc limit 10) order by ts desc",
                    "Sort light lo: 10\n" +
                            "  keys: [ts desc, l desc]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts asc, l limit 10) lt join (select * from a) order by ts asc",
                    "SelectedRecord\n" +
                            "    Lt Join\n" +
                            "        Sort light lo: 10\n" +
                            "          keys: [ts, l]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain3a() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from " +
                            "(select * from (select * from a order by ts asc, l) limit 10) " +
                            "lt join " +
                            "(select * from a) order by ts asc",
                    "SelectedRecord\n" +
                            "    Lt Join\n" +
                            "        Limit lo: 10\n" +
                            "            Sort light\n" +
                            "              keys: [ts, l]\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain3b() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from " +
                            "(select * from (select * from a order by ts desc, l desc) limit 10) " +
                            "order by ts asc",
                    "Sort light\n" +
                            "  keys: [ts]\n" +
                            "    Limit lo: 10\n" +
                            "        Sort light\n" +
                            "          keys: [ts desc, l desc]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain4a() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from " +
                            "(select * from " +
                            "   (select * from a) " +
                            "    cross join " +
                            "   (select * from a) " +
                            " order by ts asc, l  " +
                            " limit 10" +
                            ") " +
                            "lt join (select * from a) " +
                            "order by ts asc",
                    "SelectedRecord\n" +
                            "    Lt Join\n" +
                            "        Limit lo: 10\n" +
                            "            Sort\n" +
                            "              keys: [ts, l]\n" +
                            "                SelectedRecord\n" +
                            "                    Cross Join\n" +
                            "                        DataFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: a\n" +
                            "                        DataFrame\n" +
                            "                            Row forward scan\n" +
                            "                            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain4b() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from " +
                            "(select * from " +
                            "   (select * from a) " +
                            "    cross join " +
                            "   (select * from a) " +
                            " order by ts desc " +
                            " limit 10" +
                            ") " +
                            "order by ts desc",
                    "Limit lo: 10\n" +
                            "    Sort\n" +
                            "      keys: [ts desc]\n" +
                            "        SelectedRecord\n" +
                            "            Cross Join\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: a\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortAscLimitAndSortDesc() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts asc limit 10) order by ts desc",
                    "Sort light\n" +
                            "  keys: [ts desc]\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSortDescLimitAndSortAgain() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts desc limit 10) order by ts desc",
                    "Limit lo: 10\n" +
                            "    DataFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: a\n");
        });
    }

    @Test
    public void testSortDescLimitAndSortAsc1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from (select * from a order by ts desc limit 10) order by ts asc",
                    "Sort light\n" +
                            "  keys: [ts]\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: a\n");
        });
    }

    @Test//TODO: sorting by ts, l again is not necessary
    public void testSortDescLimitAndSortAsc2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long)");

            assertPlan("select * from (select * from a order by ts, l limit 10) order by ts, l",
                    "Sort light\n" +
                            "  keys: [ts, l]\n" +
                            "    Sort light lo: 10\n" +
                            "      keys: [ts, l]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n");
        });
    }

    @Test//TODO: sorting by ts, l again is not necessary
    public void testSortDescLimitAndSortAsc3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long)");

            assertPlan("select * from (select * from a order by ts, l limit 10,-10) order by ts, l",
                    "Sort light\n" +
                            "  keys: [ts, l]\n" +
                            "    Limit lo: 10 hi: -10\n" +
                            "        Sort light\n" +
                            "          keys: [ts, l]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n");
        });
    }

    @Test
    public void testSpliceJoin0() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            compile("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from a splice join b on ts where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Splice Join\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testSpliceJoin0a() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            compile("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan("select * from a splice join b on ts where a.i + b.i = 1",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i+b.i=1\n" +
                            "        Splice Join\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testSpliceJoin1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a splice join b on ts",
                    "SelectedRecord\n" +
                            "    Splice Join\n" +
                            "      condition: b.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n");
        });
    }

    @Test
    public void testSpliceJoin2() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a splice join (select * from b limit 10) on ts",
                    "SelectedRecord\n" +
                            "    Splice Join\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testSpliceJoin3() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a splice join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                    "SelectedRecord\n" +
                            "    Splice Join\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        SelectedRecord\n" +
                            "            Sort light\n" +
                            "              keys: [ts, i]\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: b\n");
        });
    }

    @Test
    public void testSpliceJoin4() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table a ( i int, ts timestamp) timestamp(ts)");
            compile("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan("select * from a splice join b where a.i = b.i",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.i\n" +
                            "        Splice Join\n" +
                            "          condition: \n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n");
        });
    }

    @Test
    public void testUnion() throws Exception {
        assertPlan("create table a ( i int, s string);",
                "select * from a union select * from a",
                "Union\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testUnionAll() throws Exception {
        assertPlan("create table a ( i int, s string);", "select * from a union all select * from a",
                "Union All\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testWhereOrderByTsLimit1() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t ( x long, ts timestamp) timestamp(ts)");

            String query = "select * from t where x < 100 order by ts desc limit -5";
            assertPlan(query,
                    "Async JIT Filter\n" +
                            "  limit: 5\n" +
                            "  filter: x<100\n" +
                            "  workers: 1\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n");

            compile("insert into t select x, x::timestamp from long_sequence(10000)");

            assertQuery("x\tts\n" +
                    "5\t1970-01-01T00:00:00.000005Z\n" +
                    "4\t1970-01-01T00:00:00.000004Z\n" +
                    "3\t1970-01-01T00:00:00.000003Z\n" +
                    "2\t1970-01-01T00:00:00.000002Z\n" +
                    "1\t1970-01-01T00:00:00.000001Z\n", query, "ts###DESC", true, true);
        });
    }

    @Test
    public void testWhereUuid() throws Exception {
        assertPlan("create table a (u uuid, ts timestamp) timestamp(ts);",
                "select u, ts from a where u = '11111111-1111-1111-1111-111111111111' or u = '22222222-2222-2222-2222-222222222222' or u = '33333333-3333-3333-3333-333333333333'",
                "Async JIT Filter\n" +
                        "  filter: ((u='11111111-1111-1111-1111-111111111111' or u='22222222-2222-2222-2222-222222222222') or u='33333333-3333-3333-3333-333333333333')\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testWithBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table t ( x int );");

            int jitMode = sqlExecutionContext.getJitMode();
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            try {
                bindVariableService.clear();
                bindVariableService.setStr("v1", "a");
                assertBindVarPlan("string");

                bindVariableService.clear();
                bindVariableService.setLong("v1", 123);
                assertBindVarPlan("long");

                bindVariableService.clear();
                bindVariableService.setByte("v1", (byte) 123);
                assertBindVarPlan("byte");

                bindVariableService.clear();
                bindVariableService.setShort("v1", (short) 123);
                assertBindVarPlan("short");

                bindVariableService.clear();
                bindVariableService.setInt("v1", 12345);
                assertBindVarPlan("int");

                bindVariableService.clear();
                bindVariableService.setFloat("v1", 12f);
                assertBindVarPlan("float");

                bindVariableService.clear();
                bindVariableService.setDouble("v1", 12d);
                assertBindVarPlan("double");

                bindVariableService.clear();
                bindVariableService.setDate("v1", 123456L);
                assertBindVarPlan("date");

                bindVariableService.clear();
                bindVariableService.setTimestamp("v1", 123456L);
                assertBindVarPlan("timestamp");
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
        });
    }

    private void assertBindVarPlan(String type) throws SqlException {
        assertPlan("select * from t where x = :v1 ",
                "Async Filter\n" +
                        "  filter: x=:v1::" + type + "\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n");
    }

    private void assertPlan(String ddl, String query, String expectedPlan) throws Exception {
        assertMemoryLeak(() -> {
            compile(ddl);
            assertPlan(query, expectedPlan);
        });
    }

    private Function getConst(IntObjHashMap<Function> values, int type, int paramNo) {
        //use param number to work around rnd factories validation logic
        int val = paramNo + 1;

        switch (type) {
            case ColumnType.BYTE:
                return new ByteConstant((byte) val);
            case ColumnType.SHORT:
                return new ShortConstant((short) val);
            case ColumnType.INT:
                return new IntConstant(val);
            case ColumnType.LONG:
                return new LongConstant(val);
            case ColumnType.DATE:
                return new DateConstant(val * 86_400_000L);
            case ColumnType.TIMESTAMP:
                return new TimestampConstant(val * 86_400_000L);
            default:
                return values.get(type);
        }
    }

    private void test2686Prepare() throws Exception {
        assertMemoryLeak(() -> {
            compile("create table table_1 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          member boolean\n" +
                    "        ) timestamp(ts) PARTITION by month");

            compile("insert into table_1 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60, True )");
            compile("insert into table_1 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, False )");
            compile("insert into table_1 values ( '2022-10-25T03:00:00.000000Z', 'david',  21, True )");

            compile("create table table_2 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          address string\n" +
                    "        ) timestamp(ts) PARTITION by month");

            compile("insert into table_2 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60,  '1 Glebe St' )");
            compile("insert into table_2 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, '1 Broon St' )");
        });
    }
}
