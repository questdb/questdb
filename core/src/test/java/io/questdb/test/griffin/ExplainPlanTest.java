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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.*;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.NegatableBooleanFunction;
import io.questdb.griffin.engine.functions.bool.InCharFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InDoubleFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampStrFunctionFactory;
import io.questdb.griffin.engine.functions.bool.InTimestampTimestampFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToRegClassFunctionFactory;
import io.questdb.griffin.engine.functions.cast.CastStrToStrArrayFunctionFactory;
import io.questdb.griffin.engine.functions.catalogue.StringToStringArrayFunction;
import io.questdb.griffin.engine.functions.catalogue.WalTransactionsFunctionFactory;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.conditional.CoalesceFunctionFactory;
import io.questdb.griffin.engine.functions.conditional.SwitchFunctionFactory;
import io.questdb.griffin.engine.functions.constants.*;
import io.questdb.griffin.engine.functions.date.*;
import io.questdb.griffin.engine.functions.eq.*;
import io.questdb.griffin.engine.functions.rnd.LongSequenceFunctionFactory;
import io.questdb.griffin.engine.functions.rnd.RndIPv4CCFunctionFactory;
import io.questdb.griffin.engine.functions.test.TestSumXDoubleGroupByFunctionFactory;
import io.questdb.griffin.engine.table.DataFrameRecordCursorFactory;
import io.questdb.griffin.model.WindowColumn;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.StationaryMicrosClock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

public class ExplainPlanTest extends AbstractCairoTest {

    protected final static Log LOG = LogFactory.getLog(ExplainPlanTest.class);

    @BeforeClass
    public static void setUpStatic() throws Exception {
        testMicrosClock = StationaryMicrosClock.INSTANCE;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void test2686LeftJoinDoesntMoveOtherInnerJoinPredicate() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
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
                        "                    Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void test2686LeftJoinDoesntMoveOtherLeftJoinPredicate() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
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
                        "                Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void test2686LeftJoinDoesntMoveOtherTwoTableEqJoinPredicate() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
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
                        "                    Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void test2686LeftJoinDoesntPushJoinPredicateToLeftTable() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
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
                        "                Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void test2686LeftJoinDoesntPushJoinPredicateToRightTable() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts \n" +
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
                        "            Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void test2686LeftJoinDoesntPushWherePredicateToRightTable() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
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
                        "                    Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void test2686LeftJoinPushesWherePredicateToLeftJoinCondition() throws Exception {
        test2686Prepare();

        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts\n" +
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
                        "                Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void test2686LeftJoinPushesWherePredicateToLeftTable() throws Exception {
        test2686Prepare();
        assertMemoryLeak(() -> assertPlan(
                "select a.name, a.age, b.address, a.ts, dateadd('m', -1, b.ts), dateadd('m', 1, b.ts)\n" +
                        "from table_1 as a \n" +
                        "left join table_2 as b on a.ts >=  dateadd('m', -1, b.ts)  and a.ts <= dateadd('m', 1, b.ts)" +
                        "where a.age = 10 ",
                "VirtualRecord\n" +
                        "  functions: [name,age,address,ts,dateadd('m',-1,ts1),dateadd('m',1,ts1)]\n" +
                        "    SelectedRecord\n" +
                        "        Nested Loop Left Join\n" +
                        "          filter: (a.ts>=dateadd('m',-1,b.ts) and dateadd('m',1,b.ts)>=a.ts)\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: age=10\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: table_1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: table_2\n"
        ));
    }

    @Test
    public void testAsOfJoin0() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a asof join b on ts where a.i = b.ts::int",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts::int\n" +
                            "        AsOf Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testAsOfJoin0a() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select ts, ts1, i, i1 from (select * from a asof join b on ts ) where i/10 = i1",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i/10=b.i\n" +
                            "        AsOf Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testAsOfJoin1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a asof join b on ts",
                    "SelectedRecord\n" +
                            "    AsOf Join Light\n" +
                            "      condition: b.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testAsOfJoin2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a asof join (select * from b limit 10) on ts",
                    "SelectedRecord\n" +
                            "    AsOf Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testAsOfJoin3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a asof join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                    "SelectedRecord\n" +
                            "    AsOf Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Sort light\n" +
                            "          keys: [ts, i]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testAsOfJoin4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * " +
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
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test // where clause predicate can't be pushed to join clause because asof is and outer join
    public void testAsOfJoin5() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * " +
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
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testAsOfJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertPlan(
                        compiler,
                        "select * " +
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
                                "            Frame forward scan on: b\n",
                        sqlExecutionContext
                );
            }
        });
    }

    @Test
    public void testAsOfJoinNoKey() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a asof join b",
                    "SelectedRecord\n" +
                            "    AsOf Join\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testCachedWindowRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as ( " +
                    "  select " +
                    "    cast(x as int) i, " +
                    "    rnd_symbol('a','b','c') sym, " +
                    "    timestamp_sequence(0, 100000000) ts " +
                    "   from long_sequence(100)" +
                    ") timestamp(ts) partition by hour");

            String sql = "select i, " +
                    "row_number() over (partition by sym), " +
                    "avg(i) over (), " +
                    "sum(i) over (), " +
                    "first_value(i) over (), " +
                    "from x limit 3";
            assertPlan(
                    sql,
                    "Limit lo: 3\n" +
                            "    CachedWindow\n" +
                            "      unorderedFunctions: [row_number() over (partition by [sym]),avg(i) over (),sum(i) over (),first_value(i) over ()]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n"
            );

            assertSql("i\trow_number\tavg\tsum\tfirst_value\n" +
                    "1\t1\t50.5\t5050.0\t1.0\n" +
                    "2\t2\t50.5\t5050.0\t1.0\n" +
                    "3\t1\t50.5\t5050.0\t1.0\n", sql);
        });
    }

    @Test
    public void testCastFloatToDouble() throws Exception {
        assertPlan(
                "select rnd_float()::double ",
                "VirtualRecord\n" +
                        "  functions: [rnd_float()::double]\n" +
                        "    long_sequence count: 1\n"
        );
    }

    @Test
    public void testCountOfColumnsVectorized() throws Exception {
        assertPlan(
                "create table x " +
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
                "GroupBy vectorized: true workers: 1\n" +
                        "  keys: [k]\n" +
                        "  values: [count(*),count(*),count(i),count(l),count(d),count(dat),count(ts)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: x\n"
        );
    }

    @Test
    public void testCrossJoin0() throws Exception {
        assertPlan(
                "create table a ( i int, s1 string, s2 string)",
                "select * from a cross join a b where length(a.s1) = length(b.s2)",
                "SelectedRecord\n" +
                        "    Filter filter: length(a.s1)=length(b.s2)\n" +
                        "        Cross Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test
    public void testCrossJoin0Output() throws Exception {
        assertQuery("cnt\n9\n",
                "select count(*) cnt from a cross join a b where length(a.s1) = length(b.s2)",
                "create table a as (select x, 's' || x as s1, 's' || (x%3) as s2 from long_sequence(3))", null, false, true
        );
    }

    @Test
    public void testCrossJoin1() throws Exception {
        assertPlan(
                "create table a ( i int)",
                "select * from a cross join a b",
                "SelectedRecord\n" +
                        "    Cross Join\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testCrossJoin2() throws Exception {
        assertPlan(
                "create table a ( i int)",
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
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testCrossJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {"select * from t t1 cross join t t2 order by t1.ts",
                    "select * from (select * from t order by ts desc) t1 cross join t t2 order by t1.ts"};
            for (String query : queries) {
                assertPlan(
                        query,
                        "SelectedRecord\n" +
                                "    Cross Join\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n"
                );

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
            ddl("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " +
                    "((select * from t order by ts desc) limit 10) t1 " +
                    "cross join t t2 " +
                    "order by t1.ts desc";

            assertPlan(
                    query,
                    "SelectedRecord\n" +
                            "    Cross Join\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: t\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testCrossJoinWithSort3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t (x int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from " +
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
                            "            Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testCrossJoinWithSort4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t (x int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from " +
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
                            "                Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testCrossJoinWithSort5() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t (x int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from " +
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
                            "            Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testDistinctOverWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test (event double )");
            insert("insert into test select x from long_sequence(3)");

            String query = "select * from ( SELECT DISTINCT avg(event) OVER (PARTITION BY 1) FROM test )";
            assertPlan(query,
                    "Distinct\n" +
                            "  keys: avg\n" +
                            "    CachedWindow\n" +
                            "      unorderedFunctions: [avg(event) over (partition by [1])]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            assertSql("avg\n2.0\n", query);
            assertSql("avg\n2.0\n", "select * from ( " + query + " )");
        });
    }

    @Test
    public void testDistinctTsWithLimit1() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di order by 1 limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit2() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di order by 1 desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [ts desc]\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit3() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit4() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di limit -10",
                "Limit lo: -10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit5a() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where y = 5 limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: y=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit5b() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where y = 5 limit -10",
                "Limit lo: -10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: y=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit6a() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit 10",
                "Limit lo: 10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter workers: 1\n" +
                        "              filter: abs(y)=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit6b() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit -10",
                "Limit lo: -10\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter workers: 1\n" +
                        "              filter: abs(y)=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctTsWithLimit7() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select distinct ts from di where abs(y) = 5 limit 10, 20",
                "Limit lo: 10 hi: 20\n" +
                        "    DistinctTimeSeries\n" +
                        "      keys: ts\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter workers: 1\n" +
                        "              filter: abs(y)=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit1() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di order by 1 limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x]\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true workers: 1\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit2() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di order by 1 desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x desc]\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true workers: 1\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit3() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di limit 10",
                "Limit lo: 10\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true workers: 1\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit4() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di limit -10",
                "Limit lo: -10\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true workers: 1\n" +
                        "          keys: [x]\n" +
                        "          values: [count(*)]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit5a() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di where y = 5 limit 10",
                "Limit lo: 10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: y=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit5b() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di where y = 5 limit -10",
                "Limit lo: -10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: y=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit6a() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit 10",
                "Limit lo: 10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter workers: 1\n" +
                        "              filter: abs(y)=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit6b() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit -10",
                "Limit lo: -10\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter workers: 1\n" +
                        "              filter: abs(y)=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testDistinctWithLimit7() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select distinct x from di where abs(y) = 5 limit 10, 20",
                "Limit lo: 10 hi: 20\n" +
                        "    Distinct\n" +
                        "      keys: x\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter workers: 1\n" +
                        "              filter: abs(y)=5\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: di\n"
        );
    }

    @Test
    public void testExcept() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
                "select * from a except select * from a",
                "Except\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testExceptAll() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
                "select * from a except all select * from a",
                "Except All\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testExceptAndSort1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts desc",
                    "Except\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testExceptAndSort2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts asc",
                    "Except\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testExceptAndSort3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts desc limit 10) except (select * from a) order by ts asc",
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
                            "                Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testExceptAndSort4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts asc limit 10) except (select * from a) order by ts desc",
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
                            "                Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testExplainCreateTable() throws Exception {
        assertSql("QUERY PLAN\n" +
                "Create table: a\n", "explain create table a ( l long, d double)"
        );
    }

    @Test
    public void testExplainCreateTableAsSelect() throws Exception {
        assertSql("QUERY PLAN\n" +
                "Create table: a\n" +
                "    VirtualRecord\n" +
                "      functions: [x,1]\n" +
                "        long_sequence count: 10\n", "explain create table a as (select x, 1 from long_sequence(10))"
        );
    }

    @Test
    public void testExplainDeferredSingleSymbolFilterDataFrame() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab \n" +
                    "(\n" +
                    "   id symbol index,\n" +
                    "   ts timestamp,\n" +
                    "   val double  \n" +
                    ") timestamp(ts);");
            insert("insert into tab values ( 'XXX', 0::timestamp, 1 );");

            assertPlan(
                    "  select\n" +
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
                            "        Frame forward scan on: tab\n"
            );

        });
    }

    @Test
    public void testExplainInsert() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( l long, d double)");
            assertSql("QUERY PLAN\n" +
                    "Insert into table: a\n", "explain insert into a values (1, 2.0)"
            );
        });
    }

    @Test
    public void testExplainInsertAsSelect() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( l long, d double)");
            assertSql("QUERY PLAN\n" +
                    "Insert into table: a\n" +
                    "    VirtualRecord\n" +
                    "      functions: [x,1]\n" +
                    "        long_sequence count: 10\n", "explain insert into a select x, 1 from long_sequence(10)"
            );
        });
    }

    @Test
    public void testExplainPlanNoTrailingQuote() throws Exception {
        assertQuery("QUERY PLAN\n" +
                "[\n" +
                "  {\n" +
                "    \"Plan\": {\n" +
                "        \"Node Type\": \"long_sequence\",\n" +
                "        \"count\":  1\n" +
                "    }\n" +
                "  }\n" +
                "]\n", "explain (format json) select * from long_sequence(1)", null, null, false, true);
    }

    @Test
    public void testExplainPlanWithEOLs1() throws Exception {
        assertPlan(
                "create table a (s string)",
                "select * from a where s = '\b\f\n\r\t\\u0013'",
                "Async Filter workers: 1\n" +
                        "  filter: s='\\b\\f\\n\\r\\t\\u0013'\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testExplainSelect() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( l long, d double)");
            assertSql("QUERY PLAN\n" +
                    "DataFrame\n" +
                    "    Row forward scan\n" +
                    "    Frame forward scan on: a\n", "explain select * from a;"
            );
        });
    }

    @Test
    public void testExplainSelectWithCte1() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
                "with b as (select * from a where i = 0)" +
                        "select * from a union all select * from b",
                "Union All\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Async JIT Filter workers: 1\n" +
                        "      filter: i=0\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testExplainSelectWithCte2() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
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
                        "                        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testExplainSelectWithCte3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( l long, d double)");
            assertSql("QUERY PLAN\n" +
                    "Limit lo: 10\n" +
                    "    DataFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: a\n", "explain with b as (select * from a limit 10) select * from b;"
            );
        });
    }

    @Test
    public void testExplainUpdate1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( l long, d double)");
            assertSql("QUERY PLAN\n" +
                    "Update table: a\n" +
                    "    VirtualRecord\n" +
                    "      functions: [1,10.1]\n" +
                    "        DataFrame\n" +
                    "            Row forward scan\n" +
                    "            Frame forward scan on: a\n", "explain update a set l = 1, d=10.1;"
            );
        });
    }

    @Test
    public void testExplainUpdate2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( l1 long, d1 double)");
            ddl("create table b ( l2 long, d2 double)");
            assertSql("QUERY PLAN\n" +
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
                    "                        Frame forward scan on: b\n", "explain update a set l1 = 1, d1=d2 from b where l1=l2;"
            );
        });
    }

    @Test
    public void testExplainUpdateWithFilter() throws Exception {
        assertPlan(
                "create table a ( l long, d double, ts timestamp) timestamp(ts)",
                "update a set l = 20, d = d+rnd_double() " +
                        "where d < 100.0d and ts > dateadd('d', 1, now()  );",
                "Update table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [20,d+rnd_double()]\n" +
                        "        Async Filter workers: 1\n" +
                        "          filter: d<100.0\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Interval forward scan on: a\n" +
                        "                  intervals: [(\"1970-01-02T00:00:00.000001Z\",\"MAX\")]\n"
        );
    }

    @Test
    public void testExplainWindowFunctionWithCharConstantFrameBounds() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab ( key int, value double, ts timestamp) timestamp(ts)");

            assertPlan("select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN '1' MINUTES PRECEDING AND CURRENT ROW) from tab",
                    "Window\n" +
                            "  functions: [avg(value) over (partition by [key] range between 60000000 preceding and current row)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN '4' MINUTES PRECEDING AND '3' MINUTES PRECEDING) from tab",
                    "Window\n" +
                            "  functions: [avg(value) over (partition by [key] range between 240000000 preceding and 180000000 preceding)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select avg(value) over (PARTITION BY key ORDER BY ts RANGE BETWEEN UNBOUNDED PRECEDING AND '10' MINUTES PRECEDING) from tab",
                    "Window\n" +
                            "  functions: [avg(value) over (partition by [key] range between unbounded preceding and 600000000 preceding)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");
        });
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
                "]\n", "explain (format json) select count (*) from long_sequence(10)", null, null, false, true);
    }

    @Test
    public void testExplainWithJsonFormat2() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
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
                    "                        \"workers\":  1,\n" +
                    "                        \"limit\":  4,\n" +
                    "                        \"filter\": \"10<l\",\n" +
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

            ddl("create table a ( l long)");
            assertQuery(
                    compiler,
                    expected,
                    "explain (format json) select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ",
                    null,
                    false,
                    sqlExecutionContext,
                    true
            );
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
                "create table a ( i int, d double)", null, false, true
        );
    }

    @Test
    public void testExplainWithJsonFormat4() throws Exception {
        ddl("create table taba (a1 int, a2 long)");
        ddl("create table tabb (b1 int, b2 long)");
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
                " explain (format json) select * from taba left join tabb on a1=b1  or a2=b2", null, null, false, true
        );
    }

    @Test
    public void testExplainWithQueryInParentheses1() throws SqlException {
        assertPlan(
                "(select 1)",
                "VirtualRecord\n" +
                        "  functions: [1]\n" +
                        "    long_sequence count: 1\n"
        );
    }

    @Test
    public void testExplainWithQueryInParentheses2() throws Exception {
        assertPlan(
                "create table x ( i int)",
                "(select * from x)",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: x\n"
        );
    }

    @Test
    public void testExplainWithQueryInParentheses3() throws Exception {
        assertPlan(
                "create table x ( i int)",
                "((select * from x))",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: x\n"
        );
    }

    @Test
    public void testExplainWithQueryInParentheses4() throws Exception {
        assertPlan(
                "create table x ( i int)",
                "((x))",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: x\n"
        );
    }

    @Test
    public void testExplainWithQueryInParentheses5() throws Exception {
        assertPlan(
                "CREATE TABLE trades (\n" +
                        "  symbol SYMBOL,\n" +
                        "  side SYMBOL,\n" +
                        "  price DOUBLE,\n" +
                        "  amount DOUBLE,\n" +
                        "  timestamp TIMESTAMP\n" +
                        ") timestamp (timestamp) PARTITION BY DAY",
                "((select last(timestamp) as x, last(price) as btcusd " +
                        "from trades " +
                        "where symbol = 'BTC-USD' " +
                        "and timestamp > dateadd('m', -30, now())) " +
                        "timestamp(x))",
                "SelectedRecord\n" +
                        "    GroupBy vectorized: false\n" +
                        "      values: [last(timestamp),last(price)]\n" +
                        "        Async JIT Filter workers: 1\n" +
                        "          filter: symbol='BTC-USD'\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Interval forward scan on: trades\n" +
                        "                  intervals: [(\"1969-12-31T23:30:00.000001Z\",\"MAX\")]\n"
        );
    }

    @Test
    public void testFilterOnExcludedIndexedSymbolManyValues() throws Exception {
        assertMemoryLeak(() -> {
            drop("drop table if exists trips");
            ddl("CREATE TABLE trips(l long, s symbol index capacity 5, ts TIMESTAMP) " +
                    "timestamp(ts) partition by month");

            assertPlan("select s, count() from trips where s is not null order by count desc",
                    "Sort light\n" +
                            "  keys: [count desc]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [s]\n" +
                            "      values: [count(*)]\n" +
                            "        FilterOnExcludedValues symbolOrder: desc\n" +
                            "          symbolFilter: s not in [null]\n" +
                            "            Cursor-order scan\n" +
                            "            Frame forward scan on: trips\n"
            );

            insert("insert into trips " +
                    "  select x, 'A' || ( x%3000 )," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(10000);");
            insert("insert into trips " +
                    "  select x, null," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(4000);"
            );

            assertPlan("select s, count() from trips where s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: s is not null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where s is not null and s != 'A1000'",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null and s!='A1000')\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A100");
            assertPlan("select s, count() from trips where s is not null and s != :s1",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null and s!=:s1::string)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where s is not null and l != 0",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null and l!=0)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where s is not null or l != 0",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null or l!=0)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where l != 0 and s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (l!=0 and s is not null)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where l != 0 or s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (l!=0 or s is not null)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where l > 100 or l != 0 and s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: ((100<l or l!=0) and s is not null)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', 'A2000')",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: ((100<l or l!=0) and not (s in [null,A1000,A2000]))\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A500");
            assertPlan("select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', :s1)",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: ((100<l or l!=0) and not (s in [null,A1000] or s in [:s1::string]))\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );
        });
    }

    @Test
    public void testFilterOnExcludedNonIndexedSymbolManyValues() throws Exception {
        assertMemoryLeak(() -> {
            drop("drop table if exists trips");
            ddl("CREATE TABLE trips(l long, s symbol capacity 5, ts TIMESTAMP) " +
                    "timestamp(ts) partition by month");

            assertPlan("select s, count() from trips where s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: s is not null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            insert("insert into trips " +
                    "  select x, 'A' || ( x%3000 )," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(10000);");
            insert("insert into trips " +
                    "  select x, null," +
                    "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 1000000) " +
                    "  from long_sequence(4000);"
            );

            assertPlan("select s, count() from trips where s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: s is not null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where s is not null and s != 'A1000'",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null and s!='A1000')\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A100");
            assertPlan("select s, count() from trips where s is not null and s != :s1",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null and s!=:s1::string)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where s is not null and l != 0",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null and l!=0)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where s is not null or l != 0",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (s is not null or l!=0)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where l != 0 and s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (l!=0 and s is not null)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where l != 0 or s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: (l!=0 or s is not null)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            assertPlan("select s, count() from trips where l > 100 or l != 0 and s is not null",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: ((100<l or l!=0) and s is not null)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );

            bindVariableService.clear();
            bindVariableService.setStr("s1", "A500");
            assertPlan("select s, count() from trips where l > 100 or l != 0 and s not in (null, 'A1000', :s1)",
                    "Async Group By workers: 1\n" +
                            "  keys: [s]\n" +
                            "  values: [count(*)]\n" +
                            "  filter: ((100<l or l!=0) and not (s in [null,A1000] or s in [:s1::string]))\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: trips\n"
            );
        });
    }

    @Test
    public void testFiltersOnIndexedSymbolColumns() throws SqlException {
        ddl("CREATE TABLE reference_prices (\n" +
                "  venue SYMBOL index ,\n" +
                "  symbol SYMBOL index,\n" +
                "  instrumentType SYMBOL index,\n" +
                "  referencePriceType SYMBOL index,\n" +
                "  resolutionType SYMBOL ,\n" +
                "  ts TIMESTAMP,\n" +
                "  referencePrice DOUBLE\n" +
                ") timestamp (ts)");

        insert("insert into reference_prices \n" +
                "select rnd_symbol('VENUE1', 'VENUE2', 'VENUE3'), \n" +
                "          'symbol', \n" +
                "          'instrumentType', \n" +
                "          rnd_symbol('TYPE1', 'TYPE2'), \n" +
                "          'resolutionType', \n" +
                "          cast(x as timestamp), \n" +
                "          rnd_double()\n" +
                "from long_sequence(10000)");

        String query1 = "select referencePriceType,count(*) " +
                "from reference_prices " +
                "where referencePriceType not in ('TYPE1') " +
                "and venue in ('VENUE1', 'VENUE2')";
        String expectedResult = "referencePriceType\tcount\n" +
                "TYPE2\t3344\n";
        String expectedPlan = "GroupBy vectorized: false\n" +
                "  keys: [referencePriceType]\n" +
                "  values: [count(*)]\n" +
                "    FilterOnValues symbolOrder: desc\n" +
                "        Cursor-order scan\n" +
                "            Index forward scan on: referencePriceType\n" +
                "              filter: referencePriceType=1 and not (referencePriceType in [TYPE1])\n" +
                "            Index forward scan on: referencePriceType\n" +
                "              filter: referencePriceType=3 and not (referencePriceType in [TYPE1])\n" +
                "        Frame forward scan on: reference_prices\n";

        assertPlan(query1, expectedPlan);
        assertSql(expectedResult, query1);

        String query2 = "select referencePriceType, count(*) \n" +
                "from reference_prices \n" +
                "where venue in ('VENUE1', 'VENUE2') \n" +
                "and referencePriceType not in ('TYPE1')";

        assertPlan(query2, expectedPlan);
        assertSql(expectedResult, query2);
    }

    @Test
    public void testFullFatHashJoin0() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(true);
            ddl("create table a ( l long)");
            assertPlan(
                    compiler,
                    "select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ",
                    "SelectedRecord\n" +
                            "    Filter filter: 0<a.l+b.l\n" +
                            "        Hash Join\n" +
                            "          condition: b.l=a.l\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            Hash\n" +
                            "                Async JIT Filter workers: 1\n" +
                            "                  limit: 4\n" +
                            "                  filter: 10<l\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: a\n",
                    sqlExecutionContext
            );
        }
    }

    @Test
    public void testFullFatHashJoin1() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(true);
            ddl("create table a ( l long)");
            assertPlan(
                    compiler,
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
                            "                    Frame forward scan on: a\n",
                    sqlExecutionContext
            );
        }
    }

    @Test
    public void testFullFatHashJoin2() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(true);
            ddl("create table a ( l long)");
            assertPlan(
                    compiler,
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
                            "                Frame forward scan on: a\n",
                    sqlExecutionContext
            );
        }
    }

    @Test
    public void testFunctions() throws Exception {
        assertMemoryLeak(() -> { // test table for show_columns
            ddl("create table bbb (a int)");
        });

        final StringSink sink = new StringSink();

        IntObjHashMap<ObjList<Function>> constFuncs = new IntObjHashMap<ObjList<Function>>();
        constFuncs.put(ColumnType.BOOLEAN, list(BooleanConstant.TRUE, BooleanConstant.FALSE));
        constFuncs.put(ColumnType.BYTE, list(new ByteConstant((byte) 1)));
        constFuncs.put(ColumnType.SHORT, list(new ShortConstant((short) 2)));
        constFuncs.put(ColumnType.CHAR, list(new CharConstant('a')));
        constFuncs.put(ColumnType.INT, list(new IntConstant(3)));
        constFuncs.put(ColumnType.IPv4, list(new IPv4Constant(3)));
        constFuncs.put(ColumnType.LONG, list(new LongConstant(4)));
        constFuncs.put(ColumnType.DATE, list(new DateConstant(0)));
        constFuncs.put(ColumnType.TIMESTAMP, list(new TimestampConstant(86400000000L)));
        constFuncs.put(ColumnType.FLOAT, list(new FloatConstant(5f)));
        constFuncs.put(ColumnType.DOUBLE, list(new DoubleConstant(1))); // has to be [0.0, 1.0] for approx_percentile
        constFuncs.put(ColumnType.STRING, list(new StrConstant("bbb"), new StrConstant("1"), new StrConstant("1.1.1.1"), new StrConstant("1.1.1.1/24")));
        constFuncs.put(ColumnType.SYMBOL, list(new SymbolConstant("symbol", 0)));
        constFuncs.put(ColumnType.LONG256, list(new Long256Constant(0, 1, 2, 3)));
        constFuncs.put(ColumnType.GEOBYTE, list(new GeoByteConstant((byte) 1, ColumnType.getGeoHashTypeWithBits(5))));
        constFuncs.put(ColumnType.GEOSHORT, list(new GeoShortConstant((short) 1, ColumnType.getGeoHashTypeWithBits(10))));
        constFuncs.put(ColumnType.GEOINT, list(new GeoIntConstant(1, ColumnType.getGeoHashTypeWithBits(20))));
        constFuncs.put(ColumnType.GEOLONG, list(new GeoLongConstant(1, ColumnType.getGeoHashTypeWithBits(35))));
        constFuncs.put(ColumnType.GEOHASH, list(new GeoShortConstant((short) 1, ColumnType.getGeoHashTypeWithBits(15))));
        constFuncs.put(ColumnType.BINARY, list(new NullBinConstant()));
        constFuncs.put(ColumnType.LONG128, list(new Long128Constant(0, 1)));
        constFuncs.put(ColumnType.UUID, list(new UuidConstant(0, 1)));
        constFuncs.put(ColumnType.NULL, list(NullConstant.NULL));

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("bbb", ColumnType.INT));
        constFuncs.put(ColumnType.RECORD, list(new RecordColumn(0, metadata)));

        GenericRecordMetadata cursorMetadata = new GenericRecordMetadata();
        cursorMetadata.add(new TableColumnMetadata("s", ColumnType.STRING));
        constFuncs.put(ColumnType.CURSOR, list(new CursorFunction(new EmptyTableRecordCursorFactory(cursorMetadata) {
            public boolean supportPageFrameCursor() {
                return true;
            }
        })));

        IntObjHashMap<Function> colFuncs = new IntObjHashMap<>();
        colFuncs.put(ColumnType.BOOLEAN, new BooleanColumn(1));
        colFuncs.put(ColumnType.BYTE, new ByteColumn(1));
        colFuncs.put(ColumnType.SHORT, new ShortColumn(2));
        colFuncs.put(ColumnType.CHAR, new CharColumn(1));
        colFuncs.put(ColumnType.INT, new IntColumn(1));
        colFuncs.put(ColumnType.IPv4, new IPv4Column(1));
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

        FunctionFactoryCache cache = engine.getFunctionFactoryCache();
        LowerCaseCharSequenceObjHashMap<ObjList<FunctionFactoryDescriptor>> factories = cache.getFactories();
        factories.forEach((key, value) -> {
            FUNCTIONS:
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
                    int argTypeMask = descriptor.getArgTypeMask(p);
                    boolean isConstant = FunctionFactoryDescriptor.isConstant(argTypeMask);
                    short sigArgType = FunctionFactoryDescriptor.toType(argTypeMask);
                    ObjList<Function> availableValues = constFuncs.get(sigArgType);
                    int constValues = availableValues != null ? availableValues.size() : 1;
                    combinations *= (constValues + (isConstant ? 0 : 1));
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
                                } else if (factory instanceof InDoubleFunctionFactory) {
                                    sigArgType = ColumnType.DOUBLE;
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
                            } else if (factory instanceof EqIPv4FunctionFactory && sigArgType == ColumnType.STRING) {
                                args.add(new StrConstant("10.8.6.5"));
                            } else if (factory instanceof ContainsIPv4FunctionFactory && sigArgType == ColumnType.STRING) {
                                args.add(new StrConstant("12.6.5.10/24"));
                            } else if (factory instanceof ContainsEqIPv4FunctionFactory && sigArgType == ColumnType.STRING) {
                                args.add(new StrConstant("12.6.5.10/24"));
                            } else if (factory instanceof NegContainsEqIPv4FunctionFactory && sigArgType == ColumnType.STRING) {
                                args.add(new StrConstant("34.56.22.11/12"));
                            } else if (factory instanceof NegContainsIPv4FunctionFactory && sigArgType == ColumnType.STRING) {
                                args.add(new StrConstant("32.12.22.11/12"));
                            } else if (factory instanceof RndIPv4CCFunctionFactory) {
                                args.add(new StrConstant("4.12.22.11/12"));
                                args.add(new IntConstant(2));
                            } else if (!useConst) {
                                args.add(colFuncs.get(sigArgType));
                            } else if (factory instanceof WalTransactionsFunctionFactory && sigArgType == ColumnType.STRING) {
                                // Skip it, it requires a WAL table to exist
                                break FUNCTIONS;
                            } else {
                                args.add(getConst(constFuncs, sigArgType, p, no));
                            }

                            if (!isConstant) {
                                tempNo >>= 1;
                            }
                        }

                        argPositions.setAll(args.size(), 0);

                        //TODO: test with partition by, order by and various frame modes
                        if (factory.isWindow()) {
                            sqlExecutionContext.configureWindowContext(null, null, null, false, DataFrameRecordCursorFactory.SCAN_DIRECTION_FORWARD, -1, true, WindowColumn.FRAMING_RANGE, Long.MIN_VALUE, 10, 0, 20, WindowColumn.EXCLUDE_NO_OTHERS, 0, -1);
                        }
                        Function function = null;
                        try {
                            function = factory.newInstance(0, args, argPositions, engine.getConfiguration(), sqlExecutionContext);
                            function.toPlan(planSink);
                        } finally {
                            sqlExecutionContext.clearWindowContext();
                        }

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

                            Assert.assertFalse(
                                    "function " + factory.getSignature() + " should serialize to text properly",
                                    Chars.contains(tmpPlanSink.getSink(), "io.questdb")
                            );
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
        assertPlan(
                "create table a (l long, b boolean)",
                "select b, min(l)  from a group by b",
                "Async Group By workers: 1\n" +
                        "  keys: [b]\n" +
                        "  values: [min(l)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByBooleanFunction() throws Exception {
        assertPlan(
                "create table a (l long, b1 boolean, b2 boolean)",
                "select b1||b2, min(l) from a group by b1||b2",
                "Async Group By workers: 1\n" +
                        "  keys: [concat]\n" +
                        "  values: [min(l)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByBooleanWithFilter() throws Exception {
        assertPlan(
                "create table a (l long, b boolean)",
                "select b, min(l)  from a where b = true group by b",
                "Async Group By workers: 1\n" +
                        "  keys: [b]\n" +
                        "  values: [min(l)]\n" +
                        "  filter: b=true\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByDouble() throws Exception {
        assertPlan(
                "create table a (l long, d double)",
                "select d, min(l) from a group by d",
                "Async Group By workers: 1\n" +
                        "  keys: [d]\n" +
                        "  values: [min(l)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByFloat() throws Exception {
        assertPlan(
                "create table a (l long, f float)",
                "select f, min(l) from a group by f",
                "Async Group By workers: 1\n" +
                        "  keys: [f]\n" +
                        "  values: [min(l)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test //special case
    public void testGroupByHour() throws Exception {
        assertPlan(
                "create table a (ts timestamp, d double)",
                "select hour(ts), min(d) from a group by hour(ts)",
                "GroupBy vectorized: true workers: 1\n" +
                        "  keys: [ts]\n" +
                        "  values: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByInt1() throws Exception {
        assertPlan(
                "create table a (i int, d double)",
                "select min(d), i from a group by i",
                "VirtualRecord\n" +
                        "  functions: [min,i]\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [i]\n" +
                        "      values: [min(d)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test // repeated group by keys get merged at group by level
    public void testGroupByInt2() throws Exception {
        assertPlan(
                "create table a (i int, d double)",
                "select i, i, min(d) from a group by i, i",
                "VirtualRecord\n" +
                        "  functions: [i,i,min]\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [i]\n" +
                        "      values: [min(d)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByIntOperation() throws Exception {
        assertPlan(
                "create table a (i int, d double)",
                "select min(d), i * 42 from a group by i",
                "VirtualRecord\n" +
                        "  functions: [min,i*42]\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [i]\n" +
                        "      values: [min(d)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByKeyedAliased() throws Exception {
        assertPlan(
                "create table a (s symbol, ts timestamp) timestamp(ts) partition by year;",
                "select s as symbol, count() from a",
                "GroupBy vectorized: true workers: 1\n" +
                        "  keys: [symbol]\n" +
                        "  values: [count(*)]\n" +
                        "    SelectedRecord\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByKeyedNoAlias() throws Exception {
        assertPlan(
                "create table a (s symbol, ts timestamp) timestamp(ts) partition by year;",
                "select s, count() from a",
                "GroupBy vectorized: true workers: 1\n" +
                        "  keys: [s]\n" +
                        "  values: [count(*)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByKeyedOnExcept() throws Exception {
        ddl("create table a ( i int, d double)");

        assertPlan(
                "create table b ( j int, e double)",
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
                        "                Frame forward scan on: b\n"
        );
    }

    @Test
    public void testGroupByKeyedOnIntersect() throws Exception {
        ddl("create table a ( i int, d double)");

        assertPlan(
                "create table b ( j int, e double)",
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
                        "                Frame forward scan on: b\n"
        );
    }

    @Test
    public void testGroupByKeyedOnUnion() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
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
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByKeyedOnUnionAll() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
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
                        "            Frame forward scan on: a\n"
        );
    }

    @Test // only none, single int|symbol key cases are vectorized
    public void testGroupByLong() throws Exception {
        assertPlan(
                "create table a ( l long, d double)",
                "select l, min(d) from a group by l",
                "Async Group By workers: 1\n" +
                        "  keys: [l]\n" +
                        "  values: [min(d)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions1() throws Exception {
        assertPlan(
                "create table a (i int, d double)",
                "select i from a group by i",
                "GroupBy vectorized: true workers: 1\n" +
                        "  keys: [i]\n" +
                        "  values: [count(*)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions2() throws Exception {
        assertPlan(
                "create table a (i int, d double)",
                "select i from a where d < 42 group by i",
                "Async Group By workers: 1\n" +
                        "  keys: [i]\n" +
                        "  filter: d<42\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions3() throws Exception {
        assertPlan(
                "create table a (i short, d double)",
                "select i from a group by i",
                "Async Group By workers: 1\n" +
                        "  keys: [i]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions4() throws Exception {
        assertPlan(
                "create table a (i long, j long)",
                "select i, j from a group by i, j",
                "Async Group By workers: 1\n" +
                        "  keys: [i,j]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions5() throws Exception {
        assertPlan(
                "create table a (i long, j long, d double)",
                "select i, j from a where d > 42 group by i, j",
                "Async Group By workers: 1\n" +
                        "  keys: [i,j]\n" +
                        "  filter: 42<d\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions6() throws Exception {
        assertPlan(
                "create table a (s symbol)",
                "select s from a group by s",
                "GroupBy vectorized: true workers: 1\n" +
                        "  keys: [s]\n" +
                        "  values: [count(*)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions7() throws Exception {
        assertPlan(
                "create table a (s symbol, d double)",
                "select s from a where d = 42 group by s",
                "Async Group By workers: 1\n" +
                        "  keys: [s]\n" +
                        "  filter: d=42\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions8() throws Exception {
        assertPlan(
                "create table a (s string)",
                "select s from a group by s",
                "Async Group By workers: 1\n" +
                        "  keys: [s]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNoFunctions9() throws Exception {
        assertPlan(
                "create table a (s string)",
                "select s from a where s like '%foobar%' group by s",
                "Async Group By workers: 1\n" +
                        "  keys: [s]\n" +
                        "  filter: s like %foobar%\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNotKeyed1() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select min(d) from a",
                "GroupBy vectorized: true\n" +
                        "  values: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNotKeyed10() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
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
                        "                    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNotKeyed11() throws Exception {
        assertPlan(
                "create table a ( gb geohash(4b), gs geohash(12b), gi geohash(24b), gl geohash(40b))",
                "select first(gb), last(gb), first(gs), last(gs), first(gi), last(gi), first(gl), last(gl) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [first(gb),last(gb),first(gs),last(gs),first(gi),last(gi),first(gl),last(gl)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNotKeyed12() throws Exception {
        assertPlan(
                "create table a ( gb geohash(4b), gs geohash(12b), gi geohash(24b), gl geohash(40b), i int)",
                "select first(gb), last(gb), first(gs), last(gs), first(gi), last(gi), first(gl), last(gl) from a where i > 42",
                "GroupBy vectorized: false\n" +
                        "  values: [first(gb),last(gb),first(gs),last(gs),first(gi),last(gi),first(gl),last(gl)]\n" +
                        "    Async JIT Filter workers: 1\n" +
                        "      filter: 42<i\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed2() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select min(d), max(d*d) from a",
                "Async Group By workers: 1\n" +
                        "  values: [min(d),max(d*d)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed3() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select max(d+1) from a",
                "Async Group By workers: 1\n" +
                        "  values: [max(d+1)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByNotKeyed4() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select count(*), max(i), min(d) from a",
                "GroupBy vectorized: true\n" +
                        "  values: [count(*),max(i),min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // constant values used in aggregates disable vectorization
    public void testGroupByNotKeyed5() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select first(10), last(d), avg(10), min(10), max(10) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [first(10),last(d),avg(10),min(10),max(10)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // group by on filtered data is not vectorized
    public void testGroupByNotKeyed6() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select max(i) from a where i < 10",
                "Async Group By workers: 1\n" +
                        "  values: [max(i)]\n" +
                        "  filter: i<10\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // order by is ignored and grouped by - vectorized
    public void testGroupByNotKeyed7() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select max(i) from (select * from a order by d)",
                "GroupBy vectorized: true\n" +
                        "  values: [max(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // order by can't be ignored; group by is not vectorized
    public void testGroupByNotKeyed8() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select max(i) from (select * from a order by d limit 10)",
                "GroupBy vectorized: false\n" +
                        "  values: [max(i)]\n" +
                        "    Sort light lo: 10\n" +
                        "      keys: [d]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test // TODO: group by could be vectorized for union tables and result merged
    public void testGroupByNotKeyed9() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select max(i) from (select * from a union all select * from a)",
                "GroupBy vectorized: false\n" +
                        "  values: [max(i)]\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByStringFunction() throws Exception {
        assertPlan(
                "create table a (l long, s1 string, s2 string)",
                "select s1||s2 s, avg(l) a from a",
                "Async Group By workers: 1\n" +
                        "  keys: [s]\n" +
                        "  values: [avg(l)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByStringFunctionWithFilter() throws Exception {
        assertPlan(
                "create table a (l long, s1 string, s2 string)",
                "select s1||s2 s, avg(l) a from a where l > 42",
                "Async Group By workers: 1\n" +
                        "  keys: [s]\n" +
                        "  values: [avg(l)]\n" +
                        "  filter: 42<l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupBySymbol() throws Exception {
        assertPlan(
                "create table a (l long, s symbol)",
                "select s, avg(l) a from a",
                "GroupBy vectorized: true workers: 1\n" +
                        "  keys: [s]\n" +
                        "  values: [avg(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupBySymbolFunction() throws Exception {
        assertPlan(
                "create table a (l long, s string)",
                "select s::symbol, avg(l) a from a",
                "Async Group By workers: 1\n" +
                        "  keys: [cast]\n" +
                        "  values: [avg(l)]\n" +
                        "  filter: null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupBySymbolWithSubQueryFilter() throws Exception {
        assertPlan(
                "create table a (l long, s symbol)",
                "select s, avg(l) a from a where s in (select s from a where s = 'key')",
                "Async Group By workers: 1\n" +
                        "  keys: [s]\n" +
                        "  values: [avg(l)]\n" +
                        "  filter: s in cursor \n" +
                        "    Filter filter: s='key'\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testGroupByWithLimit1() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x order by 1 limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x]\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit2() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x order by 1 desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [x desc]\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit3() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x limit 10",
                "Limit lo: 10\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit4() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di group by x limit -10",
                "Limit lo: -10\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit5a() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di where y = 5 group by x limit 10",
                "Limit lo: 10\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      filter: y=5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit5b() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di where y = 5 group by x limit -10",
                "Limit lo: -10\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      filter: y=5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit6a() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit 10",
                "Limit lo: 10\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      filter: abs(y)=5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit6b() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit -10",
                "Limit lo: -10\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      filter: abs(y)=5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit7() throws Exception {
        assertPlan(
                "create table di (x int, y long)",
                "select x, count(*) from di where abs(y) = 5 group by x limit 10, 20",
                "Limit lo: 10 hi: 20\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [x]\n" +
                        "      values: [count(*)]\n" +
                        "      filter: abs(y)=5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testGroupByWithLimit8() throws Exception {
        assertPlan(
                "create table di (x int, y long, ts timestamp) timestamp(ts)",
                "select ts, count(*) from di where y = 5 group by ts order by ts desc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [ts desc]\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [ts]\n" +
                        "      values: [count(*)]\n" +
                        "      filter: y=5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: di\n"
        );
    }

    @Test
    public void testHashInnerJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s1 string)");
            ddl("create table b ( i int, s2 string)");

            assertPlan(
                    "select s1, s2 from (select a.s1, b.s2, b.i, a.i  from a join b on i) where i < i1 and s1 = s2",
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
                            "                    Frame forward scan on: b\n"
            );
        });
    }

    @Test // inner hash join maintains order metadata and can be part of asof join
    public void testHashInnerJoinWithAsOf() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, ts1 timestamp) timestamp(ts1)");
            ddl("create table tabb (b1 int, b2 long)");
            ddl("create table tabc (c1 int, c2 long, ts3 timestamp) timestamp(ts3)");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertPlan(
                        compiler,
                        "select * " +
                                "from taba " +
                                "inner join tabb on a1=b1 " +
                                "asof join tabc on b1=c1",
                        "SelectedRecord\n" +
                                "    AsOf Join\n" +
                                "      condition: c1=b1\n" +
                                "        Hash Join\n" +
                                "          condition: b1=a1\n" +
                                "            DataFrame\n" +
                                "                Row forward scan\n" +
                                "                Frame forward scan on: taba\n" +
                                "            Hash\n" +
                                "                DataFrame\n" +
                                "                    Row forward scan\n" +
                                "                    Frame forward scan on: tabb\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: tabc\n",
                        sqlExecutionContext
                );
            }
        });
    }

    @Test
    public void testHashLeftJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int)");
            ddl("create table b ( i int)");

            assertPlan(
                    "select * from a left join b on i",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b.i=a.i\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testHashLeftJoin1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int)");
            ddl("create table b ( i int)");

            assertPlan(
                    "select * from a left join b on i where b.i is not null",
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
                            "                    Frame forward scan on: b\n"
            );
        });
    }

    @Ignore
    //FIXME
    //@Ignore("Fails with 'io.questdb.griffin.SqlException: [17] unexpected token: b'")
    @Test
    public void testImplicitJoin() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i1 int)");
            ddl("create table b ( i2 int)");

            assertSql("", "select * from a, b where a.i1 = b.i2");

            assertPlan(
                    "select * from a , b where a.i1 = b.i2",
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
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testInUuid() throws Exception {
        assertPlan(
                "create table a (u uuid, ts timestamp) timestamp(ts);",
                "select u, ts from a where u in ('11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', '33333333-3333-3333-3333-333333333333')",
                "Async Filter workers: 1\n" +
                        "  filter: u in ['22222222-2222-2222-2222-222222222222','11111111-1111-1111-1111-111111111111','33333333-3333-3333-3333-333333333333']\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testIntersect1() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
                "select * from a intersect select * from a",
                "Intersect\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testIntersect2() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
                "select * from a intersect select * from a where i > 0",
                "Intersect\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        Async JIT Filter workers: 1\n" +
                        "          filter: 0<i\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test
    public void testIntersectAll() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
                "select * from a intersect all select * from a",
                "Intersect All\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    Hash\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testIntersectAndSort1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts desc",
                    "Intersect\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testIntersectAndSort2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts asc",
                    "Intersect\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "    Hash\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testIntersectAndSort3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts desc limit 10) intersect (select * from a) order by ts asc",
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
                            "                Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testIntersectAndSort4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts asc limit 10) intersect (select * from a) order by ts desc",
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
                            "                Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testLatestByAllSymbolsFilteredFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table maps\n" +
                    "(\n" +
                    "  timestamp timestamp, \n" +
                    "  cluster symbol, \n" +
                    "  alias symbol, \n" +
                    "  octets int, \n" +
                    "  packets int\n" +
                    ") timestamp(timestamp);");

            insert("insert into maps values ('2023-09-01T09:41:00.000Z', 'cluster10', 'a', 1, 1), " +
                    "('2023-09-01T09:42:00.000Z', 'cluster10', 'a', 2, 2)");

            String sql = "select timestamp, cluster, alias, timestamp - timestamp1 interval, (octets-octets1)*8 bits, packets-packets1 packets from (\n" +
                    "  (select timestamp, cluster, alias, octets, packets\n" +
                    "  from maps\n" +
                    "  where cluster in ('cluster10') and timestamp BETWEEN '2023-09-01T09:40:27.286Z' AND '2023-09-01T10:40:27.286Z'\n" +
                    "  latest on timestamp partition by cluster,alias)\n" +
                    "  lt join maps on (cluster,alias)\n" +
                    "  ) order by bits desc\n" +
                    "limit 10";
            assertPlan(
                    sql,
                    "Limit lo: 10\n" +
                            "    Sort\n" +
                            "      keys: [bits desc]\n" +
                            "        VirtualRecord\n" +
                            "          functions: [timestamp,cluster,alias,timestamp-timestamp1,octets-octets1*8,packets-packets1]\n" +
                            "            SelectedRecord\n" +
                            "                Lt Join Light\n" +
                            "                  condition: maps.cluster=_xQdbA3.cluster and maps.alias=_xQdbA3.alias\n" +
                            "                    LatestByAllSymbolsFiltered\n" +
                            "                      filter: cluster in [cluster10]\n" +
                            "                        Row backward scan\n" +
                            "                          expectedSymbolsCount: 2147483647\n" +
                            "                        Interval backward scan on: maps\n" +
                            "                          intervals: [(\"2023-09-01T09:40:27.286000Z\",\"2023-09-01T10:40:27.286000Z\")]\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: maps\n"
            );

            assertSql("timestamp\tcluster\talias\tinterval\tbits\tpackets\n" +
                    "2023-09-01T09:42:00.000000Z\tcluster10\ta\t60000000\t8\t1\n", sql);
        });
    }

    @Test
    public void testLatestByRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab as ( " +
                    "  select " +
                    "    rnd_str('a','b','c') s, " +
                    "    timestamp_sequence(0, 100000000) ts " +
                    "   from long_sequence(100)" +
                    ") timestamp(ts) partition by hour");

            String sql = "with yy as (select ts, max(s) s from tab sample by 1h) " +
                    "select * from yy latest on ts partition by s limit 10";
            assertPlan(
                    sql,
                    "Limit lo: 10\n" +
                            "    LatestBy\n" +
                            "        SampleBy\n" +
                            "          values: [max(s)]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n"
            );

            assertSql("ts\ts\n" +
                    "1970-01-01T02:00:00.000000Z\tc\n", sql);
        });
    }

    @Test
    public void testLatestOn0() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from a latest on ts partition by i",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn0a() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from (select * from a where i = 10 union select * from a where i =20) latest on ts partition by i",
                "SelectedRecord\n" +
                        "    LatestBy\n" +
                        "        Union\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: i=10\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: i=20\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn0b() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select ts,i from a where s in ('ABC') and i > 0 latest on ts partition by s",
                "SelectedRecord\n" +
                        "    LatestByValueDeferredFiltered\n" +
                        "      filter: 0<i\n" +
                        "      symbolFilter: s='ABC'\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn0c() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertPlan(
                    "select ts,i from a where s in ('a1') and i > 0 latest on ts partition by s",
                    "SelectedRecord\n" +
                            "    LatestByValueFiltered\n" +
                            "        Row backward scan\n" +
                            "          symbolFilter: s=0\n" +
                            "          filter: 0<i\n" +
                            "        Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testLatestOn0d() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
            compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertPlan(
                    "select ts,i from a where s in ('a1') latest on ts partition by s",
                    "SelectedRecord\n" +
                            "    LatestByValueFiltered\n" +
                            "        Row backward scan\n" +
                            "          symbolFilter: s=0\n" +
                            "        Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testLatestOn0e() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

            assertPlan(
                    "select ts,i, s from a where s in ('a1') and i > 0 latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  filter: 0<i\n" +
                            "  symbolFilter: s=1\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testLatestOn1() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by i",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test // TODO: should use index
    public void testLatestOn10() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s = 'S1' or s = 'S2' latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  filter: (s='S1' or s='S2')\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn11() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in ('S1', 'S2') latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  includedSymbols: ['S1','S2']\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn12() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctSymbol\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "    Row backward scan on: s\n" +
                        "      filter: length(s)=2\n" +
                        "    Frame backward scan on: a\n"
        );

    }

    @Test
    public void testLatestOn12a() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctSymbol\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "    Row backward scan on: s\n" +
                        "    Frame backward scan on: a\n"
        );

    }

    @Test
    public void testLatestOn13() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctSymbol\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "    Index backward scan on: s\n" +
                        "      filter: length(s)=2\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn13a() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctSymbol\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "    Index backward scan on: s\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test // TODO: should use one or two indexes
    public void testLatestOn14() throws Exception {
        assertPlan(
                "create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' and i > 0 latest on ts partition by s1,s2",
                "LatestByAllSymbolsFiltered\n" +
                        "  filter: ((s1 in [S1,S2] and s2='S3') and 0<i)\n" +
                        "    Row backward scan\n" +
                        "      expectedSymbolsCount: 2\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test // TODO: should use one or two indexes
    public void testLatestOn15() throws Exception {
        assertPlan(
                "create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' latest on ts partition by s1,s2",
                "LatestByAllSymbolsFiltered\n" +
                        "  filter: (s1 in [S1,S2] and s2='S3')\n" +
                        "    Row backward scan\n" +
                        "      expectedSymbolsCount: 2\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn16() throws Exception {
        assertPlan(
                "create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 = 'S1' and ts > 0::timestamp latest on ts partition by s1,s2",
                "LatestByAllSymbolsFiltered\n" +
                        "  filter: s1='S1'\n" +
                        "    Row backward scan\n" +
                        "      expectedSymbolsCount: 2147483647\n" +
                        "    Interval backward scan on: a\n" +
                        "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"MAX\")]\n"
        );
    }

    @Test
    public void testLatestOn1a() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from (select ts, i as i1, i as i2 from a ) where 0 < i1 and i2 < 10 latest on ts partition by i1",
                "LatestBy light order_by_timestamp: true\n" +
                        "    SelectedRecord\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: (0<i and i<10)\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn1b() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select ts, i as i1, i as i2 from a where 0 < i and i < 10 latest on ts partition by i",
                "SelectedRecord\n" +
                        "    SelectedRecord\n" +
                        "        LatestByAllFiltered\n" +
                        "            Row backward scan\n" +
                        "              filter: (0<i and i<10)\n" +
                        "            Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn2() throws Exception {
        assertPlan(
                "create table a ( i int, d double, ts timestamp) timestamp(ts);",
                "select ts, d from a latest on ts partition by i",
                "SelectedRecord\n" +
                        "    LatestByAllFiltered\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn3() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by s",
                "LatestByAllIndexed\n" +
                        "    Index backward scan on: s parallel: true\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn4() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  = 'S1' latest on ts partition by s",
                "DataFrame\n" +
                        "    Index backward scan on: s deferred: true\n" +
                        "      filter: s='S1'\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn5a() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertPlan(
                    "select s, i, ts from a where s  in ('def1', 'def2') latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  symbolFilter: s in ['def1','def2']\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testLatestOn5b() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertPlan(
                    "select s, i, ts from a where s  in ('1', 'deferred') latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  symbolFilter: s in [1] or s in ['deferred']\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testLatestOn5c() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

            assertPlan(
                    "select s, i, ts from a where s  in ('1', '2') latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  symbolFilter: s in [1,2]\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testLatestOn6() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and i > 0 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: 0<i\n" +
                        "  symbolFilter: s in ['S1','S2']\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn7() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and length(s)<10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)<10\n" +
                        "  symbolFilter: s in ['S1','S2']\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn8() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            assertPlan(
                    "select s, i, ts from a where s  in ('s1') latest on ts partition by s",
                    "DataFrame\n" +
                            "    Index backward scan on: s\n" +
                            "      filter: s=1\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test // key outside list of symbols
    public void testLatestOn8a() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

            assertPlan(
                    "select s, i, ts from a where s  in ('bogus_key') latest on ts partition by s",
                    "DataFrame\n" +
                            "    Index backward scan on: s deferred: true\n" +
                            "      filter: s='bogus_key'\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test // columns in order different to table's
    public void testLatestOn9() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)=10\n" +
                        "  symbolFilter: s='S1'\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test // columns in table's order
    public void testLatestOn9a() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, s, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)=10\n" +
                        "  symbolFilter: s='S1'\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testLatestOn9b() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
            compile("insert into a select x::int, 'S' || x, x::timestamp from long_sequence(10)");

            assertPlan(
                    "select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                    "Index backward scan on: s\n" +
                            "  filter: length(s)=10\n" +
                            "  symbolFilter: s=1\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithEquality1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a int)");
            ddl("create table tabb (b int)");

            assertPlan(
                    "select * from taba left join tabb on a=b",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b=a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithEquality2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  and a2=b2",
                    "SelectedRecord\n" +
                            "    Hash Outer Join Light\n" +
                            "      condition: b2=a2 and b1=a1\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // FIXME: there should be no separate filter
    public void testLeftJoinWithEquality3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  or a2=b2",
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: (taba.a1=tabb.b1 or taba.a2=tabb.b2)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // FIXME: join and where clause filters should be separated
    public void testLeftJoinWithEquality4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  or a2=b2 where a1 > b2",
                    "SelectedRecord\n" +
                            "    Filter filter: tabb.b2<taba.a1\n" +
                            "        Nested Loop Left Join\n" +
                            "          filter: (taba.a1=tabb.b1 or taba.a2=tabb.b2)\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: taba\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // FIXME: ORed predicates should be applied as filter in hash join
    public void testLeftJoinWithEquality5() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba " +
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
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    // left join conditions aren't transitive because left record + null right is produced if they fail
    // that means select * from a left join b on a.i = b.i and a.i=10 doesn't mean resulting records will have a.i = 10 !
    @Test
    public void testLeftJoinWithEquality6() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");
            ddl("create table tabc (c1 int, c2 long)");

            assertPlan(
                    "select * from taba " +
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
                            "                Frame forward scan on: tabc\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithEquality7() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            testHashAndAsOfJoin(compiler, true);
        }
    }

    @Test
    public void testLeftJoinWithEquality8() throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.setFullFatJoins(true);
            testHashAndAsOfJoin(compiler, false);
        }
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2)",
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
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and a2+5 = b2+10",
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
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressions3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and a2+5 = b2+10 and 1=0",
                    "SelectedRecord\n" +
                            "    Hash Outer Join\n" +
                            "      condition: b2=a2 and b1=a1\n" +
                            "      filter: false\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        Hash\n" +
                            "            Empty table\n"
            );
        });
    }

    // FIXME provably false predicate like x!=x in left join means we can skip join and return left + nulls or join with empty right table
    @Test
    public void testLeftJoinWithEqualityAndExpressions4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  and a2!=a2",
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
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // FIXME: a2=a2 run as past of left join or be optimized away !
    public void testLeftJoinWithEqualityAndExpressions5() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  and a2=a2",
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
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // left join filter must remain intact !
    public void testLeftJoinWithEqualityAndExpressions6() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 string)");
            ddl("create table tabb (b1 int, b2 string)");

            assertPlan(
                    "select * from taba " +
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
                            "                Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // FIXME:  abs(a2+1) = abs(b2) should be applied as left join filter  !
    public void testLeftJoinWithEqualityAndExpressionsAhdWhere1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba " +
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
                            "                    Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressionsAhdWhere2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1  and a2=b2 and abs(a2+1) = abs(b2) where a1=b1",
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
                            "                    Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithEqualityAndExpressionsAhdWhere3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on a1=b1 and abs(a2+1) = abs(b2) where a2=b2",
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
                            "                    Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testLeftJoinWithExpressions1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on abs(a2+1) = abs(b2)",
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: abs(taba.a2+1)=abs(tabb.b2)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tabb\n"
            );
        });
    }

    @Test // FIXME: this should work as hash outer join of function results
    public void testLeftJoinWithExpressions2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, a2 long)");
            ddl("create table tabb (b1 int, b2 long)");

            assertPlan(
                    "select * from taba left join tabb on abs(a2+1) = abs(b2) or a2/2 = b2+1",
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: (abs(taba.a2+1)=abs(tabb.b2) or taba.a2/2=tabb.b2+1)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: taba\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testLeftJoinWithPostJoinFilter() throws Exception {
        assertMemoryLeak(() -> {
            compile("  CREATE TABLE tab ( created timestamp, value int ) timestamp(created)");

            String[] joinTypes = {"LEFT", "LT", "ASOF"};
            String[] joinFactoryTypes = {"Hash Outer Join Light", "Lt Join", "AsOf Join"};

            for (int i = 0; i < joinTypes.length; i++) {
                // do not push down predicate to the 'right' table of left join but apply it after join
                String joinType = joinTypes[i];
                String factoryType = joinFactoryTypes[i];

                assertPlan(
                        "SELECT count(1) " +
                                "FROM tab as T1 " +
                                joinType + " JOIN tab as T2 " + (i == 0 ? " ON T1.created=T2.created " : "") +
                                "WHERE not T2.value<>T2.value",
                        "GroupBy vectorized: false\n" +
                                "  values: [count(*)]\n" +
                                "    Filter filter: T2.value=T2.value\n" +
                                "        " + factoryType + "\n" +
                                (i == 0 ? "          condition: T2.created=T1.created\n" : "") +
                                "            DataFrame\n" +
                                "                Row forward scan\n" +
                                "                Frame forward scan on: tab\n" +
                                (i == 0 ? "            Hash\n" : "") +
                                (i == 0 ? "    " : "") + "            DataFrame\n" +
                                (i == 0 ? "    " : "") + "                Row forward scan\n" +
                                (i == 0 ? "    " : "") + "                Frame forward scan on: tab\n"
                );

                assertPlan(
                        "SELECT count(1) " +
                                "FROM tab as T1 " +
                                joinType + " JOIN tab as T2 " + (i == 0 ? " ON T1.created=T2.created " : "") +
                                "WHERE not T2.value=1",
                        "GroupBy vectorized: false\n" +
                                "  values: [count(*)]\n" +
                                "    Filter filter: T2.value!=1\n" +
                                "        " + factoryType + "\n" +
                                (i == 0 ? "          condition: T2.created=T1.created\n" : "") +
                                "            DataFrame\n" +
                                "                Row forward scan\n" +
                                "                Frame forward scan on: tab\n" +
                                (i == 0 ? "            Hash\n" : "") +
                                (i == 0 ? "    " : "") + "            DataFrame\n" +
                                (i == 0 ? "    " : "") + "                Row forward scan\n" +
                                (i == 0 ? "    " : "") + "                Frame forward scan on: tab\n"
                );

                // push down predicate to the 'left' table of left join
                assertPlan(
                        "SELECT count(1) " +
                                "FROM tab as T1 " +
                                joinType + " JOIN tab as T2 " + (i == 0 ? " ON T1.created=T2.created " : "") +
                                "WHERE not T1.value=1",
                        "GroupBy vectorized: false\n" +
                                "  values: [count(*)]\n" +
                                "    " + factoryType + "\n" +
                                (i == 0 ? "      condition: T2.created=T1.created\n" : "") +
                                "        Async JIT Filter workers: 1\n" +
                                "          filter: value!=1\n" +
                                "            DataFrame\n" +
                                "                Row forward scan\n" +
                                "                Frame forward scan on: tab\n" +
                                (i == 0 ? "        Hash\n" : "") +
                                (i == 0 ? "    " : "") + "        DataFrame\n" +
                                (i == 0 ? "    " : "") + "            Row forward scan\n" +
                                (i == 0 ? "    " : "") + "            Frame forward scan on: tab\n"
                );
            }


            // two joins
            assertPlan(
                    "SELECT count(1) " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created " +
                            "JOIN tab as T3 ON T2.created=T3.created " +
                            "WHERE T1.value=1",
                    "GroupBy vectorized: false\n" +
                            "  values: [count(*)]\n" +
                            "    Hash Join Light\n" +
                            "      condition: T3.created=T2.created\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: T2.created=T1.created\n" +
                            "            Async JIT Filter workers: 1\n" +
                            "              filter: value=1\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT count(1) " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created " +
                            "JOIN tab as T3 ON T2.created=T3.created " +
                            "WHERE T2.created=1",
                    "GroupBy vectorized: false\n" +
                            "  values: [count(*)]\n" +
                            "    Hash Join Light\n" +
                            "      condition: T3.created=T2.created\n" +
                            "        Filter filter: T2.created=1\n" +
                            "            Hash Outer Join Light\n" +
                            "              condition: T2.created=T1.created\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tab\n" +
                            "        Hash\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT count(1) " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created " +
                            "JOIN tab as T3 ON T2.created=T3.created " +
                            "WHERE T3.value=1",
                    "GroupBy vectorized: false\n" +
                            "  values: [count(*)]\n" +
                            "    Hash Join Light\n" +
                            "      condition: T3.created=T2.created\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: T2.created=T1.created\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "        Hash\n" +
                            "            Async JIT Filter workers: 1\n" +
                            "              filter: value=1\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n"
            );

            // where clause in parent model
            assertPlan(
                    "SELECT count(1) " +
                            "FROM ( " +
                            "SELECT * " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created ) e " +
                            "WHERE not value1<>value1",
                    "GroupBy vectorized: false\n" +
                            "  values: [count(*)]\n" +
                            "    SelectedRecord\n" +
                            "        Filter filter: T2.value=T2.value\n" +
                            "            Hash Outer Join Light\n" +
                            "              condition: T2.created=T1.created\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT count(1) " +
                            "FROM ( " +
                            "SELECT * " +
                            "FROM tab as T1 " +
                            "LEFT JOIN tab as T2 ON T1.created=T2.created ) e " +
                            "WHERE not value<>value",
                    "GroupBy vectorized: false\n" +
                            "  values: [count(*)]\n" +
                            "    SelectedRecord\n" +
                            "        Hash Outer Join Light\n" +
                            "          condition: T2.created=T1.created\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testLikeFilters() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (s1 string, s2 string, s3 string, s4 string, s5 string, s6 string);");

            assertPlan(
                    "select * from tab " +
                            "where s1 like '%a'  and s2 ilike '%a' " +
                            "  and s3 like 'a%'  and s4 ilike 'a%' " +
                            "  and s5 like '%a%' and s6 ilike '%a%';",
                    "Async Filter workers: 1\n" +
                            "  filter: (((((s1 like %a and s2 ilike %a) and s3 like a%) and s4 ilike a%) and s5 like %a%) and s6 ilike %a%)\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testLtJoin0() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select ts1, ts2, i1, i2 from (select a.i as i1, a.ts as ts1, b.i as i2, b.ts as ts2 from a lt join b on ts) where ts1::long*i1<ts2::long*i2",
                    "SelectedRecord\n" +
                            "    Filter filter: a.ts::long*a.i<b.ts::long*b.i\n" +
                            "        Lt Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testLtJoin1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a lt join b on ts",
                    "SelectedRecord\n" +
                            "    Lt Join Light\n" +
                            "      condition: b.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testLtJoin1a() throws Exception {
        // lt join guarantees that a.ts > b.ts [join cond is not an equality predicate]
        // CONCLUSION: a join b on X can't always be translated to a join b on a.X = b.X
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a lt join b on ts where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Lt Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testLtJoin1b() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a lt join b on ts where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Lt Join Light\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testLtJoin1c() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a lt join b where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Lt Join\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testLtJoin2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a lt join (select * from b limit 10) on ts",
                    "SelectedRecord\n" +
                            "    Lt Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testLtJoinFullFat() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                assertPlan(
                        compiler,
                        "select * " +
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
                                "            Frame forward scan on: b\n",
                        sqlExecutionContext
                );
            }
        });
    }

    @Test
    public void testLtOfJoin3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a lt join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                    "SelectedRecord\n" +
                            "    Lt Join Light\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Sort light\n" +
                            "          keys: [ts, i]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testLtOfJoin4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * " +
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
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testMultiExcept() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
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
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testMultiIntersect() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
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
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testMultiUnion() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
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
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testMultiUnionAll() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
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
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testNestedLoopLeftJoinWithSort1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");
            String[] queries = {"select * from t t1 left join t t2 on t1.x*t2.x>0 order by t1.ts",
                    "select * from (select * from t order by ts desc) t1 left join t t2 on t1.x*t2.x>0 order by t1.ts"};
            for (String query : queries) {
                assertPlan(
                        query,
                        "SelectedRecord\n" +
                                "    Nested Loop Left Join\n" +
                                "      filter: 0<t1.x*t2.x\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n" +
                                "        DataFrame\n" +
                                "            Row forward scan\n" +
                                "            Frame forward scan on: t\n"
                );

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
            ddl("create table t (x int, ts timestamp) timestamp(ts)");
            compile("insert into t select x, x::timestamp from long_sequence(2)");

            String query = "select * from " +
                    "((select * from t order by ts desc) limit 10) t1 " +
                    "left join t t2 on t1.x*t2.x > 0 " +
                    "order by t1.ts desc";

            assertPlan(
                    query,
                    "SelectedRecord\n" +
                            "    Nested Loop Left Join\n" +
                            "      filter: 0<t1.x*t2.x\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: t\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testOrderByAdvicePushdown() throws SqlException {
        // TODO: improve :
        // - limit propagation to async filter factory
        // - negative limit rewrite
        // if order by is via alias of designated timestamp
        ddl("create table device_data " +
                "( " +
                "  timestamp timestamp, " +
                "  val double, " +
                "  id symbol " +
                ") timestamp(timestamp)");

        insert("insert into device_data select x::timestamp, x, '12345678' from long_sequence(10)");

        // use column name in order by
        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY timestamp DESC " +
                        "LIMIT 1",
                "VirtualRecord\n" +
                        "  functions: [date,val,val+1]\n" +
                        "    SelectedRecord\n" +
                        "        Async JIT Filter workers: 1\n" +
                        "          limit: 1\n" +
                        "          filter: id='12345678'\n" +
                        "            DataFrame\n" +
                        "                Row backward scan\n" +
                        "                Frame backward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000010Z\t10.0\t11.0\n");

        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY timestamp  " +
                        "LIMIT -1",
                "VirtualRecord\n" +
                        "  functions: [date,val,val+1]\n" +
                        "    SelectedRecord\n" +
                        "        Async JIT Filter workers: 1\n" +
                        "          limit: 1\n" +
                        "          filter: id='12345678'\n" +
                        "            DataFrame\n" +
                        "                Row backward scan\n" +
                        "                Frame backward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000010Z\t10.0\t11.0\n");

        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY timestamp DESC " +
                        "LIMIT -2",
                "VirtualRecord\n" +
                        "  functions: [date,val,val+1]\n" +
                        "    SelectedRecord\n" +
                        "        Async JIT Filter workers: 1\n" +
                        "          limit: 2\n" +
                        "          filter: id='12345678'\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000002Z\t2.0\t3.0\n" +
                        "1970-01-01T00:00:00.000001Z\t1.0\t2.0\n");

        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY timestamp DESC " +
                        "LIMIT 1,3",
                "Limit lo: 1 hi: 3\n" +
                        "    VirtualRecord\n" +
                        "      functions: [date,val,val+1]\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: id='12345678'\n" +
                        "                DataFrame\n" +
                        "                    Row backward scan\n" +
                        "                    Frame backward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000009Z\t9.0\t10.0\n" +
                        "1970-01-01T00:00:00.000008Z\t8.0\t9.0\n");

        // use alias in order by
        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY date DESC " +
                        "LIMIT 1",
                "Limit lo: 1\n" +
                        "    VirtualRecord\n" +
                        "      functions: [date,val,val+1]\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: id='12345678'\n" +
                        "                DataFrame\n" +
                        "                    Row backward scan\n" +
                        "                    Frame backward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000010Z\t10.0\t11.0\n");

        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY date  " +
                        "LIMIT -1",
                "Limit lo: -1\n" +
                        "    VirtualRecord\n" +
                        "      functions: [date,val,val+1]\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: id='12345678'\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000010Z\t10.0\t11.0\n");

        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY date DESC " +
                        "LIMIT -2",
                "Limit lo: -2\n" +
                        "    VirtualRecord\n" +
                        "      functions: [date,val,val+1]\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: id='12345678'\n" +
                        "                DataFrame\n" +
                        "                    Row backward scan\n" +
                        "                    Frame backward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000002Z\t2.0\t3.0\n" +
                        "1970-01-01T00:00:00.000001Z\t1.0\t2.0\n");

        assertSqlAndPlan("SELECT timestamp AS date, val, val + 1 " +
                        "FROM device_data " +
                        "WHERE device_data.id = '12345678' " +
                        "ORDER BY date DESC " +
                        "LIMIT 1,3",
                "Limit lo: 1 hi: 3\n" +
                        "    VirtualRecord\n" +
                        "      functions: [date,val,val+1]\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter workers: 1\n" +
                        "              filter: id='12345678'\n" +
                        "                DataFrame\n" +
                        "                    Row backward scan\n" +
                        "                    Frame backward scan on: device_data\n",
                "date\tval\tcolumn\n" +
                        "1970-01-01T00:00:00.000009Z\t9.0\t10.0\n" +
                        "1970-01-01T00:00:00.000008Z\t8.0\t9.0\n");

    }

    @Test
    public void testOrderByIsMaintainedInLtAndAsofSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table gas_prices (timestamp TIMESTAMP, galon_price DOUBLE ) timestamp (timestamp);");

            for (String joinType : Arrays.asList("AsOf", "Lt")) {
                String query = "with gp as \n" +
                        "(\n" +
                        "selecT * from (\n" +
                        "selecT * from gas_prices order by timestamp asc, galon_price desc\n" +
                        ") timestamp(timestamp))\n" +
                        "selecT * from gp gp1 \n" +
                        joinType + " join gp gp2 \n" +
                        "order by gp1.timestamp; ";

                String expectedPlan = "SelectedRecord\n" +
                        "    " + joinType + " Join\n" +
                        "        Sort light\n" +
                        "          keys: [timestamp, galon_price desc]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: gas_prices\n" +
                        "        Sort light\n" +
                        "          keys: [timestamp, galon_price desc]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: gas_prices\n";

                assertPlan(query, expectedPlan);
            }
        });
    }

    @Test
    public void testOrderByIsMaintainedInSpliceSubqueries() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table gas_prices (timestamp TIMESTAMP, galon_price DOUBLE ) timestamp (timestamp);");

            String query = "with gp as (\n" +
                    "selecT * from (\n" +
                    "selecT * from gas_prices order by timestamp asc, galon_price desc\n" +
                    ") timestamp(timestamp))\n" +
                    "selecT * from gp gp1 \n" +
                    "splice join gp gp2 \n" +
                    "order by gp1.timestamp; ";

            String expectedPlan = "Sort\n" +
                    "  keys: [timestamp]\n" +
                    "    SelectedRecord\n" +
                    "        Splice Join\n" +
                    "            Sort light\n" +
                    "              keys: [timestamp, galon_price desc]\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: gas_prices\n" +
                    "            Sort light\n" +
                    "              keys: [timestamp, galon_price desc]\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: gas_prices\n";

            assertPlan(query, expectedPlan);
        });
    }

    @Test
    public void testOrderByIsMaintainedInSubquery() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table gas_prices " +
                    "(timestamp TIMESTAMP, " +
                    "galon_price DOUBLE) " +
                    "timestamp (timestamp);");

            String query = "WITH full_range AS (  \n" +
                    "  SELECT timestamp, galon_price FROM (\n" +
                    "    SELECT * FROM (\n" +
                    "      SELECT * FROM gas_prices\n" +
                    "      UNION\n" +
                    "      SELECT to_timestamp('1999-01-01', 'yyyy-MM-dd'), NULL as galon_price -- First Date\n" +
                    "      UNION \n" +
                    "      SELECT to_timestamp('2023-02-20', 'yyyy-MM-dd'), NULL  as galon_price -- Last Date\n" +
                    "    ) AS unordered_data \n" +
                    "    order by timestamp\n" +
                    "  ) TIMESTAMP(timestamp)\n" +
                    ") \n" +
                    "select * " +
                    "from full_range";

            String expectedPlan = "SelectedRecord\n" +
                    "    Sort\n" +
                    "      keys: [timestamp]\n" +
                    "        Union\n" +
                    "            Union\n" +
                    "                DataFrame\n" +
                    "                    Row forward scan\n" +
                    "                    Frame forward scan on: gas_prices\n" +
                    "                VirtualRecord\n" +
                    "                  functions: [915148800000000,null]\n" +
                    "                    long_sequence count: 1\n" +
                    "            VirtualRecord\n" +
                    "              functions: [1676851200000000,null]\n" +
                    "                long_sequence count: 1\n";
            assertPlan(query, expectedPlan);
            assertPlan(query + " order by timestamp", expectedPlan);
        });
    }

    @Test
    public void testOrderByTimestampAndOtherColumns1() throws Exception {
        assertPlan(
                "create table tab (i int, ts timestamp) timestamp(ts)",
                "select * from (select * from tab order by ts, i desc limit 10) order by ts",
                "Sort light lo: 10 partiallySorted: true\n" +
                        "  keys: [ts, i desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testOrderByTimestampAndOtherColumns2() throws Exception {
        assertPlan(
                "create table tab (i int, ts timestamp) timestamp(ts)",
                "select * from (select * from tab order by ts desc, i asc limit 10) order by ts desc",
                "Sort light lo: 10\n" +
                        "  keys: [ts desc, i]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testPostJoinConditionColumnsAreResolved() throws Exception {
        assertMemoryLeak(() -> {
            String query = "SELECT count(*)\n" +
                    "FROM test as T1\n" +
                    "JOIN ( SELECT * FROM test ) as T2 ON T1.event < T2.event\n" +
                    "JOIN test as T3 ON T2.created = T3.created";

            ddl("create table test (event int, created timestamp)");
            insert("insert into test values (1, 1), (2, 2)");

            assertPlan(query,
                    "Count\n" +
                            "    Filter filter: T1.event<T2.event\n" +
                            "        Cross Join\n" +
                            "            Hash Join Light\n" +
                            "              condition: T3.created=T2.created\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: test\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: test\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: test\n"
            );

            assertSql("count\n1\n", query);
        });
    }

    @Test
    public void testPredicatesArentPushedIntoWindowModel() throws Exception {
        assertMemoryLeak(() -> {
            String query = "SELECT * " +
                    "FROM ( " +
                    "  SELECT *, ROW_NUMBER() OVER ( PARTITION BY a ORDER BY b ) rownum " +
                    "  FROM (" +
                    "    SELECT 1 a, 2 b, 4 c " +
                    "    UNION " +
                    "    SELECT 1, 3, 5 " +
                    "  ) o " +
                    ") ra " +
                    "WHERE ra.rownum = 1 " +
                    "AND   c = 5";

            assertPlan(query,
                    "Filter filter: (rownum=1 and c=5)\n" +
                            "    CachedWindow\n" +
                            "      orderedFunctions: [[b] => [row_number() over (partition by [a])]]\n" +
                            "        Union\n" +
                            "            VirtualRecord\n" +
                            "              functions: [1,2,4]\n" +
                            "                long_sequence count: 1\n" +
                            "            VirtualRecord\n" +
                            "              functions: [1,3,5]\n" +
                            "                long_sequence count: 1\n");
            assertSql("a\tb\tc\trownum\n", query);

            ddl("CREATE TABLE tab AS (SELECT x FROM long_sequence(10))");

            assertPlan("SELECT *, ROW_NUMBER() OVER () FROM tab WHERE x = 10",
                    "Window\n" +
                            "  functions: [row_number()]\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      filter: x=10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");

            assertPlan("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab ) WHERE x = 10",
                    "Filter filter: x=10\n" +
                            "    Window\n" +
                            "      functions: [row_number()]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n");

            assertPlan("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab UNION ALL select 11, 11  ) WHERE x = 10",
                    "Filter filter: x=10\n" +
                            "    Union All\n" +
                            "        Window\n" +
                            "          functions: [row_number()]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: tab\n" +
                            "        VirtualRecord\n" +
                            "          functions: [11,11]\n" +
                            "            long_sequence count: 1\n");

            assertPlan("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab cross join (select 11, 11)  ) WHERE x = 10",
                    "Filter filter: x=10\n" +
                            "    Window\n" +
                            "      functions: [row_number()]\n" +
                            "        SelectedRecord\n" +
                            "            Cross Join\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "                VirtualRecord\n" +
                            "                  functions: [11,11]\n" +
                            "                    long_sequence count: 1\n");

            assertPlan("SELECT * FROM (SELECT *, ROW_NUMBER() OVER () FROM tab ) join (select 11L y, 11) on x=y WHERE x = 10",
                    "SelectedRecord\n" +
                            "    Hash Join Light\n" +
                            "      condition: y=x\n" +
                            "        Filter filter: x=10\n" +
                            "            Window\n" +
                            "              functions: [row_number()]\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tab\n" +
                            "        Hash\n" +
                            "            Filter filter: y=10\n" +
                            "                VirtualRecord\n" +
                            "                  functions: [11L,11]\n" +
                            "                    long_sequence count: 1\n");
        });
    }

    @Test
    public void testRewriteAggregateWithAdditionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x double );");

            assertPlan(
                    "SELECT sum(x), sum(x+10) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(x+10)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10+x) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(10+x)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithAdditionOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE taba ( x int, id int );");
            compile("CREATE TABLE tabb ( x int, id int );");

            assertPlan(
                    "SELECT sum(taba.x),sum(tabb.x), sum(taba.x+10), sum(tabb.x+10) " +
                            "FROM taba " +
                            "join tabb on (id)",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum1,sum+COUNT*10,sum1+COUNT1*10]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(x),sum(x1),count(x),count(x1)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: tabb.id=taba.id\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: taba\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tabb\n"
            );

            assertPlan(
                    "SELECT sum(tabb.x),sum(taba.x),sum(10+taba.x), sum(10+tabb.x) " +
                            "FROM taba " +
                            "join tabb on (id)",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum1,COUNT*10+sum1,COUNT1*10+sum]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(x),sum(x1),count(x1),count(x)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: tabb.id=taba.id\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: taba\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithIntAddition() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x int );");

            assertPlan(
                    "SELECT sum(x), sum(x+10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum+COUNT*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10+x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*10+sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithIntMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x int );");

            assertPlan(
                    "SELECT sum(x), sum(x*10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10*x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,10*sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithIntSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x int );");

            assertPlan(
                    "SELECT sum(x), sum(x-10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum-COUNT*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10-x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*10-sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithLongAddition() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x long );");

            assertPlan(
                    "SELECT sum(x), sum(x+2) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum+COUNT*2]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(2+x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*2+sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithLongMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x long );");

            assertPlan(
                    "SELECT sum(x), sum(x*10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10*x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,10*sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithLongSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x long );");

            assertPlan(
                    "SELECT sum(x), sum(x-10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum-COUNT*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10-x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*10-sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationIsDisabledForNonIntegerColumnType() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x double );");

            assertPlan(
                    "SELECT sum(x), sum(x*10) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(x*10)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10*x) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(10*x)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationIsDisabledForNonIntegerConstantType() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x double );");

            assertPlan(
                    "SELECT sum(x), sum(x*10.0) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(x*10.0)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10.0*x) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(10.0*x)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithMultiplicationOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE taba ( x int, id int );");
            compile("CREATE TABLE tabb ( x int, id int );");

            assertPlan(
                    "SELECT sum(taba.x),sum(tabb.x),sum(taba.x*10), sum(tabb.x*10) " +
                            "FROM taba " +
                            "join tabb on (id)",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum1,sum*10,sum1*10]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(x),sum(x1)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: tabb.id=taba.id\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: taba\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tabb\n"
            );

            assertPlan(
                    "SELECT sum(taba.x),sum(tabb.x),sum(10*taba.x), sum(10*tabb.x) " +
                            "FROM taba " +
                            "join tabb on (id)",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum1,10*sum,10*sum1]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(x),sum(x1)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: tabb.id=taba.id\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: taba\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithShortAddition() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x short );");

            assertPlan(
                    "SELECT sum(x), sum(x+42) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum+COUNT*42]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(*)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(42+x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*42+sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(*)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithShortMultiplication() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x short );");

            assertPlan(
                    "SELECT sum(x), sum(x*10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10*x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,10*sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithShortSubtraction() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x short );");

            assertPlan(
                    "SELECT sum(x), sum(x-10) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum-COUNT*10]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(*)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10-x) FROM tab",
                    "VirtualRecord\n" +
                            "  functions: [sum,COUNT*10-sum]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(x),count(*)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithSubtractionIsDisabledForNonIntegerType() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE tab ( x double );");

            assertPlan(
                    "SELECT sum(x), sum(x-10) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(x-10)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );

            assertPlan(
                    "SELECT sum(x), sum(10-x) FROM tab",
                    "Async Group By workers: 1\n" +
                            "  values: [sum(x),sum(10-x)]\n" +
                            "  filter: null\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n"
            );
        });
    }

    @Test
    public void testRewriteAggregateWithSubtractionOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE taba ( x int, id int );");
            compile("CREATE TABLE tabb ( x int, id int );");

            assertPlan(
                    "SELECT sum(taba.x),sum(tabb.x),sum(taba.x-10), sum(tabb.x-10) " +
                            "FROM taba " +
                            "join tabb on (id)",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum1,sum-COUNT*10,sum1-COUNT1*10]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(x),sum(x1),count(x),count(x1)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: tabb.id=taba.id\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: taba\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tabb\n"
            );

            assertPlan(
                    "SELECT sum(taba.x),sum(tabb.x),sum(10-taba.x), sum(10-tabb.x) " +
                            "FROM taba " +
                            "join tabb on (id)",
                    "VirtualRecord\n" +
                            "  functions: [sum,sum1,COUNT*10-sum,COUNT1*10-sum1]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(x),sum(x1),count(x),count(x1)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: tabb.id=taba.id\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: taba\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: tabb\n"
            );
        });
    }

    @Test
    public void testRewriteAggregates() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE hits\n" +
                    "(\n" +
                    "    EventTime timestamp,\n" +
                    "    ResolutionWidth int,\n" +
                    "    ResolutionHeight int\n" +
                    ") TIMESTAMP(EventTime) PARTITION BY DAY;");

            assertPlan(
                    "SELECT sum(resolutIONWidth), count(resolutionwIDTH), SUM(ResolutionWidth), sum(ResolutionWidth) + count(), " +
                            "SUM(ResolutionWidth+1),SUM(ResolutionWidth*2),sUM(ResolutionWidth), count()\n" +
                            "FROM hits",
                    "VirtualRecord\n" +
                            "  functions: [sum,count,sum,sum+count1,sum+count*1,sum*2,sum,count1]\n" +
                            "    GroupBy vectorized: true\n" +
                            "      values: [sum(resolutIONWidth),count(resolutIONWidth),count(*)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: hits\n"
            );
        });
    }

    @Test
    public void testRewriteAggregatesOnJoin() throws Exception {
        assertMemoryLeak(() -> {
            compile("CREATE TABLE hits1" +
                    "(" +
                    "    EventTime timestamp, " +
                    "    ResolutionWidth int, " +
                    "    ResolutionHeight int, " +
                    "    id int" +
                    ")");
            ddl("create table hits2 as (select * from hits1)");

            assertPlan(
                    "SELECT sum(h1.resolutIONWidth), count(h1.resolutionwIDTH), SUM(h2.ResolutionWidth), sum(h2.ResolutionWidth) + count(), " +
                            "SUM(h1.ResolutionWidth+1),SUM(h2.ResolutionWidth*2),sUM(h1.ResolutionWidth), count()\n" +
                            "FROM hits1 h1 " +
                            "join hits2 h2 on (id)",
                    "VirtualRecord\n" +
                            "  functions: [sum,count,SUM1,SUM1+count1,sum+count*1,SUM1*2,sum,count1]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(resolutIONWidth),count(resolutIONWidth),sum(ResolutionWidth1),count(*)]\n" +
                            "        SelectedRecord\n" +
                            "            Hash Join Light\n" +
                            "              condition: h2.id=h1.id\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: hits1\n" +
                            "                Hash\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: hits2\n"
            );
        });
    }

    @Test
    public void testRewriteSelectCountDistinct() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table test(s string, x long, ts timestamp, substring string) timestamp(ts) partition by day");
            insert("insert into test " +
                    "select 's' || (x%10), " +
                    " x, " +
                    " (x*86400000000)::timestamp, " +
                    " 'substring' " +
                    "from long_sequence(10)");

            // multiple count_distinct, no re-write
            assertPlan("SELECT count_distinct(s), count_distinct(x) FROM test",
                    "GroupBy vectorized: false\n" +
                            "  values: [count_distinct(s),count_distinct(x)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: test\n");

            // no where clause, distinct constant
            assertPlan("SELECT count_distinct(10) FROM test",
                    "Count\n" +
                            "    GroupBy vectorized: false\n" +
                            "      keys: [10]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            // no where clause, distinct column
            assertPlan("SELECT count_distinct(s) FROM test",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [s]\n" +
                            "      filter: s is not null \n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            // with where clause, distinct column
            assertPlan("SELECT count_distinct(s) FROM test where s like '%abc%'",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [s]\n" +
                            "      filter: (s like %abc% and s is not null )\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            // no where clause, distinct expression 1
            assertPlan("SELECT count_distinct(substring(s,1,1)) FROM test ",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [substring]\n" +
                            "      filter: substring(s,1,1) is not null \n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            // where clause, distinct expression 2
            assertPlan("SELECT count_distinct(substring(s,1,1)) FROM test where s like '%abc%'",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [substring]\n" +
                            "      filter: (s like %abc% and substring(s,1,1) is not null )\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            // where clause, distinct expression 3, function name clash with column name
            assertPlan("SELECT count_distinct(substring(s,1,1)) FROM test where s like '%abc%' and substring != null",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [substring]\n" +
                            "      filter: ((s like %abc% and substring is not null ) and substring(s,1,1) is not null )\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            // where clause, distinct expression 3
            assertPlan("SELECT count_distinct(x+1) FROM test where x > 5",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column]\n" +
                            "      filter: (5<x and null!=x+1)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            // where clause, distinct expression, col alias
            assertPlan("SELECT count_distinct(x+1) cnt_dst FROM test where x > 5",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column]\n" +
                            "      filter: (5<x and null!=x+1)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            assertSql("cnt_dst\n" +
                            "5\n",
                    "SELECT count_distinct(x+1) cnt_dst FROM test where x > 5");

            // where clause, distinct expression, table alias
            assertPlan("SELECT count_distinct(x+1) FROM test tab where x > 5",
                    "Count\n" +
                            "    Async Group By workers: 1\n" +
                            "      keys: [column]\n" +
                            "      filter: (5<x and null!=x+1)\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: test\n");

            assertSql("count_distinct\n" +
                            "5\n",
                    "SELECT count_distinct(x+1) FROM test tab where x > 5");
        });
    }

    @Test
    public void testSampleBy() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h",
                "SampleBy\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByFillLinear() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(linear)",
                "SampleBy\n" +
                        "  fill: linear\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByFillNull() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(null)",
                "SampleBy\n" +
                        "  fill: null\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByFillPrevKeyed() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, first(i) from a sample by 1h fill(prev)",
                "SampleBy\n" +
                        "  fill: prev\n" +
                        "  keys: [s]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByFillPrevNotKeyed() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(prev)",
                "SampleByFillPrev\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByFillValueKeyed() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, first(i) from a sample by 1h fill(1)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  keys: [s]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByFillValueNotKeyed() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(1)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByFirstLast() throws Exception {
        assertPlan(
                "create table a ( l long, s symbol, sym symbol index, i int, ts timestamp) timestamp(ts) partition by day;",
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
                        "          intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.000099Z\")]\n"
        );
    }

    @Test
    public void testSampleByKeyed0() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h",
                "SampleBy\n" +
                        "  keys: [l,i]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByKeyed1() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h",
                "SampleBy\n" +
                        "  keys: [l,i]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByKeyed2() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1h fill(null)",
                "SampleBy\n" +
                        "  fill: null\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByKeyed3() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1d fill(linear)",
                "SampleBy\n" +
                        "  fill: linear\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByKeyed4() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(1,2)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i),last(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSampleByKeyed5() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(prev,prev)",
                "SampleBy\n" +
                        "  fill: value\n" +
                        "  keys: [l]\n" +
                        "  values: [first(i),last(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelect0() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectConcat() throws Exception {
        assertPlan(
                "select concat('a', 'b', rnd_str('c', 'd', 'e'))",
                "VirtualRecord\n" +
                        "  functions: [concat(['a','b',rnd_str([c,d,e])])]\n" +
                        "    long_sequence count: 1\n"
        );
    }

    @Test
    public void testSelectCount1() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select count(*) from a",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount10() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select 1 from a limit 1) ",
                "Count\n" +
                        "    Limit lo: 1\n" +
                        "        VirtualRecord\n" +
                        "          functions: [1]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test // TODO: should return count on first table instead
    public void testSelectCount11() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a lt join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Lt Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test // TODO: should return count on first table instead
    public void testSelectCount12() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a asof join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        AsOf Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test // TODO: should return count(first table)*count(second_table) instead
    public void testSelectCount13() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a cross join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Cross Join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount14() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts)",
                "select * from a where s = 'S1' order by ts desc ",
                "DeferredSingleSymbolFilterDataFrame\n" +
                        "    Index backward scan on: s deferred: true\n" +
                        "      filter: s='S1'\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount15() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index, ts timestamp) timestamp(ts)",
                "select * from a where s = 'S1' order by ts asc",
                "DeferredSingleSymbolFilterDataFrame\n" +
                        "    Index forward scan on: s deferred: true\n" +
                        "      filter: s='S1'\n" +
                        "    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount16() throws Exception {
        assertPlan(
                "create table a (i long, j long)",
                "select count(*) from (select i, j from a group by i, j)",
                "Count\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [i,j]\n" +
                        "      filter: null\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount17() throws Exception {
        assertPlan(
                "create table a (i long, j long, d double)",
                "select count(*) from (select i, j from a where d > 42 group by i, j)",
                "Count\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [i,j]\n" +
                        "      filter: 42<d\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount2() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select count() from a",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // TODO: this should use Count factory same as queries above
    public void testSelectCount3() throws Exception {
        assertPlan(
                "create table a ( i int, d double)",
                "select count(2) from a",
                "GroupBy vectorized: false\n" +
                        "  values: [count(*)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount4() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index)",
                "select count(*) from a where s = 'S1'",
                "Count\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount5() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union all select * from a) ",
                "Count\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount6() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union select * from a) ",
                "Count\n" +
                        "    Union\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount7() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index)",
                "select count(*) from (select * from a intersect select * from a) ",
                "Count\n" +
                        "    Intersect\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCount8() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index)",
                "select count(*) from a where 1=0 ",
                "Count\n" +
                        "    Empty table\n"
        );
    }

    @Test
    public void testSelectCount9() throws Exception {
        assertPlan(
                "create table a ( i int, s symbol index)",
                "select count(*) from a where 1=1 ",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectCountDistinct1() throws Exception {
        assertPlan(
                "create table tab ( s symbol, ts timestamp);",
                "select count_distinct(s) from tab",
                "GroupBy vectorized: false\n" +
                        "  values: [count_distinct(s)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectCountDistinct2() throws Exception {
        assertPlan(
                "create table tab ( s symbol index, ts timestamp);",
                "select count_distinct(s) from tab",
                "GroupBy vectorized: false\n" +
                        "  values: [count_distinct(s)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectCountDistinct3() throws Exception {
        assertPlan(
                "create table tab ( s string, l long )",
                "select count_distinct(l) from tab",
                "Count\n" +
                        "    Async Group By workers: 1\n" +
                        "      keys: [l]\n" +
                        "      filter: null!=l\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectDesc() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc",
                "DataFrame\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectDesc2() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) ;",
                "select * from a order by ts desc",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectDescMaterialized() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) ;",
                "select * from (select i, ts from a union all select 1, null ) order by ts desc",
                "Sort\n" +
                        "  keys: [ts desc]\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        VirtualRecord\n" +
                        "          functions: [1,null]\n" +
                        "            long_sequence count: 1\n"
        );
    }

    @Test
    public void testSelectDistinct0() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct l, ts from tab",
                "DistinctTimeSeries\n" +
                        "  keys: l,ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Ignore
    @Test // FIXME: somehow only ts gets included, pg returns record type
    public void testSelectDistinct0a() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct (l, ts) from tab",
                "DistinctTimeSeries\n" +
                        "  keys: l,ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectDistinct1() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select distinct(l) from tab",
                "Distinct\n" +
                        "  keys: l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectDistinct2() throws Exception {
        assertPlan(
                "create table tab ( s symbol, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctSymbol\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectDistinct3() throws Exception {
        assertPlan(
                "create table tab ( s symbol index, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctSymbol\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectDistinct4() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select distinct ts, l  from tab",
                "Distinct\n" +
                        "  keys: ts,l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectDoubleInList() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t ( d double)");

            assertPlan(
                    "select * from t where d in (5, -1, 1, null)",
                    "Async Filter workers: 1\n" +
                            "  filter: d in [-1.0,1.0,5.0,NaN]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertPlan(
                    "select * from t where d not in (5, -1, 1, null)",
                    "Async Filter workers: 1\n" +
                            "  filter: not (d in [-1.0,1.0,5.0,NaN])\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test // there's no interval scan because sysdate is evaluated per-row
    public void testSelectDynamicTsInterval1() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > sysdate()",
                "Async Filter workers: 1\n" +
                        "  filter: sysdate()<ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // there's no interval scan because systimestamp is evaluated per-row
    public void testSelectDynamicTsInterval2() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > systimestamp()",
                "Async Filter workers: 1\n" +
                        "  filter: systimestamp()<ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectDynamicTsInterval3() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"MAX\")]\n"
        );
    }

    @Test
    public void testSelectDynamicTsInterval4() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > dateadd('d', -1, now()) and ts < now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [(\"1969-12-31T00:00:00.000001Z\",\"1969-12-31T23:59:59.999999Z\")]\n"
        );
    }

    @Test
    public void testSelectDynamicTsInterval5() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [(\"2022-01-01T00:00:00.000001Z\",\"MAX\")]\n"
        );
    }

    @Test
    public void testSelectDynamicTsInterval6() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now() order by ts desc",
                "DataFrame\n" +
                        "    Row backward scan\n" +
                        "    Interval backward scan on: tab\n" +
                        "      intervals: [(\"2022-01-01T00:00:00.000001Z\",\"MAX\")]\n"
        );
    }

    @Test
    public void testSelectFromAllTables() throws Exception {
        assertPlan(
                "select * from all_tables()",
                "all_tables()\n"
        );
    }

    @Test
    public void testSelectFromMemoryMetrics() throws Exception {
        assertPlan(
                "select * from memory_metrics()",
                "memory_metrics\n"
        );
    }

    @Test
    public void testSelectFromReaderPool() throws Exception {
        assertPlan(
                "select * from reader_pool()",
                "reader_pool\n"
        );
    }

    @Test
    public void testSelectFromTableColumns() throws Exception {
        assertPlan(
                "create table tab ( s string, sy symbol, i int, ts timestamp)",
                "select * from table_columns('tab')",
                "show_columns of: tab\n"
        );
    }

    @Test
    public void testSelectFromTablePartitions() throws Exception {
        assertPlan(
                "create table tab ( s string, sy symbol, i int, ts timestamp)",
                "select * from table_partitions('tab')",
                "show_partitions of: tab\n"
        );
    }

    @Test
    public void testSelectFromTableWriterMetrics() throws Exception {
        assertPlan(
                "select * from table_writer_metrics()",
                "table_writer_metrics\n"
        );
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsAscNotPartitioned() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' order by ts desc limit 1 ",
                "Limit lo: 1\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index backward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsAscPartitioned() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day;",
                "select * from a where s = 'S1' order by ts desc limit 1 ",
                "Limit lo: 1\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index backward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsDescNotPartitioned() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' order by ts desc limit 1 ",
                "Limit lo: 1\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index backward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLoOrderByTsDescPartitioned() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day;",
                "select * from a where s = 'S1' order by ts desc limit 1 ",
                "Limit lo: 1\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index backward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols01a() throws Exception {
        //if query is ordered by symbol and there's only one partition to scan, there's no need to sort
        testSelectIndexedSymbol("");
        testSelectIndexedSymbol("timestamp(ts)");
        testSelectIndexedSymbolWithIntervalFilter();
    }

    @Test
    public void testSelectIndexedSymbols01b() throws Exception {
        //if query is ordered by symbol and there's more than partition to scan, then sort is necessary even if we use cursor order scan
        assertMemoryLeak(() -> {
            ddl("create table a ( s symbol index, ts timestamp)  timestamp(ts) partition by hour");
            insert("insert into a values ('S2', 0), ('S1', 1), ('S3', 2+3600000000), ( 'S2' ,3+3600000000)");

            String queryDesc = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s desc limit 5";
            bindVariableService.clear();
            bindVariableService.setStr("s1", "S1");
            bindVariableService.setStr("s2", "S2");

            String expectedPlan = "Sort light lo: 5\n" +
                    "  keys: [s#ORDER#]\n" +
                    "    FilterOnValues symbolOrder: desc\n" +
                    "        Cursor-order scan\n" +
                    "            Index forward scan on: s deferred: true\n" +
                    "              filter: s=:s1::string\n" +
                    "            Index forward scan on: s deferred: true\n" +
                    "              filter: s=:s2::string\n" +
                    "        Interval forward scan on: a\n" +
                    "          intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T23:59:59.999999Z\")]\n";

            assertPlan(queryDesc, expectedPlan.replace("#ORDER#", " desc"));
            assertQuery("s\tts\n" +
                    "S2\t1970-01-01T01:00:00.000003Z\n" +
                    "S2\t1970-01-01T00:00:00.000000Z\n" +
                    "S1\t1970-01-01T00:00:00.000001Z\n", queryDesc, null, true, true);

            //order by asc
            String queryAsc = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s asc limit 5";
            assertPlan(queryAsc, expectedPlan.replace("#ORDER#", ""));
            assertQuery("s\tts\n" +
                    "S1\t1970-01-01T00:00:00.000001Z\n" +
                    "S2\t1970-01-01T01:00:00.000003Z\n" +
                    "S2\t1970-01-01T00:00:00.000000Z\n", queryAsc, null, true, true);
        });
    }

    @Test
    public void testSelectIndexedSymbols01c() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select ts, s from a where s in ('S1', 'S2') and length(s) = 2 order by s desc limit 1",
                "Limit lo: 1\n" +
                        "    FilterOnValues symbolOrder: desc\n" +
                        "        Cursor-order scan\n" + //actual order is S2, S1
                        "            Index forward scan on: s deferred: true\n" +
                        "              symbolFilter: s='S1'\n" +
                        "              filter: length(s)=2\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              symbolFilter: s='S2'\n" +
                        "              filter: length(s)=2\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testSelectIndexedSymbols02() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = $1 or s = $2 order by ts desc limit 1",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: (s=$0::string or s=$1::string)\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test // TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !
    public void testSelectIndexedSymbols03() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' or s = 'S2' order by ts desc limit 1",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: (s='S1' or s='S2')\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test // TODO: it would be better to get rid of unnecessary sort and limit factories
    public void testSelectIndexedSymbols04() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' and s = 'S2' order by ts desc limit 1",
                "Limit lo: 1\n" +
                        "    Sort\n" +
                        "      keys: [ts desc]\n" +
                        "        Empty table\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols05() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
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
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols05a() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
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
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols06() throws Exception {
        assertPlan(
                "create table a ( s symbol index) ;",
                "select * from a where s = 'S1' order by s asc limit 10",
                "Limit lo: 10\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols06a() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day",
                "select * from a where s = 'S1' order by s asc limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [s]\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols07NonPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s symbol index)");
            String expectedPlan = "FilterOnExcludedValues symbolOrder: #ORDER#\n" +
                    "  symbolFilter: s not in ['S1']\n" +
                    "  filter: length(s)=2\n" +
                    "    Cursor-order scan\n" +
                    "    Frame forward scan on: a\n";
            assertPlan(
                    "select * from a where s != 'S1' and length(s) = 2 order by s ",
                    expectedPlan.replace("#ORDER#", "asc")
            );
            assertPlan(
                    "select * from a where s != 'S1' and length(s) = 2 order by s desc",
                    expectedPlan.replace("#ORDER#", "desc")
            );
        });
    }

    @Test
    public void testSelectIndexedSymbols07Partitioned() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by day");

            String query = "select * from a where s != 'S1' and length(s) = 2 and ts in '2023-03-15' order by s #ORDER#";
            String expectedPlan = "FilterOnExcludedValues symbolOrder: #ORDER#\n" +
                    "  symbolFilter: s not in ['S1']\n" +
                    "  filter: length(s)=2\n" +
                    "    Cursor-order scan\n" +
                    "    Interval forward scan on: a\n" +
                    "      intervals: [(\"2023-03-15T00:00:00.000000Z\",\"2023-03-15T23:59:59.999999Z\")]\n";

            assertPlan(
                    query.replace("#ORDER#", "asc"),
                    expectedPlan.replace("#ORDER#", "asc")
            );

            assertPlan(
                    query.replace("#ORDER#", "desc"),
                    expectedPlan.replace("#ORDER#", "desc")
            );
        });
    }

    @Test
    public void testSelectIndexedSymbols08() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s symbol index)");
            insert("insert into a values ('a'), ('w'), ('b'), ('a'), (null);");

            String query = "select * from a where s != 'a' order by s";
            assertPlan(
                    query,
                    "FilterOnExcludedValues symbolOrder: asc\n" +
                            "  symbolFilter: s not in ['a']\n" +
                            "    Cursor-order scan\n" +
                            "    Frame forward scan on: a\n"
            );

            assertQuery("s\n" +
                    "\n" +//null
                    "b\n" +
                    "w\n", query, null, true, false);

            query = "select * from a where s != 'a' order by s desc";
            assertPlan(
                    query,
                    "FilterOnExcludedValues symbolOrder: desc\n" +
                            "  symbolFilter: s not in ['a']\n" +
                            "    Cursor-order scan\n" +
                            "    Frame forward scan on: a\n"
            );

            assertQuery("s\n" +
                    "w\n" +
                    "b\n" +
                    "\n"/*null*/, query, null, true, false);

            query = "select * from a where s != null order by s desc";
            assertPlan(
                    query,
                    "FilterOnExcludedValues symbolOrder: desc\n" +
                            "  symbolFilter: s not in [null]\n" +
                            "    Cursor-order scan\n" +
                            "    Frame forward scan on: a\n"
            );

            assertQuery("s\n" +
                    "w\n" +
                    "b\n" +
                    "a\n" +
                    "a\n", query, null, true, false);
        });
    }

    @Test
    public void testSelectIndexedSymbols09() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) partition by year ;",
                "select * from a where ts >= 0::timestamp and ts < 100::timestamp order by s asc",
                "SortedSymbolIndex\n" +
                        "    Index forward scan on: s\n" +
                        "      symbolOrder: asc\n" +
                        "    Interval forward scan on: a\n" +
                        "      intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T00:00:00.000099Z\")]\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols10() throws Exception {
        assertPlan(
                "create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in ('S1', 'S2') limit 1",
                "Limit lo: 1\n" +
                        "    FilterOnValues\n" +
                        "        Table-order scan\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S1'\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S2'\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectIndexedSymbols10WithOrder() throws Exception {
        testSelectIndexedSymbols10WithOrder("");
        testSelectIndexedSymbols10WithOrder("partition by hour");
    }

    @Test
    public void testSelectIndexedSymbols11() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");

            assertPlan(
                    "select * from a where s in ('S1', 'S2') and length(s) = 2 limit 1",
                    "Limit lo: 1\n" +
                            "    FilterOnValues\n" +
                            "        Table-order scan\n" +
                            "            Index forward scan on: s\n" +
                            "              filter: s=1 and length(s)=2\n" +
                            "            Index forward scan on: s\n" +
                            "              filter: s=2 and length(s)=2\n" +
                            "        Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSelectIndexedSymbols12() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan(
                    "select * from a where s1 in ('S1', 'S2') and s2 in ('S2') limit 1",
                    "Limit lo: 1\n" +
                            "    DataFrame\n" +
                            "        Index forward scan on: s2\n" +
                            "          filter: s2=2 and s1 in [S1,S2]\n" +
                            "        Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSelectIndexedSymbols13() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
            compile("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan(
                    "select * from a where s1 in ('S1')  order by ts desc",
                    "DeferredSingleSymbolFilterDataFrame\n" +
                            "    Index backward scan on: s1\n" +
                            "      filter: s1=1\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testSelectIndexedSymbols14() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan(
                    "select * from a where s1 = 'S1'  order by ts desc",
                    "DeferredSingleSymbolFilterDataFrame\n" +
                            "    Index backward scan on: s1\n" +
                            "      filter: s1=1\n" +
                            "    Frame backward scan on: a\n"
            );
        });
    }

    @Test // backward index scan is triggered only if query uses a single partition and orders by key column and ts desc
    public void testSelectIndexedSymbols15() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan(
                    "select * from a " +
                            "where s1 = 'S1' " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc",
                    "DeferredSingleSymbolFilterDataFrame\n" +
                            "    Index backward scan on: s1\n" +
                            "      filter: s1=1\n" +
                            "    Interval forward scan on: a\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.000008Z\")]\n"
            );
        });
    }

    @Test
    public void testSelectIndexedSymbols16() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan(
                    "select * from a " +
                            "where s1 in ('S1', 'S2') " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc",
                    "FilterOnValues symbolOrder: asc\n" +
                            "    Cursor-order scan\n" +
                            "        Index backward scan on: s1\n" +
                            "          filter: s1=1\n" +
                            "        Index backward scan on: s1\n" +
                            "          filter: s1=2\n" +
                            "    Interval forward scan on: a\n" +
                            "      intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.000008Z\")]\n"
            );
        });
    }

    @Test // TODO: should use the same plan as above
    public void testSelectIndexedSymbols17() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
            compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
            assertPlan(
                    "select * from a " +
                            "where (s1 = 'S1' or s1 = 'S2') " +
                            "and ts > 0::timestamp and ts < 9::timestamp  " +
                            "order by s1,ts desc",
                    "Sort light\n" +
                            "  keys: [s1, ts desc]\n" +
                            "    Async JIT Filter workers: 1\n" +
                            "      filter: (s1='S1' or s1='S2')\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Interval forward scan on: a\n" +
                            "              intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.000008Z\")]\n"
            );
        });
    }

    @Test
    public void testSelectIndexedSymbols18() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by hour;");
            compile("insert into a select 'S' || (6-x), dateadd('m', 20*x::int, 0::timestamp) from long_sequence(5)");
            String query = "select * from " +
                    "(" +
                    "  select * from a " +
                    "  where s1 not in ('S1', 'S2') " +
                    "  order by ts asc " +
                    "  limit 5" +
                    ") order by ts asc";
            assertPlan(
                    query,
                    "Limit lo: 5\n" +
                            "    FilterOnExcludedValues\n" +
                            "      symbolFilter: s1 not in ['S1','S2']\n" +
                            "        Table-order scan\n" +
                            "        Frame forward scan on: a\n"
            );

            assertQuery("s1\tts\n" +
                    "S5\t1970-01-01T00:20:00.000000Z\n" +
                    "S4\t1970-01-01T00:40:00.000000Z\n" +
                    "S3\t1970-01-01T01:00:00.000000Z\n", query, "ts", true, false);
        });
    }

    @Test
    public void testSelectIndexedSymbols7b() throws Exception {
        assertPlan(
                "create table a ( ts timestamp, s symbol index) timestamp(ts);",
                "select s from a where s != 'S1' and length(s) = 2 order by s ",
                "FilterOnExcludedValues symbolOrder: asc\n" +
                        "  symbolFilter: s not in ['S1']\n" +
                        "  filter: length(s)=2\n" +
                        "    Cursor-order scan\n" +
                        "    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectLongInList() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t ( l long)");

            assertPlan(
                    "select * from t where l in (5, -1, 1, null)",
                    "Async Filter workers: 1\n" +
                            "  filter: l in [NaN,-1,1,5]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

            assertPlan(
                    "select * from t where l not in (5, -1, 1, null)",
                    "Async Filter workers: 1\n" +
                            "  filter: not (l in [NaN,-1,1,5])\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );
        });
    }

    @Test
    public void testSelectNoOrderByWithNegativeLimit() throws Exception {
        ddl("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select * from a limit -5",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectNoOrderByWithNegativeLimitArithmetic() throws Exception {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select * from a limit -10+2",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 8\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsAsIndexDescNegativeLimit() throws Exception {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by 2 desc limit -10",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsAsc() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts asc",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsAscAndDesc() throws Exception {
        ddl("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select * from (select * from a order by ts asc limit 5) order by ts desc",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsDescAndAsc() throws Exception {
        ddl("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select * from (select * from a order by ts desc limit 5) order by ts asc",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsDescLargeNegativeLimit1() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit 9223372036854775806L+3L ",
                "Limit lo: -9223372036854775807L\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsDescLargeNegativeLimit2() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -1000000 ",
                "Limit lo: -1000000\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsDescNegativeLimit() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsWithNegativeLimit() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts)",
                "select * from a order by ts  limit -5",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 5\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderByTsWithNegativeLimit1() throws Exception {
        ddl("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("insert into a select x,x::timestamp from long_sequence(10)");

        assertPlan(
                "select ts, count(*)  from a sample by 1s limit -5",
                "Limit lo: -5\n" +
                        "    SampleBy\n" +
                        "      values: [count(*)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );

        assertPlan(
                "select i, count(*)  from a group by i limit -5",
                "Limit lo: -5\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [i]\n" +
                        "      values: [count(*)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );

        assertPlan(
                "select i, count(*)  from a limit -5",
                "Limit lo: -5\n" +
                        "    GroupBy vectorized: true workers: 1\n" +
                        "      keys: [i]\n" +
                        "      values: [count(*)]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );

        assertPlan(
                "select distinct(i) from a limit -5",
                "Limit lo: -5\n" +
                        "    DistinctKey\n" +
                        "        GroupBy vectorized: true workers: 1\n" +
                        "          keys: [i]\n" +
                        "          values: [count(*)]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderedAsc() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i asc",
                "Sort light\n" +
                        "  keys: [i]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderedDesc() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i desc",
                "Sort light\n" +
                        "  keys: [i desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectOrderedWithLimitLoHi() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i limit 10, 100",
                "Sort light lo: 10 hi: 100\n" +
                        "  keys: [i]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectRandomBoolean() throws Exception {
        assertPlan(
                "select rnd_boolean()",
                "VirtualRecord\n" +
                        "  functions: [rnd_boolean()]\n" +
                        "    long_sequence count: 1\n"
        );
    }

    @Test
    public void testSelectStaticTsInterval1() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2020-03-01'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [(\"2020-03-01T00:00:00.000001Z\",\"MAX\")]\n"
        );
    }

    @Test
    public void testSelectStaticTsInterval10() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc ",
                "Sort light\n" +
                        "  keys: [l desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [(\"2020-01-01T03:00:00.000000Z\",\"2020-01-01T04:00:00.999999Z\"),(\"2020-01-02T03:00:00.000000Z\",\"2020-01-02T04:00:00.999999Z\"),(\"2020-01-03T03:00:00.000000Z\",\"2020-01-03T04:00:00.999999Z\")]\n"
        );
    }

    @Test
    public void testSelectStaticTsInterval10a() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc, ts desc ",
                "Sort light\n" +
                        "  keys: [l desc, ts desc]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [(\"2020-01-01T03:00:00.000000Z\",\"2020-01-01T04:00:00.999999Z\"),(\"2020-01-02T03:00:00.000000Z\",\"2020-01-02T04:00:00.999999Z\"),(\"2020-01-03T03:00:00.000000Z\",\"2020-01-03T04:00:00.999999Z\")]\n"
        );
    }

    @Test
    public void testSelectStaticTsInterval2() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [(\"2020-03-01T00:00:00.000000Z\",\"2020-03-01T23:59:59.999999Z\")]\n"
        );
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval3() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' or ts in '2020-03-10'",
                "Async Filter workers: 1\n" +
                        "  filter: (ts in [1583020800000000,1583107199999999] or ts in [1583798400000000,1583884799999999])\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // ranges don't overlap so result is empty
    public void testSelectStaticTsInterval4() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' and ts in '2020-03-10'",
                "Empty table\n"
        );
    }

    @Test // only 2020-03-10->2020-03-31 needs to be scanned
    public void testSelectStaticTsInterval5() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03' and ts > '2020-03-10'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [(\"2020-03-10T00:00:00.000001Z\",\"2020-03-31T23:59:59.999999Z\")]\n"
        );
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval6() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts > '2020-03-01' and ts < '2020-03-10') or (ts > '2020-04-01' and ts < '2020-04-10') ",
                "Async Filter workers: 1\n" +
                        "  filter: ((1583020800000000<ts and ts<1583798400000000) or (1585699200000000<ts and ts<1586476800000000))\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval7() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts between '2020-03-01' and '2020-03-10') or (ts between '2020-04-01' and '2020-04-10') ",
                "Async Filter workers: 1\n" +
                        "  filter: (ts between 1583020800000000 and 1583798400000000 or ts between 1585699200000000 and 1586476800000000)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectStaticTsInterval8() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' ",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [(\"2020-01-01T03:00:00.000000Z\",\"2020-01-01T04:00:00.999999Z\"),(\"2020-01-02T03:00:00.000000Z\",\"2020-01-02T04:00:00.999999Z\"),(\"2020-01-03T03:00:00.000000Z\",\"2020-01-03T04:00:00.999999Z\")]\n"
        );
    }

    @Test
    public void testSelectStaticTsInterval9() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by ts desc",
                "DataFrame\n" +
                        "    Row backward scan\n" +
                        "    Interval backward scan on: tab\n" +
                        "      intervals: [(\"2020-01-01T03:00:00.000000Z\",\"2020-01-01T04:00:00.999999Z\"),(\"2020-01-02T03:00:00.000000Z\",\"2020-01-02T04:00:00.999999Z\"),(\"2020-01-03T03:00:00.000000Z\",\"2020-01-03T04:00:00.999999Z\")]\n"
        );
    }

    @Test
    public void testSelectStaticTsIntervalOnTabWithoutDesignatedTimestamp() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where ts > '2020-03-01'",
                "Async Filter workers: 1\n" +
                        "  filter: 1583020800000000<ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWalTransactions() throws Exception {
        assertPlan(
                "create table tab ( s string, sy symbol, i int, ts timestamp) timestamp(ts) partition by day WAL",
                "select * from wal_transactions('tab')",
                "wal_transactions of: tab\n"
        );
    }

    @Test
    public void testSelectWhereOrderByLimit() throws Exception {
        assertPlan(
                "create table xx ( x long, str string) ",
                "select * from xx where str = 'A' order by str,x limit 10",
                "Sort light lo: 10\n" +
                        "  keys: [str, x]\n" +
                        "    Async Filter workers: 1\n" +
                        "      filter: str='A'\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: xx\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter1() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: 100<l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: this one should use jit
    public void testSelectWithJittedFilter10() throws Exception {
        assertPlan(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where s in ( 'A', 'B' )",
                "Async Filter workers: 1\n" +
                        "  filter: s in [A,B]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: this one should interval scan without filter
    public void testSelectWithJittedFilter11() throws Exception {
        assertPlan(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-02' )",
                "Async Filter workers: 1\n" +
                        "  filter: ts in [1577836800000000,1577923200000000]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter12() throws Exception {
        assertPlan(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-03' ) and s = 'ABC'",
                "Async Filter workers: 1\n" +
                        "  filter: (ts in [1577836800000000,1578009600000000] and s='ABC')\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter13() throws Exception {
        assertPlan(
                "create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01' ) and s = 'ABC'",
                "Async Filter workers: 1\n" +
                        "  filter: (ts in [1577836800000000,1577923199999999] and s='ABC')\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter14() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12 or l = 15 ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: (l=12 or l=15)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter15() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12.345 ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: l=12.345\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter16() throws Exception {
        assertPlan(
                "create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = false ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: b=false\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter17() throws Exception {
        assertPlan(
                "create table tab ( b boolean, ts timestamp);",
                "select * from tab where not(b = false or ts = 123) ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: (b!=false and ts!=123)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter18() throws Exception {
        assertPlan(
                "create table tab ( l1 long, l2 long);",
                "select * from tab where l1 < l2 ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: l1<l2\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter19() throws Exception {
        assertPlan(
                "create table tab ( l1 long, l2 long);",
                "select * from tab where l1 * l2 > 0  ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: 0<l1*l2\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter2() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: (100<l and l<1000)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter20() throws Exception {
        assertPlan(
                "create table tab ( l1 long, l2 long, l3 long);",
                "select * from tab where l1 * l2 > l3  ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: l3<l1*l2\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter21() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1 ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: l=$0::long\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter22() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1 + 1 ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: d=1024.1+1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter23() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = null ",
                "Async JIT Filter workers: 1\n" +
                        "  filter: d is null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter24a() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit 1 ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter24b() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter24b2() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit -1 ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter24c() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter24d() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit -1 ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter24e() throws Exception {
        bindVariableService.setInt("maxRows", -1);

        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 limit :maxRows ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter25() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter26() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 ",
                "Async JIT Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: d=1.2\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test // TODO: this one should use jit !
    public void testSelectWithJittedFilter3() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and ts = '2022-01-01' ",
                "Async Filter workers: 1\n" +
                        "  filter: ((100<l and l<1000) and ts=1640995200000000)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter4() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and l = 20",
                "Async JIT Filter workers: 1\n" +
                        "  filter: ((100<l and l<1000) and l=20)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter5() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or l = 20",
                "Async JIT Filter workers: 1\n" +
                        "  filter: ((100<l and l<1000) or l=20)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter6() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or ts = 123",
                "Async JIT Filter workers: 1\n" +
                        "  filter: ((100<l and l<1000) or ts=123)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: this one should use jit
    public void testSelectWithJittedFilter7() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 or ts > '2021-01-01'",
                "Async Filter workers: 1\n" +
                        "  filter: ((100<l and l<1000) or 1609459200000000<ts)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithJittedFilter8() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 and ts in '2021-01-01'",
                "Async JIT Filter workers: 1\n" +
                        "  filter: (100<l and l<1000)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [(\"2021-01-01T00:00:00.000000Z\",\"2021-01-01T23:59:59.999999Z\")]\n"
        );
    }

    @Test // TODO: this one should use jit
    public void testSelectWithJittedFilter9() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l in ( 100, 200 )",
                "Async Filter workers: 1\n" +
                        "  filter: l in [100,200]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithLimitLo() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10",
                "Limit lo: 10\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithLimitLoHi() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10, 100",
                "Limit lo: 10 hi: 100\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithLimitLoHiNegative() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10, -100",
                "Limit lo: -10 hi: -100\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithLimitLoNegative() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n"
        );
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter1() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::short ",
                "Async Filter workers: 1\n" +
                        "  filter: l=12::short\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit filter doesn't work with type casts
    public void testSelectWithNonJittedFilter10() throws Exception {
        assertPlan(
                "create table tab ( s short, ts timestamp);",
                "select * from tab where s = 1::short ",
                "Async Filter workers: 1\n" +
                        "  filter: s=1::short\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: should run with jitted filter just like b = true
    public void testSelectWithNonJittedFilter11() throws Exception {
        assertPlan(
                "create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = true::boolean ",
                "Async Filter workers: 1\n" +
                        "  filter: b=true\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: should run with jitted filter just like l = 1024
    public void testSelectWithNonJittedFilter12() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 1024::long ",
                "Async Filter workers: 1\n" +
                        "  filter: l=1024::long\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: should run with jitted filter just like d = 1024.1
    public void testSelectWithNonJittedFilter13() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1::double ",
                "Async Filter workers: 1\n" +
                        "  filter: d=1024.1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // TODO: should run with jitted filter just like d = null
    public void testSelectWithNonJittedFilter14() throws Exception {
        assertPlan(
                "create table tab ( d double, ts timestamp);",
                "select * from tab where d = null::double ",
                "Async Filter workers: 1\n" +
                        "  filter: d is null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter15() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l | l) > 0  ",
                "Async Filter workers: 1\n" +
                        "  filter: 0<l|l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter16() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l & l) > 0  ",
                "Async Filter workers: 1\n" +
                        "  filter: 0<l&l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit doesn't work for bitwise operators
    public void testSelectWithNonJittedFilter17() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0  ",
                "Async Filter workers: 1\n" +
                        "  filter: 0<l^l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithNonJittedFilter18() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0 limit -1",
                "Async Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: 0<l^l\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithNonJittedFilter19() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong("maxRows", -1);

        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0 limit :maxRows",
                "Async Filter workers: 1\n" +
                        "  limit: 1\n" +
                        "  filter: 0<l^l\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: tab\n"
        );
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter2() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::byte ",
                "Async Filter workers: 1\n" +
                        "  filter: l=12::byte\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter3() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = '123' ",
                "Async Filter workers: 1\n" +
                        "  filter: l='123'\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit is not because rnd_long() value is not stable
    public void testSelectWithNonJittedFilter4() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = rnd_long() ",
                "Async Filter workers: 1\n" +
                        "  filter: l=rnd_long()\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithNonJittedFilter5() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = case when l > 0 then 1 when l = 0 then 0 else -1 end ",
                "Async Filter workers: 1\n" +
                        "  filter: l=case([0<l,1,l=0,0,-1])\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // interval scan is not used because of type mismatch
    public void testSelectWithNonJittedFilter6() throws Exception {
        assertPlan(
                "create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1::string ",
                "Async Filter workers: 1\n" +
                        "  filter: l=$0::double::string\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit filter doesn't work for string type
    public void testSelectWithNonJittedFilter7() throws Exception {
        assertPlan(
                "create table tab ( s string, ts timestamp);",
                "select * from tab where s = 'test' ",
                "Async Filter workers: 1\n" +
                        "  filter: s='test'\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit filter doesn't work for string type
    public void testSelectWithNonJittedFilter8() throws Exception {
        assertPlan(
                "create table tab ( s string, ts timestamp);",
                "select * from tab where s = null ",
                "Async Filter workers: 1\n" +
                        "  filter: s is null\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test // jit filter doesn't work with type casts
    public void testSelectWithNonJittedFilter9() throws Exception {
        assertPlan(
                "create table tab ( b byte, ts timestamp);",
                "select * from tab where b = 1::byte ",
                "Async Filter workers: 1\n" +
                        "  filter: b=1::byte\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n"
        );
    }

    @Test
    public void testSelectWithNotOperator() throws Exception {
        assertPlan(
                "CREATE TABLE tst ( timestamp TIMESTAMP );",
                "select * from tst where timestamp not between '2021-01-01' and '2021-01-10' ",
                "Async Filter workers: 1\n" +
                        "  filter: not (timestamp between 1609459200000000 and 1610236800000000)\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tst\n"
        );
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLo() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit 10",
                "Limit lo: 10\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLoNegative1() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10",
                "Sort light\n" +
                        "  keys: [ts desc]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLoNegative2() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select i from a order by ts desc limit -10",
                "SelectedRecord\n" +
                        "    Sort light\n" +
                        "      keys: [ts desc]\n" +
                        "        Limit lo: 10\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithOrderByTsLimitLoNegative1() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts limit -10",
                "Sort light\n" +
                        "  keys: [ts]\n" +
                        "    Limit lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithOrderByTsLimitLoNegative2() throws Exception {
        assertPlan(
                "create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select i from a order by ts limit -10",
                "SelectedRecord\n" +
                        "    Sort light\n" +
                        "      keys: [ts]\n" +
                        "        Limit lo: 10\n" +
                        "            DataFrame\n" +
                        "                Row backward scan\n" +
                        "                Frame backward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithReorder1() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l<i",
                "Async JIT Filter workers: 1\n" +
                        "  filter: l<i\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithReorder2() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l::short<i",
                "Async Filter workers: 1\n" +
                        "  filter: l::short<i\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithReorder2a() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
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
                        "                Async Filter workers: 1\n" +
                        "                  limit: 100\n" +
                        "                  filter: l::short<i\n" +
                        "                    DataFrame\n" +
                        "                        Row forward scan\n" +
                        "                        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithReorder2b() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select i2, i1, ts1 from " +
                        "(select ts as ts1, l as l1, i as i1, i as i2 " +
                        "from a " +
                        "order by ts, l1 " +
                        "limit 100 ) " +
                        "where i1*i2 != 0",
                "SelectedRecord\n" +
                        "    Filter filter: i1*i2!=0\n" +
                        "        Sort light lo: 100 partiallySorted: true\n" +
                        "          keys: [ts1, l1]\n" +
                        "            SelectedRecord\n" +
                        "                SelectedRecord\n" +
                        "                    DataFrame\n" +
                        "                        Row forward scan\n" +
                        "                        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithReorder3() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select k, max(ts) from ( select ts, l as k, i from a where l::short<i ) where k < 0 ",
                "GroupBy vectorized: false\n" +
                        "  keys: [k]\n" +
                        "  values: [max(ts)]\n" +
                        "    SelectedRecord\n" +
                        "        Async Filter workers: 1\n" +
                        "          filter: (l::short<i and l<0)\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSelectWithReorder4() throws Exception {
        assertPlan(
                "create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select mil, k, minl, mini from " +
                        "( select ts as k, max(i*l) as mil, min(i) as mini, min(l) as minl  " +
                        "from a where l::short<i ) " +
                        "where mil + mini> 1 ",
                "Filter filter: 1<mil+mini\n" +
                        "    GroupBy vectorized: false\n" +
                        "      keys: [k]\n" +
                        "      values: [max(i*l),min(l),min(i)]\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter workers: 1\n" +
                        "              filter: l::short<i\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n"
        );
    }

    @Test
    public void testSortAscLimitAndSortAgain1a() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts asc limit 10) order by ts asc",
                    "Limit lo: 10\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain1b() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts desc, l desc limit 10) order by ts desc",
                    "Sort light lo: 10\n" +
                            "  keys: [ts desc, l desc]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts asc, l limit 10) lt join (select * from a) order by ts asc",
                    "SelectedRecord\n" +
                            "    Lt Join\n" +
                            "        Sort light lo: 10 partiallySorted: true\n" +
                            "          keys: [ts, l]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain3a() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from " +
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
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain3b() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from " +
                            "(select * from (select * from a order by ts desc, l desc) limit 10) " +
                            "order by ts asc",
                    "Sort light\n" +
                            "  keys: [ts]\n" +
                            "    Limit lo: 10\n" +
                            "        Sort light\n" +
                            "          keys: [ts desc, l desc]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain4a() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from " +
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
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortAscLimitAndSortAgain4b() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from " +
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
                            "                    Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortAscLimitAndSortDesc() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts asc limit 10) order by ts desc",
                    "Sort light\n" +
                            "  keys: [ts desc]\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortDescLimitAndSortAgain() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts desc limit 10) order by ts desc",
                    "Limit lo: 10\n" +
                            "    DataFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: a\n"
            );
        });
    }

    @Test
    public void testSortDescLimitAndSortAsc1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from (select * from a order by ts desc limit 10) order by ts asc",
                    "Sort light\n" +
                            "  keys: [ts]\n" +
                            "    Limit lo: 10\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: a\n"
            );
        });
    }

    @Test//TODO: sorting by ts, l again is not necessary
    public void testSortDescLimitAndSortAsc2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long)");

            assertPlan(
                    "select * from (select * from a order by ts, l limit 10) order by ts, l",
                    "Sort light\n" +
                            "  keys: [ts, l]\n" +
                            "    Sort light lo: 10\n" +
                            "      keys: [ts, l]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n"
            );
        });
    }

    @Test//TODO: sorting by ts, l again is not necessary
    public void testSortDescLimitAndSortAsc3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long)");

            assertPlan(
                    "select * from (select * from a order by ts, l limit 10,-10) order by ts, l",
                    "Sort light\n" +
                            "  keys: [ts, l]\n" +
                            "    Limit lo: 10 hi: -10\n" +
                            "        Sort light\n" +
                            "          keys: [ts, l]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n"
            );
        });
    }

    @Test
    public void testSpliceJoin0() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from a splice join b on ts where a.i = b.ts",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.ts\n" +
                            "        Splice Join\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testSpliceJoin0a() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp, l long) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp, l long) timestamp(ts)");

            assertPlan(
                    "select * from a splice join b on ts where a.i + b.i = 1",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i+b.i=1\n" +
                            "        Splice Join\n" +
                            "          condition: b.ts=a.ts\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testSpliceJoin1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a splice join b on ts",
                    "SelectedRecord\n" +
                            "    Splice Join\n" +
                            "      condition: b.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testSpliceJoin2() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a splice join (select * from b limit 10) on ts",
                    "SelectedRecord\n" +
                            "    Splice Join\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Limit lo: 10\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testSpliceJoin3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a splice join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                    "SelectedRecord\n" +
                            "    Splice Join\n" +
                            "      condition: _xQdbA1.ts=a.ts\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: a\n" +
                            "        Sort light\n" +
                            "          keys: [ts, i]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testSpliceJoin4() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table a ( i int, ts timestamp) timestamp(ts)");
            ddl("create table b ( i int, ts timestamp) timestamp(ts)");

            assertPlan(
                    "select * from a splice join b where a.i = b.i",
                    "SelectedRecord\n" +
                            "    Filter filter: a.i=b.i\n" +
                            "        Splice Join\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: b\n"
            );
        });
    }

    @Test
    public void testUnion() throws Exception {
        assertPlan(
                "create table a ( i int, s string);",
                "select * from a union select * from a",
                "Union\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
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
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testWhereOrderByTsLimit1() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t ( x long, ts timestamp) timestamp(ts)");

            String query = "select * from t where x < 100 order by ts desc limit -5";
            assertPlan(
                    query,
                    "Async JIT Filter workers: 1\n" +
                            "  limit: 5\n" +
                            "  filter: x<100\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: t\n"
            );

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
        assertPlan(
                "create table a (u uuid, ts timestamp) timestamp(ts);",
                "select u, ts from a where u = '11111111-1111-1111-1111-111111111111' or u = '22222222-2222-2222-2222-222222222222' or u = '33333333-3333-3333-3333-333333333333'",
                "Async JIT Filter workers: 1\n" +
                        "  filter: ((u='11111111-1111-1111-1111-111111111111' or u='22222222-2222-2222-2222-222222222222') or u='33333333-3333-3333-3333-333333333333')\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n"
        );
    }

    @Test
    public void testWindow0() throws Exception {
        assertPlan(
                "create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select ts, str,  row_number() over (order by l), row_number() over (partition by l) from t",
                "CachedWindow\n" +
                        "  orderedFunctions: [[l] => [row_number()]]\n" +
                        "  unorderedFunctions: [row_number() over (partition by [l])]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n"
        );
    }

    @Test
    public void testWindow1() throws Exception {
        assertPlan(
                "create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select str, ts, l, 10, row_number() over ( partition by l order by ts) from t",
                "CachedWindow\n" +
                        "  orderedFunctions: [[ts] => [row_number() over (partition by [l])]]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [str,ts,l,10]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: t\n"
        );
    }

    @Test
    public void testWindow2() throws Exception {
        assertPlan(
                "create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select str, ts, l as l1, ts::long+l as tsum, row_number() over ( partition by l, ts order by str) from t",
                "CachedWindow\n" +
                        "  orderedFunctions: [[str] => [row_number() over (partition by [l1,ts])]]\n" +
                        "    VirtualRecord\n" +
                        "      functions: [str,ts,l1,ts::long+l1]\n" +
                        "        SelectedRecord\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: t\n"
        );
    }

    @Test
    public void testWindow3() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table tab (ts timestamp, i long, j long) timestamp(ts)");

            assertPlan("select ts, i, j, " +
                            "avg(j) over (order by i, j rows unbounded preceding), " +
                            "sum(j) over (order by i, j rows unbounded preceding), " +
                            "first_value(j) over (order by i, j rows unbounded preceding), " +
                            "from tab",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[i, j] => [avg(j) over (rows between unbounded preceding and current row)," +
                            "sum(j) over (rows between unbounded preceding and current row),first_value(j) over ()]]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select ts, i, j, " +
                            "avg(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "sum(j) over (partition by i order by ts rows between 1 preceding and current row), " +
                            "first_value(j) over (partition by i order by ts rows between 1 preceding and current row) " +
                            "from tab",
                    "Window\n" +
                            "  functions: [avg(j) over (partition by [i] rows between 1 preceding and current row)," +
                            "sum(j) over (partition by [i] rows between 1 preceding and current row),first_value(j) over (partition by [i] rows between 1 preceding and current row)]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: tab\n");

            assertPlan("select row_number() over (partition by i order by i desc, j asc), " +
                            "avg(j) over (partition by i order by j, i desc rows unbounded preceding), " +
                            "sum(j) over (partition by i order by j, i desc rows unbounded preceding), " +
                            "first_value(j) over (partition by i order by j, i desc rows unbounded preceding) " +
                            "from tab " +
                            "order by ts desc",
                    "SelectedRecord\n" +
                            "    CachedWindow\n" +
                            "      orderedFunctions: [[i desc, j] => [row_number() over (partition by [i])]," +
                            "[j, i desc] => [avg(j) over (partition by [i] rows between unbounded preceding and current row )," +
                            "sum(j) over (partition by [i] rows between unbounded preceding and current row )," +
                            "first_value(j) over (partition by [i] rows between unbounded preceding and current row )]]\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: tab\n");

            assertPlan("select row_number() over (partition by i order by i desc, j asc), " +
                            "        avg(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " +
                            "        sum(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " +
                            "        first_value(j) over (partition by i, j order by i desc, j asc rows unbounded preceding), " +
                            "        rank() over (partition by j, i) " +
                            "from tab order by ts desc",
                    "SelectedRecord\n" +
                            "    CachedWindow\n" +
                            "      orderedFunctions: [[i desc, j] => [row_number() over (partition by [i]),avg(j) over (partition by [i,j] rows between unbounded preceding and current row )," +
                            "sum(j) over (partition by [i,j] rows between unbounded preceding and current row )," +
                            "first_value(j) over (partition by [i,j] rows between unbounded preceding and current row )]]\n" +
                            "      unorderedFunctions: [rank() over (partition by [j,i])]\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: tab\n");
        });
    }

    @Test
    public void testWindowModelOrderByIsNotIgnored() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            assertPlan("select sum(avg), sum(sum), sum(first_value) from (\n" +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts desc\n" +
                            ") ",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(avg),sum(sum),sum(first_value)]\n" +
                            "    Window\n" +
                            "      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: cpu_ts\n");

            assertPlan("select sum(avg), sum(sum), sum(first_value) from (\n" +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            ") ",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(avg),sum(sum),sum(first_value)]\n" +
                            "    Window\n" +
                            "      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: cpu_ts\n");

            assertPlan("select sum(avg), sum(sum) sm, sum(first_value) fst from (\n" +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc\n" +
                            ") order by sm ",
                    "Sort\n" +
                            "  keys: [sm]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(avg),sum(sum),sum(first_value)]\n" +
                            "        Window\n" +
                            "          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)" +
                            "]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: cpu_ts\n");
        });
    }

    @Test
    public void testWindowOrderByUnderWindowModelIsPreserved() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            assertPlan("select sum(avg), sum(sum), first(first_value) from ( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over (partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts desc" +
                            ") ",
                    "GroupBy vectorized: false\n" +
                            "  values: [sum(avg),sum(sum),first(first_value)]\n" +
                            "    Window\n" +
                            "      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "        DataFrame\n" +
                            "            Row backward scan\n" +
                            "            Frame backward scan on: cpu_ts\n");

            assertPlan("select sum(avg), sum(sum), first(first_value) from ( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by 1 desc",
                    "Sort\n" +
                            "  keys: [sum desc]\n" +
                            "    GroupBy vectorized: false\n" +
                            "      values: [sum(avg),sum(sum),first(first_value)]\n" +
                            "        CachedWindow\n" +
                            "          orderedFunctions: [[ts desc] => [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: cpu_ts\n");
        });
    }

    //TODO: remove artificial limit models used to force ordering on window models (and avoid unnecessary sorts)
    @Test
    public void testWindowParentModelOrderPushdownIsBlockedWhenWindowModelSpecifiesOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value, " +
                            "from cpu_ts " +
                            "order by ts desc " +
                            ") order by ts asc",
                    "Sort\n" +
                            "  keys: [ts]\n" +
                            "    Limit lo: 9223372036854775807L\n" +
                            "        Window\n" +
                            "          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc " +
                            ") order by ts desc",
                    "Sort\n" +
                            "  keys: [ts desc]\n" +
                            "    Limit lo: 9223372036854775807L\n" +
                            "        Window\n" +
                            "          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc " +
                            ") order by hostname",
                    "Sort\n" +
                            "  keys: [hostname]\n" +
                            "    Limit lo: 9223372036854775807L\n" +
                            "        Window\n" +
                            "          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by ts asc ",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[ts desc] => [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]]\n" +
                            "    DataFrame\n" +
                            "        Row forward scan\n" +
                            "        Frame forward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by ts desc ",
                    "CachedWindow\n" +
                            "  orderedFunctions: [[ts] => [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]]\n" +
                            "    DataFrame\n" +
                            "        Row backward scan\n" +
                            "        Frame backward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by hostname ",
                    "Sort\n" +
                            "  keys: [hostname]\n" +
                            "    Window\n" +
                            "      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by hostname ",
                    "Sort\n" +
                            "  keys: [hostname]\n" +
                            "    Window\n" +
                            "      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            "order by ts desc " +
                            ") order by ts asc ",
                    "Sort\n" +
                            "  keys: [ts]\n" +
                            "    Limit lo: 9223372036854775807L\n" +
                            "        Window\n" +
                            "          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "            DataFrame\n" +
                            "                Row backward scan\n" +
                            "                Frame backward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            "order by ts asc " +
                            ") order by ts desc ",
                    "Sort\n" +
                            "  keys: [ts desc]\n" +
                            "    Limit lo: 9223372036854775807L\n" +
                            "        Window\n" +
                            "          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: cpu_ts\n");

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc ) " +
                            "order by ts asc " +
                            ") order by hostname ",
                    "Sort\n" +
                            "  keys: [hostname]\n" +
                            "    Limit lo: 9223372036854775807L\n" +
                            "        Window\n" +
                            "          functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: cpu_ts\n");


        });
    }

    @Test
    public void testWindowParentModelOrderPushdownIsDoneWhenNestedModelsSpecifyNoneOrMatchingOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table  cpu_ts ( hostname symbol, usage_system double, ts timestamp ) timestamp(ts);");

            String expectedForwardPlan = "Window\n" +
                    "  functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                    "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                    "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                    "    DataFrame\n" +
                    "        Row forward scan\n" +
                    "        Frame forward scan on: cpu_ts\n";

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            ") order by ts asc",
                    expectedForwardPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value, " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by ts asc",
                    expectedForwardPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by ts asc",
                    expectedForwardPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by hostname) " +
                            ") order by ts asc",
                    expectedForwardPlan);

            String expectedForwardLimitPlan =
                    "Limit lo: 9223372036854775807L\n" +
                            "    Window\n" +
                            "      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                            "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: cpu_ts\n";

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts asc  " +
                            ") order by ts asc",
                    expectedForwardLimitPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts asc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            "order by ts asc  " +
                            ") order by ts asc",
                    expectedForwardLimitPlan);

            String expectedBackwardPlan = "Window\n" +
                    "  functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                    "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                    "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                    "    DataFrame\n" +
                    "        Row backward scan\n" +
                    "        Frame backward scan on: cpu_ts\n";
            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            ") order by ts desc",
                    expectedBackwardPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            ") order by ts desc",
                    expectedBackwardPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts asc) " +
                            ") order by ts desc",
                    expectedBackwardPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by hostname) " +
                            ") order by ts desc",
                    expectedBackwardPlan);

            String expectedBackwardLimitPlan = "Limit lo: 9223372036854775807L\n" +
                    "    Window\n" +
                    "      functions: [avg(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                    "sum(usage_system) over (partition by [hostname] rows between 100 preceding and current row)," +
                    "first_value(usage_system) over (partition by [hostname] rows between 100 preceding and current row)]\n" +
                    "        DataFrame\n" +
                    "            Row backward scan\n" +
                    "            Frame backward scan on: cpu_ts\n";

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from cpu_ts " +
                            "order by ts desc  " +
                            ") order by ts desc",
                    expectedBackwardLimitPlan);

            assertPlan("select * from " +
                            "( " +
                            "select ts, hostname, usage_system, " +
                            "avg(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) avg, " +
                            "sum(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) sum, " +
                            "first_value(usage_system) over(partition by hostname order by ts desc rows between 100 preceding and current row) first_value " +
                            "from (select * from cpu_ts order by ts desc) " +
                            "order by ts desc  " +
                            ") order by ts desc",
                    expectedBackwardLimitPlan);
        });
    }

    @Test
    public void testWindowRecordCursorFactoryWithLimit() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table x as ( " +
                    "  select " +
                    "    cast(x as int) i, " +
                    "    rnd_symbol('a','b','c') sym, " +
                    "    timestamp_sequence(0, 100000000) ts " +
                    "   from long_sequence(100)" +
                    ") timestamp(ts) partition by hour");

            String sql = "select i, " +
                    "row_number() over (partition by sym), " +
                    "avg(i) over (partition by i rows unbounded preceding), " +
                    "sum(i) over (partition by i rows unbounded preceding), " +
                    "first_value(i) over (partition by i rows unbounded preceding) " +
                    "from x limit 3";
            assertPlan(
                    sql,
                    "Limit lo: 3\n" +
                            "    Window\n" +
                            "      functions: [row_number() over (partition by [sym])," +
                            "avg(i) over (partition by [i] rows between unbounded preceding and current row )," +
                            "sum(i) over (partition by [i] rows between unbounded preceding and current row )," +
                            "first_value(i) over (partition by [i] rows between unbounded preceding and current row )]\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: x\n"
            );

            assertSql("i\trow_number\tavg\tsum\tfirst_value\n" +
                    "1\t1\t1.0\t1.0\t1.0\n" +
                    "2\t2\t2.0\t2.0\t2.0\n" +
                    "3\t1\t3.0\t3.0\t3.0\n", sql);
        });
    }

    @Test
    public void testWithBindVariables() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table t ( x int );");

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
        assertPlan(
                "select * from t where x = :v1 ",
                "Async Filter workers: 1\n" +
                        "  filter: x=:v1::" + type + "\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n"
        );
    }

    private void assertPlan(String ddl, String query, String expectedPlan) throws Exception {
        assertMemoryLeak(() -> {
            compile(ddl);
            assertPlan(query, expectedPlan);
        });
    }

    private void assertSqlAndPlan(String sql, String expectedPlan, String expectedResult) throws SqlException {
        assertPlan(sql, expectedPlan);
        assertSql(expectedResult, sql);
    }

    private Function getConst(IntObjHashMap<ObjList<Function>> values, int type, int paramNo, int iteration) {
        //use param number to work around rnd factories validation logic
        int val = paramNo + 1;

        switch (type) {
            case ColumnType.BYTE:
                return new ByteConstant((byte) val);
            case ColumnType.SHORT:
                return new ShortConstant((short) val);
            case ColumnType.INT:
                return new IntConstant(val);
            case ColumnType.IPv4:
                return new IPv4Constant(val);
            case ColumnType.LONG:
                return new LongConstant(val);
            case ColumnType.DATE:
                return new DateConstant(val * 86_400_000L);
            case ColumnType.TIMESTAMP:
                return new TimestampConstant(val * 86_400_000L);
            default:
                ObjList<Function> availableValues = values.get(type);
                if (availableValues != null) {
                    int n = availableValues.size();
                    return availableValues.get(iteration % n);
                } else {
                    return null;
                }
        }
    }

    private <T> ObjList<T> list(T... values) {
        return new ObjList<T>(values);
    }

    private void test2686Prepare() throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table table_1 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          member boolean\n" +
                    "        ) timestamp(ts) PARTITION by month");

            insert("insert into table_1 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60, True )");
            insert("insert into table_1 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, False )");
            insert("insert into table_1 values ( '2022-10-25T03:00:00.000000Z', 'david',  21, True )");

            ddl("create table table_2 (\n" +
                    "          ts timestamp,\n" +
                    "          name string,\n" +
                    "          age int,\n" +
                    "          address string\n" +
                    "        ) timestamp(ts) PARTITION by month");

            insert("insert into table_2 values ( '2022-10-25T01:00:00.000000Z', 'alice',  60,  '1 Glebe St' )");
            insert("insert into table_2 values ( '2022-10-25T02:00:00.000000Z', 'peter',  58, '1 Broon St' )");
        });
    }

    // left join maintains order metadata and can be part of asof join
    private void testHashAndAsOfJoin(SqlCompiler compiler, boolean isLight) throws Exception {
        assertMemoryLeak(() -> {
            ddl("create table taba (a1 int, ts1 timestamp) timestamp(ts1)");
            ddl("create table tabb (b1 int, b2 long)");
            ddl("create table tabc (c1 int, c2 long, ts3 timestamp) timestamp(ts3)");

            assertPlan(
                    compiler,
                    "select * " +
                            "from taba " +
                            "left join tabb on a1=b1 " +
                            "asof join tabc on b1=c1",
                    "SelectedRecord\n" +
                            "    AsOf Join" + (isLight ? " Light" : "") + "\n" +
                            "      condition: c1=b1\n" +
                            "        Hash Outer Join" + (isLight ? " Light" : "") + "\n" +
                            "          condition: b1=a1\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: taba\n" +
                            "            Hash\n" +
                            "                DataFrame\n" +
                            "                    Row forward scan\n" +
                            "                    Frame forward scan on: tabb\n" +
                            "        DataFrame\n" +
                            "            Row forward scan\n" +
                            "            Frame forward scan on: tabc\n",
                    sqlExecutionContext
            );
        });
    }

    private void testSelectIndexedSymbol(String timestampAndPartitionByClause) throws Exception {
        assertMemoryLeak(() -> {
            compile("drop table if exists a");
            ddl("create table a ( s symbol index, ts timestamp) " + timestampAndPartitionByClause);
            insert("insert into a values ('S2', 0), ('S1', 1), ('S3', 2+3600000000), ( 'S2' ,3+3600000000)");

            String query = "select * from a where s in (:s1, :s2) order by s desc limit 5";
            bindVariableService.clear();
            bindVariableService.setStr("s1", "S1");
            bindVariableService.setStr("s2", "S2");

            // even though plan shows cursors in S1, S2 order, FilterOnValues sorts them before query execution
            // actual order is S2, S1
            assertPlan(
                    query,
                    "Limit lo: 5\n" +
                            "    FilterOnValues symbolOrder: desc\n" +
                            "        Cursor-order scan\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s1::string\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s2::string\n" +
                            "        Frame forward scan on: a\n"
            );

            assertQuery("s\tts\n" +
                    "S2\t1970-01-01T00:00:00.000000Z\n" +
                    "S2\t1970-01-01T01:00:00.000003Z\n" +
                    "S1\t1970-01-01T00:00:00.000001Z\n", query, null, true, false);

            //order by asc
            query = "select * from a where s in (:s1, :s2) order by s asc limit 5";

            assertPlan(
                    query,
                    "Limit lo: 5\n" +
                            "    FilterOnValues symbolOrder: asc\n" +
                            "        Cursor-order scan\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s1::string\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s2::string\n" +
                            "        Frame forward scan on: a\n"
            );

            assertQuery("s\tts\n" +
                    "S1\t1970-01-01T00:00:00.000001Z\n" +
                    "S2\t1970-01-01T00:00:00.000000Z\n" +
                    "S2\t1970-01-01T01:00:00.000003Z\n", query, null, true, false);
        });
    }

    @SuppressWarnings("SameParameterValue")
    private void testSelectIndexedSymbolWithIntervalFilter() throws Exception {
        assertMemoryLeak(() -> {
            compile("drop table if exists a");
            ddl("create table a ( s symbol index, ts timestamp) " + "timestamp(ts) partition by day");
            insert("insert into a values ('S2', 0), ('S1', 1), ('S3', 2+3600000000), ( 'S2' ,3+3600000000)");

            String query = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s desc limit 5";
            bindVariableService.clear();
            bindVariableService.setStr("s1", "S1");
            bindVariableService.setStr("s2", "S2");

            // even though plan shows cursors in S1, S2 order, FilterOnValues sorts them before query execution
            // actual order is S2, S1
            assertPlan(
                    query,
                    "Limit lo: 5\n" +
                            "    FilterOnValues symbolOrder: desc\n" +
                            "        Cursor-order scan\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s1::string\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s2::string\n" +
                            "        Interval forward scan on: a\n" +
                            "          intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T23:59:59.999999Z\")]\n"
            );

            assertQuery("s\tts\n" +
                    "S2\t1970-01-01T00:00:00.000000Z\n" +
                    "S2\t1970-01-01T01:00:00.000003Z\n" +
                    "S1\t1970-01-01T00:00:00.000001Z\n", query, null, true, false);

            //order by asc
            query = "select * from a where s in (:s1, :s2) and ts in '1970-01-01' order by s asc limit 5";

            assertPlan(
                    query,
                    "Limit lo: 5\n" +
                            "    FilterOnValues symbolOrder: asc\n" +
                            "        Cursor-order scan\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s1::string\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s2::string\n" +
                            "        Interval forward scan on: a\n" +
                            "          intervals: [(\"1970-01-01T00:00:00.000000Z\",\"1970-01-01T23:59:59.999999Z\")]\n"
            );

            assertQuery("s\tts\n" +
                    "S1\t1970-01-01T00:00:00.000001Z\n" +
                    "S2\t1970-01-01T00:00:00.000000Z\n" +
                    "S2\t1970-01-01T01:00:00.000003Z\n", query, null, true, false);
        });
    }

    private void testSelectIndexedSymbols10WithOrder(String partitionByClause) throws Exception {
        assertMemoryLeak(() -> {
            compile("drop table if exists a");
            ddl("create table a ( s symbol index, ts timestamp) timestamp(ts)" + partitionByClause);
            insert("insert into a values ('S2', 1), ('S3', 2),('S1', 3+3600000000),('S2', 4+3600000000), ('S1', 5+3600000000);");

            bindVariableService.clear();
            bindVariableService.setStr("s1", "S1");
            bindVariableService.setStr("s2", "S2");

            String queryAsc = "select * from a where s in (:s2, :s1) order by ts asc limit 5";
            assertPlan(
                    queryAsc,
                    "Limit lo: 5\n" +
                            "    FilterOnValues\n" +
                            "        Table-order scan\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s2::string\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s1::string\n" +
                            "        Frame forward scan on: a\n"
            );
            assertQuery("s\tts\n" +
                    "S2\t1970-01-01T00:00:00.000001Z\n" +
                    "S1\t1970-01-01T01:00:00.000003Z\n" +
                    "S2\t1970-01-01T01:00:00.000004Z\n" +
                    "S1\t1970-01-01T01:00:00.000005Z\n", queryAsc, "ts", true, false);

            String queryDesc = "select * from a where s in (:s2, :s1) order by ts desc limit 5";
            assertPlan(
                    queryDesc,
                    "Sort light lo: 5\n" +
                            "  keys: [ts desc]\n" +
                            "    FilterOnValues symbolOrder: desc\n" +
                            "        Cursor-order scan\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s2::string\n" +
                            "            Index forward scan on: s deferred: true\n" +
                            "              filter: s=:s1::string\n" +
                            "        Frame backward scan on: a\n"
            );
            assertQuery("s\tts\n" +
                    "S1\t1970-01-01T01:00:00.000005Z\n" +
                    "S2\t1970-01-01T01:00:00.000004Z\n" +
                    "S1\t1970-01-01T01:00:00.000003Z\n" +
                    "S2\t1970-01-01T00:00:00.000001Z\n", queryDesc, "ts###DESC", true, true);
        });
    }
}

