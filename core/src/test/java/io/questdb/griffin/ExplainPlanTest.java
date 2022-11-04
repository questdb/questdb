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

import org.junit.Ignore;
import org.junit.Test;

public class ExplainPlanTest extends AbstractGriffinTest {

    @Test
    public void testSelectFromTableWriterMetrics() throws SqlException {
        assertPlan("select * from table_writer_metrics()",
                "table_writer_metrics\n");
    }

    @Test
    public void testSelectFromAllTables() throws SqlException {
        assertPlan("select * from all_tables()",
                "all_tables\n");
    }

    @Test
    public void testSelectFromTableColumns() throws SqlException {
        assertPlan("create table tab ( s string, sy symbol, i int, ts timestamp)",
                "select * from table_columns('tab')",
                "show_columns of: tab\n");
    }

    @Test
    public void testSelectFromReaderPool() throws SqlException {
        assertPlan("select * from reader_pool()",
                "reader_pool\n");
    }

    @Test
    public void testSelectFromMemoryMetrics() throws SqlException {
        assertPlan("select * from memory_metrics()",
                "memory_metrics\n");
    }

    @Test
    public void testSelectRandomBoolean() throws SqlException {
        assertPlan("select rnd_boolean()",
                "VirtualRecord\n" +
                        "  functions: [rnd_boolean()]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testSelectConcat() throws SqlException {
        assertPlan("select concat('a', 'b', rnd_str('c', 'd', 'e'))",
                "VirtualRecord\n" +
                        "  functions: [concat(['a','b',rnd_str([c,d,e])])]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testSelectDistinct0() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct l, ts from tab",
                "DistinctTimeSeries\n" +
                        "  keys: l,ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Ignore
    @Test//FIXME: somehow only ts gets included, pg returns record type   
    public void testSelectDistinct0a() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select distinct (l, ts) from tab",
                "DistinctTimeSeries\n" +
                        "  keys: l,ts\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectDistinct1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select distinct(l) from tab",
                "Distinct\n" +
                        "  keys: l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: should scan symbols table 
    public void testSelectDistinct2() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctKey\n" +
                        "    GroupByRecord vectorized: true\n" +
                        "      groupByFunctions: [count(1)]\n" +
                        "      keyColumn: s\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n");
    }

    @Test//TODO: should scan symbols table
    public void testSelectDistinct3() throws SqlException {
        assertPlan("create table tab ( s symbol index, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctKey\n" +
                        "    GroupByRecord vectorized: true\n" +
                        "      groupByFunctions: [count(1)]\n" +
                        "      keyColumn: s\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectDistinct4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select distinct ts, l  from tab",
                "Distinct\n" +
                        "  keys: ts,l\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: should use symbol list
    public void testSelectCountDistinct1() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select count_distinct(s)  from tab",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(s)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: should use symbol list
    public void testSelectCountDistinct2() throws SqlException {
        assertPlan("create table tab ( s symbol index, ts timestamp);",
                "select count_distinct(s)  from tab",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(s)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectCountDistinct3() throws SqlException {
        assertPlan("create table tab ( s string, l long );",
                "select count_distinct(l)  from tab",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 ",
                "Async JIT Filter\n" +
                        "  filter: 100<l\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 ",
                "Async JIT Filter\n" +
                        "  filter: 100<l and l<1000\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }


    @Test//TODO: this one should use jit !
    public void testSelectWithJittedFilter3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and ts = '2022-01-01' ",
                "Async Filter\n" +
                        "  filter: 100<l and l<1000 and ts=1640995200000000\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and l = 20",
                "Async JIT Filter\n" +
                        "  filter: 100<l and l<1000 and l=20\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter5() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or l = 20",
                "Async JIT Filter\n" +
                        "  filter: (100<l and l<1000 or l=20)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or ts = 123",
                "Async JIT Filter\n" +
                        "  filter: (100<l and l<1000 or ts=123)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: this one should use jit  
    public void testSelectWithJittedFilter7() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 or ts > '2021-01-01'",
                "Async Filter\n" +
                        "  filter: (100<l and l<1000 or 1609459200000000<ts)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter8() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 and ts in '2021-01-01'",
                "Async JIT Filter\n" +
                        "  filter: 100<l and l<1000\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [static=[1609459200000000,1609545599999999]\n");
    }

    @Test//TODO: this one should use jit
    public void testSelectWithJittedFilter9() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l in ( 100, 200 )",
                "Async Filter\n" +
                        "  filter: l in [100,200]\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: this one should use jit
    public void testSelectWithJittedFilter10() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where s in ( 'A', 'B' )",
                "Async Filter\n" +
                        "  filter: s in [A,B]\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: this one should interval scan without filter
    public void testSelectWithJittedFilter11() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-02' )",
                "Async Filter\n" +
                        "  filter: ts in [1577836800000000,1577923200000000]\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: this one should interval scan with jit filter 
    public void testSelectWithJittedFilter12() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-03' ) and s = 'ABC'",
                "Async Filter\n" +
                        "  filter: ts in [1577836800000000,1578009600000000] and s='ABC'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter13() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01' ) and s = 'ABC'",
                "Async Filter\n" +
                        "  filter: ts in [1577836800000000,1577923199999999] and s='ABC'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter14() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12 or l = 15 ",
                "Async JIT Filter\n" +
                        "  filter: (l=12 or l=15)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter15() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12.345 ",
                "Async JIT Filter\n" +
                        "  filter: l=12.345\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter16() throws SqlException {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = false ",
                "Async JIT Filter\n" +
                        "  filter: b=false\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter17() throws SqlException {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where not(b = false or ts = 123) ",
                "Async JIT Filter\n" +
                        "  filter: b!=false and ts!=123\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter18() throws SqlException {
        assertPlan("create table tab ( l1 long, l2 long);",
                "select * from tab where l1 < l2 ",
                "Async JIT Filter\n" +
                        "  filter: l1<l2\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter19() throws SqlException {
        assertPlan("create table tab ( l1 long, l2 long);",
                "select * from tab where l1 * l2 > 0  ",
                "Async JIT Filter\n" +
                        "  filter: 0<l1*l2\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter20() throws SqlException {
        assertPlan("create table tab ( l1 long, l2 long, l3 long);",
                "select * from tab where l1 * l2 > l3  ",
                "Async JIT Filter\n" +
                        "  filter: l3<l1*l2\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter21() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1 ",
                "Async JIT Filter\n" +
                        "  filter: l=$0::long\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter22() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1 + 1 ",
                "Async JIT Filter\n" +
                        "  filter: d=1024.1+1\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter23() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = null ",
                "Async JIT Filter\n" +
                        "  filter: d is null\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24a() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit 1 ",
                "Limit lo: 1\n" +
                        "    Async JIT Filter\n" +
                        "      filter: d=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n");
    }

    @Test//TODO: this one should scan from the end like in testSelectWithJittedFilter24c()  
    public void testSelectWithJittedFilter24b() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 ",
                "Limit lo: -1\n" +
                        "    Async JIT Filter\n" +
                        "      filter: d=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24c() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 ",
                "Limit lo: 1\n" +
                        "    Async JIT Filter\n" +
                        "      filter: d=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter25() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 ",
                "Limit lo: 1\n" +
                        "    Async JIT Filter\n" +
                        "      filter: d=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: tab\n");
    }

    //TODO: this is misleading because actual scan order is determined in getCursor() and depends on base order and actual limit value
    @Test
    public void testSelectWithJittedFilter26() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 ",
                "Limit lo: -1\n" +
                        "    Async JIT Filter\n" +
                        "      filter: d=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: tab\n");
    }

    @Test//jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::short ",
                "Async Filter\n" +
                        "  filter: l=12::short\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::byte ",
                "Async Filter\n" +
                        "  filter: l=12::byte\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = '123' ",
                "Async Filter\n" +
                        "  filter: l='123'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit is not because rnd_long() value is not stable
    public void testSelectWithNonJittedFilter4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = rnd_long() ",
                "Async Filter\n" +
                        "  filter: l=rnd_long()\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectWithNonJittedFilter5() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = case when l > 0 then 1 when l = 0 then 0 else -1 end ",
                "Async Filter\n" +
                        "  filter: l=case([0<l,1,l=0,0,-1])\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//interval scan is not used because of type mismatch
    public void testSelectWithNonJittedFilter6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1::string ",
                "Async Filter\n" +
                        "  filter: l=$0::double::string\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit filter doesn't work for string type 
    public void testSelectWithNonJittedFilter7() throws SqlException {
        assertPlan("create table tab ( s string, ts timestamp);",
                "select * from tab where s = 'test' ",
                "Async Filter\n" +
                        "  filter: s='test'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit filter doesn't work for string type 
    public void testSelectWithNonJittedFilter8() throws SqlException {
        assertPlan("create table tab ( s string, ts timestamp);",
                "select * from tab where s = null ",
                "Async Filter\n" +
                        "  filter: s is null\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit filter doesn't work with type casts 
    public void testSelectWithNonJittedFilter9() throws SqlException {
        assertPlan("create table tab ( b byte, ts timestamp);",
                "select * from tab where b = 1::byte ",
                "Async Filter\n" +
                        "  filter: b=1::byte\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit filter doesn't work with type casts 
    public void testSelectWithNonJittedFilter10() throws SqlException {
        assertPlan("create table tab ( s short, ts timestamp);",
                "select * from tab where s = 1::short ",
                "Async Filter\n" +
                        "  filter: s=1::short\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like b = true 
    public void testSelectWithNonJittedFilter11() throws SqlException {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = true::boolean ",
                "Async Filter\n" +
                        "  filter: b=true\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like l = 1024 
    public void testSelectWithNonJittedFilter12() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 1024::long ",
                "Async Filter\n" +
                        "  filter: l=1024::long\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like d = 1024.1 
    public void testSelectWithNonJittedFilter13() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1::double ",
                "Async Filter\n" +
                        "  filter: d=1024.1\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like d = null 
    public void testSelectWithNonJittedFilter14() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = null::double ",
                "Async Filter\n" +
                        "  filter: d is null\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit doesn't work for bitwise operators  
    public void testSelectWithNonJittedFilter15() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l | l) > 0  ",
                "Async Filter\n" +
                        "  filter: 0<l|l\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit doesn't work for bitwise operators  
    public void testSelectWithNonJittedFilter16() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l & l) > 0  ",
                "Async Filter\n" +
                        "  filter: 0<l&l\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//jit doesn't work for bitwise operators  
    public void testSelectWithNonJittedFilter17() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0  ",
                "Async Filter\n" +
                        "  filter: 0<l^l\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectStaticTsIntervalOnTabWithoutDesignatedTimestamp() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where ts > '2020-03-01'",
                "Async Filter\n" +
                        "  filter: 1583020800000000<ts\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectStaticTsInterval1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2020-03-01'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1583020800000001,9223372036854775807]\n");
    }

    @Test
    public void testSelectStaticTsInterval2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1583020800000000,1583107199999999]\n");
    }

    @Test//TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' or ts in '2020-03-10'",
                "Async Filter\n" +
                        "  filter: (ts in [1583020800000000,1583107199999999] or ts in [1583798400000000,1583884799999999])\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//ranges don't overlap so result is empty
    public void testSelectStaticTsInterval4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' and ts in '2020-03-10'",
                "Empty table\n");
    }

    @Test//only 2020-03-10->2020-03-31 needs to be scanned 
    public void testSelectStaticTsInterval5() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03' and ts > '2020-03-10'",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1583798400000001,1585699199999999]\n");
    }

    @Test//TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts > '2020-03-01' and ts < '2020-03-10') or (ts > '2020-04-01' and ts < '2020-04-10') ",
                "Async Filter\n" +
                        "  filter: (1583020800000000<ts and ts<1583798400000000 or 1585699200000000<ts and ts<1586476800000000)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval7() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts between '2020-03-01' and '2020-03-10') or (ts between '2020-04-01' and '2020-04-10') ",
                "Async Filter\n" +
                        "  filter: (ts between 1583020800000000 and 1583798400000000 or ts between 1585699200000000 and 1586476800000000)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectStaticTsInterval8() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' ",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test//TODO: this should use backward interval scan without sorting
    public void testSelectStaticTsInterval9() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by ts desc ",
                "Sort light\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test
    public void testSelectStaticTsInterval10() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc ",
                "Sort light\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test//there's no interval scan because sysdate is evaluated per-row
    public void testSelectDynamicTsInterval1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > sysdate()",
                "Async Filter\n" +
                        "  filter: sysdate()<ts\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test//there's no interval scan because systimestamp is evaluated per-row
    public void testSelectDynamicTsInterval2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > systimestamp()",
                "Async Filter\n" +
                        "  filter: systimestamp()<ts\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: tab\n");
    }

    @Test
    public void testSelectDynamicTsInterval3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[0,9223372036854775807,281481419161601,4294967296] dynamic=[now()]]\n");
    }

    @Test
    public void testSelectDynamicTsInterval4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > dateadd('d', -1, now()) and ts < now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[NaN,0,844422782648321,4294967296,0,9223372036854775807,281481419161601,4294967296] dynamic=[now(),dateadd('d',-1,now())]]\n");
    }

    @Test
    public void testSelectDynamicTsInterval5() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now()",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Interval forward scan on: tab\n" +
                        "      intervals: [static=[0,9223372036854775807,281481419161601,4294967296,1640995200000001,9223372036854775807,2147483649,4294967296] dynamic=[now(),null]]\n");
    }

    @Test//TODO: should use backward interval scan
    public void testSelectDynamicTsInterval6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now() order by ts desc",
                "Sort light\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Interval forward scan on: tab\n" +
                        "          intervals: [static=[0,9223372036854775807,281481419161601,4294967296,1640995200000001,9223372036854775807,2147483649,4294967296] dynamic=[now(),null]]\n");
    }

    @Test
    public void testSelect0() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder1() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l<i",
                "Async JIT Filter\n" +
                        "  filter: l<i\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder2() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select ts, l, i from a where l::short<i",
                "Async Filter\n" +
                        "  filter: l::short<i\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder2a() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select i2, i1, ts1 from " +
                        "(select ts as ts1, l as l1, i as i1, i as i2 " +
                        "from a " +
                        "where l::short<i " +
                        "limit 100) " +
                        "where l1*i2 != 0",
                "SelectedRecord\n" +
                        "    Filter filter: l1*i2!=0\n" +
                        "        Limit lo: 100\n" +
                        "            SelectedRecord\n" +
                        "                SelectedRecord\n" +
                        "                    Async Filter\n" +
                        "                      limit: 100\n" +
                        "                      filter: l::short<i\n" +
                        "                      preTouch: true\n" +
                        "                      workers: 1\n" +
                        "                        DataFrame\n" +
                        "                            Row forward scan\n" +
                        "                            Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder2b() throws SqlException {
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
                        "            SelectedRecord\n" +
                        "                SelectedRecord\n" +
                        "                    DataFrame\n" +
                        "                        Row forward scan\n" +
                        "                        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder3() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select k, max(ts) from ( select ts, l as k, i from a where l::short<i ) where k < 0 ",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [max(ts)]\n" +
                        "    SelectedRecord\n" +
                        "        Async Filter\n" +
                        "          filter: l::short<i and l<0\n" +
                        "          preTouch: true\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithReorder4() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts) ;",
                "select mil, k, minl, mini from " +
                        "( select ts as k, max(i*l) as mil, min(i) as mini, min(l) as minl  " +
                        "from a where l::short<i ) " +
                        "where mil + mini> 1 ",
                "Filter filter: 1<mil+mini\n" +
                        "    GroupByRecord vectorized: false\n" +
                        "      groupByFunctions: [max(i*l),min(l),min(i)]\n" +
                        "        SelectedRecord\n" +
                        "            Async Filter\n" +
                        "              filter: l::short<i\n" +
                        "              preTouch: true\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLo() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10",
                "Limit lo: 10\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLo() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' order by ts desc limit 1 ",
                "Sort light lo: 1\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols1() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in ('S1', 'S2') order by s desc limit 1",
                "Sort light lo: 1\n" +
                        "    FilterOnValues\n" +
                        "        Cursor-order scan\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S1'\n" +
                        "            Index forward scan on: s deferred: true\n" +
                        "              filter: s='S2'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols1a() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select ts, s from a where s in ('S1', 'S2') and length(s) = 2 order by s desc limit 1",
                "Sort light lo: 1\n" +
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

    @Test //TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !  
    public void testSelectIndexedSymbols2() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = $1 or s = $2 order by ts desc limit 1",
                "Limit lo: 1\n" +
                        "    Async JIT Filter\n" +
                        "      filter: (s=$0::string or s=$1::string)\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test //TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !  
    public void testSelectIndexedSymbols3() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' or s = 'S2' order by ts desc limit 1",
                "Limit lo: 1\n" +
                        "    Async JIT Filter\n" +
                        "      filter: (s='S1' or s='S2')\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row backward scan\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test//TODO: it would be better to get rid of unnecessary sort and limit factories 
    public void testSelectIndexedSymbols4() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' and s = 'S2' order by ts desc limit 1",
                "Limit lo: 1\n" +
                        "    Sort\n" +
                        "        Empty table\n");
    }

    @Test
    public void testSelectIndexedSymbols5() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in (select 'S1' union all select 'S2') order by ts desc limit 1",
                "Sort light lo: 1\n" +
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
    public void testSelectIndexedSymbols5a() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in (select 'S1' union all select 'S2') and length(s) = 2 order by ts desc limit 1",
                "Sort light lo: 1\n" +
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

    @Test//TODO: this one should scan index/data file backward and skip sorting  
    public void testSelectIndexedSymbols6() throws SqlException {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s = 'S1' order by s asc limit 10",
                "Sort light lo: 10\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols7() throws SqlException {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s != 'S1' and length(s) = 2 order by s ",
                "Sort light\n" +
                        "    FilterOnExcludedValues\n" +
                        "      symbolFilter: s not in ['S1']\n" +
                        "      filter: length(s)=2\n" +
                        "        Cursor-order scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols8() throws SqlException {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s != 'S1' order by s ",
                "Sort light\n" +
                        "    FilterOnExcludedValues\n" +
                        "      symbolFilter: s not in ['S1']\n" +
                        "        Cursor-order scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols9() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by year ;",
                "select * from a where ts >= 0::timestamp and ts < 100::timestamp order by s asc",
                "SortedSymbolIndex\n" +
                        "    Index forward scan on: s\n" +
                        "      symbolOrder: asc\n" +
                        "    Interval forward scan on: a\n" +
                        "      intervals: [static=[0,99]\n");
    }

    @Test//TODO: having multiple cursors on the same level isn't very clear 
    public void testSelectIndexedSymbols10() throws SqlException {
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

    @Test//TODO: having multiple cursors on the same level isn't very clear 
    public void testSelectIndexedSymbols11() throws SqlException {
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
    }

    @Test
    public void testSelectIndexedSymbols12() throws SqlException {
        compile("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
        compile("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
        assertPlan("select * from a where s1 in ('S1', 'S2') and s2 in ('S2') limit 1",
                "Limit lo: 1\n" +
                        "    DataFrame\n" +
                        "        Index forward scan on: s2\n" +
                        "          filter: s2=2 and s1 in [S1,S2]\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols13() throws SqlException {
        compile("create table a ( s1 symbol index, s2 symbol index, ts timestamp) timestamp(ts)");
        compile("insert into a select 'S' || x, 'S' || x, x::timestamp from long_sequence(10)");
        assertPlan("select * from a where s1 in ('S1')  order by ts desc",
                "Sort light\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s1\n" +
                        "          filter: s1=1\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols14() throws SqlException {
        compile("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
        compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
        assertPlan("select * from a where s1 = 'S1'  order by ts desc",
                "Sort light\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s1\n" +
                        "          filter: s1=1\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//backward index scan is triggered only if query uses a single partition and orders by key column and ts desc  
    public void testSelectIndexedSymbols15() throws SqlException {
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
    }

    @Test
    public void testSelectIndexedSymbols16() throws SqlException {
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
    }

    @Test//TODO: should use the same plan as above 
    public void testSelectIndexedSymbols17() throws SqlException {
        compile("create table a ( s1 symbol index, ts timestamp) timestamp(ts) partition by year;");
        compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");
        assertPlan("select * from a " +
                        "where (s1 = 'S1' or s1 = 'S2') " +
                        "and ts > 0::timestamp and ts < 9::timestamp  " +
                        "order by s1,ts desc",
                "Sort light\n" +
                        "    Async JIT Filter\n" +
                        "      filter: (s1='S1' or s1='S2')\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Interval forward scan on: a\n" +
                        "              intervals: [static=[1,8]\n");
    }


    @Test//TODO: query should scan from end of table with limit = 10 
    public void testSelectWithLimitLoNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10",
                "Limit lo: -10\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//TODO: query should scan from end of table with limit = 10 
    public void testSelectWithOrderByTsLimitLoNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts limit -10",
                "Limit lo: -10\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLo() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit 10",
                "Limit lo: 10\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test//TODO: query should scan from start of table with limit = 10
    public void testSelectWithOrderByTsDescLimitLoNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10",
                "Limit lo: -10\n" +
                        "    DataFrame\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testSelectOrderedWithLimitLoHi() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i limit 10, 100",
                "Sort light lo: 10 hi: 100\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLoHi() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10, 100",
                "Limit lo: 10 hi: 100\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLoHiNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10, -100",
                "Limit lo: -10 hi: -100\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderedAsc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i asc",
                "Sort light\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderedDesc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i desc",
                "Sort light\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsAsc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts asc",
                "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: a\n");
    }

    @Test
    public void testSelectDesc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc",
                "DataFrame\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testSelectDesc2() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) ;",
                "select * from a order by ts desc",
                "Sort light\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectDescMaterialized() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) ;",
                "select * from (select i, ts from a union all select 1, null ) order by ts desc",
                "Sort\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        VirtualRecord\n" +
                        "          functions: [1,null]\n" +
                        "            long_sequence count: 1\n");
    }

    @Test
    public void testUnionAll() throws SqlException {
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
    public void testUnion() throws SqlException {
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
    public void testExcept() throws SqlException {
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
    public void testIntersect() throws SqlException {
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
    public void testMultiUnionAll() throws SqlException {
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
    public void testMultiUnion() throws SqlException {
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
    public void testMultiExcept() throws SqlException {
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
    public void testMultiIntersect() throws SqlException {
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
    public void testSampleByFirstLast() throws SqlException {
        assertPlan("create table a ( l long, s string, sym symbol index, i int, ts timestamp) timestamp(ts) partition by day;",
                "select sym, first(i), last(sym), first(l) " +
                        "from a " +
                        "where sym in ('S') " +
                        "and   ts > 0::timestamp and ts < 100::timestamp " +
                        "sample by 1h",
                "SampleByFirstLast\n" +
                        "  groupByColumn: sym\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: sym deferred: true\n" +
                        "          filter: sym='S'\n" +
                        "        Interval forward scan on: a\n" +
                        "          intervals: [static=[1,99]\n");
    }

    @Test
    public void testSampleBy() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h",
                "SampleByFillNoneNotKeyed\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillPrev() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(prev)",
                "SampleByFillPrevNotKeyed\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillNull() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(null)",
                "SampleByFillNullNotKeyed\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillValue() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(1)",
                "SampleByFillValueNotKeyed\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByFillLinear() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(linear)",
                "SampleByInterpolate\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed0() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h",
                "SampleByFillNone\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed1() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, i, first(i) from a sample by 1h",
                "SampleByFillNone\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed2() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1h fill(null)",
                "SampleByFillNull\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed3() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1d fill(linear)",
                "SampleByInterpolate\n" +
                        "  groupByFunctions: [first(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed4() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(1,2)",
                "SampleByFillValue\n" +
                        "  groupByFunctions: [first(i),last(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed5() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(prev,prev)",
                "SampleByFillValue\n" +
                        "  groupByFunctions: [first(i),last(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testLatestOn0a() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from (select * from a where i = 10 union select * from a where i =20) latest on ts partition by i",
                "SelectedRecord\n" +
                        "    LatestBy\n" +
                        "        Union\n" +
                        "            Async JIT Filter\n" +
                        "              filter: i=10\n" +
                        "              preTouch: true\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "            Async JIT Filter\n" +
                        "              filter: i=20\n" +
                        "              preTouch: true\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test
    public void testLatestOn0b() throws SqlException {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select ts,i from a where s in ('ABC') and i > 0 latest on ts partition by s",
                "SelectedRecord\n" +
                        "    LatestByValueDeferredFiltered\n" +
                        "      filter: 0<i\n" +
                        "      symbolFilter: s='ABC'\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn0c() throws SqlException {
        compile("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
        compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

        assertPlan("select ts,i from a where s in ('a1') and i > 0 latest on ts partition by s",
                "SelectedRecord\n" +
                        "    LatestByValueFiltered\n" +
                        "        Row backward scan\n" +
                        "          symbolFilter: s=0\n" +
                        "          filter: 0<i\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn0d() throws SqlException {
        compile("create table a ( i int, s symbol, ts timestamp) timestamp(ts);");
        compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

        assertPlan("select ts,i from a where s in ('a1') latest on ts partition by s",
                "SelectedRecord\n" +
                        "    LatestByValueFiltered\n" +
                        "        Row backward scan\n" +
                        "          symbolFilter: s=0\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn0e() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
        compile("insert into a select 10-x, 'a' || x, x::timestamp from long_sequence(10)");

        assertPlan("select ts,i, s from a where s in ('a1') and i > 0 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: 0<i\n" +
                        "  symbolFilter: s=1\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn0() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select i from a latest on ts partition by i",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn1() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by i",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn1a() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from (select ts, i as i1, i as i2 from a ) where 0 < i1 and i2 < 10 latest on ts partition by i1",
                "LatestBy light order_by_timestamp: true\n" +
                        "    SelectedRecord\n" +
                        "        SelectedRecord\n" +
                        "            Async JIT Filter\n" +
                        "              filter: 0<i and i<10\n" +
                        "              preTouch: true\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test
    public void testLatestOn1b() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select ts, i as i1, i as i2 from a where 0 < i and i < 10 latest on ts partition by i",
                "SelectedRecord\n" +
                        "    SelectedRecord\n" +
                        "        LatestByAllFiltered\n" +
                        "            Row backward scan\n" +
                        "              filter: 0<i and i<10\n" +
                        "            Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn2() throws SqlException {
        assertPlan("create table a ( i int, d double, ts timestamp) timestamp(ts);",
                "select ts, d from a latest on ts partition by i",
                "SelectedRecord\n" +
                        "    LatestByAllFiltered\n" +
                        "        Row backward scan\n" +
                        "        Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn3() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by s",
                "LatestByAllIndexed\n" +
                        "    Index backward scan on: s parallel: true\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn4() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  = 'S1' latest on ts partition by s",
                "DataFrame\n" +
                        "    Index backward scan on: s deferred: true\n" +
                        "      filter: s='S1'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn5a() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
        compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

        assertPlan(
                "select s, i, ts from a where s  in ('def1', 'def2') latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  symbolFilter: s in ['def1','def2']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn5b() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
        compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

        assertPlan(
                "select s, i, ts from a where s  in ('1', 'deferred') latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  symbolFilter: s in [1] or s in ['deferred']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn5c() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
        compile("insert into a select x, x::symbol, x::timestamp from long_sequence(10) ");

        assertPlan(
                "select s, i, ts from a where s  in ('1', '2') latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  symbolFilter: s in [1,2]\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn6() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and i > 0 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: 0<i\n" +
                        "  symbolFilter: s in ['S1','S2']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn7() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and length(s)<10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)<10\n" +
                        "  symbolFilter: s in ['S1','S2']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn8() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
        compile("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

        assertPlan("select s, i, ts from a where s  in ('s1') latest on ts partition by s",
                "DataFrame\n" +
                        "    Index backward scan on: s\n" +
                        "      filter: s=1\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//key outside list of symbols 
    public void testLatestOn8a() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
        compile("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

        assertPlan("select s, i, ts from a where s  in ('bogus_key') latest on ts partition by s",
                "DataFrame\n" +
                        "    Index backward scan on: s deferred: true\n" +
                        "      filter: s='bogus_key'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//columns in order different to table's 
    public void testLatestOn9() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)=10\n" +
                        "  symbolFilter: s='S1'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//columns in table's order 
    public void testLatestOn9a() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, s, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)=10\n" +
                        "  symbolFilter: s='S1'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn9b() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);");
        compile("insert into a select x::int, 'S' || x, x::timestamp from long_sequence(10)");

        assertPlan("select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "Index backward scan on: s\n" +
                        "  filter: length(s)=10\n" +
                        "  symbolFilter: s=1\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//TODO: should use index
    public void testLatestOn10() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s = 'S1' or s = 'S2' latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  filter: (s='S1' or s='S2')\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testLatestOn11() throws SqlException {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in ('S1', 'S2') latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  symbolFunctions: ['S1','S2']\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//TODO: subquery should just read symbols from map  
    public void testLatestOn12() throws SqlException {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupByRecord vectorized: true\n" +
                        "              groupByFunctions: [count(1)]\n" +
                        "              keyColumn: s\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Row backward scan on: s\n" +
                        "      filter: length(s)=2\n" +
                        "    Frame backward scan on: a\n");

    }

    @Test//TODO: subquery should just read symbols from map  
    public void testLatestOn12a() throws SqlException {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupByRecord vectorized: true\n" +
                        "              groupByFunctions: [count(1)]\n" +
                        "              keyColumn: s\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Row backward scan on: s\n" +
                        "    Frame backward scan on: a\n");

    }

    @Test//TODO: subquery should just read symbols from map  
    public void testLatestOn13() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) and length(s) = 2 latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupByRecord vectorized: true\n" +
                        "              groupByFunctions: [count(1)]\n" +
                        "              keyColumn: s\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Index backward scan on: s\n" +
                        "      filter: length(s)=2\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//TODO: subquery should just read symbols from map  
    public void testLatestOn13a() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select i, ts, s from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery\n" +
                        "    Subquery\n" +
                        "        DistinctKey\n" +
                        "            GroupByRecord vectorized: true\n" +
                        "              groupByFunctions: [count(1)]\n" +
                        "              keyColumn: s\n" +
                        "              workers: 1\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n" +
                        "    Index backward scan on: s\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//TODO: should use one or two indexes   
    public void testLatestOn14() throws SqlException {
        assertPlan("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' and i > 0 latest on ts partition by s1,s2",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "      filter: s1 in [S1,S2] and s2='S3' and 0<i\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test//TODO: should use one or two indexes   
    public void testLatestOn15() throws SqlException {
        assertPlan("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' latest on ts partition by s1,s2",
                "LatestByAllFiltered\n" +
                        "    Row backward scan\n" +
                        "      filter: s1 in [S1,S2] and s2='S3'\n" +
                        "    Frame backward scan on: a\n");
    }

    @Test
    public void testCastFloatToDouble() throws SqlException {
        assertPlan("select rnd_float()::double) ",
                "VirtualRecord\n" +
                        "  functions: [rnd_float()::double]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testLtJoin0() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select ts1, ts2, i1, i2 from (select a.i as i1, a.ts as ts1, b.i as i2, b.ts as ts2 from a lt join b on ts) where ts1::long*i1<ts2::long*i2",
                "SelectedRecord\n" +
                        "    Filter filter: a.ts::long*a.i<b.ts::long*b.i\n" +
                        "        Lt join light\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testLtJoin1() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a lt join b on ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: b\n");
    }

    @Ignore
    @Test //FIXME 
    public void testLtJoin1a() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a lt join b on ts where a.i = b.ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        Async JIT Filter\n" +
                        "          filter: i=ts WRONG!\n" +//no guarantee that a.ts = b.ts 
                        "          preTouch: true\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: b\n");
    }

    @Test
    public void testLtJoin2() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a lt join (select * from b limit 10) on ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Limit lo: 10\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testLtOfJoin3() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a lt join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        SelectedRecord\n" +
                        "            Sort light\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: b\n");
    }

    @Test
    public void testLtOfJoin4() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * " +
                        "from a " +
                        "lt join b on ts " +
                        "lt join a c on ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        Lt join light\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testAsOfJoinNoKey() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join b",
                "SelectedRecord\n" +
                        "    AsOf join [no key record]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: b\n");

    }

    @Ignore
    @Test//FIXME
    public void testAsOfJoin0() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join b on ts where a.i = b.ts::int",
                "");
    }

    @Test
    public void testAsOfJoin0a() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select ts, ts1, i, i1 from (select * from a asof join b on ts ) where i/10 = i1",
                "SelectedRecord\n" +
                        "    Filter filter: a.i/10=b.i\n" +
                        "        AsOf join light\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testAsOfJoin1() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join b on ts",
                "SelectedRecord\n" +
                        "    AsOf join light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: b\n");
    }

    @Test
    public void testAsOfJoin2() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join (select * from b limit 10) on ts",
                "SelectedRecord\n" +
                        "    AsOf join light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Limit lo: 10\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testAsOfJoin3() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                "SelectedRecord\n" +
                        "    AsOf join light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        SelectedRecord\n" +
                        "            Sort light\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: b\n");
    }

    @Test
    public void testAsOfJoin4() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * " +
                        "from a " +
                        "asof join b on ts " +
                        "asof join a c on ts",
                "SelectedRecord\n" +
                        "    AsOf join light\n" +
                        "        AsOf join light\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Ignore
    @Test
    public void testSpliceJoin0() throws SqlException {
        compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");
        compile("create table b ( i int, ts timestamp, l long) timestamp(ts)");

        assertPlan("select * from a splice join b on ts where a.i = b.ts",
                "SelectedRecord\n" +
                        "    Splice join\n" +
                        "        Async JIT Filter\n" +
                        "          filter: i=ts WRONG!\n" +
                        "          preTouch: true\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: b\n");
    }

    @Test
    public void testSpliceJoin0a() throws SqlException {
        compile("create table a ( i int, ts timestamp, l long) timestamp(ts)");
        compile("create table b ( i int, ts timestamp, l long) timestamp(ts)");

        assertPlan("select * from a splice join b on ts where a.i + b.i = 1",
                "SelectedRecord\n" +
                        "    Filter filter: a.i+b.i=1\n" +
                        "        Splice join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testSpliceJoin1() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a splice join b on ts",
                "SelectedRecord\n" +
                        "    Splice join\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: b\n");
    }

    @Test
    public void testSpliceJoin2() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a splice join (select * from b limit 10) on ts",
                "SelectedRecord\n" +
                        "    Splice join\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Limit lo: 10\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testSpliceJoin3() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a splice join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                "SelectedRecord\n" +
                        "    Splice join\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        SelectedRecord\n" +
                        "            Sort light\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: b\n");
    }

    @Ignore
    @Test//TODO: check this plan, hash join is not expected here. It's treated as  a.s1=b.s2! 
    public void testCrossJoin0() throws SqlException {
        assertPlan("create table a ( i int, s1 string, s2 string)",
                "select * from a cross join a b where length(a.s1) = length(b.s2)",
                "SelectedRecord\n" +
                        "    Hash Join Light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n\n");
    }

    @Ignore
    @Test//FIXME! where clause condition should be true for all row pairs but is executed as a.s1 = b.s2!
    public void testCrossJoinOutput() throws Exception {
        assertQuery("",
                "select * from a cross join a b where length(a.s1) = length(b.s2)",
                "create table a as (select x, 's' || x as s1, 's' || (x%3) as s2 from long_sequence(6))", null, false);
    }

    @Test
    public void testCrossJoin1() throws SqlException {
        assertPlan("create table a ( i int)",
                "select * from a cross join a b",
                "SelectedRecord\n" +
                        "    Cross join\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testCrossJoin2() throws SqlException {
        assertPlan("create table a ( i int)",
                "select * from a cross join a b cross join a c",
                "SelectedRecord\n" +
                        "    Cross join\n" +
                        "        Cross join\n" +
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

    //FIXME
    @Ignore("Fails with 'io.questdb.griffin.SqlException: [17] unexpected token: b'")
    @Test
    public void testImplicitJoin() throws SqlException {
        compile("create table a ( i1 int)");
        compile("create table b ( i2 int)");

        assertPlan("select * from a, b where a.i1 = b.i2",
                "SelectedRecord\n" +
                        "    Cross join\n" +
                        "        Cross join\n" +
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
    public void testHashInnerJoin() throws SqlException {
        compile("create table a ( i int, s1 string)");
        compile("create table b ( i int, s2 string)");

        assertPlan("select s1, s2 from (select a.s1, b.s2, b.i, a.i  from a join b on i) where i < i1 and s1 = s2",
                "SelectedRecord\n" +
                        "    Filter filter: b.i<a.i and a.s1=b.s2\n" +
                        "        Hash Join Light\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            Hash\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: b\n");
    }

    @Test
    public void testHashLeftJoin() throws SqlException {
        compile("create table a ( i int)");
        compile("create table b ( i int)");

        assertPlan("select * from a left join b on i",
                "SelectedRecord\n" +
                        "    Hash outer join light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testHashLeftJoin1() throws SqlException {
        compile("create table a ( i int)");
        compile("create table b ( i int)");

        assertPlan("select * from a left join b on i where b.i is not null",
                "SelectedRecord\n" +
                        "    Filter filter: b.i!=null\n" +
                        "        Hash outer join light\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            Hash\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: b\n");
    }

    @Test
    public void testHashRightJoin() throws SqlException {
        compile("create table a ( i int)");
        compile("create table b ( i int)");

        assertPlan("select * from a right join b on i",
                "SelectedRecord\n" +
                        "    Hash Join Light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    //FIXME
    //fails with io.questdb.griffin.SqlException: [42] Invalid table name or alias
    //looks like column aliasing doesn't work for right-joined tables
    @Ignore
    @Test
    public void testHashRightJoin1() throws SqlException {
        compile("create table a (i int)");
        compile("create table b (i int)");

        assertPlan("select bi,ai from (select b.i bi, a.i ai from a right join b on i) where bi < ai",
                "SelectedRecord\n" +
                        "    Hash Join Light\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        Hash\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: b\n");
    }

    @Test
    public void testFullFatHashJoin0() throws SqlException {
        compiler.setFullFatJoins(true);
        try {
            assertPlan("create table a ( l long)",
                    "select * from a join (select l from a where l > 10 limit 4) b on l where a.l+b.l > 0 ",
                    "SelectedRecord\n" +
                            "    Filter filter: 0<a.l+b.l\n" +
                            "        Hash join\n" +
                            "            DataFrame\n" +
                            "                Row forward scan\n" +
                            "                Frame forward scan on: a\n" +
                            "            Hash\n" +
                            "                Async JIT Filter\n" +
                            "                  limit: 4\n" +
                            "                  filter: 10<l\n" +
                            "                  preTouch: true\n" +
                            "                  workers: 1\n" +
                            "                    DataFrame\n" +
                            "                        Row forward scan\n" +
                            "                        Frame forward scan on: a\n");
        } finally {
            compiler.setFullFatJoins(false);
        }
    }

    @Test
    public void testFullFatHashJoin1() throws SqlException {
        compiler.setFullFatJoins(true);
        try {
            assertPlan("create table a ( l long)",
                    "select * from a join (select l from a limit 40) on l",
                    "SelectedRecord\n" +
                            "    Hash join\n" +
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
    public void testGroupByNotKeyed1() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select min(d) from a",
                "GroupByNotKeyed vectorized: true\n" +
                        "  groupByFunctions: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed2() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select min(d), max(d*d) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [min(d),max(d*d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed3() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(d+1) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(d+1)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed4() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count(*), max(i), min(d) from a",
                "GroupByNotKeyed vectorized: true\n" +
                        "  groupByFunctions: [count(0),max(i),min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//group by on filtered data is not vectorized 
    public void testGroupByNotKeyed6() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from a where i < 10",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(i)]\n" +
                        "    Async JIT Filter\n" +
                        "      filter: i<10\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test//order by is ignored and grouped by - vectorized
    public void testGroupByNotKeyed7() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a order by d)",
                "GroupByNotKeyed vectorized: true\n" +
                        "  groupByFunctions: [max(i)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//order by can't be ignored; group by is not vectorized 
    public void testGroupByNotKeyed8() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a order by d limit 10)",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(i)]\n" +
                        "    Sort light lo: 10\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test//TODO: group by could be vectorized for union tables and result merged 
    public void testGroupByNotKeyed9() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a union all select * from a)",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(i)]\n" +
                        "    Union All\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed10() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a join a b on i )",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(i)]\n" +
                        "    SelectedRecord\n" +
                        "        Hash Join Light\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            Hash\n" +
                        "                DataFrame\n" +
                        "                    Row forward scan\n" +
                        "                    Frame forward scan on: a\n");
    }

    @Test//constant values used in aggregates disable vectorization
    public void testGroupByNotKeyed5() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select first(10), last(d), avg(10), min(10), max(10) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [first(10),last(d),avg(10),min(10),max(10)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount1() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count(*) from a",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount2() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count() from a",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//TODO: this should use Count factory same as queries above 
    public void testSelectCount3() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count(2) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(0)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount4() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from a where s = 'S1'",
                "Count\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount5() throws SqlException {
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
    public void testSelectCount6() throws SqlException {
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
    public void testSelectCount7() throws SqlException {
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
    public void testSelectCount8() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from a where 1=0 ",
                "Count\n" +
                        "    Empty table\n");
    }

    @Test
    public void testSelectCount9() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from a where 1=1 ",
                "Count\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount10() throws SqlException {
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


    @Test//TODO: should return count on first table instead 
    public void testSelectCount11() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a lt join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Lt join no key\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test//TODO: should return count on first table instead 
    public void testSelectCount12() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a asof join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        AsOf join [no key record]\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test//TODO: should return count(first table)*count(second_table) instead 
    public void testSelectCount13() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a cross join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Cross join\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Frame forward scan on: a\n");
    }

    @Test
    public void testSelectCount14() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)",
                "select * from a where s = 'S1' order by ts desc ",
                "Sort light\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        Index forward scan on: s deferred: true\n" +
                        "          filter: s='S1'\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testGroupByInt1() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select min(d), i from a group by i",
                "GroupByRecord vectorized: true\n" +
                        "  groupByFunctions: [min(d)]\n" +
                        "  keyColumn: i\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//repeated int key disables vectorized impl
    public void testGroupByInt2() throws SqlException {
        assertPlan("create table a ( i int, d double)", "select i, i, min(d) from a group by i, i",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test //special case
    public void testGroupByHour() throws SqlException {
        assertPlan("create table a ( ts timestamp, d double)",
                "select hour(ts), min(d) from a group by hour(ts)",
                "GroupByRecord vectorized: true\n" +
                        "  groupByFunctions: [min(d)]\n" +
                        "  keyColumn: ts\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByLong() throws SqlException {
        assertPlan("create table a ( l long, d double)",
                "select l, min(d) from a group by l",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(d)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByDouble() throws SqlException {
        assertPlan("create table a ( l long, d double)",
                "select d, min(l) from a group by d",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByFloat() throws SqlException {
        assertPlan("create table a ( l long, f float)",
                "select f, min(l) from a group by f",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByBoolean() throws SqlException {
        assertPlan("create table a ( l long, b boolean)",
                "select b, min(l) from a group by b",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(l)]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testExplainCreateTable() throws SqlException {
        assertSql("explain create table a ( l long, d double)",
                "QUERY PLAN\n" +
                        "CREATE_TABLE table: a\n");
    }

    @Test
    public void testExplainCreateTableAsSelect() throws SqlException {
        assertSql("explain create table a as (select x, 1 from long_sequence(10))",
                "QUERY PLAN\n" +
                        "CREATE_TABLE table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [x,1]\n" +
                        "        long_sequence count: 10\n");
    }

    @Test
    public void testExplainInsert() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain insert into a values (1, 2.0)",
                "QUERY PLAN\n" +
                        "INSERT table: a\n");
    }

    @Test
    public void testExplainInsertAsSelect() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain insert into a select x, 1 from long_sequence(10)",
                "QUERY PLAN\n" +
                        "INSERT table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [x,1]\n" +
                        "        long_sequence count: 10\n");
    }

    @Test
    public void testExplainSelect() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain select * from a;",
                "QUERY PLAN\n" +
                        "DataFrame\n" +
                        "    Row forward scan\n" +
                        "    Frame forward scan on: a\n");
    }

    @Test
    public void testExplainSelectWith() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain with b as (select * from a limit 10) select * from b;",
                "QUERY PLAN\n" +
                        "Limit lo: 10\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testExplainUpdate() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain update a set l = 1, d=10.1;",
                "QUERY PLAN\n" +
                        "UPDATE table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [1,10.1]\n" +
                        "        DataFrame\n" +
                        "            Row forward scan\n" +
                        "            Frame forward scan on: a\n");
    }

    @Test
    public void testExplainUpdateWithFilter() throws SqlException {
        assertPlan("create table a ( l long, d double, ts timestamp) timestamp(ts)",
                "update a set l = 20, d = d+rnd_double() " +
                        "where d < 100.0d and ts > dateadd('d', -1, now());",
                "UPDATE table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [20,d+rnd_double()]\n" +
                        "        Async JIT Filter\n" +
                        "          filter: d<100.0\n" +
                        "          preTouch: true\n" +
                        "          workers: 1\n" +
                        "            DataFrame\n" +
                        "                Row forward scan\n" +
                        "                Interval forward scan on: a\n" +
                        "                  intervals: [static=[0,9223372036854775807,281481419161601,4294967296] dynamic=[dateadd('d',-1,now())]]\n");
    }

    @Test
    public void testExplainPlanWithEOLs1() throws SqlException {
        assertPlan("create table a (s string)",
                "select * from a where s = '\n'",
                "Async Filter\n" +
                        "  filter: s='\\n'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: a\n");
    }

    @Test
    public void testAnalytic0() throws SqlException {
        assertPlan("create table t as ( select x l, x::string str, x::timestamp ts from long_sequence(100))",
                "select ts, str,  row_number() over (order by l), row_number() over (partition by l) from t",
                "CachedAnalytic\n" +
                        "  functions: [row_number(),row_number()]\n" +
                        "    DataFrame\n" +
                        "        Row forward scan\n" +
                        "        Frame forward scan on: t\n");
    }

    @Test
    public void testAnalytic1() throws SqlException {
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
    public void testAnalytic2() throws SqlException {
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

    private void assertPlan(String ddl, String query, String expectedPlan) throws SqlException {
        compile(ddl);
        assertPlan(query, expectedPlan);
    }
}
