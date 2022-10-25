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
    public void testSelectDistinct1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select distinct(l) from tab",
                "Distinct\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: should scan symbols table 
    public void testSelectDistinct2() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctKey\n" +
                        "    GroupByRecord vectorized: true\n" +
                        "      groupByFunctions: [count(1)]\n" +
                        "      keyColumnIndex: 0\n" +
                        "      workers: 1\n" +
                        "        Full forward scan on: tab\n");
    }

    @Test//TODO: should scan symbols table
    public void testSelectDistinct3() throws SqlException {
        assertPlan("create table tab ( s symbol index, ts timestamp);",
                "select distinct(s) from tab",
                "DistinctKey\n" +
                        "    GroupByRecord vectorized: true\n" +
                        "      groupByFunctions: [count(1)]\n" +
                        "      keyColumnIndex: 0\n" +
                        "      workers: 1\n" +
                        "        Full forward scan on: tab\n");
    }

    @Test
    public void testSelectDistinct4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select distinct ts, l  from tab",
                "Distinct\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: should use symbol list
    public void testSelectCountDistinct1() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select count_distinct(s)  from tab",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(Symbol(0))]\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: should use symbol list
    public void testSelectCountDistinct2() throws SqlException {
        assertPlan("create table tab ( s symbol index, ts timestamp);",
                "select count_distinct(s)  from tab",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(Symbol(0))]\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectCountDistinct3() throws SqlException {
        assertPlan("create table tab ( s string, l long );",
                "select count_distinct(l)  from tab",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(Long(0))]\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 ",
                "async jit filter\n" +
                        "  filter: 100<Long(0)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 ",
                "async jit filter\n" +
                        "  filter: 100<Long(0) and Long(0)<1000\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }


    @Test//TODO: this one should use jit !
    public void testSelectWithJittedFilter3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and ts = '2022-01-01' ",
                "async filter\n" +
                        "  filter: 100<Long(0) and Long(0)<1000 and Timestamp(1)=1640995200000000\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 and l = 20",
                "async jit filter\n" +
                        "  filter: 100<Long(0) and Long(0)<1000 and Long(0)=20\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter5() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or l = 20",
                "async jit filter\n" +
                        "  filter: (100<Long(0) and Long(0)<1000 or Long(0)=20)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l > 100 and l < 1000 or ts = 123",
                "async jit filter\n" +
                        "  filter: (100<Long(0) and Long(0)<1000 or Timestamp(1)=123)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: this one should use jit  
    public void testSelectWithJittedFilter7() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 or ts > '2021-01-01'",
                "async filter\n" +
                        "  filter: (100<Long(0) and Long(0)<1000 or 1609459200000000<Timestamp(1))\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter8() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp (ts);",
                "select * from tab where l > 100 and l < 1000 and ts in '2021-01-01'",
                "async jit filter\n" +
                        "  filter: 100<Long(0) and Long(0)<1000\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Interval forward Scan on: tab\n" +
                        "      intervals: [static=[1609459200000000,1609545599999999]\n");
    }

    @Test//TODO: this one should use jit
    public void testSelectWithJittedFilter9() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l in ( 100, 200 )",
                "async filter\n" +
                        "  filter: Long(0) in [100,200]\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: this one should use jit
    public void testSelectWithJittedFilter10() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where s in ( 'A', 'B' )",
                "async filter\n" +
                        "  filter: Symbol(0) in [A,B]\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: this one should interval scan without filter
    public void testSelectWithJittedFilter11() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-02' )",
                "async filter\n" +
                        "  filter: Timestamp(1) in [1577836800000000,1577923200000000]\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: this one should interval scan with jit filter 
    public void testSelectWithJittedFilter12() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01', '2020-01-03' ) and s = 'ABC'",
                "async filter\n" +
                        "  filter: Timestamp(1) in [1577836800000000,1578009600000000] and Symbol(0)='ABC'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: this one should interval scan with jit filter
    public void testSelectWithJittedFilter13() throws SqlException {
        assertPlan("create table tab ( s symbol, ts timestamp);",
                "select * from tab where ts in ( '2020-01-01' ) and s = 'ABC'",
                "async filter\n" +
                        "  filter: Timestamp(1) in [1577836800000000,1577923199999999] and Symbol(0)='ABC'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter14() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12 or l = 15 ",
                "async jit filter\n" +
                        "  filter: (Long(0)=12 or Long(0)=15)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter15() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12.345 ",
                "async jit filter\n" +
                        "  filter: Long(0)=12.345\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter16() throws SqlException {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = false ",
                "async jit filter\n" +
                        "  filter: Boolean(0)=false\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter17() throws SqlException {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where not(b = false or ts = 123) ",
                "async jit filter\n" +
                        "  filter: Boolean(0)!=false and Timestamp(1)!=123\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter18() throws SqlException {
        assertPlan("create table tab ( l1 long, l2 long);",
                "select * from tab where l1 < l2 ",
                "async jit filter\n" +
                        "  filter: Long(0)<Long(1)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter19() throws SqlException {
        assertPlan("create table tab ( l1 long, l2 long);",
                "select * from tab where l1 * l2 > 0  ",
                "async jit filter\n" +
                        "  filter: 0<Long(0)*Long(1)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter20() throws SqlException {
        assertPlan("create table tab ( l1 long, l2 long, l3 long);",
                "select * from tab where l1 * l2 > l3  ",
                "async jit filter\n" +
                        "  filter: Long(2)<Long(0)*Long(1)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter21() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1 ",
                "async jit filter\n" +
                        "  filter: Long(0)=$0::long\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter22() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1 + 1 ",
                "async jit filter\n" +
                        "  filter: Double(0)=1024.1+1\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter23() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = null ",
                "async jit filter\n" +
                        "  filter: Double(0) is null\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter24() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit 1 ",
                "Limit lo: 1\n" +
                        "    async jit filter\n" +
                        "      filter: Double(0)=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithJittedFilter25() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts desc limit 1 ",
                "Limit lo: 1\n" +
                        "    async jit filter\n" +
                        "      filter: Double(0)=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        Full backward scan on: tab\n");
    }

    //TODO: this is misleading because actual scan order is determined in getCursor() and depends on base order and actual limit value
    @Test
    public void testSelectWithJittedFilter26() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp) timestamp(ts);",
                "select * from tab where d = 1.2 order by ts limit -1 ",
                "Limit lo: -1\n" +
                        "    async jit filter\n" +
                        "      filter: Double(0)=1.2\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        Full forward scan on: tab\n");
    }

    @Test//jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::short ",
                "async filter\n" +
                        "  filter: Long(0)=12::short\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 12::byte ",
                "async filter\n" +
                        "  filter: Long(0)=12::byte\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit is not used due to type mismatch
    public void testSelectWithNonJittedFilter3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = '123' ",
                "async filter\n" +
                        "  filter: Long(0)='123'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit is not because rnd_long() value is not stable
    public void testSelectWithNonJittedFilter4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = rnd_long() ",
                "async filter\n" +
                        "  filter: Long(0)=rnd_long()\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectWithNonJittedFilter5() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = case when l > 0 then 1 when l = 0 then 0 else -1 end ",
                "async filter\n" +
                        "  filter: Long(0)=case([0<Long(0),1,Long(0)=0,0,-1])\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//interval scan is not used because of type mismatch
    public void testSelectWithNonJittedFilter6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = $1::string ",
                "async filter\n" +
                        "  filter: Long(0)=$0::double::string\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit filter doesn't work for string type 
    public void testSelectWithNonJittedFilter7() throws SqlException {
        assertPlan("create table tab ( s string, ts timestamp);",
                "select * from tab where s = 'test' ",
                "async filter\n" +
                        "  filter: Str(0)='test'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit filter doesn't work for string type 
    public void testSelectWithNonJittedFilter8() throws SqlException {
        assertPlan("create table tab ( s string, ts timestamp);",
                "select * from tab where s = null ",
                "async filter\n" +
                        "  filter: Str(0) is null\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit filter doesn't work with type casts 
    public void testSelectWithNonJittedFilter9() throws SqlException {
        assertPlan("create table tab ( b byte, ts timestamp);",
                "select * from tab where b = 1::byte ",
                "async filter\n" +
                        "  filter: Byte(0)=1::byte\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit filter doesn't work with type casts 
    public void testSelectWithNonJittedFilter10() throws SqlException {
        assertPlan("create table tab ( s short, ts timestamp);",
                "select * from tab where s = 1::short ",
                "async filter\n" +
                        "  filter: Short(0)=1::short\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like b = true 
    public void testSelectWithNonJittedFilter11() throws SqlException {
        assertPlan("create table tab ( b boolean, ts timestamp);",
                "select * from tab where b = true::boolean ",
                "async filter\n" +
                        "  filter: Boolean(0)=true\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like l = 1024 
    public void testSelectWithNonJittedFilter12() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where l = 1024::long ",
                "async filter\n" +
                        "  filter: Long(0)=1024::long\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like d = 1024.1 
    public void testSelectWithNonJittedFilter13() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = 1024.1::double ",
                "async filter\n" +
                        "  filter: Double(0)=1024.1\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: should run with jitted filter just like d = null 
    public void testSelectWithNonJittedFilter14() throws SqlException {
        assertPlan("create table tab ( d double, ts timestamp);",
                "select * from tab where d = null::double ",
                "async filter\n" +
                        "  filter: Double(0) is null\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit doesn't work for bitwise operators  
    public void testSelectWithNonJittedFilter15() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l | l) > 0  ",
                "async filter\n" +
                        "  filter: 0<Long(0)|Long(0)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit doesn't work for bitwise operators  
    public void testSelectWithNonJittedFilter16() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l & l) > 0  ",
                "async filter\n" +
                        "  filter: 0<Long(0)&Long(0)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//jit doesn't work for bitwise operators  
    public void testSelectWithNonJittedFilter17() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where (l ^ l) > 0  ",
                "async filter\n" +
                        "  filter: 0<Long(0)^Long(0)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectStaticTsIntervalOnTabWithoutDesignatedTimestamp() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp);",
                "select * from tab where ts > '2020-03-01'",
                "async filter\n" +
                        "  filter: 1583020800000000<Timestamp(1)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectStaticTsInterval1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2020-03-01'",
                "Interval forward Scan on: tab\n" +
                        "  intervals: [static=[1583020800000001,9223372036854775807]\n");
    }

    @Test
    public void testSelectStaticTsInterval2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01'",
                "Interval forward Scan on: tab\n" +
                        "  intervals: [static=[1583020800000000,1583107199999999]\n");
    }

    @Test//TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-03-01' or ts in '2020-03-10'",
                "async filter\n" +
                        "  filter: (Timestamp(1) in [1583020800000000,1583107199999999] or Timestamp(1) in [1583798400000000,1583884799999999])\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
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
                "Interval forward Scan on: tab\n" +
                        "  intervals: [static=[1583798400000001,1585699199999999]\n");
    }

    @Test//TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts > '2020-03-01' and ts < '2020-03-10') or (ts > '2020-04-01' and ts < '2020-04-10') ",
                "async filter\n" +
                        "  filter: (1583020800000000<Timestamp(1) and Timestamp(1)<1583798400000000 or 1585699200000000<Timestamp(1) and Timestamp(1)<1586476800000000)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//TODO: this should use interval scan with two ranges !
    public void testSelectStaticTsInterval7() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where (ts between '2020-03-01' and '2020-03-10') or (ts between '2020-04-01' and '2020-04-10') ",
                "async filter\n" +
                        "  filter: (Timestamp(1) between 1583020800000000 and 1583798400000000 or Timestamp(1) between 1585699200000000 and 1586476800000000)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectStaticTsInterval8() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' ",
                "Interval forward Scan on: tab\n" +
                        "  intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test//TODO: this should use backward interval scan without sorting
    public void testSelectStaticTsInterval9() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by ts desc ",
                "Sort light\n" +
                        "    Interval forward Scan on: tab\n" +
                        "      intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test
    public void testSelectStaticTsInterval10() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts in '2020-01-01T03:00:00;1h;24h;3' order by l desc ",
                "Sort light\n" +
                        "    Interval forward Scan on: tab\n" +
                        "      intervals: [static=[1577847600000000,1577851200999999,1577934000000000,1577937600999999,1578020400000000,1578024000999999]\n");
    }

    @Test//sysdate is evaluated per-row
    public void testSelectDynamicTsInterval1() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > sysdate()",
                "async filter\n" +
                        "  filter: sysdate()<Timestamp(1)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test//systimestamp is evaluated per-row
    public void testSelectDynamicTsInterval2() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > systimestamp()",
                "async filter\n" +
                        "  filter: systimestamp()<Timestamp(1)\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: tab\n");
    }

    @Test
    public void testSelectDynamicTsInterval3() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > now()",
                "Interval forward Scan on: tab\n" +
                        "  intervals: [static=[0,9223372036854775807,281481419161601,4294967296] dynamic=[now()]]\n");
    }

    @Test
    public void testSelectDynamicTsInterval4() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > dateadd('d', -1, now()) and ts < now()",
                "Interval forward Scan on: tab\n" +
                        "  intervals: [static=[NaN,0,844422782648321,4294967296,0,9223372036854775807,281481419161601,4294967296] dynamic=[now(),dateadd('d',-1,now())]]\n");
    }

    @Test
    public void testSelectDynamicTsInterval5() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now()",
                "Interval forward Scan on: tab\n" +
                        "  intervals: [static=[0,9223372036854775807,281481419161601,4294967296,1640995200000001,9223372036854775807,2147483649,4294967296] dynamic=[now(),null]]\n");
    }

    @Test//TODO: should use backward interval scan
    public void testSelectDynamicTsInterval6() throws SqlException {
        assertPlan("create table tab ( l long, ts timestamp) timestamp(ts);",
                "select * from tab where ts > '2022-01-01' and ts > now() order by ts desc",
                "Sort light\n" +
                        "    Interval forward Scan on: tab\n" +
                        "      intervals: [static=[0,9223372036854775807,281481419161601,4294967296,1640995200000001,9223372036854775807,2147483649,4294967296] dynamic=[now(),null]]\n");
    }

    @Test
    public void testSelect() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a",
                "Full forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLo() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10",
                "Limit lo: 10\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbolWithLimitLo() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' order by ts desc limit 1 ",
                "Sort light lo: 1\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        DeferredSymbolIndexRowCursor\n" +
                        "          direction: forward\n" +
                        "          usesIndex: true\n" +
                        "          filter: Symbol(0)='S1'\n" +
                        "        Full forward scan on: a\n");
    }


    @Test//TODO: having multiple cursors on the same level isn't very clear 
    public void testSelectIndexedSymbols1() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in ('S1', 'S2') order by s desc limit 1",
                "Sort light lo: 1\n" +
                        "    FilterOnValues\n" +
                        "        SequentialRowCursor\n" +
                        "            DeferredSymbolIndexRowCursor\n" +
                        "              direction: forward\n" +
                        "              usesIndex: true\n" +
                        "              filter: Symbol(0)='S1'\n" +
                        "            DeferredSymbolIndexRowCursor\n" +
                        "              direction: forward\n" +
                        "              usesIndex: true\n" +
                        "              filter: Symbol(0)='S2'\n" +
                        "        Full forward scan on: a\n");
    }

    @Test //TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !  
    public void testSelectIndexedSymbols2() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = $1 or s = $2 order by ts desc limit 1",
                "Limit lo: 1\n" +
                        "    async jit filter\n" +
                        "      filter: (Symbol(0)=$0::string or Symbol(0)=$1::string)\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        Full backward scan on: a\n");
    }

    @Test //TODO: sql is same as in testSelectIndexedSymbols1 but doesn't use index !  
    public void testSelectIndexedSymbols3() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s = 'S1' or s = 'S2' order by ts desc limit 1",
                "Limit lo: 1\n" +
                        "    async jit filter\n" +
                        "      filter: (Symbol(0)='S1' or Symbol(0)='S2')\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        Full backward scan on: a\n");
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
                        "        Union all\n" +
                        "            VirtualRecord\n" +
                        "              functions: ['S1']\n" +
                        "                long_sequence count: 1\n" +
                        "            VirtualRecord\n" +
                        "              functions: ['S2']\n" +
                        "                long_sequence count: 1\n" +
                        "        Full forward scan on: a\n");
    }

    @Test//TODO: this one should scan index/data file backward and skip sorting  
    public void testSelectIndexedSymbols6() throws SqlException {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s = 'S1' order by s asc limit 10",
                "Sort light lo: 10\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        DeferredSymbolIndexRowCursor\n" +
                        "          direction: forward\n" +
                        "          usesIndex: true\n" +
                        "          filter: Symbol(0)='S1'\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols7() throws SqlException {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s != 'S1' and length(s) = 2 order by s ",
                "Sort light\n" +
                        "    FilterOnExcludedValues\n" +
                        "      symbolFilter: Symbol(0) not in ['S1']\n" +
                        "      filter: length(Symbol(0))=2\n" +
                        "        SequentialRowCursor\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols8() throws SqlException {
        assertPlan("create table a ( s symbol index) ;",
                "select * from a where s != 'S1' order by s ",
                "Sort light\n" +
                        "    FilterOnExcludedValues\n" +
                        "      symbolFilter: Symbol(0) not in ['S1']\n" +
                        "        SequentialRowCursor\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSelectIndexedSymbols9() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) partition by year ;",
                "select * from a where ts >= 0::timestamp and ts < 100::timestamp order by s asc",
                "SortedSymbolIndex\n" +
                        "    SortedSymbolIndexRowCursor\n" +
                        "      usesIndex: true\n" +
                        "      direction: forward\n" +
                        "    Interval forward Scan on: a\n" +
                        "      intervals: [static=[0,99]\n");
    }

    @Test//TODO: having multiple cursors on the same level isn't very clear 
    public void testSelectIndexedSymbols10() throws SqlException {
        assertPlan("create table a ( s symbol index, ts timestamp) timestamp(ts) ;",
                "select * from a where s in ('S1', 'S2') limit 1",
                "Limit lo: 1\n" +
                        "    FilterOnValues\n" +
                        "        HeapRowCursor\n" +
                        "            DeferredSymbolIndexRowCursor\n" +
                        "              direction: forward\n" +
                        "              usesIndex: true\n" +
                        "              filter: Symbol(0)='S1'\n" +
                        "            DeferredSymbolIndexRowCursor\n" +
                        "              direction: forward\n" +
                        "              usesIndex: true\n" +
                        "              filter: Symbol(0)='S2'\n" +
                        "        Full forward scan on: a\n");
    }

    @Test//TODO: having multiple cursors on the same level isn't very clear 
    public void testSelectIndexedSymbols11() throws SqlException {
        compile("create table a ( s symbol index, ts timestamp) timestamp(ts)");
        compile("insert into a select 'S' || x, x::timestamp from long_sequence(10)");

        assertPlan("select * from a where s in ('S1', 'S2') and length(s) = 2 limit 1",
                "Limit lo: 1\n" +
                        "    FilterOnValues\n" +
                        "        HeapRowCursor\n" +
                        "            SymbolIndexFilteredRowCursor\n" +
                        "              direction: forward\n" +
                        "              usesIndex: true\n" +
                        "              symbolValue: 'S1'\n" +
                        "              filter: length(Symbol(0))=2\n" +
                        "            SymbolIndexFilteredRowCursor\n" +
                        "              direction: forward\n" +
                        "              usesIndex: true\n" +
                        "              symbolValue: 'S2'\n" +
                        "              filter: length(Symbol(0))=2\n" +
                        "        Full forward scan on: a\n");
    }

    @Test//TODO: query should scan from end of table with limit = 10 
    public void testSelectWithLimitLoNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10",
                "Limit lo: -10\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//TODO: query should scan from end of table with limit = 10 
    public void testSelectWithOrderByTsLimitLoNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts limit -10",
                "Limit lo: -10\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectWithOrderByTsDescLimitLo() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit 10",
                "Limit lo: 10\n" +
                        "    Full backward scan on: a\n");
    }

    @Test//TODO: query should scan from start of table with limit = 10
    public void testSelectWithOrderByTsDescLimitLoNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc limit -10",
                "Limit lo: -10\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testSelectOrderedWithLimitLoHi() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i limit 10, 100",
                "Sort light lo: 10 hi: 100\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLoHi() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit 10, 100",
                "Limit lo: 10 hi: 100\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectWithLimitLoHiNegative() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a limit -10, -100",
                "Limit lo: -10 hi: -100\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectOrderedAsc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i asc",
                "Sort light\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectOrderedDesc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by i desc",
                "Sort light\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectOrderByTsAsc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts asc",
                "Full forward scan on: a\n");
    }

    @Test
    public void testSelectDesc() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts) ;",
                "select * from a order by ts desc",
                "Full backward scan on: a\n");
    }

    @Test
    public void testSelectDesc2() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) ;",
                "select * from a order by ts desc",
                "Sort light\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectDescMaterialized() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) ;",
                "select * from (select i, ts from a union all select 1, null ) order by ts desc",
                "Sort\n" +
                        "    Union all\n" +
                        "        Full forward scan on: a\n" +
                        "        VirtualRecord\n" +
                        "          functions: [1,null]\n" +
                        "            long_sequence count: 1\n");
    }

    @Test
    public void testUnionAll() throws SqlException {
        assertPlan("create table a ( i int, s string);", "select * from a union all select * from a",
                "Union all\n" +
                        "    Full forward scan on: a\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testUnion() throws SqlException {
        assertPlan("create table a ( i int, s string);",
                "select * from a union select * from a",
                "Union\n" +
                        "    Full forward scan on: a\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testExcept() throws SqlException {
        assertPlan("create table a ( i int, s string);",
                "select * from a except select * from a",
                "Except\n" +
                        "    Full forward scan on: a\n" +
                        "    Hash\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testIntersect() throws SqlException {
        assertPlan("create table a ( i int, s string);",
                "select * from a intersect select * from a",
                "Intersect\n" +
                        "    Full forward scan on: a\n" +
                        "    Hash\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testMultiUnionAll() throws SqlException {
        assertPlan("create table a ( i int, s string);",
                "select * from a union all select * from a union all select * from a",
                "Union all\n" +
                        "    Union all\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: a\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testMultiUnion() throws SqlException {
        assertPlan("create table a ( i int, s string);",
                "select * from a union select * from a union select * from a",
                "Union\n" +
                        "    Union\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: a\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testMultiExcept() throws SqlException {
        assertPlan("create table a ( i int, s string);",
                "select * from a except select * from a except select * from a",
                "Except\n" +
                        "    Except\n" +
                        "        Full forward scan on: a\n" +
                        "        Hash\n" +
                        "            Full forward scan on: a\n" +
                        "    Hash\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testMultiIntersect() throws SqlException {
        assertPlan("create table a ( i int, s string);",
                "select * from a intersect select * from a intersect select * from a",
                "Intersect\n" +
                        "    Intersect\n" +
                        "        Full forward scan on: a\n" +
                        "        Hash\n" +
                        "            Full forward scan on: a\n" +
                        "    Hash\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSampleBy() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h",
                "SampleByFillNoneNotKeyed\n" +
                        "  groupByFunctions: [first(Int(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByFillPrev() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(prev)",
                "SampleByFillPrevNotKeyed\n" +
                        "  groupByFunctions: [first(Int(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByFillNull() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(null)",
                "SampleByFillNullNotKeyed\n" +
                        "  groupByFunctions: [first(Int(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByFillValue() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(1)",
                "SampleByFillValueNotKeyed\n" +
                        "  groupByFunctions: [first(Int(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByFillLinear() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select first(i) from a sample by 1h fill(linear)",
                "SampleByInterpolate\n" +
                        "  groupByFunctions: [first(Int(0))]\n" +
                        "  recordFunctions: [first(Int(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed1() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1h",
                "SampleByFillNone\n" +
                        "  functions: [Long(2),first(Int(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed2() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1h fill(null)",
                "SampleByFillNull\n" +
                        "  groupByFunctions: [first(Int(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed3() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i) from a sample by 1d fill(linear)",
                "SampleByInterpolate\n" +
                        "  groupByFunctions: [first(Int(1))]\n" +
                        "  recordFunctions: [Long(2),first(Int(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed4() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(1,2)",
                "SampleByFillValue\n" +
                        "  groupByFunctions: [first(Int(1)),last(Int(1))]\n" +
                        "  recordFunctions: [Long(3),first(Int(1)),last(Int(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSampleByKeyed5() throws SqlException {
        assertPlan("create table a ( i int, l long, ts timestamp) timestamp(ts);",
                "select l, first(i), last(i) from a sample by 1d fill(prev,prev)",
                "SampleByFillValue\n" +
                        "  groupByFunctions: [first(Int(1)),last(Int(1))]\n" +
                        "  recordFunctions: [Long(3),first(Int(1)),last(Int(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testLatestOn1() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by i",
                "LatestByAllFiltered\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn2() throws SqlException {
        assertPlan("create table a ( i int, d double, ts timestamp) timestamp(ts);",
                "select ts, d from a latest on ts partition by i",
                "SelectedRecord\n" +
                        "    LatestByAllFiltered\n" +
                        "        Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn3() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select * from a latest on ts partition by s",
                "LatestByAllIndexed\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn4() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  = 'S1' latest on ts partition by s",
                "DataFrame\n" +
                        "    LatestByValueDeferredIndexedRowCursor\n" +
                        "      direction: backward\n" +
                        "      usesIndex: true\n" +
                        "      filter: Symbol(1)='S1'\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn5() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') latest on ts partition by s",
                "LatestByValuesIndexed\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn6() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and i > 0 latest on ts partition by s",
                "LatestByValuesIndexed\n" +
                        "  filter: 0<Int(1)\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn7() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1', 'S2') and length(s)<10 latest on ts partition by s",
                "LatestByValuesIndexed\n" +
                        "  filter: length(Symbol(0))<10\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn8() throws SqlException {
        compile("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)");
        compile("insert into a select x::int, 's' ||(x%10), x::timestamp from long_sequence(1000)");

        assertPlan("select s, i, ts from a where s  in ('S1') latest on ts partition by s",
                "DataFrame\n" +
                        "    LatestByValueDeferredIndexedRowCursor\n" +
                        "      direction: backward\n" +
                        "      usesIndex: true\n" +
                        "      filter: Symbol(1)='S1'\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn9() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s  in ('S1') and length(s) = 10 latest on ts partition by s",
                "LatestByValueDeferredIndexedFiltered\n" +
                        "  filter: length(Symbol(0))=10\n" +
                        "    Full backward scan on: a\n");
    }

    @Test//TODO: should use index
    public void testLatestOn10() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s = 'S1' or s = 'S2' latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  filter: (Symbol(0)='S1' or Symbol(0)='S2')\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testLatestOn11() throws SqlException {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in ('S1', 'S2') latest on ts partition by s",
                "LatestByDeferredListValuesFiltered\n" +
                        "  symbolFunctions: ['S1','S2']\n" +
                        "    Full backward scan on: a\n");
    }

    @Test//TODO: subquery should just read symbols from map  
    public void testLatestOn12() throws SqlException {
        assertPlan("create table a ( i int, s symbol, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery indexed: false\n" +
                        "  columnIdx: 0\n" +
                        "    Full backward scan on: a\n" +
                        "    DistinctKey\n" +
                        "        GroupByRecord vectorized: true\n" +
                        "          groupByFunctions: [count(1)]\n" +
                        "          keyColumnIndex: 0\n" +
                        "          workers: 1\n" +
                        "            Full forward scan on: a\n");
    }

    @Test//TODO: subquery should just read symbols from map  
    public void testLatestOn13() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts);",
                "select s, i, ts from a where s in (select distinct s from a) latest on ts partition by s",
                "LatestBySubQuery indexed: true\n" +
                        "  columnIdx: 0\n" +
                        "    Full backward scan on: a\n" +
                        "    DistinctKey\n" +
                        "        GroupByRecord vectorized: true\n" +
                        "          groupByFunctions: [count(1)]\n" +
                        "          keyColumnIndex: 0\n" +
                        "          workers: 1\n" +
                        "            Full forward scan on: a\n");
    }

    @Test//TODO: should use one or two indexes   
    public void testLatestOn14() throws SqlException {
        assertPlan("create table a ( i int, s1 symbol index, s2 symbol index,  ts timestamp) timestamp(ts);",
                "select s1, s2, i, ts from a where s1 in ('S1', 'S2') and s2 = 'S3' and i > 0 latest on ts partition by s1,s2",
                "LatestByAllFiltered\n" +
                        "  filter: Symbol(0) in [S1,S2] and Symbol(1)='S3' and 0<Int(2)\n" +
                        "    Full backward scan on: a\n");
    }

    @Test
    public void testCastFloatToDouble() throws SqlException {
        assertPlan("select rnd_float()::double) ",
                "VirtualRecord\n" +
                        "  functions: [rnd_float()::double]\n" +
                        "    long_sequence count: 1\n");
    }

    @Test
    public void testLtJoin1() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a lt join b on ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: b\n");
    }

    @Test
    public void testLtJoin2() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a lt join (select * from b limit 10) on ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        Full forward scan on: a\n" +
                        "        Limit lo: 10\n" +
                        "            Full forward scan on: b\n");
    }

    @Test
    public void testLtOfJoin3() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a lt join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                "SelectedRecord\n" +
                        "    Lt join light\n" +
                        "        Full forward scan on: a\n" +
                        "        SelectedRecord\n" +
                        "            Sort light\n" +
                        "                Full forward scan on: b\n");
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
                        "            Full forward scan on: a\n" +
                        "            Full forward scan on: b\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testAsOfJoin1() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join b on ts",
                "SelectedRecord\n" +
                        "    AsOf join light\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: b\n");
    }

    @Test
    public void testAsOfJoin2() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join (select * from b limit 10) on ts",
                "SelectedRecord\n" +
                        "    AsOf join light\n" +
                        "        Full forward scan on: a\n" +
                        "        Limit lo: 10\n" +
                        "            Full forward scan on: b\n");
    }

    @Test
    public void testAsOfJoin3() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a asof join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                "SelectedRecord\n" +
                        "    AsOf join light\n" +
                        "        Full forward scan on: a\n" +
                        "        SelectedRecord\n" +
                        "            Sort light\n" +
                        "                Full forward scan on: b\n");
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
                        "            Full forward scan on: a\n" +
                        "            Full forward scan on: b\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSpliceJoin1() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a splice join b on ts",
                "SelectedRecord\n" +
                        "    Splice join\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: b\n");
    }

    @Test
    public void testSpliceJoin2() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a splice join (select * from b limit 10) on ts",
                "SelectedRecord\n" +
                        "    Splice join\n" +
                        "        Full forward scan on: a\n" +
                        "        Limit lo: 10\n" +
                        "            Full forward scan on: b\n");
    }

    @Test
    public void testSpliceJoin3() throws SqlException {
        compile("create table a ( i int, ts timestamp) timestamp(ts)");
        compile("create table b ( i int, ts timestamp) timestamp(ts)");

        assertPlan("select * from a splice join ((select * from b order by ts, i ) timestamp(ts))  on ts",
                "SelectedRecord\n" +
                        "    Splice join\n" +
                        "        Full forward scan on: a\n" +
                        "        SelectedRecord\n" +
                        "            Sort light\n" +
                        "                Full forward scan on: b\n");
    }

    @Test
    public void testCrossJoin1() throws SqlException {
        assertPlan("create table a ( i int)",
                "select * from a cross join a b",
                "SelectedRecord\n" +
                        "    Cross join\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testCrossJoin2() throws SqlException {
        assertPlan("create table a ( i int)",
                "select * from a cross join a b cross join a c",
                "SelectedRecord\n" +
                        "    Cross join\n" +
                        "        Cross join\n" +
                        "            Full forward scan on: a\n" +
                        "            Full forward scan on: a\n" +
                        "        Full forward scan on: a\n");
    }

    //TODO: fix
    @Ignore("Fails with 'io.questdb.griffin.SqlException: [17] unexpected token: b'")
    @Test
    public void testImplicitJoin() throws SqlException {
        compile("create table a ( i1 int)");
        compile("create table b ( i2 int)");

        assertPlan("select * from a, b where a.i1 = b.i2",
                "SelectedRecord\n" +
                        "    Cross join\n" +
                        "        Cross join\n" +
                        "            Full forward scan on: a\n" +
                        "            Full forward scan on: a\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testHashInnerJoin() throws SqlException {
        compile("create table a ( i int)");
        compile("create table b ( i int)");

        assertPlan("select * from a join b on i",
                "SelectedRecord\n" +
                        "    Hash join light\n" +
                        "        Full forward scan on: a\n" +
                        "        Hash\n" +
                        "            Full forward scan on: b\n");
    }

    @Test
    public void testHashLeftJoin() throws SqlException {
        compile("create table a ( i int)");
        compile("create table b ( i int)");

        assertPlan("select * from a left join b on i",
                "SelectedRecord\n" +
                        "    Hash outer join light\n" +
                        "        Full forward scan on: a\n" +
                        "        Hash\n" +
                        "            Full forward scan on: b\n");
    }

    @Test
    public void testHashRightJoin() throws SqlException {
        compile("create table a ( i int)");
        compile("create table b ( i int)");

        assertPlan("select * from a right join b on i",
                "SelectedRecord\n" +
                        "    Hash join light\n" +
                        "        Full forward scan on: a\n" +
                        "        Hash\n" +
                        "            Full forward scan on: b\n");
    }

    @Test
    public void testFullFatHashJoin() throws SqlException {
        compiler.setFullFatJoins(true);
        try {
            assertPlan("create table a ( l long)",
                    "select * from a join (select l from a limit 40) on l",
                    "SelectedRecord\n" +
                            "    Hash join\n" +
                            "        Full forward scan on: a\n" +
                            "        Hash\n" +
                            "            Limit lo: 40\n" +
                            "                Full forward scan on: a\n");
        } finally {
            compiler.setFullFatJoins(false);
        }
    }

    @Test
    public void testGroupByNotKeyed1() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select min(d) from a",
                "GroupByNotKeyed vectorized: true\n" +
                        "  groupByFunctions: [min(Double(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed2() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select min(d), max(d*d) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [min(Double(0)),max(Double(0)*Double(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//expressions in aggregates disable vectorized impl
    public void testGroupByNotKeyed3() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(d+1) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(Double(0)+1)]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed4() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count(*), max(i), min(d) from a",
                "GroupByNotKeyed vectorized: true\n" +
                        "  groupByFunctions: [count(0),max(Int(0)),min(Double(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//group by on filtered data is not vectorized 
    public void testGroupByNotKeyed6() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from a where i < 10",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(Int(0))]\n" +
                        "    async jit filter\n" +
                        "      filter: Int(0)<10\n" +
                        "      preTouch: true\n" +
                        "      workers: 1\n" +
                        "        Full forward scan on: a\n");
    }

    @Test//order by is ignored and grouped by - vectorized
    public void testGroupByNotKeyed7() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a order by d)",
                "GroupByNotKeyed vectorized: true\n" +
                        "  groupByFunctions: [max(Int(0))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//order by can't be ignored; group by is not vectorized 
    public void testGroupByNotKeyed8() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a order by d limit 10)",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(Int(0))]\n" +
                        "    Sort light lo: 10\n" +
                        "        Full forward scan on: a\n");
    }

    @Test//TODO: group by could be vectorized for union tables and result merged 
    public void testGroupByNotKeyed9() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a union all select * from a)",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(Int(0))]\n" +
                        "    Union all\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testGroupByNotKeyed10() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select max(i) from (select * from a join a b on i )",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [max(Int(0))]\n" +
                        "    SelectedRecord\n" +
                        "        Hash join light\n" +
                        "            Full forward scan on: a\n" +
                        "            Hash\n" +
                        "                Full forward scan on: a\n");
    }


    @Test//constant values used in aggregates disable vectorization
    public void testGroupByNotKeyed5() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select first(10), last(d), avg(10), min(10), max(10) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [first(10),last(Double(0)),avg(10),min(10),max(10)]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount1() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count(*) from a",
                "Count\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount2() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count() from a",
                "Count\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//TODO: this should use Count factory same as queries above 
    public void testSelectCount3() throws SqlException {
        assertPlan("create table a ( i int, d double)",
                "select count(2) from a",
                "GroupByNotKeyed vectorized: false\n" +
                        "  groupByFunctions: [count(0)]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount4() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from a where s = 'S1'",
                "Count\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        DeferredSymbolIndexRowCursor\n" +
                        "          direction: forward\n" +
                        "          usesIndex: true\n" +
                        "          filter: Symbol(1)='S1'\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount5() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union all select * from a) ",
                "Count\n" +
                        "    Union all\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount6() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select * from a union select * from a) ",
                "Count\n" +
                        "    Union\n" +
                        "        Full forward scan on: a\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount7() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select * from a intersect select * from a) ",
                "Count\n" +
                        "    Intersect\n" +
                        "        Full forward scan on: a\n" +
                        "        Hash\n" +
                        "            Full forward scan on: a\n");
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
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount10() throws SqlException {
        assertPlan("create table a ( i int, s symbol index)",
                "select count(*) from (select 1 from a limit 1) ",
                "Count\n" +
                        "    Limit lo: 1\n" +
                        "        VirtualRecord\n" +
                        "          functions: [1]\n" +
                        "            Full forward scan on: a\n");
    }


    @Test//TODO: should return count on first table instead 
    public void testSelectCount11() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a lt join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Lt join no key\n" +
                        "            Full forward scan on: a\n" +
                        "            Full forward scan on: a\n");
    }

    @Test//TODO: should return count on first table instead 
    public void testSelectCount12() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a asof join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        AsOf join [no key record]\n" +
                        "            Full forward scan on: a\n" +
                        "            Full forward scan on: a\n");
    }

    @Test//TODO: should return count(first table)*count(second_table) instead 
    public void testSelectCount13() throws SqlException {
        assertPlan("create table a ( i int, ts timestamp ) timestamp(ts)",
                "select count(*) from (select * from a cross join a b) ",
                "Count\n" +
                        "    SelectedRecord\n" +
                        "        Cross join\n" +
                        "            Full forward scan on: a\n" +
                        "            Full forward scan on: a\n");
    }

    @Test
    public void testSelectCount14() throws SqlException {
        assertPlan("create table a ( i int, s symbol index, ts timestamp) timestamp(ts)",
                "select * from a where s = 'S1' order by ts desc ",
                "Sort light\n" +
                        "    DeferredSingleSymbolFilterDataFrame\n" +
                        "        DeferredSymbolIndexRowCursor\n" +
                        "          direction: forward\n" +
                        "          usesIndex: true\n" +
                        "          filter: Symbol(1)='S1'\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testGroupByInt1() throws SqlException {
        assertPlan("create table a ( i int, d double)", "select i, min(d) from a group by i",
                "GroupByRecord vectorized: true\n" +
                        "  groupByFunctions: [min(Double(1))]\n" +
                        "  keyColumnIndex: 0\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//repeated int key disables vectorized impl
    public void testGroupByInt2() throws SqlException {
        assertPlan("create table a ( i int, d double)", "select i, i, min(d) from a group by i, i",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(Double(1))]\n" +
                        "  recordFunctions: [Int(1),Int(1),min(Double(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test //special case
    public void testGroupByHour() throws SqlException {
        assertPlan("create table a ( ts timestamp, d double)",
                "select hour(ts), min(d) from a group by hour(ts)",
                "GroupByRecord vectorized: true\n" +
                        "  groupByFunctions: [min(Double(1))]\n" +
                        "  keyColumnIndex: 0\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByLong() throws SqlException {
        assertPlan("create table a ( l long, d double)", "select l, min(d) from a group by l",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(Double(1))]\n" +
                        "  recordFunctions: [Long(1),min(Double(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByDouble() throws SqlException {
        assertPlan("create table a ( l long, d double)",
                "select d, min(l) from a group by d",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(Long(1))]\n" +
                        "  recordFunctions: [Double(1),min(Long(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByFloat() throws SqlException {
        assertPlan("create table a ( l long, f float)",
                "select f, min(l) from a group by f",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(Long(1))]\n" +
                        "  recordFunctions: [Float(1),min(Long(1))]\n" +
                        "    Full forward scan on: a\n");
    }

    @Test//only none, single int|symbol key cases are vectorized   
    public void testGroupByBoolean() throws SqlException {
        assertPlan("create table a ( l long, b boolean)",
                "select b, min(l) from a group by b",
                "GroupByRecord vectorized: false\n" +
                        "  groupByFunctions: [min(Long(1))]\n" +
                        "  recordFunctions: [Boolean(1),min(Long(1))]\n" +
                        "    Full forward scan on: a\n");
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
                        "      functions: [Long(0),1]\n" +
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
                        "      functions: [Long(0),1]\n" +
                        "        long_sequence count: 10\n");
    }

    @Test
    public void testExplainSelect() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain select * from a;",
                "QUERY PLAN\n" +
                        "Full forward scan on: a\n");
    }

    @Test
    public void testExplainSelectWith() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain with b as (select * from a limit 10) select * from b;",
                "QUERY PLAN\n" +
                        "Limit lo: 10\n" +
                        "    Full forward scan on: a\n");
    }

    @Test
    public void testExplainUpdate() throws SqlException {
        compile("create table a ( l long, d double)");
        assertSql("explain update a set l = 1;",
                "QUERY PLAN\n" +
                        "UPDATE table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [1]\n" +
                        "        Full forward scan on: a\n");
    }

    @Test
    public void testExplainUpdateWithFilter() throws SqlException {
        assertPlan("create table a ( l long, d double, ts timestamp) timestamp(ts)",
                "update a set l = 20, d = d+rnd_double() " +
                        "where d < 100.0d and ts > dateadd('d', -1, now());",
                "UPDATE table: a\n" +
                        "    VirtualRecord\n" +
                        "      functions: [20,Double(0)+rnd_double()]\n" +
                        "        async jit filter\n" +
                        "          filter: Double(0)<100.0\n" +
                        "          preTouch: true\n" +
                        "          workers: 1\n" +
                        "            Interval forward Scan on: a\n" +
                        "              intervals: [static=[0,9223372036854775807,281481419161601,4294967296] dynamic=[dateadd('d',-1,now())]]\n");
    }

    @Test
    public void testExplainPlanWithEOLs1() throws SqlException {
        assertPlan("create table a (s string)",
                "select * from a where s = '\n'",
                "async filter\n" +
                        "  filter: Str(0)='\\n'\n" +
                        "  preTouch: true\n" +
                        "  workers: 1\n" +
                        "    Full forward scan on: a\n");
    }

    private void assertPlan(String ddl, String query, String expectedPlan) throws SqlException {
        compile(ddl);
        assertPlan(query, expectedPlan);
    }
}
