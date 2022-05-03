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

import org.junit.Test;

/*
 * This class tests various distinct with order by and limit combinations.
 */
public class DistinctWithLimitTest extends AbstractGriffinTest {

    @Test
    public void testDistinctWithLimitOnLongColumn() throws Exception {
        assertQuery("id\n9\n8\n",
                "select DISTINCT id FROM limtest LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select 10-x as id from long_sequence(9)) ",
                null, true, true, true);
    }

    @Test
    public void testDistinctWithOrderByLimitOnLongColumn() throws Exception {
        assertQuery("id\n9\n8\n",
                "select DISTINCT id FROM limtest order by id desc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select 10-x as id, rnd_double() as reading  from long_sequence(9) order by 2) ",
                null, true, true, true);
    }

    @Test
    public void testDistinctWithLimitOnLongColumnInSubquery() throws Exception {
        assertQuery("id\n3\n2\n",
                "select DISTINCT id FROM ( select * from limtest order by id asc LIMIT 3) order by id desc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select 10-x as id, rnd_double() as reading  from long_sequence(9) order by 2) ",
                null, true, true, true);
    }

    @Test
    public void testDistinctWithLimitOnStringColumn() throws Exception {
        assertQuery("id\n9\n8\n",
                "select DISTINCT id FROM limtest order by id desc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select cast(x as string) as id, rnd_double() as reading  from long_sequence(9) order by 2)",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnWithLimitWithoutOrderBy() throws Exception {
        assertQuery("id\n1\n2\n",
                "select DISTINCT id FROM limtest order by id asc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select cast(x as symbol) as id, rnd_double() as reading  from long_sequence(9) order by 2)",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnWithLimitOrderByDescInSubquery() throws Exception {
        assertQuery("id\n8\n9\n",
                "select DISTINCT id FROM ( select id from limtest order by id desc LIMIT 2) order by id asc",
                "CREATE TABLE limtest as (" +
                        "select cast(x as symbol) as id, cast(x as double) as reading  from long_sequence(9))",
                null);
    }

    @Test
    public void testDistinctOnNonIndexedRepeatingSymbolColumnWithLimitOrderByDescInSubqueryV2() throws Exception {
        assertQuery("id\n1\n2\n",
                "select DISTINCT id FROM ( select id from limtest order by id desc LIMIT 4) order by id asc",
                "CREATE TABLE limtest as (" +
                        "select cast(x%3 as symbol) as id, cast(x as double) as reading  from long_sequence(9))",
                null);
    }

    @Test
    public void testDistinctOnNonIndexedRepeatingSymbolColumnWithLimitOrderByAscInSubqueryV2() throws Exception {
        assertQuery("id\n1\n0\n",
                "select DISTINCT id FROM ( select id from limtest order by id asc LIMIT 4) order by id desc",
                "CREATE TABLE limtest as (" +
                        "select cast(x%3 as symbol) as id, cast(x as double) as reading  from long_sequence(9))",
                null);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnWithLimitOrderByDesc() throws Exception {
        assertQuery("id\n9\n8\n",
                "select DISTINCT id FROM limtest order by id desc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select cast(x as symbol) as id, cast(x as double) as reading  from long_sequence(9))",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnWithLimitOrderByDescV2() throws Exception {
        assertQuery("id\n9\n8\n",
                "select DISTINCT id FROM (select cast(x as symbol) as id, cast(x as double) as reading  from long_sequence(9)) order by id desc LIMIT 2",
                null, true, true);
    }

    @Test
    public void testDistinctOnTwoNonIndexedColumnsWithLimitOrderByDesc() throws Exception {
        assertQuery("id\treading\n" +
                        "9\t9.0\n" +
                        "8\t8.0\n",
                "select DISTINCT id, reading FROM limtest order by id desc, reading desc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select x as id, cast(x as double) as reading  from long_sequence(9))",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnTwoNonIndexedColumnsWithLimitOrderByAsc() throws Exception {
        assertQuery("id\treading\n" +
                        "1\t1.0\n" +
                        "2\t2.0\n",
                "select DISTINCT id, reading FROM limtest order by id asc, reading asc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select x as id, cast(x as double) as reading  from long_sequence(9))",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnTwoNonIndexedColumnsWithLimitOrderByAscAndDesc() throws Exception {
        assertQuery("id\treading\n" +
                        "0\t1.0\n" +
                        "1\t1.0\n",
                "select DISTINCT id, reading FROM limtest order by id asc, reading desc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select x%5 as id, cast(x%2 as double) as reading  from long_sequence(9))",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnTwoNonIndexedColumnsWithLimitOrderByDescAndAsc() throws Exception {
        assertQuery("id\treading\n" +
                        "4\t0.0\n" +
                        "4\t1.0\n",
                "select DISTINCT id, reading FROM limtest order by id desc, reading asc LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select x%5 as id, cast(x%2 as double) as reading  from long_sequence(9))",
                null, true, true, true);
    }


    @Test
    public void testDistinctOnThreeNonIndexedColumnsWithLimitOrderByChangingInOuterQuery() throws Exception {
        assertQuery("id\treading\tst\n" +
                        "4\t0.0\ts1\n" +
                        "3\t0.0\ts1\n" +
                        "2\t1.0\ts1\n",
                "select DISTINCT id, reading, st from " +
                        "( select *  from test order by st desc, reading asc, id desc  LIMIT 5) " + //1,0,3 1,0,4 1,1,2 1,1,1 1,2,0   
                        "order by id desc, reading asc LIMIT 3",
                "CREATE TABLE test as (" +
                        "select x%5 as id, cast(x%3 as double) as reading, 's' || x%2 as st  " +
                        "from long_sequence(9) order by 2)",
                null, true, true, true);
    }

    @Test
    public void testMultilevelDistinct() throws Exception {
        assertQuery("id\treading\n" +
                        "3\t1.0\n" +
                        "2\t0.0\n",
                "select DISTINCT id, reading from " +
                        "( select distinct id, reading  from test order by reading asc, id desc  LIMIT 3) " + //  2,0 0,0 3,1 1,1
                        "order by id desc, reading asc LIMIT 2",
                "CREATE TABLE test as (" +  // 1,1 2,0 3,1 0,0 1,1 2,0 3,1 0,0 1,1
                        "select cast( x%4 as int) as id, cast(x%2 as double) as reading, rnd_double() as rnd " +
                        "from long_sequence(9) order by 3)",
                null, true, true, true);
    }

    @Test
    public void testMultilevelOrderBy() throws Exception {
        assertQuery("id\treading\n" +
                        "0\t0.0\n" +
                        "0\t0.0\n",
                "select id, reading from " + // 0,0 0,0 2,0 2,0
                        "( select id, reading  from test order by reading asc, id desc  LIMIT 4) " + // 2,0 2,0 0,0 0,0 3,1 3,1 1,1 1,1 1,1
                        "order by id asc, reading asc LIMIT 2",
                "CREATE TABLE test as (" +  // 1,1 2,0 3,1 0,0 1,1 2,0 3,1 0,0 1,1
                        "select cast( x%4 as int) as id, cast(x%2 as double) as reading, rnd_double() as rnd " +
                        "from long_sequence(9) order by 3)",
                null, true, true, true);
    }

    @Test
    public void testMultilevelOrderByWithInnerQueryUsingTableOrder() throws Exception {
        assertQuery("id\n" +
                        "5\n" +
                        "4\n",
                "select id from " +
                        "( select id from test LIMIT 5) " +
                        "order by id desc LIMIT 2",
                "CREATE TABLE test as ( select x as id from long_sequence(9) )",
                null, true, true, true);
    }

    @Test
    public void testMultilevelOrderByWithOuterQueryUsingCurrentOrder() throws Exception {
        assertQuery("id\n" +
                        "9\n" +
                        "8\n",
                "select id from " +
                        "( select id from test order by id desc LIMIT 5) " +
                        " LIMIT 2",
                "CREATE TABLE test as ( select x as id from long_sequence(9) )",
                null, true, true, true);
    }

    @Test
    public void testMultilevelOrderByWithLimit() throws Exception {
        assertQuery("id\n" +
                        "3\n4\n5\n6\n7\n",
                "select id from " +
                        "( select id from  " +
                        "  ( select id from " +
                        "   ( select id from test order by id desc LIMIT 8) " +
                        "    order by id asc LIMIT 7) " +
                        "   order by id desc LIMIT 6) " +
                        "order by id asc limit 5",
                "CREATE TABLE test as ( select x as id from long_sequence(9) )",
                null, true, true, true);
    }

    @Test
    public void testMultilevelLimit() throws Exception {
        assertQuery("id\n" +
                        "1\n2\n3\n4\n5\n",
                "select id from " +
                        "( select id from  " +
                        "  ( select id from " +
                        "   ( select id from test LIMIT 8) " +
                        "    LIMIT 7) " +
                        "   LIMIT 6) " +
                        "LIMIT 5",
                "CREATE TABLE test as ( select x as id from long_sequence(9) )",
                null, true, true, true);
    }

    @Test
    public void testMultiLimitAndOrderOnDifferentDepth() throws Exception {
        assertQuery("id\n" +
                        "9\n8\n7\n6\n5\n",
                "select id from " +
                        "( select id from  " +
                        "  ( select id from " +
                        "   ( select id from test order by id desc) " +
                        "    ) " +
                        "   ) " +
                        "LIMIT 5",
                "CREATE TABLE test as ( select x as id from long_sequence(9) )",
                null, true, true, true);
    }

    @Test
    public void testMultiOrderAndLimitOnDifferentDepth() throws Exception {
        assertQuery("id\n" +
                        "5\n4\n3\n2\n1\n",
                "select id from " +
                        "( select id from  " +
                        "  ( select id from " +
                        "   ( select id from test limit 5) " +
                        "    ) " +
                        "   ) " +
                        "order by id desc",
                "CREATE TABLE test as ( select x as id from long_sequence(9) )",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnInSubqueryWithOrderByDescInOuterQuery() throws Exception {
        assertQuery("id\n5\n4\n3\n2\n1\n0\n",
                "SELECT * from ( select DISTINCT id FROM limtest ) ORDER BY id desc",
                "CREATE TABLE limtest as (select cast((x%6) as symbol) as id from long_sequence(20))",
                null, true, true, true);
    }

    @Test//values in symbol key file aren't sorted and usually reflect first insert order 
    public void testDistinctOnNonIndexedSymbolColumnInSubqueryWithLimitInOuterQuery() throws Exception {
        assertQuery("id\n1\n2\n3\n4\n5\n",
                "SELECT * from ( select DISTINCT id FROM limtest ) limit 5",
                "CREATE TABLE limtest as (select cast((x%6) as symbol) as id from long_sequence(20))",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnInSubqueryWithOrderByDescLimitInOuterQuery() throws Exception {
        assertQuery("id\n9\n8\n",
                "SELECT * from ( select DISTINCT id FROM limtest ) ORDER BY id desc LIMIT 2 ",
                "CREATE TABLE limtest as (select cast((x%10) as symbol) as id from long_sequence(20))",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnInSubqueryOrderByAscWithLimitInOuterQuery() throws Exception {
        assertQuery("id\n1\n2\n",
                "SELECT * from ( select DISTINCT id FROM limtest ORDER BY id asc ) LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select cast(x as symbol) as id, cast(x as double) as reading  from long_sequence(9))",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnIndexedSymbolColumnInSubqueryOrderByAscWithLimitInOuterQuery() throws Exception {
        assertQuery("id\n1\n2\n",
                "SELECT * from ( select DISTINCT id FROM limtest ORDER BY id asc ) LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select cast(x as symbol) as id, cast(x as double) as reading  from long_sequence(9)), index(id)",
                null, true, true, true);
    }

    @Test
    public void testDistinctOnNonIndexedSymbolColumnInSubqueryOrderByDescWithLimitInOuterQuery() throws Exception {
        assertQuery("id\n9\n8\n",
                "SELECT * from ( select DISTINCT id FROM limtest ORDER BY id desc ) LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select cast(x as symbol) as id, cast(x as double) as reading  from long_sequence(9))",
                null,
                true, true, true);
    }

    @Test
    public void testDistinctOnIndexedSymbolColumnInSubqueryOrderByDescWithLimitInOuterQuery() throws Exception {
        assertQuery("id\n9\n8\n",
                "SELECT * from ( select DISTINCT id FROM limtest ORDER BY id desc ) LIMIT 2",
                "CREATE TABLE limtest as (" +
                        "select cast(x as symbol) as id, cast(x as double) as reading  from long_sequence(9)), index(id)",
                null,
                true, true, true);
    }

    @Test
    public void testDistinctOnIndexedSymbolColumnWithLimitInInnerQuery() throws Exception {
        assertQuery("id\n1\n2\n",
                "SELECT DISTINCT id from ( select id FROM test LIMIT 2 ) ",
                "CREATE TABLE test as (" +
                        "select cast(x as symbol) as id, rnd_double() as reading  from long_sequence(9)), index(id)",
                null,
                true, true, false);
    }

    @Test
    public void testDistinctOnIndexedSymbolColumnWithOrderByLimitInInnerQuery() throws Exception {
        assertQuery("id\n9\n8\n",
                "SELECT DISTINCT id from ( select id FROM test ORDER BY id desc LIMIT 2 ) ",
                "CREATE TABLE test as (" +
                        "select cast(x as symbol) as id, rnd_double() as reading  from long_sequence(9) order by 2), index(id)",
                null,
                true, true, false);
    }

    @Test
    public void testDistinctOnIndexedSymbolColumnWithWhereOrderByLimitInInnerQuery() throws Exception {
        assertQuery("id\n9\n8\n",
                "SELECT DISTINCT id from test where rnd_double() >= 0.0 ORDER BY id desc LIMIT 2 ",
                "CREATE TABLE test as (" +
                        "select cast(x as symbol) as id, rnd_double() as reading  from long_sequence(9) order by 2), index(id)",
                null,
                true, true, true);
    }

}
