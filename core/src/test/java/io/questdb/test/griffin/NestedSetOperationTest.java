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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Tests set operations that occur in subquery.
 */
public class NestedSetOperationTest extends AbstractCairoTest {

    @Test
    public void testColumnPushdownWithDistinctAndUnionAll() throws Exception {
        assertQuery("select c from " +
                        "(select distinct a c, b from test " +   //0,1 ; 0,2;
                        "union all " +
                        "select distinct c, d b from test)")
                .ddl(//0,1 ; 0,2;
                "create table test as (" +
                        "select 0 as a, x as b, 0 as c, x as d from long_sequence(2)" +
                        ")")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        c
                        0
                        0
                        0
                        0
                        """);
    }

    @Test
    public void testColumnsPushdownWith2UnionAllQueryOnTableReturnsAllRowsOnTable() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "union all " +
                        "select * from test where id = 2 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        abc
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWith2UnionQueryOnTableReturnsOnlyDistinctRows() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "union " +
                        "select * from test where id = 1 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWith2UnionQueryReturnsAllRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'def' status " +
                        "union " +
                        "select 3 as id, 100 as amount, 'ghi' status ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        def
                        ghi
                        """);
    }

    @Test
    public void testColumnsPushdownWithAllSetOpsQueryReturnsNoRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "union " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 2 as id, 101 as amount, 'abc' status  ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("status\n");
    }

    @Test
    public void testColumnsPushdownWithAllSetOpsQueryReturnsOneRow() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "union " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 1 as id, 101 as amount, 'abc' status  ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryOnTableReturnsAllRowsFromFirstTable() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "except " +
                        "select * from test where id = 2 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .returns("""
                        status
                        abc
                        """);
    }

    //latest by pushdown test - end  

    @Test
    public void testColumnsPushdownWithExceptQueryOnTableReturnsNoRows() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "except " +
                        "select * from test where id = 1 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .returns("status\n");
    }

    @Test
    public void testColumnsPushdownWithExceptQueryReturnsDistinctRow() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'def' status ) ")
                .ddl(null)
                .returns("""
                        status
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryReturnsDistinctRow2() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 101 as amount, 'abc' status ) ")
                .ddl(null)
                .returns("""
                        status
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryReturnsZeroRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status ) ")
                .ddl(null)
                .returns("status\n");
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryOnTableReturnsCommonRow() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "intersect  " +
                        "select * from test where id = 1 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .returns("""
                        status
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryOnTableReturnsNoRows() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "intersect " +
                        "select * from test where id = 2 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .returns("status\n");
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryReturnsSharedRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 1 as id, 100 as amount, 'abc' status ) ")
                .ddl(null)
                .returns("""
                        status
                        abc
                        """);
    }

    //order by pushdown test - end

    @Test
    public void testColumnsPushdownWithIntersectQueryReturnsZeroRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 2 as id, 100 as amount, 'def' status ) ")
                .ddl(null)
                .returns("status\n");
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryReturnsZeroRows2() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 2 as id, 101 as amount, 'abc' status ) ")
                .ddl(null)
                .returns("status\n");
    }

    @Test
    public void testColumnsPushdownWithUnionAllAndExceptOpsQueryReturnsUniqueRow() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        """);
    }

    //test with combinations of set operations, e.g. union with union all
    @Test
    public void testColumnsPushdownWithUnionAllAndUnionOpsQueryReturnsDistinctRow2() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'abc' status ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        abc
                        abc
                        """);
    }

    //select columns pushdown with real tables
    @Test
    public void testColumnsPushdownWithUnionAllQueryOnTableReturnsAllRowsOnTable() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "union all " +
                        "select * from test where id = 2 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithUnionAllQueryReturnsAllRepeatingRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 100 as amount, 'abc' status ) ")
                .ddl(null)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        status
                        abc
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithUnionAllQueryReturnsAllRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'def' status ) ")
                .ddl(null)
                .noRandomAccess()
                .expectSize()
                .returns("""
                        status
                        abc
                        def
                        """);
    }

    @Test
    public void testColumnsPushdownWithUnionQueryOnTableReturnsAllRows() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "union " +
                        "select * from test where id = 2 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithUnionQueryOnTableReturnsOnlyDistinctRows() throws Exception {
        assertQuery("select status from ( " +
                        "select * from test where id = 1 " +
                        "union " +
                        "select * from test where id = 1 ) ")
                .ddl("create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")")
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        """);
    }

    //select columns pushdown with virtual selects - start
    @Test
    public void testColumnsPushdownWithUnionQueryReturnsAllRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'def' status ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        def
                        """);
    }


    //where clause pushdown tests end

    @Test
    public void testColumnsPushdownWithUnionQueryReturnsAllRows2() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'abc' status ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        abc
                        """);
    }

    @Test
    public void testColumnsPushdownWithUnionQueryReturnsOnlyDistinctRows() throws Exception {
        assertQuery("select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 1 as id, 100 as amount, 'abc' status ) ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        status
                        abc
                        """);
    }

    @Test
    public void testGroupByPushdownWithExceptQueryReturnsNoRows() throws Exception {
        assertQuery("select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts ) " +
                        "group by id ")
                .ddl(null)
                .returns("id\tminv\tmaxv\n");
    }

    @Test
    public void testGroupByPushdownWithIntersectQueryReturnsCommonRow() throws Exception {
        assertQuery("select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts ) " +
                        "group by id ")
                .ddl(null)
                .expectSize()
                .returns("""
                        id\tminv\tmaxv
                        1\t2\t2
                        """);
    }

    @Test
    public void testGroupByPushdownWithIntersectQueryReturnsNoRows() throws Exception {
        assertQuery("select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 3 as val, cast(1 as timestamp) ts ) " +
                        "group by id ")
                .ddl(null)
                .returns("id\tminv\tmaxv\n");
    }

    @Test
    public void testGroupByPushdownWithUnionQuery() throws Exception {
        assertQuery("select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "union " +
                        "select 1 as id, 3 as val, cast(1 as timestamp) ts ) " +
                        "group by id ")
                .ddl(null)
                .expectSize()
                .returns("""
                        id\tminv\tmaxv
                        1\t2\t3
                        """);
    }

    // latest by pushdown test - start
    @Test
    public void testLatestByPushdownWithUnionQueryOnTableReturnsLatestRow() throws Exception {
        assertQuery("select * from ( " +
                        "select *  from test where amount = 101 " +
                        "union " +
                        "select * from test where amount = 100 ) latest on ts partition by status ")
                .ddl("create table test as ( " +
                        "select cast(1 as timestamp) as ts, 'open' as status, 100 as amount from long_sequence(1) " +
                        "union all " +
                        "select cast(2 as timestamp) as ts, 'open' as status, 101 as amount from long_sequence(1) " +
                        ") timestamp(ts)")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        ts\tstatus\tamount
                        1970-01-01T00:00:00.000002Z\topen\t101
                        """);
    }

    @Test
    public void testOrderByPushdownWithExceptQueryReturnsFirstRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 't2' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )")
                .ddl(null)
                .returns("""
                        rec_type
                        t1
                        """);
    }

    @Test
    public void testOrderByPushdownWithExceptQueryReturnsNoRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )")
                .ddl(null)
                .returns("rec_type\n");
    }

    @Test
    public void testOrderByPushdownWithIntersectQueryReturnsCommonRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )")
                .ddl(null)
                .returns("""
                        rec_type
                        t1
                        """);
    }

    @Test
    public void testOrderByPushdownWithIntersectQueryReturnsNoCommonRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 't2' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )")
                .ddl(null)
                .returns("rec_type\n");
    }
    //timestamp pushdown tests end 

    //order by pushdown test - start
    //order by can be triggered by using column other than one in select clause in order by clause - but not on top level !
    @Test
    public void testOrderByPushdownWithUnionAllQueryReturnsAllRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) )" +
                        "order by id desc )")
                .ddl(null)
                .expectSize()
                .returns("""
                        rec_type
                        t2
                        t1
                        """);
    }

    @Test
    public void testOrderByPushdownWithUnionQueryReturnsAllUniqueRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select '1 ' as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select '1 ' as id, 't2' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )")
                .ddl(null)
                .returns("""
                        rec_type
                        t1
                        t2
                        """);
    }

    @Test
    public void testOrderByPushdownWithUnionQueryReturnsOneUniqueRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't3' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 1 as id, 't3' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )")
                .ddl(null)
                .returns("""
                        rec_type
                        t3
                        """);
    }

    @Test
    public void testSumOverUnionAll() throws Exception {
        assertQuery("select * from ( select sum(x) as sm from (select * from test union all select * from test ) ) where sm = 6")
                .ddl("create table test as (" +
                        "select x from long_sequence(2)" +
                        ")")
                .noRandomAccess()
                .returns("""
                        sm
                        6
                        """);

    }

    @Test
    public void testTimestampPushdownWith2UnionAllQueryReturnsAllRows0() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts " +
                        "union all " +
                        "select 3 as id, 'st' as type, cast(3 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        st\t1970-01-01T00:00:00.000002Z
                        st\t1970-01-01T00:00:00.000003Z
                        """);
    }

    //timestamp pushdown tests start
    //to test timestamp pushdown we've to specify it using timestamp clause - timestamp(ts) in a way that could clash
    //with column of different type (e.g. string or symbol) in set component other than first
    @Test
    public void testTimestampPushdownWith2UnionQueryReturnsAllDistinctRows() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts " +
                        "union " +
                        "select 3 as id, 'st' as type, cast(3 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .noRandomAccess()
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        st\t1970-01-01T00:00:00.000002Z
                        st\t1970-01-01T00:00:00.000003Z
                        """);
    }

    @Test
    public void testTimestampPushdownWithExceptQueryReturnsOneRecord() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        """);
    }

    @Test
    public void testTimestampPushdownWithExceptQueryReturnsZeroRecords() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .returns("type\tts\n");
    }

    @Test
    public void testTimestampPushdownWithIntersectQueryReturnsOneCommonRecords() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        """);
    }

    @Test
    public void testTimestampPushdownWithIntersectQueryReturnsZeroRecords() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .returns("type\tts\n");
    }

    @Test
    public void testTimestampPushdownWithUnionAllQueryReturnsAllRows0() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        st\t1970-01-01T00:00:00.000002Z
                        """);
    }

    @Test
    public void testTimestampPushdownWithUnionAllQueryReturnsAllRows1() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        st\t1970-01-01T00:00:00.000002Z
                        """);
    }

    //same as above but with duplicate rows
    @Test
    public void testTimestampPushdownWithUnionAllQueryReturnsAllRows2() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        st\t1970-01-01T00:00:00.000001Z
                        """);
    }

    @Test
    public void testTimestampPushdownWithUnionQueryReturnsAllDistinctRows() throws Exception {
        assertQuery("select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .noRandomAccess()
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        st\t1970-01-01T00:00:00.000002Z
                        """);
    }

    @Test
    public void testTimestampPushdownWithUnionQueryReturnsReturnsOnlyDistinctRow() throws Exception {
        assertQuery("select type,ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ")
                .ddl(null)
                .timestamp("ts")
                .noRandomAccess()
                .returns("""
                        type\tts
                        st\t1970-01-01T00:00:00.000001Z
                        """);
    }

    @Test
    public void testWhereClausePushdownWith2UnionQueryReturnsOnlyMatchingRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts " +
                        "union " +
                        "select 3 as id, 't3' as rec_type, cast(2 as timestamp) ts " +
                        ") " +
                        "where id=1 ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        rec_type
                        t1
                        """);
    }

    @Test
    public void testWhereClausePushdownWithExceptEmptySetQueryReturnsFirstRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 2 as id, 't1' as rec_type, cast(2 as timestamp) ts from long_sequence(1) where x < 0 ) " +
                        "where id!=0 ")
                .ddl(null)
                .returns("""
                        rec_type
                        t1
                        """);
    }

    //where clause pushdown tests start
    @Test
    public void testWhereClausePushdownWithExceptQueryReturnsFirstRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 2 as id, 't1' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id!=0 ")
                .ddl(null)
                .returns("""
                        rec_type
                        t1
                        """);
    }

    @Test
    public void testWhereClausePushdownWithExceptQueryReturnsNoRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) " +
                        "where id<10 ")
                .ddl(null)
                .returns("rec_type\n");
    }

    @Test
    public void testWhereClausePushdownWithIntersectQueryReturnsCommonRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) " +
                        "where id<10 ")
                .ddl(null)
                .returns("""
                        rec_type
                        t1
                        """);
    }

    @Test
    public void testWhereClausePushdownWithIntersectQueryReturnsZeroRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 2 as id, 't1' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id!=0 ")
                .ddl(null)
                .returns("rec_type\n");
    }

    @Test
    public void testWhereClausePushdownWithUnionAllQueryReturnsAllMatchingRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id<10 ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        rec_type
                        t1
                        t2
                        """);
    }

    @Test
    public void testWhereClausePushdownWithUnionAllQueryReturnsOneMatchingRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id=1 ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        rec_type
                        t1
                        """);
    }

    @Test
    public void testWhereClausePushdownWithUnionQueryReturnsAllRows() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id>0 ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        rec_type
                        t1
                        t2
                        """);
    }

    @Test
    public void testWhereClausePushdownWithUnionQueryReturnsOneRow() throws Exception {
        assertQuery("select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id=1 ")
                .ddl(null)
                .noRandomAccess()
                .returns("""
                        rec_type
                        t1
                        """);
    }

    //select columns pushdown with virtual selects - end 

}
