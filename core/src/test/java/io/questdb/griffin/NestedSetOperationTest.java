package io.questdb.griffin;

import org.junit.Test;

/**
 * Tests set operations that occur in subquery.
 */
public class NestedSetOperationTest extends AbstractGriffinTest {

    @Test
    public void testSumOverUnionAll() throws Exception {
        assertQuery("sm\n" +
                        "6\n",
                "select * from ( select sum(x) as sm from (select * from test union all select * from test ) ) where sm = 6",
                "create table test as (" +
                        "select x from long_sequence(2)" +
                        ")", null, false, true);

    }

    @Test
    public void testColumnPushdownWithDistinctAndUnionAll() throws Exception {
        assertQuery("c\n" +
                        "0\n" +
                        "0\n" +
                        "0\n" +
                        "0\n",
                "select c from " +
                        "(select distinct a c, b from test " +   //0,1 ; 0,2; 
                        "union all " +
                        "select distinct c, d b from test)",    //0,1 ; 0,2; 
                "create table test as (" +
                        "select 0 as a, x as b, 0 as c, x as d from long_sequence(2)" +
                        ")", null, false, true);
    }

    @Test
    public void testGroupByPushdownWithUnionQuery() throws Exception {
        assertQuery("id\tminv\tmaxv\n" +
                        "1\t2\t3\n",
                "select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "union " +
                        "select 1 as id, 3 as val, cast(1 as timestamp) ts ) " +
                        "group by id ", null, null, true, false, true);
    }

    @Test
    public void testGroupByPushdownWithExceptQueryReturnsNoRows() throws Exception {
        assertQuery("id\tminv\tmaxv\n",
                "select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts ) " +
                        "group by id ", null, null, true, false, false);
    }

    @Test
    public void testGroupByPushdownWithIntersectQueryReturnsNoRows() throws Exception {
        assertQuery("id\tminv\tmaxv\n",
                "select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 3 as val, cast(1 as timestamp) ts ) " +
                        "group by id ", null, null, true, false, false);
    }

    @Test
    public void testGroupByPushdownWithIntersectQueryReturnsCommonRow() throws Exception {
        assertQuery("id\tminv\tmaxv\n" +
                        "1\t2\t2\n",
                "select id, min(val) as minv, max(val) as maxv  from ( " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 2 as val, cast(1 as timestamp) ts ) " +
                        "group by id ", null, null, true, false, true);
    }

    //latest by pushdown test - start
    @Test
    public void testLatestByPushdownWithUnionQueryOnTableReturnsLatestRow() throws Exception {
        assertQuery("ts\tstatus\tamount\n" +
                        "1970-01-01T00:00:00.000002Z\topen\t101\n",
                "select * from ( " +
                        "select *  from test where amount = 101 " +
                        "union " +
                        "select * from test where amount = 100 ) latest on ts partition by status ",
                "create table test as ( " +
                        "select cast(1 as timestamp) as ts, 'open' as status, 100 as amount from long_sequence(1) " +
                        "union all " +
                        "select cast(2 as timestamp) as ts, 'open' as status, 101 as amount from long_sequence(1) " +
                        ") timestamp(ts)", null, false, false, true);
    }

    //latest by pushdown test - end  

    //order by pushdown test - start
    //order by can be triggered by using column other than one in select clause in order by clause - but not on top level !
    @Test
    public void testOrderByPushdownWithUnionAllQueryReturnsAllRows() throws Exception {
        assertQuery("rec_type\n" +
                        "t2\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) )" +
                        "order by id desc )", null, null, true, false, true);
    }

    @Test
    public void testOrderByPushdownWithUnionQueryReturnsOneUniqueRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t3\n",
                "select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't3' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 1 as id, 't3' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )", null, null, true, false, false);
    }

    @Test
    public void testOrderByPushdownWithUnionQueryReturnsAllUniqueRows() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n" +
                        "t2\n",
                "select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select '1 ' as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select '1 ' as id, 't2' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )", null, null, true, false, false);
    }

    @Test
    public void testOrderByPushdownWithIntersectQueryReturnsNoCommonRow() throws Exception {
        assertQuery("rec_type\n",
                "select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 't2' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )", null, null, true, false, false);
    }

    @Test
    public void testOrderByPushdownWithIntersectQueryReturnsCommonRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )", null, null, true, false, false);
    }

    @Test
    public void testOrderByPushdownWithExceptQueryReturnsFirstRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 't2' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )", null, null, true, false, false);
    }

    @Test
    public void testOrderByPushdownWithExceptQueryReturnsNoRows() throws Exception {
        assertQuery("rec_type\n",
                "select rec_type from ( " +
                        "select rec_type from (" +
                        "select * from (" +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) )" +
                        "order by id desc )", null, null, true, false, false);
    }

    //order by pushdown test - end

    //where clause pushdown tests start
    @Test
    public void testWhereClausePushdownWithExceptQueryReturnsFirstRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 2 as id, 't1' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id!=0 ", null, null, true, false);
    }

    @Test
    public void testWhereClausePushdownWithExceptEmptySetQueryReturnsFirstRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 2 as id, 't1' as rec_type, cast(2 as timestamp) ts from long_sequence(1) where x < 0 ) " +
                        "where id!=0 ", null, null, true, false);
    }

    @Test
    public void testWhereClausePushdownWithExceptQueryReturnsNoRows() throws Exception {
        assertQuery("rec_type\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) " +
                        "where id<10 ", null, null, true, false);
    }

    @Test
    public void testWhereClausePushdownWithIntersectQueryReturnsZeroRows() throws Exception {
        assertQuery("rec_type\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 2 as id, 't1' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id!=0 ", null, null, true, false);
    }

    @Test
    public void testWhereClausePushdownWithIntersectQueryReturnsCommonRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts ) " +
                        "where id<10 ", null, null, true, false);
    }

    @Test
    public void testWhereClausePushdownWithUnionAllQueryReturnsOneMatchingRows() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id=1 ", null, null, false, false);
    }

    @Test
    public void testWhereClausePushdownWithUnionAllQueryReturnsAllMatchingRows() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n" +
                        "t2\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id<10 ", null, null, false, false);
    }

    @Test
    public void testWhereClausePushdownWithUnionQueryReturnsOneRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id=1 ", null, null, false, false);
    }

    @Test
    public void testWhereClausePushdownWithUnionQueryReturnsAllRows() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n" +
                        "t2\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts ) " +
                        "where id>0 ", null, null, false, false);
    }

    @Test
    public void testWhereClausePushdownWith2UnionQueryReturnsOnlyMatchingRow() throws Exception {
        assertQuery("rec_type\n" +
                        "t1\n",
                "select rec_type from ( " +
                        "select 1 as id, 't1' as rec_type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 't2' as rec_type, cast(2 as timestamp) ts " +
                        "union " +
                        "select 3 as id, 't3' as rec_type, cast(2 as timestamp) ts " +
                        ") " +
                        "where id=1 ", null, null, false, false);
    }


    //where clause pushdown tests end

    //timestamp pushdown tests start
    //to test timestamp pushdown we've to specify it using timestamp clause - timestamp(ts) in a way that could clash 
    //with column of different type (e.g. string or symbol) in set component other than first
    @Test
    public void testTimestampPushdownWith2UnionQueryReturnsAllDistinctRows() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n" +
                        "st\t1970-01-01T00:00:00.000002Z\n" +
                        "st\t1970-01-01T00:00:00.000003Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts " +
                        "union " +
                        "select 3 as id, 'st' as type, cast(3 as timestamp) ts ) timestamp(ts)  ",
                null, "ts", false, false);
    }

    @Test
    public void testTimestampPushdownWithUnionQueryReturnsAllDistinctRows() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n" +
                        "st\t1970-01-01T00:00:00.000002Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ", null, "ts", false, false);
    }

    @Test
    public void testTimestampPushdownWithUnionQueryReturnsReturnsOnlyDistinctRow() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n",
                "select type,ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ", null, "ts", false, false);
    }

    @Test
    public void testTimestampPushdownWithUnionAllQueryReturnsAllRows0() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n" +
                        "st\t1970-01-01T00:00:00.000002Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ", null, "ts", false, false, true);
    }

    @Test
    public void testTimestampPushdownWith2UnionAllQueryReturnsAllRows0() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n" +
                        "st\t1970-01-01T00:00:00.000002Z\n" +
                        "st\t1970-01-01T00:00:00.000003Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts " +
                        "union all " +
                        "select 3 as id, 'st' as type, cast(3 as timestamp) ts ) timestamp(ts)  ",
                null, "ts", false, false, true);
    }

    @Test
    public void testTimestampPushdownWithUnionAllQueryReturnsAllRows1() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n" +
                        "st\t1970-01-01T00:00:00.000002Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ", null, "ts", false, false, true);
    }

    //same as above but with duplicate rows
    @Test
    public void testTimestampPushdownWithUnionAllQueryReturnsAllRows2() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "union all " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ", null, "ts", false, false, true);
    }

    @Test
    public void testTimestampPushdownWithIntersectQueryReturnsZeroRecords() throws Exception {
        assertQuery("type\tts\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ", null, "ts", true, false);
    }

    @Test
    public void testTimestampPushdownWithIntersectQueryReturnsOneCommonRecords() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "intersect " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ", null, "ts", true, false);
    }

    @Test
    public void testTimestampPushdownWithExceptQueryReturnsOneRecord() throws Exception {
        assertQuery("type\tts\n" +
                        "st\t1970-01-01T00:00:00.000001Z\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 2 as id, 'st' as type, cast(2 as timestamp) ts ) timestamp(ts)  ", null, "ts", true, false);
    }

    @Test
    public void testTimestampPushdownWithExceptQueryReturnsZeroRecords() throws Exception {
        assertQuery("type\tts\n",
                "select type, ts from ( " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts " +
                        "except " +
                        "select 1 as id, 'st' as type, cast(1 as timestamp) ts ) timestamp(ts)  ", null, "ts", true, false);
    }
    //timestamp pushdown tests end 

    //select columns pushdown with real tables 
    @Test
    public void testColumnsPushdownWithUnionAllQueryOnTableReturnsAllRowsOnTable() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "abc\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "union all " +
                        "select * from test where id = 2 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, false, false);
    }

    @Test
    public void testColumnsPushdownWith2UnionAllQueryOnTableReturnsAllRowsOnTable() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "abc\n" +
                        "abc\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "union all " +
                        "select * from test where id = 2 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, false, false);
    }

    @Test
    public void testColumnsPushdownWithUnionQueryOnTableReturnsAllRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "abc\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "union " +
                        "select * from test where id = 2 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, false, false);
    }

    @Test
    public void testColumnsPushdownWithUnionQueryOnTableReturnsOnlyDistinctRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "union " +
                        "select * from test where id = 1 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, false, false);
    }

    @Test
    public void testColumnsPushdownWith2UnionQueryOnTableReturnsOnlyDistinctRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "union " +
                        "select * from test where id = 1 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, false, false);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryOnTableReturnsAllRowsFromFirstTable() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "except " +
                        "select * from test where id = 2 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, true, false);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryOnTableReturnsNoRows() throws Exception {
        assertQuery("status\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "except " +
                        "select * from test where id = 1 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, true, false);
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryOnTableReturnsCommonRow() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "intersect  " +
                        "select * from test where id = 1 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, true, false);
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryOnTableReturnsNoRows() throws Exception {
        assertQuery("status\n",
                "select status from ( " +
                        "select * from test where id = 1 " +
                        "intersect " +
                        "select * from test where id = 2 ) ",
                "create table test as ( " +
                        "select 1 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'abc' status from long_sequence(1) " +
                        ")", null, true, false);
    }

    //select columns pushdown with virtual selects - start
    @Test
    public void testColumnsPushdownWithUnionQueryReturnsAllRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "def\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'def' status ) ", null, null, false, false);
    }

    @Test
    public void testColumnsPushdownWith2UnionQueryReturnsAllRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "def\n" +
                        "ghi\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'def' status " +
                        "union " +
                        "select 3 as id, 100 as amount, 'ghi' status ) ", null, null, false, false);
    }

    @Test
    public void testColumnsPushdownWithUnionQueryReturnsAllRows2() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'abc' status ) ", null, null, false, false);
    }

    @Test
    public void testColumnsPushdownWithUnionQueryReturnsOnlyDistinctRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union " +
                        "select 1 as id, 100 as amount, 'abc' status ) ", null, null, false, false);
    }

    @Test
    public void testColumnsPushdownWithUnionAllQueryReturnsAllRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "def\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 2 as id, 100 as amount, 'def' status ) ", null, null, false, false, true);
    }

    @Test
    public void testColumnsPushdownWithUnionAllQueryReturnsAllRepeatingRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 100 as amount, 'abc' status ) ", null, null, false, false, true);
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryReturnsZeroRows() throws Exception {
        assertQuery("status\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 2 as id, 100 as amount, 'def' status ) ", null, null, true, false, false);
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryReturnsZeroRows2() throws Exception {
        assertQuery("status\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 2 as id, 101 as amount, 'abc' status ) ", null, null, true, false, false);
    }

    @Test
    public void testColumnsPushdownWithIntersectQueryReturnsSharedRows() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 1 as id, 100 as amount, 'abc' status ) ", null, null, true, false, false);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryReturnsZeroRows() throws Exception {
        assertQuery("status\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status ) ", null, null, true, false, false);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryReturnsDistinctRow() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'def' status ) ", null, null, true, false, false);
    }

    @Test
    public void testColumnsPushdownWithExceptQueryReturnsDistinctRow2() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 101 as amount, 'abc' status ) ", null, null, true, false, false);
    }

    //test with combinations of set operations, e.g. union with union all 
    @Test
    public void testColumnsPushdownWithUnionAllAndUnionOpsQueryReturnsDistinctRow2() throws Exception {
        assertQuery("status\n" +
                        "abc\n" +
                        "abc\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "union " +
                        "select 2 as id, 100 as amount, 'abc' status ) ", null, null, false, false, false);
    }

    @Test
    public void testColumnsPushdownWithUnionAllAndExceptOpsQueryReturnsUniqueRow() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status ) ", null, null, false, false, false);
    }

    @Test
    public void testColumnsPushdownWithAllSetOpsQueryReturnsOneRow() throws Exception {
        assertQuery("status\n" +
                        "abc\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "union " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 1 as id, 101 as amount, 'abc' status  ) ", null, null, false, false, false);
    }


    @Test
    public void testColumnsPushdownWithAllSetOpsQueryReturnsNoRows() throws Exception {
        assertQuery("status\n",
                "select status from ( " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "union all " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "union " +
                        "select 1 as id, 101 as amount, 'abc' status " +
                        "except " +
                        "select 1 as id, 100 as amount, 'abc' status " +
                        "intersect " +
                        "select 2 as id, 101 as amount, 'abc' status  ) ", null, null, false, false, false);
    }

    //select columns pushdown with virtual selects - end 

}
