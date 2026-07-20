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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

/**
 * A scalar boolean sub-query used directly as a predicate in WHERE evaluates once per query
 * execution, in every shape: bare, AND conjunct, NOT, OR, joins, LATEST ON, UPDATE. An empty
 * sub-query yields NULL, which QuestDB's two-valued BOOLEAN renders as false, so the predicate
 * matches no rows. Non-boolean and multi-column sub-queries are rejected at compile time;
 * multi-row sub-queries are rejected at execution time.
 * <p>
 * Historically these conjuncts carried a null token and were either silently dropped from the
 * rebuilt WHERE clause (wrong results: the filter vanished from the plan entirely, and UPDATE
 * modified rows it should not have) or crashed intrinsic extraction and join analysis with an
 * NPE, while the OR shape failed with a type error. These tests pin the evaluated semantics for
 * all shapes.
 */
public class BooleanSubQueryPredicateTest extends AbstractCairoTest {

    private static final String NO_ROWS = "ts\tv\tsym\n";
    private static final String THE_ROW = "ts\tv\tsym\n2018-01-01T00:00:00.000000Z\t1\ta\n";

    @Test
    public void testAndConjunctSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to be silently dropped: returned the row despite the false predicate
            assertPredicate(NO_ROWS, "select * from t where ts = '2018-01-01' and (select b from x_false limit 1)");
            assertPredicate(THE_ROW, "select * from t where ts = '2018-01-01' and (select b from x_true limit 1)");
            assertPredicate(NO_ROWS, "select * from t where (select b from x_false limit 1) and ts = '2018-01-01'");
            assertPredicate(NO_ROWS, "select * from t where ts = '2018-01-01' and (select b from x_empty limit 1)");
            // nested AND group; the timestamp intrinsic is extracted, the sub-query filters
            assertPredicate(NO_ROWS, "select * from t where v = 1 and (ts in '2018' and (select b from x_false limit 1))");
            assertPredicate(THE_ROW, "select * from t where v = 1 and (ts in '2018' and (select b from x_true limit 1))");
            // both conjuncts are sub-queries
            assertPredicate(NO_ROWS, "select * from t where (select b from x_false limit 1) and (select b from x_true limit 1)");
            assertPredicate(THE_ROW, "select * from t where (select b from x_true limit 1) and (select b from x_true limit 1)");
        });
    }

    @Test
    public void testBareSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to be silently dropped: no filter node in the plan at all
            assertPredicate(NO_ROWS, "select * from t where (select b from x_false limit 1)");
            assertPredicate(THE_ROW, "select * from t where (select b from x_true limit 1)");
            // empty sub-query yields NULL, which two-valued BOOLEAN renders as false
            assertPredicate(NO_ROWS, "select * from t where (select b from x_empty limit 1)");
            assertPredicate(NO_ROWS, "select * from t where (select null::boolean from x_true limit 1)");
            // extra parentheses
            assertPredicate(THE_ROW, "select * from t where ((select b from x_true limit 1))");
        });
    }

    @Test
    public void testBooleanComparisonWithSubQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // boolean expression compared with a scalar boolean sub-query
            assertPredicate(THE_ROW, "select * from t where (v = 1) = (select b from x_true limit 1)");
            assertPredicate(NO_ROWS, "select * from t where (v = 1) = (select b from x_false limit 1)");
        });
    }

    @Test
    public void testExplainShowsSubQueryFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            printSql("explain select * from t where (select b from x_false limit 1)");
            TestUtils.assertContains(sink, "filter: cursor");
            TestUtils.assertContains(sink, "Frame forward scan on: x_false");
        });
    }

    @Test
    public void testJoinWhereSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to throw NPE in join condition analysis
            assertCount("0", "select count() from t t1 join t t2 on t1.v = t2.v where (select b from x_false limit 1)");
            assertCount("1", "select count() from t t1 join t t2 on t1.v = t2.v where (select b from x_true limit 1)");
            assertCount("0", "select count() from t t1 left join t t2 on t1.v = t2.v where (select b from x_false limit 1)");
            assertCount("0", "select count() from t t1 cross join t t2 where (select b from x_false limit 1)");
            assertCount("1", "select count() from t t1 cross join t t2 where (select b from x_true limit 1)");
            assertCount("0", "select count() from t t1 asof join t t2 where (select b from x_false limit 1)");
        });
    }

    @Test
    public void testLatestOnWhereSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertPredicate(NO_ROWS, "select * from t where (select b from x_false limit 1) latest on ts partition by sym");
            assertPredicate(THE_ROW, "select * from t where sym in ('a') and (select b from x_true limit 1) latest on ts partition by sym");
        });
    }

    @Test
    public void testLatestOnWithinWhereSubQueryPredicate() throws Exception {
        // exercises the extractWithin path, which runs ahead of intrinsic extraction
        setProperty(PropertyKey.QUERY_WITHIN_LATEST_BY_OPTIMISATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("create table gt (ts timestamp, g geohash(8c), sym symbol index) timestamp(ts) partition by day");
            execute("insert into gt values ('2018-01-01', #sp052w92, 'a')");
            execute("create table x_false (b boolean)");
            execute("insert into x_false values (false)");
            execute("create table x_true (b boolean)");
            execute("insert into x_true values (true)");
            final String expected = "ts\tg\tsym\n2018-01-01T00:00:00.000000Z\tsp052w92\ta\n";
            printSql("select * from gt where g within(#sp05) and (select b from x_true limit 1) latest on ts partition by sym");
            TestUtils.assertEquals(expected, sink);
            printSql("select * from gt where g within(#sp05) and (select b from x_false limit 1) latest on ts partition by sym");
            TestUtils.assertEquals("ts\tg\tsym\n", sink);
        });
    }

    @Test
    public void testNestedModelSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertPredicate(NO_ROWS, "select * from (select * from t where (select b from x_false limit 1))");
            assertPredicate(THE_ROW, "with q as (select * from t where (select b from x_true limit 1)) select * from q");
            assertCount("0", "select count() from t where (select b from x_false limit 1)");
            printSql("select sum(v) from t where (select b from x_false limit 1)");
            TestUtils.assertEquals("sum\nnull\n", sink);
        });
    }

    @Test
    public void testNotSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the negation must not be silently discarded
            assertPredicate(NO_ROWS, "select * from t where not (select b from x_true limit 1)");
            assertPredicate(THE_ROW, "select * from t where not (select b from x_false limit 1)");
            // an empty sub-query yields NULL, which two-valued BOOLEAN renders as false,
            // so its negation is true - consistent with "not null::boolean"
            assertPredicate(THE_ROW, "select * from t where not null::boolean");
            assertPredicate(THE_ROW, "select * from t where not (select b from x_empty limit 1)");
            assertPredicate(THE_ROW, "select * from t where not not (select b from x_true limit 1)");
        });
    }

    @Test
    public void testOrSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the OR shape used to fail with "expression type mismatch"; it now evaluates
            // like every other shape
            assertPredicate(THE_ROW, "select * from t where v = 5 or (select b from x_true limit 1)");
            assertPredicate(NO_ROWS, "select * from t where v = 5 or (select b from x_false limit 1)");
            assertPredicate(THE_ROW, "select * from t where v = 1 or (select b from x_false limit 1)");
        });
    }

    @Test
    public void testRejectedSubQueryShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("create table x_multi (b boolean)");
            execute("insert into x_multi values (true), (false)");
            // non-boolean scalar sub-query
            assertExceptionNoLeakCheck("select * from t where (select 42 from x_true limit 1)", 23, "boolean expression expected");
            // multi-column sub-query
            assertExceptionNoLeakCheck("select * from t where (select b, b from x_true limit 1)", 23, "boolean expression expected");
            // multi-row sub-query is rejected at execution time
            assertExceptionNoLeakCheck("select * from t where (select b from x_multi)", 23, "scalar sub-query returned more than one row");
        });
    }

    @Test
    public void testScalarSubQueryComparisonsStillWork() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertPredicate(THE_ROW, "select * from t where ts = (select max(ts) from t)");
            assertPredicate(THE_ROW, "select * from t where v = (select 1)");
            assertPredicate(THE_ROW, "select * from t where v > (select 0)");
            assertPredicate(THE_ROW, "select * from t where not (v = 5)");
        });
    }

    @Test
    public void testUpdateWhereSubQueryPredicate() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to be silently dropped: the UPDATE modified every row
            update("update t set v = 42 where (select b from x_false limit 1)");
            assertCount("0", "select count() from t where v = 42");
            update("update t set v = 42 where ts = '2018-01-01' and (select b from x_false limit 1)");
            assertCount("0", "select count() from t where v = 42");
            update("update t set v = 42 where (select b from x_empty limit 1)");
            assertCount("0", "select count() from t where v = 42");
            // a true predicate updates the matching rows
            update("update t set v = 42 where (select b from x_true limit 1)");
            assertCount("1", "select count() from t where v = 42");
        });
    }

    private void assertCount(String expectedCount, String sql) throws Exception {
        printSql(sql);
        TestUtils.assertEquals("count\n" + expectedCount + "\n", sink);
    }

    private void assertPredicate(String expected, String sql) throws Exception {
        printSql(sql);
        TestUtils.assertEquals(expected, sink);
    }

    private void createTables() throws Exception {
        execute("create table t (ts timestamp, v int, sym symbol index) timestamp(ts) partition by day");
        execute("insert into t values ('2018-01-01T00:00:00.000000Z', 1, 'a')");
        execute("create table x_false (b boolean)");
        execute("insert into x_false values (false)");
        execute("create table x_true (b boolean)");
        execute("insert into x_true values (true)");
        execute("create table x_empty (b boolean)");
    }
}
