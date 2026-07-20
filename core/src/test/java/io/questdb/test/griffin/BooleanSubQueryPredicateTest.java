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
 * A scalar sub-query used directly as a boolean predicate in WHERE must be
 * rejected with a clean SQL error in every shape: bare, AND conjunct, NOT,
 * joins, LATEST ON, UPDATE.
 * <p>
 * Historically these conjuncts carried a null token and were either silently
 * dropped from the rebuilt WHERE clause (wrong results: the filter vanished
 * from the plan entirely, and UPDATE modified rows it should not have) or
 * crashed intrinsic extraction and join analysis with an NPE. The OR shape
 * always produced a clean type error; these tests pin the same behaviour for
 * all other shapes.
 */
public class BooleanSubQueryPredicateTest extends AbstractCairoTest {

    private static final String BOOL_EXPECTED = "boolean expression expected";
    private static final String TYPE_MISMATCH = "expression type mismatch, expected: BOOLEAN, actual: CURSOR";

    @Test
    public void testAndConjunctSubQueryPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to be silently dropped: returned the row despite the false predicate
            assertExceptionNoLeakCheck("select * from t where ts = '2018-01-01' and (select b from x_false limit 1)", 45, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t where (select b from x_false limit 1) and ts = '2018-01-01'", 23, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t where ts = '2018-01-01' and (select b from x_empty limit 1)", 45, BOOL_EXPECTED);
            // nested AND group
            assertExceptionNoLeakCheck("select * from t where v = 1 and (ts in '2018' and (select b from x_false limit 1))", 51, TYPE_MISMATCH);
            // both conjuncts tokenless
            assertExceptionNoLeakCheck("select * from t where (select b from x_false limit 1) and (select b from x_true limit 1)", 23, TYPE_MISMATCH);
        });
    }

    @Test
    public void testBareSubQueryPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to be silently dropped: no filter node in the plan at all
            assertExceptionNoLeakCheck("select * from t where (select b from x_false limit 1)", 23, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t where (select b from x_true limit 1)", 23, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t where (select b from x_empty limit 1)", 23, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t where (select null::boolean from x_true limit 1)", 23, BOOL_EXPECTED);
            // non-boolean scalar sub-query
            assertExceptionNoLeakCheck("select * from t where (select 42 from x_true limit 1)", 23, BOOL_EXPECTED);
            // extra parentheses
            assertExceptionNoLeakCheck("select * from t where ((select b from x_false limit 1))", 24, BOOL_EXPECTED);
            // explain compiles the same plan and must fail the same way
            assertExceptionNoLeakCheck("explain select * from t where (select b from x_false limit 1)", 31, BOOL_EXPECTED);
        });
    }

    @Test
    public void testJoinWhereSubQueryPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to throw NPE in join condition analysis
            assertExceptionNoLeakCheck("select * from t t1 join t t2 on t1.v = t2.v where (select b from x_false limit 1)", 51, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t t1 left join t t2 on t1.v = t2.v where (select b from x_false limit 1)", 56, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t t1 cross join t t2 where (select b from x_false limit 1)", 42, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t t1 asof join t t2 where (select b from x_false limit 1)", 41, BOOL_EXPECTED);
        });
    }

    @Test
    public void testLatestOnWhereSubQueryPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertExceptionNoLeakCheck("select * from t where (select b from x_false limit 1) latest on ts partition by sym", 23, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from t where sym in ('a') and (select b from x_false limit 1) latest on ts partition by sym", 40, BOOL_EXPECTED);
        });
    }

    @Test
    public void testLatestOnWithinWhereSubQueryPredicateRejected() throws Exception {
        // exercises the extractWithin path, which runs ahead of intrinsic extraction
        setProperty(PropertyKey.QUERY_WITHIN_LATEST_BY_OPTIMISATION_ENABLED, "true");
        assertMemoryLeak(() -> {
            execute("create table gt (ts timestamp, g geohash(8c), sym symbol index) timestamp(ts) partition by day");
            execute("insert into gt values ('2018-01-01', #sp052w92, 'a')");
            execute("create table x_false (b boolean)");
            execute("insert into x_false values (false)");
            assertExceptionNoLeakCheck("select * from gt where (select b from x_false limit 1) latest on ts partition by sym", 24, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from gt where sym in ('a') and (select b from x_false limit 1) latest on ts partition by sym", 41, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select * from gt where g within(#sp05) and (select b from x_false limit 1) latest on ts partition by sym", 44, BOOL_EXPECTED);
        });
    }

    @Test
    public void testNestedModelSubQueryPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertExceptionNoLeakCheck("select * from (select * from t where (select b from x_false limit 1))", 38, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("with q as (select * from t where (select b from x_false limit 1)) select * from q", 34, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("select sum(v) from t where (select b from x_false limit 1)", 28, BOOL_EXPECTED);
        });
    }

    @Test
    public void testNotSubQueryPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // the negation must not be silently discarded; NOT over a cursor is rejected
            assertExceptionNoLeakCheck("select * from t where not (select b from x_true limit 1)", 27,
                    "argument type mismatch for function `not` at #1 expected: BOOLEAN, actual: CURSOR");
            // double negation collapses to the bare sub-query
            assertExceptionNoLeakCheck("select * from t where not not (select b from x_true limit 1)", 31, BOOL_EXPECTED);
        });
    }

    @Test
    public void testOrSubQueryPredicateRejected() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // consistency control: the OR shape always produced this error; the other
            // shapes above must fail validation the same way instead of being dropped
            assertExceptionNoLeakCheck("select * from t where v = 5 or (select b from x_false limit 1)", 32, TYPE_MISMATCH);
        });
    }

    @Test
    public void testScalarSubQueryComparisonsStillWork() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String expected = "ts\tv\tsym\n2018-01-01T00:00:00.000000Z\t1\ta\n";
            printSql("select * from t where ts = (select max(ts) from t)");
            TestUtils.assertEquals(expected, sink);
            printSql("select * from t where v = (select 1)");
            TestUtils.assertEquals(expected, sink);
            printSql("select * from t where v > (select 0)");
            TestUtils.assertEquals(expected, sink);
            printSql("select * from t where not (v = 5)");
            TestUtils.assertEquals(expected, sink);
        });
    }

    @Test
    public void testUpdateWhereSubQueryPredicateRejectedAndRowsUntouched() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // used to be silently dropped: the UPDATE modified every row
            assertExceptionNoLeakCheck("update t set v = 42 where (select false)", 27, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("update t set v = 42 where (select b from x_false limit 1)", 27, BOOL_EXPECTED);
            assertExceptionNoLeakCheck("update t set v = 42 where ts = '2018-01-01' and (select b from x_false limit 1)", 49, BOOL_EXPECTED);
            printSql("select count() from t where v = 42");
            TestUtils.assertEquals("count\n0\n", sink);
        });
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
