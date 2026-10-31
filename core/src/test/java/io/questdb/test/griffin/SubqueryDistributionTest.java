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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.functions.test.TestFaultFunctionFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Every consumer of a sub-query evaluates its own copy of it, so the optimiser distributes a predicate over a
 * sub-query into several places only when the sub-query's plan proves that every evaluation within one execution
 * yields the same rows; a predicate over any other sub-query keeps a single consumer.
 */
public class SubqueryDistributionTest extends AbstractCairoTest {
    private static final String DISTRIBUTED_BOUND = "SELECT ts FROM (SELECT ts FROM a UNION ALL SELECT ts FROM b) WHERE ts > (SELECT max(ts) FROM c)";
    private static final String DISTRIBUTED_BOUND_ROWS = """
            ts
            1970-01-01T00:00:00.000026Z
            1970-01-01T00:00:00.000027Z
            1970-01-01T00:00:00.000028Z
            1970-01-01T00:00:00.000029Z
            1970-01-01T00:00:00.000030Z
            1970-01-01T00:00:00.000026Z
            1970-01-01T00:00:00.000027Z
            1970-01-01T00:00:00.000028Z
            1970-01-01T00:00:00.000029Z
            1970-01-01T00:00:00.000030Z
            """;

    @Test
    public void testCachedFactoryEvaluatesDistributedSubqueryOnEveryExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT count() FROM (SELECT ts FROM a UNION ALL SELECT ts FROM b) WHERE ts > (SELECT max(ts) FROM c)")
                    .noLeakCheck()
                    .noRandomAccess()
                    .mutateWith("INSERT INTO c VALUES (27::TIMESTAMP)")
                    .expectSize()
                    .returns(
                            """
                                    count
                                    10
                                    """,
                            """
                                    count
                                    6
                                    """
                    );
        });
    }

    @Test
    public void testFailureDuringDistributedEvaluation() throws Exception {
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT ts FROM (SELECT ts FROM a UNION ALL SELECT ts FROM b) WHERE ts > (SELECT max(ts) FROM c WHERE test_fault())";
            try (RecordCursorFactory factory = select(sql)) {
                TestFaultFunctionFactory.armToFailAfter(0);
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail();
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "test_fault: injected failure");
                } finally {
                    TestFaultFunctionFactory.disarm();
                }
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursor(DISTRIBUTED_BOUND_ROWS, cursor, factory.getMetadata(), true);
                }
            }
        });
    }

    @Test
    public void testNestedSubqueryOfDistributedSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT count() FROM (SELECT ts FROM a UNION ALL SELECT ts FROM b) "
                    + "WHERE ts > (SELECT max(ts) - 5 FROM a WHERE s IN (SELECT s FROM b WHERE x = 4))")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            count
                            14
                            """);
        });
    }

    @Test
    public void testNonDeterministicBoundIsNotDistributedIntoParallelFilters() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT count_distinct(ts) distinct_ts, count() matched FROM (SELECT ts FROM a UNION ALL SELECT ts FROM b) "
                    + "WHERE ts::LONG = (SELECT rnd_long(1, 30, 0) FROM long_sequence(1))";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Async Filter workers: 1\n");
            for (int i = 0; i < 10; i++) {
                assertQuery(sql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                distinct_ts\tmatched
                                1\t2
                                """);
            }
        });
    }

    @Test
    public void testNonDeterministicBoundIsNotDistributedIntoSetBranches() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = "SELECT count_distinct(ts) distinct_ts, count() matched FROM (SELECT ts FROM a UNION ALL SELECT ts FROM b) "
                    + "WHERE ts = (SELECT rnd_long(1, 30, 0)::TIMESTAMP FROM long_sequence(1))";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Interval forward scan on: a", "Interval forward scan on: b");
            for (int i = 0; i < 10; i++) {
                assertQuery(sql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                distinct_ts\tmatched
                                1\t2
                                """);
            }
        });
    }

    @Test
    public void testStableBoundIsDistributedIntoSetBranches() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery(DISTRIBUTED_BOUND)
                    .noLeakCheck()
                    .assertsPlanContaining("Interval forward scan on: a", "Interval forward scan on: b");
            assertQuery(DISTRIBUTED_BOUND).noLeakCheck().noRandomAccess().returns(DISTRIBUTED_BOUND_ROWS);
        });
    }

    @Test
    public void testSubqueryUnderGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("SELECT s, count() FROM (SELECT s, ts FROM a UNION ALL SELECT s, ts FROM b) "
                    + "WHERE ts::LONG > (SELECT max(x) - 5 FROM a) GROUP BY s ORDER BY s")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            s\tcount
                            S0\t4
                            S1\t2
                            S2\t4
                            """);
        });
    }

    @Test
    public void testTopKOverParallelGroupBySubqueryIsNotDistributed() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.setParallelGroupByEnabled(true);
            final String sql = "SELECT ts FROM (SELECT ts FROM a UNION ALL SELECT ts FROM b) "
                    + "WHERE ts > (SELECT max(last_ts) FROM (SELECT s, max(ts) last_ts FROM a GROUP BY s ORDER BY last_ts LIMIT 2))";
            assertQuery(sql)
                    .noLeakCheck()
                    .assertsPlanNotContaining("Interval forward scan on: a", "Interval forward scan on: b");
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("""
                            ts
                            1970-01-01T00:00:00.000030Z
                            1970-01-01T00:00:00.000030Z
                            """);
        });
    }

    private static void createTables() throws Exception {
        execute("CREATE TABLE a (s SYMBOL, x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE b (s SYMBOL, x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE c (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO a SELECT ('S' || (x % 3))::SYMBOL, x, x::TIMESTAMP FROM long_sequence(30)");
        execute("INSERT INTO b SELECT ('S' || (x % 3))::SYMBOL, x, x::TIMESTAMP FROM long_sequence(30)");
        execute("INSERT INTO c VALUES (25::TIMESTAMP)");
    }
}
