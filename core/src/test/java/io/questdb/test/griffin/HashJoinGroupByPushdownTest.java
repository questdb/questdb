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
 * Covers the inner-join ON conjunct branch of {@code SqlOptimiser.isWherePushableBelowJoin()}.
 * A WHERE predicate that reads only the preserved right side of a RIGHT join filters that side's
 * scan, as JoinTest#testRightJoinPreservedSideFilterRunsBeforeJoin shows. An ON conjunct of a later
 * INNER join that reads only that side keeps its routing: it stays a filter above the RIGHT join.
 * Each query runs with the fused hash join GROUP BY enabled and disabled, and both must plan and
 * answer alike.
 */
public class HashJoinGroupByPushdownTest extends AbstractCairoTest {
    private static final String CHAIN = "SELECT a.k ak, b.k bk, b.x, c.k ck FROM a RIGHT JOIN b ON a.k = b.k JOIN c ON c.k = b.k AND ";
    private static final String CHAIN_OVER_SUB_QUERY = "SELECT a.k ak, b.k bk, b.x, c.k ck FROM a RIGHT JOIN (SELECT * FROM b) b ON a.k = b.k JOIN c ON c.k = b.k AND ";
    private static final String GROUPED_CHAIN = "SELECT b.k, count(*) n, sum(c.w) w FROM a RIGHT JOIN b ON a.k = b.k JOIN c ON c.k = b.k AND ";
    private static final String POSITIVE = """
            ak\tbk\tx\tck
            1\t1\t100\t1
            null\t3\t300\t3
            null\t4\t400\t4
            """;

    @Test
    public void testInnerJoinOnConjunctReachesRightJoinPreservedInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (boolean isFusedEnabled : new boolean[]{true, false}) {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(isFusedEnabled);
                try {
                    // The later INNER predicate can filter the RIGHT join's preserved input.
                    assertQuery(CHAIN + "b.x > 0 ORDER BY bk")
                            .noLeakCheck()
                            .withPlanContaining("filter: 0<x", "Hash Right Outer Join Light")
                            .withPlanNotContaining("Filter filter: 0<b.x", "Hash Join Group By")
                            .returns(POSITIVE);
                    // A sub-query side takes the same route.
                    assertQuery(CHAIN_OVER_SUB_QUERY + "b.x > 0 ORDER BY bk")
                            .noLeakCheck()
                            .withPlanContaining("filter: 0<x", "Hash Right Outer Join Light")
                            .withPlanNotContaining("Filter filter: 0<b.x", "Hash Join Group By")
                            .returns(POSITIVE);
                    // Column and constant equalities follow the same pushdown rule.
                    assertQuery(CHAIN + "b.x = b.y ORDER BY bk")
                            .noLeakCheck()
                            .withPlanContaining("filter: x=y")
                            .withPlanNotContaining("Filter filter: b.x=b.y", "Hash Join Group By")
                            .returns("""
                                    ak\tbk\tx\tck
                                    1\t1\t100\t1
                                    null\t3\t300\t3
                                    """);
                    assertQuery(CHAIN + "b.x = 400 ORDER BY bk")
                            .noLeakCheck()
                            .withPlanContaining("filter: x=400")
                            .withPlanNotContaining("Filter filter: b.x=400", "Hash Join Group By")
                            .returns("""
                                    ak\tbk\tx\tck
                                    null\t4\t400\t4
                                    """);
                    // The preserved timestamp predicate becomes an interval scan.
                    assertQuery(CHAIN + "b.ts >= '2024-01-03' ORDER BY bk")
                            .noLeakCheck()
                            .withPlanContaining("Interval forward scan on: b")
                            .withPlanNotContaining("Filter filter: b.ts>=", "Hash Join Group By")
                            .returns("""
                                    ak\tbk\tx\tck
                                    null\t3\t300\t3
                                    null\t4\t400\t4
                                    """);
                    // The fused analysis keeps the ordinary plan for a GROUP BY over the chain.
                    assertQuery(GROUPED_CHAIN + "b.x > 0 ORDER BY b.k")
                            .noLeakCheck()
                            .expectSize()
                            .withPlanContaining("filter: 0<x")
                            .withPlanNotContaining("Filter filter: 0<b.x", "Hash Join Group By")
                            .returns("""
                                    k\tn\tw
                                    1\t1\t1000
                                    3\t1\t3000
                                    4\t1\t4000
                                    """);
                } finally {
                    sqlExecutionContext.setParallelHashJoinGroupByEnabled(true);
                }
            }
        });
    }

    @Test
    public void testInnerJoinOnConjunctReachesRightJoinPreservedSideThroughSubQuery() throws Exception {
        // An ON conjunct that reads only a sub-query moves into the sub-query as a WHERE predicate,
        // with innerPredicate cleared, so inside it the RIGHT join admits the predicate below itself
        // and runs it in b's scan. The filter reduces the rows that the ordinary plan hashes, so the
        // fused analysis keeps the ordinary plan, and both settings plan alike.
        assertMemoryLeak(() -> {
            createTables();
            final String sql = """
                    SELECT f.k, f.n, c.w
                    FROM (SELECT b.k, b.x, count(*) n FROM a RIGHT JOIN b ON a.k = b.k GROUP BY b.k, b.x) f
                    JOIN c ON c.k = f.k AND f.x > 0
                    ORDER BY f.k
                    """;
            final String expected = """
                    k\tn\tw
                    1\t1\t1000
                    3\t1\t3000
                    4\t1\t4000
                    """;
            for (boolean isFusedEnabled : new boolean[]{true, false}) {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(isFusedEnabled);
                try {
                    assertQuery(sql)
                            .noLeakCheck()
                            .withPlanContaining("Hash Right Outer Join Light", "filter: 0<x")
                            .withPlanNotContaining("Hash Join Group By", "Filter filter: 0<")
                            .returns(expected);
                } finally {
                    sqlExecutionContext.setParallelHashJoinGroupByEnabled(true);
                }
            }
        });
    }

    private void createTables() throws Exception {
        execute("CREATE TABLE a (k INT, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE b (k INT, x INT, y INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE c (k INT, w INT)");
        execute("""
                INSERT INTO a VALUES
                    (1, 10, '2024-01-01T00:00:00.000000Z'),
                    (2, 20, '2024-01-02T00:00:00.000000Z'),
                    (5, 50, '2024-01-03T00:00:00.000000Z')
                """);
        execute("""
                INSERT INTO b VALUES
                    (1, 100, 100, '2024-01-01T00:00:00.000000Z'),
                    (2, -1, 7, '2024-01-02T00:00:00.000000Z'),
                    (3, 300, 300, '2024-01-03T00:00:00.000000Z'),
                    (4, 400, 1, '2024-01-04T00:00:00.000000Z')
                """);
        execute("INSERT INTO c VALUES (1, 1_000), (2, 2_000), (3, 3_000), (4, 4_000), (6, 6_000)");
    }
}
