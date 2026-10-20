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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.QueryAssertion;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Plans a GROUP BY over a hash join that reads a live view. The view serves the rows of its
 * table and, from its in-memory tier, the rows that the refresh has not flushed to that table
 * yet, so the view can serve more rows than its table holds.
 */
public class LiveViewHashJoinGroupByTest extends AbstractLiveViewTest {

    // A non-SEED view drops base rows below the CREATE wall-clock moment, so pin the test clock
    // below the test data.
    @Before
    public void pinClockBelowTestData() {
        setCurrentMicros(0L);
    }

    // A RIGHT join builds the input before it. Here that input is a live view whose table holds
    // one row, fewer than the dimension's two, while the view serves seven. The ordinary plan
    // hashes the two dimension rows, so the fused plan would build more rows, and the planner
    // keeps the ordinary plan. The LEFT spelling builds the view in both plans and stays fused.
    // A view after RIGHT JOIN serves at least the rows of its table, so a RIGHT join that
    // preserves the view still fuses when the input before it holds no more rows than that table.
    @Test
    public void testRightJoinBuildingLiveViewKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP, g SYMBOL) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s IN MEMORY 30m START FROM NOW AS " +
                    "SELECT sym, val, ts, count(*) OVER (PARTITION BY g ORDER BY ts ROWS BETWEEN 1_000_000 PRECEDING AND CURRENT ROW) AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // The first refresh flushes its row to the view's table.
                execute("INSERT INTO base (sym, val, ts) VALUES ('a', 1, '2026-01-01T00:00:01.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
                // Within FLUSH EVERY, with the clock still at 0, the next six rows stay in the
                // in-memory tier.
                execute("""
                        INSERT INTO base (sym, val, ts) VALUES
                            ('a', 1, '2026-01-01T00:00:02.000000Z'), ('a', 1, '2026-01-01T00:00:03.000000Z'),
                            ('a', 2, '2026-01-01T00:00:04.000000Z'), ('a', 2, '2026-01-01T00:00:05.000000Z'),
                            ('a', 2, '2026-01-01T00:00:06.000000Z'), ('a', 3, '2026-01-01T00:00:07.000000Z')
                        """);
                drainWalQueue();
                drainJob(job);
            }
            execute("CREATE TABLE dim (w INT, label SYMBOL)");
            execute("INSERT INTO dim VALUES (1, 'one'), (2, 'two')");
            execute("CREATE TABLE dim1 (w INT, label SYMBOL)");
            execute("INSERT INTO dim1 VALUES (1, 'one')");
            try (TableReader reader = getReader("lv")) {
                Assert.assertEquals(1, reader.size());
            }
            assertQuery("SELECT count() n FROM lv")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            n
                            7
                            """);

            final String rightSql = "SELECT d.w, count(l.val) m, sum(l.val) s FROM lv l RIGHT JOIN dim d ON l.val = d.w ORDER BY d.w";
            final String leftSql = "SELECT d.w, count(l.val) m, sum(l.val) s FROM dim d LEFT JOIN lv l ON d.w = l.val ORDER BY d.w";
            final String expected = """
                    w\tm\ts
                    1\t3\t3
                    2\t3\t6
                    """;
            final String preservedSql = "SELECT l.val, count(d.w) m, count() n FROM dim1 d RIGHT JOIN lv l ON d.w = l.val ORDER BY l.val";
            final String preservedExpected = """
                    val\tm\tn
                    1\t3\t3
                    2\t0\t3
                    3\t0\t1
                    """;
            for (boolean isFused : new boolean[]{false, true}) {
                try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)
                        .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null)) {
                    context.setParallelGroupByEnabled(true);
                    context.setParallelHashJoinGroupByEnabled(isFused);
                    final QueryAssertion leftAssertion = assertQuery(leftSql)
                            .withContext(context)
                            .noLeakCheck()
                            .expectSize();
                    if (isFused) {
                        leftAssertion.withPlanContaining("Async Hash Join Group By", "Build\n            LiveView", "inMemory: true");
                    } else {
                        leftAssertion.withPlanNotContaining("Hash Join Group By");
                    }
                    leftAssertion.returns(expected);
                    final QueryAssertion preservedAssertion = assertQuery(preservedSql)
                            .withContext(context)
                            .noLeakCheck()
                            .expectSize();
                    if (isFused) {
                        preservedAssertion.withPlanContaining("Async Hash Join Group By", "logicalJoinType: right outer", "LiveView", "inMemory: true");
                    } else {
                        preservedAssertion.withPlanNotContaining("Hash Join Group By");
                    }
                    preservedAssertion.returns(preservedExpected);
                    assertQuery(rightSql)
                            .withContext(context)
                            .noLeakCheck()
                            .expectSize()
                            .withPlanContaining("Hash Right Outer Join", "LiveView", "inMemory: true")
                            .withPlanNotContaining("Hash Join Group By")
                            .returns(expected);
                }
            }
            execute("DROP LIVE VIEW lv");
        });
    }
}
