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

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Binds sub-queries in the middle of the block state of the query that contains them, so a sub-query that bound
 * with the scope of its container would corrupt the container's result.
 */
public class BindScopeNestingTest extends AbstractCairoTest {

    @Test
    public void testGroupedSubqueryInsideAggregateArgument() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT k, sum(CASE WHEN v > (SELECT count() FROM (SELECT u.k FROM u JOIN t ON u.k = t.k GROUP BY u.k)) THEN v ELSE 0 END) s
                    FROM t
                    ORDER BY k
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ts
                            a\t3
                            b\t4
                            """);
        });
    }

    @Test
    public void testLateralBodyHoldsSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT a.k, l.c
                    FROM u a
                    CROSS JOIN LATERAL (SELECT count() c FROM t WHERE t.k = a.k AND t.v > (SELECT min(v) FROM t)) l
                    ORDER BY a.k
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tc
                            a\t1
                            b\t2
                            """);
        });
    }

    @Test
    public void testLateralInsideSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT k, v
                    FROM t
                    WHERE v * 5 = (SELECT max(l.w) FROM u a CROSS JOIN LATERAL (SELECT w FROM u b WHERE b.k = a.k AND b.w > 15) l)
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tv
                            b\t4
                            """);
        });
    }

    @Test
    public void testPivotOverJoinFilteredBySubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT * FROM (
                        SELECT t.k, t.v FROM t JOIN u ON t.k = u.k
                        WHERE t.v > (SELECT min(t2.v) FROM t t2 JOIN u u2 ON t2.k = u2.k WHERE u2.w > 5)
                    ) PIVOT (sum(v) FOR k IN (SELECT DISTINCT k FROM u WHERE w >= (SELECT min(w) FROM u) ORDER BY k))
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            a\tb
                            3\t6
                            """);
        });
    }

    @Test
    public void testSubqueryInsideGroupedOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT k, sum(v) s
                    FROM t
                    ORDER BY CASE WHEN sum(v) * 4 > (SELECT max(m) FROM (SELECT k, max(w) m FROM u ORDER BY m DESC)) THEN 0 ELSE 1 END, k
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            k\ts
                            b\t6
                            a\t4
                            """);
        });
    }

    @Test
    public void testSubqueryInsideQualifiedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT a.k, b.k, CASE WHEN a.v < (SELECT count() FROM t x JOIN u y ON x.k = y.k) THEN a.v ELSE 0 END c
                    FROM t a
                    JOIN u b ON a.k = b.k
                    ORDER BY a.v
                    """)
                    .noLeakCheck()
                    .returns("""
                            k\tk1\tc
                            a\ta\t1
                            b\tb\t2
                            a\ta\t3
                            b\tb\t0
                            """);
        });
    }

    @Test
    public void testSubqueryInsideWindowArgument() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT v, sum(CASE WHEN v < (SELECT min(w) FROM u) THEN v ELSE 0 END) OVER (ORDER BY ts) cs
                    FROM t
                    ORDER BY ts
                    """)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            v\tcs
                            1\t1.0
                            2\t3.0
                            3\t6.0
                            4\t10.0
                            """);
        });
    }

    @Test
    public void testWindowedSubqueryInsideWindowedProjection() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertQuery("""
                    SELECT v, row_number() OVER (ORDER BY ts) rn,
                           CASE WHEN v <= (SELECT max(r) FROM (SELECT row_number() OVER (PARTITION BY k ORDER BY w) r FROM u)) THEN 'x' ELSE 'y' END f,
                           sum(v) OVER () total
                    FROM t
                    ORDER BY ts
                    """)
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            v\trn\tf\ttotal
                            1\t1\tx\t10.0
                            2\t2\ty\t10.0
                            3\t3\ty\t10.0
                            4\t4\ty\t10.0
                            """);
        });
    }

    private static void createTables() throws SqlException {
        execute("CREATE TABLE t (k SYMBOL, v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("""
                INSERT INTO t VALUES
                ('a', 1, '2024-01-01T00:00:00.000000Z'),
                ('b', 2, '2024-01-01T01:00:00.000000Z'),
                ('a', 3, '2024-01-01T02:00:00.000000Z'),
                ('b', 4, '2024-01-01T03:00:00.000000Z')
                """);
        execute("CREATE TABLE u (k SYMBOL, w INT)");
        execute("INSERT INTO u VALUES ('a', 10), ('b', 20)");
    }
}
